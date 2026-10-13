// Durable Object `PgliteDb`: DO の中で PGlite (@electric-sql/pglite、postgres の wasm 版) を動かし、
// Worker から `Stub::connect` で届いた TCP の生バイトを postgres の wire protocol としてそのまま PGlite へ流す試作
// (Refs ohishi-exp/rust-leave-worker#1)。測るものは README の「PGlite の probe」。
//
// - 外から届く口は無い: HTTP の fetch は 404 だけ。到達できるのは DO binding を持つ Worker だけ。
//   **認可はしない。** 接続は superuser の session を alc_api_rt に切り替えたものなので、接続側が
//   `RESET SESSION AUTHORIZATION` すれば superuser に戻れる。seed の 2 テナントだけを置く測定用で、本実装の境界にはしない
// - 永続化しない (揮発)。最初の connect の中で起動し、migration 全量を流す (グローバルスコープでは起動しない)
// - 接続は 1 本ずつ直列に捌く (後から来た接続は前の接続が閉じるまで待つ)
// - frame の切り方は @electric-sql/pglite-socket の PGLiteSocketHandler と同じ (SSLRequest に 'N'、
//   CancelRequest は捨てる、StartupMessage は丸ごと、それ以外は 1 + 長さで 1 通ずつ execProtocolRawStream)
import { DurableObject } from "cloudflare:workers";
import { PGlite } from "@electric-sql/pglite";
// Workers は wasm を実行時にコンパイルできないので、wasm と data は bundle に同梱して渡す
// (package の exports が dist を出していないので相対パスで取る)。pglite.wasm だけは memory の宣言を書き換えた版
// (scripts/gen-prebuilt-wasm.mjs が作る)
import pgliteWasm from "../.prebuilt/pglite.wasm";
// initdb.wasm は実行しない (datadir は同梱)。渡さないと PGlite が裏で import.meta.url から取りに行くので、module だけ渡す
import initdbWasm from "../node_modules/@electric-sql/pglite/dist/initdb.wasm";
import pgliteData from "../node_modules/@electric-sql/pglite/dist/pglite.data";
// initdb を済ませた空の datadir (gzip の tar。scripts/gen-prebuilt-wasm.mjs が作る)。実行時に initdb を走らせない
import dataDir from "../.prebuilt/datadir.tar.gz";
// scripts/fetch-migrations.sh が ALC_MIGRATIONS_REV の版から作る (commit しない)
import bundle from "../.alc-migrations/bundle.json";
import { PGLITE_INITIAL_MIB, POSTGRESQL_CONF, RUNTIME_ROLE, setupSteps } from "./steps.js";
import { installWasmShim } from "./wasm-shim";

const SSL_REQUEST = 80877103;
const CANCEL_REQUEST = 80877102;
const PROTOCOL_V3 = 196608;

function errorText(e: unknown): string {
  return e instanceof Error ? e.message : String(e);
}

/// PGlite の wasm の線形メモリ (isolate 全体ではない)
function heapBytes(db: PGlite): number | null {
  const mod = (db as unknown as { mod?: { HEAPU8?: Uint8Array } }).mod;
  return mod?.HEAPU8?.length ?? null;
}

/// 起動から migration・seed までを流す。結果は 1 行の JSON で出す (失敗したらどこで止まったか・エラーの原文も)
async function start(): Promise<PGlite> {
  const started = Date.now();
  const report: Record<string, unknown> = { probe: "pglite-db", rev: bundle.rev };
  let step = "create";
  let db: PGlite | undefined;
  try {
    installWasmShim();
    db = await PGlite.create({
      pgliteWasmModule: pgliteWasm,
      initdbWasmModule: initdbWasm,
      fsBundle: new Blob([pgliteData]),
      loadDataDir: new Blob([dataDir]),
      initialMemory: PGLITE_INITIAL_MIB * 1024 * 1024,
      postgresqlconf: POSTGRESQL_CONF,
    });
    report.ready_ms = Date.now() - started;
    report.heap_bytes_ready = heapBytes(db);

    const setupStarted = Date.now();
    let migrateMs = 0;
    let applied = 0;
    for (const s of setupSteps(bundle)) {
      step = s.name;
      const stepStarted = Date.now();
      try {
        await db.exec(s.sql);
      } catch (e) {
        if (db.isInTransaction()) await db.exec("ROLLBACK");
        throw e;
      }
      if (s.migration) {
        migrateMs += Date.now() - stepStarted;
        applied += 1;
      }
    }
    report.migrations = applied;
    report.migrate_ms = migrateMs;
    report.setup_ms = Date.now() - setupStarted;
    report.total_ms = Date.now() - started;
    report.heap_bytes = heapBytes(db);
    report.ok = true;
    console.log(JSON.stringify(report));
    return db;
  } catch (e) {
    report.ok = false;
    report.failed_at = step;
    report.error = errorText(e);
    if (e instanceof Error) report.stack = e.stack;
    report.elapsed_ms = Date.now() - started;
    if (db) report.heap_bytes = heapBytes(db);
    console.log(JSON.stringify(report));
    throw e;
  }
}

/// 受け取ったバイト列の頭から、完結した 1 通を切り出す (足りなければ null)。
/// StartupMessage・SSLRequest・CancelRequest は種別の 1 バイトを持たない (長さ + コード)
function nextMessage(buf: Uint8Array, startup: boolean): { kind: "ssl" | "cancel" | "msg"; len: number } | null {
  const view = new DataView(buf.buffer, buf.byteOffset, buf.byteLength);
  if (startup) {
    if (buf.length < 8) return null;
    const len = view.getInt32(0);
    const code = view.getInt32(4);
    // postgres でない先頭 (probe-hyperdrive の echo など) は、残りを待たずに閉じる (直列の接続を塞がない)
    if (code !== SSL_REQUEST && code !== CANCEL_REQUEST && code !== PROTOCOL_V3) {
      throw new Error(`unsupported startup code ${code}`);
    }
    if (buf.length < len) return null;
    if (len === 8 && code === SSL_REQUEST) return { kind: "ssl", len };
    if (len === 16 && code === CANCEL_REQUEST) return { kind: "cancel", len };
    return { kind: "msg", len };
  }
  if (buf.length < 5) return null;
  const len = 1 + view.getInt32(1);
  return buf.length < len ? null : { kind: "msg", len };
}

function concat(a: Uint8Array, b: Uint8Array): Uint8Array {
  const out = new Uint8Array(a.length + b.length);
  out.set(a);
  out.set(b, a.length);
  return out;
}

interface Env {
  /// "echo" なら PGlite を起動せず、connect で受けたバイトをそのまま返す (JS の DO の connect 経路だけを確かめる。
  /// `wrangler deploy --env staging --var PGLITE_MODE:echo`)。それ以外は PGlite
  PGLITE_MODE?: string;
}

export class PgliteDb extends DurableObject<Env> {
  private db: Promise<PGlite> | null = null;
  /// 接続を 1 本ずつに直列化する鎖
  private queue: Promise<void> = Promise.resolve();

  async fetch(): Promise<Response> {
    return new Response("not found", { status: 404 });
  }

  async connect(socket: Socket): Promise<void> {
    if (this.env.PGLITE_MODE === "echo") {
      const started = Date.now();
      const reader = socket.readable.getReader();
      const writer = socket.writable.getWriter();
      let bytes = 0;
      try {
        for (;;) {
          const { value, done } = await reader.read();
          if (done) break;
          bytes += value.length;
          await writer.write(value);
        }
        console.log(JSON.stringify({ probe: "pglite-db", mode: "echo", ok: true, bytes, ms: Date.now() - started }));
      } catch (e) {
        console.log(JSON.stringify({ probe: "pglite-db", mode: "echo", ok: false, bytes, error: errorText(e) }));
      } finally {
        await socket.close().catch(() => {});
      }
      return;
    }
    const turn = this.queue.then(() => this.serve(socket));
    this.queue = turn.catch(() => {});
    return turn;
  }

  /// 最初の接続で起動する。起動に失敗したら次の接続で起動し直す
  private async ready(): Promise<PGlite> {
    this.db ??= start();
    try {
      return await this.db;
    } catch (e) {
      this.db = null;
      throw e;
    }
  }

  private async serve(socket: Socket): Promise<void> {
    const writer = socket.writable.getWriter();
    const reader = socket.readable.getReader();
    // PGlite は、最初の postgres のメッセージ (StartupMessage) が届いてから起動する (postgres でない接続では起動しない)
    let db: PGlite | null = null;
    let buf: Uint8Array<ArrayBufferLike> = new Uint8Array(0);
    let startup = true;
    let messages = 0;
    try {
      read: for (;;) {
        const { value, done } = await reader.read();
        if (done) break;
        buf = concat(buf, value);
        for (;;) {
          const next = nextMessage(buf, startup);
          if (!next) break;
          const message = buf.slice(0, next.len);
          buf = buf.slice(next.len);
          if (next.kind === "ssl") {
            await writer.write(new TextEncoder().encode("N"));
            continue;
          }
          if (next.kind === "cancel") continue;
          // Terminate: PGlite には渡さずにここで閉じる (後片付けは finally)
          if (!startup && message[0] === 0x58) break read;
          db ??= await this.ready();
          const pg = db;
          const out: Uint8Array[] = [];
          await pg.runExclusive(() =>
            pg.execProtocolRawStream(message, { onRawData: (chunk) => out.push(chunk.slice()) }),
          );
          if (startup) {
            startup = false;
            // 応答 (AuthenticationOk … ReadyForQuery) を返す前に、この接続を実行用ロールにする
            await db.exec(`SET SESSION AUTHORIZATION ${RUNTIME_ROLE}`);
          }
          for (const chunk of out) await writer.write(chunk);
          messages += 1;
        }
      }
    } catch (e) {
      console.log(JSON.stringify({ probe: "pglite-db", connection_error: errorText(e), messages }));
    } finally {
      // 次の接続へ持ち越さない: 開いたままの transaction・SET・ロールを戻す
      try {
        if (db) {
          if (db.isInTransaction()) await db.exec("ROLLBACK");
          await db.exec("RESET ALL; RESET SESSION AUTHORIZATION;");
        }
      } catch (e) {
        console.log(JSON.stringify({ probe: "pglite-db", reset_error: errorText(e) }));
      }
      await socket.close().catch(() => {});
    }
  }
}

export default {
  async fetch(): Promise<Response> {
    return new Response("not found", { status: 404 });
  },
} satisfies ExportedHandler;

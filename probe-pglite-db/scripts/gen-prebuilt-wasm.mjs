// DO に同梱する wasm を .prebuilt/ に作る (wrangler.toml の [build] が fetch-migrations.sh の後に通す)。
//   node scripts/gen-prebuilt-wasm.mjs
//
// 1. pglite.wasm の線形メモリの宣言 (`env.memory` の import の最小 2048 pages = 128 MiB。isolate の上限 128 MB と同じ) を
//    PGLITE_INITIAL_MIB (src/steps.js) に書き換えた版を .prebuilt/pglite.wasm に出す。中身 (コード・data) は変えない
// 2. Workers は実行時の wasm のコンパイル (`new WebAssembly.Module(bytes)`・`WebAssembly.instantiate(bytes)`) を許さない。
//    PGlite (Emscripten) は起動と migration の途中で 2 種類の wasm を bytes からコンパイルする:
//      - `addFunction` が JS の関数を関数表に入れるために作る、シグネチャごとの小さな module
//      - pglite.data の中の拡張 (dict_snowball.so・plpgsql.so 等の side module) を dlopen するとき
//    node で DO と同じ流れ (1 の版で起動 → migration 全量 → RLS 付きの読み) を 1 回通してその bytes を捕まえ、.prebuilt/ に
//    .wasm として書き出す (bundle に CompiledWasm で入る)。実行時は src/wasm-shim.ts が、同じ bytes のコンパイルを同梱のものに差し替える
import { mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { createRequire } from "node:module";
import { PGlite } from "@electric-sql/pglite";
import { PGLITE_INITIAL_MIB, POSTGRESQL_CONF, RUNTIME_ROLE, SEED_TENANTS, setupSteps } from "../src/steps.js";
import { wasmKey } from "../src/wasm-key.js";

const require = createRequire(import.meta.url);
const bundle = JSON.parse(readFileSync(new URL("../.alc-migrations/bundle.json", import.meta.url), "utf8"));
const out = new URL("../.prebuilt/", import.meta.url);
const dist = (f) => new Uint8Array(readFileSync(require.resolve("@electric-sql/pglite").replace(/index\.c?js$/, f)));

/// wasm の import section の `env.memory` の最小 pages を書き換える (LEB128 は元と同じ長さで書き戻すので、ほかの位置はずれない)
function patchMemoryMin(src, pages) {
  const b = src.slice();
  let p = 8;
  const leb = () => {
    let r = 0, s = 0, c;
    do { c = b[p++]; r |= (c & 0x7f) << s; s += 7; } while (c & 0x80);
    return r >>> 0;
  };
  const skipName = () => { const n = leb(); p += n; };
  let found = null;
  while (p < b.length) {
    const id = b[p++];
    const size = leb();
    const end = p + size;
    if (id === 2) {
      const count = leb();
      for (let i = 0; i < count; i++) {
        skipName();
        skipName();
        const kind = b[p++];
        if (kind === 0) leb();
        else if (kind === 1) { p++; const fl = leb(); leb(); if (fl & 1) leb(); }
        else if (kind === 2) {
          const flags = leb();
          const at = p;
          found = leb();
          const len = p - at;
          if (pages >= 2 ** (7 * len)) throw new Error(`${pages} pages は ${len} バイトの LEB128 に収まらない`);
          let v = pages;
          for (let j = 0; j < len; j++) { b[at + j] = (v & 0x7f) | (j < len - 1 ? 0x80 : 0); v >>>= 7; }
          if (flags & 1) leb();
        } else if (kind === 3) p += 2;
      }
    }
    p = end;
  }
  if (found === null) throw new Error("pglite.wasm に memory の import が無い");
  return { bytes: b, from: found };
}

const original = { pglite: dist("pglite.wasm"), initdb: dist("initdb.wasm") };
const patched = patchMemoryMin(original.pglite, PGLITE_INITIAL_MIB * 16);
// DO には module として渡すので、本体と initdb は捕まえない
const skip = new Set([wasmKey(patched.bytes), wasmKey(original.pglite), wasmKey(original.initdb)]);

const captured = new Map();
function capture(src) {
  if (src instanceof WebAssembly.Module) return;
  const bytes = ArrayBuffer.isView(src)
    ? new Uint8Array(src.buffer, src.byteOffset, src.byteLength)
    : new Uint8Array(src);
  const key = wasmKey(bytes);
  if (!skip.has(key)) captured.set(key, bytes.slice());
}
const OriginalModule = WebAssembly.Module;
WebAssembly.Module = new Proxy(OriginalModule, {
  construct(target, args) {
    capture(args[0]);
    return Reflect.construct(target, args);
  },
});
const originalInstantiate = WebAssembly.instantiate;
WebAssembly.instantiate = (src, ...rest) => {
  capture(src);
  return originalInstantiate(src, ...rest);
};

const db = await PGlite.create({
  pgliteWasmModule: new OriginalModule(patched.bytes),
  initialMemory: PGLITE_INITIAL_MIB * 1024 * 1024,
  postgresqlconf: POSTGRESQL_CONF,
});
for (const step of setupSteps(bundle)) await db.exec(step.sql);
await db.exec(`SET SESSION AUTHORIZATION ${RUNTIME_ROLE}`);
const counts = [];
for (const t of SEED_TENANTS) {
  const { rows } = await db.query("SELECT id FROM alc_api.tenants WHERE name = $1", [t.name]);
  counts.push(
    await db.transaction(async (tx) => {
      await tx.query("SELECT set_config('app.current_tenant_id', $1, true)", [rows[0].id]);
      return (await tx.query("SELECT count(*)::int AS n FROM alc_api.employees")).rows[0].n;
    }),
  );
}
if (counts.join() !== SEED_TENANTS.map((t) => t.employees).join()) throw new Error(`seed の行数が違う: ${counts}`);
await db.exec("RESET ALL; RESET SESSION AUTHORIZATION;");
await db.close();

rmSync(out, { recursive: true, force: true });
mkdirSync(out, { recursive: true });
writeFileSync(new URL("pglite.wasm", out), patched.bytes);
const keys = [...captured.keys()].sort();
keys.forEach((key, i) => writeFileSync(new URL(`m${i}.wasm`, out), captured.get(key)));
writeFileSync(
  new URL("index.js", out),
  "// scripts/gen-prebuilt-wasm.mjs が作る (commit しない)\n" +
    keys.map((_, i) => `import m${i} from "./m${i}.wasm";`).join("\n") +
    "\nexport default new Map([\n" +
    keys.map((key, i) => `  [${JSON.stringify(key)}, m${i}],`).join("\n") +
    "\n]);\n",
);
const total = keys.reduce((n, k) => n + captured.get(k).length, 0);
console.log(
  `pglite.wasm の memory の最小を ${patched.from} → ${PGLITE_INITIAL_MIB * 16} pages に書き換え、` +
    `実行時にコンパイルされる wasm ${keys.length} 個 (${total} bytes) と一緒に .prebuilt/ に書き出した`,
);

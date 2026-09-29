#!/usr/bin/env node
// probe-unknown / probe-emscripten の /zip?mb=N&mode=flush|stream|copy・/sign・/pdf を各 n 回 (既定 10) 叩き、
// p50 を両ターゲット並べて出す。mode=flush (運行ごとに R2 へ書き出して捨てる版) は probe-unknown だけ
// (emscripten の列は "-")。
// URL と Cloudflare Access の service token は env から受ける (repo に値を書かない):
//
//   PROBE_UNKNOWN_URL=https://<probe-unknown の staging> \
//   PROBE_EMSCRIPTEN_URL=https://<probe-emscripten の staging> \
//   CF_ACCESS_CLIENT_ID=... CF_ACCESS_CLIENT_SECRET=... \
//   node bench/probe.mjs [-n 10] [--mb 1,5,10,20] [--modes flush,stream,copy] [--only zip,sign,pdf] [--timeout 900] [--json]
//
// --modes は /zip で叩く mode、--only は叩くエンドポイント (既定はどちらも全部)。--timeout は 1 回の上限 (秒)。
// 超えた回は打ち切って timeout と数える (全回 timeout ならセルは timeout)。
// 片方の URL だけでも動く (無い方は表で "-")。ローカルの wrangler dev を叩くときは Access の env は要らない。
//
// 列:
//   wall      クライアントで測った往復 (p50)。Cloudflare 上の Date.now は I/O まで進まないので、
//             worker が返す *_ms は staging ではほぼ 0 になる。CPU の重さはこの列と dashboard の CPU 時間で見る
//   worker    worker が返した処理時間の p50 (/zip は process.total_ms、/sign は jwt の sign_ms、/pdf は Server-Timing の total)
//   mem       線形メモリ (after) の最大 / ヒープの最大 (heap_peak) の最大。線形メモリは縮まないので isolate ごとの
//             高水位で、同じ isolate で先に叩いた処理のピークに隠れる。そのため /zip は mb ごとに flush → stream →
//             copy の順 (軽い順) に叩き、mode どうしの比較は heap_peak (worker のアロケータが数えた、処理中に同時に
//             生きていた確保の最大。isolate の前の処理に左右されない) で見る
//   ok        200 かつ中身が期待どおり (/zip は match、/sign は各方式の ok) だった回数 / n。
//             失敗は status ごとに数える (メモリ超過は 500 系 / 接続断になる)
// 各エンドポイントの先頭 1 回は暖機として捨てる (isolate の起動と、/sign の鍵生成を外す)。
// /sign の keygen は暖機の回の値 (その isolate で鍵を作ったときだけ出る)。

const args = process.argv.slice(2);
const asJson = args.includes("--json");
const nIdx = args.indexOf("-n");
const n = nIdx >= 0 ? Number(args[nIdx + 1]) : 10;
if (!Number.isInteger(n) || n < 1) {
  console.error("-n は 1 以上の整数");
  process.exit(2);
}
const mbIdx = args.indexOf("--mb");
const mbs = (mbIdx >= 0 ? args[mbIdx + 1] : "1,5,10,20").split(",").map(Number);
if (mbs.some((m) => !Number.isInteger(m) || m < 1 || m > 20)) {
  console.error("--mb は 1..=20 の整数をカンマ区切りで");
  process.exit(2);
}
const modesIdx = args.indexOf("--modes");
const modes = (modesIdx >= 0 ? args[modesIdx + 1] : "flush,stream,copy").split(",");
if (modes.some((m) => !["flush", "stream", "copy"].includes(m))) {
  console.error("--modes は flush,stream,copy のどれかをカンマ区切りで");
  process.exit(2);
}
const onlyIdx = args.indexOf("--only");
const only = (onlyIdx >= 0 ? args[onlyIdx + 1] : "zip,sign,pdf").split(",");
if (only.some((e) => !["zip", "sign", "pdf"].includes(e))) {
  console.error("--only は zip,sign,pdf のどれかをカンマ区切りで");
  process.exit(2);
}
const timeoutIdx = args.indexOf("--timeout");
const timeoutSec = timeoutIdx >= 0 ? Number(args[timeoutIdx + 1]) : 900;
if (!(timeoutSec > 0)) {
  console.error("--timeout は正の秒数");
  process.exit(2);
}

const targets = [
  ["unknown", process.env.PROBE_UNKNOWN_URL],
  ["emscripten", process.env.PROBE_EMSCRIPTEN_URL],
].filter(([, u]) => u);
if (targets.length === 0) {
  console.error("env PROBE_UNKNOWN_URL か PROBE_EMSCRIPTEN_URL が要る");
  process.exit(2);
}

const headers = {};
const id = process.env.CF_ACCESS_CLIENT_ID;
const secret = process.env.CF_ACCESS_CLIENT_SECRET;
if (id && secret) {
  headers["CF-Access-Client-Id"] = id;
  headers["CF-Access-Client-Secret"] = secret;
}

function parseServerTiming(h) {
  const out = {};
  for (const part of (h ?? "").split(",")) {
    const m = part.trim().match(/^([\w-]+)(?:;.*?dur=([\d.]+))?/);
    if (m && m[2] !== undefined) out[m[1]] = Number(m[2]);
  }
  return out;
}

function p50(xs) {
  const s = xs.filter((x) => typeof x === "number").sort((a, b) => a - b);
  return s.length ? s[Math.ceil(s.length / 2) - 1] : undefined;
}

// 1 回叩いて { wall, status, ok, worker, mem, colo, extra } を返す。本文は出さない (ホスト名が載りうるため)
async function once(base, path) {
  const url = new URL(path, base);
  const t0 = performance.now();
  let res;
  let buf;
  try {
    // signal は本文を読み終えるまで効く
    res = await fetch(url, { headers, redirect: "manual", signal: AbortSignal.timeout(timeoutSec * 1000) });
    buf = Buffer.from(await res.arrayBuffer());
  } catch (e) {
    if (e.name === "TimeoutError") return { status: "timeout", ok: false };
    return { wall: performance.now() - t0, status: "fetch-error", ok: false };
  }
  const wall = performance.now() - t0;
  const out = { wall, status: res.status, ok: false };
  const type = res.headers.get("content-type") ?? "";
  if (type.startsWith("application/pdf")) {
    const st = parseServerTiming(res.headers.get("server-timing"));
    out.ok = res.status === 200 && buf.subarray(0, 5).toString() === "%PDF-";
    out.worker = st.total;
    out.mem = Number(res.headers.get("x-probe-memory-after")) || undefined;
    out.heap = Number(res.headers.get("x-probe-heap-peak")) || undefined;
    out.colo = res.headers.get("x-probe-colo") ?? undefined;
    out.extra = { pdf_bytes: buf.length };
    return out;
  }
  let body;
  try {
    body = JSON.parse(buf.toString());
  } catch {
    return out;
  }
  out.colo = body.colo ?? undefined;
  // /zip は処理の区間 (memory.process)、/sign は memory
  const mem = body.memory?.process ?? body.memory;
  out.mem = mem?.after;
  out.heap = mem?.heap_peak;
  if (body.unsupported) {
    out.unsupported = body.unsupported;
    return out;
  }
  if (path.startsWith("/zip")) {
    out.ok = res.status === 200 && body.match === true;
    out.worker = body.process?.total_ms;
    out.extra = { zip_bytes: body.input?.zip_bytes, gen_ms: body.input?.gen_ms };
    if (body.puts) {
      // mode=flush: PUT の件数・総バイト数・完了待ち、後始末で消した数
      out.extra.puts = body.puts.count;
      out.extra.put_bytes = body.puts.bytes;
      out.extra.put_wait_ms = body.process?.put_wait_ms;
      out.extra.deleted = body.cleanup?.deleted;
    }
  } else if (path === "/sign") {
    const methods = ["jwt_ring", "jwt_rsa", "aes_gcm_ring"];
    out.ok = res.status === 200 && methods.every((m) => body[m]?.ok === true || body[m]?.unsupported);
    out.worker = body.jwt_rsa?.sign_ms;
    out.extra = {
      keygen_ms: body.key?.keygen_ms,
      ...Object.fromEntries(
        methods.map((m) => [m, body[m]?.unsupported ? "unsupported" : body[m]?.ok ? "ok" : "ng"]),
      ),
    };
  }
  return out;
}

async function run(base, path) {
  const warm = await once(base, path);
  const xs = [];
  for (let i = 0; i < n; i++) xs.push(await once(base, path));
  const fails = {};
  for (const x of xs) if (!x.ok && !x.unsupported) fails[x.status] = (fails[x.status] ?? 0) + 1;
  return {
    path,
    wall_p50: p50(xs.map((x) => x.wall)),
    worker_p50: p50(xs.map((x) => x.worker)),
    mem_max: Math.max(0, ...xs.map((x) => x.mem ?? 0)) || undefined,
    heap_max: Math.max(0, ...xs.map((x) => x.heap ?? 0)) || undefined,
    ok: xs.filter((x) => x.ok).length,
    n,
    fails,
    unsupported: xs.find((x) => x.unsupported)?.unsupported,
    colos: [...new Set(xs.map((x) => x.colo).filter(Boolean))],
    warmup: { status: warm.status, ...(warm.extra ?? {}) },
    extra: xs.find((x) => x.ok)?.extra,
  };
}

// mb ごとに flush → stream → copy の順 (--modes の並びによらない)
const paths = [
  ...(only.includes("zip")
    ? mbs.flatMap((m) => ["flush", "stream", "copy"].filter((d) => modes.includes(d)).map((d) => `/zip?mb=${m}&mode=${d}`))
    : []),
  ...(only.includes("sign") ? ["/sign"] : []),
  ...(only.includes("pdf") ? ["/pdf"] : []),
];
const results = {};
for (const [name, base] of targets) {
  results[name] = {};
  for (const p of paths) {
    if (name !== "unknown" && p.endsWith("mode=flush")) continue;
    results[name][p] = await run(base, p);
  }
}

if (asJson) {
  console.log(JSON.stringify({ n, results }));
} else {
  const ms = (v) => (v === undefined ? "-" : `${Math.round(v)}`);
  const mb = (v) => (v === undefined ? "-" : `${(v / 1048576).toFixed(0)}MB`);
  const cell = (r) => {
    if (!r) return "-";
    if (r.unsupported) return "unsupported";
    if (r.fails.timeout === r.n) return `timeout (>${timeoutSec}s ×${r.n})`;
    const f = Object.entries(r.fails).map(([s, c]) => `${s}×${c}`).join(" ");
    return `${ms(r.wall_p50)} / ${ms(r.worker_p50)} / ${mb(r.mem_max)} / ${mb(r.heap_max)} / ${r.ok}/${r.n}${f ? ` (${f})` : ""}`;
  };
  const names = ["unknown", "emscripten"];
  console.log(`n=${n}。セルは wall p50 ms / worker p50 ms / 線形メモリ最大 / heap_peak 最大 / ok 数 (失敗は status×回数)`);
  console.log(`| path | ${names.join(" | ")} |`);
  console.log(`|---|${names.map(() => "---").join("|")}|`);
  for (const p of paths) console.log(`| ${p} | ${names.map((t) => cell(results[t]?.[p])).join(" | ")} |`);
  for (const t of names) {
    if (!results[t]) continue;
    const colos = [...new Set(Object.values(results[t]).flatMap((r) => r.colos))];
    const sign = results[t]["/sign"];
    console.log(`${t}: worker colo=${colos.join(",") || "?"}${sign ? ` / sign 暖機=${JSON.stringify(sign.warmup)}` : ""}`);
  }
  for (const p of paths.filter((p) => p.endsWith("mode=flush"))) {
    const r = results.unknown?.[p];
    if (r?.extra) console.log(`unknown ${p}: ${JSON.stringify(r.extra)}`);
  }
}

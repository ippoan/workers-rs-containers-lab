#!/usr/bin/env node
// worker の GET /query を叩き、Server-Timing の connect / rtt / db と全体の往復時間を p50 / p95 で出す。
// 応答の where (Worker / DO が動いている colo) も回数つきで出す。
// URL と Cloudflare Access の service token は env から受ける (repo に値を書かない):
//
//   LAB_URL=https://<staging の workers.dev>/query \
//   CF_ACCESS_CLIENT_ID=... CF_ACCESS_CLIENT_SECRET=... \
//   node bench/measure.mjs [-n 50] [--cold]
//
//   -n N     warm の回数 (既定 50)。先頭 1 回は暖機として捨てる
//   --cold   1 回だけ叩いて出す (Container が 10 分の alarm で止まった後の cold start 用)
//   --json   結果を JSON 1 行で出す
//
// LAB_URL は /query まで含めて渡す (パスが無ければ /query を足す)。
// ローカルの wrangler dev を叩くときは Access のヘッダーは要らない (env が無ければ付けない)。

const args = process.argv.slice(2);
const cold = args.includes("--cold");
const asJson = args.includes("--json");
const nIdx = args.indexOf("-n");
const n = cold ? 1 : nIdx >= 0 ? Number(args[nIdx + 1]) : 50;
if (!Number.isInteger(n) || n < 1) {
  console.error("-n は 1 以上の整数");
  process.exit(2);
}

const base = process.env.LAB_URL;
if (!base) {
  console.error("env LAB_URL (…/query) が要る");
  process.exit(2);
}
const url = new URL(base);
if (url.pathname === "/" || url.pathname === "") url.pathname = "/query";

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

async function once() {
  const t0 = performance.now();
  const res = await fetch(url, { headers, redirect: "manual" });
  const body = await res.text();
  const total = performance.now() - t0;
  if (res.status !== 200) {
    // Access を通らないと 302 (ログインへ) か 403。本文は出さない (ホスト名が載るため)
    throw new Error(`status ${res.status}`);
  }
  const json = JSON.parse(body);
  if (json.count === undefined) throw new Error("unexpected body");
  const st = parseServerTiming(res.headers.get("server-timing"));
  const where = json.where ? `worker=${json.where.worker ?? "?"} do=${json.where.do ?? "?"}` : undefined;
  return { total, connect: st.connect, rtt: st.rtt, db: st.db, where };
}

function pct(xs, p) {
  const s = xs.filter((x) => x !== undefined).sort((a, b) => a - b);
  if (s.length === 0) return undefined;
  return s[Math.min(s.length - 1, Math.ceil((p / 100) * s.length) - 1)];
}

const samples = [];
if (!cold) await once(); // 暖機 (Container 起動 / isolate の初回を外す)
for (let i = 0; i < n; i++) samples.push(await once());

const keys = ["total", "connect", "rtt", "db"];
const summary = { mode: cold ? "cold" : "warm", n };
for (const k of keys) {
  const xs = samples.map((s) => s[k]);
  summary[k] = { p50: pct(xs, 50), p95: pct(xs, 95) };
}
// where は値ごとの回数 (Worker は叩くたびに colo が変わりうる)
summary.where = {};
for (const s of samples) {
  if (s.where) summary.where[s.where] = (summary.where[s.where] ?? 0) + 1;
}

if (asJson) {
  console.log(JSON.stringify(summary));
} else {
  const fmt = (v) => (v === undefined ? "-" : `${v.toFixed(1)}ms`);
  console.log(`${summary.mode} n=${n}`);
  for (const k of keys) {
    console.log(`  ${k.padEnd(8)} p50=${fmt(summary[k].p50)} p95=${fmt(summary[k].p95)}`);
  }
  for (const [w, c] of Object.entries(summary.where)) console.log(`  where    ${w} (${c})`);
}

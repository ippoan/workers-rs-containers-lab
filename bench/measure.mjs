#!/usr/bin/env node
// worker の GET /query を叩き、Server-Timing の connect / rtt / db (/ rtt_do) と全体の往復時間を p50 / p95 で出す。
// 応答の where (Worker / DO が動いている colo) も回数つきで出し、Worker の colo ごとにも集計を分けて出す
// (Worker の拠点で往復が大きく変わるため。拠点が混ざったら warning を 1 行出す)。
// URL と Cloudflare Access の service token は env から受ける (repo に値を書かない):
//
//   LAB_URL=https://<staging の workers.dev>/query \
//   CF_ACCESS_CLIENT_ID=... CF_ACCESS_CLIENT_SECRET=... \
//   node bench/measure.mjs [-n 50] [--cold] [--rtt-do] [--same-colo NRT]
//
//   -n N              warm の回数 (既定 50)。先頭 1 回は暖機として捨てる
//   --cold            1 回だけ叩いて出す (Container が 10 分の alarm で止まった後の cold start 用)
//   --rtt-do          ?rtt_do=1 を付け、DO の中で測った DO ↔ Container の往復 (rtt_do) も出す。
//                     DO への往復が 1 回増えるので total はその分大きくなる
//   --same-colo COLO  Worker がその colo で動いた回だけで p50 / p95 を出す (全体の集計の代わり)
//   --json            結果を JSON 1 行で出す
//
// rtt は Worker → DO → Container の往復、rtt_do は DO ↔ Container だけの往復。
//
// LAB_URL は /query まで含めて渡す (パスが無ければ /query を足す)。
// ローカルの wrangler dev を叩くときは Access のヘッダーは要らない (env が無ければ付けない)。

const args = process.argv.slice(2);
const cold = args.includes("--cold");
const asJson = args.includes("--json");
const rttDo = args.includes("--rtt-do");
const nIdx = args.indexOf("-n");
const n = cold ? 1 : nIdx >= 0 ? Number(args[nIdx + 1]) : 50;
if (!Number.isInteger(n) || n < 1) {
  console.error("-n は 1 以上の整数");
  process.exit(2);
}
const coloIdx = args.indexOf("--same-colo");
const sameColo = coloIdx >= 0 ? args[coloIdx + 1]?.toUpperCase() : undefined;
if (coloIdx >= 0 && !sameColo) {
  console.error("--same-colo には colo (例: NRT) が要る");
  process.exit(2);
}

const base = process.env.LAB_URL;
if (!base) {
  console.error("env LAB_URL (…/query) が要る");
  process.exit(2);
}
const url = new URL(base);
if (url.pathname === "/" || url.pathname === "") url.pathname = "/query";
if (rttDo) url.searchParams.set("rtt_do", "1");

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
  const colo = json.where?.worker ?? "?";
  return { total, connect: st.connect, rtt: st.rtt, rtt_do: st.rtt_do, db: st.db, where, colo };
}

function pct(xs, p) {
  const s = xs.filter((x) => x !== undefined).sort((a, b) => a - b);
  if (s.length === 0) return undefined;
  return s[Math.min(s.length - 1, Math.ceil((p / 100) * s.length) - 1)];
}

const samples = [];
if (!cold) await once(); // 暖機 (Container 起動 / isolate の初回を外す)
for (let i = 0; i < n; i++) samples.push(await once());

const keys = ["total", "connect", "rtt", "rtt_do", "db"];
function stats(xs) {
  const out = { samples: xs.length };
  for (const k of keys) {
    const v = xs.map((s) => s[k]);
    out[k] = { p50: pct(v, 50), p95: pct(v, 95) };
  }
  return out;
}

// Worker の colo ごとに分ける (Worker は叩くたびに colo が変わりうる)
const byColo = {};
for (const s of samples) (byColo[s.colo] ??= []).push(s);
const colos = Object.keys(byColo);
const picked = sameColo ? byColo[sameColo] ?? [] : samples;

const summary = { mode: cold ? "cold" : "warm", n, ...(sameColo ? { same_colo: sameColo } : {}) };
Object.assign(summary, stats(picked));
summary.by_worker_colo = Object.fromEntries(colos.map((c) => [c, stats(byColo[c])]));
// where は値ごとの回数
summary.where = {};
for (const s of samples) {
  if (s.where) summary.where[s.where] = (summary.where[s.where] ?? 0) + 1;
}

if (colos.length > 1) {
  const counts = colos.map((c) => `${c}=${byColo[c].length}`).join(" ");
  console.error(`warning: Worker の colo が混ざった (${counts})。拠点をそろえるなら --same-colo <COLO>`);
}
if (sameColo && picked.length === 0) {
  console.error(`warning: Worker が ${sameColo} で動いた回が無い`);
}

if (asJson) {
  console.log(JSON.stringify(summary));
} else {
  const fmt = (v) => (v === undefined ? "-" : `${v.toFixed(1)}ms`);
  const print = (st, indent) => {
    for (const k of keys) {
      if (st[k].p50 === undefined) continue; // rtt_do は --rtt-do のときだけ
      console.log(`${indent}${k.padEnd(8)} p50=${fmt(st[k].p50)} p95=${fmt(st[k].p95)}`);
    }
  };
  console.log(`${summary.mode} n=${n}${sameColo ? ` (worker=${sameColo} の ${picked.length} 回だけ)` : ""}`);
  print(summary, "  ");
  if (colos.length > 1) {
    for (const c of colos) {
      console.log(`  [worker=${c}] n=${byColo[c].length}`);
      print(summary.by_worker_colo[c], "    ");
    }
  }
  for (const [w, c] of Object.entries(summary.where)) console.log(`  where    ${w} (${c})`);
}

# workers-rs-containers-lab

workers-rs (wasm32-unknown-unknown / wasm32-unknown-emscripten) と Cloudflare Containers (PgBouncer + postgres) の比較テスト台。

> rust-alc-api `workers/vein` の 15bf45e 時点のスナップショット。同期しない。

## 目的

Worker → Durable Object → Cloudflare Containers の PgBouncer (transaction mode) → postgres という経路を、
ドメイン非依存の最小構成で持つ。同じ Container を相手に、ビルドターゲットだけが違う worker を並べて

- bundle サイズ (raw / gzip)
- cold start (Container が止まった状態からの 1 回目)
- warm のクエリ往復 (connect / db / 全体) と CPU 時間

を比べる。

| worker | ターゲット | 状態 |
|---|---|---|
| `worker-unknown/` | `wasm32-unknown-unknown` (worker-build 0.8.7) | あり |
| `worker-emscripten/` | `wasm32-unknown-emscripten` | 後続 |

## 構成

```
container/          postgres 16 + PgBouncer (6432, transaction mode, default_pool_size=4, max_prepared_statements=0)
                    起動のたびに空の DB から role bench (非 superuser) と items (1000 行) を作る
worker-unknown/     独立 Cargo workspace (toolchain 1.92.0)
  src/lib.rs        GET /query だけ。それ以外は 404
  src/db.rs         DO LAB_DB へ Stub::connect → tokio-postgres の handshake (user bench / db postgres)
  src/lab_db.rs     DO LabDb: Container の getTcpPort(6432) へ TCP を中継。最後の接続から 10 分で Container を止める
scripts/            check-exposure.sh (wrangler.toml の公開範囲の検査) と陰性対照
bench/              bundle-size.sh / measure.mjs
```

- `GET /query` は 1 トランザクション (`BEGIN; SELECT count(*) FROM items; SELECT id, v FROM items ORDER BY id LIMIT 10; COMMIT`)
  を流して `{"count":1000,"items":[…]}` を返し、`Server-Timing: connect;dur=…, db;dur=…` を付ける。
- **`Row` はトランザクションの中で owned な値に変換してから COMMIT する。** `Row` は prepared statement を
  握っていて、COMMIT 後に drop すると Close がトランザクションの外に出て別のサーバー接続へ回り、
  `prepared statement "s1" already exists` (42P05) になる (transaction mode のプーラー越しの罠)。
- DB への経路は staging の Container だけ。DO → Container は Cloudflare 内の平文、PgBouncer は trust 認証。

## 公開範囲

- トップレベル (本番相当) は `workers_dev = false` / `preview_urls = false`、route を持たない。
- `workers_dev = true` は `env.staging` だけ。**その workers.dev のホスト名は Cloudflare Access で保護してから
  deploy する** (Access のアプリ・ポリシー・service token はこの repo に書かない)。
- `scripts/check-exposure.sh <wrangler.toml>...` が CI で毎回これを検査し、`scripts/check-exposure-test.sh` が
  陰性対照 (wrangler.toml を崩すと exit 1) を回す。
- CI にデプロイ job は無い。

## 計測

```bash
# bundle サイズ (worker-build --release の後)
bash bench/bundle-size.sh lab-unknown worker-unknown/build/index_bg.wasm

# warm: 暖機 1 回 + N 回の p50 / p95 (URL と Access の service token は env から)
LAB_URL=https://<staging のホスト>/query CF_ACCESS_CLIENT_ID=… CF_ACCESS_CLIENT_SECRET=… \
  node bench/measure.mjs -n 50

# cold: Container が 10 分の alarm で止まった後に 1 回だけ
LAB_URL=… CF_ACCESS_CLIENT_ID=… CF_ACCESS_CLIENT_SECRET=… node bench/measure.mjs --cold
```

Cloudflare 上の `Date.now` は I/O まで進まないので、Server-Timing は I/O 待ちの内訳として読む。CPU 時間は dashboard で見る。

## ローカル

```bash
# Container 単体
docker build -t lab-db container
docker run -d --name lab-db -p 127.0.0.1::6432 lab-db
psql "postgresql://bench@127.0.0.1:$(docker port lab-db 6432 | cut -d: -f2)/postgres" -Atc 'SELECT count(*) FROM items'

# worker
cd worker-unknown
cargo clippy --target wasm32-unknown-unknown -- -D warnings
worker-build --release   # 0.8.7
```

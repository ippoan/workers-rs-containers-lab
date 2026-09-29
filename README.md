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
| `worker-emscripten/` | `wasm32-unknown-emscripten` (worker-build 0.8.7 `--emscripten`、`experimental_tokio`) | あり (B-socket: A の DO へ `Stub::connect`) |

### 比較

staging (Access 越し) の実測。warm は p50、単位は ms。bundle は rtt_do と location hint を足した後の値。
時間の列は placement を入れる前の A の値で、placement を入れた後の A・B の値は計測後に埋める。

| worker | wasm raw (B) | wasm gzip (B) | warm total | connect | rtt | rtt_do | db | cold total |
|---|---|---|---|---|---|---|---|---|
| A `worker-unknown` | 618,048 | 246,833 | 144 | 22 | 14 | | 83 | 2,000 (connect 1,750) |
| B `worker-emscripten` | 518,882 | 228,323 | | | | | | |

- A の where は Worker=KIX / DO=NRT。B は A と同じ DO・Container を使う
- **Worker の拠点で往復が大きく変わる** (placement 前の実測: Worker=SIN の回は total p50 約 800 ms / rtt 80 ms、
  NRT/KIX の回は約 200 ms / rtt 11〜18 ms)。ターゲットを比べるときは拠点をそろえる
  (staging は Placement Hints で Worker を `aws:ap-northeast-1` の近くに寄せ、`measure.mjs --same-colo` で拠点ごとに集計する)
- B の bundle は wasm のほかに emscripten の JS glue (`build/index.js`、約 47 KB) を持つ

## 構成

```
container/          postgres 16 + PgBouncer (6432, transaction mode, default_pool_size=4, max_prepared_statements=0)
                    起動のたびに空の DB から role bench (非 superuser) と items (1000 行) を作る
worker-unknown/     独立 Cargo workspace (toolchain 1.92.0)
  src/lib.rs        GET /query だけ。それ以外は 404
  src/db.rs         DO LAB_DB へ Stub::connect → tokio-postgres の handshake (user bench / db postgres)
  src/lab_db.rs     DO LabDb: Container の getTcpPort(6432) へ TCP を中継。最後の接続から 10 分で Container を止める
                    HTTP の口は GET /where (DO の colo) と GET /rtt (DO ↔ Container の往復)
worker-emscripten/  独立 Cargo workspace (toolchain beta-2026-09-20、target wasm32-unknown-emscripten)
  src/main.rs       A の src/lib.rs の移植 (bin。空の fn main)
  src/db.rs         A の src/db.rs の移植。DO は持たず、wrangler.toml の script_name で A の LabDb を参照する
scripts/            check-exposure.sh (wrangler.toml の公開範囲の検査) と陰性対照
bench/              bundle-size.sh / measure.mjs
```

- `GET /query` は 1 トランザクション (`BEGIN; SELECT count(*) FROM items; SELECT id, v FROM items ORDER BY id LIMIT 10; COMMIT`)
  を流して `{"count":1000,"items":[…],"where":{"worker":"<colo>","do":"<colo>"}}` を返し、
  `Server-Timing: connect;dur=…, rtt;dur=…, db;dur=…` を付ける。
  - `rtt`: Worker が tx の外で `SELECT 1` を 1 回投げた往復 (**Worker → DO → Container** の 1 往復)。`db` はその後の tx だけ
  - `rtt_do`: `GET /query?rtt_do=1` のときだけ付く。DO の `GET /rtt` が DO の中で Container へ新しく繋ぎ、`SELECT 1` を
    1 回投げた往復 (**DO ↔ Container だけ**)。DO への往復が 1 回増えるので既定では測らない。
    Container が止まっていれば DO は起動せずに 503 を返し、`rtt_do` は付かない
  - `where`: Worker は `request.cf.colo`、DO は DO 内で `cdn-cgi/trace` を初回だけ引いた `colo` (取れなければ null)
  - Container は起動時に `cdn-cgi/trace` の `colo` / `loc` だけを 1 行ログに出す (Workers Logs で見る)
- **`Row` はトランザクションの中で owned な値に変換してから COMMIT する。** `Row` は prepared statement を
  握っていて、COMMIT 後に drop すると Close がトランザクションの外に出て別のサーバー接続へ回り、
  `prepared statement "s1" already exists` (42P05) になる (transaction mode のプーラー越しの罠)。
- DB への経路は staging の Container だけ。DO → Container は Cloudflare 内の平文、PgBouncer は trust 認証。
- 置き場所: staging の Worker は `[env.staging.placement] region = "aws:ap-northeast-1"` (Placement Hints。トップレベルには書かない)。
  DO は A・B とも同じ名前 + location hint `apac-ne` で取る (`get_by_name_with_location_hint`)。hint が効くのは
  DO が最初に作られるときだけで、既にある DO は動かない。

## 公開範囲

- トップレベル (本番相当) は `workers_dev = false` / `preview_urls = false`、route を持たない。
- `workers_dev = true` は `env.staging` だけ。**その workers.dev のホスト名は Cloudflare Access で保護してから
  deploy する** (Access のアプリ・ポリシー・service token はこの repo に書かない)。
- `scripts/check-exposure.sh <wrangler.toml>...` が CI で毎回これを検査し、`scripts/check-exposure-test.sh` が
  陰性対照 (wrangler.toml を崩すと exit 1) を回す。
- **staging へのデプロイは CI (`deploy-staging`) だけ**: main への push と `workflow_dispatch` のとき、
  `exposure` / `container` / `worker-unknown` がすべて通った後に `wrangler@4.143.0 deploy --env staging` を回す
  (org secret `CLOUDFLARE_API_TOKEN`。account_id は書かない)。PR では走らない。
  デプロイ直後に token 無しで `GET <staging>/query` を叩き、302 / 403 (Access が止めた) 以外なら job を落とす
  (ルートが行き渡るまでの 404 だけは 10 秒おきに最大 12 回待つ。200 などは即 fail)。
  staging の URL は `::add-mask::` で伏せ、ログに実ホスト名を出さない。
  B (`lab-emscripten-staging`) は A の DO を参照するので、`deploy-staging-emscripten` が `deploy-staging` の後に
  同じ手順 (伏せ方・Access の検査) で回る。
- 計測は CI に入れない (Access の service token は GitHub に置かない)。
- green の PR は CI の `auto-merge` job (ippoan/ci-workflows の reusable) で自動 merge される。

## 計測

```bash
# bundle サイズ (worker-build --release の後)
bash bench/bundle-size.sh lab-unknown worker-unknown/build/index_bg.wasm
bash bench/bundle-size.sh lab-emscripten worker-emscripten/build/index_bg.wasm

# warm: 暖機 1 回 + N 回の p50 / p95 (URL と Access の service token は env から)
LAB_URL=https://<staging のホスト>/query CF_ACCESS_CLIENT_ID=… CF_ACCESS_CLIENT_SECRET=… \
  node bench/measure.mjs -n 50

# Worker の拠点ごとの集計は常に出る (拠点が混ざると warning)。1 拠点だけで p50 / p95 を出すなら --same-colo、
# DO ↔ Container だけの往復も見るなら --rtt-do (total は DO への往復 1 回分大きくなる)
LAB_URL=… CF_ACCESS_CLIENT_ID=… CF_ACCESS_CLIENT_SECRET=… node bench/measure.mjs -n 50 --rtt-do --same-colo NRT

# cold: Container が 10 分の alarm で止まった後に 1 回だけ
LAB_URL=… CF_ACCESS_CLIENT_ID=… CF_ACCESS_CLIENT_SECRET=… node bench/measure.mjs --cold
```

Access の service token は、テスト用に新しく発行しない。既存の `CF_ACCESS_CLIENT_ID` / `CF_ACCESS_CLIENT_SECRET` を使う。

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

cd ../worker-emscripten
cargo clippy --target wasm32-unknown-emscripten -- -D warnings   # cfg は .cargo/config.toml
worker-build --emscripten --release   # 0.8.7。初回は emsdk 6.0.10 を ~/.cache/worker-build に入れる
```

- beta を日付で固定している理由 (`beta-2026-09-20`): 2026-09-27 の beta (1.100.0-beta.1) で cargo の中間生成物の置き場が変わり、
  worker-build 0.8.7 の `step_collect_emscripten_output` が `deps/snippets` を見つけられない (workers-rs main b57ba6e でも未修正)。
  浮動の `beta` だと esbuild が `Could not resolve "./snippets/worker-…/inline0.js"` で落ちる。CI も同じ toolchain を使う。
- ローカルで B から A の DO へつなぐには、両方を build してから、`[build]` を外した 2 つの wrangler.toml のコピーを
  並べて `wrangler dev -c … -c … --env staging` にする (`[build]` はリポジトリの root で走って落ちるため)。
  B の TCP は A の DO まで届くが、Container の起動はローカルでは A と同じく失敗する。

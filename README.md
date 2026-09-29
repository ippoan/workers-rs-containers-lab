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
| `worker-emscripten/` | `wasm32-unknown-emscripten` (worker-build 0.8.7 `--emscripten`、`experimental_tokio`) | あり (B-socket: lab-db の DO へ `Stub::connect`) |

DO `LabDb` と Container は、どちらの worker にも属さない `lab-db/` (staging: `lab-db-staging`) に置き、A・B とも
`script_name` で同じ DO・同じ Container を参照する (DO の版を変えるときは lab-db だけ先にデプロイする)。

### 比較

staging (Access 越し) の実測。warm は p50、単位は ms。bundle は DO を lab-db へ分けた後の値
(A も B も DO のコードを持たないので、同じ条件で比べられる。分ける前の A は DO 込みで raw 618,048 / gzip 246,833)。
時間の列は placement を入れる前の A の値で、placement を入れた後の A・B の値は計測後に埋める。

| worker | wasm raw (B) | wasm gzip (B) | warm total | connect | rtt | rtt_do | db | cold total |
|---|---|---|---|---|---|---|---|---|
| A `worker-unknown` | 563,965 | 228,164 | 144 | 22 | 14 | | 83 | 2,000 (connect 1,750) |
| B `worker-emscripten` | 518,882 | 228,323 | | | | | | |

- lab-db (DO + Container、wasm32-unknown-unknown) は raw 506,540 / gzip 206,158 (比較の対象外)
- A の where は Worker=KIX / DO=NRT。B は A と同じ DO・Container を使う
- **Worker の拠点で往復が大きく変わる** (placement 前の実測: Worker=SIN の回は total p50 約 800 ms / rtt 80 ms、
  NRT/KIX の回は約 200 ms / rtt 11〜18 ms)。ターゲットを比べるときは拠点をそろえる
  (staging は Placement Hints で Worker を `aws:ap-northeast-1` の近くに寄せ、`measure.mjs --same-colo` で拠点ごとに集計する)
- B の bundle は wasm のほかに emscripten の JS glue (`build/index.js`、約 47 KB) を持つ

## 構成

```
container/          postgres 16 + PgBouncer (6432, transaction mode, default_pool_size=4, max_prepared_statements=0)
                    起動のたびに空の DB から role bench (非 superuser) と items (1000 行) を作る
lab-db/             独立 Cargo workspace (toolchain 1.92.0)。DO と Container だけを持つスクリプト (外から叩く口は無い)
  src/lib.rs        fetch handler は 404 だけ
  src/lab_db.rs     DO LabDb: Container の getTcpPort(6432) へ TCP を中継。最後の接続から 10 分で Container を止める
                    HTTP の口は GET /where (DO の colo) と GET /rtt (DO ↔ Container の往復)
worker-unknown/     (A) 独立 Cargo workspace (toolchain 1.92.0)
  src/lib.rs        GET /query だけ。それ以外は 404
  src/db.rs         DO LAB_DB へ Stub::connect → tokio-postgres の handshake (user bench / db postgres)。
                    DO は持たず、wrangler.toml の script_name で lab-db の LabDb を参照する
worker-emscripten/  (B) 独立 Cargo workspace (toolchain beta-2026-09-20、target wasm32-unknown-emscripten)
  src/main.rs       A の src/lib.rs の移植 (bin。空の fn main)
  src/db.rs         A の src/db.rs の移植。A と同じく script_name で lab-db の LabDb を参照する
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
- DO の移し替え: LabDb はかつて A (`lab-unknown-staging`) にあった。中継だけで状態を持たないので、`transferred_classes`
  で移さず、`lab-db-staging` で新しく作り (migrations v1 `new_sqlite_classes`)、A 側の古い class は A の migrations v2
  (`deleted_classes`) で消す。`deleted_classes` は「ほかの Worker が旧 namespace を bind していない」ことが条件なので、
  切り替えの main の run で A が B の張り替えより先に走ると A が落ちうる (そのときは A の job を rerun する)。
  A にあった古い Container アプリが残ったら、dashboard などで消す。

## 公開範囲

- トップレベル (本番相当) は `workers_dev = false` / `preview_urls = false`、route を持たない。
- `workers_dev = true` は A・B の `env.staging` だけ。**その workers.dev のホスト名は Cloudflare Access で保護してから
  deploy する** (Access のアプリ・ポリシー・service token はこの repo に書かない)。lab-db は env.staging も含めて
  どこも `workers_dev = false` / `preview_urls = false` (外から叩く口が無い)。
- `scripts/check-exposure.sh <wrangler.toml>... --private lab-db/wrangler.toml` が CI で毎回これを検査し
  (`--private` を付けたものは env.staging の true も許さない)、`scripts/check-exposure-test.sh` が
  陰性対照 (wrangler.toml を崩すと exit 1) を回す。
- **staging へのデプロイは CI だけ**: main への push と `workflow_dispatch` のとき、`wrangler@4.143.0 deploy --env staging` を
  次の順で回す (org secret `CLOUDFLARE_API_TOKEN`。account_id は書かない)。PR では走らない。
  1. `deploy-staging-db` (lab-db-staging: DO + Container): `exposure` / `container` / `lab-db` が通った後。
     workers.dev の URL が出たら (= 公開されていたら) job を落とす
  2. `deploy-staging` (A) と `deploy-staging-emscripten` (B) を**並列**に: それぞれ `exposure` と自分の build job、
     `deploy-staging-db` が通った後。concurrency の group は 3 つとも別。
     デプロイ直後に token 無しで `GET <staging>/query` を叩き、302 / 403 (Access が止めた) 以外なら job を落とす
     (ルートが行き渡るまでの 404 だけは 10 秒おきに最大 12 回待つ。200 などは即 fail)。
  staging の URL と 32 桁の hex は `::add-mask::` で伏せ、ログに実ホスト名を出さない。
- 計測は CI に入れない (Access の service token は GitHub に置かない)。
- green の PR は CI の `auto-merge` job (ippoan/ci-workflows の reusable) で自動 merge される。

## 計測

```bash
# bundle サイズ (worker-build --release の後)
bash bench/bundle-size.sh lab-unknown worker-unknown/build/index_bg.wasm
bash bench/bundle-size.sh lab-emscripten worker-emscripten/build/index_bg.wasm
bash bench/bundle-size.sh lab-db lab-db/build/index_bg.wasm

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
cd lab-db
cargo clippy --target wasm32-unknown-unknown -- -D warnings
worker-build --release   # 0.8.7

cd ../worker-unknown
cargo clippy --target wasm32-unknown-unknown -- -D warnings
worker-build --release   # 0.8.7

cd ../worker-emscripten
cargo clippy --target wasm32-unknown-emscripten -- -D warnings   # cfg は .cargo/config.toml
worker-build --emscripten --release   # 0.8.7。初回は emsdk 6.0.10 を ~/.cache/worker-build に入れる
```

- beta を日付で固定している理由 (`beta-2026-09-20`): 2026-09-27 の beta (1.100.0-beta.1) で cargo の中間生成物の置き場が変わり、
  worker-build 0.8.7 の `step_collect_emscripten_output` が `deps/snippets` を見つけられない (workers-rs main b57ba6e でも未修正)。
  浮動の `beta` だと esbuild が `Could not resolve "./snippets/worker-…/inline0.js"` で落ちる。CI も同じ toolchain を使う。
- ローカルで A・B から lab-db の DO へつなぐには、build してから、`[build]` を外した wrangler.toml のコピーを
  並べて `wrangler dev -c … -c … --env staging` にする (`[build]` はリポジトリの root で走って落ちるため)。
  Worker の TCP は DO まで届くが、Container の起動はローカルでは失敗する (分ける前の A でも同じ)。

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

## probe (Worker に移しにくい処理)

DB とは別に、rust-alc-api (15bf45e) の「Worker に移しにくい処理」が各ターゲットで**ビルドできるか・動くか**を測る
(ippoan/rust-alc-api#694)。Container と DO は使わない。

| worker | ターゲット | staging |
|---|---|---|
| `probe-unknown/` | `wasm32-unknown-unknown` (toolchain 1.92.0) | `lab-probe-unknown-staging` |
| `probe-emscripten/` | `wasm32-unknown-emscripten` (toolchain `beta-2026-09-20`、`experimental_tokio`、[patch] は worker-emscripten と同じ) | `lab-probe-emscripten-staging` |

処理のコードは `probe-shared/` の 1 つだけで、両 crate が `#[path]` で読む (違うのは crate の入口と feature)。
依存の版は rust-alc-api の Cargo.lock に揃える (zip 2.4.2 / encoding_rs 0.8.35 / jsonwebtoken 9.3.1 / ring 0.17.14 /
rsa 0.9.10 / printpdf 0.8.2)。

- `GET /zip?mb=N` (N = 1..=20): `crates/alc-csv-parser/src/lib.rs` の `extract_zip` → `decode_shift_jis` →
  `group_csv_by_unko_no` の写し。入力の ZIP は worker の中で種を固定して作る (SHIFT_JIS の KUDGURI.csv = 運行 3000 件に
  1 行ずつ、KUDGIVT.csv = ZIP が N MB になるまでイベント行。圧縮率は約 4.6 倍)。生成 (`input.gen_ms`) と処理
  (`process.*_ms`) を分け、処理で数えた行数・運行NO の数が生成時と一致したかを `match` で返す。
  zip は `default-features = false, features = ["deflate"]` (rust-alc-api は default = bzip2 / zstd / xz の C ライブラリ込み)
- `GET /sign`: (a) `jwt_ring` = jsonwebtoken (ring) の RS256 (`from_rsa_pem` + `encode`、`crates/alc-notify/src/clients/lineworks.rs`)、
  (b) `jwt_rsa` = pure Rust の rsa crate の RS256、(c) `aes_gcm_ring` = ring の AES-256-GCM
  (`crates/alc-core/src/auth_lineworks.rs` の `encrypt_secret` / `decrypt_secret` の写し)。各 20 回の平均。
  RSA 鍵は repo に置かず、isolate の初回に rsa crate で 2048 bit を作ってメモリに持つ (`key.keygen_ms`)。
  (b) は (a) の JWT も検証する (`verifies_ring_token`)
- `GET /pdf`: printpdf で日本語 5 行の A4 1 ページ。フォントは `crates/alc-pdf` と同じ NotoSansJP-Regular.ttf (9.6 MB、
  SIL OFL 1.1、`probe-shared/fonts/OFL.txt`) を `include_bytes!`。時間は `Server-Timing: font / render / total`、
  大きさとメモリは `x-probe-pdf-bytes` / `x-probe-memory-before` / `x-probe-memory-after`
- どの応答にも `target` と、処理の前後の wasm の線形メモリ (`memory`、`core::arch::wasm32::memory_size`) を入れる。
  線形メモリは縮まないので、同じ isolate で先に大きい処理が走っていれば `before` から大きい (isolate の高水位として読む)
- **そのターゲットでビルドできない方式は feature (`ring` / `pdf`) で外し、`{"unsupported": "<理由>"}` を返す** (/pdf は 501)

### ビルドできたか

| 方式 | unknown | emscripten |
|---|---|---|
| ZIP (zip deflate + encoding_rs) | ○ | ○ |
| (a) jsonwebtoken (ring) RS256 | ○ (ring の C は clang で wasm32 へ。`wasm32_unknown_unknown_js`) | × |
| (b) rsa crate RS256 | ○ | ○ |
| (c) ring AES-256-GCM | ○ | × |
| PDF (printpdf 0.8.2 + NotoSansJP) | ○ | × |

- emscripten の ring: C / asm は emcc でコンパイルできるが、ring 0.17.14 の `SystemRandom` の `SecureRandom` 実装は
  target_os の許可リスト (`src/rand.rs`) に emscripten を含まない。jsonwebtoken の RS256 署名と `encrypt_secret` は
  どちらも `SystemRandom` を使うので E0277 (`the trait bound SystemRandom: SecureRandom is not satisfied`)
- emscripten の printpdf: printpdf 0.8.2 (最新の 0.12.8 も) は `[lib] crate-type = ["cdylib", "rlib"]` で、cargo は依存の
  cdylib もリンクする。worker-build の `-Crelocation-model=static` では wasm-ld が
  `relocation R_WASM_MEMORY_ADDR_SLEB cannot be used against symbol …; recompile with -fPIC` で落ちる
- どちらも `worker-build --emscripten --release -- --features ring` / `--features pdf` で再現できる

### bundle

worker-build 0.8.7 --release (wasm-opt 後)。gzip は `gzip -9`。

| worker | features | wasm raw (B) | wasm gzip (B) | JS glue raw / gzip (B) |
|---|---|---|---|---|
| probe-unknown | ring + pdf (既定) | 18,300,384 | 9,686,306 | 22,295 / 5,645 |
| probe-unknown | ring | 1,075,228 | 493,598 | |
| probe-unknown | なし | 690,568 | 343,964 | |
| probe-emscripten | なし (既定) | 660,137 | 350,330 | 46,121 / 16,419 |

- PDF (printpdf + フォント) だけで約 17.2 MB (raw) 増え、gzip 後でも上限 10MB の直下 (フォント以外の printpdf が
  約 7.6 MB。既定の `html` feature の azul / kuchiki / svg2pdf などを含む)
- ring は unknown で約 +385 KB (raw)

### ローカルの値 (wrangler@4.143.0 dev、参考値)

ローカルの workerd はメモリ 128 MB を強制せず、Date.now も CPU 実行中に進む (staging とは違う)。

| | unknown | emscripten |
|---|---|---|
| /zip?mb=1 process total (extract / decode / group) | 38 ms (19 / 12 / 7) | 40 ms (23 / 10 / 7) |
| /zip?mb=1 生成 | 401 ms | 395 ms |
| /zip 線形メモリ (after、mb = 1 / 5 / 10 / 20、MiB) | 43 / 152 / 290 / 567 | 37 / 147 / 309 / 608 |
| /sign keygen (2048 bit、1 回) | 260〜1,280 ms | 327〜457 ms |
| /sign jwt_ring sign / verify | 7.7 / 0.3 ms | unsupported |
| /sign jwt_rsa sign / verify | 5.6 / 0.7 ms | 5.6 / 0.7 ms |
| /sign aes_gcm_ring enc / dec | ≒0 / ≒0 ms | unsupported |
| /pdf total (font parse / render)、PDF | 36 ms (22 / 14)、18,329 B | unsupported |

- ZIP は 1〜20 MB のすべてで両ターゲットとも `match: true`
- **線形メモリは mb=5 (ZIP 5 MB、CSV 24 MB) で約 150 MB に届き、Worker の 128 MB を超える** (ZIP・展開後の SJIS・UTF-8・
  行ごとの String を同時に持つため)。staging の値は bench/probe.mjs で取る

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
probe-unknown/      独立 Cargo workspace (toolchain 1.92.0)。GET /zip・/sign・/pdf (下の probe-shared を読む)
probe-emscripten/   独立 Cargo workspace (toolchain beta-2026-09-20)。同じ口。ring と pdf は feature で外す
probe-shared/       probe の処理 (zip_probe.rs / sign.rs / pdf.rs / mem.rs / probe.rs) と fonts/ (NotoSansJP + OFL)
scripts/            check-exposure.sh (wrangler.toml の公開範囲の検査) と陰性対照
bench/              bundle-size.sh / measure.mjs / probe.mjs
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
  (`deleted_classes`) で消す。`deleted_classes` は「ほかの Worker が旧 namespace を bind していない」ことが条件。

## 公開範囲

- トップレベル (本番相当) は `workers_dev = false` / `preview_urls = false`、route を持たない。
- `workers_dev = true` は A・B と probe 2 つの `env.staging` だけ。**その workers.dev のホスト名は Cloudflare Access で保護してから
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
  3. `deploy-staging-probe-unknown` と `deploy-staging-probe-emscripten`: DB を使わないので 1・2 と独立に、それぞれ
     `exposure` と自分の build job が通った後に並列に。Access の検査は同じ形で `GET <staging>/sign` を叩く
  staging の URL と 32 桁の hex は `::add-mask::` で伏せ、ログに実ホスト名を出さない。
  DO の class を作り直す回は、同じクラス名への binding と `deleted_classes` を同時に適用できない (code 10061) ので、
  2 段 (binding を外して消す → binding を戻す) にする。
- worker-build は全 job で `cargo install worker-build@0.8.7 --locked` (rust-cache の `~/.cargo/bin` 頼み)。
  ビルド済みバイナリで入れる手は無い: workers-rs の GitHub Release に 0.8.7 のバイナリが無く、taiki-e/install-action の
  manifest にも無く、cargo-binstall の fallback 先 (cargo-quickinstall) にも linux x86_64 の 0.8.7 が無い (2026-09-29 時点)
- デプロイの job は、対応する build の job と同じ rust-cache (`shared-key` = workspace 名) と emsdk のキャッシュを
  復元するだけで保存しない (保存は main の build の job)。worker-build の `cargo install` と worker の build を省くため。
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

```bash
# probe: /zip?mb=1,5,10,20・/sign・/pdf を各 10 回 (暖機 1 回を除く) 叩き、両ターゲットの p50 を表で並べる
PROBE_UNKNOWN_URL=https://<probe-unknown の staging> PROBE_EMSCRIPTEN_URL=https://<probe-emscripten の staging> \
  CF_ACCESS_CLIENT_ID=… CF_ACCESS_CLIENT_SECRET=… node bench/probe.mjs -n 10
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
- probe は DB を使わないので、そのまま `npx wrangler@4.143.0 dev --env staging` で叩ける (`probe-unknown/` と
  `probe-emscripten/` の中で。`[build]` が worker-build を走らせる)。emscripten の glue は `node:module` を読むので、
  古い wrangler (4.58 で確認) では `Uncaught TypeError: Module not found: node:module` で起動しない。
  `PROBE_UNKNOWN_URL=http://localhost:<port> PROBE_EMSCRIPTEN_URL=http://localhost:<port> node bench/probe.mjs -n 3`

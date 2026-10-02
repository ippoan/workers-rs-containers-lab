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
| `probe-unknown/` | `wasm32-unknown-unknown` (toolchain 1.92.0。ビルドは `worker-build --release --panic-unwind` で浮動の `nightly`、下の「panic=unwind」) | `lab-probe-unknown-staging` |
| `probe-emscripten/` | `wasm32-unknown-emscripten` (toolchain `beta-2026-09-20`、`experimental_tokio`、[patch] は worker-emscripten と同じ) | `lab-probe-emscripten-staging` |

処理のコードは `probe-shared/` の 1 つだけで、両 crate が `#[path]` で読む (違うのは crate の入口と feature)。
依存の版は rust-alc-api の Cargo.lock に揃える (zip 2.4.2 / encoding_rs 0.8.35 / jsonwebtoken 9.3.1 / ring 0.17.14 /
rsa 0.9.10 / printpdf 0.8.2)。

- `GET /zip?mb=N&mode=copy|stream` (N = 1..=20、mode の既定は copy): `crates/alc-csv-parser/src/lib.rs` の `extract_zip` → `decode_shift_jis` →
  `group_csv_by_unko_no`。`copy` は 3 関数をそのまま写した版 (全エントリを Vec に展開 → 全部 decode → group。中間物を
  同時に持つ)、`stream` はエントリを 1 つずつ展開 → decode (展開したバイト列をすぐ捨てる) → group (decode した文字列を
  すぐ捨てる) する版。どちらも group の結果は全ファイルぶん持ったまま測る。入力の ZIP は worker の中で種を固定して作る (SHIFT_JIS の KUDGURI.csv = 運行 3000 件に
  1 行ずつ、KUDGIVT.csv = ZIP が N MB になるまでイベント行。圧縮率は約 4.6 倍)。生成 (`input.gen_ms`) と処理
  (`process.*_ms`) を分け、処理で数えた行数・運行NO の数が生成時と一致したかを `match` で返す。
  zip は `default-features = false, features = ["deflate"]` (rust-alc-api は default = bzip2 / zstd / xz の C ライブラリ込み)
- `GET /zip?mb=N&mode=flush` (**probe-unknown だけ**。`probe-unknown/src/flush.rs`): 運行ごとに R2 (`LAB_R2`) へ書き出して
  捨てる版 (`crates/alc-dtako/src/dtako_upload.rs` の `split_csv_from_r2` の作りを変えたもの)。ZIP 本体は Vec で持ち、
  エントリを 1 つずつ**行単位で** SHIFT_JIS → UTF-8 にし (エントリ全体の文字列を作らない)、運行NO が変わったらその運行の
  CSV (ヘッダー + 行) を `probe-flush/<時刻>-<乱数>/unko/<運行NO>/<CSV>` に PUT してバッファを手放す。同時に走らせる PUT は 8 まで。
  応答は copy / stream と同じ項目に加え、`puts` (件数・総バイト数・失敗・同時数の最大・1 件の最大) と
  `process.put_wait_ms` (PUT の完了を待った時間)、`files[].runs` (運行NO の連続した塊の数)。書き出したものは応答の前に
  prefix ごと消す (`cleanup`)。`match` は行数・運行NO の数・CSV のバイト数の一致に加え、PUT が全部成功し運行ごとに 1 件ずつだったか。
  - **行の並び**: rust-alc-api の fixture (`tests/common/mod.rs` の dtako ZIP) は KUDGURI・KUDGIVT とも運行NO 昇順で、
    運行ごとに行が連続している (本体の `group_csv_by_unko_no` は HashMap なので並びに依存せず、保証ではない。実データは未確認)。
    flush はこの並びを前提にし、入力は運行NO 順の生成器 (`zip_probe::generate_sorted`) で作る。連続していない運行が出たら
    `<CSV>.part<n>` として別に PUT し、`runs > groups` (= `match: false`) で分かるようにする (結合はしない)
  - 運行NO 順の KUDGIVT は混ざった順より縮むので、同じ N MB でも CSV は大きい (KUDGIVT の SJIS は 1 MB で 6.7 MB = copy / stream の入力の約 1.5 倍、20 MB で 169 MB)
  - **CPU の重さは Workers Logs / Observability のリクエストごとの CPU 時間で比べる**。Worker 内の時計は I/O まで進まないので
    `process.process_ms` は staging ではほぼ 0 になり (`put_wait_ms` は PUT の I/O を含むので進む)、手元の wall には入口の
    拠点のぶれが乗る。応答で見るのは heap_peak / 線形メモリ / 件数 (`match`・`runs`・`groups`・`puts.count`)
  - ローカル (`wrangler dev --env staging`、手元の R2) の実測: 1 MB は heap_peak 1.7 MiB、20 MB は 20.8 MiB
    (うち ZIP 本体 20.0 MiB)。どちらも `match: true`、PUT 6000 件
  - R2 の bucket `lab-probe-apac` は APAC を明示して事前に `wrangler r2 bucket create <名前> --location apac` で作る
    (deploy 時の自動作成 = 4.143.0 の resource provisioning は location を指定できず ENAM になった。無い名前のまま deploy
    すると ENAM で自動作成される)。R2 の呼び出しも subrequest に数える
    (Workers Paid は既定 10,000/リクエスト)。1 リクエストで PUT 6000 + list / delete 各十数回
- `GET /sign`: (a) `jwt_ring` = jsonwebtoken (ring) の RS256 (`from_rsa_pem` + `encode`、`crates/alc-notify/src/clients/lineworks.rs`)、
  (b) `jwt_rsa` = pure Rust の rsa crate の RS256、(c) `aes_gcm_ring` = ring の AES-256-GCM
  (`crates/alc-core/src/auth_lineworks.rs` の `encrypt_secret` / `decrypt_secret` の写し)。各 20 回の平均。
  RSA 鍵は repo に置かず、isolate の初回に rsa crate で 2048 bit を作ってメモリに持つ (`key.keygen_ms`)。
  (b) は (a) の JWT も検証する (`verifies_ring_token`)
- `GET /pdf`: printpdf で日本語 5 行の A4 1 ページ。フォントは `crates/alc-pdf` と同じ NotoSansJP-Regular.ttf (9.6 MB、
  SIL OFL 1.1、`probe-shared/fonts/OFL.txt`) を `include_bytes!`。時間は `Server-Timing: font / render / total`、
  大きさとメモリは `x-probe-pdf-bytes` / `x-probe-memory-before` / `x-probe-memory-after`
- `GET /_lab/panic`: わざと panic する (panic やメモリ不足で中断した後、次のリクエストが回復するかを測るため)。
  メモリ不足の口は `/zip?mb=20&mode=copy` (heap_peak 542 MiB) を使う。staging は Access 越しにしか届かない。
  ローカル (wrangler@4.143.0 dev) で `/sign` → `/_lab/panic` → `/sign` ×5 を流した結果 (下の「panic=unwind」):
  - abort (`--panic-unwind` なし): panic は 500 (本文は `The Workers runtime canceled this request because it detected that your Worker's code had hung …`)。
    dev のログに `Critical RuntimeError: unreachable` と `Reinitializing Wasm application` が出て、直後の `/sign` は `key.cached: false`
    (isolate の状態が捨てられ、鍵を作り直した)。以降の 4 回は true
  - unwind: panic は 500 (本文は `PanicError: lab: intentional panic`)。`Reinitializing` は出ず、直後の `/sign` ×5 はすべて 200 で
    `key.cached: true` (panic の前に作った鍵が残る)。panic と同時に投げた `/sign` ×5 もすべて 200・`cached: true`
  - メモリ不足はローカルでは試せない (ローカルの workerd は 128 MB を強制せず、`/zip?mb=20&mode=copy` は 200・線形メモリ 566 MiB)。
    unwind でも OOM は捕まらない見込みで、staging で確かめる
- どの応答にも `target`・`colo` と、処理の区間のメモリ (`memory`。/zip は生成 `gen` と処理 `process` に分ける) を入れる:
  - `before` / `after` / `grown`: 線形メモリ (`core::arch::wasm32::memory_size`) の前・後・後 − 前。Worker の 128 MB に
    効く実物だが**伸びたら縮まない**ので、同じ isolate で先に大きい処理が走っていれば `grown` は 0 (isolate の高水位)
  - `heap_before` / `heap_peak`: グローバルアロケータ (`probe-shared/mem.rs`) で数えた、区間の前に生きていた確保と、
    区間中に同時に生きていた確保の最大。isolate の前の処理に左右されないので、**copy と stream の比較はこちらで見る**
    (断片化や線形メモリの伸ばし方の分は入らないので、線形メモリより小さい)
  - bench/probe.mjs は mb ごとに flush (unknown だけ) → stream → copy の順に叩く (線形メモリの列が後の版に隠れないように)
- **そのターゲットでビルドできない方式は feature (`ring` / `pdf`) で外し、`{"unsupported": "<理由>"}` を返す** (/pdf は 501)

### ビルドできたか

| 方式 | unknown | emscripten |
|---|---|---|
| ZIP (zip deflate + encoding_rs) | ○ | ○ |
| (a) jsonwebtoken (ring) RS256 | ○ (ring の C は clang で wasm32 へ。`wasm32_unknown_unknown_js`) | ビルドできない (ring の `SystemRandom` が emscripten 未対応、E0277) |
| (b) rsa crate RS256 | ○ | ○ |
| (c) ring AES-256-GCM | ○ | ビルドできない (同上。`encrypt_secret` の nonce が `SystemRandom`) |
| PDF (printpdf 0.8.2 + NotoSansJP) | ○ | ビルドできない (printpdf の `crate-type = cdylib` を wasm-ld がリンクできない) |

- emscripten の ring: C / asm は emcc でコンパイルできるが、ring 0.17.14 の `SystemRandom` の `SecureRandom` 実装は
  target_os の許可リスト (`src/rand.rs`) に emscripten を含まない。jsonwebtoken の RS256 署名と `encrypt_secret` は
  どちらも `SystemRandom` を使うので E0277 (`the trait bound SystemRandom: SecureRandom is not satisfied`)
- emscripten の printpdf: printpdf 0.8.2 (最新の 0.12.8 も) は `[lib] crate-type = ["cdylib", "rlib"]` で、cargo は依存の
  cdylib もリンクする。worker-build の `-Crelocation-model=static` では wasm-ld が
  `relocation R_WASM_MEMORY_ADDR_SLEB cannot be used against symbol …; recompile with -fPIC` で落ちる
- どちらも `worker-build --emscripten --release -- --features ring` / `--features pdf` で再現できる

#### panic=unwind (probe-unknown)

probe-unknown は `worker-build --release --panic-unwind` (worker-build 0.8.7) でビルドする (ippoan/rust-alc-api#694)。
worker-build は `cargo +nightly build -Z build-std=std,panic_unwind` と `RUSTFLAGS=-Cpanic=unwind` を打ち、shim は `shim-unwind.js` を使う
(ログに `Compiling to Wasm (with panic=unwind)...`)。`--emscripten` とは併用できないので probe-emscripten は abort のまま。
clippy / fmt は rust-toolchain.toml の 1.92.0 (stable) のままで、unwind のビルドだけ nightly を使う。

- **nightly は日付で固定できない** (浮動の `nightly` を使う): worker-build 0.8.7 は toolchain 名を `nightly` と決め打ちし
  (`src/build/target.rs` の `NIGHTLY_TOOLCHAIN`、`cargo +nightly` / `rustc +nightly` / `rustup … --toolchain nightly`)、日付を渡す口が無い。
  `cargo +<toolchain>` は rustup の最優先の上書きなので `RUSTUP_TOOLCHAIN` も rust-toolchain.toml も効かず、
  日付版を別名にする `rustup toolchain link nightly <日付版の sysroot>` は `invalid custom toolchain name 'nightly'` で拒否される。
  CI はビルドした `rustc +nightly -V` をログと step summary に残す (手元で確かめた版: `rustc 1.101.0-nightly (c1070d693 2026-09-28)`)
- nightly が無ければ worker-build が `rustup toolchain install nightly` と rust-src・wasm32-unknown-unknown の追加を自分で打つ。
  CI は build / deploy の job で先に入れる (`rustup toolchain install nightly --profile minimal --component rust-src --target wasm32-unknown-unknown`)
- CI の rust-cache は abort 版と target を取り合わないよう `shared-key: probe-unknown-unwind` にした
- 手元の CPU (wrangler dev、10 回の中央値、abort → unwind): jwt_ring sign 7.8 → 7.8 ms、jwt_rsa sign 5.65 → 5.9 ms、
  /pdf render 5 → 4 ms / total 21.5 → 22 ms。ローカルでは差が見えない (staging の CPU 時間は Workers Logs で比べる)

### bundle

worker-build 0.8.7 --release (wasm-opt 後)。gzip は `gzip -9`。

| worker | features | wasm raw (B) | wasm gzip (B) | JS glue raw / gzip (B) |
|---|---|---|---|---|
| probe-unknown | ring + pdf (既定)、`--panic-unwind` | 19,539,304 | 9,845,172 | 24,383 / 6,187 |
| probe-unknown | ring + pdf、abort (`--panic-unwind` なし) | 18,430,391 | 9,715,740 | 22,449 / 5,666 |
| probe-unknown | ring | 1,097,670 | 496,786 | |
| probe-unknown | なし | 705,086 | 346,697 | |
| probe-emscripten | なし (既定) | 668,347 | 352,317 | 46,246 / 16,440 |

- 上限はスクリプトの圧縮前 64 MiB (圧縮後の上限は無い。ほかに起動 1 秒・メモリ 128 MB)。bench/bundle-size.sh は圧縮前で判定する
- PDF (printpdf + フォント) だけで約 17.3 MB (raw) 増える (上限は圧縮前 64 MiB なので収まる。フォント以外の printpdf が
  約 7.7 MB。既定の `html` feature の azul / kuchiki / svg2pdf などを含む)
- ring は unknown で約 +393 KB (raw)
- `--panic-unwind` は約 +1.1 MB (raw、+6.0%) / +129 KB (gzip、+1.3%)。unwind の表 (landing pad) と `-Z build-std` で作り直した std の分と見られる。
  同じ手元で作った abort 版 (raw 18,495,913 / gzip 9,744,974) と比べると +1,043,391 (+5.6%) / +100,198 (+1.0%)

### ローカルの値 (wrangler@4.143.0 dev、参考値)

ローカルの workerd はメモリ 128 MB を強制せず、Date.now も CPU 実行中に進む (staging とは違う)。

| | unknown | emscripten |
|---|---|---|
| /zip?mb=1 process total (extract / decode / group) | 38 ms (19 / 12 / 7) | 40 ms (23 / 10 / 7) |
| /zip?mb=1 生成 | 401 ms | 395 ms |
| /zip process total (mb = 5 / 10 / 20、copy と stream でほぼ同じ) | 114 / 226 / 450 ms | 129 / 257 / 511 ms |
| /zip heap_peak copy (mb = 1 / 5 / 10 / 20、MiB) | 30 / 137 / 272 / 542 | 30 / 137 / 272 / 542 |
| /zip heap_peak stream (mb = 1 / 5 / 10 / 20、MiB) | 20 / 103 / 206 / 412 | 20 / 103 / 206 / 412 |
| /zip 線形メモリ (after の最大、mb = 1 / 5 / 10 / 20、MiB) | 47 / 187 / 325 / 602 | 36 / 142 / 283 / 563 |
| /sign keygen (2048 bit、1 回) | 260〜1,280 ms | 327〜457 ms |
| /sign jwt_ring sign / verify | 7.7 / 0.3 ms | unsupported |
| /sign jwt_rsa sign / verify | 5.6 / 0.7 ms | 5.6 / 0.7 ms |
| /sign aes_gcm_ring enc / dec | ≒0 / ≒0 ms | unsupported |
| /pdf total (font parse / render)、PDF | 36 ms (22 / 14)、18,329 B、heap_peak 22 MiB | unsupported |

- ZIP は 1〜20 MB のすべてで両ターゲット・両 mode とも `match: true` (行数・運行NO の数が一致)
- heap_peak はターゲットによらず同じ (同じコード・同じ確保)。stream は copy より約 25% 小さいだけで、
  **stream でも mb=5 (ZIP 5 MB、SJIS の CSV 24 MB) で約 100 MiB、線形メモリは copy で 140〜190 MiB に届き、Worker の
  128 MB を超える見込み**。残るのは group の結果 (行ごとの `String` + `Vec`、UTF-8 の約 4 倍) と入力の ZIP で、
  中間物を捨てるだけでは足りない (行を持たずに DB へ流す形が要る)
- staging の値 (128 MB を超えたときの 5xx を含む) は bench/probe.mjs で取る

## Hyperdrive の probe (`probe-hyperdrive/`)

Workers の TCP の代わりに **Cloudflare Hyperdrive 経由**で DB に繋ぐ形を、workers-rs 0.8.7 + tokio-postgres の組で確かめる
(Refs ippoan/rust-alc-api#725 / ippoan/rust-alc-api#723)。`wrangler dev` は Hyperdrive を通らないので、配信した worker で測る。

- **外から届く口が無い**: HTTP のハンドラを持たず、定時実行 (5 分ごと) だけ。`workers_dev` / `preview_urls` はどの env も false、
  route 無し (`check-exposure.sh --private`)。DB の宛先は Hyperdrive の設定が決め、repo に在るのは設定の ID だけ
- 読むのは `current_user`・`pg_roles` の自分の行・`current_setting` だけ (業務の表は読まない・書かない)
- 1 回の実行で 1 行の JSON をログに出す。出すのはロール名・真偽・回数・SQLSTATE・ms・固定の label だけ
  (エラーの文は出さない。label は `hyperdrive_binding` / `config_parse` / `socket` / `handshake` / `connection_task` / `query`)
- 配信は手動 (CI は build まで): `cd probe-hyperdrive && wrangler deploy --env staging`。
  ログは `wrangler tail --env staging --format json` で読む (`logs[].message` が上の 1 行)。
  測り終えたら `wrangler delete --env staging` で片付ける
- 期限: `src/lib.rs` の `EXPIRES_AT_MS` (2026-10-05T00:00:00Z) を過ぎた実行は、DB に繋がず `{"probe":"hyperdrive","expired":true}` だけ出す

確かめる問いと JSON の項目:

| 問い | 中身 | JSON の項目 |
|---|---|---|
| Q2 | 公式例の形 (`env.hyperdrive` → `Socket` の StartTls → `connect_raw(socket, PassthroughTls)`) で繋がるか | `connect` (1 本目。失敗は label と SQLSTATE) |
| Q3 | 接続のロール | `role` |
| Q4 | トランザクション単位の設定 (`set_config(…, true)`) が、中では効き・COMMIT で消え・次の接続に漏れないか。接続を 10 回張り直す | `tx_setting` |
| Q5 | 名前付き prepared statement がトランザクションの中で使えるか。対照は `Row` を COMMIT の後まで持つ形 (5 回、最後に流す) | `prepared` / `prepared_row_dropped_after_commit` |
| Q6 | 接続・1 トランザクションの中央値と、1 トランザクションの中の `SELECT 1` 20 回の 1 文あたりの平均 (`Date.now` の差。参考値) | `timing_ms` |

対照が握ったままにした prepared statement の影響は次の実行以降に出うるので、`prepared` は配信後 1 回目の実行の値も見る。

結果 (staging。実測の後に埋める):

| 問い | 結果 |
|---|---|
| Q2 接続 | |
| Q4 トランザクション単位の設定 | |
| Q5 prepared statement | |
| Q6 所要時間 (ms) | |

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
probe-unknown/      独立 Cargo workspace (toolchain 1.92.0)。GET /zip・/sign・/pdf (下の probe-shared を読む)。/zip の mode=flush (R2) はここだけ
probe-emscripten/   独立 Cargo workspace (toolchain beta-2026-09-20)。同じ口。ring と pdf は feature で外す
probe-hyperdrive/   独立 Cargo workspace (toolchain 1.92.0)。Hyperdrive 経由の DB 接続を測る。HTTP の口は無く、定時実行だけ (配信は手動)
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
# probe: /zip?mb=1,5,10,20 (flush / stream / copy)・/sign・/pdf を各 10 回 (暖機 1 回を除く) 叩き、両ターゲットの p50 を表で並べる
PROBE_UNKNOWN_URL=https://<probe-unknown の staging> PROBE_EMSCRIPTEN_URL=https://<probe-emscripten の staging> \
  CF_ACCESS_CLIENT_ID=… CF_ACCESS_CLIENT_SECRET=… node bench/probe.mjs -n 10

# 絞り込み: --only zip で /sign・/pdf を飛ばし、--modes で /zip の mode を選ぶ。--timeout <秒> (既定 900) を超えた回は表に timeout と出る
PROBE_UNKNOWN_URL=… CF_ACCESS_CLIENT_ID=… CF_ACCESS_CLIENT_SECRET=… node bench/probe.mjs -n 1 --mb 1 --only zip --modes flush --timeout 900
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
- probe-unknown の `--panic-unwind` は浮動の `nightly` を使う (worker-build 0.8.7 が toolchain 名 `nightly` を決め打ちし、
  rustup が `nightly` という名前の link を拒むため日付で固定できない。詳しくは「panic=unwind」)。nightly の変更でビルドが落ちたら
  CI の step summary に残した `rustc +nightly -V` と見比べる。
- ローカルで A・B から lab-db の DO へつなぐには、build してから、`[build]` を外した wrangler.toml のコピーを
  並べて `wrangler dev -c … -c … --env staging` にする (`[build]` はリポジトリの root で走って落ちるため)。
  Worker の TCP は DO まで届くが、Container の起動はローカルでは失敗する (分ける前の A でも同じ)。
- probe は DB を使わないので、そのまま `npx wrangler@4.143.0 dev --env staging` で叩ける (`probe-unknown/` と
  `probe-emscripten/` の中で。`[build]` が worker-build を走らせる)。emscripten の glue は `node:module` を読むので、
  古い wrangler (4.58 で確認) では `Uncaught TypeError: Module not found: node:module` で起動しない。
  `PROBE_UNKNOWN_URL=http://localhost:<port> PROBE_EMSCRIPTEN_URL=http://localhost:<port> node bench/probe.mjs -n 3`

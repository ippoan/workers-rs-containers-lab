//! Hyperdrive 経由の DB 接続を測る probe。**HTTP のハンドラを持たず、定時実行だけ**で動き、
//! 1 回の実行につき 1 行の JSON をログに出す (項目は README の「Hyperdrive の probe」)。
//!
//! - 読むのは `current_user`・`pg_roles` の自分の行・`current_setting` だけ。業務の表は読まない・書かない。
//! - **ログに出すのはロール名・真偽・回数・ms と、固定の label・SQLSTATE・`io::ErrorKind` の名前だけ。**
//!   接続文字列・宛先・認証情報と、エラーの文 (`Display` / `Debug` / DB の message) は出さない ([`kind`])。
//! - [`EXPIRES_AT_MS`] を過ぎたら DB に繋がない。

#![deny(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::error::Error as _;
use std::rc::Rc;

use serde::Serialize;
use tokio_postgres::types::Type;
use tokio_postgres::{Client, Config, Error as PgError, SimpleQueryMessage};
use worker::postgres_tls::PassthroughTls;
use worker::{
    console_log, event, Date, Env, ScheduleContext, ScheduledEvent, SecureTransport, Socket,
};

const HD_BINDING: &str = "HD";
/// 2026-10-05T00:00:00Z。これを過ぎた実行は DB に繋がずに終わる
const EXPIRES_AT_MS: u64 = 1_791_158_400_000;
const RUNTIME_ROLE: &str = "alc_api_rt";
/// 系列ごとに接続を張り直す回数 (テナント A / B を交互に)。接続は合計 2 + 3 × 5 + 3 = 20 本
const ITERATIONS: u32 = 5;
/// 対照系列 (Row を COMMIT の後まで持つ) の回数
const CONTROL_ITERATIONS: u32 = 3;
const ROUNDTRIPS: u32 = 20;
const SET_CONFIG: &str = "SELECT set_config('app.current_tenant_id', $1, true), set_config('search_path', 'alc_api', true)";
const READ_TENANT: &str = "SELECT current_setting('app.current_tenant_id', true)";
const READ_SETTINGS: &str =
    "SELECT current_setting('app.current_tenant_id', true), current_setting('search_path')";
const ECHO: &str = "SELECT $1::text AS v";

/// tokio-postgres のエラーの種類。`Display` の先頭 (種類ごとに固定の文) → 固定の label
const PG_KINDS: &[(&str, &str)] = &[
    ("error communicating with the server", "io"),
    ("unexpected message from server", "unexpected_message"),
    ("error performing TLS handshake", "tls"),
    ("error serializing parameter", "to_sql"),
    ("error deserializing column", "from_sql"),
    ("connection closed", "closed"),
    ("error parsing response from server", "parse"),
    ("error encoding message to server", "encode"),
    ("authentication error", "authentication"),
    ("invalid configuration", "config"),
    ("error connecting to server", "connect"),
    ("timeout waiting for server", "timeout"),
];

/// 接続までの失敗の label (エラーの文は出さない)
#[derive(Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
enum Label {
    HyperdriveBinding,
    ConfigParse,
    Socket,
    Handshake,
    Query,
}

/// トランザクションの流し方
#[derive(Clone, Copy)]
enum Mode {
    /// `tx.execute` / `tx.query` (名前付き prepared statement)
    Named,
    /// `tx.query_typed` (prepare の往復をしない、名前なしの statement)
    Typed,
    /// BEGIN から COMMIT まで `simple_query` / `batch_execute`
    Simple,
    /// 対照: Named と同じ問い合わせで、`Row` を COMMIT の後まで持ってから drop する
    RowDroppedAfterCommit,
}

/// (失敗した step, エラーの種類)
type Failure = (&'static str, String);
/// connection task が Err で終わった回数 (種類ごと)
type TaskErrors = Rc<RefCell<BTreeMap<String, u32>>>;

/// 1 本目の接続の結果
#[derive(Serialize)]
struct Connect {
    ok: bool,
    ms: u64,
    error: Option<Label>,
    kind: Option<String>,
}

#[derive(Serialize)]
struct Role {
    current_user: String,
    is_runtime_role: bool,
    rolsuper: Option<bool>,
    rolbypassrls: Option<bool>,
}

/// トランザクションの外の単発 (`"ok"` かエラーの種類)
#[derive(Default, Serialize)]
struct Single {
    named: Option<String>,
    typed: Option<String>,
}

/// 1 系列 (接続を張り直しながら同じトランザクションを流す) の集計
#[derive(Default, Serialize)]
struct Series {
    iterations: u32,
    ok: u32,
    held_in_tx: u32,
    search_path_held: u32,
    empty_after_commit: u32,
    leak_before_set: u32,
    tx_p50: u64,
    failed_step: BTreeMap<&'static str, u32>,
    errors_by_kind: BTreeMap<String, u32>,
}

impl Series {
    /// 失敗を step と種類ごとに数える (呼び出し側がそのまま return できるよう None を返す)
    fn fail(&mut self, (step, kind): Failure) -> Option<u64> {
        *self.failed_step.entry(step).or_insert(0) += 1;
        *self.errors_by_kind.entry(kind).or_insert(0) += 1;
        None
    }
}

#[derive(Default, Serialize)]
struct Timing {
    connect_p50: u64,
    stmt_roundtrip_avg: f64,
}

#[derive(Default, Serialize)]
struct Report {
    probe: &'static str,
    connect: Option<Connect>,
    role: Option<Role>,
    single: Single,
    tx_simple: Series,
    tx_typed: Series,
    tx_named: Series,
    prepared_row_dropped_after_commit: Series,
    connection_task: BTreeMap<String, u32>,
    timing_ms: Timing,
    errors: Vec<Label>,
}

/// 1 トランザクションの中で見えたもの (owned な値だけ)
struct TxSeen {
    tenant_held: bool,
    search_path_held: bool,
    echoed: bool,
}

fn now() -> u64 {
    Date::now().as_millis()
}

fn median(mut ms: Vec<u64>) -> u64 {
    ms.sort_unstable();
    ms.get(ms.len() / 2).copied().unwrap_or(0)
}

/// エラーを、識別子を含まない固定の語に落とす: DB のエラーは SQLSTATE、それ以外は tokio-postgres の
/// 種類の label ([`PG_KINDS`]。当たらなければ `closed` / `other`)。原因が `io::Error` なら `:<ErrorKind の名前>` を足す。
/// **`Display` の文は種類の判定に使うだけで、出さない。**
fn kind(e: &PgError) -> String {
    if let Some(db) = e.as_db_error() {
        return db.code().code().to_owned();
    }
    let text = e.to_string();
    let fallback = if e.is_closed() { "closed" } else { "other" };
    let base = PG_KINDS
        .iter()
        .find(|(prefix, _)| text.starts_with(prefix))
        .map_or(fallback, |(_, label)| label);
    match e.source().and_then(|s| s.downcast_ref::<std::io::Error>()) {
        Some(io) => format!("{base}:{:?}", io.kind()),
        None => base.to_owned(),
    }
}

fn outcome<T>(result: Result<T, PgError>) -> String {
    result.map_or_else(|e| kind(&e), |_| "ok".to_owned())
}

/// 失敗に step の名前を付ける
fn at(step: &'static str) -> impl Fn(PgError) -> Failure {
    move |e| (step, kind(&e))
}

/// 乱数の UUID (v4)。実在のテナントに当たらない値を `app.current_tenant_id` に入れるため。ログには出さない
fn random_uuid() -> String {
    let mut bytes = [0u8; 16];
    // 乱数が取れなければ 0 のまま (それでも v4 の形にはなる)
    let _ = getrandom::getrandom(&mut bytes);
    let n = (u128::from_be_bytes(bytes) & !(0xf << 76) & !(0x3 << 62)) | (0x4 << 76) | (0x2 << 62);
    format!(
        "{:08x}-{:04x}-{:04x}-{:04x}-{:012x}",
        n >> 96,
        (n >> 80) & 0xffff,
        (n >> 64) & 0xffff,
        (n >> 48) & 0xffff,
        n & 0xffff_ffff_ffff
    )
}

/// `simple_query` (prepared statement を作らない経路) の最初の行を owned な列にする
fn first_row(messages: &[SimpleQueryMessage]) -> Vec<Option<String>> {
    messages
        .iter()
        .find_map(|m| match m {
            SimpleQueryMessage::Row(row) => Some(
                (0..row.len())
                    .map(|i| row.try_get(i).ok().flatten().map(str::to_owned))
                    .collect(),
            ),
            _ => None,
        })
        .unwrap_or_default()
}

fn col(cols: &[Option<String>], i: usize) -> Option<&str> {
    cols.get(i).and_then(|c| c.as_deref())
}

/// Q2: workers-rs の公式例 (examples/tokio-postgres) と同じ形で Hyperdrive へ繋ぐ
async fn connect(env: &Env, task_errors: &TaskErrors) -> Result<Client, (Label, Option<String>)> {
    let hd = env
        .hyperdrive(HD_BINDING)
        .map_err(|_| (Label::HyperdriveBinding, None))?;
    let config = hd
        .connection_string()
        .parse::<Config>()
        .map_err(|_| (Label::ConfigParse, None))?;
    let socket = Socket::builder()
        .secure_transport(SecureTransport::StartTls)
        .connect(hd.host(), hd.port())
        .map_err(|_| (Label::Socket, None))?;
    let (client, connection) = config
        .connect_raw(socket, PassthroughTls)
        .await
        .map_err(|e| (Label::Handshake, Some(kind(&e))))?;
    let task_errors = Rc::clone(task_errors);
    wasm_bindgen_futures::spawn_local(async move {
        if let Err(e) = connection.await {
            let kind = kind(&e);
            // JSON を出した後に落ちた分も残るよう、ここでも固定の形の 1 行を出す
            console_log!(r#"{{"probe":"hyperdrive","error":"connection_task","kind":"{kind}"}}"#);
            if let Ok(mut errors) = task_errors.try_borrow_mut() {
                *errors.entry(kind).or_insert(0) += 1;
            }
        }
    });
    Ok(client)
}

/// Q3: この接続のロール
async fn role(client: &Client) -> Result<Role, PgError> {
    let user = first_row(&client.simple_query("SELECT current_user").await?);
    let flags = first_row(
        &client
            .simple_query(
                "SELECT rolsuper, rolbypassrls FROM pg_roles WHERE rolname = current_user",
            )
            .await?,
    );
    let current_user = col(&user, 0).unwrap_or_default().to_owned();
    Ok(Role {
        is_runtime_role: current_user == RUNTIME_ROLE,
        current_user,
        rolsuper: col(&flags, 0).map(|v| v == "t"),
        rolbypassrls: col(&flags, 1).map(|v| v == "t"),
    })
}

/// Q6: 1 トランザクションの中で `SELECT 1` を [`ROUNDTRIPS`] 回流した 1 文あたりの平均 ms
async fn roundtrip_avg(client: &mut Client) -> Result<f64, PgError> {
    let tx = client.transaction().await?;
    let started = now();
    for _ in 0..ROUNDTRIPS {
        tx.simple_query("SELECT 1").await?;
    }
    let spent = now().saturating_sub(started);
    tx.commit().await?;
    Ok(spent as f64 / f64::from(ROUNDTRIPS))
}

fn seen(settings: &[Option<String>], tenant: &str, echoed: Option<&str>) -> TxSeen {
    TxSeen {
        tenant_held: col(settings, 0) == Some(tenant),
        search_path_held: col(settings, 1) == Some("alc_api"),
        echoed: echoed == Some("x"),
    }
}

/// Q4 / Q5: テナントをトランザクション単位で設定し (骨格は alc-vein-worker の `in_tenant_tx` と同じ)、その中で
/// パラメータ付きの問い合わせを流す。`Row` は owned な値にして COMMIT より前に drop する (対照だけ後で drop)
async fn extended_tx(client: &mut Client, mode: Mode, tenant: &str) -> Result<TxSeen, Failure> {
    let tx = client.transaction().await.map_err(at("begin"))?;
    let rows = if matches!(mode, Mode::Typed) {
        tx.query_typed(SET_CONFIG, &[(&tenant, Type::TEXT)])
            .await
            .map_err(at("set_config"))?;
        let settings = tx.simple_query(READ_SETTINGS).await;
        let settings = first_row(&settings.map_err(at("read_setting"))?);
        let rows = tx.query_typed(ECHO, &[(&"x", Type::TEXT)]).await;
        let rows = rows.map_err(at("query"))?;
        tx.query_typed("SELECT 1", &[])
            .await
            .map_err(at("execute"))?;
        (settings, rows)
    } else {
        tx.execute(SET_CONFIG, &[&tenant])
            .await
            .map_err(at("set_config"))?;
        let settings = tx.simple_query(READ_SETTINGS).await;
        let settings = first_row(&settings.map_err(at("read_setting"))?);
        let rows = tx.query(ECHO, &[&"x"]).await.map_err(at("query"))?;
        tx.execute("SELECT 1", &[]).await.map_err(at("execute"))?;
        (settings, rows)
    };
    let (settings, rows) = rows;
    let echoed: Option<String> = rows.first().and_then(|row| row.try_get(0).ok());
    if matches!(mode, Mode::RowDroppedAfterCommit) {
        tx.commit().await.map_err(at("commit"))?;
        drop(rows);
    } else {
        drop(rows);
        tx.commit().await.map_err(at("commit"))?;
    }
    Ok(seen(&settings, tenant, echoed.as_deref()))
}

/// Q4 を拡張プロトコル抜きで: BEGIN から COMMIT まで `simple_query` / `batch_execute`
async fn simple_tx(client: &Client, tenant: &str) -> Result<TxSeen, Failure> {
    // 自分で作った UUID だが、SQL に埋める前に 16 進と `-` だけであることを確かめる
    if !tenant.bytes().all(|b| b.is_ascii_hexdigit() || b == b'-') {
        return Err(("set_config", "invalid_uuid".to_owned()));
    }
    client.batch_execute("BEGIN").await.map_err(at("begin"))?;
    let set_config = SET_CONFIG.replace("$1", &format!("'{tenant}'"));
    client
        .simple_query(&set_config)
        .await
        .map_err(at("set_config"))?;
    let settings = client.simple_query(READ_SETTINGS).await;
    let settings = first_row(&settings.map_err(at("read_setting"))?);
    let rows = client.simple_query("SELECT 'x'::text AS v").await;
    let rows = first_row(&rows.map_err(at("query"))?);
    client
        .simple_query("SELECT 1")
        .await
        .map_err(at("execute"))?;
    client.batch_execute("COMMIT").await.map_err(at("commit"))?;
    Ok(seen(&settings, tenant, col(&rows, 0)))
}

/// 1 接続ぶん: (i) 設定する前に見えないか → (ii) トランザクション → (iii) COMMIT の後に消えているか
async fn iteration(client: &mut Client, mode: Mode, tenant: &str, s: &mut Series) -> Option<u64> {
    let is_set = |m: &[SimpleQueryMessage]| col(&first_row(m), 0).is_some_and(|v| !v.is_empty());
    match client.simple_query(READ_TENANT).await {
        Ok(m) => s.leak_before_set += u32::from(is_set(&m)),
        Err(e) => return s.fail(at("read_before")(e)),
    }
    let started = now();
    let result = match mode {
        Mode::Simple => {
            let result = simple_tx(client, tenant).await;
            if result.is_err() {
                // 開いたままのトランザクションを残さない (失敗しても数えない)
                let _ = client.batch_execute("ROLLBACK").await;
            }
            result
        }
        _ => extended_tx(client, mode, tenant).await,
    };
    let seen = match result {
        Ok(seen) if seen.echoed => seen,
        Ok(_) => return s.fail(("query", "mismatch".to_owned())),
        Err(failure) => return s.fail(failure),
    };
    let ms = now().saturating_sub(started);
    s.ok += 1;
    s.held_in_tx += u32::from(seen.tenant_held);
    s.search_path_held += u32::from(seen.search_path_held);
    match client.simple_query(READ_TENANT).await {
        Ok(m) => s.empty_after_commit += u32::from(!is_set(&m)),
        Err(e) => drop(s.fail(at("read_after")(e))),
    }
    Some(ms)
}

struct Probe<'a> {
    env: &'a Env,
    task_errors: TaskErrors,
    connect_ms: Vec<u64>,
    report: Report,
}

impl Probe<'_> {
    /// 接続を 1 本張る。1 本目の結果は `connect` に、失敗の label は `errors` に残す
    async fn connect(&mut self) -> Option<Client> {
        let started = now();
        let conn = connect(self.env, &self.task_errors).await;
        let ms = now().saturating_sub(started);
        let (error, kind) = match &conn {
            Ok(_) => (None, None),
            Err((label, kind)) => (Some(*label), kind.clone()),
        };
        let ok = conn.is_ok();
        self.report.connect.get_or_insert(Connect {
            ok,
            ms,
            error,
            kind,
        });
        match conn {
            Ok(client) => {
                self.connect_ms.push(ms);
                Some(client)
            }
            Err((label, _)) => {
                self.report.errors.push(label);
                None
            }
        }
    }

    async fn series(&mut self, mode: Mode, iterations: u32, tenants: (&str, &str)) -> Series {
        let mut s = Series::default();
        let mut tx_ms = Vec::new();
        for i in 0..iterations {
            s.iterations += 1;
            let Some(mut client) = self.connect().await else {
                s.fail(("connect", "connect".to_owned()));
                continue;
            };
            let tenant = if i % 2 == 0 { tenants.0 } else { tenants.1 };
            tx_ms.extend(iteration(&mut client, mode, tenant, &mut s).await);
        }
        s.tx_p50 = median(tx_ms);
        s
    }

    async fn run(mut self) -> Report {
        let (a, b) = (random_uuid(), random_uuid());
        let tenants = (a.as_str(), b.as_str());
        // 1 本目: ロール・往復の時間・単発の query_typed (どれも prepare の往復をしない)
        if let Some(mut client) = self.connect().await {
            match role(&client).await {
                Ok(role) => self.report.role = Some(role),
                Err(_) => self.report.errors.push(Label::Query),
            }
            match roundtrip_avg(&mut client).await {
                Ok(avg) => self.report.timing_ms.stmt_roundtrip_avg = avg,
                Err(_) => self.report.errors.push(Label::Query),
            }
            self.report.single.typed = Some(outcome(client.query_typed("SELECT 1", &[]).await));
        }
        // 2 本目: 単発の名前付き prepared statement (接続ごと落ちても他に響かないよう、これだけで 1 本使う)
        if let Some(client) = self.connect().await {
            self.report.single.named = Some(outcome(client.query("SELECT 1", &[]).await));
        }
        // 接続を壊しにくい順に流す。対照は最後 (握ったままの prepared statement の影響は次の接続・次の実行以降に出る)
        self.report.tx_simple = self.series(Mode::Simple, ITERATIONS, tenants).await;
        self.report.tx_typed = self.series(Mode::Typed, ITERATIONS, tenants).await;
        self.report.tx_named = self.series(Mode::Named, ITERATIONS, tenants).await;
        self.report.prepared_row_dropped_after_commit = self
            .series(Mode::RowDroppedAfterCommit, CONTROL_ITERATIONS, tenants)
            .await;
        if let Ok(errors) = self.task_errors.try_borrow() {
            self.report.connection_task = errors.clone();
        }
        self.report.timing_ms.connect_p50 = median(self.connect_ms);
        self.report
    }
}

/// 外へ Err を返さない (未捕捉の例外としてエラーの文がログに残るため)
#[event(scheduled)]
async fn scheduled(_event: ScheduledEvent, env: Env, _ctx: ScheduleContext) {
    if now() >= EXPIRES_AT_MS {
        console_log!(r#"{{"probe":"hyperdrive","expired":true}}"#);
        return;
    }
    let probe = Probe {
        env: &env,
        task_errors: TaskErrors::default(),
        connect_ms: Vec::new(),
        report: Report {
            probe: "hyperdrive",
            ..Report::default()
        },
    };
    match serde_json::to_string(&probe.run().await) {
        Ok(line) => console_log!("{line}"),
        Err(_) => console_log!(r#"{{"probe":"hyperdrive","errors":["serialize"]}}"#),
    }
}

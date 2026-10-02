//! Hyperdrive 経由の DB 接続を測る probe。**HTTP のハンドラを持たず、定時実行だけ**で動き、
//! 1 回の実行につき 1 行の JSON をログに出す (項目は README の「Hyperdrive の probe」)。
//!
//! - 読むのは `current_user`・`pg_roles` の自分の行・`current_setting` だけ。業務の表は読まない・書かない。
//! - **ログに出すのはロール名・真偽・回数・SQLSTATE・ms・固定の label だけ。** 接続文字列・宛先・認証情報と、
//!   エラーの文 (`Display` / `Debug` / DB の message) は出さない。失敗は [`Label`] と SQLSTATE にだけ落とす。
//! - [`EXPIRES_AT_MS`] を過ぎたら DB に繋がない。

#![deny(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]

use std::cell::Cell;
use std::collections::BTreeMap;
use std::rc::Rc;

use serde::Serialize;
use tokio_postgres::{Client, Config, Error as PgError, SimpleQueryMessage};
use worker::postgres_tls::PassthroughTls;
use worker::{
    console_log, event, Date, Env, ScheduleContext, ScheduledEvent, SecureTransport, Socket,
};

const HD_BINDING: &str = "HD";
/// 2026-10-05T00:00:00Z。これを過ぎた実行は DB に繋がずに終わる
const EXPIRES_AT_MS: u64 = 1_791_158_400_000;
const RUNTIME_ROLE: &str = "alc_api_rt";
/// 接続を張り直す回数 (テナント A / B を交互に)
const ITERATIONS: u32 = 10;
/// 対照系列 (Row を COMMIT の後まで持つ) の回数
const CONTROL_ITERATIONS: u32 = 5;
const ROUNDTRIPS: u32 = 20;
const READ_TENANT: &str = "SELECT current_setting('app.current_tenant_id', true)";

/// 失敗の出し方はこの固定の label だけ (エラーの文は出さない)
#[derive(Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
enum Label {
    HyperdriveBinding,
    ConfigParse,
    Socket,
    Handshake,
    ConnectionTask,
    Query,
}

/// 1 本目の接続の結果
#[derive(Serialize)]
struct Connect {
    ok: bool,
    ms: u64,
    error: Option<Label>,
    sqlstate: Option<String>,
}

#[derive(Serialize)]
struct Role {
    current_user: String,
    is_runtime_role: bool,
    rolsuper: Option<bool>,
    rolbypassrls: Option<bool>,
}

#[derive(Default, Serialize)]
struct TxSetting {
    iterations: u32,
    held_in_tx: u32,
    empty_after_commit: u32,
    leak_before_set: u32,
    search_path_held: u32,
}

#[derive(Default, Serialize)]
struct Prepared {
    iterations: u32,
    ok: u32,
    errors_by_sqlstate: BTreeMap<String, u32>,
}

impl Prepared {
    /// 成功なら `ok` に、失敗なら SQLSTATE (取れなければ `none`) ごとに数える。失敗のとき false
    fn record(&mut self, result: &Result<TxSeen, PgError>) -> bool {
        let key = match result {
            Ok(seen) if seen.echoed => {
                self.ok += 1;
                return true;
            }
            Ok(_) => "mismatch".to_owned(),
            Err(e) => sqlstate(e).unwrap_or_else(|| "none".to_owned()),
        };
        *self.errors_by_sqlstate.entry(key).or_insert(0) += 1;
        false
    }
}

#[derive(Default, Serialize)]
struct Timing {
    connect_p50: u64,
    tx_p50: u64,
    stmt_roundtrip_avg: f64,
}

#[derive(Default, Serialize)]
struct Report {
    probe: &'static str,
    connect: Option<Connect>,
    role: Option<Role>,
    tx_setting: TxSetting,
    prepared: Prepared,
    prepared_row_dropped_after_commit: Prepared,
    timing_ms: Timing,
    errors: Vec<Label>,
}

/// 1 トランザクションの中で見えたもの (owned な値だけ)
#[derive(Default)]
struct TxSeen {
    tenant_held: bool,
    search_path_held: bool,
    echoed: bool,
}

fn now() -> u64 {
    Date::now().as_millis()
}

fn sqlstate(e: &PgError) -> Option<String> {
    e.as_db_error().map(|db| db.code().code().to_owned())
}

fn median(mut ms: Vec<u64>) -> u64 {
    ms.sort_unstable();
    ms.get(ms.len() / 2).copied().unwrap_or(0)
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
async fn connect(
    env: &Env,
    task_failed: &Rc<Cell<bool>>,
) -> Result<Client, (Label, Option<String>)> {
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
        .map_err(|e| (Label::Handshake, sqlstate(&e)))?;
    let task_failed = Rc::clone(task_failed);
    wasm_bindgen_futures::spawn_local(async move {
        if connection.await.is_err() {
            task_failed.set(true);
            console_log!(r#"{{"probe":"hyperdrive","error":"connection_task"}}"#);
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

/// Q4 / Q5: テナントをトランザクション単位で設定し (骨格は alc-vein-worker の `in_tenant_tx` と同じ)、
/// その中で名前付き prepared statement を使う。`Row` は owned な値にして、COMMIT より前に drop する
async fn tenant_tx(client: &mut Client, tenant: &str) -> Result<TxSeen, PgError> {
    let tx = client.transaction().await?;
    tx.execute(
        "SELECT set_config('app.current_tenant_id', $1, true), set_config('search_path', 'alc_api', true)",
        &[&tenant],
    )
    .await?;
    let settings = first_row(
        &tx.simple_query(
            "SELECT current_setting('app.current_tenant_id', true), current_setting('search_path')",
        )
        .await?,
    );
    let echoed: Option<String> = tx
        .query("SELECT $1::text AS v", &[&"x"])
        .await?
        .first()
        .and_then(|row| row.try_get(0).ok());
    tx.execute("SELECT 1", &[]).await?;
    tx.commit().await?;
    Ok(TxSeen {
        tenant_held: col(&settings, 0) == Some(tenant),
        search_path_held: col(&settings, 1) == Some("alc_api"),
        echoed: echoed.as_deref() == Some("x"),
    })
}

/// Q5 の対照: 同じ問い合わせで、`Row` (prepared statement を握る) を COMMIT の後まで持ってから drop する
async fn row_dropped_after_commit_tx(client: &mut Client) -> Result<TxSeen, PgError> {
    let tx = client.transaction().await?;
    let rows = tx.query("SELECT $1::text AS v", &[&"x"]).await?;
    tx.execute("SELECT 1", &[]).await?;
    tx.commit().await?;
    let echoed: Option<String> = rows.first().and_then(|row| row.try_get(0).ok());
    drop(rows);
    Ok(TxSeen {
        echoed: echoed.as_deref() == Some("x"),
        ..TxSeen::default()
    })
}

async fn run(env: &Env) -> Report {
    let task_failed = Rc::new(Cell::new(false));
    let (tenant_a, tenant_b) = (random_uuid(), random_uuid());
    let (mut connect_ms, mut tx_ms) = (Vec::new(), Vec::new());
    let mut r = Report {
        probe: "hyperdrive",
        ..Report::default()
    };
    for i in 0..ITERATIONS {
        r.tx_setting.iterations += 1;
        r.prepared.iterations += 1;
        let started = now();
        let conn = connect(env, &task_failed).await;
        let ms = now().saturating_sub(started);
        if r.connect.is_none() {
            let (error, sqlstate) = match &conn {
                Ok(_) => (None, None),
                Err((label, code)) => (Some(*label), code.clone()),
            };
            let ok = conn.is_ok();
            r.connect = Some(Connect {
                ok,
                ms,
                error,
                sqlstate,
            });
        }
        let mut client = match conn {
            Ok(client) => client,
            Err((label, _)) => {
                r.errors.push(label);
                continue;
            }
        };
        connect_ms.push(ms);
        if i == 0 {
            match role(&client).await {
                Ok(role) => r.role = Some(role),
                Err(_) => r.errors.push(Label::Query),
            }
            match roundtrip_avg(&mut client).await {
                Ok(avg) => r.timing_ms.stmt_roundtrip_avg = avg,
                Err(_) => r.errors.push(Label::Query),
            }
        }
        // (i) トランザクションの前: 前の利用者の設定が残っていないか
        match client.simple_query(READ_TENANT).await {
            Ok(m) if col(&first_row(&m), 0).is_some_and(|v| !v.is_empty()) => {
                r.tx_setting.leak_before_set += 1
            }
            Ok(_) => {}
            Err(_) => r.errors.push(Label::Query),
        }
        // (ii) トランザクションの中
        let tenant = if i % 2 == 0 { &tenant_a } else { &tenant_b };
        let started = now();
        let seen = tenant_tx(&mut client, tenant).await;
        if !r.prepared.record(&seen) {
            r.errors.push(Label::Query);
        }
        let Ok(seen) = seen else { continue };
        tx_ms.push(now().saturating_sub(started));
        r.tx_setting.held_in_tx += u32::from(seen.tenant_held);
        r.tx_setting.search_path_held += u32::from(seen.search_path_held);
        // (iii) COMMIT の後: 同じ接続で設定が消えているか
        match client.simple_query(READ_TENANT).await {
            Ok(m) if col(&first_row(&m), 0).is_some_and(|v| !v.is_empty()) => {}
            Ok(_) => r.tx_setting.empty_after_commit += 1,
            Err(_) => r.errors.push(Label::Query),
        }
    }
    // 対照は最後に流す (握ったままの prepared statement の影響は、次の接続・次の実行以降に出る)
    for _ in 0..CONTROL_ITERATIONS {
        r.prepared_row_dropped_after_commit.iterations += 1;
        match connect(env, &task_failed).await {
            Ok(mut client) => {
                let seen = row_dropped_after_commit_tx(&mut client).await;
                if !r.prepared_row_dropped_after_commit.record(&seen) {
                    r.errors.push(Label::Query);
                }
            }
            Err((label, _)) => r.errors.push(label),
        }
    }
    if task_failed.get() {
        r.errors.push(Label::ConnectionTask);
    }
    r.timing_ms.connect_p50 = median(connect_ms);
    r.timing_ms.tx_p50 = median(tx_ms);
    r
}

/// 外へ Err を返さない (未捕捉の例外としてエラーの文がログに残るため)
#[event(scheduled)]
async fn scheduled(_event: ScheduledEvent, env: Env, _ctx: ScheduleContext) {
    if now() >= EXPIRES_AT_MS {
        console_log!(r#"{{"probe":"hyperdrive","expired":true}}"#);
        return;
    }
    match serde_json::to_string(&run(&env).await) {
        Ok(line) => console_log!("{line}"),
        Err(_) => console_log!(r#"{{"probe":"hyperdrive","errors":["serialize"]}}"#),
    }
}

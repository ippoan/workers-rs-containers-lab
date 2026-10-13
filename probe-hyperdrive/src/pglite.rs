//! DO 経由の系列: probe-pglite-db の Durable Object `PgliteDb` (DO の中の PGlite) へ `Stub::connect` で繋ぎ、
//! 共通 crate `alc-worker-db` の `PgClient::tenant_tx` でテナントの行数を読む (テナント漏れ検査。
//! Refs ohishi-exp/rust-leave-worker#1)。1 回の実行につき 1 行の JSON (`"probe":"pglite"`) をログに出す。
//!
//! - DO への繋ぎ方は `worker-unknown/src/db.rs` の `connect_container` と同じ (`Stub::connect` → `Socket` →
//!   `connect_raw`、平文)。tokio-postgres は無改造
//! - 相手の DB は DO の中で毎回 migration から作る揮発の DB で、中身は seed の 2 テナント (乗務員 7 人 / 13 人) だけ。
//!   **このため DB のエラーは SQLSTATE に加えて DB の message も出す** (Hyperdrive の系列とは違い、宛先・認証情報を
//!   含みうる本物の DB ではない。失敗の原文を残すのがこの系列の目的)
//! - Hyperdrive の系列とは独立に、[`EXPIRES_AT_MS`] まで動く

use alc_worker_db::{kind, PgClient};
use serde::Serialize;
use tokio_postgres::config::SslMode;
use tokio_postgres::types::{ToSql, Type};
use tokio_postgres::{Client, Config, Error as PgError, NoTls, SimpleQueryMessage};
use uuid::Uuid;
use worker::{console_log, Date, Env};

const BINDING: &str = "PGLITE_DB";
/// 1 個の DO (= 1 個の PGlite) に集める
const DO_NAME: &str = "probe-pglite-db";
/// 2026-10-20T00:00:00Z。これを過ぎた実行は DO に繋がない
pub const EXPIRES_AT_MS: u64 = 1_792_454_400_000;
const RUNTIME_ROLE: &str = "alc_api_rt";
/// seed のテナントの名前と乗務員の数 (probe-pglite-db/src/steps.js の SEED_TENANTS と同じ)。
/// ID は DO の起動のたびに gen_random_uuid() で作られるので、名前で引く (tenants 表には RLS が無い)
const TENANT_A: &str = "leak-a";
const TENANT_B: &str = "leak-b";
const EXPECTED_A: i64 = 7;
const EXPECTED_B: i64 = 13;
const COUNT_ALL: &str = "SELECT count(*) FROM alc_api.employees";
const COUNT_OF: &str = "SELECT count(*) FROM alc_api.employees WHERE tenant_id = $1";
const TENANT_ID: &str = "SELECT id FROM alc_api.tenants WHERE name = $1";

#[derive(Default, Serialize)]
pub struct Report {
    probe: &'static str,
    /// 1 本目の接続 (DO の起動 = PGlite の起動・migration を含む) と 2 本目の接続の ms (handshake まで)
    connect_cold_ms: Option<u64>,
    connect_warm_ms: Option<u64>,
    /// 接続の `current_user` (`alc_api_rt` のはず) と、それが実行用ロールか
    current_user: Option<String>,
    is_runtime_role: Option<bool>,
    /// tenant を設定せずに (transaction の外で) 数えた行数。RLS が効いていれば 0 かエラー
    unset_rows: Option<i64>,
    unset_error: Option<String>,
    /// `tenant_tx(A)` / `tenant_tx(B)` の中で数えた行数 (seed は A = 7、B = 13)
    tenant_a_rows: Option<i64>,
    tenant_b_rows: Option<i64>,
    /// `tenant_tx(A)` の中で B の行を名指しで数えた数 (0 でなければ漏れ)
    a_sees_b_rows: Option<i64>,
    /// 全部が期待どおりか (ロールが alc_api_rt・A = 7・B = 13・A から B が 0・tenant 未設定で 0 行かエラー)
    isolated: bool,
    /// 1 tx (tenant_tx で 1 文) の往復の ms
    tx_ms: Option<u64>,
    /// 失敗した step と、そのエラー (SQLSTATE: message、または tokio-postgres の種類)
    error: Option<String>,
}

fn now() -> u64 {
    Date::now().as_millis()
}

/// DB のエラーは SQLSTATE と message、それ以外は kit の [`kind`]
fn describe(e: &PgError) -> String {
    match e.as_db_error() {
        Some(db) => format!("{}: {}", db.code().code(), db.message()),
        None => kind(e),
    }
}

async fn connect(env: &Env) -> Result<Client, String> {
    let ns = env
        .durable_object(BINDING)
        .map_err(|_| "binding".to_owned())?;
    let socket = ns
        .get_by_name(DO_NAME)
        .map_err(|_| "stub".to_owned())?
        .connect(&format!("{DO_NAME}:5432"))
        .map_err(|_| "socket".to_owned())?;
    let mut config = Config::new();
    // ロールは DO が接続の頭で SET SESSION AUTHORIZATION する (ここの user は PGlite が使わない)
    config
        .user(RUNTIME_ROLE)
        .dbname("postgres")
        .ssl_mode(SslMode::Disable);
    let (client, connection) = config
        .connect_raw(socket, NoTls)
        .await
        .map_err(|e| format!("handshake: {}", describe(&e)))?;
    wasm_bindgen_futures::spawn_local(async move {
        if let Err(e) = connection.await {
            let kind = kind(&e);
            console_log!(r#"{{"probe":"pglite","error":"connection_task","kind":"{kind}"}}"#);
        }
    });
    Ok(client)
}

/// `simple_query` の最初の行の最初の列
fn first(messages: &[SimpleQueryMessage]) -> Option<String> {
    messages.iter().find_map(|m| match m {
        SimpleQueryMessage::Row(row) => row.get(0).map(str::to_owned),
        _ => None,
    })
}

/// `tenant_tx(tenant)` の中で 1 文数える (`filter` があれば tenant_id で名指し)
async fn count_in(pg: &mut PgClient, tenant: Uuid, filter: Option<Uuid>) -> Result<i64, PgError> {
    pg.tenant_tx(tenant, move |tx| {
        Box::pin(async move {
            let row = match &filter {
                Some(id) => {
                    let params: &[(&(dyn ToSql + Sync), Type)] = &[(id, Type::UUID)];
                    tx.query_typed_one(COUNT_OF, params).await?
                }
                None => tx.query_typed_one(COUNT_ALL, &[]).await?,
            };
            row.try_get::<_, i64>(0)
        })
    })
    .await
}

/// seed のテナントの ID を名前で引く (transaction の外、型付きの名前なしの文)
async fn tenant_id(client: &Client, name: &str) -> Result<Uuid, String> {
    let params: &[(&(dyn ToSql + Sync), Type)] = &[(&name, Type::TEXT)];
    client
        .query_typed_one(TENANT_ID, params)
        .await
        .and_then(|row| row.try_get(0))
        .map_err(|e| format!("tenant_id {name}: {}", describe(&e)))
}

async fn measure(env: &Env, r: &mut Report) -> Result<(), String> {
    let started = now();
    let client = connect(env)
        .await
        .map_err(|e| format!("connect cold: {e}"))?;
    r.connect_cold_ms = Some(now().saturating_sub(started));

    // tenant を設定しない読み (transaction の外)。RLS が効いていれば 0 行か、設定値の変換エラー
    match client.simple_query(COUNT_ALL).await {
        Ok(m) => r.unset_rows = first(&m).and_then(|v| v.parse().ok()),
        Err(e) => r.unset_error = Some(describe(&e)),
    }
    let (a, b) = (
        tenant_id(&client, TENANT_A).await?,
        tenant_id(&client, TENANT_B).await?,
    );
    let mut pg = PgClient::new(client);
    let user = pg
        .current_user()
        .await
        .map_err(|e| format!("current_user: {}", describe(&e)))?;
    r.is_runtime_role = Some(user.as_deref() == Some(RUNTIME_ROLE));
    r.current_user = user;

    let tx_started = now();
    r.tenant_a_rows = Some(
        count_in(&mut pg, a, None)
            .await
            .map_err(|e| format!("tenant_tx A: {}", describe(&e)))?,
    );
    r.tx_ms = Some(now().saturating_sub(tx_started));
    r.tenant_b_rows = Some(
        count_in(&mut pg, b, None)
            .await
            .map_err(|e| format!("tenant_tx B: {}", describe(&e)))?,
    );
    r.a_sees_b_rows = Some(
        count_in(&mut pg, a, Some(b))
            .await
            .map_err(|e| format!("tenant_tx A→B: {}", describe(&e)))?,
    );
    drop(pg);

    // 2 本目: DO と PGlite が起きている状態での接続 (1 本目が閉じるまで DO の中で待つ)
    let started = now();
    let warm = connect(env)
        .await
        .map_err(|e| format!("connect warm: {e}"))?;
    r.connect_warm_ms = Some(now().saturating_sub(started));
    drop(warm);
    Ok(())
}

pub async fn run(env: &Env) -> Report {
    let mut r = Report {
        probe: "pglite",
        ..Report::default()
    };
    if let Err(e) = measure(env, &mut r).await {
        r.error = Some(e);
    }
    r.isolated = r.is_runtime_role == Some(true)
        && r.tenant_a_rows == Some(EXPECTED_A)
        && r.tenant_b_rows == Some(EXPECTED_B)
        && r.a_sees_b_rows == Some(0)
        && (r.unset_rows == Some(0) || r.unset_error.is_some());
    r
}

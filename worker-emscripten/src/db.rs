//! postgres への接続 ((A) `worker-unknown/src/db.rs` の移植)。**DB に繋ぐのはこの 1 か所だけ**で、経路は staging の Container だけ:
//! Durable Object `LAB_DB` へ TCP (`Stub::connect`) → DO が Container の PgBouncer (6432、
//! transaction mode) へ中継する (DO は (A) の `worker-unknown/src/lab_db.rs`)。
//!
//! `Socket` は JS の値なので、この関数は fetch handler から呼ぶ。

use tokio_postgres::config::SslMode;
use tokio_postgres::{Client, Config, NoTls};
use worker::{console_error, Env, Socket};

/// DO binding。1 個の DO (= 1 個の Container) に全リクエストを集める
pub const LAB_DB_BINDING: &str = "LAB_DB";
const LAB_DB_NAME: &str = "lab-db";
/// Container 内の PgBouncer のポート (container/pgbouncer.ini)
pub const PGBOUNCER_PORT: u16 = 6432;

/// DO `LabDb` が動いている colo (`GET /where`)。DO の場所は変わらないので、Worker の isolate でも
/// 1 回取れたら覚えておき、以後は DO へ聞きに行かない (計測の total に余計な往復を足さないため)
pub async fn do_colo(env: &Env) -> Result<String, String> {
    thread_local! {
        static DO_COLO: std::cell::RefCell<Option<String>> = const { std::cell::RefCell::new(None) };
    }
    if let Some(c) = DO_COLO.with(|c| c.borrow().clone()) {
        return Ok(c);
    }
    let stub = env
        .durable_object(LAB_DB_BINDING)
        .and_then(|ns| ns.get_by_name(LAB_DB_NAME))
        .map_err(|e| format!("lab-db stub: {e}"))?;
    let mut resp = stub
        .fetch_with_str("https://lab-db/where")
        .await
        .map_err(|e| format!("lab-db /where: {e}"))?;
    if resp.status_code() != 200 {
        return Err(format!("lab-db /where: status {}", resp.status_code()));
    }
    let colo = resp
        .text()
        .await
        .map_err(|e| format!("lab-db /where: {e}"))?;
    DO_COLO.with(|c| *c.borrow_mut() = Some(colo.clone()));
    Ok(colo)
}

pub async fn connect(env: &Env) -> Result<Client, String> {
    let ns = env
        .durable_object(LAB_DB_BINDING)
        .map_err(|e| format!("no {LAB_DB_BINDING} binding: {e}"))?;
    connect_container(&ns).await
}

/// DO への TCP をそのまま postgres のソケットとして使う (DO が Container の PgBouncer へ
/// バイトを中継する)。DO → Container は Cloudflare 内なので平文。PgBouncer は trust 認証で、
/// superuser ではない `bench` で繋ぐ。
async fn connect_container(ns: &worker::ObjectNamespace) -> Result<Client, String> {
    let stub = ns
        .get_by_name(LAB_DB_NAME)
        .map_err(|e| format!("lab-db stub: {e}"))?;
    let socket = stub
        .connect(&format!("{LAB_DB_NAME}:{PGBOUNCER_PORT}"))
        .map_err(|e| format!("lab-db connect: {e}"))?;
    let mut config = Config::new();
    config
        .user("bench")
        .dbname("postgres")
        .ssl_mode(SslMode::Disable);
    handshake(config, socket).await
}

async fn handshake(config: Config, socket: Socket) -> Result<Client, String> {
    let (client, connection) = config.connect_raw(socket, NoTls).await.map_err(|e| {
        // tokio_postgres::Error の Display は "db error" だけなので、DB の message も載せる
        let detail = e
            .as_db_error()
            .map(|db| format!("{} ({})", db.message(), db.code().code()))
            .unwrap_or_else(|| e.to_string());
        format!("postgres handshake: {detail}")
    })?;
    wasm_bindgen_futures::spawn_local(async move {
        if let Err(e) = connection.await {
            console_error!("postgres connection: {e}");
        }
    });
    Ok(client)
}

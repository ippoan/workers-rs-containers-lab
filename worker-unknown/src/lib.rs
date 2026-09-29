//! (A) wasm32-unknown-unknown 版の worker。`GET /query` だけを持つ:
//! DO `LabDb` 経由で Container の PgBouncer (transaction mode) へ繋ぎ、1 トランザクション
//! (`BEGIN; SELECT count(*) …; SELECT id, v … LIMIT 10; COMMIT`) を流して JSON で返す。
//! それ以外のパスは 404。
//!
//! 応答ヘッダー `Server-Timing: connect;dur=…, db;dur=…` は bench/measure.mjs が読む。
//! ローカル (wrangler dev) の Date.now は CPU 実行中も進むが、Cloudflare 上では Spectre 対策で
//! I/O まで止まるので、CPU 時間は dashboard で見る。

mod db;
mod lab_db;

use serde::Serialize;
use tokio_postgres::Client;
use worker::{console_error, event, Context, Date, Env, Method, Request, Response, Result};

pub use crate::lab_db::LabDb;

#[derive(Serialize)]
struct Item {
    id: i32,
    v: String,
}

#[derive(Serialize)]
struct QueryResult {
    count: i64,
    items: Vec<Item>,
}

/// 1 トランザクション。**`Row` は tx の中で owned な値に変換してから COMMIT する** —
/// `Row` は prepared statement を握っていて、COMMIT 後に drop すると Close がトランザクションの
/// 外に出て別のサーバー接続へ回り、`prepared statement "s1" already exists` (42P05) になる
/// (transaction mode のプーラー越しの罠)。
async fn run_query(client: &mut Client) -> std::result::Result<QueryResult, tokio_postgres::Error> {
    let tx = client.transaction().await?;
    let count: i64 = tx
        .query_one("SELECT count(*) FROM items", &[])
        .await?
        .get(0);
    let items = tx
        .query("SELECT id, v FROM items ORDER BY id LIMIT 10", &[])
        .await?
        .into_iter()
        .map(|row| Item {
            id: row.get(0),
            v: row.get(1),
        })
        .collect();
    tx.commit().await?;
    Ok(QueryResult { count, items })
}

#[derive(Serialize)]
struct ErrorBody<'a> {
    error: &'a str,
}

fn error_json(code: &str, status: u16) -> Result<Response> {
    Ok(Response::from_json(&ErrorBody { error: code })?.with_status(status))
}

#[event(fetch)]
async fn fetch(req: Request, env: Env, _ctx: Context) -> Result<Response> {
    if req.method() != Method::Get || req.path() != "/query" {
        return error_json("not_found", 404);
    }
    let started = Date::now().as_millis();
    let mut client = match db::connect(&env).await {
        Ok(c) => c,
        Err(e) => {
            console_error!("lab: {e}");
            return error_json("db_connect_failed", 502);
        }
    };
    let connected = Date::now().as_millis();
    let result = run_query(&mut client).await;
    let db_ms = Date::now().as_millis() - connected;
    let connect_ms = connected - started;
    let body = match result {
        Ok(r) => r,
        Err(e) => {
            console_error!("lab: query: {e}");
            return error_json("db_query_failed", 500);
        }
    };
    let mut resp = Response::from_json(&body)?;
    resp.headers_mut().set(
        "server-timing",
        &format!("connect;dur={connect_ms}, db;dur={db_ms}"),
    )?;
    Ok(resp)
}

//! (A) wasm32-unknown-unknown 版の worker。`GET /query` だけを持つ:
//! DO `LabDb` 経由で Container の PgBouncer (transaction mode) へ繋ぎ、1 トランザクション
//! (`BEGIN; SELECT count(*) …; SELECT id, v … LIMIT 10; COMMIT`) を流して JSON で返す。
//! それ以外のパスは 404。
//!
//! 応答ヘッダー `Server-Timing: connect;dur=…, rtt;dur=…, db;dur=…` は bench/measure.mjs が読む
//! (`rtt` は tx の外で `SELECT 1` を 1 回投げた往復 = Worker → DO → Container の 1 往復)。
//! `GET /query?rtt_do=1` のときだけ、応答の前に DO の `GET /rtt` を呼び、DO の中で測った
//! DO ↔ Container だけの往復を `rtt_do;dur=…` として足す (DO への往復が 1 回増えるので既定では呼ばない)。
//! JSON の `where` は Worker と DO `LabDb` が動いている colo (Container との距離を見るため)。
//! ローカル (wrangler dev) の Date.now は CPU 実行中も進むが、Cloudflare 上では Spectre 対策で
//! I/O まで止まるので、CPU 時間は dashboard で見る。

mod db;

use serde::Serialize;
use tokio_postgres::Client;
use worker::{console_error, event, Context, Date, Env, Method, Request, Response, Result};

// DO `LabDb` は lab-db (../lab-db、staging では lab-db-staging) のものを wrangler.toml の script_name で参照する。
// A は DO を持たない

#[derive(Serialize)]
struct Item {
    id: i32,
    v: String,
}

struct QueryResult {
    count: i64,
    items: Vec<Item>,
}

/// Worker と DO が動いている colo。取れなければ null
#[derive(Serialize)]
struct Where {
    worker: Option<String>,
    #[serde(rename = "do")]
    durable_object: Option<String>,
}

#[derive(Serialize)]
struct QueryResponse {
    count: i64,
    items: Vec<Item>,
    #[serde(rename = "where")]
    location: Where,
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

/// `?rtt_do=1` があるか (url crate で解くと idna の表を bundle に引き込むので、文字列で見る)
fn wants_rtt_do(req: &Request) -> bool {
    req.inner()
        .url()
        .split_once('?')
        .is_some_and(|(_, q)| q.split('&').any(|kv| kv == "rtt_do=1"))
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
    // tx の外で 1 往復だけ (simple query なので prepare の往復が無い)
    if let Err(e) = client.simple_query("SELECT 1").await {
        console_error!("lab: rtt: {e}");
        return error_json("db_query_failed", 500);
    }
    let pinged = Date::now().as_millis();
    let result = run_query(&mut client).await;
    let db_ms = Date::now().as_millis() - pinged;
    let rtt_ms = pinged - connected;
    let connect_ms = connected - started;
    let QueryResult { count, items } = match result {
        Ok(r) => r,
        Err(e) => {
            console_error!("lab: query: {e}");
            return error_json("db_query_failed", 500);
        }
    };
    // 計測の外 (Server-Timing には入れない)。DO の colo は isolate ごとに初回だけ DO へ聞く
    let durable_object = match db::do_colo(&env).await {
        Ok(c) => Some(c),
        Err(e) => {
            console_error!("lab: where: {e}");
            None
        }
    };
    // 計測の外。DO が測った DO ↔ Container の往復 (?rtt_do=1 のときだけ)
    let rtt_do = if wants_rtt_do(&req) {
        match db::do_rtt(&env).await {
            Ok(ms) => Some(ms),
            Err(e) => {
                console_error!("lab: rtt_do: {e}");
                None
            }
        }
    } else {
        None
    };
    let location = Where {
        worker: req.cf().map(|cf| cf.colo()),
        durable_object,
    };
    let mut resp = Response::from_json(&QueryResponse {
        count,
        items,
        location,
    })?;
    let mut timing = format!("connect;dur={connect_ms}, rtt;dur={rtt_ms}, db;dur={db_ms}");
    if let Some(ms) = rtt_do {
        timing.push_str(&format!(", rtt_do;dur={ms}"));
    }
    resp.headers_mut().set("server-timing", &timing)?;
    Ok(resp)
}

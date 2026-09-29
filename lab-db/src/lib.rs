//! Durable Object `LabDb` ([`lab_db`]) を持つだけのスクリプト。Worker (A / B) は wrangler.toml の
//! `script_name` でこの DO を参照し、`Stub::connect` で TCP を、`GET /where`・`GET /rtt` を DO へ直接送る。
//! このスクリプト自身の fetch handler は使わない (workers_dev も route も無い) ので 404 を返すだけ。

mod lab_db;

use worker::{event, Context, Env, Request, Response, Result};

pub use crate::lab_db::LabDb;

#[event(fetch)]
async fn fetch(_req: Request, _env: Env, _ctx: Context) -> Result<Response> {
    Response::error("not found", 404)
}

//! wasm32-unknown-unknown 版の probe。口と処理は ../probe-shared/ (probe-emscripten と共通)。
//! `GET /zip?mb=N&mode=flush` (運行ごとに R2 へ書き出して捨てる版) だけはこの crate にある (flush.rs)。

mod flush;
#[path = "../../probe-shared/mem.rs"]
mod mem;
#[cfg(feature = "pdf")]
#[path = "../../probe-shared/pdf.rs"]
mod pdf;
#[path = "../../probe-shared/probe.rs"]
mod probe;
#[path = "../../probe-shared/sign.rs"]
mod sign;
#[path = "../../probe-shared/zip_probe.rs"]
mod zip_probe;

use probe::now_ms;
use worker::{console_error, event, Context, Env, Method, Request, Response, Result};

/// `mode=flush` の書き出し先 (wrangler.toml の env.staging。ローカルの wrangler dev では手元の R2)
const R2_BINDING: &str = "LAB_R2";

/// feature `ring` を外したときに /sign が返す理由
#[cfg(not(feature = "ring"))]
const RING_UNSUPPORTED: &str = "ring is disabled for this target (feature `ring` off)";
/// feature `pdf` を外したときに /pdf が返す理由
#[cfg(not(feature = "pdf"))]
const PDF_UNSUPPORTED: &str = "printpdf is disabled for this target (feature `pdf` off)";

#[event(fetch)]
async fn fetch(req: Request, env: Env, _ctx: Context) -> Result<Response> {
    if req.method() == Method::Get
        && req.path() == "/zip"
        && probe::query(&req, "mode").as_deref() == Some("flush")
    {
        return zip_flush(&req, &env).await;
    }
    probe::handle(&req)
}

async fn zip_flush(req: &Request, env: &Env) -> Result<Response> {
    let Some(mb) = probe::query(req, "mb")
        .and_then(|v| v.parse().ok())
        .filter(|mb| (1..=20).contains(mb))
    else {
        return probe::error_json("mb must be 1..=20", 400);
    };
    let Ok(bucket) = env.bucket(R2_BINDING) else {
        return probe::error_json(
            "R2 binding LAB_R2 is not configured (use --env staging)",
            501,
        );
    };
    match flush::run(&bucket, mb).await {
        Ok(r) => probe::json(req, r),
        Err(e) => {
            console_error!("probe: zip flush: {e}");
            probe::error_json(&e, 500)
        }
    }
}

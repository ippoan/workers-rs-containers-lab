//! wasm32-unknown-unknown 版の probe。口と処理は ../probe-shared/ (probe-emscripten と共通)。

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
use worker::{event, Context, Env, Request, Response, Result};

/// feature `ring` を外したときに /sign が返す理由
#[cfg(not(feature = "ring"))]
const RING_UNSUPPORTED: &str = "ring is disabled for this target (feature `ring` off)";
/// feature `pdf` を外したときに /pdf が返す理由
#[cfg(not(feature = "pdf"))]
const PDF_UNSUPPORTED: &str = "printpdf is disabled for this target (feature `pdf` off)";

#[event(fetch)]
async fn fetch(req: Request, _env: Env, _ctx: Context) -> Result<Response> {
    probe::handle(&req)
}

//! wasm32-unknown-emscripten 版の probe。口と処理は ../probe-shared/ (probe-unknown と共通)。

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

/// feature `ring` を外したときに /sign が返す理由 (Cargo.toml の [features] 参照)
#[cfg(not(feature = "ring"))]
const RING_UNSUPPORTED: &str = "does not build on wasm32-unknown-emscripten: ring 0.17.14 SystemRandom has no SecureRandom impl for target_os=emscripten (E0277 in jsonwebtoken 9.3.1 RS256 signing and in encrypt_secret)";
/// feature `pdf` を外したときに /pdf が返す理由 (Cargo.toml の [features] 参照)
#[cfg(not(feature = "pdf"))]
const PDF_UNSUPPORTED: &str = "does not build on wasm32-unknown-emscripten: printpdf 0.8.2 declares crate-type [\"cdylib\", \"rlib\"], and wasm-ld cannot link the dependency's cdylib (SIDE_MODULE) with -Crelocation-model=static (R_WASM_MEMORY_ADDR_SLEB ... recompile with -fPIC). 0.12.8 declares the same";

// emscripten は cdylib ではリンクできないので bin にする。handler は #[event(fetch)] が export する
fn main() {}

#[event(fetch)]
async fn fetch(req: Request, _env: Env, _ctx: Context) -> Result<Response> {
    probe::handle(&req)
}

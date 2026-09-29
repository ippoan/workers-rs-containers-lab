//! probe-unknown / probe-emscripten の共通のルーティング。両方の crate が `#[path]` でこのディレクトリの
//! モジュールを読むので、処理のコードはターゲット間で同じ (違うのは crate の入口と feature だけ)。
//!
//! - `GET /zip?mb=N[&mode=copy|stream]` (N = 1..=20): dtako の ZIP の展開 → SHIFT_JIS の decode → 運行NO で group (JSON)
//! - `GET /sign`: RS256 (jsonwebtoken / rsa) と AES-256-GCM (ring) (JSON)
//! - `GET /pdf`: printpdf で日本語 1 ページ (application/pdf。時間とメモリは応答ヘッダー)
//!
//! そのターゲットでビルドできない方式は feature (`ring` / `pdf`) で外し、`{"unsupported": "<理由>"}` を返す
//! (/pdf は 501)。ビルドできないこと自体が結果。
//!
//! JSON には `target` (ビルドターゲット) と、処理の前後の wasm の線形メモリ (`memory`) を入れる。
//! Cloudflare 上の Date.now は I/O まで進まないので、`*_ms` はローカル (wrangler dev) でだけ意味がある。
//! staging では bench/probe.mjs が測る応答時間と、dashboard の CPU 時間を見る。

use serde::Serialize;
use worker::{console_error, Date, Method, Request, Response, Result};

#[cfg(target_os = "emscripten")]
pub const TARGET: &str = "wasm32-unknown-emscripten";
#[cfg(not(target_os = "emscripten"))]
pub const TARGET: &str = "wasm32-unknown-unknown";

const MAX_MB: u32 = 20;

pub fn now_ms() -> u64 {
    Date::now().as_millis()
}

#[derive(Serialize)]
struct Report<'a, T: Serialize> {
    target: &'a str,
    /// Worker が動いた colo (request.cf.colo。取れなければ null)
    colo: Option<String>,
    #[serde(flatten)]
    body: T,
}

#[derive(Serialize)]
struct ErrorBody<'a> {
    target: &'a str,
    error: &'a str,
}

#[cfg(not(feature = "pdf"))]
#[derive(Serialize)]
struct Unsupported<'a> {
    target: &'a str,
    unsupported: &'a str,
}

pub fn error_json(error: &str, status: u16) -> Result<Response> {
    Ok(Response::from_json(&ErrorBody {
        target: TARGET,
        error,
    })?
    .with_status(status))
}

pub fn json<T: Serialize>(req: &Request, body: T) -> Result<Response> {
    Response::from_json(&Report {
        target: TARGET,
        colo: colo(req),
        body,
    })
}

fn colo(req: &Request) -> Option<String> {
    req.cf().map(|cf| cf.colo())
}

/// クエリの値 (url crate で解くと idna の表を bundle に引き込むので、文字列で見る)
pub fn query(req: &Request, key: &str) -> Option<String> {
    let url = req.inner().url();
    let (_, q) = url.split_once('?')?;
    q.split('&').find_map(|kv| {
        kv.strip_prefix(key)
            .and_then(|rest| rest.strip_prefix('='))
            .map(str::to_string)
    })
}

pub fn handle(req: &Request) -> Result<Response> {
    if req.method() != Method::Get {
        return error_json("not_found", 404);
    }
    match req.path().as_str() {
        "/zip" => {
            let Some(mb) = query(req, "mb")
                .and_then(|v| v.parse().ok())
                .filter(|mb| (1..=MAX_MB).contains(mb))
            else {
                return error_json("mb must be 1..=20", 400);
            };
            let Some(mode) =
                crate::zip_probe::Mode::parse(query(req, "mode").as_deref().unwrap_or("copy"))
            else {
                return error_json("mode must be copy or stream", 400);
            };
            match crate::zip_probe::run(mb, mode) {
                Ok(r) => json(req, r),
                Err(e) => {
                    console_error!("probe: zip: {e}");
                    error_json(&e, 500)
                }
            }
        }
        // 中断後に次のリクエストが回復するかを測るための口 (README の probe の節)
        "/_lab/panic" => panic!("lab: intentional panic"),
        "/sign" => match crate::sign::run() {
            Ok(r) => json(req, r),
            Err(e) => {
                console_error!("probe: sign: {e}");
                error_json(&e, 500)
            }
        },
        #[cfg(not(feature = "pdf"))]
        "/pdf" => Ok(Response::from_json(&Unsupported {
            target: TARGET,
            unsupported: crate::PDF_UNSUPPORTED,
        })?
        .with_status(501)),
        #[cfg(feature = "pdf")]
        "/pdf" => match crate::pdf::run() {
            Ok(r) => {
                let mut resp = Response::from_bytes(r.bytes.clone())?;
                let h = resp.headers_mut();
                h.set("content-type", "application/pdf")?;
                h.set("x-probe-target", TARGET)?;
                if let Some(c) = colo(req) {
                    h.set("x-probe-colo", &c)?;
                }
                h.set("x-probe-pdf-bytes", &r.bytes.len().to_string())?;
                h.set("x-probe-memory-before", &r.memory.before.to_string())?;
                h.set("x-probe-memory-after", &r.memory.after.to_string())?;
                h.set("x-probe-heap-peak", &r.memory.heap_peak.to_string())?;
                h.set(
                    "server-timing",
                    &format!(
                        "font;dur={}, render;dur={}, total;dur={}",
                        r.font_parse_ms, r.render_ms, r.total_ms
                    ),
                )?;
                Ok(resp)
            }
            Err(e) => {
                console_error!("probe: pdf: {e}");
                error_json(&e, 500)
            }
        },
        _ => error_json("not_found", 404),
    }
}

//! `GET /pdf`: printpdf (rust-alc-api `crates/alc-pdf` と同じ 0.8) で日本語を含む 1 ページの PDF を作る。
//!
//! フォントは rust-alc-api `crates/alc-pdf/src/render.rs:6` と同じ NotoSansJP-Regular.ttf (9.6 MB) を
//! `include_bytes!` で同梱し、要求ごとに `ParsedFont::from_bytes` で読む (render.rs と同じ)。
//! ライセンスは SIL OFL 1.1 (`fonts/OFL.txt`)。

use printpdf::{
    Mm, ParsedFont, PdfDocument, PdfPage, PdfSaveOptions, Point, Pt, TextShapingOptions,
};

use crate::mem::{Memory, Span};
use crate::now_ms;

const FONT_DATA: &[u8] = include_bytes!("fonts/NotoSansJP-Regular.ttf");

const LINES: [(f32, &str); 5] = [
    (14.0, "拘束時間管理表 (probe)"),
    (10.0, "運行NO 2609100007 乗務員 佐藤太郎"),
    (10.0, "出社 2026/09/01 06:12 退社 2026/09/01 17:48"),
    (10.0, "運転 7:30 荷役 1:05 休憩 1:10 拘束 11:36"),
    (8.0, "ひらがな・カタカナ・漢字・ABC 0123456789"),
];

pub struct PdfReport {
    pub bytes: Vec<u8>,
    pub font_parse_ms: u64,
    pub render_ms: u64,
    pub total_ms: u64,
    pub memory: Memory,
}

pub fn run() -> Result<PdfReport, String> {
    let span = Span::start();
    let t0 = now_ms();
    let mut doc = PdfDocument::new("probe");
    let mut warnings = Vec::new();
    let font = ParsedFont::from_bytes(FONT_DATA, 0, &mut warnings)
        .ok_or_else(|| "embedded font must parse".to_string())?;
    let font_id = doc.add_font(&font);
    let t1 = now_ms();

    let mut ops = Vec::new();
    let mut y = 280.0;
    for (size, text) in LINES {
        let options = TextShapingOptions::new(Pt(size));
        let shaped = doc
            .shape_text(text, &font_id, &options)
            .ok_or_else(|| format!("shape_text: {text}"))?;
        ops.extend(shaped.get_ops(Point::new(Mm(15.0), Mm(y))));
        y -= size * 0.8;
    }
    doc.with_pages(vec![PdfPage::new(Mm(210.0), Mm(297.0), ops)]);
    let bytes = doc.save(&PdfSaveOptions::default(), &mut warnings);
    let t2 = now_ms();
    Ok(PdfReport {
        bytes,
        font_parse_ms: t1 - t0,
        render_ms: t2 - t1,
        total_ms: t2 - t0,
        memory: span.finish(),
    })
}

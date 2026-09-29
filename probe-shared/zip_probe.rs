//! `GET /zip?mb=N`: dtako の ZIP 取り込み (rust-alc-api `crates/alc-csv-parser/src/lib.rs` の
//! `extract_zip` → `decode_shift_jis` → `group_csv_by_unko_no`) を worker の中で回す。
//!
//! 入力の ZIP は worker の中で決定的に作る (種は固定、実データは使わない): SHIFT_JIS の KUDGURI.csv
//! (運行ごとに 1 行) と KUDGIVT.csv (運行のイベント。ZIP が N MB になるまで足す)。
//! 生成と処理の時間を分け、処理で数えた行数・運行NO の数が生成時の値と一致するかも返す。

use std::cell::Cell;
use std::collections::HashMap;
use std::io::{Cursor, Read, Seek, SeekFrom, Write};
use std::rc::Rc;

use serde::Serialize;

use crate::mem::linear_memory_bytes;
use crate::now_ms;

/// 運行NO の数 (dtako の 1 か月ぶんの ZIP の規模)
const UNKO_COUNT: usize = 3000;
const SEED: u64 = 0x6474_616b_6f5f_7a69; // "dtako_zi"

const KUDGURI_HEADER: &str = "運行NO,読取日,運行日,事業所CD,事業所名,車輌CD,車輌名,乗務員CD1,乗務員名１,対象乗務員CD,対象乗務員名,対象乗務員区分,出社日時,退社日時,総走行距離";
const KUDGIVT_HEADER: &str = "運行NO,読取日,乗務員CD1,乗務員名１,対象乗務員区分,開始日時,終了日時,イベントCD,イベント名,区間時間,区間距離";
const OFFICES: [&str; 4] = ["本社営業所", "北営業所", "港湾センター", "第二倉庫"];
const SURNAMES: [&str; 8] = [
    "佐藤", "鈴木", "高橋", "田中", "伊藤", "渡辺", "山本", "中村",
];
const GIVEN: [&str; 8] = [
    "太郎", "花子", "健一", "美咲", "翔太", "由美", "大輔", "直子",
];
const EVENTS: [(u16, &str); 7] = [
    (101, "出社"),
    (201, "走行"),
    (202, "積み"),
    (203, "降し"),
    (301, "休憩"),
    (302, "休息"),
    (102, "退社"),
];

// ===== rust-alc-api crates/alc-csv-parser/src/lib.rs:11-49 の写し (依存としては引かない) =====

/// ZIP バイト列を展開し、(ファイル名, バイト列) のリストを返す
pub fn extract_zip(bytes: &[u8]) -> Result<Vec<(String, Vec<u8>)>, zip::result::ZipError> {
    let cursor = std::io::Cursor::new(bytes);
    let mut archive = zip::ZipArchive::new(cursor)?;
    let mut files = Vec::new();
    for i in 0..archive.len() {
        let mut file = archive.by_index(i)?;
        let name = file.name().to_string();
        let mut contents = Vec::new();
        file.read_to_end(&mut contents)?;
        files.push((name, contents));
    }
    Ok(files)
}

/// Shift-JIS バイト列を UTF-8 文字列に変換
pub fn decode_shift_jis(bytes: &[u8]) -> String {
    let (decoded, _, _) = encoding_rs::SHIFT_JIS.decode(bytes);
    decoded.into_owned()
}

/// 運行NOでCSVデータをグループ化
pub fn group_csv_by_unko_no(csv_text: &str) -> HashMap<String, Vec<String>> {
    let mut map: HashMap<String, Vec<String>> = HashMap::new();
    let mut lines = csv_text.lines();
    let _header = lines.next(); // skip header
    for line in lines {
        if line.trim().is_empty() {
            continue;
        }
        // 運行NO is always the first column
        if let Some(unko_no) = line.split(',').next() {
            map.entry(unko_no.to_string())
                .or_default()
                .push(line.to_string());
        }
    }
    map
}

// ===== 入力の生成 =====

/// xorshift64* (種を固定した決定的な擬似乱数。暗号用ではない)
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_f491_4f6c_dd1d)
    }
    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
}

/// 書いたバイト数を外から覗ける Cursor (ZipWriter は中の writer を返さないので、途中の ZIP の大きさをこれで見る)
struct Counting {
    inner: Cursor<Vec<u8>>,
    len: Rc<Cell<u64>>,
}

impl Write for Counting {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let n = self.inner.write(buf)?;
        self.len.set(self.len.get().max(self.inner.position()));
        Ok(n)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

impl Seek for Counting {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        self.inner.seek(pos)
    }
}

/// 生成時に数えた期待値 (ファイルごと)
#[derive(Serialize, Clone, Copy, Default)]
pub struct FileCount {
    pub csv_bytes: usize,
    pub rows: usize,
    pub groups: usize,
}

struct Generated {
    zip: Vec<u8>,
    kudguri: FileCount,
    kudgivt: FileCount,
}

fn unko_no(i: usize) -> String {
    format!("2609{:06}", 100_000 + i * 7)
}

fn driver(i: usize) -> (String, String) {
    (
        format!("D{:04}", 1000 + i % 400),
        format!(
            "{}{}",
            SURNAMES[i % SURNAMES.len()],
            GIVEN[(i / SURNAMES.len()) % GIVEN.len()]
        ),
    )
}

fn day(i: usize) -> usize {
    1 + i % 30
}

fn sjis(s: &str) -> Vec<u8> {
    encoding_rs::SHIFT_JIS.encode(s).0.into_owned()
}

fn generate(target_bytes: u64) -> Result<Generated, zip::result::ZipError> {
    let len = Rc::new(Cell::new(0));
    let mut zw = zip::ZipWriter::new(Counting {
        inner: Cursor::new(Vec::with_capacity(target_bytes as usize + 64 * 1024)),
        len: len.clone(),
    });
    let opts = zip::write::SimpleFileOptions::default()
        .compression_method(zip::CompressionMethod::Deflated);
    let mut rng = Rng(SEED);

    // KUDGURI: 運行ごとに 1 行
    let mut kudguri = FileCount {
        groups: UNKO_COUNT,
        rows: UNKO_COUNT,
        ..FileCount::default()
    };
    let mut text = String::with_capacity(UNKO_COUNT * 200);
    text.push_str(KUDGURI_HEADER);
    text.push('\n');
    for i in 0..UNKO_COUNT {
        let (dcd, dname) = driver(i);
        let d = day(i);
        let office = rng.below(OFFICES.len());
        let dep_h = 4 + rng.below(6);
        let ret_h = dep_h + 8 + rng.below(6);
        text.push_str(&format!(
            "{},2026/09/{d:02},2026/09/{d:02},{:02},{},V{:04},{}号車,{dcd},{dname},{dcd},{dname},1,2026/09/{d:02} {dep_h:02}:{:02}:00,2026/09/{d:02} {ret_h:02}:{:02}:00,{}.{}\n",
            unko_no(i),
            office + 1,
            OFFICES[office],
            2000 + i % 250,
            100 + i % 250,
            rng.below(60),
            rng.below(60),
            50 + rng.below(450),
            rng.below(10),
        ));
    }
    let bytes = sjis(&text);
    kudguri.csv_bytes = bytes.len();
    drop(text);
    zw.start_file("KUDGURI.csv", opts)?;
    zw.write_all(&bytes)?;
    drop(bytes);

    // KUDGIVT: ZIP が target_bytes に届くまでイベントを足す (64 KiB ずつ SJIS にして書く)
    let mut kudgivt = FileCount::default();
    let mut seen = vec![false; UNKO_COUNT];
    zw.start_file("KUDGIVT.csv", opts)?;
    let header = sjis(&format!("{KUDGIVT_HEADER}\n"));
    zw.write_all(&header)?;
    kudgivt.csv_bytes += header.len();
    let mut chunk = String::with_capacity(80 * 1024);
    while len.get() < target_bytes {
        chunk.clear();
        while chunk.len() < 64 * 1024 {
            let u = rng.below(UNKO_COUNT);
            if !seen[u] {
                seen[u] = true;
                kudgivt.groups += 1;
            }
            let (dcd, dname) = driver(u);
            let d = day(u);
            let (cd, name) = EVENTS[rng.below(EVENTS.len())];
            let h = rng.below(24);
            let m = rng.below(60);
            let dur = 1 + rng.below(240);
            chunk.push_str(&format!(
                "{},2026/09/{d:02},{dcd},{dname},1,2026/09/{d:02} {h:02}:{m:02}:00,2026/09/{d:02} {:02}:{:02}:00,{cd},{name},{dur},{}.{}\n",
                unko_no(u),
                (h + dur / 60) % 24,
                (m + dur) % 60,
                rng.below(120),
                rng.below(10),
            ));
            kudgivt.rows += 1;
        }
        let bytes = sjis(&chunk);
        zw.write_all(&bytes)?;
        kudgivt.csv_bytes += bytes.len();
    }
    let zip = zw.finish()?.inner.into_inner();
    Ok(Generated {
        zip,
        kudguri,
        kudgivt,
    })
}

// ===== 口 =====

#[derive(Serialize)]
pub struct Input {
    pub zip_bytes: usize,
    pub gen_ms: u64,
    #[serde(rename = "KUDGURI.csv")]
    pub kudguri: FileCount,
    #[serde(rename = "KUDGIVT.csv")]
    pub kudgivt: FileCount,
}

#[derive(Serialize)]
pub struct Processed {
    pub name: String,
    pub csv_bytes: usize,
    pub utf8_bytes: usize,
    pub rows: usize,
    pub groups: usize,
}

#[derive(Serialize)]
pub struct Timing {
    pub extract_ms: u64,
    pub decode_ms: u64,
    pub group_ms: u64,
    pub total_ms: u64,
}

#[derive(Serialize)]
pub struct ZipReport {
    pub mb: u32,
    pub input: Input,
    pub process: Timing,
    pub files: Vec<Processed>,
    /// 処理で数えた行数・運行NO の数が生成時の値と一致したか
    #[serde(rename = "match")]
    pub matched: bool,
    /// before: 生成の前 / after_gen: 生成の後 (ZIP を持った状態) / after: 処理の後
    pub memory: ZipMemory,
}

#[derive(Serialize)]
pub struct ZipMemory {
    pub before: usize,
    pub after_gen: usize,
    pub after: usize,
}

pub fn run(mb: u32) -> Result<ZipReport, String> {
    let before = linear_memory_bytes();
    let t0 = now_ms();
    let gen = generate(u64::from(mb) * 1024 * 1024).map_err(|e| format!("generate: {e}"))?;
    let gen_ms = now_ms() - t0;
    let after_gen = linear_memory_bytes();

    // rust-alc-api と同じく、全エントリを展開 → ファイルごとに decode → group
    let t1 = now_ms();
    let entries = extract_zip(&gen.zip).map_err(|e| format!("extract: {e}"))?;
    let t2 = now_ms();
    let mut decoded = Vec::with_capacity(entries.len());
    for (name, bytes) in &entries {
        decoded.push((name.clone(), bytes.len(), decode_shift_jis(bytes)));
    }
    let t3 = now_ms();
    let mut files = Vec::with_capacity(decoded.len());
    let mut groups = Vec::with_capacity(decoded.len());
    for (name, csv_bytes, text) in &decoded {
        let map = group_csv_by_unko_no(text);
        files.push(Processed {
            name: name.clone(),
            csv_bytes: *csv_bytes,
            utf8_bytes: text.len(),
            rows: map.values().map(Vec::len).sum(),
            groups: map.len(),
        });
        // 実処理は map を後段 (DB への書き込み) へ渡すので、全ファイルぶん持ったまま測る
        groups.push(map);
    }
    let t4 = now_ms();
    let after = linear_memory_bytes();
    drop(groups);

    let expect = |name: &str, c: &FileCount| {
        files.iter().any(|f| {
            f.name == name && f.rows == c.rows && f.groups == c.groups && f.csv_bytes == c.csv_bytes
        })
    };
    let matched = files.len() == 2
        && expect("KUDGURI.csv", &gen.kudguri)
        && expect("KUDGIVT.csv", &gen.kudgivt);

    Ok(ZipReport {
        mb,
        input: Input {
            zip_bytes: gen.zip.len(),
            gen_ms,
            kudguri: gen.kudguri,
            kudgivt: gen.kudgivt,
        },
        process: Timing {
            extract_ms: t2 - t1,
            decode_ms: t3 - t2,
            group_ms: t4 - t3,
            total_ms: t4 - t1,
        },
        files,
        matched,
        memory: ZipMemory {
            before,
            after_gen,
            after,
        },
    })
}

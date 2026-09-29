//! `GET /zip?mb=N&mode=flush` (wasm32-unknown-unknown だけ): dtako の ZIP を運行ごとに R2 へ書き出して捨てる版。
//!
//! rust-alc-api の `split_csv_from_r2` (`crates/alc-dtako/src/dtako_upload.rs`) は ZIP の全エントリを展開・decode・
//! group して、運行ごとの CSV を全部組み立ててから PUT するので、ヒープのピークが ZIP の 20〜27 倍になる
//! (copy / stream の実測)。ここでは
//!
//! 1. ZIP 本体は Vec で持つ
//! 2. エントリを 1 つずつ、**行単位で** 読んで SHIFT_JIS → UTF-8 にする (エントリ全体の文字列を作らない)
//! 3. 運行NO が変わったら、その運行の CSV (ヘッダー + 行) を R2 に PUT してバッファを手放す
//!    (同時に走らせる PUT は `MAX_INFLIGHT` まで。溜まったら 1 つ終わるのを待つ)
//!
//! 行が運行NO ごとに連続している前提 (rust-alc-api の fixture の並び)。入力は運行NO 順の生成器
//! (`zip_probe::generate_sorted`) で作る。連続していない運行が出たら `<CSV>.part<n>` として別に PUT し、
//! `runs` が `groups` を上回ることで分かるようにする (結合はしない)。
//!
//! 書き出したオブジェクトは、応答の前に prefix ごと消す (`cleanup`)。

use std::collections::HashSet;
use std::future::Future;
use std::io::{BufRead, BufReader, Cursor};
use std::pin::Pin;

use futures_util::stream::{FuturesUnordered, StreamExt};
use serde::Serialize;
use worker::{Bucket, Error};

use crate::mem::{Memory, Span};
use crate::now_ms;
use crate::zip_probe::{FileCount, Input, Processed};

/// 同時に走らせる PUT の上限
const MAX_INFLIGHT: usize = 8;
/// R2 の delete_multiple が 1 回で受ける鍵の数
const DELETE_BATCH: usize = 1000;

#[derive(Serialize)]
pub struct FileReport {
    #[serde(flatten)]
    pub processed: Processed,
    /// 連続した運行NO の塊の数 (行が運行NO ごとに連続していれば groups と同じ)
    pub runs: usize,
}

#[derive(Serialize, Default)]
pub struct Timing {
    /// 展開・decode・組み立て (PUT の完了待ちを除く)
    pub process_ms: u64,
    /// PUT の完了を待っていた時間 (上限に達したときの待ちと、最後の待ち)
    pub put_wait_ms: u64,
    pub total_ms: u64,
    pub cleanup_ms: u64,
}

#[derive(Serialize, Default)]
pub struct Puts {
    pub count: usize,
    pub bytes: usize,
    pub failed: usize,
    /// 同時に走っていた PUT の最大
    pub max_inflight: usize,
    /// 1 回の PUT の本文の最大 (バイト)
    pub max_object_bytes: usize,
    /// 最初に失敗した PUT のエラー
    #[serde(skip_serializing_if = "Option::is_none")]
    pub first_error: Option<String>,
}

#[derive(Serialize, Default)]
pub struct Cleanup {
    pub listed: usize,
    pub deleted: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Serialize)]
pub struct FlushMemory {
    pub gen: Memory,
    /// 処理の区間 (ZIP を持った状態から、最後の PUT が終わるまで)
    pub process: Memory,
}

#[derive(Serialize)]
pub struct FlushReport {
    pub mb: u32,
    pub mode: &'static str,
    pub input: Input,
    pub process: Timing,
    pub files: Vec<FileReport>,
    pub puts: Puts,
    pub cleanup: Cleanup,
    /// 行数・運行NO の数・CSV のバイト数が生成時と一致し、PUT が全部成功し、運行ごとに 1 つずつ PUT したか
    #[serde(rename = "match")]
    pub matched: bool,
    pub memory: FlushMemory,
}

type PutFuture<'a> = Pin<Box<dyn Future<Output = Result<(), Error>> + 'a>>;

/// 走っている PUT (本文は JS 側へ渡した時点で wasm のヒープから消える)
struct Uploader<'a> {
    bucket: &'a Bucket,
    inflight: FuturesUnordered<PutFuture<'a>>,
    puts: Puts,
    wait_ms: u64,
}

impl<'a> Uploader<'a> {
    async fn put(&mut self, key: String, body: Vec<u8>) {
        while self.inflight.len() >= MAX_INFLIGHT {
            self.wait_one().await;
        }
        self.puts.count += 1;
        self.puts.bytes += body.len();
        self.puts.max_object_bytes = self.puts.max_object_bytes.max(body.len());
        let bucket = self.bucket;
        self.inflight.push(Box::pin(async move {
            bucket.put(key, body).execute().await.map(|_| ())
        }));
        self.puts.max_inflight = self.puts.max_inflight.max(self.inflight.len());
    }

    async fn wait_one(&mut self) {
        let t = now_ms();
        if let Some(Err(e)) = self.inflight.next().await {
            self.puts.failed += 1;
            self.puts.first_error.get_or_insert_with(|| e.to_string());
        }
        self.wait_ms += now_ms() - t;
    }

    async fn drain(&mut self) {
        while !self.inflight.is_empty() {
            self.wait_one().await;
        }
    }
}

/// 1 運行ぶんの組み立て中の CSV
struct Current {
    unko_no: String,
    body: String,
}

/// 1 エントリを行単位で読み、運行NO が変わるたびに PUT する
async fn flush_entry<R: std::io::Read>(
    reader: R,
    name: &str,
    prefix: &str,
    up: &mut Uploader<'_>,
) -> Result<FileReport, String> {
    let csv_name = name
        .rsplit('/')
        .next()
        .unwrap_or(name)
        .to_uppercase()
        .replace(".CSV", ".csv");
    let mut reader = BufReader::with_capacity(64 * 1024, reader);
    let mut raw = Vec::with_capacity(512);
    let mut csv_bytes = 0usize;
    let mut utf8_bytes = 0usize;
    let mut rows = 0usize;
    let mut runs = 0usize;
    let mut seen: HashSet<String> = HashSet::new();
    let mut header: Option<String> = None;
    let mut cur: Option<Current> = None;

    loop {
        raw.clear();
        let n = reader
            .read_until(b'\n', &mut raw)
            .map_err(|e| format!("extract: {e}"))?;
        if n == 0 {
            break;
        }
        csv_bytes += n;
        // SHIFT_JIS の 2 バイト目は 0x40 以上なので、0x0A で切っても文字は割れない
        let (text, _, _) = encoding_rs::SHIFT_JIS.decode(&raw);
        utf8_bytes += text.len();
        // str::lines と同じく末尾の \n と \r を落とす
        let line = text.strip_suffix('\n').unwrap_or(&text);
        let line = line.strip_suffix('\r').unwrap_or(line);
        let Some(h) = &header else {
            header = Some(line.to_string());
            continue;
        };
        if line.trim().is_empty() {
            continue;
        }
        let unko_no = line.split(',').next().unwrap_or_default();
        if cur.as_ref().is_none_or(|c| c.unko_no != unko_no) {
            if let Some(done) = cur.take() {
                put_run(done, &csv_name, prefix, runs, &mut seen, up).await;
            }
            runs += 1;
            let mut body = String::with_capacity(h.len() + 64 * 1024);
            body.push_str(h);
            body.push('\n');
            cur = Some(Current {
                unko_no: unko_no.to_string(),
                body,
            });
        }
        let c = cur.as_mut().expect("set above");
        c.body.push_str(line);
        c.body.push('\n');
        rows += 1;
    }
    if let Some(done) = cur.take() {
        put_run(done, &csv_name, prefix, runs, &mut seen, up).await;
    }
    Ok(FileReport {
        processed: Processed {
            name: name.to_string(),
            csv_bytes,
            utf8_bytes,
            rows,
            groups: seen.len(),
        },
        runs,
    })
}

/// 1 塊を PUT する。その運行が前にも出ていたら (行が連続していない) `.part<run>` を付けて別の鍵にする
async fn put_run(
    run: Current,
    csv_name: &str,
    prefix: &str,
    run_no: usize,
    seen: &mut HashSet<String>,
    up: &mut Uploader<'_>,
) {
    let mut key = format!("{prefix}unko/{}/{csv_name}", run.unko_no);
    if !seen.insert(run.unko_no) {
        key.push_str(&format!(".part{run_no}"));
    }
    up.put(key, run.body.into_bytes()).await;
}

/// prefix の下を全部消す (list は 1 回 1000 件まで、delete_multiple も 1000 件まで)
async fn cleanup(bucket: &Bucket, prefix: &str) -> Cleanup {
    let mut out = Cleanup::default();
    loop {
        let listed = match bucket.list().prefix(prefix).execute().await {
            Ok(l) => l,
            Err(e) => {
                out.error = Some(format!("list: {e}"));
                return out;
            }
        };
        let keys: Vec<String> = listed.objects().iter().map(|o| o.key()).collect();
        if keys.is_empty() {
            return out;
        }
        out.listed += keys.len();
        for batch in keys.chunks(DELETE_BATCH) {
            if let Err(e) = bucket.delete_multiple(batch.to_vec()).await {
                out.error = Some(format!("delete: {e}"));
                return out;
            }
            out.deleted += batch.len();
        }
        // 消した分は次の list に出ないので、cursor を使わず先頭から取り直す
    }
}

/// リクエストごとの prefix (同時に叩かれても混ざらないよう、時刻と乱数を入れる)
fn prefix() -> String {
    let mut r = [0u8; 8];
    // 取れなくても時刻だけで動かす (probe の同時実行は稀)
    let _ = getrandom::getrandom(&mut r);
    format!("probe-flush/{}-{:016x}/", now_ms(), u64::from_le_bytes(r))
}

pub async fn run(bucket: &Bucket, mb: u32) -> Result<FlushReport, String> {
    let span = Span::start();
    let t0 = now_ms();
    let gen = crate::zip_probe::generate_sorted(u64::from(mb) * 1024 * 1024)
        .map_err(|e| format!("generate: {e}"))?;
    let gen_ms = now_ms() - t0;
    let gen_mem = span.finish();

    let prefix = prefix();
    let mut up = Uploader {
        bucket,
        inflight: FuturesUnordered::new(),
        puts: Puts::default(),
        wait_ms: 0,
    };
    let span = Span::start();
    let t1 = now_ms();
    let result = process(&gen.zip, &prefix, &mut up).await;
    up.drain().await;
    let t2 = now_ms();
    let process_mem = span.finish();

    let cleanup = cleanup(bucket, &prefix).await;
    let cleanup_ms = now_ms() - t2;
    let files = result?;

    let expect = |name: &str, c: &FileCount| {
        files.iter().any(|f| {
            let p = &f.processed;
            p.name == name
                && p.rows == c.rows
                && p.groups == c.groups
                && p.csv_bytes == c.csv_bytes
                && f.runs == p.groups
        })
    };
    let groups: usize = files.iter().map(|f| f.processed.groups).sum();
    let matched = files.len() == 2
        && expect("KUDGURI.csv", &gen.kudguri)
        && expect("KUDGIVT.csv", &gen.kudgivt)
        && up.puts.failed == 0
        && up.puts.count == groups;

    let total_ms = t2 - t1;
    Ok(FlushReport {
        mb,
        mode: "flush",
        input: Input {
            zip_bytes: gen.zip.len(),
            gen_ms,
            kudguri: gen.kudguri,
            kudgivt: gen.kudgivt,
        },
        process: Timing {
            process_ms: total_ms - up.wait_ms,
            put_wait_ms: up.wait_ms,
            total_ms,
            cleanup_ms,
        },
        files,
        puts: up.puts,
        cleanup,
        matched,
        memory: FlushMemory {
            gen: gen_mem,
            process: process_mem,
        },
    })
}

async fn process(
    zip: &[u8],
    prefix: &str,
    up: &mut Uploader<'_>,
) -> Result<Vec<FileReport>, String> {
    let mut archive =
        zip::ZipArchive::new(Cursor::new(zip)).map_err(|e| format!("extract: {e}"))?;
    let mut files = Vec::with_capacity(archive.len());
    for i in 0..archive.len() {
        let file = archive.by_index(i).map_err(|e| format!("extract: {e}"))?;
        let name = file.name().to_string();
        if !name.to_lowercase().ends_with(".csv") {
            continue;
        }
        files.push(flush_entry(file, &name, prefix, up).await?);
    }
    Ok(files)
}

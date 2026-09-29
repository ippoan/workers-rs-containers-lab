//! DB を持つ Durable Object `LabDb`。Container (../container: postgres + PgBouncer transaction mode)
//! を 1 個抱え、Worker から `Stub::connect` で届いた TCP を Container の PgBouncer
//! (`getTcpPort(6432)`) へ素通しで中継する。SQL・トランザクションはすべて Worker 側にあり、
//! ここはバイトを運ぶだけ。
//!
//! - Container が止まっていれば起動し、PgBouncer が応答するまで待つ ([`STARTUP_TIMEOUT_MS`])。
//!   `getTcpPort().connect()` は listen していないポートでも「開いた」ことになるので、準備完了は
//!   **postgres の StartupMessage を送って最初の応答が返ったか**で判定する (返らなければ繋ぎ直して
//!   同じ StartupMessage を送り直す)。PgBouncer は表の投入が終わってから起動するので、
//!   応答が返った時点で DB は準備済み
//! - 常時起動にはしない: 最後の接続が閉じてから [`SLEEP_AFTER_MS`] 経つと alarm で Container を
//!   止める (`@cloudflare/containers` の既定 sleepAfter と同じ 10 分)。ディスクは揮発なので、
//!   次の起動は空の DB から作り直す (= cold start の計測はこの後に回す)
//! - HTTP の口は `GET /where` だけ: この DO が動いている colo を text で返す (Container との距離を見るため。
//!   `cdn-cgi/trace` を初回に 1 回だけ引き、以後はメモリの値を返す)

use std::cell::{Cell, RefCell};
use std::pin::pin;
use std::time::Duration;

use futures_util::future::{select, Either};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use wasm_bindgen::prelude::*;
use worker::{
    console_error, console_log, durable_object, Container, Date, Delay, Env, Fetch, Request,
    Response, Result, Socket, State,
};

use crate::db::PGBOUNCER_PORT;

const SLEEP_AFTER_MS: u64 = 10 * 60 * 1000;
const STARTUP_TIMEOUT_MS: u64 = 90 * 1000;
/// 1 回の試行 (接続 → StartupMessage → 最初の応答) の上限
const ATTEMPT_TIMEOUT_MS: u64 = 5 * 1000;
const RETRY_INTERVAL_MS: u64 = 250;

// workers-rs の `Fetcher` は `connect` を持たないので、JS の `Fetcher.connect()` を直接呼ぶ
#[wasm_bindgen]
extern "C" {
    type TcpPort;

    #[wasm_bindgen(method, catch)]
    fn connect(
        this: &TcpPort,
        address: &str,
    ) -> std::result::Result<worker::worker_sys::Socket, JsValue>;
}

#[durable_object]
pub struct LabDb {
    state: State,
    /// 中継中の接続数
    active: Cell<u32>,
    /// この DO が動いている colo (初回の `GET /where` で `cdn-cgi/trace` から取る)
    colo: RefCell<Option<String>>,
}

/// `cdn-cgi/trace` の `colo=` を引く (この isolate が動いている Cloudflare のデータセンター)
async fn trace_colo() -> Result<String> {
    // Fetch::Url (Rust の url crate でパース) は idna の表を bundle に引き込むので、JS の Request で組む
    let req = Request::new("https://cloudflare.com/cdn-cgi/trace", worker::Method::Get)?;
    let body = Fetch::Request(req).send().await?.text().await?;
    body.lines()
        .find_map(|l| l.strip_prefix("colo="))
        .map(str::to_owned)
        .ok_or_else(|| "cdn-cgi/trace has no colo=".into())
}

/// 1 回の試行: PgBouncer へ繋ぎ、クライアントの最初のメッセージを送り、最初の応答を読む。
/// 開いたソケットは `slot` に置く (失敗・時間切れのときに呼び出し側が閉じられるように)
async fn try_upstream(
    container: &Container,
    first: &[u8],
    slot: &mut Option<Socket>,
) -> std::result::Result<Vec<u8>, String> {
    let port: TcpPort = JsValue::from(
        container
            .get_tcp_port(PGBOUNCER_PORT)
            .map_err(|e| e.to_string())?,
    )
    .unchecked_into();
    let raw = port
        .connect(&format!("10.0.0.1:{PGBOUNCER_PORT}"))
        .map_err(|e| format!("{e:?}"))?;
    let upstream = slot.insert(Socket::from(raw));
    upstream.write_all(first).await.map_err(|e| e.to_string())?;
    upstream.flush().await.map_err(|e| e.to_string())?;
    let mut reply = vec![0u8; 16 * 1024];
    let n = upstream.read(&mut reply).await.map_err(|e| e.to_string())?;
    if n == 0 {
        return Err("closed before first reply".into());
    }
    reply.truncate(n);
    Ok(reply)
}

/// `worker::Socket` は drop しても閉じない (PgBouncer 側にクライアント接続が残り、
/// max_client_conn を食い潰す — staging で実測) ので、必ず明示的に閉じる
async fn close(socket: Option<Socket>) {
    if let Some(mut s) = socket {
        let _ = s.close().await;
    }
}

impl LabDb {
    fn container(&self) -> Result<Container> {
        self.state
            .container()
            .ok_or_else(|| "LabDb has no container (wrangler.toml [[containers]])".into())
    }

    fn ensure_started(&self, container: &Container) -> Result<()> {
        if container.running() {
            return Ok(());
        }
        container.start(None)?;
        console_log!("lab-db: container starting");
        // 落ちたときの理由をログに残す (start.sh の失敗など)
        let watched = self.container()?;
        wasm_bindgen_futures::spawn_local(async move {
            match watched.wait_for_exit().await {
                Ok(()) => console_log!("lab-db: container exited"),
                Err(e) => console_error!("lab-db: container exited: {e}"),
            }
        });
        Ok(())
    }

    /// PgBouncer が応答するまで繋ぎ直す (Container の起動直後は initdb と表の投入の分だけ返らない)
    async fn open_upstream(
        &self,
        container: &Container,
        first: &[u8],
    ) -> Result<(Socket, Vec<u8>)> {
        let started = Date::now().as_millis();
        let mut attempts = 0u32;
        loop {
            attempts += 1;
            // 起動途中で落ちた場合も含めて、止まっていれば (再) 起動する
            self.ensure_started(container)?;
            let mut slot = None;
            let outcome = {
                let attempt = pin!(try_upstream(container, first, &mut slot));
                let timeout = pin!(Delay::from(Duration::from_millis(ATTEMPT_TIMEOUT_MS)));
                match select(attempt, timeout).await {
                    Either::Left((r, _)) => r,
                    Either::Right(_) => Err(format!("no reply in {ATTEMPT_TIMEOUT_MS}ms")),
                }
            };
            let last_err = match outcome {
                Ok(reply) => {
                    let waited = Date::now().as_millis() - started;
                    if attempts > 1 {
                        console_log!(
                            "lab-db: pgbouncer ready after {waited}ms ({attempts} attempts)"
                        );
                    }
                    let upstream = slot.expect("try_upstream sets the socket before replying");
                    return Ok((upstream, reply));
                }
                Err(e) => {
                    close(slot).await;
                    e
                }
            };
            if Date::now().as_millis() - started > STARTUP_TIMEOUT_MS {
                return Err(format!(
                    "pgbouncer not ready in {STARTUP_TIMEOUT_MS}ms ({attempts} attempts): {last_err}"
                )
                .into());
            }
            Delay::from(Duration::from_millis(RETRY_INTERVAL_MS)).await;
        }
    }

    async fn relay(&self, client: &mut Socket) -> Result<()> {
        let container = self.container()?;
        // tokio-postgres は StartupMessage を 1 回で書き、サーバーの応答を待つ (NoTls なので
        // SSLRequest は無い)。これを持っておき、PgBouncer の準備ができるまで送り直す
        let mut first = vec![0u8; 16 * 1024];
        let n = client.read(&mut first).await?;
        if n == 0 {
            return Ok(());
        }
        first.truncate(n);
        let (mut upstream, reply) = self.open_upstream(&container, &first).await?;
        if client.write_all(&reply).await.is_ok() {
            // Worker は応答を返すと (Terminate を送らずに) ソケットを切ることがあり、そのときは
            // "client disconnected" で終わる。どちらの終わり方でも PgBouncer 側を閉じれば足りる
            let _ = tokio::io::copy_bidirectional(client, &mut upstream).await;
        }
        close(Some(upstream)).await;
        Ok(())
    }
}

impl worker::DurableObject for LabDb {
    fn new(state: State, _env: Env) -> Self {
        Self {
            state,
            active: Cell::new(0),
            colo: RefCell::new(None),
        }
    }

    async fn fetch(&self, req: Request) -> Result<Response> {
        if req.path() != "/where" {
            return Response::error("LabDb accepts TCP and GET /where only", 404);
        }
        let cached = self.colo.borrow().clone();
        let colo = match cached {
            Some(c) => c,
            None => {
                let c = trace_colo().await?;
                *self.colo.borrow_mut() = Some(c.clone());
                c
            }
        };
        // 本文は colo の 3 文字だけ (Worker 側で JSON を解かずに済ませ、bundle を増やさない)
        Response::ok(colo)
    }

    async fn connect(&self, mut socket: Socket) -> Result<()> {
        self.active.set(self.active.get() + 1);
        let result = self.relay(&mut socket).await;
        self.active.set(self.active.get() - 1);
        // 接続が閉じるたびに「今から 10 分後」へ張り直す = alarm が鳴るのは最後の close から 10 分後
        self.state
            .storage()
            .set_alarm(Duration::from_millis(SLEEP_AFTER_MS))
            .await?;
        if let Err(e) = &result {
            console_error!("lab-db: {e}");
        }
        result
    }

    async fn alarm(&self) -> Result<Response> {
        let container = self.container()?;
        // 中継中の接続は DO をメモリに留めるので、退避されて数え直しになった場合は 0 で正しい
        let active = self.active.get();
        if active > 0 {
            console_log!("lab-db: {active} connection(s) still open, keeping container");
            self.state
                .storage()
                .set_alarm(Duration::from_millis(SLEEP_AFTER_MS))
                .await?;
        } else if container.running() {
            console_log!("lab-db: idle for {SLEEP_AFTER_MS}ms, stopping container");
            container.destroy(None).await?;
        } else {
            console_log!("lab-db: idle, container already stopped");
        }
        Response::ok("")
    }
}

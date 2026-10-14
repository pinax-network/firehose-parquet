//! A loopback HyperLiquid `/info` for the real `fireparq hypercore-reference`
//! (`hypercore_reference_cli.rs`, `hypercore_reference_s3.rs`). It answers
//! through the reference crate's [`FakeInfo`], from the recorded fixtures of
//! `reference/tests/fixtures/`, over plain HTTP on `127.0.0.1`: the job takes
//! plain HTTP only for a loopback host. A test changes the answers with
//! `fake.edit`, for example a later outcomeMeta or a run of 503s.
//!
//! The server runs on its own thread and runtime, so a synchronous test and
//! a scenario shared by several tests can use it; it stops when dropped.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use fireparq_reference::testing::FakeInfo;
use http_body_util::{BodyExt, Full};
use hyper::{body::Incoming, Request, Response};

pub struct InfoServer {
    /// `http://127.0.0.1:<port>`: any path is the same `/info`.
    pub endpoint: String,
    pub fake: Arc<FakeInfo>,
    requests: Arc<AtomicUsize>,
    stop: Option<tokio::sync::oneshot::Sender<()>>,
}

impl InfoServer {
    pub fn start() -> Self {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let fake = Arc::new(FakeInfo::new());
        let requests = Arc::new(AtomicUsize::new(0));
        let (stop, mut stopped) = tokio::sync::oneshot::channel::<()>();
        let (served, counted) = (fake.clone(), requests.clone());
        std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            runtime.block_on(async move {
                let listener = tokio::net::TcpListener::from_std(listener).unwrap();
                loop {
                    let socket = tokio::select! {
                        _ = &mut stopped => return,
                        accepted = listener.accept() => match accepted {
                            Ok((socket, _)) => socket,
                            Err(_) => continue,
                        },
                    };
                    let (fake, counted) = (served.clone(), counted.clone());
                    tokio::spawn(async move {
                        let service =
                            hyper::service::service_fn(move |request: Request<Incoming>| {
                                let (fake, counted) = (fake.clone(), counted.clone());
                                async move {
                                    counted.fetch_add(1, Ordering::SeqCst);
                                    let body = match request.into_body().collect().await {
                                        Ok(body) => body.to_bytes(),
                                        Err(_) => Bytes::new(),
                                    };
                                    let (status, body) = fake.answer(&body);
                                    Response::builder()
                                        .status(status)
                                        .header("content-type", "application/json")
                                        .body(Full::new(Bytes::from(body)))
                                }
                            });
                        let _ = hyper::server::conn::http1::Builder::new()
                            .serve_connection(hyper_util::rt::TokioIo::new(socket), service)
                            .await;
                    });
                }
            });
        });
        Self {
            endpoint,
            fake,
            requests,
            stop: Some(stop),
        }
    }

    /// Every HTTP request received, whatever its body.
    pub fn requests(&self) -> usize {
        self.requests.load(Ordering::SeqCst)
    }
}

impl Drop for InfoServer {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
    }
}

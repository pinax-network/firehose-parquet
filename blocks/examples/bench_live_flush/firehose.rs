//! Loopback Firehose that replays a fixture in a loop. Block `n` carries the
//! payload of fixture block `(n - base) % len` with rewritten metadata: number,
//! synthetic id/parent id, LIB and a timestamp on the fixture's own block
//! spacing. The payload bytes are not rewritten (the mapper takes the canonical
//! identity columns from the metadata). With a rate, block `start + k` of a
//! stream is released no earlier than `request + k / rate`.
use crate::fixture::{metadata, timestamp_ns, Fixture};
use firehose_protos::firehose;
use sha2::{Digest, Sha256};
use std::{
    pin::Pin,
    sync::{Arc, Mutex},
    time::{Duration, Instant, SystemTime},
};
use tonic::codegen::{http, BoxFuture, Service};

pub struct Timeline {
    blocks: Vec<firehose::Response>,
    base: u64,
    first_ns: i128,
    interval_ns: u64,
    final_only: bool,
    lib_lag: u64,
}

impl Timeline {
    pub fn new(fixture: &Fixture, final_only: bool, lib_lag: u64, noon_utc: bool) -> Self {
        let first = metadata(&fixture.blocks[0]);
        let mut first_ns = timestamp_ns(first);
        if noon_utc {
            // Rebase to 12:00 UTC of the first block's day, so runs of a few
            // hours of chain time never cross a daily partition boundary.
            let day = first_ns.div_euclid(86_400_000_000_000);
            first_ns = day * 86_400_000_000_000 + 43_200_000_000_000;
        }
        Self {
            base: first.num,
            first_ns,
            interval_ns: fixture.block_interval_ns(),
            blocks: fixture.blocks.clone(),
            final_only,
            lib_lag,
        }
    }

    pub fn base(&self) -> u64 {
        self.base
    }

    fn id(number: u64) -> String {
        let digest = Sha256::digest(number.to_be_bytes());
        digest.iter().map(|byte| format!("{byte:02x}")).collect()
    }

    fn metadata(&self, number: u64) -> firehose::BlockMetadata {
        let offset = number - self.base;
        let time = self.first_ns + offset as i128 * self.interval_ns as i128;
        firehose::BlockMetadata {
            num: number,
            id: Self::id(number),
            parent_num: number.saturating_sub(1),
            parent_id: Self::id(number.saturating_sub(1)),
            lib_num: if self.final_only {
                number
            } else {
                number.saturating_sub(self.lib_lag)
            },
            time: Some(prost_types::Timestamp {
                seconds: time.div_euclid(1_000_000_000) as i64,
                nanos: time.rem_euclid(1_000_000_000) as i32,
            }),
            ..Default::default()
        }
    }

    pub fn response(&self, number: u64) -> firehose::Response {
        let source = &self.blocks[((number - self.base) % self.blocks.len() as u64) as usize];
        firehose::Response {
            block: source.block.clone(),
            step: if self.final_only { 3 } else { 1 },
            cursor: format!("bench-{number}"),
            metadata: Some(self.metadata(number)),
        }
    }
}

/// When a stream was requested and from which block, for lag accounting.
#[derive(Clone, Debug)]
pub struct StreamStart {
    pub requested: SystemTime,
    pub start: u64,
}

#[derive(Clone)]
pub struct Mock {
    pub timeline: Arc<Timeline>,
    pub info: firehose::InfoResponse,
    /// Blocks per second; zero releases blocks as fast as they are read.
    pub rate: f64,
    pub streams: Arc<Mutex<Vec<StreamStart>>>,
}

#[derive(Clone)]
struct Info(Mock);
impl tonic::server::UnaryService<firehose::InfoRequest> for Info {
    type Response = firehose::InfoResponse;
    type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
    fn call(&mut self, _: tonic::Request<firehose::InfoRequest>) -> Self::Future {
        let mut info = self.0.info.clone();
        info.first_streamable_block_num = self.0.timeline.base;
        info.first_streamable_block_id = Timeline::id(self.0.timeline.base);
        Box::pin(async move { Ok(tonic::Response::new(info)) })
    }
}

#[derive(Clone)]
struct Fetch(Mock);
impl tonic::server::UnaryService<firehose::SingleBlockRequest> for Fetch {
    type Response = firehose::SingleBlockResponse;
    type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
    fn call(&mut self, request: tonic::Request<firehose::SingleBlockRequest>) -> Self::Future {
        let timeline = self.0.timeline.clone();
        Box::pin(async move {
            let Some(firehose::single_block_request::Reference::BlockNumber(number)) =
                request.into_inner().reference
            else {
                return Err(tonic::Status::unimplemented("only block-number fetches"));
            };
            if number.num < timeline.base {
                return Err(tonic::Status::not_found("below the replay base"));
            }
            let mut metadata = timeline.metadata(number.num);
            metadata.lib_num = number.num;
            Ok(tonic::Response::new(firehose::SingleBlockResponse {
                metadata: Some(metadata),
                block: None,
            }))
        })
    }
}

#[derive(Clone)]
struct Stream(Mock);
impl tonic::server::ServerStreamingService<firehose::Request> for Stream {
    type Response = firehose::Response;
    type ResponseStream =
        Pin<Box<dyn futures::Stream<Item = Result<Self::Response, tonic::Status>> + Send>>;
    type Future = BoxFuture<tonic::Response<Self::ResponseStream>, tonic::Status>;
    fn call(&mut self, request: tonic::Request<firehose::Request>) -> Self::Future {
        let mock = self.0.clone();
        let request = request.into_inner();
        Box::pin(async move {
            let start = match request.cursor.strip_prefix("bench-") {
                Some(number) => number
                    .parse::<u64>()
                    .map_err(|_| tonic::Status::invalid_argument("unknown cursor"))?
                    .saturating_add(1),
                None if request.cursor.is_empty() => {
                    (request.start_block_num.max(0) as u64).max(mock.timeline.base)
                }
                None => return Err(tonic::Status::invalid_argument("unknown cursor")),
            };
            let stop = if request.stop_block_num == 0 {
                u64::MAX
            } else {
                request.stop_block_num
            };
            let requested = SystemTime::now();
            mock.streams
                .lock()
                .unwrap()
                .push(StreamStart { requested, start });
            let origin = Instant::now();
            let rate = mock.rate;
            let timeline = mock.timeline.clone();
            let stream = futures::stream::unfold(start, move |next| {
                let timeline = timeline.clone();
                async move {
                    if next > stop {
                        return None;
                    }
                    if rate > 0.0 {
                        let due = origin + Duration::from_secs_f64((next - start) as f64 / rate);
                        tokio::time::sleep_until(due.into()).await;
                    }
                    Some((Ok(timeline.response(next)), next + 1))
                }
            });
            let stream: Self::ResponseStream = Box::pin(stream);
            Ok(tonic::Response::new(stream))
        })
    }
}

macro_rules! service {
    ($type:ty, $name:literal, $method:ident) => {
        impl tonic::server::NamedService for $type {
            const NAME: &'static str = $name;
        }
        impl Service<http::Request<tonic::body::Body>> for $type {
            type Response = http::Response<tonic::body::Body>;
            type Error = std::convert::Infallible;
            type Future = BoxFuture<Self::Response, Self::Error>;
            fn poll_ready(
                &mut self,
                _: &mut std::task::Context<'_>,
            ) -> std::task::Poll<Result<(), Self::Error>> {
                std::task::Poll::Ready(Ok(()))
            }
            fn call(&mut self, request: http::Request<tonic::body::Body>) -> Self::Future {
                let service = self.clone();
                Box::pin(async move {
                    let mut grpc = tonic::server::Grpc::new(tonic_prost::ProstCodec::default())
                        .max_encoding_message_size(usize::MAX);
                    Ok(grpc.$method(service, request).await)
                })
            }
        }
    };
}
service!(Info, "sf.firehose.v2.EndpointInfo", unary);
service!(Fetch, "sf.firehose.v2.Fetch", unary);
service!(Stream, "sf.firehose.v2.Stream", server_streaming);

pub struct Server {
    pub endpoint: String,
    task: tokio::task::JoinHandle<()>,
}

impl Server {
    pub async fn start(mock: Mock) -> anyhow::Result<Self> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("http://{}", listener.local_addr()?);
        let incoming = futures::stream::unfold(listener, |listener| async {
            Some((listener.accept().await.map(|(socket, _)| socket), listener))
        });
        let task = tokio::spawn(async move {
            let _ = tonic::transport::Server::builder()
                .add_service(Info(mock.clone()))
                .add_service(Fetch(mock.clone()))
                .add_service(Stream(mock))
                .serve_with_incoming(incoming)
                .await;
        });
        Ok(Self { endpoint, task })
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}

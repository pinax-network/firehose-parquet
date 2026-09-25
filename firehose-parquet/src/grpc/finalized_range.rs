//! Bounded, untransformed finalized metadata traversal for exact index building.
use super::{checked_block_identity, unless_shutdown, CancellationToken, FirehoseClient};
use crate::traits::BlockIdentity;
use anyhow::{ensure, Context, Result};
use firehose_protos::firehose;
use std::time::Duration;

pub struct FinalizedMetadataStream {
    stream: tonic::Streaming<firehose::Response>,
    start: u64,
    end_inclusive: u64,
    finished: bool,
    timeout: Duration,
    shutdown: CancellationToken,
}

impl FirehoseClient {
    /// Open a finite final-only request. `end_inclusive=0` is unbounded on the
    /// wire, so the reader itself also enforces the range and stops at its end.
    /// The connection is shared with finality and parent Fetch requests.
    pub async fn finalized_metadata_stream(
        &self,
        start: u64,
        end_inclusive: u64,
        timeout: Duration,
        shutdown: &CancellationToken,
    ) -> Result<FinalizedMetadataStream> {
        ensure!(start <= end_inclusive, "finalized traversal range is empty");
        let signed_start =
            i64::try_from(start).context("finalized traversal start exceeds signed range")?;
        ensure!(
            end_inclusive <= i64::MAX as u64,
            "finalized traversal end exceeds signed range"
        );
        let stream = unless_shutdown(
            shutdown,
            tokio::time::timeout(timeout, async {
                let channel = self.fetch_channel().await?;
                let mut client = self.stream_client(channel);
                let request = tonic::Request::new(firehose::Request {
                    start_block_num: signed_start,
                    stop_block_num: end_inclusive,
                    final_blocks_only: true,
                    ..Default::default()
                });
                Ok::<_, anyhow::Error>(client.blocks(request).await?.into_inner())
            }),
        )
        .await?
        .context("finalized traversal connection/header deadline exceeded")??;
        Ok(FinalizedMetadataStream {
            stream,
            start,
            end_inclusive,
            finished: false,
            timeout,
            shutdown: shutdown.clone(),
        })
    }
}

impl FinalizedMetadataStream {
    pub async fn next(&mut self) -> Result<Option<BlockIdentity>> {
        if self.finished {
            return Ok(None);
        }
        let response = unless_shutdown(
            &self.shutdown,
            tokio::time::timeout(self.timeout, self.stream.message()),
        )
        .await?
        .context("finalized traversal message deadline exceeded")??;
        let Some(response) = response else {
            self.finished = true;
            return Ok(None);
        };
        ensure!(
            response.step == 3,
            "partition traversal received a non-final block"
        );
        ensure!(
            response.block.is_some(),
            "partition traversal response has no block payload"
        );
        let metadata = response
            .metadata
            .as_ref()
            .context("partition traversal response has no metadata")?;
        let identity = checked_block_identity(metadata, None)?;
        ensure!(
            !identity.block_id.is_empty(),
            "partition traversal response has an empty block identity"
        );
        ensure!(
            self.start <= identity.block_num && identity.block_num <= self.end_inclusive,
            "partition traversal response lies outside its requested range"
        );
        if identity.block_num == self.end_inclusive {
            self.finished = true;
        }
        Ok(Some(identity))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::PartitionBuildType;
    use crate::grpc::FinalizedAnchor;
    use crate::partition_index::{
        scan_time_index, CoveredBlock, IndexRoutingPolicy, RoutingWitness,
    };
    use futures::StreamExt;
    use std::collections::VecDeque;
    use std::sync::{Arc, Mutex};
    use tokio::net::TcpListener;

    #[derive(Clone)]
    enum Reply {
        Messages(Vec<firehose::Response>),
        Pending,
        HeaderPending,
    }
    #[derive(Clone)]
    struct StreamService {
        replies: Arc<Mutex<VecDeque<Reply>>>,
        requests: Arc<Mutex<Vec<firehose::Request>>>,
    }
    #[derive(Clone)]
    struct FetchService {
        replies: Arc<Mutex<VecDeque<Result<Option<firehose::BlockMetadata>, tonic::Status>>>>,
        requests: Arc<Mutex<Vec<u64>>>,
    }
    impl tonic::server::ServerStreamingService<firehose::Request> for StreamService {
        type Response = firehose::Response;
        type ResponseStream =
            futures::stream::BoxStream<'static, Result<Self::Response, tonic::Status>>;
        type Future =
            tonic::codegen::BoxFuture<tonic::Response<Self::ResponseStream>, tonic::Status>;
        fn call(&mut self, request: tonic::Request<firehose::Request>) -> Self::Future {
            assert_eq!(
                request.metadata().get("x-api-key").unwrap(),
                "index-test-key"
            );
            self.requests.lock().unwrap().push(request.into_inner());
            let reply = self
                .replies
                .lock()
                .unwrap()
                .pop_front()
                .expect("unexpected stream RPC");
            Box::pin(async move {
                match reply {
                    Reply::Messages(messages) => Ok(tonic::Response::new(
                        futures::stream::iter(messages.into_iter().map(Ok)).boxed(),
                    )),
                    Reply::Pending => Ok(tonic::Response::new(futures::stream::pending().boxed())),
                    Reply::HeaderPending => futures::future::pending().await,
                }
            })
        }
    }
    impl tonic::server::UnaryService<firehose::SingleBlockRequest> for FetchService {
        type Response = firehose::SingleBlockResponse;
        type Future = tonic::codegen::BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
        fn call(&mut self, request: tonic::Request<firehose::SingleBlockRequest>) -> Self::Future {
            assert_eq!(
                request.metadata().get("x-api-key").unwrap(),
                "index-test-key"
            );
            let Some(firehose::single_block_request::Reference::BlockNumber(number)) =
                request.into_inner().reference
            else {
                panic!("unexpected parent reference")
            };
            self.requests.lock().unwrap().push(number.num);
            let result = self
                .replies
                .lock()
                .unwrap()
                .pop_front()
                .expect("unexpected parent RPC");
            Box::pin(async move {
                Ok(tonic::Response::new(firehose::SingleBlockResponse {
                    metadata: result?,
                    block: None,
                }))
            })
        }
    }
    macro_rules! service {
        ($name:ident, $route:literal, $method:ident) => {
            impl tonic::server::NamedService for $name {
                const NAME: &'static str = $route;
            }
            impl tonic::codegen::Service<tonic::codegen::http::Request<tonic::body::Body>>
                for $name
            {
                type Response = tonic::codegen::http::Response<tonic::body::Body>;
                type Error = std::convert::Infallible;
                type Future = tonic::codegen::BoxFuture<Self::Response, Self::Error>;
                fn poll_ready(
                    &mut self,
                    _: &mut std::task::Context<'_>,
                ) -> std::task::Poll<Result<(), Self::Error>> {
                    std::task::Poll::Ready(Ok(()))
                }
                fn call(
                    &mut self,
                    request: tonic::codegen::http::Request<tonic::body::Body>,
                ) -> Self::Future {
                    let service = self.clone();
                    Box::pin(async move {
                        Ok(tonic::server::Grpc::new(tonic_prost::ProstCodec::default())
                            .$method(service, request)
                            .await)
                    })
                }
            }
        };
    }
    service!(StreamService, "sf.firehose.v2.Stream", server_streaming);
    service!(FetchService, "sf.firehose.v2.Fetch", unary);
    struct Endpoint {
        client: FirehoseClient,
        task: tokio::task::JoinHandle<()>,
        requests: Arc<Mutex<Vec<firehose::Request>>>,
        parents: Arc<Mutex<Vec<u64>>>,
    }
    impl Drop for Endpoint {
        fn drop(&mut self) {
            self.task.abort();
        }
    }
    async fn spawn_endpoint(
        streams: Vec<Reply>,
        parents: Vec<Result<Option<firehose::BlockMetadata>, tonic::Status>>,
    ) -> Endpoint {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = format!("http://{}", listener.local_addr().unwrap());
        let incoming = futures::stream::unfold(listener, |listener| async move {
            Some((listener.accept().await.map(|(socket, _)| socket), listener))
        });
        let requests = Arc::new(Mutex::new(Vec::new()));
        let parent_requests = Arc::new(Mutex::new(Vec::new()));
        let stream = StreamService {
            replies: Arc::new(Mutex::new(streams.into())),
            requests: requests.clone(),
        };
        let fetch = FetchService {
            replies: Arc::new(Mutex::new(parents.into())),
            requests: parent_requests.clone(),
        };
        let task = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(stream)
                .add_service(fetch)
                .serve_with_incoming(incoming)
                .await
                .unwrap();
        });
        let mut config = crate::grpc::tests::test_config(&address);
        config.api_key = Some("index-test-key".into());
        Endpoint {
            client: FirehoseClient::new(config).unwrap(),
            task,
            requests,
            parents: parent_requests,
        }
    }
    const A: i64 = 1_700_000_000;
    const B: i64 = A + 3_600;
    fn block(num: u64, parent: u64, timestamp: i64) -> firehose::Response {
        firehose::Response {
            block: Some(prost_types::Any::default()),
            step: 3,
            metadata: Some(firehose::BlockMetadata {
                num,
                id: format!("id-{num}"),
                parent_num: parent,
                parent_id: format!("id-{parent}"),
                time: (timestamp != 0).then_some(prost_types::Timestamp {
                    seconds: timestamp,
                    nanos: 0,
                }),
                ..Default::default()
            }),
            ..Default::default()
        }
    }
    fn left() -> RoutingWitness {
        RoutingWitness {
            block: CoveredBlock {
                block_num: 9,
                block_id: "id-9".into(),
                parent_num: 8,
                parent_id: "id-8".into(),
            },
            timestamp: B,
        }
    }
    async fn scan(
        endpoint: &Endpoint,
        left: Option<RoutingWitness>,
        policy: IndexRoutingPolicy,
        stop: u64,
        anchor: u64,
    ) -> Result<crate::partition_index::VerifiedPartitionIndex> {
        scan_time_index(
            &endpoint.client,
            "test-chain".into(),
            PartitionBuildType::Hour,
            10,
            stop,
            FinalizedAnchor {
                block_num: anchor,
                block_id: format!("id-{anchor}"),
            },
            policy,
            left,
            Duration::from_millis(500),
            &CancellationToken::new(),
        )
        .await
    }

    #[tokio::test]
    async fn exact_rpc_traversal_preserves_nonmonotonic_runs_and_caps_witness() {
        let endpoint = spawn_endpoint(
            vec![
                Reply::Messages(vec![block(10, 9, A), block(11, 10, B), block(12, 11, A)]),
                Reply::Messages(vec![block(13, 12, B)]),
            ],
            vec![],
        )
        .await;
        let result = scan(
            &endpoint,
            Some(left()),
            IndexRoutingPolicy::CanonicalTimestamp,
            13,
            1_000_000,
        )
        .await
        .unwrap();
        assert_eq!(result.spans.len(), 3);
        assert!(result.spans.iter().all(|span| span.proof.complete()));
        let requests = endpoint.requests.lock().unwrap();
        assert_eq!(requests.len(), 2);
        assert_eq!(
            (requests[0].start_block_num, requests[0].stop_block_num),
            (10, 12)
        );
        assert_eq!(
            (requests[1].start_block_num, requests[1].stop_block_num),
            (13, 13 + 65_535)
        );
        assert!(requests.iter().all(|request| request.final_blocks_only
            && request.cursor.is_empty()
            && request.transforms.is_empty()));
        assert!(endpoint.parents.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn actual_parent_rpcs_restore_solana_prior_time_and_verify_each_identity() {
        let endpoint = spawn_endpoint(
            vec![
                Reply::Messages(vec![block(10, 9, 0), block(11, 10, 0), block(12, 11, B)]),
                Reply::Messages(vec![block(13, 12, A)]),
            ],
            vec![Ok(block(9, 8, 0).metadata), Ok(block(8, 7, A).metadata)],
        )
        .await;
        let result = scan(
            &endpoint,
            None,
            IndexRoutingPolicy::SolanaPriorTimestamp,
            13,
            20,
        )
        .await
        .unwrap();
        assert_eq!(result.spans[0].proof.routing_start_timestamp, Some(A));
        assert!(!result.spans[0].proof.start_complete);
        assert_eq!(*endpoint.parents.lock().unwrap(), vec![9, 8]);
        let endpoint = spawn_endpoint(
            vec![Reply::Messages(vec![block(10, 9, 0)])],
            vec![Ok(block(8, 7, A).metadata)],
        )
        .await;
        let error = scan(
            &endpoint,
            None,
            IndexRoutingPolicy::SolanaPriorTimestamp,
            13,
            20,
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("canonical parent"));
    }

    #[tokio::test]
    async fn omitted_metadata_nonfinal_wrong_head_and_gap_eof_never_validate() {
        for response in [
            firehose::Response {
                metadata: None,
                ..block(10, 9, A)
            },
            firehose::Response {
                step: 1,
                ..block(10, 9, A)
            },
            block(11, 10, A),
        ] {
            let endpoint = spawn_endpoint(vec![Reply::Messages(vec![response])], vec![]).await;
            assert!(scan(
                &endpoint,
                Some(left()),
                IndexRoutingPolicy::CanonicalTimestamp,
                13,
                20
            )
            .await
            .is_err());
        }
        let endpoint = spawn_endpoint(
            vec![
                Reply::Messages(vec![block(10, 9, A)]),
                Reply::Messages(vec![]),
            ],
            vec![],
        )
        .await;
        assert!(scan(
            &endpoint,
            Some(left()),
            IndexRoutingPolicy::CanonicalTimestamp,
            13,
            20
        )
        .await
        .is_err());
        let endpoint = spawn_endpoint(
            vec![Reply::Messages(vec![block(10, 9, A), block(11, 10, A)])],
            vec![],
        )
        .await;
        assert!(scan(
            &endpoint,
            Some(left()),
            IndexRoutingPolicy::CanonicalTimestamp,
            13,
            12
        )
        .await
        .is_err());
        let endpoint = spawn_endpoint(
            vec![Reply::Messages(vec![block(10, 9, A), block(12, 11, B)])],
            vec![],
        )
        .await;
        assert!(scan(
            &endpoint,
            Some(left()),
            IndexRoutingPolicy::CanonicalTimestamp,
            13,
            12
        )
        .await
        .is_err());
    }

    #[tokio::test]
    async fn parent_missing_metadata_budget_and_non_solana_bootstrap_fail_closed() {
        let endpoint =
            spawn_endpoint(vec![Reply::Messages(vec![block(10, 9, 0)])], vec![Ok(None)]).await;
        assert!(scan(
            &endpoint,
            None,
            IndexRoutingPolicy::SolanaPriorTimestamp,
            13,
            20
        )
        .await
        .unwrap_err()
        .to_string()
        .contains("no block metadata"));
        let endpoint = spawn_endpoint(
            vec![Reply::Messages(vec![block(10, 9, 0)])],
            vec![Err(tonic::Status::not_found("missing"))],
        )
        .await;
        assert!(scan(
            &endpoint,
            None,
            IndexRoutingPolicy::SolanaPriorTimestamp,
            13,
            20
        )
        .await
        .is_err());
        let endpoint = spawn_endpoint(
            vec![Reply::Messages(vec![block(10, 9, 0)])],
            vec![Ok(block(9, 8, A).metadata)],
        )
        .await;
        assert!(scan(
            &endpoint,
            None,
            IndexRoutingPolicy::CanonicalTimestamp,
            13,
            20
        )
        .await
        .unwrap_err()
        .to_string()
        .contains("bootstrap"));
    }

    #[tokio::test]
    async fn range_header_message_and_right_witness_waits_are_bounded_and_cancellable() {
        for replies in [
            vec![Reply::HeaderPending],
            vec![Reply::Pending],
            vec![Reply::Messages(vec![block(10, 9, A)]), Reply::Pending],
        ] {
            for cancel in [false, true] {
                let endpoint = spawn_endpoint(replies.clone(), vec![]).await;
                let shutdown = CancellationToken::new();
                if cancel {
                    let shutdown = shutdown.clone();
                    tokio::spawn(async move {
                        tokio::time::sleep(Duration::from_millis(20)).await;
                        shutdown.cancel();
                    });
                }
                let error = scan_time_index(
                    &endpoint.client,
                    "test-chain".into(),
                    PartitionBuildType::Hour,
                    10,
                    13,
                    FinalizedAnchor {
                        block_num: 20,
                        block_id: "id-20".into(),
                    },
                    IndexRoutingPolicy::CanonicalTimestamp,
                    Some(left()),
                    Duration::from_millis(80),
                    &shutdown,
                )
                .await
                .unwrap_err();
                if cancel {
                    assert!(crate::grpc::is_shutdown_error(&error), "{error:#}");
                } else {
                    assert!(format!("{error:#}").contains("deadline"), "{error:#}");
                }
            }
        }
    }
    #[tokio::test]
    async fn parent_scan_budget_is_finite_and_future_stop_makes_no_request() {
        let endpoint = spawn_endpoint(
            vec![Reply::Messages(vec![block(100, 99, 0)])],
            (36..100)
                .rev()
                .map(|number| Ok(block(number, number - 1, 0).metadata))
                .collect(),
        )
        .await;
        let error = scan_time_index(
            &endpoint.client,
            "test-chain".into(),
            PartitionBuildType::Hour,
            100,
            101,
            FinalizedAnchor {
                block_num: 100,
                block_id: "id-100".into(),
            },
            IndexRoutingPolicy::SolanaPriorTimestamp,
            None,
            Duration::from_secs(5),
            &CancellationToken::new(),
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("ancestor budget"), "{error:#}");
        assert_eq!(endpoint.parents.lock().unwrap().len(), 64);
        let endpoint = spawn_endpoint(vec![], vec![]).await;
        assert!(scan(
            &endpoint,
            None,
            IndexRoutingPolicy::CanonicalTimestamp,
            22,
            20
        )
        .await
        .is_err());
        assert!(endpoint.requests.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn actual_genesis_zero_stops_locally_and_uses_only_the_genesis_seed() {
        let mut genesis = block(0, 0, 0);
        genesis.metadata.as_mut().unwrap().parent_id.clear();
        let endpoint =
            spawn_endpoint(vec![Reply::Messages(vec![genesis, block(1, 0, A)])], vec![]).await;
        let result = scan_time_index(
            &endpoint.client,
            "test-chain".into(),
            PartitionBuildType::Hour,
            0,
            1,
            FinalizedAnchor {
                block_num: 0,
                block_id: "id-0".into(),
            },
            IndexRoutingPolicy::SolanaPriorTimestamp,
            None,
            Duration::from_millis(500),
            &CancellationToken::new(),
        )
        .await
        .unwrap();
        assert_eq!(result.coverage.last_observed.unwrap().block_num, 0);
        assert_eq!(
            result.spans[0].proof.routing_start_timestamp,
            Some(crate::partition_index::SOLANA_GENESIS_TIMESTAMP)
        );
        assert!(result.spans[0].proof.start_complete);
        assert!(!result.spans[0].proof.end_complete);
        assert!(endpoint.parents.lock().unwrap().is_empty());
    }
}

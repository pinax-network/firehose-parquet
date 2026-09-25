//! Bounded finality evidence for partition indexes. Fetch availability alone is
//! not finality: require an explicit STEP_FINAL response at the advertised LIB.
use super::{checked_block_identity, unless_shutdown, CancellationToken, FirehoseClient};
use anyhow::{ensure, Context, Result};
use firehose_protos::firehose;
use std::time::Duration;

/// A block explicitly returned with STEP_FINAL by the endpoint.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct FinalizedAnchor {
    pub block_num: u64,
    pub block_id: String,
}

impl FinalizedAnchor {
    pub fn exclusive_stop(&self) -> Result<u64> {
        self.block_num
            .checked_add(1)
            .context("finalized block has no representable exclusive stop")
    }
}

#[derive(Debug, thiserror::Error)]
#[error("finality proof timed out after {timeout:?}")]
pub struct FinalityTimeoutError {
    pub timeout: Duration,
}

impl FirehoseClient {
    /// Obtain a conservative finalized anchor with two bounded Stream requests.
    /// A near-head NEW/FINAL envelope advertises a LIB candidate; a final-only
    /// request must then return that exact candidate with STEP_FINAL. No Fetch
    /// fallback, missing-block heuristic or server-specific head sentinel is used.
    /// The deadline includes connecting, response headers and both message reads.
    pub async fn finalized_anchor(
        &self,
        timeout: Duration,
        shutdown: &CancellationToken,
    ) -> Result<FinalizedAnchor> {
        unless_shutdown(shutdown, async {
            tokio::time::timeout(timeout, self.finalized_anchor_inner())
                .await
                .map_err(|_| anyhow::Error::from(FinalityTimeoutError { timeout }))?
        })
        .await?
    }

    async fn finalized_anchor_inner(&self) -> Result<FinalizedAnchor> {
        let channel = self.fetch_channel().await?;
        let mut client = self.stream_client(channel);
        let mut request = tonic::Request::new(firehose::Request {
            start_block_num: -1,
            stop_block_num: 0,
            final_blocks_only: false,
            ..Default::default()
        });
        self.auth.apply(&mut request);
        let mut head = client
            .blocks(request)
            .await
            .context("requesting near-head finality witness")?
            .into_inner();
        let mut candidate = None;
        for _ in 0..4 {
            let response = head
                .message()
                .await
                .context("reading near-head finality witness")?
                .context("near-head stream ended without a finality witness")?;
            match response.step {
                2 => continue, // STEP_UNDO cannot advertise a canonical witness.
                1 | 3 => {}    // STEP_NEW / STEP_FINAL
                other => anyhow::bail!("near-head witness has invalid fork step {other}"),
            }
            ensure!(
                response.block.is_some(),
                "near-head witness has no block payload"
            );
            let metadata = response
                .metadata
                .as_ref()
                .context("near-head witness has no block metadata")?;
            let identity = checked_block_identity(metadata, None)?;
            ensure!(
                !identity.block_id.is_empty(),
                "near-head witness has an empty block identity"
            );
            ensure!(
                identity.lib_num <= identity.block_num,
                "near-head witness finalized height {} exceeds witness block {}",
                identity.lib_num,
                identity.block_num
            );
            candidate = Some(identity.lib_num);
            break;
        }
        drop(head);
        let candidate =
            candidate.context("near-head response budget exhausted without a canonical witness")?;
        let signed_candidate = i64::try_from(candidate)
            .context("finalized candidate exceeds signed stream start range")?;
        let mut request = tonic::Request::new(firehose::Request {
            start_block_num: signed_candidate,
            stop_block_num: candidate,
            final_blocks_only: true,
            ..Default::default()
        });
        self.auth.apply(&mut request);
        let mut proof = client
            .blocks(request)
            .await
            .context("requesting finalized candidate proof")?
            .into_inner();
        // For L=0, zero is the protocol's unbounded-stop sentinel. The client
        // still accepts exactly one matching response and drops the stream here.
        let response = proof
            .message()
            .await
            .context("reading finalized candidate proof")?
            .context("finalized candidate stream ended without a block")?;
        ensure!(
            response.step == 3,
            "finalized candidate was not returned with STEP_FINAL"
        );
        ensure!(
            response.block.is_some(),
            "finalized candidate has no block payload"
        );
        let metadata = response
            .metadata
            .as_ref()
            .context("finalized candidate has no block metadata")?;
        let identity = checked_block_identity(metadata, None)?;
        ensure!(
            identity.block_num == candidate,
            "finalized candidate {candidate} returned block {}",
            identity.block_num
        );
        ensure!(
            !identity.block_id.is_empty(),
            "finalized candidate has an empty block identity"
        );
        Ok(FinalizedAnchor {
            block_num: identity.block_num,
            block_id: identity.block_id,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;
    use std::collections::VecDeque;
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    };
    use tokio::net::TcpListener;

    #[derive(Clone)]
    enum Reply {
        Messages(Vec<firehose::Response>),
        PendingHeaders,
        PendingMessage,
        Error(tonic::Status),
    }

    #[derive(Clone)]
    struct Service {
        replies: Arc<Mutex<VecDeque<Reply>>>,
        requests: Arc<Mutex<Vec<firehose::Request>>>,
    }

    impl tonic::server::ServerStreamingService<firehose::Request> for Service {
        type Response = firehose::Response;
        type ResponseStream =
            futures::stream::BoxStream<'static, Result<firehose::Response, tonic::Status>>;
        type Future =
            tonic::codegen::BoxFuture<tonic::Response<Self::ResponseStream>, tonic::Status>;
        fn call(&mut self, request: tonic::Request<firehose::Request>) -> Self::Future {
            assert_eq!(
                request.metadata().get("x-api-key").unwrap(),
                "proof-test-key"
            );
            self.requests.lock().unwrap().push(request.into_inner());
            let reply = self
                .replies
                .lock()
                .unwrap()
                .pop_front()
                .expect("unexpected extra RPC");
            Box::pin(async move {
                match reply {
                    Reply::Messages(messages) => Ok(tonic::Response::new(
                        futures::stream::iter(messages.into_iter().map(Ok)).boxed(),
                    )),
                    Reply::PendingHeaders => futures::future::pending().await,
                    Reply::PendingMessage => {
                        Ok(tonic::Response::new(futures::stream::pending().boxed()))
                    }
                    Reply::Error(error) => Err(error),
                }
            })
        }
    }
    impl tonic::server::NamedService for Service {
        const NAME: &'static str = "sf.firehose.v2.Stream";
    }
    impl tonic::codegen::Service<tonic::codegen::http::Request<tonic::body::Body>> for Service {
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
                    .server_streaming(service, request)
                    .await)
            })
        }
    }

    fn block(num: u64, lib: u64, step: i32) -> firehose::Response {
        firehose::Response {
            block: Some(prost_types::Any::default()),
            step,
            metadata: Some(firehose::BlockMetadata {
                num,
                id: format!("block-{num}"),
                lib_num: lib,
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    async fn run(
        replies: Vec<Reply>,
        timeout: Duration,
        cancel: bool,
    ) -> (Result<FinalizedAnchor>, Vec<firehose::Request>, usize) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let connections = Arc::new(AtomicUsize::new(0));
        let accepted = connections.clone();
        let incoming =
            futures::stream::unfold((listener, accepted), |(listener, accepted)| async move {
                let socket = listener.accept().await.map(|(socket, _)| socket);
                accepted.fetch_add(1, Ordering::SeqCst);
                Some((socket, (listener, accepted)))
            });
        let requests = Arc::new(Mutex::new(Vec::new()));
        let service = Service {
            replies: Arc::new(Mutex::new(replies.into())),
            requests: requests.clone(),
        };
        let server = tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(service)
                .serve_with_incoming(incoming),
        );
        let mut config = crate::grpc::tests::test_config(&endpoint);
        config.api_key = Some("proof-test-key".into());
        let client = FirehoseClient::new(config).unwrap();
        let shutdown = CancellationToken::new();
        if cancel {
            let shutdown = shutdown.clone();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(40)).await;
                shutdown.cancel();
            });
        }
        let result = client.finalized_anchor(timeout, &shutdown).await;
        drop(client);
        server.abort();
        let _ = server.await;
        let requests = requests.lock().unwrap().clone();
        (result, requests, connections.load(Ordering::SeqCst))
    }

    #[tokio::test]
    async fn finality_proof_requires_negative_head_request_then_exact_final_candidate() {
        for candidate in [0, 8] {
            let (result, requests, connections) = run(
                vec![
                    Reply::Messages(vec![block(10, candidate, 1)]),
                    Reply::Messages(vec![block(candidate, 0, 3)]),
                ],
                Duration::from_secs(2),
                false,
            )
            .await;
            let anchor = result.unwrap();
            assert_eq!(
                anchor,
                FinalizedAnchor {
                    block_num: candidate,
                    block_id: format!("block-{candidate}")
                }
            );
            assert_eq!(anchor.exclusive_stop().unwrap(), candidate + 1);
            assert_eq!(requests.len(), 2);
            assert_eq!(
                (
                    requests[0].start_block_num,
                    requests[0].stop_block_num,
                    requests[0].final_blocks_only
                ),
                (-1, 0, false)
            );
            assert_eq!(
                (
                    requests[1].start_block_num,
                    requests[1].stop_block_num,
                    requests[1].final_blocks_only
                ),
                (candidate as i64, candidate, true)
            );
            assert!(requests
                .iter()
                .all(|request| request.cursor.is_empty() && request.transforms.is_empty()));
            assert_eq!(
                connections, 1,
                "the proof should reuse its authenticated channel"
            );
        }
    }

    #[tokio::test]
    async fn finality_proof_rejects_unproven_or_inconsistent_candidates() {
        let mut no_metadata = block(8, 0, 3);
        no_metadata.metadata = None;
        let mut no_payload = block(8, 0, 3);
        no_payload.block = None;
        let mut empty_id = block(8, 0, 3);
        empty_id.metadata.as_mut().unwrap().id.clear();
        for (response, message) in [
            (block(7, 0, 3), "returned block 7"),
            (block(9, 0, 3), "returned block 9"),
            (block(8, 0, 1), "STEP_FINAL"),
            (no_metadata, "no block metadata"),
            (no_payload, "no block payload"),
            (empty_id, "empty block identity"),
        ] {
            let (result, requests, _) = run(
                vec![
                    Reply::Messages(vec![block(10, 8, 1)]),
                    Reply::Messages(vec![response]),
                ],
                Duration::from_secs(2),
                false,
            )
            .await;
            assert!(
                format!("{:#}", result.unwrap_err()).contains(message),
                "{message}"
            );
            assert_eq!(requests.len(), 2);
        }
    }

    #[tokio::test]
    async fn finality_proof_rejects_invalid_head_and_signed_overflow_before_second_rpc() {
        let mut no_metadata = block(10, 8, 1);
        no_metadata.metadata = None;
        let mut empty_id = block(10, 8, 1);
        empty_id.metadata.as_mut().unwrap().id.clear();
        for (responses, message) in [
            (vec![no_metadata], "no block metadata"),
            (vec![empty_id], "empty block identity"),
            (vec![block(10, 11, 1)], "exceeds witness block"),
            (
                vec![block(u64::MAX, i64::MAX as u64 + 1, 1)],
                "signed stream start range",
            ),
            (vec![block(10, 8, 0)], "invalid fork step"),
            (vec![block(10, 8, 2); 4], "response budget exhausted"),
            (vec![], "ended without a finality witness"),
        ] {
            let (result, requests, _) = run(
                vec![Reply::Messages(responses)],
                Duration::from_secs(2),
                false,
            )
            .await;
            assert!(
                format!("{:#}", result.unwrap_err()).contains(message),
                "{message}"
            );
            assert_eq!(requests.len(), 1);
        }
    }

    #[tokio::test]
    async fn finality_proof_has_deadline_and_cancellation_for_headers_and_messages() {
        for reply in [Reply::PendingHeaders, Reply::PendingMessage] {
            for second_rpc in [false, true] {
                for cancel in [false, true] {
                    let replies = if second_rpc {
                        vec![Reply::Messages(vec![block(10, 8, 1)]), reply.clone()]
                    } else {
                        vec![reply.clone()]
                    };
                    let (result, _, _) = run(replies, Duration::from_millis(150), cancel).await;
                    let error = result.unwrap_err();
                    if cancel {
                        assert!(crate::grpc::is_shutdown_error(&error));
                    } else {
                        assert!(error.downcast_ref::<FinalityTimeoutError>().is_some());
                    }
                }
            }
        }
    }

    #[tokio::test]
    async fn finality_proof_propagates_status_and_empty_final_stream() {
        for reply in [
            Reply::Error(tonic::Status::not_found("candidate unavailable")),
            Reply::Messages(vec![]),
        ] {
            let (result, requests, _) = run(
                vec![Reply::Messages(vec![block(10, 8, 1)]), reply],
                Duration::from_secs(2),
                false,
            )
            .await;
            assert!(result.is_err());
            assert_eq!(requests.len(), 2);
        }
    }
}

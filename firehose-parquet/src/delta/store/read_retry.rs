//! Bounded retries of the Delta log client's idempotent reads (#680).
//!
//! The log client sends every request once (object_store `max_retries: 0`,
//! [`super::s3_builder`]), because object_store would resend a conditional
//! log commit on a 5xx or a lost connection, and a commit whose first copy
//! landed would then come back as a 412 and be committed again at the next
//! version (design §3.5). Reads carry no such risk, yet with one attempt a
//! single dropped connection on a `GET _delta_log/_last_checkpoint` ended a
//! `build` on Ceph RGW.
//!
//! [`ReadRetryConnector`] therefore wraps the log client's HTTP client, below
//! object_store's request logic, where the method and the status of every
//! request are known. A `GET` or `HEAD` (every object read, byte range and
//! ListObjectsV2 page of a log, its checkpoints and whatever delta-rs reads
//! while loading a snapshot) that fails without a response, or answers 408,
//! 429 or 5xx, is sent again, at most [`READ_ATTEMPTS`] times in all, after a
//! jittered exponential backoff, and each retry is logged at `warn`. Every
//! other method (`PUT`, including the conditional commit, `POST`, `DELETE`)
//! passes through exactly once, so a failed commit keeps its single attempt
//! and its ambiguous-outcome handling (`delta::commit`, design §3.5).
//!
//! A read whose response body breaks after its headers arrived is not
//! retried here: its response was already handed to object_store.

use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hasher};
use std::time::Duration;

use async_trait::async_trait;
use object_store_delta::client::{
    HttpClient, HttpConnector, HttpError, HttpRequest, HttpResponse, HttpService, ReqwestConnector,
};
use object_store_delta::ClientOptions;

/// Attempts of one idempotent read, the first included.
pub const READ_ATTEMPTS: u32 = 3;

/// The backoff before the first retry; it doubles for each later one.
const BACKOFF_BASE: Duration = Duration::from_millis(200);

/// The longest backoff before a retry.
const BACKOFF_MAX: Duration = Duration::from_secs(2);

/// The log client's HTTP connector: object_store's reqwest connector, with
/// [`ReadRetry`] around each client it builds.
#[derive(Debug, Default)]
pub(crate) struct ReadRetryConnector;

impl HttpConnector for ReadRetryConnector {
    fn connect(&self, options: &ClientOptions) -> object_store_delta::Result<HttpClient> {
        Ok(HttpClient::new(ReadRetry::new(
            ReqwestConnector::default().connect(options)?,
        )))
    }
}

/// An HTTP client that retries idempotent reads on transient failures and
/// sends everything else once.
#[derive(Debug)]
pub(crate) struct ReadRetry {
    inner: HttpClient,
    attempts: u32,
    base: Duration,
    max: Duration,
}

impl ReadRetry {
    pub(crate) fn new(inner: HttpClient) -> Self {
        Self {
            inner,
            attempts: READ_ATTEMPTS,
            base: BACKOFF_BASE,
            max: BACKOFF_MAX,
        }
    }

    #[cfg(test)]
    fn with_backoff(mut self, base: Duration, max: Duration) -> Self {
        self.base = base;
        self.max = max;
        self
    }

    /// The backoff before retry `retry` (1 for the first): the exponential
    /// delay, capped, with its upper half randomized.
    fn backoff(&self, retry: u32) -> Duration {
        let delay = self
            .base
            .saturating_mul(1 << retry.saturating_sub(1).min(16))
            .min(self.max);
        let half = delay / 2;
        let spread = u64::try_from(half.as_nanos()).unwrap_or(u64::MAX);
        half + Duration::from_nanos(random() % spread.saturating_add(1))
    }
}

/// Whether `method` is a read that can be sent again without changing
/// anything: the only requests [`ReadRetry`] retries.
pub(crate) fn is_idempotent_read(method: &str) -> bool {
    matches!(method, "GET" | "HEAD")
}

/// A status worth another attempt: a timeout, throttling or a server error.
fn is_transient_status(status: u16) -> bool {
    status == 408 || status == 429 || (500..600).contains(&status)
}

fn random() -> u64 {
    RandomState::new().build_hasher().finish()
}

/// `error` and its sources, `: `-separated (object_store's own message stops
/// at reqwest's "error sending request").
fn error_chain(error: &HttpError) -> String {
    let mut text = error.to_string();
    let mut source = std::error::Error::source(error);
    while let Some(cause) = source {
        let cause_text = cause.to_string();
        if !text.ends_with(&cause_text) {
            text.push_str(": ");
            text.push_str(&cause_text);
        }
        source = cause.source();
    }
    text
}

#[async_trait]
impl HttpService for ReadRetry {
    async fn call(&self, request: HttpRequest) -> Result<HttpResponse, HttpError> {
        if !is_idempotent_read(request.method().as_str()) {
            return self.inner.execute(request).await;
        }
        let mut attempt = 1;
        loop {
            let result = self.inner.execute(request.clone()).await;
            if attempt >= self.attempts {
                return result;
            }
            let reason = match &result {
                Ok(response) if is_transient_status(response.status().as_u16()) => {
                    format!("status {}", response.status())
                }
                Ok(_) => return result,
                Err(error) => error_chain(error),
            };
            drop(result);
            let backoff = self.backoff(attempt);
            tracing::warn!(
                method = %request.method(),
                path = request.uri().path_and_query().map_or("/", |path| path.as_str()),
                attempt,
                attempts = self.attempts,
                backoff_ms = u64::try_from(backoff.as_millis()).unwrap_or(u64::MAX),
                error = %reason,
                "retrying an idempotent Delta log read after a transient error"
            );
            tokio::time::sleep(backoff).await;
            attempt += 1;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::{Arc, Mutex};

    use bytes::Bytes;
    use object_store_delta::client::{HttpErrorKind, HttpRequestBody, HttpResponseBody};

    use super::*;

    /// What the scripted server answers to one request.
    #[derive(Debug)]
    enum Answer {
        Status(u16),
        Transport(HttpErrorKind),
    }

    /// An HTTP service that answers from a script and records each request's
    /// method and path.
    #[derive(Debug, Default)]
    struct Scripted {
        answers: Mutex<VecDeque<Answer>>,
        requests: Arc<Mutex<Vec<(String, String)>>>,
    }

    #[async_trait]
    impl HttpService for Scripted {
        async fn call(&self, request: HttpRequest) -> Result<HttpResponse, HttpError> {
            self.requests.lock().unwrap().push((
                request.method().to_string(),
                request.uri().path().to_string(),
            ));
            let status = match self.answers.lock().unwrap().pop_front() {
                None => 200,
                Some(Answer::Status(status)) => status,
                Some(Answer::Transport(kind)) => {
                    return Err(HttpError::new(
                        kind,
                        std::io::Error::new(std::io::ErrorKind::ConnectionReset, "reset by peer"),
                    ))
                }
            };
            let mut response = HttpResponse::new(HttpResponseBody::from(Bytes::from_static(b"")));
            *response.status_mut() = status.try_into().unwrap();
            Ok(response)
        }
    }

    /// Sends one `method` request to `path` through [`ReadRetry`] over a
    /// server that answers `script` in order (then 200); returns the final
    /// status (or `None` for a transport error) and the requests the server
    /// saw.
    async fn send(
        method: &str,
        path: &str,
        script: Vec<Answer>,
    ) -> (Option<u16>, Vec<(String, String)>) {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let service = Scripted {
            answers: Mutex::new(script.into()),
            requests: Arc::clone(&requests),
        };
        let retry = ReadRetry::new(HttpClient::new(service))
            .with_backoff(Duration::from_millis(1), Duration::from_millis(4));
        let mut request = HttpRequest::new(HttpRequestBody::from(if method == "PUT" {
            Bytes::from_static(b"{\"commitInfo\":{}}")
        } else {
            Bytes::new()
        }));
        *request.method_mut() = method.parse().unwrap();
        *request.uri_mut() = format!("http://127.0.0.1:1/bucket/{path}").parse().unwrap();
        let status = retry
            .call(request)
            .await
            .ok()
            .map(|response| response.status().as_u16());
        let seen = requests.lock().unwrap().clone();
        (status, seen)
    }

    const CHECKPOINT: &str = "chain/blocks/_delta_log/_last_checkpoint";
    const COMMIT: &str = "chain/blocks/_delta_log/00000000000000000001.json";

    #[tokio::test]
    async fn idempotent_reads_retry_transient_failures() {
        for (method, script) in [
            ("GET", vec![Answer::Status(503)]),
            ("GET", vec![Answer::Transport(HttpErrorKind::Request)]),
            ("GET", vec![Answer::Transport(HttpErrorKind::Interrupted)]),
            ("GET", vec![Answer::Transport(HttpErrorKind::Timeout)]),
            ("HEAD", vec![Answer::Status(429)]),
            (
                "GET",
                vec![
                    Answer::Transport(HttpErrorKind::Connect),
                    Answer::Status(500),
                ],
            ),
        ] {
            let failures = script.len();
            let (status, seen) = send(method, CHECKPOINT, script).await;
            assert_eq!(status, Some(200), "{method}");
            assert_eq!(seen.len(), failures + 1, "{method}: {seen:?}");
            assert!(seen
                .iter()
                .all(|(m, p)| m == method && p.ends_with(CHECKPOINT)));
        }
    }

    #[tokio::test]
    async fn a_read_stops_after_its_attempts_and_returns_the_last_answer() {
        let (status, seen) = send(
            "GET",
            CHECKPOINT,
            vec![
                Answer::Status(503),
                Answer::Status(502),
                Answer::Status(504),
            ],
        )
        .await;
        assert_eq!(status, Some(504));
        assert_eq!(seen.len(), READ_ATTEMPTS as usize);

        let (status, seen) = send(
            "GET",
            CHECKPOINT,
            (0..READ_ATTEMPTS)
                .map(|_| Answer::Transport(HttpErrorKind::Request))
                .collect(),
        )
        .await;
        assert_eq!(status, None, "the last transport error is returned");
        assert_eq!(seen.len(), READ_ATTEMPTS as usize);
    }

    #[tokio::test]
    async fn definite_read_answers_are_not_retried() {
        for status in [200, 206, 304, 400, 403, 404, 412, 416] {
            let (answered, seen) = send("GET", CHECKPOINT, vec![Answer::Status(status)]).await;
            assert_eq!(answered, Some(status));
            assert_eq!(seen.len(), 1, "{status}");
        }
    }

    /// A log commit (`PUT` with `If-None-Match: *`), and every other write,
    /// is sent exactly once whatever happens to it: a lost response must
    /// surface as an unknown outcome, never as a second copy that the store
    /// answers with 412 (design §3.5).
    #[tokio::test]
    async fn writes_are_sent_once_whatever_their_outcome() {
        for method in ["PUT", "POST", "DELETE"] {
            for answer in [
                Answer::Status(503),
                Answer::Status(500),
                Answer::Status(429),
                Answer::Status(408),
                Answer::Transport(HttpErrorKind::Request),
                Answer::Transport(HttpErrorKind::Interrupted),
                Answer::Transport(HttpErrorKind::Timeout),
                Answer::Transport(HttpErrorKind::Connect),
            ] {
                let (_, seen) = send(method, COMMIT, vec![answer]).await;
                assert_eq!(
                    seen,
                    vec![(method.to_string(), format!("/bucket/{COMMIT}"))]
                );
            }
        }
    }

    #[test]
    fn backoff_grows_is_capped_and_jittered() {
        let retry = ReadRetry::new(HttpClient::new(Scripted::default()));
        for _ in 0..100 {
            let first = retry.backoff(1);
            assert!(
                first >= BACKOFF_BASE / 2 && first <= BACKOFF_BASE,
                "{first:?}"
            );
            let second = retry.backoff(2);
            assert!(
                second >= BACKOFF_BASE && second <= BACKOFF_BASE * 2,
                "{second:?}"
            );
            let late = retry.backoff(30);
            assert!(late >= BACKOFF_MAX / 2 && late <= BACKOFF_MAX, "{late:?}");
        }
        let samples: std::collections::BTreeSet<_> = (0..20).map(|_| retry.backoff(1)).collect();
        assert!(samples.len() > 1, "the backoff is randomized");
    }

    #[test]
    fn only_get_and_head_are_idempotent_reads() {
        for method in ["GET", "HEAD"] {
            assert!(is_idempotent_read(method));
        }
        for method in ["PUT", "POST", "DELETE", "PATCH", "OPTIONS"] {
            assert!(!is_idempotent_read(method));
        }
    }
}

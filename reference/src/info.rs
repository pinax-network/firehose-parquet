//! A rate-limited, retrying client for HyperLiquid's `POST /info`.
//!
//! HyperLiquid limits `/info` to 1200 weight per minute per IP. Every request
//! of this job weighs 20, and fundingHistory one more per 20 items returned.
//! The client keeps its own sliding one-minute window under a configured
//! budget ([`WeightLimiter`]), well below the shared limit, and backs off
//! exponentially on HTTP 429, 5xx, timeouts, connection errors and invalid
//! JSON. `/info` requests are read-only, so retrying them is safe. At most one
//! request is in flight (`&mut self`): the limiter, the 429 pause and the order
//! of `observed_at` assume it.
//!
//! The endpoint URL may embed an API key (a gateway), so it is never logged or
//! put in an error: only [`describe_endpoint`]'s scheme and host are shown,
//! messages name the request ([`subject`]), and transport errors are reduced
//! to a fixed reason ([`TransportError`]). Error bodies are never read.

use std::collections::VecDeque;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures::future::BoxFuture;
use futures::FutureExt as _;
use reqwest::header::{ACCEPT, CONTENT_TYPE};
use serde_json::json;
use url::{Host, Url};

use crate::clock::Clock;
use crate::json::{subject, Json};
use crate::log::Logger;
use crate::ConfigError;

pub const PUBLIC_INFO_URL: &str = "https://api.hyperliquid.xyz/info";
/// The weight of every request this job makes.
pub const BASE_WEIGHT: u32 = 20;
/// The most items a fundingHistory answer holds: a full page asks for more.
pub const FUNDING_PAGE: usize = 500;
/// The largest answer read; a longer one fails the request.
pub const MAX_RESPONSE_BYTES: usize = 64 << 20;
/// Attempts for HTTP 5xx, fewer than for 429 and connection errors.
pub const SERVER_ERROR_ATTEMPTS: u32 = 3;
/// The statuses that are retried.
pub const RETRY_STATUSES: [u16; 5] = [429, 500, 502, 503, 504];

const MAX_ATTEMPTS: u32 = 6;
const BACKOFF_INITIAL: f64 = 2.0;
const BACKOFF_CAP: f64 = 120.0;
/// The limiter's window, in seconds.
const WINDOW: f64 = 60.0;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);
/// Between two reads of an answer.
const READ_TIMEOUT: Duration = Duration::from_secs(30);
/// A whole request: HYPE's several-MB tokenDetails fits easily.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(300);

const URL_INVALID: &str = "HL_INFO_URL must be an http(s) URL";
const URL_NOT_HTTPS: &str = "HL_INFO_URL must use https (plain http only for a loopback host)";

/// Why an attempt got no HTTP status; its message would carry the URL, so
/// only this is kept.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TransportError {
    Timeout,
    Connect,
    Body,
    Request,
}

impl TransportError {
    /// The reason in `retry` events and errors.
    pub fn reason(self) -> &'static str {
        match self {
            TransportError::Timeout => "timeout",
            TransportError::Connect => "connect",
            TransportError::Body => "body",
            TransportError::Request => "request",
        }
    }

    fn of(error: &reqwest::Error) -> Self {
        if error.is_timeout() {
            TransportError::Timeout
        } else if error.is_connect() {
            TransportError::Connect
        } else if error.is_body() || error.is_decode() {
            TransportError::Body
        } else {
            TransportError::Request
        }
    }
}

/// Sends a request body to `/info`.
pub trait Transport: Send + Sync {
    /// `(status, body)`: the body only for status 200, read up to
    /// [`MAX_RESPONSE_BYTES`] + 1 bytes; any other answer's body is dropped
    /// unread.
    fn post<'a>(&'a self, body: Vec<u8>) -> BoxFuture<'a, Result<(u16, Bytes), TransportError>>;
}

/// `POST`s to the endpoint with reqwest (rustls, the system's roots): no
/// redirects (every 3xx is refused), no reqwest retries, no compression.
pub struct HttpTransport {
    client: reqwest::Client,
    url: Url,
}

impl HttpTransport {
    /// A transport for `url`, an [`info_url`].
    pub fn new(url: Url) -> Result<Self, ConfigError> {
        let client = reqwest::Client::builder()
            .https_only(!is_loopback(&url))
            .redirect(reqwest::redirect::Policy::none())
            .referer(false)
            .retry(reqwest::retry::never())
            .connect_timeout(CONNECT_TIMEOUT)
            .read_timeout(READ_TIMEOUT)
            .timeout(REQUEST_TIMEOUT)
            .no_gzip()
            .no_brotli()
            .no_deflate()
            .no_zstd()
            .user_agent(concat!(
                "pinax-hypercore-reference/",
                env!("CARGO_PKG_VERSION")
            ))
            .build()
            .map_err(|_| ConfigError("building the /info HTTP client failed".into()))?;
        Ok(Self { client, url })
    }

    async fn send(&self, body: Vec<u8>) -> Result<(u16, Bytes), reqwest::Error> {
        let mut response = self
            .client
            .post(self.url.clone())
            .header(CONTENT_TYPE, "application/json")
            .header(ACCEPT, "application/json")
            .body(body)
            .send()
            .await?;
        let status = response.status().as_u16();
        if status != 200 {
            // Diagnostic text from the server: dropped unread.
            return Ok((status, Bytes::new()));
        }
        let mut body = Vec::new();
        while let Some(chunk) = response.chunk().await? {
            let room = MAX_RESPONSE_BYTES + 1 - body.len();
            body.extend_from_slice(&chunk[..chunk.len().min(room)]);
            if body.len() > MAX_RESPONSE_BYTES {
                break;
            }
        }
        Ok((status, Bytes::from(body)))
    }
}

impl Transport for HttpTransport {
    fn post<'a>(&'a self, body: Vec<u8>) -> BoxFuture<'a, Result<(u16, Bytes), TransportError>> {
        async move {
            self.send(body)
                .await
                .map_err(|error| TransportError::of(&error))
        }
        .boxed()
    }
}

/// `127.0.0.0/8`, `[::1]` or `localhost`.
fn is_loopback(url: &Url) -> bool {
    match url.host() {
        Some(Host::Ipv4(address)) => address.is_loopback(),
        Some(Host::Ipv6(address)) => address.is_loopback(),
        Some(Host::Domain(domain)) => domain == "localhost",
        None => false,
    }
}

/// The endpoint: `HL_INFO_URL` (empty is unset) or [`PUBLIC_INFO_URL`]. An
/// `http` or `https` URL with a host, `http` only for a loopback host. Leading
/// and trailing control characters and spaces and every tab and newline are
/// removed first, as WHATWG parsing would, so the checks see the URL that is
/// used. The errors never contain the value.
pub fn info_url(text: Option<&str>) -> Result<Url, ConfigError> {
    let invalid = || ConfigError(URL_INVALID.into());
    let text = text
        .filter(|text| !text.is_empty())
        .unwrap_or(PUBLIC_INFO_URL);
    let text: String = text
        .trim_matches(|c: char| c <= ' ')
        .chars()
        .filter(|c| !matches!(c, '\t' | '\n' | '\r'))
        .collect();
    // An empty authority: WHATWG would read `https:///info` as the host `info`.
    let (_, rest) = text.split_once(':').ok_or_else(invalid)?;
    let authority = rest.strip_prefix("//").ok_or_else(invalid)?;
    if authority.is_empty() || authority.starts_with(['/', '?', '#', '\\']) {
        return Err(invalid());
    }
    let url = Url::parse(&text).map_err(|_| invalid())?;
    if !matches!(url.scheme(), "http" | "https") || url.host_str().is_none_or(str::is_empty) {
        return Err(invalid());
    }
    if url.scheme() == "http" && !is_loopback(&url) {
        return Err(ConfigError(URL_NOT_HTTPS.into()));
    }
    Ok(url)
}

/// The only text of the endpoint that is ever shown: `<scheme>://<host>`,
/// lowercase, without port, path, query or credentials (IPv6 bracketed, a
/// domain in punycode).
pub fn describe_endpoint(url: &Url) -> String {
    format!("{}://{}", url.scheme(), url.host_str().unwrap_or_default())
}

/// The run's time budget ended before a request could be made.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BudgetExhausted;

/// At most `per_minute` weight in any sliding 60-second window. Times are the
/// clock's monotonic seconds.
#[derive(Debug)]
pub struct WeightLimiter {
    per_minute: u32,
    events: VecDeque<(f64, u32)>,
    paused_until: f64,
    pub used_total: u64,
}

impl WeightLimiter {
    pub fn new(per_minute: u32) -> Result<Self, ConfigError> {
        if per_minute < BASE_WEIGHT {
            return Err(ConfigError(format!(
                "weight budget must be at least {BASE_WEIGHT} per minute"
            )));
        }
        Ok(Self {
            per_minute,
            events: VecDeque::new(),
            paused_until: 0.0,
            used_total: 0,
        })
    }

    /// How long a request of `weight` must wait at `now`. A request heavier
    /// than the budget passes when the window is empty.
    pub fn wait_time(&mut self, now: f64, weight: u32) -> f64 {
        if now < self.paused_until {
            return self.paused_until - now;
        }
        while self
            .events
            .front()
            .is_some_and(|(moment, _)| *moment <= now - WINDOW)
        {
            self.events.pop_front();
        }
        let used: u64 = self.events.iter().map(|(_, item)| u64::from(*item)).sum();
        let per_minute = u64::from(self.per_minute);
        if used + u64::from(weight) <= per_minute || self.events.is_empty() {
            return 0.0;
        }
        // Wait until enough of the window has expired.
        let mut excess = (used + u64::from(weight) - per_minute) as i64;
        for (moment, item) in &self.events {
            excess -= i64::from(*item);
            if excess <= 0 {
                return moment + WINDOW - now + 0.001;
            }
        }
        WINDOW
    }

    /// Waits until `weight` fits the window and charges it; a wait that would
    /// end after `deadline` is [`BudgetExhausted`] instead.
    pub async fn acquire(
        &mut self,
        clock: &dyn Clock,
        weight: u32,
        deadline: Option<f64>,
    ) -> Result<(), BudgetExhausted> {
        loop {
            let delay = self.wait_time(clock.monotonic(), weight);
            if delay <= 0.0 {
                self.charge(clock.monotonic(), weight);
                return Ok(());
            }
            if deadline.is_some_and(|deadline| clock.monotonic() + delay > deadline) {
                return Err(BudgetExhausted);
            }
            clock.sleep(delay).await;
        }
    }

    /// Records `weight` at `now`, without waiting.
    pub fn charge(&mut self, now: f64, weight: u32) {
        if weight > 0 {
            self.events.push_back((now, weight));
            self.used_total += u64::from(weight);
        }
    }

    /// Holds every request until `now + seconds`: the IP is over its limit.
    pub fn pause(&mut self, now: f64, seconds: f64) {
        self.paused_until = self.paused_until.max(now + seconds);
    }
}

/// `/info` failed after retries, or refused the request. Never contains the
/// URL.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InfoError {
    pub message: String,
    /// The last HTTP status; `None` for a transport error or a response of an
    /// unexpected shape.
    pub status: Option<u16>,
}

impl InfoError {
    fn new(message: String, status: Option<u16>) -> Self {
        Self { message, status }
    }

    /// HTTP 500 after retries: possibly a gap in one entity (`/info` answers
    /// 500 for a third of the tokens' tokenDetails, every time). The jobs
    /// treat it so only while most requests of the same type are answered.
    pub fn entity_error(&self) -> bool {
        self.status == Some(500)
    }
}

impl fmt::Display for InfoError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for InfoError {}

/// Why a request returned no value.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum InfoFailure {
    /// The time budget ended before an attempt could start: stop and commit.
    Budget,
    Error(InfoError),
}

impl From<InfoError> for InfoFailure {
    fn from(error: InfoError) -> Self {
        InfoFailure::Error(error)
    }
}

/// Which budget rule an attempt follows.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Gate {
    /// A snapshot request (outcomeMeta, spotMeta, perpDexs, allPerpMetas):
    /// the budget ends it only when the limiter would wait past the deadline,
    /// so a 0 s budget still writes the snapshots.
    Snapshot,
    /// An entity request (settledOutcome, tokenDetails, fundingHistory): it
    /// also never starts once the deadline has passed (Q6).
    Entity,
}

/// The `/info` client of a run.
pub struct InfoClient {
    transport: Arc<dyn Transport>,
    limiter: WeightLimiter,
    clock: Arc<dyn Clock>,
    jitter: Box<dyn FnMut() -> f64 + Send>,
    log: Logger,
    /// The monotonic time the run's budget ends at; `None` for none.
    pub deadline: Option<f64>,
    /// HTTP attempts, retries included.
    pub requests: u64,
    pub retries: u64,
}

impl InfoClient {
    /// `jitter` returns a value in `[0, 1)`, added to every backoff.
    pub fn new(
        transport: Arc<dyn Transport>,
        per_minute: u32,
        clock: Arc<dyn Clock>,
        jitter: Box<dyn FnMut() -> f64 + Send>,
        log: Logger,
    ) -> Result<Self, ConfigError> {
        Ok(Self {
            transport,
            limiter: WeightLimiter::new(per_minute)?,
            clock,
            jitter,
            log,
            deadline: None,
            requests: 0,
            retries: 0,
        })
    }

    /// The weight charged so far, per-item charges included.
    pub fn used_total(&self) -> u64 {
        self.limiter.used_total
    }

    fn budget_ended(&self) -> bool {
        self.deadline
            .is_some_and(|deadline| self.clock.monotonic() >= deadline)
    }

    /// The parsed answer to `body` and when it was received (UTC µs). HTTP
    /// 5xx gets at most `server_error_attempts` attempts; 429, transport
    /// errors and invalid JSON get six; any other status none more.
    pub async fn request(
        &mut self,
        body: &Json,
        weight: u32,
        server_error_attempts: u32,
        gate: Gate,
    ) -> Result<(Json, i64), InfoFailure> {
        let data = body.canonical().into_bytes();
        let label = subject(body);
        let mut status = None;
        let mut reason = String::new();
        let mut attempt = 0;
        while attempt < MAX_ATTEMPTS {
            attempt += 1;
            if gate == Gate::Entity && self.budget_ended() {
                return Err(InfoFailure::Budget);
            }
            self.limiter
                .acquire(self.clock.as_ref(), weight, self.deadline)
                .await
                .map_err(|BudgetExhausted| InfoFailure::Budget)?;
            self.requests += 1;
            let (answer, failure) = match self.transport.post(data.clone()).await {
                Ok((code, payload)) => (Some((code, payload)), None),
                Err(error) => (None, Some(error.reason())),
            };
            let received = self.clock.now_micros();
            status = answer.as_ref().map(|(code, _)| *code);
            reason = match (answer, failure) {
                (Some((200, payload)), _) => {
                    if payload.len() > MAX_RESPONSE_BYTES {
                        return Err(InfoError::new(
                            format!("{label} response exceeded {MAX_RESPONSE_BYTES} bytes"),
                            Some(200),
                        )
                        .into());
                    }
                    match Json::parse(&payload) {
                        Ok(value) => return Ok((value, received)),
                        Err(_) => "invalid JSON".into(),
                    }
                }
                (Some((code, _)), _) if !RETRY_STATUSES.contains(&code) => {
                    return Err(InfoError::new(
                        format!("{label} refused with HTTP {code}"),
                        Some(code),
                    )
                    .into());
                }
                (Some((code, _)), _) => format!("HTTP {code}"),
                (None, failure) => failure.unwrap_or_default().into(),
            };
            if attempt == MAX_ATTEMPTS
                || status.is_some_and(|code| code >= 500 && attempt >= server_error_attempts)
            {
                break;
            }
            let delay =
                BACKOFF_CAP.min(BACKOFF_INITIAL * 2_f64.powi(attempt as i32 - 1)) + (self.jitter)();
            if status == Some(429) {
                // The whole IP is over its limit: hold every request.
                self.limiter.pause(self.clock.monotonic(), delay);
            }
            self.retries += 1;
            self.log.emit(
                "retry",
                json!({
                    "request": label,
                    "attempt": attempt,
                    "reason": reason,
                    "delay_seconds": (delay * 10.0).round() / 10.0,
                }),
            );
            if self
                .deadline
                .is_some_and(|deadline| self.clock.monotonic() + delay > deadline)
            {
                return Err(InfoError::new(
                    format!("{label} failed ({reason}) and the time budget ended"),
                    status,
                )
                .into());
            }
            self.clock.sleep(delay).await;
        }
        Err(InfoError::new(
            format!("{label} failed after {attempt} attempts ({reason})"),
            status,
        )
        .into())
    }

    async fn snapshot(&mut self, kind: &str) -> Result<(Json, i64), InfoFailure> {
        let body = Json::obj([("type", Json::Str(kind.into()))]);
        self.request(&body, BASE_WEIGHT, SERVER_ERROR_ATTEMPTS, Gate::Snapshot)
            .await
    }

    /// spotMeta: its `tokens` and `universe` must be non-empty lists (USDC and
    /// PURR/USDC always exist), or every entity would read as removed.
    pub async fn spot_meta(&mut self) -> Result<(Json, i64), InfoFailure> {
        let (value, at) = self.snapshot("spotMeta").await?;
        let listed = |key| {
            value
                .get(key)
                .and_then(Json::as_arr)
                .is_some_and(|items| !items.is_empty())
        };
        if !listed("tokens") || !listed("universe") {
            return Err(shape(
                "spotMeta response lacks tokens/universe or lists none",
            ));
        }
        Ok((value, at))
    }

    /// perpDexs: a non-empty list (item 0 is null, the default dex).
    pub async fn perp_dexs(&mut self) -> Result<(Json, i64), InfoFailure> {
        let (value, at) = self.snapshot("perpDexs").await?;
        if value.as_arr().is_none_or(<[Json]>::is_empty) {
            return Err(shape("perpDexs response is not a non-empty list"));
        }
        Ok((value, at))
    }

    /// allPerpMetas: a non-empty list of objects with a `universe` list, the
    /// default dex's non-empty (a new dex may list none).
    pub async fn all_perp_metas(&mut self) -> Result<(Json, i64), InfoFailure> {
        let (value, at) = self.snapshot("allPerpMetas").await?;
        let universe = |meta: &Json| {
            meta.get("universe")
                .and_then(Json::as_arr)
                .map(<[Json]>::len)
        };
        let metas = value.as_arr().unwrap_or_default();
        if metas.is_empty() || metas.iter().any(|meta| universe(meta).is_none()) {
            return Err(shape("allPerpMetas response is not a list of metas"));
        }
        if universe(&metas[0]) == Some(0) {
            return Err(shape("allPerpMetas lists no asset on the default dex"));
        }
        Ok((value, at))
    }

    /// outcomeMeta: `outcomes` and `questions` lists, possibly empty (the jobs'
    /// mass-removal bound handles that).
    pub async fn outcome_meta(&mut self) -> Result<(Json, i64), InfoFailure> {
        let (value, at) = self.snapshot("outcomeMeta").await?;
        let list = |key| value.get(key).and_then(Json::as_arr).is_some();
        if !list("outcomes") || !list("questions") {
            return Err(shape("outcomeMeta response lacks outcomes/questions"));
        }
        Ok((value, at))
    }

    /// settledOutcome: `None` while the outcome is live or does not exist;
    /// else an object whose `spec.outcome` is exactly the integer `id`.
    pub async fn settled_outcome(&mut self, id: i64) -> Result<(Option<Json>, i64), InfoFailure> {
        let body = Json::obj([
            ("type", Json::Str("settledOutcome".into())),
            ("outcome", Json::Int(id.to_string())),
        ]);
        let (value, at) = self
            .request(&body, BASE_WEIGHT, SERVER_ERROR_ATTEMPTS, Gate::Entity)
            .await?;
        if value == Json::Null {
            return Ok((None, at));
        }
        let outcome = value
            .get("spec")
            .filter(|spec| spec.as_obj().is_some())
            .and_then(|spec| spec.get("outcome"));
        if outcome.and_then(Json::as_i64) != Some(id) {
            return Err(shape(&format!(
                "settledOutcome(outcome={id}) response has an unexpected shape"
            )));
        }
        Ok((Some(value), at))
    }

    /// tokenDetails: `None` when `/info` does not know the token; else an
    /// object. `attempts` bounds HTTP 5xx (1 for a daily retry).
    pub async fn token_details(
        &mut self,
        token_id: &str,
        attempts: u32,
    ) -> Result<(Option<Json>, i64), InfoFailure> {
        let body = Json::obj([
            ("type", Json::Str("tokenDetails".into())),
            ("tokenId", Json::Str(token_id.into())),
        ]);
        let (value, at) = self
            .request(&body, BASE_WEIGHT, attempts, Gate::Entity)
            .await?;
        match value {
            Json::Null => Ok((None, at)),
            Json::Obj(_) => Ok((Some(value), at)),
            _ => Err(shape(&format!(
                "tokenDetails(tokenId={token_id}) response is not an object"
            ))),
        }
    }

    /// fundingHistory of `coin` from `start_ms` (to `end_ms`, inclusive):
    /// a list of objects, null as none. Each 20 items returned weigh one more,
    /// charged after the answer.
    pub async fn funding_history(
        &mut self,
        coin: &str,
        start_ms: i64,
        end_ms: Option<i64>,
    ) -> Result<(Vec<Json>, i64), InfoFailure> {
        let mut body = Json::obj([
            ("type", Json::Str("fundingHistory".into())),
            ("coin", Json::Str(coin.into())),
            ("startTime", Json::Int(start_ms.to_string())),
        ]);
        if let (Some(end_ms), Json::Obj(entries)) = (end_ms, &mut body) {
            entries.insert("endTime".into(), Json::Int(end_ms.to_string()));
        }
        let (value, at) = self
            .request(&body, BASE_WEIGHT, SERVER_ERROR_ATTEMPTS, Gate::Entity)
            .await?;
        let entries = match value {
            Json::Null => Vec::new(),
            Json::Arr(entries) if entries.iter().all(|entry| entry.as_obj().is_some()) => entries,
            _ => {
                return Err(shape(&format!(
                    "fundingHistory {coin} response is not a list"
                )))
            }
        };
        let per_item = u32::try_from(entries.len().div_ceil(20)).unwrap_or(u32::MAX);
        self.limiter.charge(self.clock.monotonic(), per_item);
        Ok((entries, at))
    }
}

/// A 200 answer of an unexpected shape: never retried, and a run failure.
fn shape(message: &str) -> InfoFailure {
    InfoFailure::Error(InfoError::new(message.into(), None))
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};

    use super::*;
    use crate::testing::{Answer, Answers, FakeClock, MemoryLog};

    const SECRET_URL: &str = "https://gateway.example/v1/SECRETKEY/info";

    fn answer(status: u16, body: &str) -> Answer {
        Ok((status, body.as_bytes().to_vec()))
    }

    struct Client {
        info: InfoClient,
        transport: Arc<Answers>,
        clock: Arc<FakeClock>,
        log: MemoryLog,
    }

    /// A client over `answers` with jitter 0, as test_info's.
    fn answering(answers: Vec<Answer>, per_minute: u32) -> Client {
        let transport = Arc::new(Answers::new(answers));
        let clock = Arc::new(FakeClock::new());
        let log = MemoryLog::new();
        let info = InfoClient::new(
            transport.clone(),
            per_minute,
            clock.clone(),
            Box::new(|| 0.0),
            log.logger(),
        )
        .unwrap();
        Client {
            info,
            transport,
            clock,
            log,
        }
    }

    fn error(failure: InfoFailure) -> InfoError {
        match failure {
            InfoFailure::Error(error) => error,
            InfoFailure::Budget => panic!("the budget ended"),
        }
    }

    #[tokio::test]
    async fn limiter_sliding_window() {
        let clock = FakeClock::new();
        let mut limiter = WeightLimiter::new(60).unwrap();
        for _ in 0..3 {
            limiter.acquire(&clock, 20, None).await.unwrap();
        }
        assert!(clock.sleeps().is_empty());
        // The fourth waits for the first to leave the window.
        limiter.acquire(&clock, 20, None).await.unwrap();
        let slept: f64 = clock.sleeps().iter().sum();
        assert!((slept - 60.001).abs() < 0.005, "{slept}");
        assert_eq!(limiter.used_total, 80);
    }

    #[tokio::test]
    async fn limiter_deadline_ends_the_budget() {
        let clock = FakeClock::new();
        let mut limiter = WeightLimiter::new(20).unwrap();
        limiter.acquire(&clock, 20, None).await.unwrap();
        let deadline = clock.monotonic() + 10.0;
        assert_eq!(
            limiter.acquire(&clock, 20, Some(deadline)).await,
            Err(BudgetExhausted)
        );
        assert!(clock.sleeps().is_empty());
        // A request heavier than the budget passes an empty window.
        let mut limiter = WeightLimiter::new(20).unwrap();
        assert_eq!(limiter.wait_time(0.0, 100), 0.0);
    }

    #[tokio::test]
    async fn http_429_backs_off_then_succeeds() {
        let spot = r#"{"universe": [{"index": 0}], "tokens": [{"index": 0}]}"#;
        let mut client = answering(
            vec![answer(429, ""), answer(429, ""), answer(200, spot)],
            1200,
        );
        let (value, observed) = client.info.spot_meta().await.unwrap();
        assert_eq!(value, Json::parse(spot.as_bytes()).unwrap());
        assert_eq!(client.transport.calls().len(), 3);
        assert_eq!(client.info.retries, 2);
        assert_eq!(client.info.requests, 3);
        // Each sleep ends exactly at the pause, so no acquire waits again.
        assert_eq!(client.clock.sleeps(), [2.0, 4.0]);
        // Read right after the answer: the start, the time slept and three
        // one-microsecond readings.
        assert_eq!(observed, crate::testing::FAKE_START + 6_000_003);
        let retries: Vec<_> = client
            .log
            .events()
            .into_iter()
            .map(|event| {
                (
                    event["attempt"].clone(),
                    event["reason"].clone(),
                    event["delay_seconds"].clone(),
                )
            })
            .collect();
        assert_eq!(
            retries,
            [
                (json!(1), json!("HTTP 429"), json!(2.0)),
                (json!(2), json!("HTTP 429"), json!(4.0))
            ]
        );
    }

    #[tokio::test]
    async fn errors_never_show_the_url() {
        let mut client = answering(vec![answer(503, "secret body")], 1200);
        let failure = error(client.info.outcome_meta().await.unwrap_err());
        assert_eq!(client.transport.calls().len(), 3);
        assert_eq!(
            failure.message,
            "outcomeMeta failed after 3 attempts (HTTP 503)"
        );
        for leaked in ["SECRETKEY", "gateway.example", "secret body"] {
            assert!(!failure.message.contains(leaked));
            assert!(!client.log.text().contains(leaked));
        }
        // What is shown of the endpoint.
        let url = info_url(Some(SECRET_URL)).unwrap();
        assert_eq!(describe_endpoint(&url), "https://gateway.example");
    }

    #[tokio::test]
    async fn server_errors_get_three_attempts_and_name_the_request() {
        let mut client = answering(vec![answer(500, "")], 1200);
        let failure = error(
            client
                .info
                .token_details("0xabc", SERVER_ERROR_ATTEMPTS)
                .await
                .unwrap_err(),
        );
        assert_eq!(client.transport.calls().len(), 3);
        assert_eq!(failure.status, Some(500));
        assert!(failure.entity_error());
        assert!(failure.message.contains("tokenDetails(tokenId=0xabc)"));
        let mut client = answering(vec![Err(TransportError::Connect)], 1200);
        let failure = error(client.info.outcome_meta().await.unwrap_err());
        assert_eq!(client.transport.calls().len(), 6);
        assert!(!failure.entity_error());
        assert_eq!(failure.status, None);
        assert_eq!(
            failure.message,
            "outcomeMeta failed after 6 attempts (connect)"
        );
        assert_eq!(client.clock.sleeps(), [2.0, 4.0, 8.0, 16.0, 32.0]);
    }

    #[tokio::test]
    async fn a_retry_of_a_failing_entity_gets_one_attempt() {
        let mut client = answering(vec![answer(500, "")], 1200);
        let failure = error(client.info.token_details("0xabc", 1).await.unwrap_err());
        assert_eq!(client.transport.calls().len(), 1);
        assert!(failure.entity_error());
        assert!(client.clock.sleeps().is_empty());
    }

    #[tokio::test]
    async fn empty_metadata_is_a_broken_response() {
        // HTTP 200 with no tokens, pairs or default-dex assets would otherwise
        // read as every entity leaving its snapshot.
        for body in [
            r#"{"universe": [], "tokens": [{"index": 0}]}"#,
            r#"{"universe": [{"index": 0}], "tokens": []}"#,
            r#"{"universe": [{"index": 0}]}"#,
            "[]",
        ] {
            let failure = error(
                answering(vec![answer(200, body)], 1200)
                    .info
                    .spot_meta()
                    .await
                    .unwrap_err(),
            );
            assert_eq!(
                failure.message,
                "spotMeta response lacks tokens/universe or lists none"
            );
            assert_eq!(failure.status, None);
        }
        let mut broken = answering(
            vec![answer(
                200,
                r#"[{"universe": []}, {"universe": [{"name": "xyz:A"}]}]"#,
            )],
            1200,
        );
        let failure = error(broken.info.all_perp_metas().await.unwrap_err());
        assert_eq!(
            failure.message,
            "allPerpMetas lists no asset on the default dex"
        );
        assert_eq!(broken.transport.calls().len(), 1);
        for body in ["[]", r#"[{"universe": [{}]}, 5]"#, r#"[{"universe": {}}]"#] {
            let failure = error(
                answering(vec![answer(200, body)], 1200)
                    .info
                    .all_perp_metas()
                    .await
                    .unwrap_err(),
            );
            assert_eq!(
                failure.message, "allPerpMetas response is not a list of metas",
                "{body}"
            );
        }
        // A new dex may list none.
        let (metas, _) = answering(
            vec![answer(
                200,
                r#"[{"universe": [{"name": "BTC"}]}, {"universe": []}]"#,
            )],
            1200,
        )
        .info
        .all_perp_metas()
        .await
        .unwrap();
        assert_eq!(metas.as_arr().unwrap().len(), 2);
        let failure = error(
            answering(vec![answer(200, "[1]")], 1200)
                .info
                .token_details("0xabc", 3)
                .await
                .unwrap_err(),
        );
        assert_eq!(
            failure.message,
            "tokenDetails(tokenId=0xabc) response is not an object"
        );
        let failure = error(
            answering(vec![answer(200, "[]")], 1200)
                .info
                .perp_dexs()
                .await
                .unwrap_err(),
        );
        assert_eq!(failure.message, "perpDexs response is not a non-empty list");
        let failure = error(
            answering(vec![answer(200, r#"{"outcomes": []}"#)], 1200)
                .info
                .outcome_meta()
                .await
                .unwrap_err(),
        );
        assert_eq!(
            failure.message,
            "outcomeMeta response lacks outcomes/questions"
        );
        let (meta, _) = answering(
            vec![answer(200, r#"{"outcomes": [], "questions": []}"#)],
            1200,
        )
        .info
        .outcome_meta()
        .await
        .unwrap();
        assert!(meta.get("outcomes").is_some());
        let failure = error(
            answering(vec![answer(200, "[1]")], 1200)
                .info
                .funding_history("BTC", 0, None)
                .await
                .unwrap_err(),
        );
        assert_eq!(failure.message, "fundingHistory BTC response is not a list");
    }

    #[tokio::test]
    async fn client_errors_are_not_retried() {
        let mut client = answering(vec![answer(422, "")], 1200);
        let failure = error(client.info.settled_outcome(-1).await.unwrap_err());
        assert_eq!(client.transport.calls().len(), 1);
        assert_eq!(
            failure.message,
            "settledOutcome(outcome=-1) refused with HTTP 422"
        );
        assert_eq!(failure.status, Some(422));
    }

    #[tokio::test]
    async fn connection_errors_and_bad_json_are_retried() {
        let mut client = answering(
            vec![
                Err(TransportError::Connect),
                answer(200, "{trunc"),
                answer(200, "null"),
            ],
            1200,
        );
        let (value, _) = client.info.settled_outcome(5).await.unwrap();
        assert_eq!(value, None);
        assert_eq!(client.transport.calls().len(), 3);
        let reasons: Vec<_> = client
            .log
            .events()
            .into_iter()
            .map(|event| event["reason"].clone())
            .collect();
        assert_eq!(reasons, [json!("connect"), json!("invalid JSON")]);
    }

    #[tokio::test]
    async fn settled_outcome_must_match_the_id() {
        for body in [
            r#"{"spec": {"outcome": 6}, "settleFraction": "1.0"}"#,
            // Not Python's `true == 1` (Q12).
            r#"{"spec": {"outcome": true}}"#,
            r#"{"spec": {"outcome": 1.0}}"#,
            r#"{"spec": 1}"#,
            "[]",
        ] {
            let id = if body.contains("true") || body.contains("1.0}") {
                1
            } else {
                5
            };
            let failure = error(
                answering(vec![answer(200, body)], 1200)
                    .info
                    .settled_outcome(id)
                    .await
                    .unwrap_err(),
            );
            assert_eq!(
                failure.message,
                format!("settledOutcome(outcome={id}) response has an unexpected shape"),
                "{body}"
            );
        }
        let (value, _) = answering(vec![answer(200, r#"{"spec": {"outcome": 5}}"#)], 1200)
            .info
            .settled_outcome(5)
            .await
            .unwrap();
        assert!(value.is_some());
    }

    #[tokio::test]
    async fn funding_history_is_charged_per_item() {
        let page: Vec<String> = (0..41)
            .map(|time| {
                format!(r#"{{"coin":"BTC","fundingRate":"0.0","premium":"0.0","time":{time}}}"#)
            })
            .collect();
        let mut client = answering(vec![answer(200, &format!("[{}]", page.join(",")))], 1200);
        let (value, _) = client
            .info
            .funding_history("BTC", 0, Some(100))
            .await
            .unwrap();
        assert_eq!(value.len(), 41);
        assert_eq!(
            client.transport.calls()[0],
            Json::parse(br#"{"type":"fundingHistory","coin":"BTC","startTime":0,"endTime":100}"#)
                .unwrap()
        );
        assert_eq!(client.info.used_total(), 20 + 3);
        // Null is no entry.
        let (value, _) = answering(vec![answer(200, "null")], 1200)
            .info
            .funding_history("BTC", 0, None)
            .await
            .unwrap();
        assert!(value.is_empty());
    }

    #[tokio::test]
    async fn budget_ends_requests() {
        let mut client = answering(vec![answer(200, "null")], 20);
        client.info.deadline = Some(client.clock.monotonic() + 5.0);
        client.info.settled_outcome(1).await.unwrap();
        assert_eq!(
            client.info.settled_outcome(2).await,
            Err(InfoFailure::Budget)
        );
        assert_eq!(client.transport.calls().len(), 1);
    }

    #[tokio::test]
    async fn entity_requests_never_start_after_the_deadline() {
        let mut client = answering(vec![answer(200, "null")], 100_000);
        client.info.deadline = Some(client.clock.monotonic());
        assert_eq!(
            client.info.settled_outcome(1).await,
            Err(InfoFailure::Budget)
        );
        assert_eq!(
            client.info.token_details("0xabc", 3).await,
            Err(InfoFailure::Budget)
        );
        assert_eq!(
            client.info.funding_history("BTC", 0, None).await,
            Err(InfoFailure::Budget)
        );
        assert!(client.transport.calls().is_empty());
        assert_eq!(client.info.requests, 0);
    }

    #[tokio::test]
    async fn snapshot_requests_keep_the_python_budget_rule() {
        let spot = r#"{"universe": [{"index": 0}], "tokens": [{"index": 0}]}"#;
        let mut client = answering(vec![answer(200, spot)], 20);
        // A 0 s budget: the snapshot is still asked, as Python did.
        client.info.deadline = Some(client.clock.monotonic());
        client.info.spot_meta().await.unwrap();
        // The next one would wait for the window past the deadline.
        assert_eq!(client.info.spot_meta().await, Err(InfoFailure::Budget));
        assert_eq!(client.transport.calls().len(), 1);
    }

    #[tokio::test]
    async fn a_backoff_past_the_deadline_fails_the_request() {
        let mut client = answering(vec![answer(500, "")], 1200);
        client.info.deadline = Some(client.clock.monotonic() + 3.0);
        let failure = error(client.info.settled_outcome(7).await.unwrap_err());
        // An InfoError, not the budget: a 500 is still the entity's gap.
        assert_eq!(
            failure.message,
            "settledOutcome(outcome=7) failed (HTTP 500) and the time budget ended"
        );
        assert!(failure.entity_error());
        assert_eq!(client.transport.calls().len(), 2);
        assert_eq!(client.clock.sleeps(), [2.0]);
    }

    #[tokio::test]
    async fn oversized_answers_fail_without_retry() {
        let mut client = answering(vec![Ok((200, vec![b' '; MAX_RESPONSE_BYTES + 1]))], 1200);
        let failure = error(client.info.spot_meta().await.unwrap_err());
        assert_eq!(failure.message, "spotMeta response exceeded 67108864 bytes");
        assert_eq!(failure.status, Some(200));
        assert_eq!(client.transport.calls().len(), 1);
    }

    #[tokio::test]
    async fn three_xx_is_refused() {
        let mut client = answering(vec![answer(302, "")], 1200);
        let failure = error(client.info.outcome_meta().await.unwrap_err());
        assert_eq!(failure.message, "outcomeMeta refused with HTTP 302");
        assert_eq!(client.transport.calls().len(), 1);
    }

    #[test]
    fn weight_floor_is_a_config_error() {
        let message = ConfigError("weight budget must be at least 20 per minute".into());
        assert_eq!(WeightLimiter::new(19).unwrap_err(), message);
        assert!(WeightLimiter::new(20).is_ok());
        let failure = InfoClient::new(
            Arc::new(Answers::new(vec![answer(200, "null")])),
            19,
            Arc::new(FakeClock::new()),
            Box::new(|| 0.0),
            MemoryLog::new().logger(),
        );
        assert_eq!(failure.err(), Some(message));
    }

    #[test]
    fn only_http_urls_are_accepted() {
        let invalid = Err(ConfigError(URL_INVALID.into()));
        for text in [
            "file:///etc/passwd",
            "https:///info",
            "https:/info",
            "https:info",
            "https://",
            "https://?q",
            "ftp://example.com/info",
            "example.com/info",
            "\n",
        ] {
            assert_eq!(info_url(Some(text)), invalid, "{text:?}");
        }
        assert_eq!(
            info_url(Some("http://example.com/info")),
            Err(ConfigError(URL_NOT_HTTPS.into()))
        );
        for text in [
            "http://127.0.0.1:1/info",
            "http://127.9.0.1/info",
            "http://[::1]:1/info",
            "http://localhost:1/",
            "HTTPS://GW.Example/v1/KEY/info",
        ] {
            assert!(info_url(Some(text)).is_ok(), "{text}");
        }
        // A Secret with a trailing newline or a tab is normalized, and no error
        // ever shows the value (Q7).
        let url = info_url(Some("https://gw.example/v1/SECRET\tKEY/info\n")).unwrap();
        assert_eq!(url.as_str(), "https://gw.example/v1/SECRETKEY/info");
        let refused = info_url(Some("gw.example/v1/SECRETKEY/info\n")).unwrap_err();
        assert!(!refused.0.contains("SECRETKEY"));
        // Only the scheme and host are ever shown (Q21).
        let show = |text: &str| describe_endpoint(&info_url(Some(text)).unwrap());
        assert_eq!(show("http://[::1]:1/info"), "http://[::1]");
        assert_eq!(
            show("https://user:pass@GW.Example:8443/x?key=1"),
            "https://gw.example"
        );
        assert_eq!(
            show("https://bücher.example/info"),
            "https://xn--bcher-kva.example"
        );
        // Empty means unset: the public endpoint.
        for text in [None, Some("")] {
            assert_eq!(info_url(text).unwrap().as_str(), PUBLIC_INFO_URL);
        }
        assert_eq!(show(PUBLIC_INFO_URL), "https://api.hyperliquid.xyz");
    }

    /// The request head and body one connection sent.
    async fn read_request(socket: &mut TcpStream) -> Option<(String, Vec<u8>)> {
        let mut buffer = Vec::new();
        let mut chunk = [0; 4096];
        let end = loop {
            if let Some(end) = buffer.windows(4).position(|window| window == b"\r\n\r\n") {
                break end;
            }
            let read = socket.read(&mut chunk).await.ok()?;
            if read == 0 {
                return None;
            }
            buffer.extend_from_slice(&chunk[..read]);
        };
        let head = String::from_utf8_lossy(&buffer[..end]).to_lowercase();
        let length: usize = head
            .lines()
            .find_map(|line| line.strip_prefix("content-length:"))
            .and_then(|value| value.trim().parse().ok())
            .unwrap_or(0);
        while buffer.len() < end + 4 + length {
            let read = socket.read(&mut chunk).await.ok()?;
            if read == 0 {
                return None;
            }
            buffer.extend_from_slice(&chunk[..read]);
        }
        Some((head, buffer[end + 4..end + 4 + length].to_vec()))
    }

    /// Serves the same answer, `head` then `body_length` bytes of `x`, to
    /// every connection on a loopback port, recording each request.
    async fn serve(head: String, body_length: usize) -> (Url, Arc<Mutex<Vec<(String, Vec<u8>)>>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let seen = requests.clone();
        tokio::spawn(async move {
            while let Ok((mut socket, _)) = listener.accept().await {
                let (head, seen) = (head.clone(), seen.clone());
                tokio::spawn(async move {
                    let Some(request) = read_request(&mut socket).await else {
                        return;
                    };
                    seen.lock().unwrap().push(request);
                    if socket.write_all(head.as_bytes()).await.is_err() {
                        return;
                    }
                    let chunk = vec![b'x'; 1 << 16];
                    let mut left = body_length;
                    while left > 0 {
                        let size = left.min(chunk.len());
                        // The client stops reading at the cap.
                        if socket.write_all(&chunk[..size]).await.is_err() {
                            return;
                        }
                        left -= size;
                    }
                    let _ = socket.shutdown().await;
                });
            }
        });
        let url = info_url(Some(&format!("http://127.0.0.1:{port}/v1/SECRETKEY/info"))).unwrap();
        (url, requests)
    }

    #[tokio::test]
    async fn http_transport_drops_error_bodies_caps_answers_and_hides_the_url() {
        let body = br#"{"type":"spotMeta"}"#.to_vec();
        // A redirect is answered as it is, not followed.
        let (url, requests) = serve(
            "HTTP/1.1 302 Found\r\nLocation: /elsewhere\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".into(),
            0,
        )
        .await;
        let transport = HttpTransport::new(url).unwrap();
        let (status, answer) = transport.post(body.clone()).await.unwrap();
        assert_eq!((status, answer.len()), (302, 0));
        {
            let requests = requests.lock().unwrap();
            assert_eq!(requests.len(), 1);
            let (head, sent) = &requests[0];
            assert!(
                head.starts_with("post /v1/secretkey/info http/1.1\r\n"),
                "{head}"
            );
            for header in [
                "content-type: application/json",
                "accept: application/json",
                "user-agent: pinax-hypercore-reference/",
            ] {
                assert!(head.contains(header), "{head}");
            }
            assert_eq!(sent, &body);
        }
        // An error's body is never returned.
        let (url, _) = serve(
            "HTTP/1.1 503 Service Unavailable\r\nContent-Length: 11\r\nConnection: close\r\n\r\nsecret body".into(),
            0,
        )
        .await;
        let (status, answer) = HttpTransport::new(url)
            .unwrap()
            .post(body.clone())
            .await
            .unwrap();
        assert_eq!((status, answer.len()), (503, 0));
        // An answer is read up to the cap plus one byte.
        let length = MAX_RESPONSE_BYTES + 100;
        let (url, _) = serve(
            format!("HTTP/1.1 200 OK\r\nContent-Length: {length}\r\nConnection: close\r\n\r\n"),
            length,
        )
        .await;
        let (status, answer) = HttpTransport::new(url)
            .unwrap()
            .post(body.clone())
            .await
            .unwrap();
        assert_eq!((status, answer.len()), (200, MAX_RESPONSE_BYTES + 1));
        // A refused connection is "connect", with nothing of the URL.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        let url = info_url(Some(&format!("http://127.0.0.1:{port}/v1/SECRETKEY/info"))).unwrap();
        let failure = HttpTransport::new(url)
            .unwrap()
            .post(body)
            .await
            .unwrap_err();
        assert_eq!(failure, TransportError::Connect);
        assert_eq!(failure.reason(), "connect");
        assert!(!format!("{failure:?}").contains("SECRETKEY"));
        // Plain http only for a loopback host: the client itself refuses it.
        let public = Url::parse("http://example.com/info").unwrap();
        let refused = HttpTransport::new(public)
            .unwrap()
            .post(Vec::new())
            .await
            .unwrap_err();
        assert_eq!(refused, TransportError::Request);
    }
}

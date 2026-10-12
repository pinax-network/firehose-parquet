//! Test doubles: a clock, an `/info` that answers from the recorded fixtures,
//! a transport that replays given answers, and an in-memory log. Built for
//! this crate's tests and, with the `test-support` feature, for `blocks`'s
//! (the `/info` fixture server, the reference joins). No test touches the
//! network.
//!
//! The fixtures are real `api.hyperliquid.xyz/info` responses recorded on
//! 2026-10-08 and trimmed to a few entities (`tests/fixtures/README.md`).

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::io::Write;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use bytes::Bytes;
use futures::future::{self, BoxFuture};
use futures::FutureExt as _;
use serde_json::Value;

use crate::clock::Clock;
use crate::info::{Transport, TransportError, FUNDING_PAGE};
use crate::json::Json;
use crate::log::Logger;

/// 2026-10-08T12:00:00Z, where a [`FakeClock`]'s wall clock starts.
pub const FAKE_START: i64 = 1_791_460_800_000_000;

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Monotonic seconds and a UTC wall clock that move together. Sleeping
/// records the delay and advances both at once; every wall clock reading
/// first advances one microsecond, so no two observed times are equal.
pub struct FakeClock {
    state: Mutex<ClockState>,
}

struct ClockState {
    /// The wall clock at monotonic 0, in microseconds.
    start: i64,
    seconds: f64,
    sleeps: Vec<f64>,
}

impl FakeClock {
    pub fn new() -> Self {
        Self {
            state: Mutex::new(ClockState {
                start: FAKE_START,
                seconds: 0.0,
                sleeps: Vec::new(),
            }),
        }
    }

    pub fn advance(&self, seconds: f64) {
        lock(&self.state).seconds += seconds;
    }

    /// Places the wall clock: it reads `start` plus the monotonic seconds.
    pub fn set_wall(&self, start: i64) {
        lock(&self.state).start = start;
    }

    /// Every sleep, in order.
    pub fn sleeps(&self) -> Vec<f64> {
        lock(&self.state).sleeps.clone()
    }
}

impl Default for FakeClock {
    fn default() -> Self {
        Self::new()
    }
}

impl Clock for FakeClock {
    fn monotonic(&self) -> f64 {
        lock(&self.state).seconds
    }

    fn now_micros(&self) -> i64 {
        let mut state = lock(&self.state);
        state.seconds += 0.000_001;
        state.start + (state.seconds * 1_000_000.0).round() as i64
    }

    fn sleep(&self, seconds: f64) -> BoxFuture<'static, ()> {
        let mut state = lock(&self.state);
        state.sleeps.push(seconds);
        state.seconds += seconds;
        future::ready(()).boxed()
    }
}

macro_rules! fixture {
    ($name:literal) => {
        include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/",
            $name
        ))
    };
}

/// A recorded fixture, parsed.
fn parsed(text: &str) -> Json {
    Json::parse(text.as_bytes()).expect("the fixtures are JSON")
}

/// What a [`FakeInfo`] answers; a test changes it through [`FakeInfo::edit`].
pub struct FakeState {
    /// outcomeMeta: `outcomeMeta_t0.json`, or what a test sets.
    pub outcome_meta: Json,
    pub spot_meta: Json,
    pub perp_dexs: Json,
    pub all_perp_metas: Json,
    /// settledOutcome by id (`settledOutcome_<id>.json`); null for others.
    pub settled: BTreeMap<i64, Json>,
    /// tokenDetails by `tokenId`: HSEI, USDC and ASI; null for others.
    pub token_details: HashMap<String, Json>,
    /// fundingHistory entries by coin: BTC, `xyz:XYZ100` and MATIC.
    pub funding: BTreeMap<String, Vec<Json>>,
    /// HTTP statuses (with an empty body) answered first, by request type.
    pub statuses: HashMap<String, VecDeque<u16>>,
    /// The most fundingHistory items in one answer.
    pub page: usize,
}

/// A transport answering like `/info` from the fixtures, recording every
/// request. fundingHistory answers a coin's entries with
/// `startTime <= time <= endTime` (`endTime` 2^62 by default), at most a page
/// of them: `[]` for a known coin with none in range, null for an unknown
/// one. Any other request type gets HTTP 400.
pub struct FakeInfo {
    state: Mutex<FakeState>,
    calls: Mutex<Vec<Json>>,
}

impl FakeInfo {
    pub fn new() -> Self {
        let spot_meta = parsed(fixture!("spotMeta.json"));
        let token_id = |name: &str| -> String {
            spot_meta
                .get("tokens")
                .and_then(Json::as_arr)
                .into_iter()
                .flatten()
                .find(|token| token.get("name").and_then(Json::as_str) == Some(name))
                .and_then(|token| token.get("tokenId").and_then(Json::as_str))
                .expect("the fixture lists the token")
                .to_owned()
        };
        let token_details = [
            ("HSEI", fixture!("tokenDetails_HSEI.json")),
            ("USDC", fixture!("tokenDetails_USDC.json")),
            ("ASI", fixture!("tokenDetails_ASI.json")),
        ]
        .into_iter()
        .map(|(name, text)| (token_id(name), parsed(text)))
        .collect();
        let settled = [
            (0, fixture!("settledOutcome_0.json")),
            (1473, fixture!("settledOutcome_1473.json")),
            (7768, fixture!("settledOutcome_7768.json")),
            (9174, fixture!("settledOutcome_9174.json")),
            (9973, fixture!("settledOutcome_9973.json")),
            (9974, fixture!("settledOutcome_9974.json")),
            (9975, fixture!("settledOutcome_9975.json")),
            (9976, fixture!("settledOutcome_9976.json")),
        ]
        .into_iter()
        .map(|(id, text)| (id, parsed(text)))
        .collect();
        let funding = [
            fixture!("fundingHistory_BTC.json"),
            fixture!("fundingHistory_xyz_XYZ100.json"),
            fixture!("fundingHistory_MATIC.json"),
        ]
        .into_iter()
        .map(|text| {
            let entries = parsed(text).as_arr().expect("a list").to_vec();
            let coin = entries[0]
                .get("coin")
                .and_then(Json::as_str)
                .expect("a coin");
            (coin.to_owned(), entries)
        })
        .collect();
        Self {
            state: Mutex::new(FakeState {
                outcome_meta: parsed(fixture!("outcomeMeta_t0.json")),
                spot_meta,
                perp_dexs: parsed(fixture!("perpDexs.json")),
                all_perp_metas: parsed(fixture!("allPerpMetas.json")),
                settled,
                token_details,
                funding,
                statuses: HashMap::new(),
                page: FUNDING_PAGE,
            }),
            calls: Mutex::new(Vec::new()),
        }
    }

    /// outcomeMeta at its second recording, five minutes later.
    pub fn outcome_meta_t1() -> Json {
        parsed(fixture!("outcomeMeta_t1.json"))
    }

    /// Changes what it answers.
    pub fn edit<R>(&self, change: impl FnOnce(&mut FakeState) -> R) -> R {
        change(&mut lock(&self.state))
    }

    /// Every request body received, in order.
    pub fn calls(&self) -> Vec<Json> {
        lock(&self.calls).clone()
    }

    /// The requests of a type.
    pub fn count(&self, kind: &str) -> usize {
        lock(&self.calls)
            .iter()
            .filter(|call| call.get("type").and_then(Json::as_str) == Some(kind))
            .count()
    }

    /// The answer to a request body: `(status, body)`.
    pub fn answer(&self, body: &[u8]) -> (u16, Vec<u8>) {
        let Ok(request) = Json::parse(body) else {
            return (400, Vec::new());
        };
        lock(&self.calls).push(request.clone());
        let kind = request
            .get("type")
            .and_then(Json::as_str)
            .unwrap_or_default();
        let mut state = lock(&self.state);
        if let Some(status) = state.statuses.get_mut(kind).and_then(VecDeque::pop_front) {
            return (status, Vec::new());
        }
        let value = match kind {
            "outcomeMeta" => state.outcome_meta.clone(),
            "spotMeta" => state.spot_meta.clone(),
            "perpDexs" => state.perp_dexs.clone(),
            "allPerpMetas" => state.all_perp_metas.clone(),
            "settledOutcome" => request
                .get("outcome")
                .and_then(Json::as_i64)
                .and_then(|id| state.settled.get(&id).cloned())
                .unwrap_or(Json::Null),
            "tokenDetails" => request
                .get("tokenId")
                .and_then(Json::as_str)
                .and_then(|token_id| state.token_details.get(token_id).cloned())
                .unwrap_or(Json::Null),
            "fundingHistory" => {
                let coin = request
                    .get("coin")
                    .and_then(Json::as_str)
                    .unwrap_or_default();
                let start = request.get("startTime").and_then(Json::as_i64).unwrap_or(0);
                let end = request
                    .get("endTime")
                    .and_then(Json::as_i64)
                    .unwrap_or(1 << 62);
                match state.funding.get(coin) {
                    Some(entries) => Json::Arr(
                        entries
                            .iter()
                            .filter(|entry| {
                                entry
                                    .get("time")
                                    .and_then(Json::as_i64)
                                    .is_some_and(|time| start <= time && time <= end)
                            })
                            .take(state.page)
                            .cloned()
                            .collect(),
                    ),
                    None => Json::Null,
                }
            }
            _ => return (400, Vec::new()),
        };
        (200, value.canonical().into_bytes())
    }
}

impl Default for FakeInfo {
    fn default() -> Self {
        Self::new()
    }
}

impl Transport for FakeInfo {
    fn post<'a>(&'a self, body: Vec<u8>) -> BoxFuture<'a, Result<(u16, Bytes), TransportError>> {
        let (status, body) = self.answer(&body);
        future::ready(Ok((status, Bytes::from(body)))).boxed()
    }
}

/// One answer of an [`Answers`] transport.
pub type Answer = Result<(u16, Vec<u8>), TransportError>;

/// A transport that gives its answers in order, then repeats the last one,
/// recording every request body.
pub struct Answers {
    answers: Vec<Answer>,
    calls: Mutex<Vec<Json>>,
}

impl Answers {
    pub fn new(answers: Vec<Answer>) -> Self {
        assert!(!answers.is_empty(), "at least one answer");
        Self {
            answers,
            calls: Mutex::new(Vec::new()),
        }
    }

    /// Every request body received, in order.
    pub fn calls(&self) -> Vec<Json> {
        lock(&self.calls).clone()
    }
}

impl Transport for Answers {
    fn post<'a>(&'a self, body: Vec<u8>) -> BoxFuture<'a, Result<(u16, Bytes), TransportError>> {
        let mut calls = lock(&self.calls);
        calls.push(Json::parse(&body).expect("the client sends JSON"));
        let answer = &self.answers[calls.len().min(self.answers.len()) - 1];
        future::ready(
            answer
                .clone()
                .map(|(status, body)| (status, Bytes::from(body))),
        )
        .boxed()
    }
}

/// A [`Logger`] into memory.
#[derive(Clone)]
pub struct MemoryLog {
    buffer: Arc<Mutex<Vec<u8>>>,
    logger: Logger,
}

struct Shared(Arc<Mutex<Vec<u8>>>);

impl Write for Shared {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        lock(&self.0).extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl MemoryLog {
    pub fn new() -> Self {
        Self::with_secrets(Vec::new())
    }

    /// A log that redacts `secrets`, as the job's does.
    pub fn with_secrets(secrets: Vec<String>) -> Self {
        let buffer = Arc::new(Mutex::new(Vec::new()));
        let logger = Logger::new(Box::new(Shared(Arc::clone(&buffer))), secrets);
        Self { buffer, logger }
    }

    pub fn logger(&self) -> Logger {
        self.logger.clone()
    }

    /// Everything written.
    pub fn text(&self) -> String {
        String::from_utf8(lock(&self.buffer).clone()).expect("JSON lines are UTF-8")
    }

    /// Every line, parsed.
    pub fn events(&self) -> Vec<Value> {
        self.text()
            .lines()
            .map(|line| serde_json::from_str(line).expect("a JSON line"))
            .collect()
    }
}

impl Default for MemoryLog {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_fake_clock_moves_like_the_python_one() {
        let clock = FakeClock::new();
        assert_eq!(clock.monotonic(), 0.0);
        assert_eq!(clock.now_micros(), FAKE_START + 1);
        assert_eq!(clock.now_micros(), FAKE_START + 2);
        assert_eq!(clock.sleep(2.0).now_or_never(), Some(()));
        clock.advance(1.0);
        assert_eq!(clock.sleeps(), [2.0]);
        assert_eq!(clock.now_micros(), FAKE_START + 3_000_003);
        clock.set_wall(0);
        assert_eq!(clock.now_micros(), 3_000_004);
    }

    #[test]
    fn fake_info_answers_from_the_fixtures() {
        let info = FakeInfo::new();
        let answer = |body: &str| {
            let (status, body) = info.answer(body.as_bytes());
            (status, Json::parse(&body).ok())
        };
        let (status, spot) = answer(r#"{"type":"spotMeta"}"#);
        assert_eq!(status, 200);
        assert_eq!(
            spot.unwrap().get("tokens").unwrap().as_arr().unwrap().len(),
            9
        );
        assert_eq!(
            answer(r#"{"type":"settledOutcome","outcome":1473}"#).1,
            Some(Json::Null)
        );
        assert_eq!(
            answer(r#"{"type":"settledOutcome","outcome":5}"#).1,
            Some(Json::Null)
        );
        let (_, settled) = answer(r#"{"type":"settledOutcome","outcome":7768}"#);
        assert!(settled.unwrap().get("spec").is_some());
        let funding = |body: &str| answer(body).1.unwrap().as_arr().map(<[Json]>::len);
        assert_eq!(
            funding(r#"{"type":"fundingHistory","coin":"BTC","startTime":0}"#),
            Some(6)
        );
        assert_eq!(
            funding(
                r#"{"type":"fundingHistory","coin":"BTC","startTime":0,"endTime":1767229200053}"#
            ),
            Some(2)
        );
        assert_eq!(
            funding(r#"{"type":"fundingHistory","coin":"BTC","startTime":1767243600041}"#),
            Some(0)
        );
        assert_eq!(
            answer(r#"{"type":"fundingHistory","coin":"ETH","startTime":0}"#).1,
            Some(Json::Null)
        );
        info.edit(|state| state.page = 2);
        assert_eq!(
            funding(r#"{"type":"fundingHistory","coin":"MATIC","startTime":0}"#),
            Some(2)
        );
        info.edit(|state| {
            state
                .statuses
                .insert("outcomeMeta".into(), [503, 500].into())
        });
        assert_eq!(answer(r#"{"type":"outcomeMeta"}"#), (503, None));
        assert_eq!(answer(r#"{"type":"outcomeMeta"}"#), (500, None));
        assert_eq!(answer(r#"{"type":"outcomeMeta"}"#).0, 200);
        assert_eq!(answer(r#"{"type":"userFills"}"#), (400, None));
        assert_eq!(info.count("outcomeMeta"), 3);
        // Unknown types are recorded too.
        assert_eq!(info.calls().len(), 13);
        let (_, details) =
            answer(r#"{"type":"tokenDetails","tokenId":"0x4519b5c4a2fd0beb66306336f63c7b99"}"#);
        assert!(details.unwrap().get("deployer").is_some());
        assert_ne!(
            FakeInfo::outcome_meta_t1(),
            info.edit(|state| state.outcome_meta.clone())
        );
    }
}

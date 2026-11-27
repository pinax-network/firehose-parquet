//! The reconnect back-off of [`super::ReconnectState`].
//!
//! It keeps the schedule of the `backoff` 0.4 crate's `ExponentialBackoff`
//! (RUSTSEC-2025-0012, unmaintained), which it replaces (#633), as
//! `ReconnectState` configured it: its defaults (a randomization factor of
//! 0.5 and a multiplier of 1.5) with a 1 s initial and a 60 s maximum interval
//! and no elapsed-time limit, so a delay is always returned.
use std::hash::{BuildHasher, Hasher};
use std::time::Duration;

/// The first interval, and the interval after [`ReconnectBackoff::reset`].
const INITIAL_INTERVAL: Duration = Duration::from_secs(1);
/// The interval stops growing here.
const MAX_INTERVAL: Duration = Duration::from_secs(60);
/// Each delay multiplies the interval by this, up to [`MAX_INTERVAL`].
const MULTIPLIER: f64 = 1.5;
/// A delay is drawn uniformly from this fraction below to this fraction above
/// the interval.
const RANDOMIZATION_FACTOR: f64 = 0.5;

/// Exponential back-off with jitter: the n-th delay since the last reset is
/// uniform in ±50 % of `min(1 s × 1.5^(n-1), 60 s)`, so from 0.5–1.5 s for the
/// first to 30–90 s once the interval reaches its cap, at the 12th.
#[derive(Debug, Clone)]
pub(super) struct ReconnectBackoff {
    interval: Duration,
}

impl ReconnectBackoff {
    pub(super) fn new() -> Self {
        Self {
            interval: INITIAL_INTERVAL,
        }
    }

    /// Start again from the initial interval.
    pub(super) fn reset(&mut self) {
        self.interval = INITIAL_INTERVAL;
    }

    /// The next delay; the interval then grows.
    pub(super) fn next_delay(&mut self) -> Duration {
        self.next_delay_with(unit_random())
    }

    /// [`Self::next_delay`] with `random` in `[0, 1)` choosing the point in
    /// the interval's range, from its low end at 0.
    fn next_delay_with(&mut self, random: f64) -> Duration {
        let delay = randomized(self.interval, random);
        self.interval = grown(self.interval);
        delay
    }
}

/// `interval` ± [`RANDOMIZATION_FACTOR`], at `random` in that range, in
/// nanoseconds as `backoff` 0.4 computed it (the `+ 1.0` makes both ends
/// reachable).
fn randomized(interval: Duration, random: f64) -> Duration {
    let nanos = interval.as_nanos() as f64;
    let delta = RANDOMIZATION_FACTOR * nanos;
    let (min, max) = (nanos - delta, nanos + delta);
    Duration::from_nanos((min + random * (max - min + 1.0)) as u64)
}

/// The interval after `interval`: [`MULTIPLIER`] times longer, or
/// [`MAX_INTERVAL`] once that would reach it.
fn grown(interval: Duration) -> Duration {
    if interval.as_secs_f64() >= MAX_INTERVAL.as_secs_f64() / MULTIPLIER {
        MAX_INTERVAL
    } else {
        interval.mul_f64(MULTIPLIER)
    }
}

/// A uniform value in `[0, 1)` from the standard library's per-process random
/// hash keys: jitter needs no stronger source (as `delta/store/read_retry.rs`).
fn unit_random() -> f64 {
    let bits = std::collections::hash_map::RandomState::new()
        .build_hasher()
        .finish();
    (bits >> 11) as f64 / (1u64 << 53) as f64
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The intervals `backoff` 0.4 grew from 1 s by 1.5 up to 60 s.
    const INTERVALS_SECS: [f64; 13] = [
        1.0,
        1.5,
        2.25,
        3.375,
        5.0625,
        7.59375,
        11.390625,
        17.0859375,
        25.62890625,
        38.443359375,
        57.6650390625,
        60.0,
        60.0,
    ];

    fn secs(duration: Duration) -> f64 {
        duration.as_secs_f64()
    }

    fn close(actual: Duration, expected_secs: f64) -> bool {
        (secs(actual) - expected_secs).abs() < 1e-6
    }

    /// The low end (random 0) is half the interval and the high end (random
    /// just below 1) one and a half times it, attempt by attempt, up to the
    /// 60 s cap, which then holds.
    #[test]
    fn schedule_grows_by_half_to_the_cap_with_fifty_percent_jitter() {
        let mut low = ReconnectBackoff::new();
        let mut high = ReconnectBackoff::new();
        let mut middle = ReconnectBackoff::new();
        for (attempt, interval) in INTERVALS_SECS.into_iter().enumerate() {
            let lowest = low.next_delay_with(0.0);
            let highest = high.next_delay_with(1.0 - f64::EPSILON);
            let centre = middle.next_delay_with(0.5);
            assert!(close(lowest, interval * 0.5), "{attempt}: {lowest:?}");
            assert!(close(highest, interval * 1.5), "{attempt}: {highest:?}");
            assert!(close(centre, interval), "{attempt}: {centre:?}");
        }
        for _ in 0..100 {
            assert_eq!(low.next_delay_with(0.0), Duration::from_secs(30));
            assert!(close(high.next_delay_with(1.0 - f64::EPSILON), 90.0));
        }
    }

    /// Real delays stay within each attempt's bounds and vary.
    #[test]
    fn random_delays_stay_within_each_attempts_bounds() {
        for _ in 0..200 {
            let mut backoff = ReconnectBackoff::new();
            for interval in INTERVALS_SECS {
                let delay = secs(backoff.next_delay());
                assert!(
                    delay >= interval * 0.5 - 1e-9 && delay <= interval * 1.5 + 1e-9,
                    "{delay} for {interval}"
                );
            }
        }
        let firsts: std::collections::BTreeSet<_> = (0..20)
            .map(|_| ReconnectBackoff::new().next_delay())
            .collect();
        assert!(firsts.len() > 1, "the delays are randomized");
        for _ in 0..1000 {
            let random = unit_random();
            assert!((0.0..1.0).contains(&random), "{random}");
        }
    }

    /// A reset returns to the 1 s interval, from anywhere in the schedule.
    #[test]
    fn reset_returns_to_the_initial_interval() {
        let mut backoff = ReconnectBackoff::new();
        for _ in 0..20 {
            backoff.next_delay_with(0.5);
        }
        assert!(close(backoff.next_delay_with(0.5), 60.0));
        backoff.reset();
        assert!(close(backoff.next_delay_with(0.5), 1.0));
        assert!(close(backoff.next_delay_with(0.5), 1.5));
        backoff.reset();
        assert!(close(backoff.next_delay_with(0.0), 0.5));
    }
}

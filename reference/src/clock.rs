//! Time for the `/info` client and the jobs: monotonic seconds for the weight
//! limiter and the time budget, the UTC wall clock for `observed_at` and the
//! jobs' hour rules, and sleeping. Tests drive a fake one
//! (`testing::FakeClock`); Delta's own clocks (tombstones, VACUUM retention)
//! stay real time.

use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use futures::future::{self, BoxFuture};
use futures::FutureExt as _;

pub trait Clock: Send + Sync {
    /// Seconds since an arbitrary origin, never going back.
    fn monotonic(&self) -> f64;
    /// The UTC wall clock in microseconds since the epoch, truncated.
    fn now_micros(&self) -> i64;
    /// Waits `seconds` (none for a negative or non-finite value).
    fn sleep(&self, seconds: f64) -> BoxFuture<'static, ()>;
}

/// The process's clocks: `Instant`, `SystemTime` and tokio's sleep.
#[derive(Debug)]
pub struct SystemClock {
    origin: Instant,
}

impl SystemClock {
    pub fn new() -> Self {
        Self {
            origin: Instant::now(),
        }
    }
}

impl Default for SystemClock {
    fn default() -> Self {
        Self::new()
    }
}

impl Clock for SystemClock {
    fn monotonic(&self) -> f64 {
        self.origin.elapsed().as_secs_f64()
    }

    fn now_micros(&self) -> i64 {
        match SystemTime::now().duration_since(UNIX_EPOCH) {
            Ok(since) => i64::try_from(since.as_micros()).unwrap_or(i64::MAX),
            Err(before) => -i64::try_from(before.duration().as_micros()).unwrap_or(i64::MAX),
        }
    }

    fn sleep(&self, seconds: f64) -> BoxFuture<'static, ()> {
        match Duration::try_from_secs_f64(seconds) {
            Ok(duration) if !duration.is_zero() => tokio::time::sleep(duration).boxed(),
            _ => future::ready(()).boxed(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn the_system_clock_moves_forward() {
        let clock = SystemClock::new();
        let (start, wall) = (clock.monotonic(), clock.now_micros());
        clock.sleep(0.01).await;
        clock.sleep(-1.0).await;
        clock.sleep(f64::NAN).await;
        assert!(clock.monotonic() >= start + 0.01);
        assert!(clock.now_micros() > wall);
        // After 2026-01-01: microseconds, not seconds or nanoseconds.
        assert!((1_767_225_600_000_000..4_102_444_800_000_000).contains(&wall));
    }
}

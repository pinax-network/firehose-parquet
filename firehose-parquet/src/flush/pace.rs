//! Catch-up detection for the adaptive flush interval (#659).
//!
//! `--flush-interval-secs` bounds how long rows wait at the chain head. While
//! `build` replays history (a first start, a restart or an outage), nobody is
//! waiting for an interval's worth of old blocks, so only the size, row and
//! block triggers flush and files reach the `--flush-bytes` target.
//! [`PaceDetector`] tells the two apart from the stream alone, by comparing how
//! fast block time advances with the wall clock.
//!
//! Wall-clock block age cannot tell them apart: a final-only stream is always
//! about the finality lag behind the tip (15.8 minutes on Ethereum), and on
//! Ethereum or Base it delivers finalized blocks in bursts, one epoch at a
//! time. So the detector measures the pace over samples of at least
//! [`PaceTiming::sample`] of wall time, each ending at a timestamped block, and
//! switches only on sustained evidence:
//!
//! - **Catching up**: at least [`ENTER_SAMPLES`] consecutive samples in which
//!   block time advanced more than [`ENTER_RATIO`] times faster than wall-clock
//!   time, together covering at least [`PaceTiming::enter`].
//! - **Caught up**: consecutive samples at no more than [`EXIT_RATIO`] times
//!   wall-clock speed covering at least [`PaceTiming::exit`], or no block
//!   timestamp for [`PaceTiming::unknown`] while catching up.
//!
//! A sample between the two ratios keeps the current state and breaks both
//! streaks. A wait longer than one sample between two blocks is measured as a
//! sample of its own, so the gap after a finality burst reads as real time and
//! the burst itself (delivered within one sample) is never evidence. The
//! writer's own commits count in the ratio but not toward leaving a catch-up
//! ([`PaceDetector::exclude_pause`]).
//!
//! The detector starts caught up, today's behavior, and returns there when in
//! doubt: block time is the newest timestamp seen so far, so missing, repeated
//! or backward timestamps never advance it and can only point to caught up.

use std::time::{Duration, Instant};

/// Block time must advance more than this many times faster than wall-clock
/// time for a sample to count as catching up.
pub const ENTER_RATIO: f64 = 2.0;
/// Block time advancing at most this many times wall-clock speed is real time.
pub const EXIT_RATIO: f64 = 1.25;
/// Consecutive fast samples needed before switching to catching up.
pub const ENTER_SAMPLES: u32 = 3;

/// Debug builds (as built by `cargo test`) read this variable to shorten every
/// pace window for real-binary tests: the value is the sample length in
/// milliseconds, and the other windows keep their multiples of it. Release
/// builds ignore it, like `FIREPARQ_DEBUG_FAULT`.
pub const DEBUG_SAMPLE_MILLIS_ENV: &str = "FIREPARQ_DEBUG_PACE_SAMPLE_MS";

/// Whether the writer replays history or follows the chain head.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum StreamPace {
    /// Block time advances at about wall-clock speed, or the pace is unknown:
    /// `--flush-interval-secs` applies.
    #[default]
    CaughtUp,
    /// Block time advances much faster than wall-clock time: the interval is
    /// suspended and only the size, row and block triggers flush.
    CatchingUp,
}

impl StreamPace {
    pub fn is_catching_up(self) -> bool {
        matches!(self, Self::CatchingUp)
    }

    /// The `pace` label value and log field.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::CaughtUp => "caught_up",
            Self::CatchingUp => "catching_up",
        }
    }
}

/// The detector's wall-clock windows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PaceTiming {
    /// Minimum wall time of one sample. Longer than a normal finality burst
    /// takes to arrive, and long enough that whole-second block timestamps
    /// measure the pace within about 20%.
    pub sample: Duration,
    /// Wall time the fast samples must cover before switching to catching up:
    /// longer than a finality burst, well under a 60-300 s head interval.
    pub enter: Duration,
    /// Wall time the real-time samples must cover before switching back. One
    /// long gap between finality bursts is enough on its own.
    pub exit: Duration,
    /// While catching up, this long without a timestamped block means caught up.
    pub unknown: Duration,
}

impl PaceTiming {
    /// The production sample length.
    pub const SAMPLE: Duration = Duration::from_secs(5);

    /// Windows as fixed multiples of one sample: enter after 6, exit after 4,
    /// unknown after 12 (30 s, 20 s and 60 s for the production sample).
    pub fn from_sample(sample: Duration) -> Self {
        let sample = sample.max(Duration::from_millis(1));
        Self {
            sample,
            enter: sample.saturating_mul(6),
            exit: sample.saturating_mul(4),
            unknown: sample.saturating_mul(12),
        }
    }

    /// The windows `build` uses: production defaults, or in a debug build a
    /// positive [`DEBUG_SAMPLE_MILLIS_ENV`].
    pub fn for_ingestion() -> Self {
        #[cfg(debug_assertions)]
        if let Some(millis) = std::env::var(DEBUG_SAMPLE_MILLIS_ENV)
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .filter(|millis| *millis > 0)
        {
            return Self::from_sample(Duration::from_millis(millis));
        }
        Self::default()
    }
}

impl Default for PaceTiming {
    fn default() -> Self {
        Self::from_sample(Self::SAMPLE)
    }
}

/// What decided a switch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PaceCause {
    /// The measured ratio of block time to wall-clock time.
    Pace,
    /// No block timestamp for [`PaceTiming::unknown`] while catching up.
    MissingTimestamps,
}

/// One switch, with the evidence that decided it.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PaceTransition {
    /// The new pace.
    pub pace: StreamPace,
    pub cause: PaceCause,
    /// Block time divided by wall-clock time over the evidence; `None` when
    /// timestamps were missing.
    pub ratio: Option<f64>,
    /// Blocks received per wall-clock second over the evidence.
    pub blocks_per_sec: f64,
    /// Wall time the evidence covers.
    pub evidence: Duration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Evidence {
    Fast,
    RealTime,
}

/// Consecutive samples of one kind of evidence.
#[derive(Debug, Clone, Copy, Default)]
struct Streak {
    evidence: Option<Evidence>,
    samples: u32,
    /// Wall time of the samples, which the ratio uses.
    wall: Duration,
    /// Wall time that counts toward a switch: for real-time evidence, the
    /// writer's own commits are left out.
    counted: Duration,
    block_millis: i64,
    blocks: u64,
}

impl Streak {
    fn ratio(&self) -> f64 {
        ratio(self.block_millis, self.wall)
    }
}

fn ratio(block_millis: i64, wall: Duration) -> f64 {
    let wall = wall.as_secs_f64();
    if wall > 0.0 {
        block_millis as f64 / 1000.0 / wall
    } else {
        0.0
    }
}

fn per_second(blocks: u64, wall: Duration) -> f64 {
    let wall = wall.as_secs_f64();
    if wall > 0.0 {
        blocks as f64 / wall
    } else {
        0.0
    }
}

/// Pure state machine over (wall instant, block time) observations: callers
/// inject the clock, so tests replay any stream shape deterministically.
#[derive(Debug, Clone)]
pub struct PaceDetector {
    timing: PaceTiming,
    pace: StreamPace,
    /// The newest block time seen, in unix milliseconds.
    newest_millis: Option<i64>,
    /// Where the open sample started: its wall instant and `newest_millis`.
    anchor: Option<(Instant, i64)>,
    /// Blocks observed since the anchor, with or without a timestamp.
    blocks: u64,
    /// The writer's own pauses within the open sample, and since the previous block.
    sample_paused: Duration,
    gap_paused: Duration,
    /// When the previous block arrived.
    last_block: Option<Instant>,
    /// When the previous timestamped block arrived.
    last_timestamped: Option<Instant>,
    /// Blocks observed since then, and the writer's pauses since then.
    untimed_blocks: u64,
    untimed_paused: Duration,
    streak: Streak,
}

impl PaceDetector {
    pub fn new(timing: PaceTiming) -> Self {
        Self {
            timing,
            pace: StreamPace::CaughtUp,
            newest_millis: None,
            anchor: None,
            blocks: 0,
            sample_paused: Duration::ZERO,
            gap_paused: Duration::ZERO,
            last_block: None,
            last_timestamped: None,
            untimed_blocks: 0,
            untimed_paused: Duration::ZERO,
            streak: Streak::default(),
        }
    }

    pub fn pace(&self) -> StreamPace {
        self.pace
    }

    pub fn timing(&self) -> PaceTiming {
        self.timing
    }

    /// Observe one block received at `now`, with its block time in unix
    /// milliseconds when it has one (never a synthesized routing time).
    /// Returns the switch it caused, if any.
    pub fn observe(
        &mut self,
        now: Instant,
        block_time_millis: Option<i64>,
    ) -> Option<PaceTransition> {
        let previous = self.last_block.replace(now);
        let gap_paused = std::mem::take(&mut self.gap_paused);
        if let (Some(previous), Some((anchor_at, _)), Some(newest)) =
            (previous, self.anchor, self.newest_millis)
        {
            // A gap longer than one sample is measured on its own. The partial
            // sample before it is dropped, so neither a finality burst nor the
            // tail of a replay lends its pace to the wait that follows.
            if previous > anchor_at && now.saturating_duration_since(previous) >= self.timing.sample
            {
                self.anchor = Some((previous, newest));
                self.blocks = 0;
                self.sample_paused = gap_paused;
            }
        }
        self.blocks = self.blocks.saturating_add(1);
        let Some(millis) = block_time_millis else {
            self.untimed_blocks = self.untimed_blocks.saturating_add(1);
            return self.observe_missing_timestamp(now);
        };
        self.last_timestamped = Some(now);
        self.untimed_blocks = 0;
        self.untimed_paused = Duration::ZERO;
        let newest = self
            .newest_millis
            .map_or(millis, |newest| newest.max(millis));
        self.newest_millis = Some(newest);
        let Some((anchor_at, anchor_millis)) = self.anchor else {
            self.anchor = Some((now, newest));
            self.blocks = 0;
            self.sample_paused = Duration::ZERO;
            return None;
        };
        let wall = now.saturating_duration_since(anchor_at);
        if wall < self.timing.sample {
            return None;
        }
        let blocks = std::mem::take(&mut self.blocks);
        let paused = std::mem::take(&mut self.sample_paused);
        self.anchor = Some((now, newest));
        self.close_sample(wall, paused, newest.saturating_sub(anchor_millis), blocks)
    }

    /// The writer's own commit blocked the stream for `pause` after the last
    /// observed block. The chain kept moving, so the pause stays in the
    /// measured ratio (leaving it out would make the head look fast whenever
    /// commits take most of an interval). It does not count toward the
    /// real-time evidence that ends a catch-up, nor toward the
    /// missing-timestamp window: a slow commit is not a stall.
    pub fn exclude_pause(&mut self, pause: Duration) {
        self.sample_paused = self.sample_paused.saturating_add(pause);
        self.gap_paused = self.gap_paused.saturating_add(pause);
        self.untimed_paused = self.untimed_paused.saturating_add(pause);
    }

    fn observe_missing_timestamp(&mut self, now: Instant) -> Option<PaceTransition> {
        let since = self.last_timestamped?;
        let wall = now.saturating_duration_since(since);
        if !self.pace.is_catching_up()
            || wall.saturating_sub(self.untimed_paused) < self.timing.unknown
        {
            return None;
        }
        // The anchor stays: the next timestamped block measures the whole gap.
        self.pace = StreamPace::CaughtUp;
        self.streak = Streak::default();
        Some(PaceTransition {
            pace: StreamPace::CaughtUp,
            cause: PaceCause::MissingTimestamps,
            ratio: None,
            blocks_per_sec: per_second(self.untimed_blocks, wall),
            evidence: wall,
        })
    }

    fn close_sample(
        &mut self,
        wall: Duration,
        paused: Duration,
        block_millis: i64,
        blocks: u64,
    ) -> Option<PaceTransition> {
        let sample_ratio = ratio(block_millis, wall);
        let (evidence, counted) = if sample_ratio > ENTER_RATIO {
            (Some(Evidence::Fast), wall)
        } else if sample_ratio <= EXIT_RATIO {
            (Some(Evidence::RealTime), wall.saturating_sub(paused))
        } else {
            (None, Duration::ZERO)
        };
        if evidence.is_none() || evidence != self.streak.evidence {
            self.streak = Streak {
                evidence,
                ..Streak::default()
            };
        }
        if evidence.is_some() {
            let streak = &mut self.streak;
            streak.samples = streak.samples.saturating_add(1);
            streak.wall = streak.wall.saturating_add(wall);
            streak.counted = streak.counted.saturating_add(counted);
            streak.block_millis = streak.block_millis.saturating_add(block_millis);
            streak.blocks = streak.blocks.saturating_add(blocks);
        }
        let switch_to = match (self.pace, self.streak.evidence) {
            (StreamPace::CaughtUp, Some(Evidence::Fast))
                if self.streak.samples >= ENTER_SAMPLES
                    && self.streak.counted >= self.timing.enter =>
            {
                StreamPace::CatchingUp
            }
            (StreamPace::CatchingUp, Some(Evidence::RealTime))
                if self.streak.counted >= self.timing.exit =>
            {
                StreamPace::CaughtUp
            }
            _ => return None,
        };
        let streak = std::mem::take(&mut self.streak);
        self.pace = switch_to;
        Some(PaceTransition {
            pace: switch_to,
            cause: PaceCause::Pace,
            ratio: Some(streak.ratio()),
            blocks_per_sec: per_second(streak.blocks, streak.wall),
            evidence: streak.wall,
        })
    }
}

impl Default for PaceDetector {
    fn default() -> Self {
        Self::new(PaceTiming::default())
    }
}

#[cfg(test)]
mod tests;

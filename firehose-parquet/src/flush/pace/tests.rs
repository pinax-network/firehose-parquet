//! Detector tests on an injected clock: every stream shape is a deterministic
//! sequence of (wall offset, block time) observations.
use super::*;

const MS: i64 = 1;
const SECOND: i64 = 1_000 * MS;
/// 2023-11-14T22:13:20Z.
const GENESIS: i64 = 1_700_000_000 * SECOND;

fn ms(millis: u64) -> Duration {
    Duration::from_millis(millis)
}
fn secs(seconds: u64) -> Duration {
    Duration::from_secs(seconds)
}

/// A detector fed by a simulated stream.
struct Replay {
    detector: PaceDetector,
    start: Instant,
    wall: Duration,
    block_millis: i64,
    switches: Vec<(Duration, PaceTransition)>,
}

impl Replay {
    fn new() -> Self {
        Self::with_timing(PaceTiming::default())
    }
    fn with_timing(timing: PaceTiming) -> Self {
        Self {
            detector: PaceDetector::new(timing),
            start: Instant::now(),
            wall: Duration::ZERO,
            block_millis: GENESIS,
            switches: Vec::new(),
        }
    }
    /// Receive one block `wall_step` after the previous one, with this block time.
    fn block_at(&mut self, wall_step: Duration, block_time: Option<i64>) -> &mut Self {
        self.wall += wall_step;
        if let Some(transition) = self.detector.observe(self.start + self.wall, block_time) {
            self.switches.push((self.wall, transition));
        }
        self
    }
    /// Receive `count` blocks, each `wall_step` after the previous one and
    /// `block_step` milliseconds of block time later.
    fn blocks(&mut self, count: u32, wall_step: Duration, block_step: i64) -> &mut Self {
        for _ in 0..count {
            self.block_millis += block_step;
            self.block_at(wall_step, Some(self.block_millis));
        }
        self
    }
    /// A real-time stream: block time advances exactly with wall time.
    fn real_time(&mut self, duration: Duration, block_step: i64) -> &mut Self {
        let count = duration.as_millis() as i64 / block_step;
        self.blocks(count as u32, ms(block_step as u64), block_step)
    }
    /// No block for `duration`; block time keeps advancing at the chain.
    fn stall(&mut self, duration: Duration) -> &mut Self {
        self.wall += duration;
        self
    }
    /// The writer's own commit blocks the stream for `duration`.
    fn commit(&mut self, duration: Duration) -> &mut Self {
        self.wall += duration;
        self.detector.exclude_pause(duration);
        self
    }
    fn pace(&self) -> StreamPace {
        self.detector.pace()
    }
    fn paces(&self) -> Vec<StreamPace> {
        self.switches.iter().map(|(_, t)| t.pace).collect()
    }
}

#[test]
fn production_windows_are_multiples_of_one_sample() {
    let timing = PaceTiming::default();
    assert_eq!(timing.sample, secs(5));
    assert_eq!(timing.enter, secs(30));
    assert_eq!(timing.exit, secs(20));
    assert_eq!(timing.unknown, secs(60));
    assert_eq!(PaceTiming::from_sample(ms(50)).enter, ms(300));
    assert_eq!(PaceTiming::from_sample(Duration::ZERO).sample, ms(1));
    assert_eq!(PaceDetector::default().pace(), StreamPace::CaughtUp);
    assert_eq!(StreamPace::CatchingUp.as_str(), "catching_up");
    assert_eq!(StreamPace::CaughtUp.as_str(), "caught_up");
}

#[test]
fn a_fast_replay_counts_as_catching_up_after_sustained_evidence() {
    // Ethereum history at 50 blocks/s: 600x wall-clock speed.
    let mut replay = Replay::new();
    replay.blocks(50 * 29, ms(20), 12 * SECOND);
    assert_eq!(
        replay.pace(),
        StreamPace::CaughtUp,
        "29 s is not yet sustained"
    );
    replay.blocks(50 * 10, ms(20), 12 * SECOND);
    assert_eq!(replay.pace(), StreamPace::CatchingUp);
    let (at, transition) = replay.switches[0];
    assert!(at >= secs(30) && at <= secs(36), "{at:?}");
    assert_eq!(transition.cause, PaceCause::Pace);
    let ratio = transition.ratio.unwrap();
    assert!((ratio - 600.0).abs() < 6.0, "{ratio}");
    assert!(
        (transition.blocks_per_sec - 50.0).abs() < 1.0,
        "{transition:?}"
    );
    assert!(transition.evidence >= secs(30));
    // Robinhood at 3x its chain rate (30 blocks/s of 0.1 s blocks) also counts.
    let mut replay = Replay::new();
    replay.blocks(30 * 40, ms(33), 100 * MS);
    assert_eq!(replay.paces(), [StreamPace::CatchingUp]);
    // The same stream on a shortened (test) scale switches at the same point.
    let mut replay = Replay::with_timing(PaceTiming::from_sample(ms(50)));
    replay.blocks(10, ms(20), 12 * SECOND);
    assert_eq!(replay.pace(), StreamPace::CaughtUp);
    replay.blocks(20, ms(20), 12 * SECOND);
    assert_eq!(replay.pace(), StreamPace::CatchingUp);
}

#[test]
fn real_time_pace_counts_as_caught_up_for_every_block_time() {
    // Arbitrum (0.25 s), Base (2 s) and Ethereum (12 s) at the head, and
    // Robinhood (0.1 s) with whole-second timestamps.
    for block_step in [250 * MS, 2 * SECOND, 12 * SECOND] {
        let mut replay = Replay::new();
        replay.real_time(secs(3_600), block_step);
        assert!(replay.switches.is_empty(), "{block_step}");
    }
    let mut replay = Replay::new();
    for block in 0..36_000 {
        let whole_second = GENESIS + (block * 100 * MS) / SECOND * SECOND;
        replay.block_at(ms(100), Some(whole_second));
    }
    assert!(replay.switches.is_empty());
    // A catch-up that reaches the head leaves catching up once the real-time
    // evidence covers the exit window.
    let mut replay = Replay::new();
    replay.blocks(50 * 60, ms(20), 12 * SECOND);
    assert_eq!(replay.pace(), StreamPace::CatchingUp);
    let head = replay.wall;
    replay.real_time(secs(60), 250 * MS);
    assert_eq!(
        replay.paces(),
        [StreamPace::CatchingUp, StreamPace::CaughtUp]
    );
    let (at, transition) = replay.switches[1];
    assert!(
        at - head >= secs(20) && at - head <= secs(26),
        "{:?}",
        at - head
    );
    assert!(
        (transition.ratio.unwrap() - 1.0).abs() < 0.05,
        "{transition:?}"
    );
    assert!(
        (transition.blocks_per_sec - 4.0).abs() < 0.2,
        "{transition:?}"
    );
}

#[test]
fn final_only_finality_bursts_at_the_head_stay_caught_up() {
    // Ethereum final-only: one epoch (32 blocks, 384 s) every 384 s, delivered
    // at 60 blocks/s, for four hours.
    let mut replay = Replay::new();
    for _ in 0..40 {
        replay.blocks(32, ms(16), 12 * SECOND);
        replay.stall(secs(384) - ms(16 * 32));
    }
    assert!(replay.switches.is_empty());
    // An interval flush on the first block of each burst stalls the rest of
    // it for 8 s.
    let mut replay = Replay::new();
    for _ in 0..40 {
        replay.blocks(1, ms(16), 12 * SECOND);
        replay.stall(secs(8));
        replay.blocks(31, ms(16), 12 * SECOND);
        replay.stall(secs(376) - ms(16 * 32));
    }
    assert!(replay.switches.is_empty());
    // Base final-only: 192 blocks of 2 s every 384 s, mapped over 10 s.
    let mut replay = Replay::new();
    for _ in 0..40 {
        replay.blocks(192, ms(52), 2 * SECOND);
        replay.stall(secs(384) - ms(52 * 192));
    }
    assert!(replay.switches.is_empty());
    // Reaching the head of a bursty chain leaves catching up at the first
    // block after the finality gap: the first block that could flush anyway.
    let mut replay = Replay::new();
    replay.blocks(50 * 60, ms(20), 12 * SECOND);
    assert_eq!(replay.pace(), StreamPace::CatchingUp);
    replay.stall(secs(300));
    replay.blocks(1, ms(16), 12 * SECOND);
    assert_eq!(
        replay.paces(),
        [StreamPace::CatchingUp, StreamPace::CaughtUp]
    );
    assert_eq!(replay.switches[1].0, replay.wall);
}

#[test]
fn switches_both_ways_repeatedly() {
    let mut replay = Replay::new();
    replay.blocks(50 * 60, ms(20), 12 * SECOND);
    replay.real_time(secs(120), 2 * SECOND);
    replay.blocks(50 * 60, ms(20), 12 * SECOND);
    replay.real_time(secs(120), 2 * SECOND);
    assert_eq!(
        replay.paces(),
        [
            StreamPace::CatchingUp,
            StreamPace::CaughtUp,
            StreamPace::CatchingUp,
            StreamPace::CaughtUp
        ]
    );
    // Each switch is reported once: steady input causes none.
    replay.real_time(secs(600), 2 * SECOND);
    assert_eq!(replay.switches.len(), 4);
}

#[test]
fn a_stall_shorter_than_the_exit_window_keeps_catching_up_and_a_longer_one_does_not() {
    let mut replay = Replay::new();
    replay.blocks(50 * 60, ms(20), 12 * SECOND);
    assert_eq!(replay.pace(), StreamPace::CatchingUp);
    // A 15 s reconnect or slow commit, then the replay resumes.
    replay.stall(secs(15));
    replay.blocks(50 * 10, ms(20), 12 * SECOND);
    assert_eq!(replay.paces(), [StreamPace::CatchingUp]);
    // A 45 s stall looks like a finality gap at the head: the first block
    // after it switches to caught up, and the resumed replay switches back
    // once it is sustained again.
    replay.stall(secs(45));
    replay.blocks(1, ms(20), 12 * SECOND);
    assert_eq!(
        replay.paces(),
        [StreamPace::CatchingUp, StreamPace::CaughtUp]
    );
    let resumed = replay.wall;
    replay.blocks(50 * 40, ms(20), 12 * SECOND);
    assert_eq!(
        replay.paces(),
        [
            StreamPace::CatchingUp,
            StreamPace::CaughtUp,
            StreamPace::CatchingUp
        ]
    );
    let (at, _) = replay.switches[2];
    assert!(at - resumed >= secs(30) && at - resumed <= secs(36));
}

#[test]
fn the_writers_own_commits_are_not_stalls() {
    // A 60 s commit during a catch-up: excluded, so the replay keeps its state.
    let mut replay = Replay::new();
    replay.blocks(50 * 60, ms(20), 12 * SECOND);
    replay.commit(secs(60));
    replay.blocks(50 * 10, ms(20), 12 * SECOND);
    replay.commit(secs(60));
    replay.blocks(50 * 10, ms(20), 12 * SECOND);
    assert_eq!(replay.paces(), [StreamPace::CatchingUp]);
    // The same pause as a stall of the stream switches to caught up.
    replay.stall(secs(60));
    replay.blocks(1, ms(20), 12 * SECOND);
    assert_eq!(
        replay.paces(),
        [StreamPace::CatchingUp, StreamPace::CaughtUp]
    );
    // Commits do not count toward the missing-timestamp window either.
    let mut replay = Replay::new();
    replay.blocks(50 * 60, ms(20), 12 * SECOND);
    for _ in 0..50 * 25 {
        replay.block_at(ms(20), None);
    }
    replay.commit(secs(60));
    for _ in 0..50 * 25 {
        replay.block_at(ms(20), None);
    }
    assert_eq!(replay.paces(), [StreamPace::CatchingUp]);
    for _ in 0..50 * 11 {
        replay.block_at(ms(20), None);
    }
    assert_eq!(
        replay.paces(),
        [StreamPace::CatchingUp, StreamPace::CaughtUp]
    );
    // At the head, commits that take most of each 1 s interval stay in the
    // ratio: the backlog that arrives after each one does not look fast.
    let mut replay = Replay::new();
    for _ in 0..600 {
        replay.blocks(3, ms(100), 100 * MS);
        replay.commit(ms(700));
        replay.blocks(7, ms(1), 100 * MS);
    }
    assert!(replay.switches.is_empty());
    // Leaving the pause out of the ratio would have read 3x.
    let mut replay = Replay::new();
    for _ in 0..600 {
        replay.blocks(3, ms(100), 100 * MS);
        replay.blocks(7, ms(1), 100 * MS);
    }
    assert_eq!(replay.paces(), [StreamPace::CatchingUp]);
}

#[test]
fn missing_timestamps_never_cause_catching_up() {
    // No block has a timestamp: the pace is unknown, which is caught up.
    let mut replay = Replay::new();
    for _ in 0..50 * 600 {
        replay.block_at(ms(20), None);
    }
    assert!(replay.switches.is_empty());
    // Repeated timestamps (a fixture's constant time) never advance block time.
    let mut replay = Replay::new();
    for _ in 0..50 * 600 {
        replay.block_at(ms(20), Some(GENESIS));
    }
    assert!(replay.switches.is_empty());
    // Solana-style sparse times: every fourth slot lacks one, and the
    // synthetic routing time (the last known one) is not passed on.
    let mut replay = Replay::new();
    for slot in 0..30_000 {
        replay.block_millis += 400 * MS;
        let time = (slot % 4 != 0).then_some(replay.block_millis);
        replay.block_at(ms(2), time);
    }
    assert_eq!(replay.paces(), [StreamPace::CatchingUp]);
    let mut replay = Replay::new();
    for slot in 0..9_000 {
        replay.block_millis += 400 * MS;
        let time = (slot % 4 != 0).then_some(replay.block_millis / SECOND * SECOND);
        replay.block_at(ms(400), time);
    }
    assert!(replay.switches.is_empty());
}

#[test]
fn timestamps_that_disappear_while_catching_up_mean_caught_up() {
    let mut replay = Replay::new();
    replay.blocks(50 * 60, ms(20), 12 * SECOND);
    let last_sample = replay.wall;
    for _ in 0..50 * 30 {
        replay.block_at(ms(20), None);
    }
    assert_eq!(
        replay.pace(),
        StreamPace::CatchingUp,
        "30 s is within the window"
    );
    for _ in 0..50 * 40 {
        replay.block_at(ms(20), None);
    }
    assert_eq!(
        replay.paces(),
        [StreamPace::CatchingUp, StreamPace::CaughtUp]
    );
    let (at, transition) = replay.switches[1];
    assert!(at - last_sample >= secs(60) && at - last_sample <= secs(61));
    assert_eq!(transition.cause, PaceCause::MissingTimestamps);
    assert_eq!(transition.ratio, None);
    assert!(
        (transition.blocks_per_sec - 50.0).abs() < 1.0,
        "{transition:?}"
    );
    // Timestamps that return at the replay pace switch back.
    replay.blocks(50 * 40, ms(20), 12 * SECOND);
    assert_eq!(replay.pace(), StreamPace::CatchingUp);
}

#[test]
fn non_monotonic_timestamps_do_not_cause_wrong_switches() {
    // Real time with block times jittered by up to +-3 s (deterministic LCG).
    let mut replay = Replay::new();
    let mut seed: u64 = 0x5eed;
    for block in 0..1_800 {
        seed = seed
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        let jitter = (seed >> 33) as i64 % (6 * SECOND + 1) - 3 * SECOND;
        replay.block_at(secs(2), Some(GENESIS + block * 2 * SECOND + jitter));
    }
    assert!(replay.switches.is_empty());
    // One block a year in the future, then normal times: block time stops at
    // the outlier, which can only mean caught up.
    let mut replay = Replay::new();
    replay.real_time(secs(60), 2 * SECOND);
    replay.block_at(secs(2), Some(replay.block_millis + 365 * 86_400 * SECOND));
    replay.real_time(secs(600), 2 * SECOND);
    assert!(replay.switches.is_empty());
    // A backward jump of an hour while catching up does not exit on its own.
    let mut replay = Replay::new();
    replay.blocks(50 * 60, ms(20), 12 * SECOND);
    replay.block_millis -= 3_600 * SECOND;
    replay.blocks(50 * 20, ms(20), 12 * SECOND);
    assert_eq!(replay.paces(), [StreamPace::CatchingUp]);
    // A non-final head: NEW blocks at real time, interleaved with UNDO and
    // FINAL events of older blocks.
    let mut replay = Replay::new();
    for block in 0..1_800 {
        let new = GENESIS + block * 2 * SECOND;
        replay.block_at(secs(2), Some(new));
        if block % 10 == 0 {
            replay.block_at(ms(1), Some(new - 4 * SECOND));
            replay.block_at(ms(1), Some(new - 400 * SECOND));
        }
    }
    assert!(replay.switches.is_empty());
}

/// One sample every 5 s whose block time advances `block_step` milliseconds.
fn samples(replay: &mut Replay, count: u32, block_step: i64) {
    replay.blocks(count, secs(5), block_step);
}

#[test]
fn hysteresis_at_the_boundaries() {
    // Exactly 2x is not faster than 2x: no switch, however long it lasts.
    let mut replay = Replay::new();
    samples(&mut replay, 1, 0);
    samples(&mut replay, 100, 10 * SECOND);
    assert!(replay.switches.is_empty());
    // Just above 2x switches after ENTER_SAMPLES samples covering 30 s.
    samples(&mut replay, 5, 10_050 * MS);
    assert_eq!(replay.pace(), StreamPace::CaughtUp);
    samples(&mut replay, 1, 10_050 * MS);
    assert_eq!(replay.paces(), [StreamPace::CatchingUp]);
    assert!((replay.switches[0].1.ratio.unwrap() - 2.01).abs() < 1e-9);
    // Between 1.25x and 2x nothing changes in either state.
    samples(&mut replay, 100, 6_300 * MS);
    samples(&mut replay, 100, 9_500 * MS);
    assert_eq!(replay.paces(), [StreamPace::CatchingUp]);
    // Exactly 1.25x is real time: switches after 20 s of it.
    samples(&mut replay, 3, 6_250 * MS);
    assert_eq!(replay.pace(), StreamPace::CatchingUp);
    samples(&mut replay, 1, 6_250 * MS);
    assert_eq!(
        replay.paces(),
        [StreamPace::CatchingUp, StreamPace::CaughtUp]
    );
    // Alternating 1.9x / 2.1x never builds a fast streak.
    for _ in 0..50 {
        samples(&mut replay, 1, 9_500 * MS);
        samples(&mut replay, 1, 10_500 * MS);
    }
    assert_eq!(replay.switches.len(), 2);
    // Two very long fast samples are not enough: ENTER_SAMPLES also counts.
    let mut replay = Replay::new();
    replay.blocks(1, secs(1), 0);
    replay.blocks(2, secs(60), 600 * SECOND);
    assert_eq!(replay.pace(), StreamPace::CaughtUp);
    replay.blocks(1, secs(60), 600 * SECOND);
    assert_eq!(replay.pace(), StreamPace::CatchingUp);
    // Blocks inside one sample only close it once the sample length passed.
    let mut replay = Replay::new();
    replay.blocks(1, secs(1), 0);
    for _ in 0..1_000 {
        replay.blocks(1, ms(4), 60 * SECOND);
    }
    assert_eq!(replay.pace(), StreamPace::CaughtUp, "4 s of wall time");
}

//! Transient compressed-size prediction for all-table ingestion transactions.
//! The independent logical-buffer threshold is not a process RSS limit.

use anyhow::{ensure, Result};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MapperBufferEstimate {
    pub largest_table_bytes: u64,
    pub total_bytes: u64,
}

impl MapperBufferEstimate {
    pub fn from_table_sizes(sizes: impl IntoIterator<Item = usize>) -> Self {
        sizes.into_iter().fold(Self::default(), |mut sum, size| {
            let size = u64::try_from(size).unwrap_or(u64::MAX);
            sum.largest_table_bytes = sum.largest_table_bytes.max(size);
            sum.total_bytes = sum.total_bytes.saturating_add(size);
            sum
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SizeFlushTrigger {
    Memory,
    Bytes,
}

/// Starts conservatively and learns only from completed transaction receipts.
/// Ratio state is deliberately not part of the durable stream descriptor.
pub struct FlushSizing {
    target_bytes: u64,
    memory_bytes: u64,
    ratio: f64,
    observed: bool,
}

impl FlushSizing {
    pub fn new(target_bytes: u64, memory_bytes: u64) -> Result<Self> {
        ensure!(memory_bytes > 0, "flush memory threshold must be positive");
        Ok(Self {
            target_bytes,
            memory_bytes,
            ratio: 1.0,
            observed: false,
        })
    }

    pub fn ratio(&self) -> f64 {
        self.ratio
    }

    pub fn predicted_file_bytes(&self, largest_table_bytes: u64) -> u64 {
        // Float-to-integer conversion saturates; ratio is always finite/positive.
        (largest_table_bytes as f64 * self.ratio).ceil() as u64
    }

    pub fn trigger(&self, estimate: MapperBufferEstimate) -> Option<SizeFlushTrigger> {
        if estimate.total_bytes >= self.memory_bytes {
            Some(SizeFlushTrigger::Memory)
        } else if self.target_bytes > 0
            && self.predicted_file_bytes(estimate.largest_table_bytes) >= self.target_bytes
        {
            Some(SizeFlushTrigger::Bytes)
        } else {
            None
        }
    }

    /// Call only after the complete all-table transaction succeeds. Maxima may
    /// belong to different tables: the goal is the largest physical output file.
    /// Tiny forced flushes are ignored to limit footer-dominated observations.
    pub fn observe_committed(&mut self, largest_mapper_bytes: u64, largest_file_bytes: u64) {
        let minimum_sample = (self.target_bytes / 4).min(1024 * 1024).max(1);
        if self.target_bytes == 0
            || largest_mapper_bytes < minimum_sample
            || largest_file_bytes == 0
        {
            return;
        }
        let sample =
            (largest_file_bytes as f64 / largest_mapper_bytes as f64).clamp(1.0 / 1024.0, 1024.0);
        self.ratio = if self.observed {
            self.ratio * 0.5 + sample * 0.5
        } else {
            sample
        };
        self.observed = true;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sum_includes_every_table_and_memory_precedes_compressed_target() {
        let sizing = FlushSizing::new(100, 150).unwrap();
        let estimates = MapperBufferEstimate::from_table_sizes([80, 80]);
        assert_eq!(estimates.total_bytes, 160);
        assert_eq!(estimates.largest_table_bytes, 80);
        assert_eq!(sizing.trigger(estimates), Some(SizeFlushTrigger::Memory));
        assert_eq!(
            sizing.trigger(MapperBufferEstimate::from_table_sizes([160])),
            Some(SizeFlushTrigger::Memory)
        );
        assert!(FlushSizing::new(100, 0).is_err());
    }

    #[test]
    fn completed_samples_calibrate_then_adapt_to_larger_files() {
        let mut sizing = FlushSizing::new(1024 * 1024, 64 * 1024 * 1024).unwrap();
        let estimate = MapperBufferEstimate::from_table_sizes([2 * 1024 * 1024]);
        assert_eq!(sizing.trigger(estimate), Some(SizeFlushTrigger::Bytes));
        sizing.observe_committed(2 * 1024 * 1024, 256 * 1024);
        assert_eq!(sizing.ratio(), 0.125);
        assert_eq!(sizing.trigger(estimate), None);
        assert_eq!(sizing.predicted_file_bytes(8 * 1024 * 1024), 1024 * 1024);
        sizing.observe_committed(8 * 1024 * 1024, 2 * 1024 * 1024);
        assert_eq!(sizing.ratio(), 0.1875);
        assert_eq!(sizing.predicted_file_bytes(8 * 1024 * 1024), 1536 * 1024);
    }

    #[test]
    fn zero_tiny_or_disabled_observations_do_not_train() {
        let mut sizing = FlushSizing::new(4 * 1024 * 1024, 1024).unwrap();
        for (estimate, physical) in [(0, 9999), (9999, 0), (100, 9999)] {
            sizing.observe_committed(estimate, physical);
            assert_eq!(sizing.ratio(), 1.0);
        }
        let mut disabled = FlushSizing::new(0, 1024).unwrap();
        disabled.observe_committed(1 << 20, 100);
        assert_eq!(disabled.ratio(), 1.0);
        assert_eq!(
            disabled.trigger(MapperBufferEstimate::from_table_sizes([600, 600])),
            Some(SizeFlushTrigger::Memory)
        );
        assert_eq!(
            disabled.trigger(MapperBufferEstimate::from_table_sizes([900])),
            None
        );
    }

    #[test]
    fn predictions_and_sums_are_bounded_for_extreme_samples() {
        let mut sizing = FlushSizing::new(1, u64::MAX).unwrap();
        sizing.observe_committed(1, u64::MAX);
        assert_eq!(sizing.ratio(), 1024.0);
        assert_eq!(sizing.predicted_file_bytes(u64::MAX), u64::MAX);
        let mut tiny = FlushSizing::new(1, u64::MAX).unwrap();
        tiny.observe_committed(u64::MAX, 1);
        assert_eq!(tiny.ratio(), 1.0 / 1024.0);
        assert!(tiny.predicted_file_bytes(1) > 0);
        assert_eq!(
            MapperBufferEstimate::from_table_sizes([usize::MAX; 4]).total_bytes,
            (usize::MAX as u128 * 4).min(u64::MAX as u128) as u64
        );
    }
}

use prometheus_client::collector::Collector;
use prometheus_client::encoding::{DescriptorEncoder, EncodeMetric};
use prometheus_client::metrics::gauge::ConstGauge;
use std::collections::BTreeMap;
use std::fmt;
use std::fs;
use std::path::Path;
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// INC-37519 had 102–105 threads throughout the memory climb. Healthy writers
/// in the comparison had at most 28. Keep thread names out of metric labels.
const THREAD_SNAPSHOT_THRESHOLD: u64 = 64;
const SNAPSHOT_INTERVAL: Duration = Duration::from_secs(5 * 60);
const MAX_TASKS: usize = 256;

#[derive(Debug, Default)]
pub(super) struct ProcessCollector {
    last_snapshot: Mutex<Option<Instant>>,
}

#[derive(Default, Debug, PartialEq)]
struct Status {
    rss_bytes: Option<u64>,
    threads: Option<u64>,
}

fn parse_status(text: &str) -> Status {
    let mut status = Status::default();
    for line in text.lines() {
        if let Some(value) = line.strip_prefix("VmRSS:") {
            status.rss_bytes = value
                .trim()
                .strip_suffix(" kB")
                .and_then(|n| n.trim_end().parse::<u64>().ok())
                .and_then(|kib| kib.checked_mul(1024));
        } else if let Some(value) = line.strip_prefix("Threads:") {
            status.threads = value.trim().parse().ok();
        }
    }
    status
}

fn thread_names(tasks: &Path) -> std::io::Result<BTreeMap<String, usize>> {
    let mut names = BTreeMap::new();
    for entry in fs::read_dir(tasks)?.take(MAX_TASKS).flatten() {
        // A thread can exit between readdir and read. Counts describe the sample.
        if let Ok(name) = fs::read_to_string(entry.path().join("comm")) {
            *names
                .entry(name.trim_end_matches('\n').to_owned())
                .or_default() += 1;
        }
    }
    Ok(names)
}

impl ProcessCollector {
    fn snapshot_due(&self, now: Instant, threads: u64) -> bool {
        if threads < THREAD_SNAPSHOT_THRESHOLD {
            return false;
        }
        let mut last = self.last_snapshot.lock().unwrap_or_else(|e| e.into_inner());
        if last.is_some_and(|t| now.saturating_duration_since(t) < SNAPSHOT_INTERVAL) {
            return false;
        }
        *last = Some(now);
        true
    }

    fn collect(&self, encoder: &mut DescriptorEncoder, status: Status) -> Result<(), fmt::Error> {
        if let Some(bytes) = status.rss_bytes {
            let gauge = ConstGauge::new(bytes);
            gauge.encode(encoder.encode_descriptor(
                "process_resident_memory_bytes",
                "Resident memory of the writer process in bytes",
                None,
                gauge.metric_type(),
            )?)?;
        }
        if let Some(threads) = status.threads {
            let gauge = ConstGauge::new(threads);
            gauge.encode(encoder.encode_descriptor(
                "process_threads",
                "Number of operating system threads in the writer process",
                None,
                gauge.metric_type(),
            )?)?;
            if self.snapshot_due(Instant::now(), threads) {
                if let Ok(names) = thread_names(Path::new("/proc/self/task")) {
                    let sampled: usize = names.values().sum();
                    let runtime_workers = tokio::runtime::Handle::try_current()
                        .ok()
                        .map(|runtime| runtime.metrics().num_workers());
                    tracing::info!(
                        threads,
                        sampled,
                        ?runtime_workers,
                        ?names,
                        "writer thread snapshot"
                    );
                }
            }
        }
        Ok(())
    }
}

impl Collector for ProcessCollector {
    fn encode(&self, mut encoder: DescriptorEncoder) -> Result<(), fmt::Error> {
        if let Ok(text) = fs::read_to_string("/proc/self/status") {
            self.collect(&mut encoder, parse_status(&text))?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn converts_linux_kib_without_inventing_missing_samples() {
        assert_eq!(
            parse_status("Name:\tfireparq\nVmRSS:\t  587108 kB\nThreads:\t3\n"),
            Status {
                rss_bytes: Some(587108 * 1024),
                threads: Some(3)
            }
        );
        assert_eq!(
            parse_status("VmRSS: 18446744073709551615 kB\nThreads: bad"),
            Status::default()
        );
        assert_eq!(parse_status("VmRSS: 900 B").rss_bytes, None);
    }

    #[test]
    fn snapshots_only_while_thread_count_is_high_and_at_most_once_per_interval() {
        let collector = ProcessCollector::default();
        let now = Instant::now();
        assert!(!collector.snapshot_due(now, THREAD_SNAPSHOT_THRESHOLD - 1));
        assert!(collector.snapshot_due(now, THREAD_SNAPSHOT_THRESHOLD));
        assert!(!collector.snapshot_due(now + Duration::from_secs(30), 104));
        assert!(!collector.snapshot_due(now + SNAPSHOT_INTERVAL, 12));
        assert!(collector.snapshot_due(now + SNAPSHOT_INTERVAL, 102));
    }

    #[test]
    fn a_large_process_snapshot_is_bounded() {
        let temporary = tempfile::tempdir().unwrap();
        for tid in 0..MAX_TASKS + 5 {
            let task = temporary.path().join(tid.to_string());
            fs::create_dir(&task).unwrap();
            fs::write(task.join("comm"), "worker\n").unwrap();
        }
        assert_eq!(thread_names(temporary.path()).unwrap()["worker"], MAX_TASKS);
    }

    #[test]
    fn counts_names_and_tolerates_exited_tasks() {
        let temporary = tempfile::tempdir().unwrap();
        for (tid, name) in [
            (1, "fireparq"),
            (2, "tokio-rt-worker"),
            (3, "tokio-rt-worker"),
        ] {
            let task = temporary.path().join(tid.to_string());
            fs::create_dir(&task).unwrap();
            fs::write(task.join("comm"), format!("{name}\n")).unwrap();
        }
        fs::create_dir(temporary.path().join("4")).unwrap();
        let counts = thread_names(temporary.path()).unwrap();
        assert_eq!(counts["fireparq"], 1);
        assert_eq!(counts["tokio-rt-worker"], 2);
        assert_eq!(counts.len(), 2);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn normal_registry_exposes_positive_linux_process_samples() {
        let (registry, _) = super::super::init();
        let mut body = String::new();
        prometheus_client::encoding::text::encode(&mut body, &registry).unwrap();
        for name in ["process_resident_memory_bytes", "process_threads"] {
            assert!(body.lines().any(|line| line
                .split_once(' ')
                .is_some_and(|(key, sample)| key == name
                    && sample.parse::<u64>().is_ok_and(|value| value > 0))));
        }
    }
}

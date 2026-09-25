//! One owned file-verification worker. Cancellation signals and joins it before
//! returning, so timed-out verification cannot accumulate detached hash workers.
use super::*;
use std::sync::atomic::{AtomicBool, Ordering};

struct Worker {
    cancelled: Arc<AtomicBool>,
    thread: Option<std::thread::JoinHandle<()>>,
}
impl Drop for Worker {
    fn drop(&mut self) {
        self.cancelled.store(true, Ordering::SeqCst);
        if let Some(thread) = self.thread.take() {
            // Hashing checks cancellation between bounded reads. Footer parsing
            // is bounded but not interruptible; drain that one operation. The
            // deadline initiates cancellation, not a claim of instantaneous I/O.
            let _ = thread.join();
        }
    }
}

async fn run(work: impl FnOnce(&AtomicBool) -> Result<()> + Send + 'static) -> Result<()> {
    let cancelled = Arc::new(AtomicBool::new(false));
    let flag = cancelled.clone();
    let (sender, receiver) = tokio::sync::oneshot::channel();
    let thread = std::thread::Builder::new()
        .name("parquet-verification".into())
        .spawn(move || {
            let result = work(&flag);
            let _ = sender.send(result);
        })
        .context("starting file verification worker")?;
    let _worker = Worker {
        cancelled,
        thread: Some(thread),
    };
    receiver
        .await
        .map_err(|_| anyhow::anyhow!("file verification worker failed"))?
}

pub(super) async fn verify(plan: PlannedPart, receipt: PartReceipt, file: File) -> Result<()> {
    run(move |cancelled| verify_file_checked(&plan, &receipt, &file, cancelled)).await
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn cancellation_joins_the_started_worker_before_task_returns() {
        let (started, received) = tokio::sync::oneshot::channel();
        let active = Arc::new(AtomicBool::new(false));
        let observed = active.clone();
        let task = tokio::spawn(run(move |cancelled| {
            observed.store(true, Ordering::SeqCst);
            started.send(()).unwrap();
            while !cancelled.load(Ordering::SeqCst) {
                std::thread::yield_now();
            }
            observed.store(false, Ordering::SeqCst);
            Ok(())
        }));
        received.await.unwrap();
        assert!(active.load(Ordering::SeqCst));
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert!(!active.load(Ordering::SeqCst));
    }
}

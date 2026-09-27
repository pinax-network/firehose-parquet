//! Supervised blocking I/O lane for local transaction parts (#516 stage A).
//!
//! Local staging, publication and verification are blocking file operations
//! that borrow the held ownership guard, so they cannot move to detached
//! `spawn_blocking` tasks. The lane runs them on at most `threads` scoped
//! threads: `std::thread::scope` joins every thread before the owner-borrowing
//! caller can return, so no local write can outlive the transaction's guards,
//! even on panic. Jobs report panics as errors instead of killing workers.

use anyhow::{anyhow, Result};
use std::future::Future;
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::thread::Scope;

type Job<'scope> = Box<dyn FnOnce() + Send + 'scope>;

pub(super) struct BlockingLane<'scope> {
    sender: Mutex<Option<Sender<Job<'scope>>>>,
}

impl<'scope> BlockingLane<'scope> {
    /// Start `threads` workers in `scope`. Dropping the lane lets them exit;
    /// the scope then joins them.
    pub(super) fn start<'env>(scope: &'scope Scope<'scope, 'env>, threads: usize) -> Self {
        let (sender, receiver) = channel::<Job<'scope>>();
        let receiver: Arc<Mutex<Receiver<Job<'scope>>>> = Arc::new(Mutex::new(receiver));
        for _ in 0..threads.max(1) {
            let receiver = Arc::clone(&receiver);
            scope.spawn(move || loop {
                let job = match receiver.lock() {
                    Ok(receiver) => receiver.recv(),
                    Err(_) => return,
                };
                match job {
                    Ok(job) => job(),
                    Err(_) => return,
                }
            });
        }
        Self {
            sender: Mutex::new(Some(sender)),
        }
    }

    /// Queue one blocking job and await its result from async code.
    pub(super) fn run<T, F>(&self, job: F) -> impl Future<Output = Result<T>> + 'scope
    where
        T: Send + 'scope,
        F: FnOnce() -> Result<T> + Send + 'scope,
    {
        let (sender, receiver) = tokio::sync::oneshot::channel();
        let queued = self
            .sender
            .lock()
            .ok()
            .and_then(|lane| lane.as_ref().cloned())
            .map(|lane| {
                lane.send(Box::new(move || {
                    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(job))
                        .unwrap_or_else(|_| Err(anyhow!("local transaction I/O worker panicked")));
                    let _ = sender.send(result);
                }))
                .is_ok()
            })
            .unwrap_or(false);
        async move {
            if !queued {
                return Err(anyhow!("local transaction I/O lane is closed"));
            }
            receiver
                .await
                .map_err(|_| anyhow!("local transaction I/O worker stopped"))?
        }
    }

    /// Stop accepting jobs; queued jobs still run and workers then exit.
    pub(super) fn close(&self) {
        if let Ok(mut sender) = self.sender.lock() {
            sender.take();
        }
    }
}

impl Drop for BlockingLane<'_> {
    fn drop(&mut self) {
        self.close();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn lane_bounds_threads_reports_panics_and_joins_before_scope_ends() {
        let running = AtomicUsize::new(0);
        let peak = AtomicUsize::new(0);
        let finished = AtomicUsize::new(0);
        std::thread::scope(|scope| {
            let lane = BlockingLane::start(scope, 3);
            let runtime = tokio::runtime::Builder::new_current_thread()
                .build()
                .unwrap();
            let results = runtime.block_on(async {
                let jobs: Vec<_> = (0..12)
                    .map(|index| {
                        let (running, peak, finished) = (&running, &peak, &finished);
                        lane.run(move || {
                            let now = running.fetch_add(1, Ordering::SeqCst) + 1;
                            peak.fetch_max(now, Ordering::SeqCst);
                            std::thread::sleep(std::time::Duration::from_millis(20));
                            running.fetch_sub(1, Ordering::SeqCst);
                            finished.fetch_add(1, Ordering::SeqCst);
                            if index == 5 {
                                panic!("injected job panic");
                            }
                            Ok(index)
                        })
                    })
                    .collect();
                futures::future::join_all(jobs).await
            });
            assert_eq!(results.iter().filter(|result| result.is_err()).count(), 1);
            assert!(results[5].is_err());
            assert_eq!(*results[11].as_ref().unwrap(), 11);
            lane.close();
            let closed = runtime.block_on(lane.run(|| Ok(())));
            assert!(closed.is_err());
        });
        assert_eq!(finished.load(Ordering::SeqCst), 12);
        let peak = peak.load(Ordering::SeqCst);
        assert!((2..=3).contains(&peak), "{peak}");
    }
}

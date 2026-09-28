//! Bounded table work inside one all-table transaction (#516 stage A).
//!
//! The transaction is already Writing when this starts, and nothing here
//! advances authority or the mirror. One coordinator owns the versioned pending
//! record: it serializes every receipt CAS and starts a part's publication only
//! after that part's exact receipt is durable. Receipts of parts that finished
//! staging while the previous CAS was in flight are persisted together in the
//! next one, so the journal lane does not serialize every part behind its own
//! round trip. Encoders run on at most
//! `encoders` blocking threads and own only immutable prepared batches plus a
//! private output buffer or anonymous spool. Publications, receipt writes and
//! local staging are owner-bound I/O polled by this coordinator, never detached:
//! remote publications are futures it polls, and blocking local staging and
//! publication run on the controller's scoped [`BlockingLane`] when one exists
//! (otherwise inline on this coordinator).
//!
//! Encoded parts hold a byte [`Reservation`] from one [`InflightBudget`]: an
//! encoder is admitted only when its initial reservation fits, grows it while
//! writing without blocking, and a part releases it when dropped (after its
//! publication, or on any failure). A part whose growth is refused is encoded
//! again later with an exclusive reservation, so the budget is never exceeded
//! except by one part larger than the whole budget, admitted alone.
//!
//! The first error stops admission. Every already started encoder, receipt write
//! and publication is drained before the error is returned, so no guard or
//! buffer is released while a worker or request could still act on the journal.
//! Unstarted parts are dropped; the Writing journal remains for recovery.

use anyhow::{Context, Result};
use futures::stream::{FuturesUnordered, StreamExt};
use std::collections::{BTreeMap, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use super::lane::BlockingLane;
use super::{checkpoint, Stage};
use crate::config::FlushConcurrency;
use crate::ingest::parts::TransactionParts;
use crate::ingest::state::{AuthorityState, Digest, PartReceipt, PendingTransaction};
use crate::ingest::store::{TransactionStateStore, Versioned};
use crate::writer::protected::{
    EncodedPart, InflightBudget, PreparedFlush, Reservation, ReservationExceeded,
};

/// Observed high-water marks of one transaction's table work. Encoder and
/// publication peaks count actually executing encoder closures and publication
/// futures, not merely admitted work.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct FlushWorkStats {
    pub peak_encoders: usize,
    pub peak_publications: usize,
    /// Peak reserved encoded bytes; above the limit only for one oversized part.
    pub peak_inflight_bytes: u64,
    /// Parts encoded again exclusively after the budget refused their growth.
    pub reencoded_parts: usize,
    /// Parts larger than the whole budget, admitted alone.
    pub oversized_parts: usize,
}

enum Event {
    Encoded {
        index: u32,
        exclusive: bool,
        result: Result<EncodedPart>,
    },
    Staged {
        result: Result<Staged>,
    },
    Recorded {
        result: Result<Versioned<PendingTransaction>>,
        prior: Option<Versioned<PendingTransaction>>,
        parts: Vec<Staged>,
    },
    Published {
        index: u32,
        result: Result<()>,
    },
}

/// A part whose bytes are encoded (and, for local output, staged).
struct Staged {
    index: u32,
    table: String,
    encoded: EncodedPart,
}

#[derive(Clone, Copy)]
struct Queued {
    index: u32,
    exclusive: bool,
}

type Work<'f> = Pin<Box<dyn Future<Output = Event> + 'f>>;

/// Counts executions currently inside a section and their peak.
#[derive(Default)]
struct Observed {
    running: AtomicUsize,
    peak: AtomicUsize,
}
impl Observed {
    fn enter(self: &Arc<Self>) -> ObservedGuard {
        let now = self.running.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(now, Ordering::SeqCst);
        ObservedGuard(Arc::clone(self))
    }
    fn peak(&self) -> usize {
        self.peak.load(Ordering::SeqCst)
    }
}
struct ObservedGuard(Arc<Observed>);
impl Drop for ObservedGuard {
    fn drop(&mut self) {
        self.0.running.fetch_sub(1, Ordering::SeqCst);
    }
}

/// Encode, stage, journal and publish every planned part of one Writing
/// transaction, returning the pending record with every receipt persisted.
pub(super) async fn publish_parts<'f, 'a: 'f>(
    parts: &'f TransactionParts<'a>,
    states: &'f TransactionStateStore<'a>,
    authority: &'f Versioned<AuthorityState>,
    prepared: Arc<PreparedFlush>,
    pending: Versioned<PendingTransaction>,
    limits: FlushConcurrency,
    lane: Option<&BlockingLane<'f>>,
) -> Result<(Versioned<PendingTransaction>, FlushWorkStats)> {
    limits.validate()?;
    let spooled = parts.encodes_spooled();
    let lane = lane.filter(|_| parts.is_local());
    // Everything fallible happens before any work starts: past this point every
    // error is recorded and the loop drains instead of returning early.
    let mut plans = BTreeMap::new();
    for part in prepared.parts() {
        plans.insert(
            part.entry_index,
            (
                part.table.clone(),
                prepared.estimated_part_bytes(part.entry_index)?,
            ),
        );
    }
    let mut queue: VecDeque<Queued> = plans
        .keys()
        .map(|index| Queued {
            index: *index,
            exclusive: false,
        })
        .collect();
    let budget = InflightBudget::new(limits.inflight_bytes);
    let mut work: FuturesUnordered<Work<'f>> = FuturesUnordered::new();
    let mut pending = Some(pending);
    let mut receipts: VecDeque<Staged> = VecDeque::new();
    let mut publishable: VecDeque<Staged> = VecDeque::new();
    let (mut encoders, mut publications, mut recording) = (0usize, 0usize, false);
    let (encoding, publishing) = (Arc::<Observed>::default(), Arc::<Observed>::default());
    let mut stats = FlushWorkStats::default();
    let mut failure: Option<anyhow::Error> = None;

    loop {
        if failure.is_none() {
            // Encoders: bounded count, byte-weighted admission in plan order.
            while encoders < limits.encoders {
                let Some(next) = queue.front().copied() else {
                    break;
                };
                let (table, estimate) = &plans[&next.index];
                let exclusive = next.exclusive || *estimate >= budget.limit();
                let reservation = if exclusive {
                    budget.try_reserve_exclusive()
                } else {
                    budget.try_reserve(*estimate)
                };
                let Some(reservation) = reservation else {
                    break;
                };
                queue.pop_front();
                encoders += 1;
                work.push(encode(
                    Arc::clone(&prepared),
                    next.index,
                    table.clone(),
                    exclusive,
                    spooled,
                    reservation,
                    Arc::clone(&encoding),
                ));
            }
            // Receipts: one CAS at a time, owned by this coordinator, carrying
            // every receipt that is ready.
            if !recording && !receipts.is_empty() {
                if let Some(prior) = pending.take() {
                    let batch: Vec<Staged> = receipts.drain(..).collect();
                    recording = true;
                    work.push(Box::pin(async move {
                        let result = match batch
                            .iter()
                            .map(|part| Ok((part.index, part_receipt(&part.encoded)?)))
                            .collect::<Result<Vec<_>>>()
                        {
                            Ok(receipts) => {
                                states.record_receipts(authority, &prior, receipts).await
                            }
                            Err(error) => Err(error),
                        };
                        Event::Recorded {
                            prior: result.is_err().then_some(prior),
                            result,
                            parts: batch,
                        }
                    }));
                }
            }
            // Publications: bounded count, only parts with durable receipts.
            while publications < limits.publications {
                let Some(part) = publishable.pop_front() else {
                    break;
                };
                publications += 1;
                let publishing = Arc::clone(&publishing);
                let index = part.index;
                work.push(match lane {
                    Some(lane) => {
                        let job = lane.run(move || publish_blocking(parts, part));
                        Box::pin(async move {
                            let _running = publishing.enter();
                            Event::Published {
                                index,
                                result: job.await,
                            }
                        })
                    }
                    None => Box::pin(async move {
                        let _running = publishing.enter();
                        let result = publish(parts, &part).await;
                        // The part, and its byte reservation, drop here.
                        Event::Published { index, result }
                    }),
                });
            }
        }

        let Some(event) = work.next().await else {
            break;
        };
        let outcome: Result<()> = match event {
            Event::Encoded {
                index,
                exclusive,
                result,
            } => {
                encoders -= 1;
                match result {
                    Ok(encoded) if failure.is_none() => {
                        if encoded.encoded_bytes() > budget.limit() {
                            stats.oversized_parts += 1;
                            tracing::warn!(
                                entry_index = index,
                                encoded_bytes = encoded.encoded_bytes(),
                                inflight_limit = budget.limit(),
                                "one encoded part exceeds the whole in-flight byte budget; it was admitted alone"
                            );
                        }
                        let part = Staged {
                            index,
                            table: plans[&index].0.clone(),
                            encoded,
                        };
                        match lane {
                            Some(lane) => {
                                let job = lane.run(move || {
                                    parts.stage(&part.encoded)?;
                                    Ok(part)
                                });
                                work.push(Box::pin(
                                    async move { Event::Staged { result: job.await } },
                                ));
                                Ok(())
                            }
                            None => parts
                                .stage(&part.encoded)
                                .and_then(|()| checkpoint(Stage::Staged(part.index)))
                                .map(|()| receipts.push_back(part)),
                        }
                    }
                    // Discarded after a failure; dropping releases its bytes.
                    Ok(_) => Ok(()),
                    Err(error)
                        if !exclusive && error.downcast_ref::<ReservationExceeded>().is_some() =>
                    {
                        // Encoding is deterministic, so encoding again alone
                        // yields the same bytes; nothing was staged or journaled.
                        stats.reencoded_parts += 1;
                        queue.push_front(Queued {
                            index,
                            exclusive: true,
                        });
                        Ok(())
                    }
                    Err(error) => Err(error),
                }
            }
            Event::Staged { result } => result.and_then(|part| {
                checkpoint(Stage::Staged(part.index))?;
                if failure.is_none() {
                    receipts.push_back(part);
                }
                Ok(())
            }),
            Event::Recorded {
                result,
                prior,
                parts: batch,
            } => {
                recording = false;
                match result {
                    Ok(next) => {
                        pending = Some(next);
                        let mut outcome = Ok(());
                        for part in batch {
                            match checkpoint(Stage::ReceiptPersisted(part.index)) {
                                Ok(()) if failure.is_none() && outcome.is_ok() => {
                                    publishable.push_back(part)
                                }
                                Ok(()) => {}
                                Err(error) => {
                                    if outcome.is_ok() {
                                        outcome = Err(error);
                                    }
                                }
                            }
                        }
                        outcome
                    }
                    Err(error) => {
                        pending = prior;
                        Err(error)
                    }
                }
            }
            Event::Published { index, result } => {
                publications -= 1;
                result.and_then(|()| checkpoint(Stage::Published(index)))
            }
        };
        if let Err(error) = outcome {
            if failure.is_none() {
                failure = Some(error);
                // Stop admission; parts not yet started are dropped with their
                // reservations. Started work keeps draining below.
                queue.clear();
                receipts.clear();
                publishable.clear();
            } else {
                tracing::debug!(error = %format!("{error:#}"), "additional failure while draining started transaction work");
            }
        }
    }

    stats.peak_inflight_bytes = budget.peak();
    stats.peak_encoders = encoding.peak();
    stats.peak_publications = publishing.peak();
    if let Some(error) = failure {
        return Err(error);
    }
    anyhow::ensure!(
        queue.is_empty() && receipts.is_empty() && publishable.is_empty() && budget.used() == 0,
        "transaction table work ended with unfinished parts"
    );
    Ok((
        pending.context("pending record is unavailable after publication")?,
        stats,
    ))
}

/// One blocking encoder. Its closure owns an `Arc` of immutable prepared
/// batches and its reservation; if the transaction future is dropped, a running
/// closure only finishes computing private bytes and then releases them.
fn encode<'f>(
    prepared: Arc<PreparedFlush>,
    index: u32,
    table: String,
    exclusive: bool,
    spooled: bool,
    reservation: Reservation,
    encoding: Arc<Observed>,
) -> Work<'f> {
    let task = tokio::task::spawn_blocking(move || {
        let _running = encoding.enter();
        #[cfg(test)]
        tests::delay(&table);
        if fault::fires("encode", &table) {
            anyhow::bail!("injected debug fault: encoding {table} failed");
        }
        if spooled {
            prepared.encode_spooled(index, Some(reservation))
        } else {
            prepared.encode(index, Some(reservation))
        }
    });
    Box::pin(async move {
        let result = task
            .await
            .context("protected encoder worker failed")
            .and_then(|result| result);
        Event::Encoded {
            index,
            exclusive,
            result,
        }
    })
}

async fn publish(parts: &TransactionParts<'_>, part: &Staged) -> Result<()> {
    fault_before_publication(&part.table)?;
    parts.publish(&part.encoded).await?;
    fault_after_publication(&part.table)
}

/// Local publication on the supervised lane; the part and its reservation
/// drop on the lane thread once the file is durable (or the attempt failed).
fn publish_blocking(parts: &TransactionParts<'_>, part: Staged) -> Result<()> {
    fault_before_publication(&part.table)?;
    parts.publish_local(&part.encoded)?;
    fault_after_publication(&part.table)
}

fn fault_before_publication(table: &str) -> Result<()> {
    if fault::fires("publish", table) {
        anyhow::bail!("injected debug fault: publishing {table} failed");
    }
    Ok(())
}

fn fault_after_publication(table: &str) -> Result<()> {
    if fault::fires("crash-after-publish", table) {
        // An abrupt process death between parts: no unwinding or cleanup.
        std::process::abort();
    }
    if fault::fires("lost-ack", table) {
        anyhow::bail!(
            "injected debug fault: {table} was published but its acknowledgement was lost"
        );
    }
    Ok(())
}

/// The journaled receipt: the part's exact bytes, plus its Delta `add`
/// statistics and modification time (#643 L3), fixed here once so that every
/// commit of the part (and a later roll-forward) is identical.
fn part_receipt(encoded: &EncodedPart) -> Result<PartReceipt> {
    let modification_time = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .context("the system clock is before the Unix epoch")?
        .as_millis();
    Ok(PartReceipt {
        byte_size: encoded.receipt().byte_size,
        sha256: Digest::parse(encoded.receipt().sha256.clone())?,
        stats: encoded.delta_stats().to_string(),
        modification_time: i64::try_from(modification_time)
            .context("the system clock is beyond the Delta modification time range")?,
    })
}

/// Fault injection for recovery tests of the real binary. Only debug builds
/// (as built by `cargo test`) read `FIREPARQ_DEBUG_FAULT=<kind>:<table>`;
/// release builds compile every check to `false`. Kinds: `encode`, `publish`
/// (fails before any request), `lost-ack` (fails after a successful
/// publication) and `crash-after-publish` (aborts the process), and for the
/// Delta commit step (#643 L3 and L4, in the controller, also during a
/// recovery roll-forward): `delta-commit` (fails before the table's commit
/// request), `crash-after-delta-commit` (aborts once the table's commit is durable;
/// with `blocks`, the last table, that is after every Delta commit and
/// before authority advances) and `delta-commit-lost-response` (the commit
/// lands, then fails as if its response were lost). `crash-at:<Stage>`
/// aborts at a transaction boundary (`CommittedPersisted`,
/// `AuthorityAdvanced`, ...), and the session's
/// `crash-after-delta-create:<table>` aborts once that table is created.
pub(super) mod fault {
    #[cfg(debug_assertions)]
    pub(in crate::ingest::controller) fn fires(kind: &str, table: &str) -> bool {
        use std::sync::OnceLock;
        #[cfg(test)]
        if let Some((test_kind, test_table)) = super::tests::fault() {
            return test_kind == kind && test_table == table;
        }
        static SPEC: OnceLock<Option<(String, String)>> = OnceLock::new();
        SPEC.get_or_init(|| {
            let value = std::env::var("FIREPARQ_DEBUG_FAULT").ok()?;
            let (kind, table) = value.split_once(':')?;
            Some((kind.to_string(), table.to_string()))
        })
        .as_ref()
        .is_some_and(|(spec_kind, spec_table)| spec_kind == kind && spec_table == table)
    }

    #[cfg(not(debug_assertions))]
    pub(in crate::ingest::controller) fn fires(_kind: &str, _table: &str) -> bool {
        false
    }
}

#[cfg(test)]
pub(super) mod tests {
    use std::sync::Mutex;

    /// Fault override for unit tests, keyed by table name: tests that set it
    /// use table names no other test uses.
    static FAULT: Mutex<Option<(&'static str, String)>> = Mutex::new(None);

    pub fn fault() -> Option<(&'static str, String)> {
        FAULT.lock().unwrap().clone()
    }
    pub fn set_fault(fault: Option<(&'static str, &str)>) {
        *FAULT.lock().unwrap() = fault.map(|(kind, table)| (kind, table.to_string()));
    }

    /// Tables named `slow_*` encode slowly, so overlap is observable.
    pub fn delay(table: &str) {
        if table.starts_with("slow_") {
            std::thread::sleep(std::time::Duration::from_millis(60));
        }
    }
}

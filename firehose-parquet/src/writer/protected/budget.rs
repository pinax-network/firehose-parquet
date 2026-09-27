//! Byte-weighted admission for encoded parts of one transaction (#516).
//!
//! A [`Reservation`] travels with its part: the encoder grows it while writing
//! (never blocking; a refused growth stops the encoder), shrinks it to the exact
//! encoded size, and dropping the part (after publication, on failure or on a
//! discarded result) releases it. Accounting therefore cannot leak on any error
//! or cancellation path, and reserved bytes never exceed the limit except for
//! the single exclusive reservation described on [`InflightBudget::try_reserve_exclusive`].

use std::sync::{Arc, Mutex};

/// Growth is requested in steps of at least this many bytes.
const GROWTH_STEP: u64 = 1024 * 1024;

#[derive(Debug)]
pub(crate) struct InflightBudget {
    limit: u64,
    state: Mutex<BudgetState>,
}

#[derive(Debug, Default)]
struct BudgetState {
    used: u64,
    peak: u64,
    exclusive: bool,
}

impl InflightBudget {
    pub(crate) fn new(limit: u64) -> Arc<Self> {
        Arc::new(Self {
            limit,
            state: Mutex::new(BudgetState::default()),
        })
    }

    pub(crate) fn limit(&self) -> u64 {
        self.limit
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, BudgetState> {
        // Plain counters: a panic while holding the lock cannot corrupt them
        // beyond what the panic itself already reports.
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    pub(crate) fn used(&self) -> u64 {
        self.lock().used
    }

    pub(crate) fn peak(&self) -> u64 {
        self.lock().peak
    }

    /// Reserve `bytes` if they fit under the limit next to everything else.
    pub(crate) fn try_reserve(self: &Arc<Self>, bytes: u64) -> Option<Reservation> {
        let mut state = self.lock();
        if state.exclusive || state.used.checked_add(bytes)? > self.limit {
            return None;
        }
        state.used += bytes;
        state.peak = state.peak.max(state.used);
        Some(Reservation {
            budget: Arc::clone(self),
            held: bytes,
            exclusive: false,
        })
    }

    /// Only when nothing else holds bytes: reserve the whole budget for one
    /// part that may then grow past the limit. This is the only way reserved
    /// bytes can exceed the limit, and it admits nothing else until released.
    pub(crate) fn try_reserve_exclusive(self: &Arc<Self>) -> Option<Reservation> {
        let mut state = self.lock();
        if state.used != 0 {
            return None;
        }
        state.used = self.limit;
        state.exclusive = true;
        state.peak = state.peak.max(state.used);
        Some(Reservation {
            budget: Arc::clone(self),
            held: self.limit,
            exclusive: true,
        })
    }
}

/// Bytes held for one part. Released on drop.
#[derive(Debug)]
pub(crate) struct Reservation {
    budget: Arc<InflightBudget>,
    held: u64,
    exclusive: bool,
}

impl Reservation {
    pub(crate) fn held(&self) -> u64 {
        self.held
    }

    /// Ensure at least `total` bytes are held, growing by at least one step.
    /// Never blocks: returns false when the budget cannot grant the growth.
    pub(crate) fn grow_to(&mut self, total: u64) -> bool {
        if total <= self.held {
            return true;
        }
        let target = total.max(self.held.saturating_add(GROWTH_STEP));
        let mut state = self.budget.lock();
        let extra = target - self.held;
        if !self.exclusive {
            let Some(next) = state.used.checked_add(extra) else {
                return false;
            };
            if next > self.budget.limit {
                // Fall back to exactly what is needed before refusing.
                let needed = total - self.held;
                if state.used.saturating_add(needed) > self.budget.limit {
                    return false;
                }
                state.used += needed;
                self.held = total;
                state.peak = state.peak.max(state.used);
                return true;
            }
        }
        state.used = state.used.saturating_add(extra);
        self.held = target;
        state.peak = state.peak.max(state.used);
        true
    }

    /// Return bytes beyond `total` (the exact encoded size) to the budget. An
    /// exclusive reservation stays exclusive until it is dropped.
    pub(crate) fn shrink_to(&mut self, total: u64) {
        if self.exclusive || total >= self.held {
            return;
        }
        let mut state = self.budget.lock();
        state.used -= self.held - total;
        self.held = total;
    }
}

impl Drop for Reservation {
    fn drop(&mut self) {
        let mut state = self.budget.lock();
        state.used -= self.held;
        if self.exclusive {
            state.exclusive = false;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reservations_grow_shrink_and_release_without_exceeding_the_limit() {
        let budget = InflightBudget::new(10 * GROWTH_STEP);
        let mut first = budget.try_reserve(GROWTH_STEP).unwrap();
        let mut second = budget.try_reserve(4 * GROWTH_STEP).unwrap();
        assert!(first.grow_to(5 * GROWTH_STEP));
        assert_eq!(budget.used(), 9 * GROWTH_STEP);
        // Needs one more step than remains: exact fallback succeeds.
        assert!(second.grow_to(5 * GROWTH_STEP));
        assert_eq!(budget.used(), 10 * GROWTH_STEP);
        assert!(!second.grow_to(5 * GROWTH_STEP + 1));
        assert!(budget.try_reserve(1).is_none());
        assert!(budget.try_reserve_exclusive().is_none());
        first.shrink_to(GROWTH_STEP);
        assert_eq!(budget.used(), 6 * GROWTH_STEP);
        drop(first);
        drop(second);
        assert_eq!(budget.used(), 0);
        assert_eq!(budget.peak(), 10 * GROWTH_STEP);
    }

    #[test]
    fn exclusive_reservation_may_overshoot_alone_and_blocks_others() {
        let budget = InflightBudget::new(GROWTH_STEP);
        let held = budget.try_reserve(1).unwrap();
        assert!(budget.try_reserve_exclusive().is_none());
        drop(held);
        let mut exclusive = budget.try_reserve_exclusive().unwrap();
        assert!(budget.try_reserve(1).is_none());
        assert!(exclusive.grow_to(3 * GROWTH_STEP));
        exclusive.shrink_to(1);
        assert_eq!(budget.used(), 3 * GROWTH_STEP);
        assert!(budget.try_reserve(1).is_none());
        drop(exclusive);
        assert_eq!(budget.used(), 0);
        assert!(budget.try_reserve(1).is_some());
        assert_eq!(budget.peak(), 3 * GROWTH_STEP);
    }
}

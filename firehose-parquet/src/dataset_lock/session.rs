//! One multi-record protocol controller per borrowed ownership guard. This is
//! separate from the short-lived per-record mutation mutex, so a controller may
//! retain its permit while its own parts, control records and mirror do I/O.

use anyhow::{bail, Result};
use std::sync::atomic::{AtomicBool, Ordering};

#[derive(Default)]
pub(crate) struct SessionSlot(AtomicBool);

pub(crate) struct SessionPermit<'a>(&'a AtomicBool);

impl SessionSlot {
    pub(crate) fn acquire(&self) -> Result<SessionPermit<'_>> {
        if self
            .0
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            bail!("an ingestion transaction controller already uses this ownership guard");
        }
        Ok(SessionPermit(&self.0))
    }
}
impl Drop for SessionPermit<'_> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

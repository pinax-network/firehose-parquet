//! Staged ingestion transaction implementation. No runtime mode is exposed until
//! the controller, recovery and complete caller integration have been qualified.

pub(crate) mod frontier;
pub(crate) mod state;

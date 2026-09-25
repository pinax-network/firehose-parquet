//! Staged ingestion transaction implementation. No runtime mode is exposed until
//! the controller, recovery and complete caller integration have been qualified.

pub(crate) mod controller;
pub(crate) mod frontier;
pub(crate) mod parts;
pub(crate) mod state;
pub(crate) mod store;

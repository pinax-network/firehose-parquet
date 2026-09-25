//! Authoritative ingestion transactions and accepted stream progress.

pub(crate) mod binding;
pub(crate) mod controller;
pub(crate) mod eligibility;
pub(crate) mod frontier;
pub(crate) mod mirror;
pub(crate) mod parts;
pub(crate) mod session;
pub(crate) mod state;
pub(crate) mod store;

pub use controller::CommittedFlush;
pub use session::{
    declare_inventory, load_authoritative_resume, IngestionSession, MapperSemantics,
};
pub use state::{BlockFamily, Digest};

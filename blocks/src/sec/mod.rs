//! SEC EDGAR (`pinax.sec.v1.Block`, firesec 0.13.0): 43 tables, one block per
//! 10-minute EDGAR window. See `docs/chains/sec.md` and the generated
//! `docs/schemas/sec.md`.
//!
//! - `schema`: the tables as data (columns, types, descriptions).
//! - `parse`: the §4 parsers; `issues`: `parse_issues` recording.
//! - `prepare`: the fallible preflight of a block; `build`: the infallible
//!   append into one typed builder per table.
//! - `mapper`: [`mapper::SecBlockMapper`].

pub mod mapper;
pub mod proto;
pub mod schema;

// Scaffolding: the shared helpers are used by the per-form modules as they
// land. Remove these allowances at integration.
#[allow(dead_code)]
pub(crate) mod build;
#[allow(dead_code)]
pub(crate) mod issues;
#[allow(dead_code)]
pub(crate) mod parse;
#[allow(dead_code)]
pub(crate) mod prepare;

#[cfg(test)]
mod parse_tests;
#[cfg(test)]
#[allow(dead_code)]
pub(crate) mod tests;

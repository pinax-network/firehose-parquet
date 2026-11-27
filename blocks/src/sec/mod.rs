//! SEC EDGAR (`pinax.sec.v1.Block`, firesec 0.13.0): 43 tables, one block per
//! 10-minute EDGAR window. See `docs/chains/sec.md` and the generated
//! `docs/schemas/sec.md`.
//!
//! - `schema`: the tables as data (columns, types, descriptions).
//! - `parse`: the §4 parsers; `issues`: `parse_issues` recording.
//! - `prepare`: the fallible preflight of a block; `build`: the infallible
//!   append into one typed builder per table.
//! - `mapper`: [`mapper::SecBlockMapper`].
//!
//! Comments cite the design specification this module was written from by
//! section (`§n`). It is not in the repository; what it specified is:
//! - §2–§3, the tables: `schema`, from which `docs/schemas/sec.md` is
//!   generated;
//! - §4, value parsing and `parse_issues`: the `parse` and `issues` docs and
//!   `docs/chains/sec.md` ("Value semantics");
//! - §1 and §8, principles and implementation rules: stated where cited.
//!   The §8.7 sample filings, which the unit tests mirror, are blocks of
//!   `blocks/tests/fixtures/sec-v013/` (its README lists them).

pub mod mapper;
pub mod proto;
pub mod schema;

pub(crate) mod build;
pub(crate) mod issues;
pub(crate) mod parse;
pub(crate) mod prepare;

#[cfg(test)]
mod parse_tests;
#[cfg(test)]
pub(crate) mod tests;

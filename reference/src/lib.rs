//! HyperLiquid `/info` reference tables (`hl_*`), the library behind
//! `fireparq hypercore-reference`: a port of k8s-parquet PR #82's Python job.
//!
//! Names, labels and parameters that HyperCore blocks do not carry (token and
//! pair symbols, perp dexes and assets, HIP-4 outcomes and questions, funding
//! history) come from HyperLiquid's `POST /info`. The job snapshots them into
//! append-only Delta tables in their own root, never a fireparq root: a row is
//! appended only when an entity's payload changed, so each table is a change
//! log, and every row keeps the entity's canonical JSON and its SHA-256.
//!
//! - [`json`]: the lossless JSON tree and the canonical text payload hashes
//!   cover;
//! - [`values`]: the typed values parsed from payloads (decimals, times,
//!   `k:v|k:v` descriptions, ids);
//! - [`model`]: the eleven tables' columns and the rows built from payloads;
//! - [`arrow`]: their Delta schemas and properties, and the rows as Arrow
//!   record batches (delta-rs's Arrow 59);
//! - [`info`]: the rate-limited, retrying `/info` client, which never shows
//!   the endpoint URL;
//! - [`clock`] and [`log`]: time, and the JSON lines on stdout.
//!
//! Nothing here links DataFusion, and no serde_json feature beyond the
//! binary's `raw_value` is enabled (#698).

pub mod arrow;
pub mod clock;
pub mod info;
pub mod json;
pub mod log;
pub mod model;
#[cfg(any(test, feature = "test-support"))]
pub mod testing;
pub mod values;

use std::fmt;

/// A setting that makes the job unsafe or impossible: exit status 2, before
/// any request. The message names settings, never their values.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConfigError(pub String);

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for ConfigError {}

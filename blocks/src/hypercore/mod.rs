//! HyperCore, the HyperLiquid L1 (`pinax.hypercore.v1`, `--block-type hypercore`).
//!
//! Twelve tables by product family (`docs/schemas/hypercore.md`): the raw
//! record (`blocks`, `fills`, the five event tables `transfers`,
//! `bridge_transfers`, `vault_events`, `staking_events` and `other_events`,
//! `funding_deltas` and `validator_rewards`) and the tables derived from the
//! same block (`outcome_fills`, `liquidations`, `funding_rates`). The chain
//! notes, refusal and derivation rules, routing, views and monitors are in
//! `docs/chains/hypercore.md`.

pub mod decimal;
pub mod mapper;
pub mod proto;
pub mod schema;

#[cfg(test)]
pub(crate) mod fixtures;
#[cfg(test)]
mod value_tests;

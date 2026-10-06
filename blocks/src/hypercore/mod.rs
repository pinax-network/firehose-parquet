//! HyperCore, the HyperLiquid L1 (`pinax.hypercore.v1`, `--block-type hypercore`).
//!
//! Five tables: `blocks`, `fills`, `events`, `funding_deltas` and
//! `validator_rewards` (`docs/schemas/hypercore.md`); the chain notes,
//! refusal rules, views and monitors are in `docs/chains/hypercore.md`.

pub mod decimal;
pub mod mapper;
pub mod proto;
pub mod schema;


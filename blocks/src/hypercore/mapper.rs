//! HyperCore (`pinax.hypercore.v1.Block`) mapper.
//!
//! One block maps onto twelve tables (`schema.rs`): one `blocks` row, one
//! `fills` row per fill, one row per event in one of the five event tables
//! (its single body, and for ledger updates its delta, flattened into the
//! row), the items of funding and validator-reward events in `funding_deltas`
//! and `validator_rewards`, and the rows derived from the same block:
//! `outcome_fills`, `liquidations` and `funding_rates`.
//!
//! Every block is decoded, guarded against unknown fields, checked against its
//! Firehose identity, then validated and converted completely into staged
//! values before any builder is touched: a refused block appends nothing to
//! any table (`docs/chains/hypercore.md`, "Refusals"). Every refusal names the
//! block, the proto path and the offending value. `derive` then computes the
//! derived values from the staged block alone (rules R-D1 to R-D6), and never
//! refuses.
//!
//! Each proto message is destructured exhaustively (no `..`) and both oneofs
//! are matched without a `_` arm, so vendoring protos with a new field or case
//! fails to compile until the new value is placed.

use std::collections::HashMap;
use std::fmt::{Debug, Display};
use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use arrow::array::{
    ArrayBuilder, ArrayRef, BooleanBuilder, Decimal128Builder, Int64Builder, ListBuilder,
    StringBuilder, StringDictionaryBuilder, StructBuilder, TimestampMillisecondBuilder,
    UInt32Builder, UInt64Builder,
};
use arrow::datatypes::{Int32Type, SchemaRef};
use arrow::record_batch::RecordBatch;
use firehose_parquet::encode::{BytesColumn, BytesListColumn, EncodeBytes};
use firehose_parquet::traits::{
    append_fork_step, est_bool, est_decimal128, est_fork_step, est_i64, est_str, est_ts_ms,
    est_u32, est_u64, estimated_dictionary_index_bytes, finish_fork_step, fork_step_builder,
    strip_enum_prefix, timestamp_millis, BlockIdentity, BlockMapper, CanonicalBuilder,
    ForkStepBuilder, PreparedIdentity, StreamEvent,
};
use prost::bytes::Bytes;
use prost::Message;

use super::decimal;
use super::proto::hypercore as pb;
use super::schema::{self, EventTable};
use pb::{event_body, ledger_update_delta};

const NANOS_PER_SECOND: i64 = 1_000_000_000;
const NANOS_PER_MILLI: i32 = 1_000_000;

// ===========================================================================
// Refusals (`docs/chains/hypercore.md`, R1–R11)
// ===========================================================================

/// A value's `Debug` text, cut to 80 characters for an error message.
fn shown(value: &impl Debug) -> String {
    let text = format!("{value:?}");
    match text.char_indices().nth(80) {
        Some((cut, _)) => format!("{}…", &text[..cut]),
        None => text,
    }
}

/// Stages every item of a list into a vector sized up front. Collecting through
/// `Result` cannot preallocate, and a funding list has about 435k items.
fn stage_all<'a, T, U>(
    items: &'a [T],
    mut stage: impl FnMut(usize, &'a T) -> Result<U>,
) -> Result<Vec<U>> {
    let mut staged = Vec::with_capacity(items.len());
    for (index, item) in items.iter().enumerate() {
        staged.push(stage(index, item)?);
    }
    Ok(staged)
}

/// The checks of one block. Per-row paths (fills, funding deltas, rewards,
/// positions, fields) are built only when a check fails; an event's body path
/// is formatted once per event.
#[derive(Clone, Copy)]
struct Check {
    block_num: u64,
}

impl Check {
    fn fail(self, path: impl Display, reason: impl Display) -> anyhow::Error {
        anyhow!("hypercore block {}: {path}: {reason}", self.block_num)
    }

    /// R8 and R9: a required decimal string.
    fn dec(self, value: &str, path: impl FnOnce() -> String) -> Result<i128> {
        if value.is_empty() {
            return Err(self.fail(path(), "empty string"));
        }
        decimal::parse(value)
            .map_err(|reason| self.fail(path(), format!("{reason} in decimal {}", shown(&value))))
    }

    /// §2.7: a decimal string whose `''` means "absent" (NULL).
    fn dec_or_null(self, value: &str, path: impl FnOnce() -> String) -> Result<Option<i128>> {
        if value.is_empty() {
            Ok(None)
        } else {
            self.dec(value, path).map(Some)
        }
    }

    /// R9: a required string.
    fn text<'a>(self, value: &'a str, path: impl FnOnce() -> String) -> Result<&'a str> {
        if value.is_empty() {
            Err(self.fail(path(), "empty string"))
        } else {
            Ok(value)
        }
    }

    /// R10: required bytes.
    fn bytes<'a>(self, value: &'a Bytes, path: impl FnOnce() -> String) -> Result<&'a [u8]> {
        if value.is_empty() {
            Err(self.fail(path(), "empty bytes"))
        } else {
            Ok(&value[..])
        }
    }

    /// R5: a `UInt64` column is a checked Delta `long`.
    fn long(self, value: u64, path: impl FnOnce() -> String) -> Result<u64> {
        if value > i64::MAX as u64 {
            Err(self.fail(path(), format!("{value} exceeds the Delta long range")))
        } else {
            Ok(value)
        }
    }

    /// R4: a list length or position stored as `UInt32`.
    fn count(self, len: usize, path: impl FnOnce() -> String) -> Result<u32> {
        u32::try_from(len).map_err(|_| self.fail(path(), format!("{len} items exceed u32")))
    }

    fn unknown_enum(self, path: String, name: &str, value: i32) -> anyhow::Error {
        self.fail(path, format!("unknown or unspecified {name} value {value}"))
    }

    /// R7: `FillSide`, prefix `FILL_SIDE_`.
    fn side(self, value: i32, path: impl FnOnce() -> String) -> Result<&'static str> {
        match pb::FillSide::try_from(value) {
            Ok(pb::FillSide::Unspecified) | Err(_) => {
                Err(self.unknown_enum(path(), "FillSide", value))
            }
            Ok(side) => Ok(strip_enum_prefix(side.as_str_name(), "FILL_SIDE_")),
        }
    }

    /// R7: `TradingDirection`, prefix `TRADING_DIRECTION_`.
    fn direction(self, value: i32, path: impl FnOnce() -> String) -> Result<&'static str> {
        match pb::TradingDirection::try_from(value) {
            Ok(pb::TradingDirection::Unspecified) | Err(_) => {
                Err(self.unknown_enum(path(), "TradingDirection", value))
            }
            Ok(direction) => Ok(strip_enum_prefix(
                direction.as_str_name(),
                "TRADING_DIRECTION_",
            )),
        }
    }

    /// R7: `LeverageType`, prefix `LEVERAGE_TYPE_`.
    fn leverage_type(self, value: i32, path: impl FnOnce() -> String) -> Result<&'static str> {
        match pb::LeverageType::try_from(value) {
            Ok(pb::LeverageType::Unspecified) | Err(_) => {
                Err(self.unknown_enum(path(), "LeverageType", value))
            }
            Ok(leverage) => Ok(strip_enum_prefix(leverage.as_str_name(), "LEVERAGE_TYPE_")),
        }
    }

    /// R11: `Fill.time`, a whole millisecond in the calendar range.
    fn fill_time(
        self,
        time: &Option<prost_types::Timestamp>,
        path: impl Fn() -> String,
    ) -> Result<i64> {
        let Some(time) = time else {
            return Err(self.fail(path(), "missing"));
        };
        if !(0..NANOS_PER_SECOND as i32).contains(&time.nanos) {
            return Err(self.fail(path(), format!("nanos {} is outside [0, 1e9)", time.nanos)));
        }
        if time.nanos % NANOS_PER_MILLI != 0 {
            return Err(self.fail(
                path(),
                format!("nanos {} is not a whole millisecond", time.nanos),
            ));
        }
        timestamp_millis(time.seconds, time.nanos).map_err(|error| self.fail(path(), error))
    }

    /// R11 (`Event.time`) and R3 (`BlockHeader.block_time`): Unix
    /// nanoseconds, checked.
    fn time_ns(
        self,
        time: &Option<prost_types::Timestamp>,
        path: impl Fn() -> String,
    ) -> Result<i64> {
        let Some(time) = time else {
            return Err(self.fail(path(), "missing"));
        };
        if !(0..NANOS_PER_SECOND as i32).contains(&time.nanos) {
            return Err(self.fail(path(), format!("nanos {} is outside [0, 1e9)", time.nanos)));
        }
        time.seconds
            .checked_mul(NANOS_PER_SECOND)
            .and_then(|ns| ns.checked_add(i64::from(time.nanos)))
            .ok_or_else(|| {
                self.fail(
                    path(),
                    format!(
                        "{}s {}ns overflows i64 nanoseconds",
                        time.seconds, time.nanos
                    ),
                )
            })
    }
}

// ===========================================================================
// Staged rows: every value validated and converted, nothing appended yet
// ===========================================================================

struct StagedBlock<'a> {
    block_time_ns: i64,
    fill_count: u32,
    event_count: u32,
    fills: Vec<StagedFill<'a>>,
    events: Vec<StagedEvent<'a>>,
}

struct StagedFill<'a> {
    user: &'a [u8],
    coin: &'a str,
    price: i128,
    size: i128,
    side: &'static str,
    fill_time: i64,
    start_position: i128,
    direction: &'static str,
    closed_pnl: i128,
    hash: &'a [u8],
    order_id: u64,
    crossed: bool,
    fee: i128,
    transaction_id: u64,
    fee_token: &'a str,
    twap_id: Option<u64>,
    client_order_id: Option<&'a [u8]>,
    liquidated_user: Option<&'a [u8]>,
    liquidation_mark_px: Option<i128>,
    liquidation_method: Option<&'a str>,
    deployer_fee: Option<i128>,
    builder: Option<&'a str>,
    builder_fee: Option<i128>,
    priority_gas: Option<i128>,
    /// The fill's `extra_json`, which the derived copies carry verbatim (D7).
    /// This version writes none: every `Fill` field has a typed column.
    extra_json: Option<&'a str>,
}

/// One event row. A column the row's type does not have stays `None`
/// (NULL); the populated set per type is the matrix in
/// `docs/chains/hypercore.md`.
#[derive(Default)]
struct StagedEvent<'a> {
    event_type: &'static str,
    ledger_type: Option<&'static str>,
    hash: &'a [u8],
    event_time_ns: i64,
    users: Option<&'a [Bytes]>,
    user: Option<&'a [u8]>,
    destination: Option<&'a [u8]>,
    vault: Option<&'a [u8]>,
    validator: Option<&'a [u8]>,
    sub_account: Option<&'a [u8]>,
    token: Option<&'a str>,
    amount: Option<i128>,
    usdc: Option<i128>,
    usdc_value: Option<i128>,
    fee: Option<i128>,
    fee_token: Option<&'a str>,
    native_token_fee: Option<i128>,
    nonce: Option<u64>,
    source_dex: Option<&'a str>,
    destination_dex: Option<&'a str>,
    dex: Option<&'a str>,
    is_deposit: Option<bool>,
    to_perp: Option<bool>,
    is_undelegate: Option<bool>,
    is_finalized: Option<bool>,
    requested_usd: Option<i128>,
    commission: Option<i128>,
    closing_cost: Option<i128>,
    basis: Option<i128>,
    net_withdrawn_usd: Option<i128>,
    interest_amount: Option<i128>,
    operation: Option<&'a str>,
    liquidated_ntl_pos: Option<i128>,
    account_value: Option<i128>,
    leverage_type: Option<&'static str>,
    liquidated_positions: Option<Vec<(&'a str, i128)>>,
    slot_id: Option<u64>,
    previous_winner_ip: Option<&'a str>,
    end_gas: Option<i128>,
    sub_account_name: Option<&'a str>,
    item_count: Option<u32>,
    items: Items<'a>,
}

/// The child rows of a funding or validator-rewards event.
#[derive(Default)]
enum Items<'a> {
    #[default]
    None,
    FundingDeltas(Vec<StagedFundingDelta<'a>>),
    ValidatorRewards(Vec<StagedValidatorReward<'a>>),
}

struct StagedFundingDelta<'a> {
    user: &'a [u8],
    coin: &'a str,
    funding_amount: i128,
    szi: i128,
    funding_rate: i128,
}

struct StagedValidatorReward<'a> {
    validator: &'a [u8],
    reward: i128,
}

/// R3: the block header must match the Firehose identity exactly. Returns the
/// block time in nanoseconds.
fn check_header(check: Check, block: &pb::Block, identity: &BlockIdentity) -> Result<i64> {
    let header_path = || "block_header".to_string();
    let Some(pb::BlockHeader {
        block_number,
        block_time,
    }) = &block.block_header
    else {
        return Err(check.fail(header_path(), "missing"));
    };
    if *block_number != identity.block_num {
        return Err(check.fail(
            header_path(),
            format!(
                "block_number {block_number} differs from the Firehose block number {}",
                identity.block_num
            ),
        ));
    }
    let block_time_ns = check.time_ns(block_time, || "block_header.block_time".to_string())?;
    let time = block_time.as_ref().expect("checked by time_ns");
    if time.seconds != identity.timestamp || time.nanos != identity.timestamp_nanos {
        return Err(check.fail(
            header_path(),
            format!(
                "block_time {}s {}ns differs from the Firehose block time {}s {}ns",
                time.seconds, time.nanos, identity.timestamp, identity.timestamp_nanos
            ),
        ));
    }
    Ok(block_time_ns)
}

/// Validate and convert the whole block (R4–R11), appending nothing.
fn stage<'a>(check: Check, block: &'a pb::Block, block_time_ns: i64) -> Result<StagedBlock<'a>> {
    let pb::Block {
        block_header: _,
        fills,
        events,
    } = block;
    let fill_count = check.count(fills.len(), || "fills".to_string())?;
    let event_count = check.count(events.len(), || "events".to_string())?;
    let fills = stage_all(fills, |index, fill| stage_fill(check, index, fill))?;
    let events = stage_all(events, |index, event| stage_event(check, index, event))?;
    Ok(StagedBlock {
        block_time_ns,
        fill_count,
        event_count,
        fills,
        events,
    })
}

fn stage_fill(check: Check, index: usize, fill: &pb::Fill) -> Result<StagedFill<'_>> {
    let path = |field: &str| format!("fills[{index}].{field}");
    let pb::Fill {
        user,
        coin,
        price,
        size,
        side,
        time,
        start_position,
        direction,
        closed_pnl,
        hash,
        order_id,
        crossed,
        fee,
        transaction_id,
        fee_token,
        twap_id,
        client_order_id,
        liquidation,
        deployer_fee,
        builder,
        builder_fee,
        priority_gas,
    } = fill;
    let user = check.bytes(user, || path("user"))?;
    let coin = check.text(coin, || path("coin"))?;
    let price = check.dec(price, || path("price"))?;
    let size = check.dec(size, || path("size"))?;
    let side = check.side(*side, || path("side"))?;
    let fill_time = check.fill_time(time, || path("time"))?;
    let start_position = check.dec(start_position, || path("start_position"))?;
    let direction = check.direction(*direction, || path("direction"))?;
    let closed_pnl = check.dec(closed_pnl, || path("closed_pnl"))?;
    let hash = check.bytes(hash, || path("hash"))?;
    let order_id = check.long(*order_id, || path("order_id"))?;
    let fee = check.dec(fee, || path("fee"))?;
    let transaction_id = check.long(*transaction_id, || path("transaction_id"))?;
    let fee_token = check.text(fee_token, || path("fee_token"))?;
    let twap_id = twap_id
        .map(|twap_id| check.long(twap_id, || path("twap_id")))
        .transpose()?;
    let client_order_id = (!client_order_id.is_empty()).then_some(&client_order_id[..]);
    let (liquidated_user, liquidation_mark_px, liquidation_method) = match liquidation {
        Some(pb::FillLiquidation {
            liquidated_user,
            mark_px,
            method,
        }) => (
            (!liquidated_user.is_empty()).then_some(&liquidated_user[..]),
            Some(check.dec(mark_px, || path("liquidation.mark_px"))?),
            Some(check.text(method, || path("liquidation.method"))?),
        ),
        None => (None, None, None),
    };
    let deployer_fee = check.dec_or_null(deployer_fee, || path("deployer_fee"))?;
    let builder = (!builder.is_empty()).then_some(builder.as_str());
    let builder_fee = check.dec_or_null(builder_fee, || path("builder_fee"))?;
    let priority_gas = check.dec_or_null(priority_gas, || path("priority_gas"))?;
    Ok(StagedFill {
        user,
        coin,
        price,
        size,
        side,
        fill_time,
        start_position,
        direction,
        closed_pnl,
        hash,
        order_id,
        crossed: *crossed,
        fee,
        transaction_id,
        fee_token,
        twap_id,
        client_order_id,
        liquidated_user,
        liquidation_mark_px,
        liquidation_method,
        deployer_fee,
        builder,
        builder_fee,
        priority_gas,
        extra_json: None,
    })
}

fn stage_event(check: Check, index: usize, event: &pb::Event) -> Result<StagedEvent<'_>> {
    let pb::Event { time, hash, events } = event;
    let event_time_ns = check.time_ns(time, || format!("events[{index}].time"))?;
    let hash = check.bytes(hash, || format!("events[{index}].hash"))?;
    // R6: upstream reads the bodies from a Go map, so several bodies would
    // have no order to keep. One body was observed on every event.
    let [pb::EventBody { event: body }] = events.as_slice() else {
        return Err(check.fail(
            format!("events[{index}].events"),
            format!("{} bodies, expected 1", events.len()),
        ));
    };
    let body_path = format!("events[{index}].events[0]");
    let Some(body) = body else {
        return Err(check.fail(body_path, "EventBody has no case"));
    };
    let row = StagedEvent {
        hash,
        event_time_ns,
        ..StagedEvent::default()
    };
    let at = |case: &'static str| {
        let body_path = &body_path;
        move |field: &str| format!("{body_path}.{case}.{field}")
    };
    Ok(match body {
        event_body::Event::LedgerUpdate(pb::LedgerUpdate { users, delta }) => {
            let p = at("ledger_update");
            for (position, user) in users.iter().enumerate() {
                check.bytes(user, || p(&format!("users[{position}]")))?;
            }
            let Some(pb::LedgerUpdateDelta { delta }) = delta else {
                return Err(check.fail(p("delta"), "missing, so LedgerUpdateDelta has no case"));
            };
            let Some(delta) = delta else {
                return Err(check.fail(p("delta"), "LedgerUpdateDelta has no case"));
            };
            let row = StagedEvent {
                event_type: "ledger_update",
                users: Some(users.as_slice()),
                ..row
            };
            stage_ledger_delta(
                check,
                &format!("{body_path}.ledger_update.delta"),
                delta,
                row,
            )?
        }
        event_body::Event::Funding(pb::Funding { deltas }) => {
            let p = at("funding");
            let item_count = check.count(deltas.len(), || p("deltas"))?;
            let deltas = stage_all(deltas, |position, delta| {
                let path = |field: &str| p(&format!("deltas[{position}].{field}"));
                let pb::FundingDelta {
                    user,
                    coin,
                    funding_amount,
                    szi,
                    funding_rate,
                } = delta;
                Ok(StagedFundingDelta {
                    user: check.bytes(user, || path("user"))?,
                    coin: check.text(coin, || path("coin"))?,
                    funding_amount: check.dec(funding_amount, || path("funding_amount"))?,
                    szi: check.dec(szi, || path("szi"))?,
                    funding_rate: check.dec(funding_rate, || path("funding_rate"))?,
                })
            })?;
            StagedEvent {
                event_type: "funding",
                item_count: Some(item_count),
                items: Items::FundingDeltas(deltas),
                ..row
            }
        }
        event_body::Event::ValidatorRewards(pb::ValidatorRewards {
            validator_to_reward,
        }) => {
            let p = at("validator_rewards");
            let item_count = check.count(validator_to_reward.len(), || p("validator_to_reward"))?;
            let rewards = stage_all(validator_to_reward, |position, reward| {
                let path = |field: &str| p(&format!("validator_to_reward[{position}].{field}"));
                let pb::ValidatorReward { validator, reward } = reward;
                Ok(StagedValidatorReward {
                    validator: check.bytes(validator, || path("validator"))?,
                    reward: check.dec(reward, || path("reward"))?,
                })
            })?;
            StagedEvent {
                event_type: "validator_rewards",
                item_count: Some(item_count),
                items: Items::ValidatorRewards(rewards),
                ..row
            }
        }
        event_body::Event::CWithdrawal(pb::CWithdrawal {
            user,
            amount,
            is_finalized,
        }) => {
            let p = at("c_withdrawal");
            StagedEvent {
                event_type: "c_withdrawal",
                user: Some(check.bytes(user, || p("user"))?),
                amount: Some(check.dec(amount, || p("amount"))?),
                is_finalized: Some(*is_finalized),
                ..row
            }
        }
        event_body::Event::CDeposit(pb::CDeposit { user, amount }) => {
            let p = at("c_deposit");
            StagedEvent {
                event_type: "c_deposit",
                user: Some(check.bytes(user, || p("user"))?),
                amount: Some(check.dec(amount, || p("amount"))?),
                ..row
            }
        }
        event_body::Event::Delegation(pb::Delegation {
            user,
            validator,
            amount,
            is_undelegate,
        }) => {
            let p = at("delegation");
            StagedEvent {
                event_type: "delegation",
                user: Some(check.bytes(user, || p("user"))?),
                validator: Some(check.bytes(validator, || p("validator"))?),
                amount: Some(check.dec(amount, || p("amount"))?),
                is_undelegate: Some(*is_undelegate),
                ..row
            }
        }
        event_body::Event::GossipPriorityAuctionRestart(pb::GossipPriorityAuctionRestart {
            slot_id,
            previous_winner,
            end_gas,
        }) => {
            let p = at("gossip_priority_auction_restart");
            let previous_winner_ip = match previous_winner {
                Some(pb::GossipPriorityAuctionPreviousWinner { ip }) => {
                    Some(check.text(ip, || p("previous_winner.ip"))?)
                }
                None => None,
            };
            let end_gas = end_gas
                .as_deref()
                .map(|end_gas| check.dec(end_gas, || p("end_gas")))
                .transpose()?;
            StagedEvent {
                event_type: "gossip_priority_auction_restart",
                slot_id: Some(check.long(*slot_id, || p("slot_id"))?),
                previous_winner_ip,
                end_gas,
                ..row
            }
        }
        event_body::Event::CreateSubAccount(pb::CreateSubAccount {
            user,
            sub_account,
            name,
        }) => {
            let p = at("create_sub_account");
            StagedEvent {
                event_type: "create_sub_account",
                user: Some(check.bytes(user, || p("user"))?),
                sub_account: Some(check.bytes(sub_account, || p("sub_account"))?),
                // User text: `''` is a name, kept.
                sub_account_name: Some(name.as_str()),
                ..row
            }
        }
    })
}

/// The `ledger_type` and columns of one `LedgerUpdateDelta` case.
fn stage_ledger_delta<'a>(
    check: Check,
    delta_path: &str,
    delta: &'a ledger_update_delta::Delta,
    row: StagedEvent<'a>,
) -> Result<StagedEvent<'a>> {
    use ledger_update_delta::Delta;
    let at = |case: &'static str| move |field: &str| format!("{delta_path}.{case}.{field}");
    // `''` means "no fee" in the fee token of `send` and `spot_transfer`.
    let fee_token = |value: &'a str| (!value.is_empty()).then_some(value);
    Ok(match delta {
        Delta::SpotTransfer(pb::SpotTransfer {
            token,
            amount,
            usdc_value,
            user,
            destination,
            fee,
            native_token_fee,
            nonce,
            fee_token: token_of_fee,
        }) => {
            let p = at("spot_transfer");
            StagedEvent {
                ledger_type: Some("spot_transfer"),
                token: Some(check.text(token, || p("token"))?),
                amount: Some(check.dec(amount, || p("amount"))?),
                usdc_value: Some(check.dec(usdc_value, || p("usdc_value"))?),
                user: Some(check.bytes(user, || p("user"))?),
                destination: Some(check.bytes(destination, || p("destination"))?),
                fee: Some(check.dec(fee, || p("fee"))?),
                native_token_fee: Some(check.dec(native_token_fee, || p("native_token_fee"))?),
                nonce: Some(check.long(*nonce, || p("nonce"))?),
                fee_token: fee_token(token_of_fee.as_str()),
                ..row
            }
        }
        Delta::CStakingTransfer(pb::CStakingTransfer {
            token,
            amount,
            is_deposit,
        }) => {
            let p = at("c_staking_transfer");
            StagedEvent {
                ledger_type: Some("c_staking_transfer"),
                token: Some(check.text(token, || p("token"))?),
                amount: Some(check.dec(amount, || p("amount"))?),
                is_deposit: Some(*is_deposit),
                ..row
            }
        }
        Delta::AccountClassTransfer(pb::AccountClassTransfer { usdc, to_perp }) => {
            let p = at("account_class_transfer");
            StagedEvent {
                ledger_type: Some("account_class_transfer"),
                usdc: Some(check.dec(usdc, || p("usdc"))?),
                to_perp: Some(*to_perp),
                ..row
            }
        }
        Delta::InternalTransfer(pb::InternalTransfer {
            usdc,
            user,
            destination,
            fee,
        }) => {
            let p = at("internal_transfer");
            StagedEvent {
                ledger_type: Some("internal_transfer"),
                usdc: Some(check.dec(usdc, || p("usdc"))?),
                user: Some(check.bytes(user, || p("user"))?),
                destination: Some(check.bytes(destination, || p("destination"))?),
                fee: Some(check.dec(fee, || p("fee"))?),
                ..row
            }
        }
        Delta::SubAccountTransfer(pb::SubAccountTransfer {
            usdc,
            user,
            destination,
        }) => {
            let p = at("sub_account_transfer");
            StagedEvent {
                ledger_type: Some("sub_account_transfer"),
                usdc: Some(check.dec(usdc, || p("usdc"))?),
                user: Some(check.bytes(user, || p("user"))?),
                destination: Some(check.bytes(destination, || p("destination"))?),
                ..row
            }
        }
        Delta::Send(pb::Send {
            user,
            destination,
            source_dex,
            destination_dex,
            token,
            amount,
            usdc_value,
            fee,
            native_token_fee,
            nonce,
            fee_token: token_of_fee,
        }) => {
            let p = at("send");
            StagedEvent {
                ledger_type: Some("send"),
                user: Some(check.bytes(user, || p("user"))?),
                destination: Some(check.bytes(destination, || p("destination"))?),
                // `''` is HyperLiquid's name of the default USDC perp dex: kept.
                source_dex: Some(source_dex.as_str()),
                destination_dex: Some(destination_dex.as_str()),
                token: Some(check.text(token, || p("token"))?),
                amount: Some(check.dec(amount, || p("amount"))?),
                usdc_value: Some(check.dec(usdc_value, || p("usdc_value"))?),
                fee: Some(check.dec(fee, || p("fee"))?),
                native_token_fee: Some(check.dec(native_token_fee, || p("native_token_fee"))?),
                nonce: Some(check.long(*nonce, || p("nonce"))?),
                fee_token: fee_token(token_of_fee.as_str()),
                ..row
            }
        }
        Delta::Deposit(pb::Deposit { usdc }) => {
            let p = at("deposit");
            StagedEvent {
                ledger_type: Some("deposit"),
                usdc: Some(check.dec(usdc, || p("usdc"))?),
                ..row
            }
        }
        Delta::Withdraw(pb::Withdraw { usdc, nonce, fee }) => {
            let p = at("withdraw");
            StagedEvent {
                ledger_type: Some("withdraw"),
                usdc: Some(check.dec(usdc, || p("usdc"))?),
                nonce: Some(check.long(*nonce, || p("nonce"))?),
                fee: Some(check.dec(fee, || p("fee"))?),
                ..row
            }
        }
        Delta::VaultDeposit(pb::VaultDeposit { vault, usdc }) => {
            let p = at("vault_deposit");
            StagedEvent {
                ledger_type: Some("vault_deposit"),
                vault: Some(check.bytes(vault, || p("vault"))?),
                usdc: Some(check.dec(usdc, || p("usdc"))?),
                ..row
            }
        }
        Delta::RewardsClaim(pb::RewardsClaim { amount, token }) => {
            let p = at("rewards_claim");
            StagedEvent {
                ledger_type: Some("rewards_claim"),
                amount: Some(check.dec(amount, || p("amount"))?),
                token: Some(check.text(token, || p("token"))?),
                ..row
            }
        }
        Delta::VaultWithdraw(pb::VaultWithdraw {
            vault,
            user,
            requested_usd,
            commission,
            closing_cost,
            basis,
            net_withdrawn_usd,
        }) => {
            let p = at("vault_withdraw");
            StagedEvent {
                ledger_type: Some("vault_withdraw"),
                vault: Some(check.bytes(vault, || p("vault"))?),
                user: Some(check.bytes(user, || p("user"))?),
                requested_usd: Some(check.dec(requested_usd, || p("requested_usd"))?),
                commission: Some(check.dec(commission, || p("commission"))?),
                closing_cost: Some(check.dec(closing_cost, || p("closing_cost"))?),
                basis: Some(check.dec(basis, || p("basis"))?),
                net_withdrawn_usd: Some(check.dec(net_withdrawn_usd, || p("net_withdrawn_usd"))?),
                ..row
            }
        }
        Delta::VaultLeaderCommission(pb::VaultLeaderCommission { user, usdc }) => {
            let p = at("vault_leader_commission");
            StagedEvent {
                ledger_type: Some("vault_leader_commission"),
                user: Some(check.bytes(user, || p("user"))?),
                usdc: Some(check.dec(usdc, || p("usdc"))?),
                ..row
            }
        }
        Delta::DeployGasAuction(pb::DeployGasAuction { token, amount }) => {
            let p = at("deploy_gas_auction");
            StagedEvent {
                ledger_type: Some("deploy_gas_auction"),
                token: Some(check.text(token, || p("token"))?),
                amount: Some(check.dec(amount, || p("amount"))?),
                ..row
            }
        }
        Delta::AccountActivationGas(pb::AccountActivationGas { amount, token }) => {
            let p = at("account_activation_gas");
            StagedEvent {
                ledger_type: Some("account_activation_gas"),
                amount: Some(check.dec(amount, || p("amount"))?),
                token: Some(check.text(token, || p("token"))?),
                ..row
            }
        }
        Delta::ActivateDexAbstraction(pb::ActivateDexAbstraction { dex, token, amount }) => {
            let p = at("activate_dex_abstraction");
            StagedEvent {
                ledger_type: Some("activate_dex_abstraction"),
                dex: Some(check.text(dex, || p("dex"))?),
                token: Some(check.text(token, || p("token"))?),
                amount: Some(check.dec(amount, || p("amount"))?),
                ..row
            }
        }
        Delta::Liquidation(pb::Liquidation {
            liquidated_ntl_pos,
            account_value,
            leverage_type,
            liquidated_positions,
        }) => {
            let p = at("liquidation");
            let positions = stage_all(liquidated_positions, |position, liquidated| {
                let path = |field: &str| p(&format!("liquidated_positions[{position}].{field}"));
                let pb::LiquidatedPosition { coin, szi } = liquidated;
                Ok((
                    check.text(coin, || path("coin"))?,
                    check.dec(szi, || path("szi"))?,
                ))
            })?;
            StagedEvent {
                ledger_type: Some("liquidation"),
                liquidated_ntl_pos: Some(
                    check.dec(liquidated_ntl_pos, || p("liquidated_ntl_pos"))?,
                ),
                account_value: Some(check.dec(account_value, || p("account_value"))?),
                leverage_type: Some(check.leverage_type(*leverage_type, || p("leverage_type"))?),
                liquidated_positions: Some(positions),
                ..row
            }
        }
        Delta::SpotGenesis(pb::SpotGenesis { token, amount }) => {
            let p = at("spot_genesis");
            StagedEvent {
                ledger_type: Some("spot_genesis"),
                token: Some(check.text(token, || p("token"))?),
                amount: Some(check.dec(amount, || p("amount"))?),
                ..row
            }
        }
        Delta::VaultDistribution(pb::VaultDistribution { vault, usdc }) => {
            let p = at("vault_distribution");
            StagedEvent {
                ledger_type: Some("vault_distribution"),
                vault: Some(check.bytes(vault, || p("vault"))?),
                usdc: Some(check.dec(usdc, || p("usdc"))?),
                ..row
            }
        }
        Delta::BorrowLend(pb::BorrowLend {
            token,
            amount,
            interest_amount,
            operation,
        }) => {
            let p = at("borrow_lend");
            StagedEvent {
                ledger_type: Some("borrow_lend"),
                token: Some(check.text(token, || p("token"))?),
                amount: Some(check.dec(amount, || p("amount"))?),
                interest_amount: Some(check.dec(interest_amount, || p("interest_amount"))?),
                operation: Some(check.text(operation, || p("operation"))?),
                ..row
            }
        }
        Delta::VaultCreate(pb::VaultCreate { vault, usdc, fee }) => {
            let p = at("vault_create");
            StagedEvent {
                ledger_type: Some("vault_create"),
                vault: Some(check.bytes(vault, || p("vault"))?),
                usdc: Some(check.dec(usdc, || p("usdc"))?),
                fee: Some(check.dec(fee, || p("fee"))?),
                ..row
            }
        }
        Delta::GossipPriorityGasAuction(pb::GossipPriorityGasAuction { token, amount }) => {
            let p = at("gossip_priority_gas_auction");
            StagedEvent {
                ledger_type: Some("gossip_priority_gas_auction"),
                token: Some(check.text(token, || p("token"))?),
                amount: Some(check.dec(amount, || p("amount"))?),
                ..row
            }
        }
        Delta::Hip3LiquidatorDeposit(pb::Hip3LiquidatorDeposit { dex, token, amount }) => {
            let p = at("hip3_liquidator_deposit");
            StagedEvent {
                ledger_type: Some("hip3_liquidator_deposit"),
                dex: Some(check.text(dex, || p("dex"))?),
                token: Some(check.text(token, || p("token"))?),
                amount: Some(check.dec(amount, || p("amount"))?),
                ..row
            }
        }
    })
}

// ===========================================================================
// Derivations (`docs/chains/hypercore.md`, rules R-D1 to R-D6)
// ===========================================================================
//
// `derive` runs between `stage` and `append`. It reads one staged block only
// and carries no state across blocks (D2), never refuses (D3), and computes
// facts only: row selection, copies, joins on exact keys within the block,
// exact checked sums and counts, and pure parsing of `coin` (D4).

/// Largest magnitude of a `decimal(38,10)` value, in units of 1e-10.
const DECIMAL_MAX: u128 = 10u128.pow(38) - 1;

/// R-D1: the market class of a coin, a pure function of its form.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Market<'a> {
    /// A perp of the default dex (`dex` `""`) or of a HIP-3 dex.
    Perp {
        dex: &'a str,
    },
    Spot,
    /// HIP-4 coin `#<n>`: `n div 10` and `n mod 10`.
    Outcome {
        outcome_id: i64,
        side_index: i64,
    },
}

impl<'a> Market<'a> {
    /// `fills.market_type`.
    pub(crate) fn label(self) -> &'static str {
        match self {
            Market::Perp { .. } => "perp",
            Market::Spot => "spot",
            Market::Outcome { .. } => "outcome",
        }
    }

    /// `fills.dex`: perps only.
    pub(crate) fn dex(self) -> Option<&'a str> {
        match self {
            Market::Perp { dex } => Some(dex),
            Market::Spot | Market::Outcome { .. } => None,
        }
    }
}

/// `[A-Za-z0-9]+`.
fn alphanumeric(text: &str) -> bool {
    !text.is_empty() && text.bytes().all(|byte| byte.is_ascii_alphanumeric())
}

/// `[0-9]+`.
fn digits(text: &str) -> bool {
    !text.is_empty() && text.bytes().all(|byte| byte.is_ascii_digit())
}

/// R-D1. Anchored patterns, tried in order: `#[0-9]+` outcome; `@[0-9]+` or
/// `[A-Za-z0-9]+/[A-Za-z0-9]+` spot; `[a-z][a-z0-9]*:[A-Za-z0-9]+` HIP-3 perp
/// (the dex is the text before `:`); `[A-Za-z0-9]+` default-dex perp. Any other
/// form, and an outcome number above `u64::MAX`, is `None`: never a guess and
/// never a refusal, because a new form can arrive without a proto change (D3).
pub(crate) fn market(coin: &str) -> Option<Market<'_>> {
    if let Some(number) = coin.strip_prefix('#') {
        let n: u64 = digits(number).then(|| number.parse().ok())??;
        return Some(Market::Outcome {
            outcome_id: (n / 10) as i64,
            side_index: (n % 10) as i64,
        });
    }
    if let Some(number) = coin.strip_prefix('@') {
        return digits(number).then_some(Market::Spot);
    }
    if let Some((base, quote)) = coin.split_once('/') {
        return (alphanumeric(base) && alphanumeric(quote)).then_some(Market::Spot);
    }
    if let Some((dex, symbol)) = coin.split_once(':') {
        let mut name = dex.bytes();
        let dex_name = name.next().is_some_and(|first| first.is_ascii_lowercase())
            && name.all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit());
        return (dex_name && alphanumeric(symbol)).then_some(Market::Perp { dex });
    }
    alphanumeric(coin).then_some(Market::Perp { dex: "" })
}

/// R-D2: for each fill, the position of the other leg of its match. Fills
/// with a non-zero trade id are grouped by `(coin, transaction_id)` within the
/// block; a group of exactly two fills on different sides pairs them, and any
/// other group (one leg, three or more, or one side) pairs none. HyperLiquid's
/// trade id is a hash of the two order ids, so the key is the match itself.
fn pair_fills(fills: &[StagedFill<'_>]) -> Vec<Option<u32>> {
    struct Group {
        first: u32,
        second: u32,
        legs: u32,
    }
    let mut groups: HashMap<(&str, u64), Group> = HashMap::with_capacity(fills.len());
    for (index, fill) in fills.iter().enumerate() {
        if fill.transaction_id == 0 {
            continue;
        }
        let index = index as u32;
        groups
            .entry((fill.coin, fill.transaction_id))
            .and_modify(|group| {
                if group.legs == 1 {
                    group.second = index;
                }
                group.legs = group.legs.saturating_add(1);
            })
            .or_insert(Group {
                first: index,
                second: index,
                legs: 1,
            });
    }
    let mut pairs = vec![None; fills.len()];
    for group in groups.values() {
        let (first, second) = (group.first as usize, group.second as usize);
        if group.legs == 2 && fills[first].side != fills[second].side {
            pairs[first] = Some(group.second);
            pairs[second] = Some(group.first);
        }
    }
    pairs
}

/// R-D6: the event table of an event's labels. Routing only grows (D8): a
/// label already written stays in its table for the life of a root, and a
/// label first vendored by a release may be routed by that release (R2 refused
/// every earlier block that carried it). Everything else is `other_events`.
pub(crate) fn route_event(event_type: &str, ledger_type: Option<&str>) -> EventTable {
    match (event_type, ledger_type) {
        (
            "ledger_update",
            Some(
                "send"
                | "spot_transfer"
                | "internal_transfer"
                | "sub_account_transfer"
                | "account_class_transfer",
            ),
        ) => EventTable::Transfers,
        ("ledger_update", Some("deposit" | "withdraw")) => EventTable::BridgeTransfers,
        (
            "ledger_update",
            Some(
                "vault_create"
                | "vault_deposit"
                | "vault_withdraw"
                | "vault_distribution"
                | "vault_leader_commission",
            ),
        ) => EventTable::VaultEvents,
        ("ledger_update", Some("c_staking_transfer"))
        | ("c_deposit" | "c_withdrawal" | "delegation", None) => EventTable::StakingEvents,
        _ => EventTable::OtherEvents,
    }
}

/// One `funding_rates` row (R-D5).
struct FundingRate<'a> {
    event_index: u32,
    dex_index: u32,
    coin: &'a str,
    /// R-D1 of `coin`.
    dex: Option<&'a str>,
    /// `None` once two deltas of the coin differ.
    funding_rate: Option<i128>,
    positions: u32,
    long_positions: u32,
    short_positions: u32,
    open_interest: Option<i128>,
    long_size: Option<i128>,
    short_size: Option<i128>,
    positive_funding: Option<i128>,
    negative_funding: Option<i128>,
}

/// An exact sum: `None` once it leaves `decimal(38,10)`.
fn add(sum: Option<i128>, value: i128) -> Option<i128> {
    sum?.checked_add(value)
        .filter(|total| total.unsigned_abs() <= DECIMAL_MAX)
}

impl<'a> FundingRate<'a> {
    fn new(event_index: u32, dex_index: u32, first: &StagedFundingDelta<'a>) -> Self {
        Self {
            event_index,
            dex_index,
            coin: first.coin,
            dex: market(first.coin).and_then(Market::dex),
            funding_rate: Some(first.funding_rate),
            positions: 0,
            long_positions: 0,
            short_positions: 0,
            open_interest: Some(0),
            long_size: Some(0),
            short_size: Some(0),
            positive_funding: Some(0),
            negative_funding: Some(0),
        }
    }

    fn add(&mut self, delta: &StagedFundingDelta<'_>) {
        if self.funding_rate != Some(delta.funding_rate) {
            self.funding_rate = None;
        }
        self.positions = self.positions.saturating_add(1);
        self.open_interest = add(self.open_interest, delta.szi.abs());
        if delta.szi > 0 {
            self.long_positions = self.long_positions.saturating_add(1);
            self.long_size = add(self.long_size, delta.szi);
        } else if delta.szi < 0 {
            self.short_positions = self.short_positions.saturating_add(1);
            self.short_size = add(self.short_size, -delta.szi);
        }
        if delta.funding_amount > 0 {
            self.positive_funding = add(self.positive_funding, delta.funding_amount);
        } else if delta.funding_amount < 0 {
            self.negative_funding = add(self.negative_funding, delta.funding_amount);
        }
    }
}

/// R-D5: per funding event in block order, one row per coin in the order the
/// coin first appears. `dex_index` is the event's ordinal among the block's
/// funding events, empty ones included; an empty event has no row.
fn funding_rates<'a>(events: &[StagedEvent<'a>]) -> Vec<FundingRate<'a>> {
    let mut rows = Vec::new();
    let mut dex_index = 0u32;
    for (event_index, event) in events.iter().enumerate() {
        let Items::FundingDeltas(deltas) = &event.items else {
            continue;
        };
        let mut by_coin: HashMap<&str, usize> = HashMap::new();
        for delta in deltas {
            let row = *by_coin.entry(delta.coin).or_insert_with(|| {
                rows.push(FundingRate::new(event_index as u32, dex_index, delta));
                rows.len() - 1
            });
            rows[row].add(delta);
        }
        dex_index += 1;
    }
    rows
}

/// What `derive` adds to a staged block.
struct Derived<'a> {
    /// R-D1, per fill.
    markets: Vec<Option<Market<'a>>>,
    /// R-D2, per fill: the position of its paired fill.
    pairs: Vec<Option<u32>>,
    /// R-D6, per event.
    routes: Vec<EventTable>,
    /// R-D5.
    funding_rates: Vec<FundingRate<'a>>,
}

/// The derivations of one staged block. Infallible: every value was checked
/// while staging, and a shape a rule does not recognise gives NULL (D3).
fn derive<'a>(block: &StagedBlock<'a>) -> Derived<'a> {
    Derived {
        markets: block.fills.iter().map(|fill| market(fill.coin)).collect(),
        pairs: pair_fills(&block.fills),
        routes: block
            .events
            .iter()
            .map(|event| route_event(event.event_type, event.ledger_type))
            .collect(),
        funding_rates: funding_rates(&block.events),
    }
}

// ===========================================================================
// HypercoreBlockMapper
// ===========================================================================

pub struct HypercoreBlockMapper {
    blocks: BlocksBuilder,
    fills: FillsBuilder,
    outcome_fills: OutcomeFillsBuilder,
    liquidations: LiquidationsBuilder,
    /// In [`EventTable::ALL`] order.
    events: Vec<EventTableBuilder>,
    funding_deltas: FundingDeltasBuilder,
    funding_rates: FundingRatesBuilder,
    validator_rewards: ValidatorRewardsBuilder,
}

impl HypercoreBlockMapper {
    pub fn new(include_fork_step: bool, encoding: EncodeBytes) -> Self {
        Self {
            blocks: BlocksBuilder::new(include_fork_step, &encoding),
            fills: FillsBuilder::new(include_fork_step, &encoding),
            outcome_fills: OutcomeFillsBuilder::new(include_fork_step, &encoding),
            liquidations: LiquidationsBuilder::new(include_fork_step, &encoding),
            events: EventTable::ALL
                .into_iter()
                .map(|table| EventTableBuilder::new(table, include_fork_step, &encoding))
                .collect(),
            funding_deltas: FundingDeltasBuilder::new(include_fork_step, &encoding),
            funding_rates: FundingRatesBuilder::new(include_fork_step, &encoding),
            validator_rewards: ValidatorRewardsBuilder::new(include_fork_step, &encoding),
        }
    }

    /// Steps 2–6 of the mapping: the unknown-field guard, the identity, the
    /// staging of every value, the derivations, then the appends.
    fn map_decoded(
        &mut self,
        block: pb::Block,
        payload_len: usize,
        identity: &BlockIdentity,
        fork_step: StreamEvent<'_>,
    ) -> Result<u64> {
        let check = Check {
            block_num: identity.block_num,
        };
        // R2, before any semantic check: prost drops unknown fields and decodes
        // an unknown oneof case as `None`, so a payload that re-encodes shorter
        // carries data the vendored protos do not describe.
        let encoded_len = block.encoded_len();
        if encoded_len != payload_len {
            return Err(anyhow!(
                "hypercore block {}: payload is {payload_len} bytes but re-encodes to \
                 {encoded_len}: it carries fields or oneof cases unknown to the vendored \
                 pinax.hypercore.v1 protos; refresh them from buf.build/pinax/hypercore",
                identity.block_num
            ));
        }
        let block_time_ns = check_header(check, &block, identity)?;
        // The Firehose block id is the decimal block number: the ids are built
        // from the identity's numbers, never from the metadata strings.
        let prepared = self
            .blocks
            .canonical
            .prepare_with_text_ids(
                identity,
                &identity.block_num.to_string(),
                &identity.parent_num.to_string(),
            )
            .with_context(|| format!("hypercore block {}: identity", identity.block_num))?;
        let staged = stage(check, &block, block_time_ns)?;
        let derived = derive(&staged);
        let fills = u64::from(staged.fill_count);
        self.append(&staged, &derived, &prepared, fork_step);
        Ok(fills)
    }

    /// Append one staged block and its derivations, in payload order.
    /// Infallible: every value was checked while staging.
    fn append(
        &mut self,
        block: &StagedBlock<'_>,
        derived: &Derived<'_>,
        identity: &PreparedIdentity,
        fork_step: StreamEvent<'_>,
    ) {
        self.blocks.append(block, identity, fork_step);
        for (index, fill) in block.fills.iter().enumerate() {
            let fill_index = index as u32;
            let market = derived.markets[index];
            let pair = derived.pairs[index].map(|other| (other, &block.fills[other as usize]));
            let counterparty = pair.map(|(_, other)| other.user);
            self.fills
                .append(fill_index, fill, market, counterparty, identity, fork_step);
            // R-D3.
            if let Some(Market::Outcome {
                outcome_id,
                side_index,
            }) = market
            {
                self.outcome_fills.append(
                    fill_index,
                    fill,
                    (outcome_id, side_index),
                    counterparty,
                    identity,
                    fork_step,
                );
            }
            // R-D4: the liquidated leg, by byte equality.
            if let (Some(method), Some(mark_price)) =
                (fill.liquidation_method, fill.liquidation_mark_px)
            {
                if fill.liquidated_user == Some(fill.user) {
                    self.liquidations.append(
                        fill_index,
                        fill,
                        (method, mark_price),
                        market,
                        pair,
                        identity,
                        fork_step,
                    );
                }
            }
        }
        for (index, event) in block.events.iter().enumerate() {
            let event_index = index as u32;
            self.events[derived.routes[index] as usize].append(
                event_index,
                event,
                identity,
                fork_step,
            );
            match &event.items {
                Items::None => {}
                Items::FundingDeltas(deltas) => {
                    for (position, delta) in deltas.iter().enumerate() {
                        self.funding_deltas.append(
                            event_index,
                            position as u32,
                            delta,
                            identity,
                            fork_step,
                        );
                    }
                }
                Items::ValidatorRewards(rewards) => {
                    for (position, reward) in rewards.iter().enumerate() {
                        self.validator_rewards.append(
                            event_index,
                            position as u32,
                            reward,
                            identity,
                            fork_step,
                        );
                    }
                }
            }
        }
        for rate in &derived.funding_rates {
            self.funding_rates.append(rate, identity, fork_step);
        }
    }

    /// The row counts of every table, in [`schema::TABLE_NAMES`] order.
    fn row_counts(&self) -> [usize; 12] {
        let events = |table: EventTable| self.events[table as usize].canonical.len();
        [
            self.blocks.canonical.len(),
            self.fills.canonical.len(),
            self.outcome_fills.canonical.len(),
            self.liquidations.canonical.len(),
            events(EventTable::Transfers),
            events(EventTable::BridgeTransfers),
            events(EventTable::VaultEvents),
            events(EventTable::StakingEvents),
            events(EventTable::OtherEvents),
            self.funding_deltas.canonical.len(),
            self.funding_rates.canonical.len(),
            self.validator_rewards.canonical.len(),
        ]
    }
}

impl BlockMapper for HypercoreBlockMapper {
    fn map_block(
        &mut self,
        block_bytes: &[u8],
        identity: &BlockIdentity,
        fork_step: StreamEvent<'_>,
    ) -> Result<u64> {
        let block = decode(block_bytes, identity)?;
        self.map_decoded(block, block_bytes.len(), identity, fork_step)
    }

    fn map_block_bytes(
        &mut self,
        block_bytes: Bytes,
        identity: &BlockIdentity,
        fork_step: StreamEvent<'_>,
    ) -> Result<u64> {
        let payload_len = block_bytes.len();
        let block = decode(block_bytes, identity)?;
        self.map_decoded(block, payload_len, identity, fork_step)
    }

    fn flush(&mut self) -> Result<HashMap<String, RecordBatch>> {
        let mut batches = HashMap::with_capacity(schema::TABLE_NAMES.len());
        batches.insert("blocks".to_string(), self.blocks.finish()?);
        batches.insert("fills".to_string(), self.fills.finish()?);
        batches.insert("outcome_fills".to_string(), self.outcome_fills.finish()?);
        batches.insert("liquidations".to_string(), self.liquidations.finish()?);
        for builder in &mut self.events {
            batches.insert(builder.table.name().to_string(), builder.finish()?);
        }
        batches.insert("funding_deltas".to_string(), self.funding_deltas.finish()?);
        batches.insert("funding_rates".to_string(), self.funding_rates.finish()?);
        batches.insert(
            "validator_rewards".to_string(),
            self.validator_rewards.finish()?,
        );
        Ok(batches)
    }

    fn max_table_rows(&self) -> usize {
        self.row_counts().into_iter().max().unwrap_or(0)
    }

    fn total_rows(&self) -> usize {
        self.row_counts().into_iter().sum()
    }

    fn table_estimates(&mut self) -> Vec<(&str, usize)> {
        let mut estimates = vec![
            ("blocks", self.blocks.estimated_bytes()),
            ("fills", self.fills.estimated_bytes()),
            ("outcome_fills", self.outcome_fills.estimated_bytes()),
            ("liquidations", self.liquidations.estimated_bytes()),
        ];
        for builder in &mut self.events {
            estimates.push((builder.table.name(), builder.estimated_bytes()));
        }
        estimates.extend([
            ("funding_deltas", self.funding_deltas.estimated_bytes()),
            ("funding_rates", self.funding_rates.estimated_bytes()),
            (
                "validator_rewards",
                self.validator_rewards.estimated_bytes(),
            ),
        ]);
        estimates
    }

    fn table_names(&self) -> Vec<&str> {
        schema::TABLE_NAMES.to_vec()
    }
}

/// R1.
fn decode(payload: impl prost::bytes::Buf, identity: &BlockIdentity) -> Result<pb::Block> {
    pb::Block::decode(payload).map_err(|error| {
        anyhow!(
            "hypercore block {}: cannot decode pinax.hypercore.v1.Block: {error}",
            identity.block_num
        )
    })
}

// ===========================================================================
// Builders
// ===========================================================================

fn decimal_builder() -> Decimal128Builder {
    Decimal128Builder::new().with_data_type(schema::decimal_type())
}

fn timestamp_builder() -> TimestampMillisecondBuilder {
    TimestampMillisecondBuilder::new().with_timezone("UTC")
}

fn append_bytes(column: &mut BytesColumn, value: Option<&[u8]>) {
    match value {
        Some(value) => column.append_value(value),
        None => column.append_null(),
    }
}

fn finish(
    canonical: &mut CanonicalBuilder,
    columns: Vec<ArrayRef>,
    fork_step: &mut Option<ForkStepBuilder>,
    schema: &SchemaRef,
) -> Result<RecordBatch> {
    let mut all = canonical.finish();
    all.extend(columns);
    finish_fork_step(fork_step, &mut all);
    Ok(RecordBatch::try_new(schema.clone(), all)?)
}

struct BlocksBuilder {
    schema: SchemaRef,
    canonical: CanonicalBuilder,
    block_time_ns: Int64Builder,
    fill_count: UInt32Builder,
    event_count: UInt32Builder,
    extra_json: StringBuilder,
    fork_step: Option<ForkStepBuilder>,
}

impl BlocksBuilder {
    fn new(include_fork_step: bool, encoding: &EncodeBytes) -> Self {
        Self {
            schema: Arc::new(schema::blocks_schema(include_fork_step, encoding)),
            canonical: CanonicalBuilder::with_encoding(encoding),
            block_time_ns: Int64Builder::new(),
            fill_count: UInt32Builder::new(),
            event_count: UInt32Builder::new(),
            extra_json: StringBuilder::new(),
            fork_step: fork_step_builder(include_fork_step),
        }
    }

    fn append(
        &mut self,
        block: &StagedBlock<'_>,
        identity: &PreparedIdentity,
        fork_step: StreamEvent<'_>,
    ) {
        self.canonical.append(identity);
        self.block_time_ns.append_value(block.block_time_ns);
        self.fill_count.append_value(block.fill_count);
        self.event_count.append_value(block.event_count);
        // This version writes no extension values.
        self.extra_json.append_null();
        append_fork_step(&mut self.fork_step, fork_step);
    }

    fn estimated_bytes(&self) -> usize {
        self.canonical.estimated_bytes()
            + est_i64(&self.block_time_ns)
            + est_u32(&self.fill_count)
            + est_u32(&self.event_count)
            + est_str(&self.extra_json)
            + est_fork_step(&self.fork_step)
    }

    fn finish(&mut self) -> Result<RecordBatch> {
        let columns: Vec<ArrayRef> = vec![
            Arc::new(self.block_time_ns.finish()),
            Arc::new(self.fill_count.finish()),
            Arc::new(self.event_count.finish()),
            Arc::new(self.extra_json.finish()),
        ];
        finish(
            &mut self.canonical,
            columns,
            &mut self.fork_step,
            &self.schema,
        )
    }
}

struct FillsBuilder {
    schema: SchemaRef,
    canonical: CanonicalBuilder,
    fill_index: UInt32Builder,
    user: BytesColumn,
    coin: StringBuilder,
    price: Decimal128Builder,
    size: Decimal128Builder,
    side: StringDictionaryBuilder<Int32Type>,
    fill_time: TimestampMillisecondBuilder,
    start_position: Decimal128Builder,
    direction: StringDictionaryBuilder<Int32Type>,
    closed_pnl: Decimal128Builder,
    hash: BytesColumn,
    order_id: UInt64Builder,
    crossed: BooleanBuilder,
    fee: Decimal128Builder,
    transaction_id: UInt64Builder,
    fee_token: StringBuilder,
    twap_id: UInt64Builder,
    client_order_id: BytesColumn,
    liquidated_user: BytesColumn,
    liquidation_mark_px: Decimal128Builder,
    liquidation_method: StringBuilder,
    deployer_fee: Decimal128Builder,
    builder: StringBuilder,
    builder_fee: Decimal128Builder,
    priority_gas: Decimal128Builder,
    market_type: StringDictionaryBuilder<Int32Type>,
    dex: StringBuilder,
    counterparty: BytesColumn,
    extra_json: StringBuilder,
    fork_step: Option<ForkStepBuilder>,
}

impl FillsBuilder {
    fn new(include_fork_step: bool, encoding: &EncodeBytes) -> Self {
        Self {
            schema: Arc::new(schema::fills_schema(include_fork_step, encoding)),
            canonical: CanonicalBuilder::with_encoding(encoding),
            fill_index: UInt32Builder::new(),
            user: BytesColumn::new(encoding),
            coin: StringBuilder::new(),
            price: decimal_builder(),
            size: decimal_builder(),
            side: StringDictionaryBuilder::new(),
            fill_time: timestamp_builder(),
            start_position: decimal_builder(),
            direction: StringDictionaryBuilder::new(),
            closed_pnl: decimal_builder(),
            hash: BytesColumn::new(encoding),
            order_id: UInt64Builder::new(),
            crossed: BooleanBuilder::new(),
            fee: decimal_builder(),
            transaction_id: UInt64Builder::new(),
            fee_token: StringBuilder::new(),
            twap_id: UInt64Builder::new(),
            client_order_id: BytesColumn::new(encoding),
            liquidated_user: BytesColumn::new(encoding),
            liquidation_mark_px: decimal_builder(),
            liquidation_method: StringBuilder::new(),
            deployer_fee: decimal_builder(),
            builder: StringBuilder::new(),
            builder_fee: decimal_builder(),
            priority_gas: decimal_builder(),
            market_type: StringDictionaryBuilder::new(),
            dex: StringBuilder::new(),
            counterparty: BytesColumn::new(encoding),
            extra_json: StringBuilder::new(),
            fork_step: fork_step_builder(include_fork_step),
        }
    }

    fn append(
        &mut self,
        fill_index: u32,
        fill: &StagedFill<'_>,
        market: Option<Market<'_>>,
        counterparty: Option<&[u8]>,
        identity: &PreparedIdentity,
        fork_step: StreamEvent<'_>,
    ) {
        self.canonical.append(identity);
        self.fill_index.append_value(fill_index);
        self.user.append_value(fill.user);
        self.coin.append_value(fill.coin);
        self.price.append_value(fill.price);
        self.size.append_value(fill.size);
        self.side.append_value(fill.side);
        self.fill_time.append_value(fill.fill_time);
        self.start_position.append_value(fill.start_position);
        self.direction.append_value(fill.direction);
        self.closed_pnl.append_value(fill.closed_pnl);
        self.hash.append_value(fill.hash);
        self.order_id.append_value(fill.order_id);
        self.crossed.append_value(fill.crossed);
        self.fee.append_value(fill.fee);
        self.transaction_id.append_value(fill.transaction_id);
        self.fee_token.append_value(fill.fee_token);
        self.twap_id.append_option(fill.twap_id);
        append_bytes(&mut self.client_order_id, fill.client_order_id);
        append_bytes(&mut self.liquidated_user, fill.liquidated_user);
        self.liquidation_mark_px
            .append_option(fill.liquidation_mark_px);
        self.liquidation_method
            .append_option(fill.liquidation_method);
        self.deployer_fee.append_option(fill.deployer_fee);
        self.builder.append_option(fill.builder);
        self.builder_fee.append_option(fill.builder_fee);
        self.priority_gas.append_option(fill.priority_gas);
        self.market_type.append_option(market.map(Market::label));
        self.dex.append_option(market.and_then(Market::dex));
        append_bytes(&mut self.counterparty, counterparty);
        self.extra_json.append_option(fill.extra_json);
        append_fork_step(&mut self.fork_step, fork_step);
    }

    fn estimated_bytes(&self) -> usize {
        let rows = self.canonical.len();
        self.canonical.estimated_bytes()
            + est_u32(&self.fill_index)
            + self.user.estimated_bytes()
            + est_str(&self.coin)
            + est_decimal128(&self.price)
            + est_decimal128(&self.size)
            + estimated_dictionary_index_bytes(rows)
            + est_ts_ms(&self.fill_time)
            + est_decimal128(&self.start_position)
            + estimated_dictionary_index_bytes(rows)
            + est_decimal128(&self.closed_pnl)
            + self.hash.estimated_bytes()
            + est_u64(&self.order_id)
            + est_bool(&self.crossed)
            + est_decimal128(&self.fee)
            + est_u64(&self.transaction_id)
            + est_str(&self.fee_token)
            + est_u64(&self.twap_id)
            + self.client_order_id.estimated_bytes()
            + self.liquidated_user.estimated_bytes()
            + est_decimal128(&self.liquidation_mark_px)
            + est_str(&self.liquidation_method)
            + est_decimal128(&self.deployer_fee)
            + est_str(&self.builder)
            + est_decimal128(&self.builder_fee)
            + est_decimal128(&self.priority_gas)
            + estimated_dictionary_index_bytes(rows)
            + est_str(&self.dex)
            + self.counterparty.estimated_bytes()
            + est_str(&self.extra_json)
            + est_fork_step(&self.fork_step)
    }

    fn finish(&mut self) -> Result<RecordBatch> {
        let columns: Vec<ArrayRef> = vec![
            Arc::new(self.fill_index.finish()),
            self.user.finish(),
            Arc::new(self.coin.finish()),
            Arc::new(self.price.finish()),
            Arc::new(self.size.finish()),
            Arc::new(self.side.finish()),
            Arc::new(self.fill_time.finish()),
            Arc::new(self.start_position.finish()),
            Arc::new(self.direction.finish()),
            Arc::new(self.closed_pnl.finish()),
            self.hash.finish(),
            Arc::new(self.order_id.finish()),
            Arc::new(self.crossed.finish()),
            Arc::new(self.fee.finish()),
            Arc::new(self.transaction_id.finish()),
            Arc::new(self.fee_token.finish()),
            Arc::new(self.twap_id.finish()),
            self.client_order_id.finish(),
            self.liquidated_user.finish(),
            Arc::new(self.liquidation_mark_px.finish()),
            Arc::new(self.liquidation_method.finish()),
            Arc::new(self.deployer_fee.finish()),
            Arc::new(self.builder.finish()),
            Arc::new(self.builder_fee.finish()),
            Arc::new(self.priority_gas.finish()),
            Arc::new(self.market_type.finish()),
            Arc::new(self.dex.finish()),
            self.counterparty.finish(),
            Arc::new(self.extra_json.finish()),
        ];
        finish(
            &mut self.canonical,
            columns,
            &mut self.fork_step,
            &self.schema,
        )
    }
}

/// `outcome_fills` (R-D3): a copy of an outcome fill with its parsed coin.
struct OutcomeFillsBuilder {
    schema: SchemaRef,
    canonical: CanonicalBuilder,
    fill_index: UInt32Builder,
    user: BytesColumn,
    coin: StringBuilder,
    outcome_id: Int64Builder,
    side_index: Int64Builder,
    price: Decimal128Builder,
    size: Decimal128Builder,
    side: StringDictionaryBuilder<Int32Type>,
    direction: StringDictionaryBuilder<Int32Type>,
    start_position: Decimal128Builder,
    closed_pnl: Decimal128Builder,
    hash: BytesColumn,
    order_id: UInt64Builder,
    crossed: BooleanBuilder,
    fee: Decimal128Builder,
    fee_token: StringBuilder,
    transaction_id: UInt64Builder,
    twap_id: UInt64Builder,
    client_order_id: BytesColumn,
    deployer_fee: Decimal128Builder,
    builder: StringBuilder,
    builder_fee: Decimal128Builder,
    priority_gas: Decimal128Builder,
    counterparty: BytesColumn,
    extra_json: StringBuilder,
    fork_step: Option<ForkStepBuilder>,
}

impl OutcomeFillsBuilder {
    fn new(include_fork_step: bool, encoding: &EncodeBytes) -> Self {
        Self {
            schema: Arc::new(schema::outcome_fills_schema(include_fork_step, encoding)),
            canonical: CanonicalBuilder::with_encoding(encoding),
            fill_index: UInt32Builder::new(),
            user: BytesColumn::new(encoding),
            coin: StringBuilder::new(),
            outcome_id: Int64Builder::new(),
            side_index: Int64Builder::new(),
            price: decimal_builder(),
            size: decimal_builder(),
            side: StringDictionaryBuilder::new(),
            direction: StringDictionaryBuilder::new(),
            start_position: decimal_builder(),
            closed_pnl: decimal_builder(),
            hash: BytesColumn::new(encoding),
            order_id: UInt64Builder::new(),
            crossed: BooleanBuilder::new(),
            fee: decimal_builder(),
            fee_token: StringBuilder::new(),
            transaction_id: UInt64Builder::new(),
            twap_id: UInt64Builder::new(),
            client_order_id: BytesColumn::new(encoding),
            deployer_fee: decimal_builder(),
            builder: StringBuilder::new(),
            builder_fee: decimal_builder(),
            priority_gas: decimal_builder(),
            counterparty: BytesColumn::new(encoding),
            extra_json: StringBuilder::new(),
            fork_step: fork_step_builder(include_fork_step),
        }
    }

    fn append(
        &mut self,
        fill_index: u32,
        fill: &StagedFill<'_>,
        (outcome_id, side_index): (i64, i64),
        counterparty: Option<&[u8]>,
        identity: &PreparedIdentity,
        fork_step: StreamEvent<'_>,
    ) {
        self.canonical.append(identity);
        self.fill_index.append_value(fill_index);
        self.user.append_value(fill.user);
        self.coin.append_value(fill.coin);
        self.outcome_id.append_value(outcome_id);
        self.side_index.append_value(side_index);
        self.price.append_value(fill.price);
        self.size.append_value(fill.size);
        self.side.append_value(fill.side);
        self.direction.append_value(fill.direction);
        self.start_position.append_value(fill.start_position);
        self.closed_pnl.append_value(fill.closed_pnl);
        self.hash.append_value(fill.hash);
        self.order_id.append_value(fill.order_id);
        self.crossed.append_value(fill.crossed);
        self.fee.append_value(fill.fee);
        self.fee_token.append_value(fill.fee_token);
        self.transaction_id.append_value(fill.transaction_id);
        self.twap_id.append_option(fill.twap_id);
        append_bytes(&mut self.client_order_id, fill.client_order_id);
        self.deployer_fee.append_option(fill.deployer_fee);
        self.builder.append_option(fill.builder);
        self.builder_fee.append_option(fill.builder_fee);
        self.priority_gas.append_option(fill.priority_gas);
        append_bytes(&mut self.counterparty, counterparty);
        // D7: the fill's own `extra_json`, verbatim.
        self.extra_json.append_option(fill.extra_json);
        append_fork_step(&mut self.fork_step, fork_step);
    }

    fn estimated_bytes(&self) -> usize {
        let rows = self.canonical.len();
        self.canonical.estimated_bytes()
            + est_u32(&self.fill_index)
            + self.user.estimated_bytes()
            + est_str(&self.coin)
            + est_i64(&self.outcome_id)
            + est_i64(&self.side_index)
            + est_decimal128(&self.price)
            + est_decimal128(&self.size)
            + 2 * estimated_dictionary_index_bytes(rows)
            + est_decimal128(&self.start_position)
            + est_decimal128(&self.closed_pnl)
            + self.hash.estimated_bytes()
            + est_u64(&self.order_id)
            + est_bool(&self.crossed)
            + est_decimal128(&self.fee)
            + est_str(&self.fee_token)
            + est_u64(&self.transaction_id)
            + est_u64(&self.twap_id)
            + self.client_order_id.estimated_bytes()
            + est_decimal128(&self.deployer_fee)
            + est_str(&self.builder)
            + est_decimal128(&self.builder_fee)
            + est_decimal128(&self.priority_gas)
            + self.counterparty.estimated_bytes()
            + est_str(&self.extra_json)
            + est_fork_step(&self.fork_step)
    }

    fn finish(&mut self) -> Result<RecordBatch> {
        let columns: Vec<ArrayRef> = vec![
            Arc::new(self.fill_index.finish()),
            self.user.finish(),
            Arc::new(self.coin.finish()),
            Arc::new(self.outcome_id.finish()),
            Arc::new(self.side_index.finish()),
            Arc::new(self.price.finish()),
            Arc::new(self.size.finish()),
            Arc::new(self.side.finish()),
            Arc::new(self.direction.finish()),
            Arc::new(self.start_position.finish()),
            Arc::new(self.closed_pnl.finish()),
            self.hash.finish(),
            Arc::new(self.order_id.finish()),
            Arc::new(self.crossed.finish()),
            Arc::new(self.fee.finish()),
            Arc::new(self.fee_token.finish()),
            Arc::new(self.transaction_id.finish()),
            Arc::new(self.twap_id.finish()),
            self.client_order_id.finish(),
            Arc::new(self.deployer_fee.finish()),
            Arc::new(self.builder.finish()),
            Arc::new(self.builder_fee.finish()),
            Arc::new(self.priority_gas.finish()),
            self.counterparty.finish(),
            Arc::new(self.extra_json.finish()),
        ];
        finish(
            &mut self.canonical,
            columns,
            &mut self.fork_step,
            &self.schema,
        )
    }
}

/// `liquidations` (R-D4): a copy of a liquidated leg with its paired leg.
struct LiquidationsBuilder {
    schema: SchemaRef,
    canonical: CanonicalBuilder,
    fill_index: UInt32Builder,
    liquidated_user: BytesColumn,
    coin: StringBuilder,
    market_type: StringDictionaryBuilder<Int32Type>,
    dex: StringBuilder,
    side: StringDictionaryBuilder<Int32Type>,
    direction: StringDictionaryBuilder<Int32Type>,
    price: Decimal128Builder,
    size: Decimal128Builder,
    start_position: Decimal128Builder,
    closed_pnl: Decimal128Builder,
    fee: Decimal128Builder,
    fee_token: StringBuilder,
    crossed: BooleanBuilder,
    liquidation_method: StringBuilder,
    mark_price: Decimal128Builder,
    order_id: UInt64Builder,
    transaction_id: UInt64Builder,
    hash: BytesColumn,
    counterparty: BytesColumn,
    counterparty_direction: StringDictionaryBuilder<Int32Type>,
    counterparty_fill_index: UInt32Builder,
    extra_json: StringBuilder,
    fork_step: Option<ForkStepBuilder>,
}

impl LiquidationsBuilder {
    fn new(include_fork_step: bool, encoding: &EncodeBytes) -> Self {
        Self {
            schema: Arc::new(schema::liquidations_schema(include_fork_step, encoding)),
            canonical: CanonicalBuilder::with_encoding(encoding),
            fill_index: UInt32Builder::new(),
            liquidated_user: BytesColumn::new(encoding),
            coin: StringBuilder::new(),
            market_type: StringDictionaryBuilder::new(),
            dex: StringBuilder::new(),
            side: StringDictionaryBuilder::new(),
            direction: StringDictionaryBuilder::new(),
            price: decimal_builder(),
            size: decimal_builder(),
            start_position: decimal_builder(),
            closed_pnl: decimal_builder(),
            fee: decimal_builder(),
            fee_token: StringBuilder::new(),
            crossed: BooleanBuilder::new(),
            liquidation_method: StringBuilder::new(),
            mark_price: decimal_builder(),
            order_id: UInt64Builder::new(),
            transaction_id: UInt64Builder::new(),
            hash: BytesColumn::new(encoding),
            counterparty: BytesColumn::new(encoding),
            counterparty_direction: StringDictionaryBuilder::new(),
            counterparty_fill_index: UInt32Builder::new(),
            extra_json: StringBuilder::new(),
            fork_step: fork_step_builder(include_fork_step),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn append(
        &mut self,
        fill_index: u32,
        fill: &StagedFill<'_>,
        (method, mark_price): (&str, i128),
        market: Option<Market<'_>>,
        pair: Option<(u32, &StagedFill<'_>)>,
        identity: &PreparedIdentity,
        fork_step: StreamEvent<'_>,
    ) {
        self.canonical.append(identity);
        self.fill_index.append_value(fill_index);
        self.liquidated_user.append_value(fill.user);
        self.coin.append_value(fill.coin);
        self.market_type.append_option(market.map(Market::label));
        self.dex.append_option(market.and_then(Market::dex));
        self.side.append_value(fill.side);
        self.direction.append_value(fill.direction);
        self.price.append_value(fill.price);
        self.size.append_value(fill.size);
        self.start_position.append_value(fill.start_position);
        self.closed_pnl.append_value(fill.closed_pnl);
        self.fee.append_value(fill.fee);
        self.fee_token.append_value(fill.fee_token);
        self.crossed.append_value(fill.crossed);
        self.liquidation_method.append_value(method);
        self.mark_price.append_value(mark_price);
        self.order_id.append_value(fill.order_id);
        self.transaction_id.append_value(fill.transaction_id);
        self.hash.append_value(fill.hash);
        append_bytes(&mut self.counterparty, pair.map(|(_, other)| other.user));
        self.counterparty_direction
            .append_option(pair.map(|(_, other)| other.direction));
        self.counterparty_fill_index
            .append_option(pair.map(|(index, _)| index));
        // D7: the fill's own `extra_json`, verbatim.
        self.extra_json.append_option(fill.extra_json);
        append_fork_step(&mut self.fork_step, fork_step);
    }

    fn estimated_bytes(&self) -> usize {
        let rows = self.canonical.len();
        self.canonical.estimated_bytes()
            + est_u32(&self.fill_index)
            + self.liquidated_user.estimated_bytes()
            + est_str(&self.coin)
            + 4 * estimated_dictionary_index_bytes(rows)
            + est_str(&self.dex)
            + est_decimal128(&self.price)
            + est_decimal128(&self.size)
            + est_decimal128(&self.start_position)
            + est_decimal128(&self.closed_pnl)
            + est_decimal128(&self.fee)
            + est_str(&self.fee_token)
            + est_bool(&self.crossed)
            + est_str(&self.liquidation_method)
            + est_decimal128(&self.mark_price)
            + est_u64(&self.order_id)
            + est_u64(&self.transaction_id)
            + self.hash.estimated_bytes()
            + self.counterparty.estimated_bytes()
            + est_u32(&self.counterparty_fill_index)
            + est_str(&self.extra_json)
            + est_fork_step(&self.fork_step)
    }

    fn finish(&mut self) -> Result<RecordBatch> {
        let columns: Vec<ArrayRef> = vec![
            Arc::new(self.fill_index.finish()),
            self.liquidated_user.finish(),
            Arc::new(self.coin.finish()),
            Arc::new(self.market_type.finish()),
            Arc::new(self.dex.finish()),
            Arc::new(self.side.finish()),
            Arc::new(self.direction.finish()),
            Arc::new(self.price.finish()),
            Arc::new(self.size.finish()),
            Arc::new(self.start_position.finish()),
            Arc::new(self.closed_pnl.finish()),
            Arc::new(self.fee.finish()),
            Arc::new(self.fee_token.finish()),
            Arc::new(self.crossed.finish()),
            Arc::new(self.liquidation_method.finish()),
            Arc::new(self.mark_price.finish()),
            Arc::new(self.order_id.finish()),
            Arc::new(self.transaction_id.finish()),
            self.hash.finish(),
            self.counterparty.finish(),
            Arc::new(self.counterparty_direction.finish()),
            Arc::new(self.counterparty_fill_index.finish()),
            Arc::new(self.extra_json.finish()),
        ];
        finish(
            &mut self.canonical,
            columns,
            &mut self.fork_step,
            &self.schema,
        )
    }
}

/// `column` when the event table has it.
fn column_of<T>(table: EventTable, column: &str, make: impl FnOnce() -> T) -> Option<T> {
    table.has_column(column).then(make)
}

/// Appends `value` to a column the event table has. Every value of a routed
/// type has a column in its table (`schema::EventTable::own_columns`, checked
/// against the documented column matrix by the value tests), so the `None`
/// arm only ever sees NULL.
fn put<B, V>(builder: &mut Option<B>, value: Option<V>, append: impl FnOnce(&mut B, Option<V>)) {
    match builder {
        Some(builder) => append(builder, value),
        None => debug_assert!(value.is_none(), "an event value without a column"),
    }
}

/// One event table: the shared columns, and the catalogue columns
/// (`schema::event_fields`) the table has.
struct EventTableBuilder {
    table: EventTable,
    schema: SchemaRef,
    canonical: CanonicalBuilder,
    event_index: UInt32Builder,
    event_type: StringDictionaryBuilder<Int32Type>,
    ledger_type: StringDictionaryBuilder<Int32Type>,
    hash: BytesColumn,
    event_time_ns: Int64Builder,
    users: BytesListColumn,
    user: Option<BytesColumn>,
    destination: Option<BytesColumn>,
    vault: Option<BytesColumn>,
    validator: Option<BytesColumn>,
    sub_account: Option<BytesColumn>,
    token: Option<StringBuilder>,
    amount: Option<Decimal128Builder>,
    usdc: Option<Decimal128Builder>,
    usdc_value: Option<Decimal128Builder>,
    fee: Option<Decimal128Builder>,
    fee_token: Option<StringBuilder>,
    native_token_fee: Option<Decimal128Builder>,
    nonce: Option<UInt64Builder>,
    source_dex: Option<StringBuilder>,
    destination_dex: Option<StringBuilder>,
    dex: Option<StringBuilder>,
    is_deposit: Option<BooleanBuilder>,
    to_perp: Option<BooleanBuilder>,
    is_undelegate: Option<BooleanBuilder>,
    is_finalized: Option<BooleanBuilder>,
    requested_usd: Option<Decimal128Builder>,
    commission: Option<Decimal128Builder>,
    closing_cost: Option<Decimal128Builder>,
    basis: Option<Decimal128Builder>,
    net_withdrawn_usd: Option<Decimal128Builder>,
    interest_amount: Option<Decimal128Builder>,
    operation: Option<StringBuilder>,
    liquidated_ntl_pos: Option<Decimal128Builder>,
    account_value: Option<Decimal128Builder>,
    leverage_type: Option<StringDictionaryBuilder<Int32Type>>,
    liquidated_positions: Option<ListBuilder<StructBuilder>>,
    slot_id: Option<UInt64Builder>,
    previous_winner_ip: Option<StringBuilder>,
    end_gas: Option<Decimal128Builder>,
    sub_account_name: Option<StringBuilder>,
    item_count: Option<UInt32Builder>,
    extra_json: StringBuilder,
    fork_step: Option<ForkStepBuilder>,
}

impl EventTableBuilder {
    fn new(table: EventTable, include_fork_step: bool, encoding: &EncodeBytes) -> Self {
        let bytes = || BytesColumn::new(encoding);
        Self {
            table,
            schema: Arc::new(schema::event_table_schema(
                table,
                include_fork_step,
                encoding,
            )),
            canonical: CanonicalBuilder::with_encoding(encoding),
            event_index: UInt32Builder::new(),
            event_type: StringDictionaryBuilder::new(),
            ledger_type: StringDictionaryBuilder::new(),
            hash: BytesColumn::new(encoding),
            event_time_ns: Int64Builder::new(),
            users: BytesListColumn::new(encoding),
            user: column_of(table, "user", bytes),
            destination: column_of(table, "destination", bytes),
            vault: column_of(table, "vault", bytes),
            validator: column_of(table, "validator", bytes),
            sub_account: column_of(table, "sub_account", bytes),
            token: column_of(table, "token", StringBuilder::new),
            amount: column_of(table, "amount", decimal_builder),
            usdc: column_of(table, "usdc", decimal_builder),
            usdc_value: column_of(table, "usdc_value", decimal_builder),
            fee: column_of(table, "fee", decimal_builder),
            fee_token: column_of(table, "fee_token", StringBuilder::new),
            native_token_fee: column_of(table, "native_token_fee", decimal_builder),
            nonce: column_of(table, "nonce", UInt64Builder::new),
            source_dex: column_of(table, "source_dex", StringBuilder::new),
            destination_dex: column_of(table, "destination_dex", StringBuilder::new),
            dex: column_of(table, "dex", StringBuilder::new),
            is_deposit: column_of(table, "is_deposit", BooleanBuilder::new),
            to_perp: column_of(table, "to_perp", BooleanBuilder::new),
            is_undelegate: column_of(table, "is_undelegate", BooleanBuilder::new),
            is_finalized: column_of(table, "is_finalized", BooleanBuilder::new),
            requested_usd: column_of(table, "requested_usd", decimal_builder),
            commission: column_of(table, "commission", decimal_builder),
            closing_cost: column_of(table, "closing_cost", decimal_builder),
            basis: column_of(table, "basis", decimal_builder),
            net_withdrawn_usd: column_of(table, "net_withdrawn_usd", decimal_builder),
            interest_amount: column_of(table, "interest_amount", decimal_builder),
            operation: column_of(table, "operation", StringBuilder::new),
            liquidated_ntl_pos: column_of(table, "liquidated_ntl_pos", decimal_builder),
            account_value: column_of(table, "account_value", decimal_builder),
            leverage_type: column_of(table, "leverage_type", StringDictionaryBuilder::new),
            liquidated_positions: column_of(table, "liquidated_positions", || {
                ListBuilder::new(StructBuilder::from_fields(schema::position_fields(), 0))
                    .with_field(schema::position_item())
            }),
            slot_id: column_of(table, "slot_id", UInt64Builder::new),
            previous_winner_ip: column_of(table, "previous_winner_ip", StringBuilder::new),
            end_gas: column_of(table, "end_gas", decimal_builder),
            sub_account_name: column_of(table, "sub_account_name", StringBuilder::new),
            item_count: column_of(table, "item_count", UInt32Builder::new),
            extra_json: StringBuilder::new(),
            fork_step: fork_step_builder(include_fork_step),
        }
    }

    fn append(
        &mut self,
        event_index: u32,
        event: &StagedEvent<'_>,
        identity: &PreparedIdentity,
        fork_step: StreamEvent<'_>,
    ) {
        let decimal =
            |builder: &mut Decimal128Builder, value: Option<i128>| builder.append_option(value);
        let text = |builder: &mut StringBuilder, value: Option<&str>| builder.append_option(value);
        let flag = |builder: &mut BooleanBuilder, value: Option<bool>| builder.append_option(value);
        self.canonical.append(identity);
        self.event_index.append_value(event_index);
        self.event_type.append_value(event.event_type);
        self.ledger_type.append_option(event.ledger_type);
        self.hash.append_value(event.hash);
        self.event_time_ns.append_value(event.event_time_ns);
        match event.users {
            Some(users) => {
                for user in users {
                    self.users.append_value(user);
                }
                self.users.append(true);
            }
            None => self.users.append(false),
        }
        put(&mut self.user, event.user, append_bytes);
        put(&mut self.destination, event.destination, append_bytes);
        put(&mut self.vault, event.vault, append_bytes);
        put(&mut self.validator, event.validator, append_bytes);
        put(&mut self.sub_account, event.sub_account, append_bytes);
        put(&mut self.token, event.token, text);
        put(&mut self.amount, event.amount, decimal);
        put(&mut self.usdc, event.usdc, decimal);
        put(&mut self.usdc_value, event.usdc_value, decimal);
        put(&mut self.fee, event.fee, decimal);
        put(&mut self.fee_token, event.fee_token, text);
        put(&mut self.native_token_fee, event.native_token_fee, decimal);
        put(&mut self.nonce, event.nonce, |builder, value| {
            builder.append_option(value)
        });
        put(&mut self.source_dex, event.source_dex, text);
        put(&mut self.destination_dex, event.destination_dex, text);
        put(&mut self.dex, event.dex, text);
        put(&mut self.is_deposit, event.is_deposit, flag);
        put(&mut self.to_perp, event.to_perp, flag);
        put(&mut self.is_undelegate, event.is_undelegate, flag);
        put(&mut self.is_finalized, event.is_finalized, flag);
        put(&mut self.requested_usd, event.requested_usd, decimal);
        put(&mut self.commission, event.commission, decimal);
        put(&mut self.closing_cost, event.closing_cost, decimal);
        put(&mut self.basis, event.basis, decimal);
        put(
            &mut self.net_withdrawn_usd,
            event.net_withdrawn_usd,
            decimal,
        );
        put(&mut self.interest_amount, event.interest_amount, decimal);
        put(&mut self.operation, event.operation, text);
        put(
            &mut self.liquidated_ntl_pos,
            event.liquidated_ntl_pos,
            decimal,
        );
        put(&mut self.account_value, event.account_value, decimal);
        put(
            &mut self.leverage_type,
            event.leverage_type,
            |builder, value| builder.append_option(value),
        );
        put(
            &mut self.liquidated_positions,
            event.liquidated_positions.as_ref(),
            |builder, positions| match positions {
                Some(positions) => {
                    let items = builder.values();
                    for (coin, szi) in positions {
                        items
                            .field_builder::<StringBuilder>(0)
                            .expect("liquidated_positions.coin is Utf8")
                            .append_value(coin);
                        items
                            .field_builder::<Decimal128Builder>(1)
                            .expect("liquidated_positions.szi is Decimal128")
                            .append_value(*szi);
                        items.append(true);
                    }
                    builder.append(true);
                }
                None => builder.append(false),
            },
        );
        put(&mut self.slot_id, event.slot_id, |builder, value| {
            builder.append_option(value)
        });
        put(&mut self.previous_winner_ip, event.previous_winner_ip, text);
        put(&mut self.end_gas, event.end_gas, decimal);
        put(&mut self.sub_account_name, event.sub_account_name, text);
        put(&mut self.item_count, event.item_count, |builder, value| {
            builder.append_option(value)
        });
        // This version writes no extension values.
        self.extra_json.append_null();
        append_fork_step(&mut self.fork_step, fork_step);
    }

    fn estimated_bytes(&mut self) -> usize {
        fn bytes(column: &Option<BytesColumn>) -> usize {
            column.as_ref().map_or(0, BytesColumn::estimated_bytes)
        }
        fn text(column: &Option<StringBuilder>) -> usize {
            column.as_ref().map_or(0, est_str)
        }
        fn decimal(column: &Option<Decimal128Builder>) -> usize {
            column.as_ref().map_or(0, est_decimal128)
        }
        fn flag(column: &Option<BooleanBuilder>) -> usize {
            column.as_ref().map_or(0, est_bool)
        }
        let rows = self.canonical.len();
        let positions = self.liquidated_positions.as_mut().map_or(0, |positions| {
            let lists = positions.len();
            let items = positions.values();
            let coins = est_str(
                items
                    .field_builder::<StringBuilder>(0)
                    .expect("liquidated_positions.coin is Utf8"),
            );
            let sizes = est_decimal128(
                items
                    .field_builder::<Decimal128Builder>(1)
                    .expect("liquidated_positions.szi is Decimal128"),
            );
            (lists + 1) * 4 + coins + sizes
        });
        self.canonical.estimated_bytes()
            + est_u32(&self.event_index)
            + 2 * estimated_dictionary_index_bytes(rows)
            + self.hash.estimated_bytes()
            + est_i64(&self.event_time_ns)
            + self.users.estimated_bytes()
            + bytes(&self.user)
            + bytes(&self.destination)
            + bytes(&self.vault)
            + bytes(&self.validator)
            + bytes(&self.sub_account)
            + text(&self.token)
            + decimal(&self.amount)
            + decimal(&self.usdc)
            + decimal(&self.usdc_value)
            + decimal(&self.fee)
            + text(&self.fee_token)
            + decimal(&self.native_token_fee)
            + self.nonce.as_ref().map_or(0, est_u64)
            + text(&self.source_dex)
            + text(&self.destination_dex)
            + text(&self.dex)
            + flag(&self.is_deposit)
            + flag(&self.to_perp)
            + flag(&self.is_undelegate)
            + flag(&self.is_finalized)
            + decimal(&self.requested_usd)
            + decimal(&self.commission)
            + decimal(&self.closing_cost)
            + decimal(&self.basis)
            + decimal(&self.net_withdrawn_usd)
            + decimal(&self.interest_amount)
            + text(&self.operation)
            + decimal(&self.liquidated_ntl_pos)
            + decimal(&self.account_value)
            + self
                .leverage_type
                .as_ref()
                .map_or(0, |_| estimated_dictionary_index_bytes(rows))
            + positions
            + self.slot_id.as_ref().map_or(0, est_u64)
            + text(&self.previous_winner_ip)
            + decimal(&self.end_gas)
            + text(&self.sub_account_name)
            + self.item_count.as_ref().map_or(0, est_u32)
            + est_str(&self.extra_json)
            + est_fork_step(&self.fork_step)
    }

    /// The table's columns in catalogue order (`schema::event_fields`).
    fn finish(&mut self) -> Result<RecordBatch> {
        fn array(builder: &mut impl ArrayBuilder) -> ArrayRef {
            builder.finish()
        }
        let mut columns: Vec<ArrayRef> = vec![
            Arc::new(self.event_index.finish()),
            Arc::new(self.event_type.finish()),
            Arc::new(self.ledger_type.finish()),
            self.hash.finish(),
            Arc::new(self.event_time_ns.finish()),
            self.users.finish(),
        ];
        let bytes = |column: &mut Option<BytesColumn>| column.as_mut().map(BytesColumn::finish);
        let bytes_columns = [
            bytes(&mut self.user),
            bytes(&mut self.destination),
            bytes(&mut self.vault),
            bytes(&mut self.validator),
            bytes(&mut self.sub_account),
        ];
        columns.extend(bytes_columns.into_iter().flatten());
        let others: [Option<ArrayRef>; 32] = [
            self.token.as_mut().map(array),
            self.amount.as_mut().map(array),
            self.usdc.as_mut().map(array),
            self.usdc_value.as_mut().map(array),
            self.fee.as_mut().map(array),
            self.fee_token.as_mut().map(array),
            self.native_token_fee.as_mut().map(array),
            self.nonce.as_mut().map(array),
            self.source_dex.as_mut().map(array),
            self.destination_dex.as_mut().map(array),
            self.dex.as_mut().map(array),
            self.is_deposit.as_mut().map(array),
            self.to_perp.as_mut().map(array),
            self.is_undelegate.as_mut().map(array),
            self.is_finalized.as_mut().map(array),
            self.requested_usd.as_mut().map(array),
            self.commission.as_mut().map(array),
            self.closing_cost.as_mut().map(array),
            self.basis.as_mut().map(array),
            self.net_withdrawn_usd.as_mut().map(array),
            self.interest_amount.as_mut().map(array),
            self.operation.as_mut().map(array),
            self.liquidated_ntl_pos.as_mut().map(array),
            self.account_value.as_mut().map(array),
            self.leverage_type.as_mut().map(array),
            self.liquidated_positions.as_mut().map(array),
            self.slot_id.as_mut().map(array),
            self.previous_winner_ip.as_mut().map(array),
            self.end_gas.as_mut().map(array),
            self.sub_account_name.as_mut().map(array),
            self.item_count.as_mut().map(array),
            Some(Arc::new(self.extra_json.finish())),
        ];
        columns.extend(others.into_iter().flatten());
        finish(
            &mut self.canonical,
            columns,
            &mut self.fork_step,
            &self.schema,
        )
    }
}

struct FundingDeltasBuilder {
    schema: SchemaRef,
    canonical: CanonicalBuilder,
    event_index: UInt32Builder,
    delta_index: UInt32Builder,
    user: BytesColumn,
    coin: StringBuilder,
    funding_amount: Decimal128Builder,
    szi: Decimal128Builder,
    funding_rate: Decimal128Builder,
    extra_json: StringBuilder,
    fork_step: Option<ForkStepBuilder>,
}

impl FundingDeltasBuilder {
    fn new(include_fork_step: bool, encoding: &EncodeBytes) -> Self {
        Self {
            schema: Arc::new(schema::funding_deltas_schema(include_fork_step, encoding)),
            canonical: CanonicalBuilder::with_encoding(encoding),
            event_index: UInt32Builder::new(),
            delta_index: UInt32Builder::new(),
            user: BytesColumn::new(encoding),
            coin: StringBuilder::new(),
            funding_amount: decimal_builder(),
            szi: decimal_builder(),
            funding_rate: decimal_builder(),
            extra_json: StringBuilder::new(),
            fork_step: fork_step_builder(include_fork_step),
        }
    }

    fn append(
        &mut self,
        event_index: u32,
        delta_index: u32,
        delta: &StagedFundingDelta<'_>,
        identity: &PreparedIdentity,
        fork_step: StreamEvent<'_>,
    ) {
        self.canonical.append(identity);
        self.event_index.append_value(event_index);
        self.delta_index.append_value(delta_index);
        self.user.append_value(delta.user);
        self.coin.append_value(delta.coin);
        self.funding_amount.append_value(delta.funding_amount);
        self.szi.append_value(delta.szi);
        self.funding_rate.append_value(delta.funding_rate);
        self.extra_json.append_null();
        append_fork_step(&mut self.fork_step, fork_step);
    }

    fn estimated_bytes(&self) -> usize {
        self.canonical.estimated_bytes()
            + est_u32(&self.event_index)
            + est_u32(&self.delta_index)
            + self.user.estimated_bytes()
            + est_str(&self.coin)
            + est_decimal128(&self.funding_amount)
            + est_decimal128(&self.szi)
            + est_decimal128(&self.funding_rate)
            + est_str(&self.extra_json)
            + est_fork_step(&self.fork_step)
    }

    fn finish(&mut self) -> Result<RecordBatch> {
        let columns: Vec<ArrayRef> = vec![
            Arc::new(self.event_index.finish()),
            Arc::new(self.delta_index.finish()),
            self.user.finish(),
            Arc::new(self.coin.finish()),
            Arc::new(self.funding_amount.finish()),
            Arc::new(self.szi.finish()),
            Arc::new(self.funding_rate.finish()),
            Arc::new(self.extra_json.finish()),
        ];
        finish(
            &mut self.canonical,
            columns,
            &mut self.fork_step,
            &self.schema,
        )
    }
}

/// `funding_rates` (R-D5).
struct FundingRatesBuilder {
    schema: SchemaRef,
    canonical: CanonicalBuilder,
    event_index: UInt32Builder,
    dex_index: UInt32Builder,
    coin: StringBuilder,
    dex: StringBuilder,
    funding_rate: Decimal128Builder,
    positions: UInt32Builder,
    long_positions: UInt32Builder,
    short_positions: UInt32Builder,
    open_interest: Decimal128Builder,
    long_size: Decimal128Builder,
    short_size: Decimal128Builder,
    positive_funding: Decimal128Builder,
    negative_funding: Decimal128Builder,
    extra_json: StringBuilder,
    fork_step: Option<ForkStepBuilder>,
}

impl FundingRatesBuilder {
    fn new(include_fork_step: bool, encoding: &EncodeBytes) -> Self {
        Self {
            schema: Arc::new(schema::funding_rates_schema(include_fork_step, encoding)),
            canonical: CanonicalBuilder::with_encoding(encoding),
            event_index: UInt32Builder::new(),
            dex_index: UInt32Builder::new(),
            coin: StringBuilder::new(),
            dex: StringBuilder::new(),
            funding_rate: decimal_builder(),
            positions: UInt32Builder::new(),
            long_positions: UInt32Builder::new(),
            short_positions: UInt32Builder::new(),
            open_interest: decimal_builder(),
            long_size: decimal_builder(),
            short_size: decimal_builder(),
            positive_funding: decimal_builder(),
            negative_funding: decimal_builder(),
            extra_json: StringBuilder::new(),
            fork_step: fork_step_builder(include_fork_step),
        }
    }

    fn append(
        &mut self,
        rate: &FundingRate<'_>,
        identity: &PreparedIdentity,
        fork_step: StreamEvent<'_>,
    ) {
        self.canonical.append(identity);
        self.event_index.append_value(rate.event_index);
        self.dex_index.append_value(rate.dex_index);
        self.coin.append_value(rate.coin);
        self.dex.append_option(rate.dex);
        self.funding_rate.append_option(rate.funding_rate);
        self.positions.append_value(rate.positions);
        self.long_positions.append_value(rate.long_positions);
        self.short_positions.append_value(rate.short_positions);
        self.open_interest.append_option(rate.open_interest);
        self.long_size.append_option(rate.long_size);
        self.short_size.append_option(rate.short_size);
        self.positive_funding.append_option(rate.positive_funding);
        self.negative_funding.append_option(rate.negative_funding);
        // Reserved: a new `Funding` or `FundingDelta` field goes to the raw
        // tables' `extra_json`.
        self.extra_json.append_null();
        append_fork_step(&mut self.fork_step, fork_step);
    }

    fn estimated_bytes(&self) -> usize {
        self.canonical.estimated_bytes()
            + est_u32(&self.event_index)
            + est_u32(&self.dex_index)
            + est_str(&self.coin)
            + est_str(&self.dex)
            + est_decimal128(&self.funding_rate)
            + est_u32(&self.positions)
            + est_u32(&self.long_positions)
            + est_u32(&self.short_positions)
            + est_decimal128(&self.open_interest)
            + est_decimal128(&self.long_size)
            + est_decimal128(&self.short_size)
            + est_decimal128(&self.positive_funding)
            + est_decimal128(&self.negative_funding)
            + est_str(&self.extra_json)
            + est_fork_step(&self.fork_step)
    }

    fn finish(&mut self) -> Result<RecordBatch> {
        let columns: Vec<ArrayRef> = vec![
            Arc::new(self.event_index.finish()),
            Arc::new(self.dex_index.finish()),
            Arc::new(self.coin.finish()),
            Arc::new(self.dex.finish()),
            Arc::new(self.funding_rate.finish()),
            Arc::new(self.positions.finish()),
            Arc::new(self.long_positions.finish()),
            Arc::new(self.short_positions.finish()),
            Arc::new(self.open_interest.finish()),
            Arc::new(self.long_size.finish()),
            Arc::new(self.short_size.finish()),
            Arc::new(self.positive_funding.finish()),
            Arc::new(self.negative_funding.finish()),
            Arc::new(self.extra_json.finish()),
        ];
        finish(
            &mut self.canonical,
            columns,
            &mut self.fork_step,
            &self.schema,
        )
    }
}

struct ValidatorRewardsBuilder {
    schema: SchemaRef,
    canonical: CanonicalBuilder,
    event_index: UInt32Builder,
    reward_index: UInt32Builder,
    validator: BytesColumn,
    reward: Decimal128Builder,
    extra_json: StringBuilder,
    fork_step: Option<ForkStepBuilder>,
}

impl ValidatorRewardsBuilder {
    fn new(include_fork_step: bool, encoding: &EncodeBytes) -> Self {
        Self {
            schema: Arc::new(schema::validator_rewards_schema(
                include_fork_step,
                encoding,
            )),
            canonical: CanonicalBuilder::with_encoding(encoding),
            event_index: UInt32Builder::new(),
            reward_index: UInt32Builder::new(),
            validator: BytesColumn::new(encoding),
            reward: decimal_builder(),
            extra_json: StringBuilder::new(),
            fork_step: fork_step_builder(include_fork_step),
        }
    }

    fn append(
        &mut self,
        event_index: u32,
        reward_index: u32,
        reward: &StagedValidatorReward<'_>,
        identity: &PreparedIdentity,
        fork_step: StreamEvent<'_>,
    ) {
        self.canonical.append(identity);
        self.event_index.append_value(event_index);
        self.reward_index.append_value(reward_index);
        self.validator.append_value(reward.validator);
        self.reward.append_value(reward.reward);
        self.extra_json.append_null();
        append_fork_step(&mut self.fork_step, fork_step);
    }

    fn estimated_bytes(&self) -> usize {
        self.canonical.estimated_bytes()
            + est_u32(&self.event_index)
            + est_u32(&self.reward_index)
            + self.validator.estimated_bytes()
            + est_decimal128(&self.reward)
            + est_str(&self.extra_json)
            + est_fork_step(&self.fork_step)
    }

    fn finish(&mut self) -> Result<RecordBatch> {
        let columns: Vec<ArrayRef> = vec![
            Arc::new(self.event_index.finish()),
            Arc::new(self.reward_index.finish()),
            self.validator.finish(),
            Arc::new(self.reward.finish()),
            Arc::new(self.extra_json.finish()),
        ];
        finish(
            &mut self.canonical,
            columns,
            &mut self.fork_step,
            &self.schema,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// R4: a list longer than `u32::MAX` cannot be counted or indexed.
    #[test]
    fn counts_above_u32_are_refused() {
        let check = Check { block_num: 7 };
        assert_eq!(check.count(3, || "fills".to_string()).unwrap(), 3);
        assert_eq!(
            check
                .count(u32::MAX as usize, || "fills".to_string())
                .unwrap(),
            u32::MAX
        );
        let too_many = u32::MAX as usize + 1;
        assert_eq!(
            check
                .count(too_many, || "events[2].events[0].funding.deltas"
                    .to_string())
                .unwrap_err()
                .to_string(),
            format!(
                "hypercore block 7: events[2].events[0].funding.deltas: {too_many} items \
                 exceed u32"
            )
        );
    }

    #[test]
    fn offending_values_are_cut_to_80_characters() {
        assert_eq!(shown(&"1e-5"), "\"1e-5\"");
        let long = "9".repeat(100);
        let cut = shown(&long);
        assert_eq!(cut.chars().count(), 81);
        assert!(cut.starts_with("\"999"));
        assert!(cut.ends_with('…'));
    }
}

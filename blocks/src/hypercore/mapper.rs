//! HyperCore (`pinax.hypercore.v1.Block`) mapper, schema epoch 1.
//!
//! One block maps onto five tables (`schema.rs`): one `blocks` row, one
//! `fills` row per fill, one `events` row per event (its single body, and for
//! ledger updates its delta, flattened into the row), and the items of funding
//! and validator-reward events in `funding_deltas` and `validator_rewards`.
//!
//! Every block is decoded, guarded against unknown fields, checked against its
//! Firehose identity, then validated and converted completely into staged
//! values before any builder is touched: a refused block appends nothing to
//! any table (`docs/chains/hypercore.md`, "Refusals"). Every refusal names the
//! block, the proto path and the offending value.
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
use arrow::datatypes::{Int32Type, Schema};
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
use super::schema;
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
}

/// One `events` row. A column the row's type does not have stays `None`
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
// HypercoreBlockMapper
// ===========================================================================

pub struct HypercoreBlockMapper {
    blocks: BlocksBuilder,
    fills: FillsBuilder,
    events: EventsBuilder,
    funding_deltas: FundingDeltasBuilder,
    validator_rewards: ValidatorRewardsBuilder,
    blocks_schema: Schema,
    fills_schema: Schema,
    events_schema: Schema,
    funding_deltas_schema: Schema,
    validator_rewards_schema: Schema,
}

impl HypercoreBlockMapper {
    pub fn new(include_fork_step: bool, encoding: EncodeBytes) -> Self {
        Self {
            blocks: BlocksBuilder::new(include_fork_step, &encoding),
            fills: FillsBuilder::new(include_fork_step, &encoding),
            events: EventsBuilder::new(include_fork_step, &encoding),
            funding_deltas: FundingDeltasBuilder::new(include_fork_step, &encoding),
            validator_rewards: ValidatorRewardsBuilder::new(include_fork_step, &encoding),
            blocks_schema: schema::blocks_schema(include_fork_step, &encoding),
            fills_schema: schema::fills_schema(include_fork_step, &encoding),
            events_schema: schema::events_schema(include_fork_step, &encoding),
            funding_deltas_schema: schema::funding_deltas_schema(include_fork_step, &encoding),
            validator_rewards_schema: schema::validator_rewards_schema(
                include_fork_step,
                &encoding,
            ),
        }
    }

    /// Steps 2–5 of the mapping: the unknown-field guard, the identity, the
    /// staging of every value, then the appends.
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
        let fills = u64::from(staged.fill_count);
        self.append(&staged, &prepared, fork_step);
        Ok(fills)
    }

    /// Append one staged block, in payload order. Infallible: every value was
    /// checked while staging.
    fn append(
        &mut self,
        block: &StagedBlock<'_>,
        identity: &PreparedIdentity,
        fork_step: StreamEvent<'_>,
    ) {
        self.blocks.append(block, identity, fork_step);
        for (index, fill) in block.fills.iter().enumerate() {
            self.fills.append(index as u32, fill, identity, fork_step);
        }
        for (index, event) in block.events.iter().enumerate() {
            let event_index = index as u32;
            self.events.append(event_index, event, identity, fork_step);
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
        Ok(HashMap::from([
            (
                "blocks".to_string(),
                self.blocks.finish(&self.blocks_schema)?,
            ),
            ("fills".to_string(), self.fills.finish(&self.fills_schema)?),
            (
                "events".to_string(),
                self.events.finish(&self.events_schema)?,
            ),
            (
                "funding_deltas".to_string(),
                self.funding_deltas.finish(&self.funding_deltas_schema)?,
            ),
            (
                "validator_rewards".to_string(),
                self.validator_rewards
                    .finish(&self.validator_rewards_schema)?,
            ),
        ]))
    }

    fn max_table_rows(&self) -> usize {
        self.blocks
            .canonical
            .len()
            .max(self.fills.canonical.len())
            .max(self.events.canonical.len())
            .max(self.funding_deltas.canonical.len())
            .max(self.validator_rewards.canonical.len())
    }

    fn total_rows(&self) -> usize {
        self.blocks.canonical.len()
            + self.fills.canonical.len()
            + self.events.canonical.len()
            + self.funding_deltas.canonical.len()
            + self.validator_rewards.canonical.len()
    }

    fn table_estimates(&mut self) -> Vec<(&str, usize)> {
        vec![
            ("blocks", self.blocks.estimated_bytes()),
            ("fills", self.fills.estimated_bytes()),
            ("events", self.events.estimated_bytes()),
            ("funding_deltas", self.funding_deltas.estimated_bytes()),
            (
                "validator_rewards",
                self.validator_rewards.estimated_bytes(),
            ),
        ]
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
    schema: &Schema,
) -> Result<RecordBatch> {
    let mut all = canonical.finish();
    all.extend(columns);
    finish_fork_step(fork_step, &mut all);
    Ok(RecordBatch::try_new(Arc::new(schema.clone()), all)?)
}

struct BlocksBuilder {
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
        // Schema epoch 1 writes no extension values.
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

    fn finish(&mut self, schema: &Schema) -> Result<RecordBatch> {
        let columns: Vec<ArrayRef> = vec![
            Arc::new(self.block_time_ns.finish()),
            Arc::new(self.fill_count.finish()),
            Arc::new(self.event_count.finish()),
            Arc::new(self.extra_json.finish()),
        ];
        finish(&mut self.canonical, columns, &mut self.fork_step, schema)
    }
}

struct FillsBuilder {
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
    extra_json: StringBuilder,
    fork_step: Option<ForkStepBuilder>,
}

impl FillsBuilder {
    fn new(include_fork_step: bool, encoding: &EncodeBytes) -> Self {
        Self {
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
            extra_json: StringBuilder::new(),
            fork_step: fork_step_builder(include_fork_step),
        }
    }

    fn append(
        &mut self,
        fill_index: u32,
        fill: &StagedFill<'_>,
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
        self.extra_json.append_null();
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
            + est_str(&self.extra_json)
            + est_fork_step(&self.fork_step)
    }

    fn finish(&mut self, schema: &Schema) -> Result<RecordBatch> {
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
            Arc::new(self.extra_json.finish()),
        ];
        finish(&mut self.canonical, columns, &mut self.fork_step, schema)
    }
}

struct EventsBuilder {
    canonical: CanonicalBuilder,
    event_index: UInt32Builder,
    event_type: StringDictionaryBuilder<Int32Type>,
    ledger_type: StringDictionaryBuilder<Int32Type>,
    hash: BytesColumn,
    event_time_ns: Int64Builder,
    users: BytesListColumn,
    user: BytesColumn,
    destination: BytesColumn,
    vault: BytesColumn,
    validator: BytesColumn,
    sub_account: BytesColumn,
    token: StringBuilder,
    amount: Decimal128Builder,
    usdc: Decimal128Builder,
    usdc_value: Decimal128Builder,
    fee: Decimal128Builder,
    fee_token: StringBuilder,
    native_token_fee: Decimal128Builder,
    nonce: UInt64Builder,
    source_dex: StringBuilder,
    destination_dex: StringBuilder,
    dex: StringBuilder,
    is_deposit: BooleanBuilder,
    to_perp: BooleanBuilder,
    is_undelegate: BooleanBuilder,
    is_finalized: BooleanBuilder,
    requested_usd: Decimal128Builder,
    commission: Decimal128Builder,
    closing_cost: Decimal128Builder,
    basis: Decimal128Builder,
    net_withdrawn_usd: Decimal128Builder,
    interest_amount: Decimal128Builder,
    operation: StringBuilder,
    liquidated_ntl_pos: Decimal128Builder,
    account_value: Decimal128Builder,
    leverage_type: StringDictionaryBuilder<Int32Type>,
    liquidated_positions: ListBuilder<StructBuilder>,
    slot_id: UInt64Builder,
    previous_winner_ip: StringBuilder,
    end_gas: Decimal128Builder,
    sub_account_name: StringBuilder,
    item_count: UInt32Builder,
    extra_json: StringBuilder,
    fork_step: Option<ForkStepBuilder>,
}

impl EventsBuilder {
    fn new(include_fork_step: bool, encoding: &EncodeBytes) -> Self {
        Self {
            canonical: CanonicalBuilder::with_encoding(encoding),
            event_index: UInt32Builder::new(),
            event_type: StringDictionaryBuilder::new(),
            ledger_type: StringDictionaryBuilder::new(),
            hash: BytesColumn::new(encoding),
            event_time_ns: Int64Builder::new(),
            users: BytesListColumn::new(encoding),
            user: BytesColumn::new(encoding),
            destination: BytesColumn::new(encoding),
            vault: BytesColumn::new(encoding),
            validator: BytesColumn::new(encoding),
            sub_account: BytesColumn::new(encoding),
            token: StringBuilder::new(),
            amount: decimal_builder(),
            usdc: decimal_builder(),
            usdc_value: decimal_builder(),
            fee: decimal_builder(),
            fee_token: StringBuilder::new(),
            native_token_fee: decimal_builder(),
            nonce: UInt64Builder::new(),
            source_dex: StringBuilder::new(),
            destination_dex: StringBuilder::new(),
            dex: StringBuilder::new(),
            is_deposit: BooleanBuilder::new(),
            to_perp: BooleanBuilder::new(),
            is_undelegate: BooleanBuilder::new(),
            is_finalized: BooleanBuilder::new(),
            requested_usd: decimal_builder(),
            commission: decimal_builder(),
            closing_cost: decimal_builder(),
            basis: decimal_builder(),
            net_withdrawn_usd: decimal_builder(),
            interest_amount: decimal_builder(),
            operation: StringBuilder::new(),
            liquidated_ntl_pos: decimal_builder(),
            account_value: decimal_builder(),
            leverage_type: StringDictionaryBuilder::new(),
            liquidated_positions: ListBuilder::new(StructBuilder::from_fields(
                schema::position_fields(),
                0,
            ))
            .with_field(schema::position_item()),
            slot_id: UInt64Builder::new(),
            previous_winner_ip: StringBuilder::new(),
            end_gas: decimal_builder(),
            sub_account_name: StringBuilder::new(),
            item_count: UInt32Builder::new(),
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
        append_bytes(&mut self.user, event.user);
        append_bytes(&mut self.destination, event.destination);
        append_bytes(&mut self.vault, event.vault);
        append_bytes(&mut self.validator, event.validator);
        append_bytes(&mut self.sub_account, event.sub_account);
        self.token.append_option(event.token);
        self.amount.append_option(event.amount);
        self.usdc.append_option(event.usdc);
        self.usdc_value.append_option(event.usdc_value);
        self.fee.append_option(event.fee);
        self.fee_token.append_option(event.fee_token);
        self.native_token_fee.append_option(event.native_token_fee);
        self.nonce.append_option(event.nonce);
        self.source_dex.append_option(event.source_dex);
        self.destination_dex.append_option(event.destination_dex);
        self.dex.append_option(event.dex);
        self.is_deposit.append_option(event.is_deposit);
        self.to_perp.append_option(event.to_perp);
        self.is_undelegate.append_option(event.is_undelegate);
        self.is_finalized.append_option(event.is_finalized);
        self.requested_usd.append_option(event.requested_usd);
        self.commission.append_option(event.commission);
        self.closing_cost.append_option(event.closing_cost);
        self.basis.append_option(event.basis);
        self.net_withdrawn_usd
            .append_option(event.net_withdrawn_usd);
        self.interest_amount.append_option(event.interest_amount);
        self.operation.append_option(event.operation);
        self.liquidated_ntl_pos
            .append_option(event.liquidated_ntl_pos);
        self.account_value.append_option(event.account_value);
        self.leverage_type.append_option(event.leverage_type);
        match &event.liquidated_positions {
            Some(positions) => {
                let items = self.liquidated_positions.values();
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
                self.liquidated_positions.append(true);
            }
            None => self.liquidated_positions.append(false),
        }
        self.slot_id.append_option(event.slot_id);
        self.previous_winner_ip
            .append_option(event.previous_winner_ip);
        self.end_gas.append_option(event.end_gas);
        self.sub_account_name.append_option(event.sub_account_name);
        self.item_count.append_option(event.item_count);
        self.extra_json.append_null();
        append_fork_step(&mut self.fork_step, fork_step);
    }

    fn estimated_bytes(&mut self) -> usize {
        let rows = self.canonical.len();
        let positions = {
            let lists = self.liquidated_positions.len();
            let items = self.liquidated_positions.values();
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
        };
        self.canonical.estimated_bytes()
            + est_u32(&self.event_index)
            + 2 * estimated_dictionary_index_bytes(rows)
            + self.hash.estimated_bytes()
            + est_i64(&self.event_time_ns)
            + self.users.estimated_bytes()
            + self.user.estimated_bytes()
            + self.destination.estimated_bytes()
            + self.vault.estimated_bytes()
            + self.validator.estimated_bytes()
            + self.sub_account.estimated_bytes()
            + est_str(&self.token)
            + est_decimal128(&self.amount)
            + est_decimal128(&self.usdc)
            + est_decimal128(&self.usdc_value)
            + est_decimal128(&self.fee)
            + est_str(&self.fee_token)
            + est_decimal128(&self.native_token_fee)
            + est_u64(&self.nonce)
            + est_str(&self.source_dex)
            + est_str(&self.destination_dex)
            + est_str(&self.dex)
            + est_bool(&self.is_deposit)
            + est_bool(&self.to_perp)
            + est_bool(&self.is_undelegate)
            + est_bool(&self.is_finalized)
            + est_decimal128(&self.requested_usd)
            + est_decimal128(&self.commission)
            + est_decimal128(&self.closing_cost)
            + est_decimal128(&self.basis)
            + est_decimal128(&self.net_withdrawn_usd)
            + est_decimal128(&self.interest_amount)
            + est_str(&self.operation)
            + est_decimal128(&self.liquidated_ntl_pos)
            + est_decimal128(&self.account_value)
            + estimated_dictionary_index_bytes(rows)
            + positions
            + est_u64(&self.slot_id)
            + est_str(&self.previous_winner_ip)
            + est_decimal128(&self.end_gas)
            + est_str(&self.sub_account_name)
            + est_u32(&self.item_count)
            + est_str(&self.extra_json)
            + est_fork_step(&self.fork_step)
    }

    fn finish(&mut self, schema: &Schema) -> Result<RecordBatch> {
        let columns: Vec<ArrayRef> = vec![
            Arc::new(self.event_index.finish()),
            Arc::new(self.event_type.finish()),
            Arc::new(self.ledger_type.finish()),
            self.hash.finish(),
            Arc::new(self.event_time_ns.finish()),
            self.users.finish(),
            self.user.finish(),
            self.destination.finish(),
            self.vault.finish(),
            self.validator.finish(),
            self.sub_account.finish(),
            Arc::new(self.token.finish()),
            Arc::new(self.amount.finish()),
            Arc::new(self.usdc.finish()),
            Arc::new(self.usdc_value.finish()),
            Arc::new(self.fee.finish()),
            Arc::new(self.fee_token.finish()),
            Arc::new(self.native_token_fee.finish()),
            Arc::new(self.nonce.finish()),
            Arc::new(self.source_dex.finish()),
            Arc::new(self.destination_dex.finish()),
            Arc::new(self.dex.finish()),
            Arc::new(self.is_deposit.finish()),
            Arc::new(self.to_perp.finish()),
            Arc::new(self.is_undelegate.finish()),
            Arc::new(self.is_finalized.finish()),
            Arc::new(self.requested_usd.finish()),
            Arc::new(self.commission.finish()),
            Arc::new(self.closing_cost.finish()),
            Arc::new(self.basis.finish()),
            Arc::new(self.net_withdrawn_usd.finish()),
            Arc::new(self.interest_amount.finish()),
            Arc::new(self.operation.finish()),
            Arc::new(self.liquidated_ntl_pos.finish()),
            Arc::new(self.account_value.finish()),
            Arc::new(self.leverage_type.finish()),
            Arc::new(self.liquidated_positions.finish()),
            Arc::new(self.slot_id.finish()),
            Arc::new(self.previous_winner_ip.finish()),
            Arc::new(self.end_gas.finish()),
            Arc::new(self.sub_account_name.finish()),
            Arc::new(self.item_count.finish()),
            Arc::new(self.extra_json.finish()),
        ];
        finish(&mut self.canonical, columns, &mut self.fork_step, schema)
    }
}

struct FundingDeltasBuilder {
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

    fn finish(&mut self, schema: &Schema) -> Result<RecordBatch> {
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
        finish(&mut self.canonical, columns, &mut self.fork_step, schema)
    }
}

struct ValidatorRewardsBuilder {
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

    fn finish(&mut self, schema: &Schema) -> Result<RecordBatch> {
        let columns: Vec<ArrayRef> = vec![
            Arc::new(self.event_index.finish()),
            Arc::new(self.reward_index.finish()),
            self.validator.finish(),
            Arc::new(self.reward.finish()),
            Arc::new(self.extra_json.finish()),
        ];
        finish(&mut self.canonical, columns, &mut self.fork_step, schema)
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

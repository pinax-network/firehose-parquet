//! Preflight of `nport_reports`, `nport_monthly_returns`, `nport_monthly_activity`, `nport_holdings`, `nport_debt_reference_instruments`, `nport_debt_conversion_currencies`, `nport_derivatives`, `nport_derivative_swap_legs`, `nport_derivative_index_components` (§3.23, §3.24, §3.25, §3.26, §3.27, §3.28, §3.29, §3.30, §3.31).
//! Owned by the `nport` group: see `/tmp/sec-fireparq/impl/contracts.md`.
//!
//! Fallible only on the structural invariants of §4.7 (positions through
//! `super::idx`); every typed value goes through `IssueSink::row`.
//!
//! Rows are prepared, and their `parse_issues` recorded, in the reference's
//! emit order: the report, its monthly returns, its monthly activity, then per
//! holding the holding, its reference instruments, its conversion currencies,
//! and per derivative level (outer first) the derivative, its swap legs and its
//! index components. Each child table keeps its rows in one flat list, in
//! table row order; every row borrows the proto message it was prepared from,
//! and the append phase copies its strings from there.

use anyhow::Result;

use super::{idx, FilingCtx};
use crate::sec::issues::IssueSink;
use crate::sec::parse::{month_end_back, Family};
use crate::sec::proto::sec;
use crate::sec::schema::{
    NPORT_DEBT_CONVERSION_CURRENCIES, NPORT_DERIVATIVES, NPORT_DERIVATIVE_INDEX_COMPONENTS,
    NPORT_DERIVATIVE_SWAP_LEGS, NPORT_HOLDINGS, NPORT_MONTHLY_ACTIVITY, NPORT_MONTHLY_RETURNS,
    NPORT_REPORTS,
};

/// The parsed and derived values of `nport_reports`, `nport_monthly_returns`, `nport_monthly_activity`, `nport_holdings`, `nport_debt_reference_instruments`, `nport_debt_conversion_currencies`, `nport_derivatives`, `nport_derivative_swap_legs`, `nport_derivative_index_components` for one filing.
#[derive(Debug, Default)]
pub(crate) struct PreparedNport<'a> {
    pub report: ReportRow,
    pub returns: Vec<ReturnRow<'a>>,
    pub activity: Vec<ActivityRow<'a>>,
    pub holdings: Vec<HoldingRow<'a>>,
    pub references: Vec<ReferenceRow<'a>>,
    pub conversions: Vec<ConversionRow<'a>>,
    pub derivatives: Vec<DerivativeRow<'a>>,
    pub legs: Vec<LegRow<'a>>,
    pub components: Vec<ComponentRow<'a>>,
}

/// `nport_reports` (§3.23). `as_of_date` is also copied onto every child row.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct ReportRow {
    pub fiscal_year_end: Option<i32>,
    pub as_of_date: Option<i32>,
    pub total_assets: Option<i128>,
    pub total_liabilities: Option<i128>,
    pub net_assets: Option<i128>,
    pub assets_invested: Option<i128>,
    pub misc_securities_assets: Option<i128>,
    pub cash_not_reported: Option<i128>,
    pub holdings_count: u32,
    pub derivative_holding_count: u32,
    pub debt_holding_count: u32,
    pub monthly_return_class_count: u32,
    pub has_parse_issues: bool,
}

/// `nport_monthly_returns` (§3.24).
#[derive(Clone, Copy, Debug)]
pub(crate) struct ReturnRow<'a> {
    pub source: &'a sec::MonthlyReturn,
    pub return_index: u32,
    pub return_month1: Option<i128>,
    pub return_month2: Option<i128>,
    pub return_month3: Option<i128>,
    pub month1_end: Option<i32>,
    pub month2_end: Option<i32>,
    pub month3_end: Option<i32>,
    pub has_parse_issues: bool,
}

/// `nport_monthly_activity` (§3.25).
#[derive(Clone, Copy, Debug)]
pub(crate) struct ActivityRow<'a> {
    pub source: &'a sec::MonthlyFundActivity,
    pub activity_index: u32,
    pub month_end: Option<i32>,
    pub sales: Option<i128>,
    pub reinvestment: Option<i128>,
    pub redemption: Option<i128>,
    pub net_realized_gain: Option<i128>,
    pub net_unrealized_appreciation: Option<i128>,
    pub has_parse_issues: bool,
}

/// `nport_holdings` (§3.26).
#[derive(Clone, Copy, Debug)]
pub(crate) struct HoldingRow<'a> {
    pub source: &'a sec::PortfolioHolding,
    pub holding_index: u32,
    pub balance: Option<i128>,
    pub exchange_rate: Option<i128>,
    pub value_usd: Option<i128>,
    pub pct_value: Option<i128>,
    pub debt_maturity_date: Option<i32>,
    pub debt_annualized_rate: Option<i128>,
    pub debt_reference_instrument_count: u32,
    pub debt_conversion_currency_count: u32,
    pub lending_loan_value: Option<i128>,
    pub lending_cash_collateral_value: Option<i128>,
    pub has_parse_issues: bool,
}

/// `nport_debt_reference_instruments` (§3.27): no typed column.
#[derive(Clone, Copy, Debug)]
pub(crate) struct ReferenceRow<'a> {
    pub source: &'a sec::DebtReferenceInstrument,
    pub holding_index: u32,
    pub reference_index: u32,
}

/// `nport_debt_conversion_currencies` (§3.28).
#[derive(Clone, Copy, Debug)]
pub(crate) struct ConversionRow<'a> {
    pub source: &'a sec::ConversionCurrency,
    pub holding_index: u32,
    pub conversion_index: u32,
    pub conversion_ratio: Option<i128>,
    pub has_parse_issues: bool,
}

/// `nport_derivatives` (§3.29): one node of `holdings[].derivative(.nested)*`.
#[derive(Clone, Copy, Debug)]
pub(crate) struct DerivativeRow<'a> {
    pub source: &'a sec::Derivative,
    pub holding_index: u32,
    pub nesting_level: u32,
    pub share_no: Option<i128>,
    pub principal_amount: Option<i128>,
    pub notional_amount: Option<i128>,
    pub exercise_price: Option<i128>,
    pub expiration_date: Option<i32>,
    pub unrealized_appreciation: Option<i128>,
    pub termination_date: Option<i32>,
    pub amount_currency_sold: Option<i128>,
    pub amount_currency_purchased: Option<i128>,
    pub settlement_date: Option<i32>,
    pub upfront_payment: Option<i128>,
    pub upfront_receipt: Option<i128>,
    pub swap_flag: Option<bool>,
    pub swap_leg_count: u32,
    pub index_component_count: u32,
    pub addl_balance: Option<i128>,
    pub addl_exchange_rate: Option<i128>,
    pub addl_value_usd: Option<i128>,
    pub addl_pct_value: Option<i128>,
    pub has_parse_issues: bool,
}

/// `nport_derivative_swap_legs` (§3.30).
#[derive(Clone, Copy, Debug)]
pub(crate) struct LegRow<'a> {
    pub source: &'a sec::SwapLeg,
    pub holding_index: u32,
    pub nesting_level: u32,
    pub leg_index: u32,
    pub amount: Option<i128>,
    pub fixed_rate: Option<i128>,
    pub floating_rate_spread: Option<i128>,
    pub payment_amount: Option<i128>,
    pub has_parse_issues: bool,
}

/// `nport_derivative_index_components` (§3.31).
#[derive(Clone, Copy, Debug)]
pub(crate) struct ComponentRow<'a> {
    pub source: &'a sec::IndexBasketComponent,
    pub holding_index: u32,
    pub nesting_level: u32,
    pub component_index: u32,
    pub notional_amount: Option<i128>,
    pub value: Option<i128>,
    pub has_parse_issues: bool,
}

/// A string member of an optional sub-message; `""` (no value, no issue) when
/// the sub-message is absent.
fn member<'a, M>(message: Option<&'a M>, field: impl FnOnce(&'a M) -> &'a String) -> &'a str {
    message.map_or("", |m| field(m).as_str())
}

pub(crate) fn prepare<'a>(
    fc: &FilingCtx<'a>,
    body: &'a sec::NportReport,
    issues: &mut IssueSink<'a>,
) -> Result<PreparedNport<'a>> {
    let _ = fc;
    let general = body.general_info.as_ref();
    let fund = body.fund_info.as_ref();
    let monthly_returns: &'a [sec::MonthlyReturn] =
        fund.map_or(&[], |f| f.monthly_total_returns.as_slice());
    let monthly_activity: &'a [sec::MonthlyFundActivity] =
        fund.map_or(&[], |f| f.monthly_activity.as_slice());

    let mut row = issues.row(NPORT_REPORTS, &[]);
    let report = ReportRow {
        fiscal_year_end: row.date("fiscal_year_end", member(general, |g| &g.rep_period_end)),
        as_of_date: row.date("as_of_date", member(general, |g| &g.rep_period_date)),
        total_assets: row.decimal(
            "total_assets",
            member(fund, |f| &f.total_assets),
            Family::N10,
        ),
        total_liabilities: row.decimal(
            "total_liabilities",
            member(fund, |f| &f.total_liabilities),
            Family::N10,
        ),
        net_assets: row.decimal("net_assets", member(fund, |f| &f.net_assets), Family::N10),
        assets_invested: row.decimal(
            "assets_invested",
            member(fund, |f| &f.assets_invested),
            Family::N10,
        ),
        misc_securities_assets: row.decimal(
            "misc_securities_assets",
            member(fund, |f| &f.misc_securities_assets),
            Family::N10,
        ),
        cash_not_reported: row.decimal(
            "cash_not_reported",
            member(fund, |f| &f.cash_not_reported),
            Family::N10,
        ),
        holdings_count: idx(body.holdings.len())?,
        derivative_holding_count: idx(body
            .holdings
            .iter()
            .filter(|h| h.derivative.is_some())
            .count())?,
        debt_holding_count: idx(body
            .holdings
            .iter()
            .filter(|h| h.debt_security.is_some())
            .count())?,
        monthly_return_class_count: idx(monthly_returns.len())?,
        has_parse_issues: row.finish(),
    };
    let as_of_date = report.as_of_date;

    let mut returns = Vec::with_capacity(monthly_returns.len());
    for (position, source) in monthly_returns.iter().enumerate() {
        let return_index = idx(position)?;
        let mut row = issues.row(NPORT_MONTHLY_RETURNS, &[return_index]);
        // §4.4: month k ends `3 - k` months before the as-of month.
        let month_end = |k: u32| as_of_date.and_then(|days| month_end_back(days, 3 - k));
        returns.push(ReturnRow {
            source,
            return_index,
            return_month1: row.decimal("return_month1", &source.return_month1, Family::R12),
            return_month2: row.decimal("return_month2", &source.return_month2, Family::R12),
            return_month3: row.decimal("return_month3", &source.return_month3, Family::R12),
            month1_end: month_end(1),
            month2_end: month_end(2),
            month3_end: month_end(3),
            has_parse_issues: row.finish(),
        });
    }

    let mut activity = Vec::with_capacity(monthly_activity.len());
    for (position, source) in monthly_activity.iter().enumerate() {
        let activity_index = idx(position)?;
        let mut row = issues.row(NPORT_MONTHLY_ACTIVITY, &[activity_index]);
        // §4.4 (C3): proto3 `0` means unset, so only months 1..=3 name a month.
        let month_end = match source.month {
            month @ 1..=3 => as_of_date.and_then(|days| month_end_back(days, 3 - month)),
            _ => None,
        };
        activity.push(ActivityRow {
            source,
            activity_index,
            month_end,
            sales: row.decimal("sales", &source.sales, Family::N10),
            reinvestment: row.decimal("reinvestment", &source.reinvestment, Family::N10),
            redemption: row.decimal("redemption", &source.redemption, Family::N10),
            net_realized_gain: row.decimal(
                "net_realized_gain",
                &source.net_realized_gain,
                Family::N10,
            ),
            net_unrealized_appreciation: row.decimal(
                "net_unrealized_appreciation",
                &source.net_unrealized_appreciation,
                Family::N10,
            ),
            has_parse_issues: row.finish(),
        });
    }

    let mut prepared = PreparedNport {
        report,
        returns,
        activity,
        holdings: Vec::with_capacity(body.holdings.len()),
        ..PreparedNport::default()
    };
    for (position, holding) in body.holdings.iter().enumerate() {
        prepare_holding(&mut prepared, idx(position)?, holding, issues)?;
    }
    Ok(prepared)
}

/// One holding and every row below it, in the reference's emit order.
fn prepare_holding<'a>(
    prepared: &mut PreparedNport<'a>,
    holding_index: u32,
    source: &'a sec::PortfolioHolding,
    issues: &mut IssueSink<'a>,
) -> Result<()> {
    let debt = source.debt_security.as_ref();
    let lending = source.security_lending.as_ref();
    let references: &'a [sec::DebtReferenceInstrument] =
        debt.map_or(&[], |d| d.reference_instruments.as_slice());
    let conversions: &'a [sec::ConversionCurrency] =
        debt.map_or(&[], |d| d.conversion_currencies.as_slice());

    let mut row = issues.row(NPORT_HOLDINGS, &[holding_index]);
    prepared.holdings.push(HoldingRow {
        source,
        holding_index,
        balance: row.decimal("balance", &source.balance, Family::N10),
        exchange_rate: row.decimal("exchange_rate", &source.exchange_rate, Family::R12),
        value_usd: row.decimal("value_usd", &source.value_usd, Family::N10),
        pct_value: row.decimal("pct_value", &source.pct_value, Family::R12),
        debt_maturity_date: row.date("debt_maturity_date", member(debt, |d| &d.maturity_date)),
        debt_annualized_rate: row.decimal(
            "debt_annualized_rate",
            member(debt, |d| &d.annualized_rate),
            Family::R12,
        ),
        debt_reference_instrument_count: idx(references.len())?,
        debt_conversion_currency_count: idx(conversions.len())?,
        lending_loan_value: row.decimal(
            "lending_loan_value",
            member(lending, |l| &l.loan_value),
            Family::N10,
        ),
        lending_cash_collateral_value: row.decimal(
            "lending_cash_collateral_value",
            member(lending, |l| &l.cash_collateral_value),
            Family::N10,
        ),
        has_parse_issues: row.finish(),
    });

    for (position, source) in references.iter().enumerate() {
        prepared.references.push(ReferenceRow {
            source,
            holding_index,
            reference_index: idx(position)?,
        });
    }

    for (position, source) in conversions.iter().enumerate() {
        let conversion_index = idx(position)?;
        let mut row = issues.row(
            NPORT_DEBT_CONVERSION_CURRENCIES,
            &[holding_index, conversion_index],
        );
        prepared.conversions.push(ConversionRow {
            source,
            holding_index,
            conversion_index,
            conversion_ratio: row.decimal(
                "conversion_ratio",
                &source.conversion_ratio,
                Family::R12,
            ),
            has_parse_issues: row.finish(),
        });
    }

    // §8.2: walk `derivative(.nested)*` with a loop, never recursion.
    let mut node = source.derivative.as_ref();
    let mut level = 0usize;
    while let Some(derivative) = node {
        prepare_derivative(prepared, holding_index, idx(level)?, derivative, issues)?;
        node = derivative.nested.as_deref();
        level += 1;
    }
    Ok(())
}

/// One derivative node, then its swap legs and index components.
fn prepare_derivative<'a>(
    prepared: &mut PreparedNport<'a>,
    holding_index: u32,
    nesting_level: u32,
    source: &'a sec::Derivative,
    issues: &mut IssueSink<'a>,
) -> Result<()> {
    let addl = source.additional_info.as_ref();
    let mut row = issues.row(NPORT_DERIVATIVES, &[holding_index, nesting_level]);
    prepared.derivatives.push(DerivativeRow {
        source,
        holding_index,
        nesting_level,
        share_no: row.decimal("share_no", &source.share_no, Family::N10),
        principal_amount: row.decimal("principal_amount", &source.principal_amount, Family::N10),
        notional_amount: row.decimal("notional_amount", &source.notional_amount, Family::N10),
        exercise_price: row.decimal("exercise_price", &source.exercise_price, Family::N10),
        expiration_date: row.date("expiration_date", &source.expiration_date),
        unrealized_appreciation: row.decimal(
            "unrealized_appreciation",
            &source.unrealized_appreciation,
            Family::N10,
        ),
        termination_date: row.date("termination_date", &source.termination_date),
        amount_currency_sold: row.decimal(
            "amount_currency_sold",
            &source.amount_currency_sold,
            Family::N10,
        ),
        amount_currency_purchased: row.decimal(
            "amount_currency_purchased",
            &source.amount_currency_purchased,
            Family::N10,
        ),
        settlement_date: row.date("settlement_date", &source.settlement_date),
        upfront_payment: row.decimal("upfront_payment", &source.upfront_payment, Family::N10),
        upfront_receipt: row.decimal("upfront_receipt", &source.upfront_receipt, Family::N10),
        swap_flag: row.yn("swap_flag", &source.swap_flag),
        swap_leg_count: idx(source.legs.len())?,
        index_component_count: idx(source.ref_index_components.len())?,
        addl_balance: row.decimal("addl_balance", member(addl, |a| &a.balance), Family::N10),
        addl_exchange_rate: row.decimal(
            "addl_exchange_rate",
            member(addl, |a| &a.exchange_rate),
            Family::R12,
        ),
        addl_value_usd: row.decimal(
            "addl_value_usd",
            member(addl, |a| &a.value_usd),
            Family::N10,
        ),
        addl_pct_value: row.decimal(
            "addl_pct_value",
            member(addl, |a| &a.pct_value),
            Family::R12,
        ),
        has_parse_issues: row.finish(),
    });

    for (position, source) in source.legs.iter().enumerate() {
        let leg_index = idx(position)?;
        let mut row = issues.row(
            NPORT_DERIVATIVE_SWAP_LEGS,
            &[holding_index, nesting_level, leg_index],
        );
        prepared.legs.push(LegRow {
            source,
            holding_index,
            nesting_level,
            leg_index,
            amount: row.decimal("amount", &source.amount, Family::N10),
            fixed_rate: row.decimal("fixed_rate", &source.fixed_rate, Family::R12),
            floating_rate_spread: row.decimal(
                "floating_rate_spread",
                &source.floating_rate_spread,
                Family::R12,
            ),
            payment_amount: row.decimal("payment_amount", &source.payment_amount, Family::N10),
            has_parse_issues: row.finish(),
        });
    }

    for (position, source) in source.ref_index_components.iter().enumerate() {
        let component_index = idx(position)?;
        let mut row = issues.row(
            NPORT_DERIVATIVE_INDEX_COMPONENTS,
            &[holding_index, nesting_level, component_index],
        );
        prepared.components.push(ComponentRow {
            source,
            holding_index,
            nesting_level,
            component_index,
            notional_amount: row.decimal("notional_amount", &source.notional_amount, Family::N10),
            value: row.decimal("value", &source.value, Family::N10),
            has_parse_issues: row.finish(),
        });
    }
    Ok(())
}

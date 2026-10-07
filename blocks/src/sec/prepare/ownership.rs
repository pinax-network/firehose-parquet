//! Preflight of `ownership_documents`, `ownership_reporting_owners`, `ownership_transactions`, `ownership_holdings`, `ownership_footnotes` (§3.9, §3.10, §3.11, §3.12, §3.13).
//! Owned by the `ownership` group: see `/tmp/sec-fireparq/impl/contracts.md`.
//!
//! Fallible only on the structural invariants of §4.7 (positions through
//! `super::idx`); every typed value goes through `IssueSink::row`.
//!
//! Issue order (the `parse_issues` row order) is the reference's emit order:
//! the document row, the reporting owners (no typed column), the transactions
//! (non-derivative, then derivative), the holdings (same), the footnotes (no
//! typed column). Within a row, columns are in schema order and the derived
//! `value_usd` overflow comes last.

use anyhow::Result;

use super::{idx, FilingCtx};
use crate::sec::issues::{IssueSink, RowIssues};
use crate::sec::parse::{mul_rescale, Family, IssueKind};
use crate::sec::proto::sec;
use crate::sec::schema::{OWNERSHIP_DOCUMENTS, OWNERSHIP_HOLDINGS, OWNERSHIP_TRANSACTIONS};

/// Every Forms 3/4/5 amount is a quantity or a per-unit price (§4.3).
const Q6: Family = Family::Q6;

/// The parsed and derived values of `ownership_documents`, `ownership_reporting_owners`, `ownership_transactions`, `ownership_holdings`, `ownership_footnotes` for one filing.
#[derive(Debug)]
pub(crate) struct PreparedOwnership<'a> {
    pub document: PreparedDocument,
    /// The owner context copied onto every transaction and holding row.
    pub owners: OwnerContext<'a>,
    /// One per row of [`transactions`], in that order.
    pub transactions: Vec<PreparedTransaction>,
    /// One per row of [`holdings`], in that order.
    pub holdings: Vec<PreparedHolding>,
}

/// The typed values of the `ownership_documents` row.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct PreparedDocument {
    pub period_of_report: Option<i32>,
    pub date_of_original_submission: Option<i32>,
    pub reporting_owner_count: u32,
    pub non_derivative_transaction_count: u32,
    pub derivative_transaction_count: u32,
    pub non_derivative_holding_count: u32,
    pub derivative_holding_count: u32,
    pub footnote_count: u32,
    pub owner_signature_count: u32,
    pub has_parse_issues: bool,
}

/// The §4.4 owner context: ORs over **all** reporting owners and the titles
/// of the officers among them. `owner_ciks`/`owner_names` are read from the
/// proto during the append.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct OwnerContext<'a> {
    pub any_owner_is_director: bool,
    pub any_owner_is_officer: bool,
    pub any_owner_is_ten_percent_owner: bool,
    pub any_owner_is_other: bool,
    /// The non-empty `officer_title` of every owner with `is_officer`, in
    /// owner order.
    pub officer_titles: Vec<&'a str>,
}

/// The typed and derived values of one `ownership_transactions` row.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct PreparedTransaction {
    pub transaction_index: u32,
    pub is_derivative: bool,
    pub transaction_date: Option<i32>,
    pub deemed_execution_date: Option<i32>,
    pub shares: Option<i128>,
    pub price_per_share: Option<i128>,
    pub total_value: Option<i128>,
    pub shares_owned_following: Option<i128>,
    pub value_owned_following: Option<i128>,
    pub conversion_or_exercise_price: Option<i128>,
    pub exercise_date: Option<i32>,
    pub expiration_date: Option<i32>,
    pub underlying_security_shares: Option<i128>,
    pub underlying_security_value: Option<i128>,
    pub signed_shares: Option<i128>,
    pub value_usd: Option<i128>,
    pub is_open_market: bool,
    pub filing_lag_days: Option<i32>,
    pub has_parse_issues: bool,
}

/// The typed values of one `ownership_holdings` row.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct PreparedHolding {
    pub holding_index: u32,
    pub is_derivative: bool,
    pub shares_owned: Option<i128>,
    pub value_owned: Option<i128>,
    pub conversion_or_exercise_price: Option<i128>,
    pub exercise_date: Option<i32>,
    pub expiration_date: Option<i32>,
    pub underlying_security_shares: Option<i128>,
    pub underlying_security_value: Option<i128>,
    pub has_parse_issues: bool,
}

/// The `ownership_transactions` rows in `transaction_index` order (§8.2):
/// Table I (non-derivative) rows first, then Table II, each in document
/// order, with their `is_derivative` flag.
pub(crate) fn transactions(
    body: &sec::OwnershipDocument,
) -> impl Iterator<Item = (bool, &sec::Transaction)> {
    let table_1 = body
        .non_derivative_transactions
        .iter()
        .map(|tx| (false, tx));
    let table_2 = body.derivative_transactions.iter().map(|tx| (true, tx));
    table_1.chain(table_2)
}

/// The `ownership_holdings` rows in `holding_index` order (§8.2): Table I
/// rows first, then Table II.
pub(crate) fn holdings(
    body: &sec::OwnershipDocument,
) -> impl Iterator<Item = (bool, &sec::Holding)> {
    let table_1 = body.non_derivative_holdings.iter().map(|h| (false, h));
    let table_2 = body.derivative_holdings.iter().map(|h| (true, h));
    table_1.chain(table_2)
}

pub(crate) fn prepare<'a>(
    fc: &FilingCtx<'a>,
    body: &'a sec::OwnershipDocument,
    issues: &mut IssueSink<'a>,
) -> Result<PreparedOwnership<'a>> {
    // Structural invariants (§4.7 item 6): every count fits `UInt32`, so every
    // position of a single list does too; the concatenated positions are
    // checked one by one below.
    let reporting_owner_count = idx(body.reporting_owners.len())?;
    let non_derivative_transaction_count = idx(body.non_derivative_transactions.len())?;
    let derivative_transaction_count = idx(body.derivative_transactions.len())?;
    let non_derivative_holding_count = idx(body.non_derivative_holdings.len())?;
    let derivative_holding_count = idx(body.derivative_holdings.len())?;
    let footnote_count = idx(body.footnotes.len())?;
    let owner_signature_count = idx(body.owner_signatures.len())?;

    let mut row = issues.row(OWNERSHIP_DOCUMENTS, &[]);
    let period_of_report = row.date("period_of_report", &body.period_of_report);
    let date_of_original_submission = row.date(
        "date_of_original_submission",
        &body.date_of_original_submission,
    );
    let document = PreparedDocument {
        period_of_report,
        date_of_original_submission,
        reporting_owner_count,
        non_derivative_transaction_count,
        derivative_transaction_count,
        non_derivative_holding_count,
        derivative_holding_count,
        footnote_count,
        owner_signature_count,
        has_parse_issues: row.finish(),
    };

    let transactions = transactions(body)
        .enumerate()
        .map(|(position, (is_derivative, tx))| {
            let transaction_index = idx(position)?;
            Ok(prepare_transaction(
                fc,
                transaction_index,
                is_derivative,
                tx,
                issues,
            ))
        })
        .collect::<Result<Vec<_>>>()?;

    let holdings = holdings(body)
        .enumerate()
        .map(|(position, (is_derivative, holding))| {
            let holding_index = idx(position)?;
            Ok(prepare_holding(
                holding_index,
                is_derivative,
                holding,
                issues,
            ))
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(PreparedOwnership {
        document,
        owners: owner_context(body),
        transactions,
        holdings,
    })
}

fn owner_context(body: &sec::OwnershipDocument) -> OwnerContext<'_> {
    let mut context = OwnerContext::default();
    for relationship in body
        .reporting_owners
        .iter()
        .filter_map(|owner| owner.relationship.as_ref())
    {
        context.any_owner_is_director |= relationship.is_director;
        context.any_owner_is_officer |= relationship.is_officer;
        context.any_owner_is_ten_percent_owner |= relationship.is_ten_percent_owner;
        context.any_owner_is_other |= relationship.is_other;
        if relationship.is_officer && !relationship.officer_title.is_empty() {
            context.officer_titles.push(&relationship.officer_title);
        }
    }
    context
}

/// `underlying_security.{shares,value}` (Q6): NULL with no issue when the
/// sub-message is absent.
fn underlying<'a>(
    row: &mut RowIssues<'_, 'a>,
    security: Option<&'a sec::UnderlyingSecurity>,
) -> (Option<i128>, Option<i128>) {
    let shares = row.decimal(
        "underlying_security_shares",
        security.map_or("", |s| s.shares.as_str()),
        Q6,
    );
    let value = row.decimal(
        "underlying_security_value",
        security.map_or("", |s| s.value.as_str()),
        Q6,
    );
    (shares, value)
}

fn prepare_transaction<'a>(
    fc: &FilingCtx<'a>,
    transaction_index: u32,
    is_derivative: bool,
    tx: &'a sec::Transaction,
    issues: &mut IssueSink<'a>,
) -> PreparedTransaction {
    let mut row = issues.row(OWNERSHIP_TRANSACTIONS, &[transaction_index]);
    // Parsed columns, in schema order.
    let transaction_date = row.date("transaction_date", &tx.transaction_date);
    let deemed_execution_date = row.date("deemed_execution_date", &tx.deemed_execution_date);
    let shares = row.decimal("shares", &tx.shares, Q6);
    let price_per_share = row.decimal("price_per_share", &tx.price_per_share, Q6);
    let total_value = row.decimal("total_value", &tx.total_value, Q6);
    let shares_owned_following =
        row.decimal("shares_owned_following", &tx.shares_owned_following, Q6);
    let value_owned_following = row.decimal("value_owned_following", &tx.value_owned_following, Q6);
    let conversion_or_exercise_price = row.decimal(
        "conversion_or_exercise_price",
        &tx.conversion_or_exercise_price,
        Q6,
    );
    let exercise_date = row.date("exercise_date", &tx.exercise_date);
    let expiration_date = row.date("expiration_date", &tx.expiration_date);
    let (underlying_security_shares, underlying_security_value) =
        underlying(&mut row, tx.underlying_security.as_ref());

    // Derived columns (§4.3, §4.4), after the parsed ones.
    let signed_shares = match tx.acquired_disposed_code.as_str() {
        "A" => shares,
        "D" => shares.map(|s| -s),
        _ => None,
    };
    let value_usd = match (shares, price_per_share) {
        (Some(shares), Some(price)) => {
            let scale = Q6.scale();
            let product = mul_rescale(shares, scale, price, scale, scale);
            if product.is_none() {
                row.record(
                    "value_usd",
                    None,
                    format!("{} * {}", tx.shares, tx.price_per_share),
                    IssueKind::Overflow,
                );
            }
            product
        }
        _ => None,
    };
    let is_open_market = !is_derivative && matches!(tx.transaction_code.as_str(), "P" | "S");
    let filing_lag_days = fc
        .filing_date
        .zip(transaction_date)
        .and_then(|(filed, transacted)| filed.checked_sub(transacted));

    PreparedTransaction {
        transaction_index,
        is_derivative,
        transaction_date,
        deemed_execution_date,
        shares,
        price_per_share,
        total_value,
        shares_owned_following,
        value_owned_following,
        conversion_or_exercise_price,
        exercise_date,
        expiration_date,
        underlying_security_shares,
        underlying_security_value,
        signed_shares,
        value_usd,
        is_open_market,
        filing_lag_days,
        has_parse_issues: row.finish(),
    }
}

fn prepare_holding<'a>(
    holding_index: u32,
    is_derivative: bool,
    holding: &'a sec::Holding,
    issues: &mut IssueSink<'a>,
) -> PreparedHolding {
    let mut row = issues.row(OWNERSHIP_HOLDINGS, &[holding_index]);
    // Parsed columns, in schema order.
    let shares_owned = row.decimal("shares_owned", &holding.shares_owned, Q6);
    let value_owned = row.decimal("value_owned", &holding.value_owned, Q6);
    let conversion_or_exercise_price = row.decimal(
        "conversion_or_exercise_price",
        &holding.conversion_or_exercise_price,
        Q6,
    );
    let exercise_date = row.date("exercise_date", &holding.exercise_date);
    let expiration_date = row.date("expiration_date", &holding.expiration_date);
    let (underlying_security_shares, underlying_security_value) =
        underlying(&mut row, holding.underlying_security.as_ref());
    PreparedHolding {
        holding_index,
        is_derivative,
        shares_owned,
        value_owned,
        conversion_or_exercise_price,
        exercise_date,
        expiration_date,
        underlying_security_shares,
        underlying_security_value,
        has_parse_issues: row.finish(),
    }
}

//! Append phase of `ownership_documents`, `ownership_reporting_owners`, `ownership_transactions`, `ownership_holdings`, `ownership_footnotes` (§3.9, §3.10, §3.11, §3.12, §3.13).
//! Owned by the `ownership` group: see `/tmp/sec-fireparq/impl/contracts.md`.
//!
//! The column structs below are generated from `super::super::schema` and checked
//! against it on construction; edit them only together with the schema.

use firehose_parquet::encode::EncodeBytes;

use super::{sec_columns, AppendCtx, SecTable, Table};
use super::{Addr, Bool, Date, Dec, Fc, ListStr, Str, I32, U32};
use crate::sec::prepare::ownership::{self as prep, OwnerContext, PreparedOwnership};
use crate::sec::prepare::FilingCtx;
use crate::sec::proto::sec;
use crate::sec::schema;

sec_columns! {
    /// The columns of `ownership_documents` (§3.9), in schema order.
    pub(crate) struct OwnershipDocumentsCols {
        /// [FC] `filing_index` … `acceptance_datetime`.
        pub fc: Fc,
        pub issuer_cik: Str,
        pub issuer_name: Str,
        pub issuer_trading_symbol: Str,
        pub issuer_foreign_trading_symbol: Str,
        pub schema_version: Str,
        pub document_type: Str,
        pub period_of_report: Date,
        pub not_subject_to_section16: Bool,
        pub aff_10b5_one: Bool,
        pub no_securities_owned: Bool,
        pub form3_holdings_reported: Bool,
        pub form4_transactions_reported: Bool,
        pub date_of_original_submission: Date,
        pub remarks: Str,
        pub reporting_owner_count: U32,
        pub non_derivative_transaction_count: U32,
        pub derivative_transaction_count: U32,
        pub non_derivative_holding_count: U32,
        pub derivative_holding_count: U32,
        pub footnote_count: U32,
        pub owner_signature_count: U32,
        pub has_parse_issues: Bool,
    }
}

sec_columns! {
    /// The columns of `ownership_reporting_owners` (§3.10), in schema order.
    pub(crate) struct OwnershipReportingOwnersCols {
        /// [FC] `filing_index` … `acceptance_datetime`.
        pub fc: Fc,
        pub issuer_cik: Str,
        pub issuer_trading_symbol: Str,
        pub owner_index: U32,
        pub owner_cik: Str,
        pub owner_name: Str,
        /// `owner_street1` … `owner_non_us_state_territory`.
        pub owner: Addr,
        pub is_director: Bool,
        pub is_officer: Bool,
        pub is_ten_percent_owner: Bool,
        pub is_other: Bool,
        pub officer_title: Str,
        pub other_text: Str,
    }
}

sec_columns! {
    /// The columns of `ownership_transactions` (§3.11), in schema order.
    pub(crate) struct OwnershipTransactionsCols {
        /// [FC] `filing_index` … `acceptance_datetime`.
        pub fc: Fc,
        pub issuer_cik: Str,
        pub issuer_name: Str,
        pub issuer_trading_symbol: Str,
        pub reporting_owner_count: U32,
        pub owner_ciks: ListStr,
        pub owner_names: ListStr,
        pub any_owner_is_director: Bool,
        pub any_owner_is_officer: Bool,
        pub any_owner_is_ten_percent_owner: Bool,
        pub any_owner_is_other: Bool,
        pub officer_titles: ListStr,
        pub aff_10b5_one: Bool,
        pub transaction_index: U32,
        pub is_derivative: Bool,
        pub security_title: Str,
        pub transaction_date: Date,
        pub deemed_execution_date: Date,
        pub transaction_form_type: Str,
        pub transaction_code: Str,
        pub equity_swap_involved: Bool,
        pub transaction_timeliness: Str,
        pub shares: Dec,
        pub price_per_share: Dec,
        pub total_value: Dec,
        pub acquired_disposed_code: Str,
        pub shares_owned_following: Dec,
        pub value_owned_following: Dec,
        pub direct_or_indirect: Str,
        pub nature_of_ownership: Str,
        pub conversion_or_exercise_price: Dec,
        pub exercise_date: Date,
        pub expiration_date: Date,
        pub underlying_security_title: Str,
        pub underlying_security_shares: Dec,
        pub underlying_security_value: Dec,
        pub footnote_ids: ListStr,
        pub signed_shares: Dec,
        pub value_usd: Dec,
        pub is_open_market: Bool,
        pub filing_lag_days: I32,
        pub has_parse_issues: Bool,
    }
}

sec_columns! {
    /// The columns of `ownership_holdings` (§3.12), in schema order.
    pub(crate) struct OwnershipHoldingsCols {
        /// [FC] `filing_index` … `acceptance_datetime`.
        pub fc: Fc,
        pub issuer_cik: Str,
        pub issuer_name: Str,
        pub issuer_trading_symbol: Str,
        pub reporting_owner_count: U32,
        pub owner_ciks: ListStr,
        pub owner_names: ListStr,
        pub any_owner_is_director: Bool,
        pub any_owner_is_officer: Bool,
        pub any_owner_is_ten_percent_owner: Bool,
        pub any_owner_is_other: Bool,
        pub officer_titles: ListStr,
        pub holding_index: U32,
        pub is_derivative: Bool,
        pub security_title: Str,
        pub shares_owned: Dec,
        pub value_owned: Dec,
        pub direct_or_indirect: Str,
        pub nature_of_ownership: Str,
        pub conversion_or_exercise_price: Dec,
        pub exercise_date: Date,
        pub expiration_date: Date,
        pub underlying_security_title: Str,
        pub underlying_security_shares: Dec,
        pub underlying_security_value: Dec,
        pub footnote_ids: ListStr,
        pub has_parse_issues: Bool,
    }
}

sec_columns! {
    /// The columns of `ownership_footnotes` (§3.13), in schema order.
    pub(crate) struct OwnershipFootnotesCols {
        /// [FC] `filing_index` … `acceptance_datetime`.
        pub fc: Fc,
        pub issuer_cik: Str,
        pub footnote_index: U32,
        pub footnote_id: Str,
        pub footnote_text: Str,
    }
}

/// One document's issuer fields (`''` when `issuer` is absent), which the
/// child rows copy from their `ownership_documents` row (§3.10–§3.13), and the
/// owner context of its transaction and holding rows.
struct Parent<'p> {
    issuer_cik: &'p str,
    issuer_name: &'p str,
    issuer_trading_symbol: &'p str,
    issuer_foreign_trading_symbol: &'p str,
    reporting_owner_count: u32,
    reporting_owners: &'p [sec::ReportingOwner],
    owners: &'p OwnerContext<'p>,
}

impl<'p> Parent<'p> {
    fn of(body: &'p sec::OwnershipDocument, prepared: &'p PreparedOwnership<'_>) -> Self {
        let issuer = body.issuer.as_ref();
        Self {
            issuer_cik: issuer.map_or("", |issuer| issuer.cik.as_str()),
            issuer_name: issuer.map_or("", |issuer| issuer.name.as_str()),
            issuer_trading_symbol: issuer.map_or("", |issuer| issuer.trading_symbol.as_str()),
            issuer_foreign_trading_symbol: issuer
                .map_or("", |issuer| issuer.foreign_trading_symbol.as_str()),
            reporting_owner_count: prepared.document.reporting_owner_count,
            reporting_owners: &body.reporting_owners,
            owners: &prepared.owners,
        }
    }

    /// `owner_ciks`: every owner's CIK in document order (`""` → NULL item).
    fn owner_ciks(&self) -> impl Iterator<Item = &'p str> {
        self.reporting_owners.iter().map(|owner| owner.cik.as_str())
    }

    /// `owner_names`: every owner's name, same order.
    fn owner_names(&self) -> impl Iterator<Item = &'p str> {
        self.reporting_owners
            .iter()
            .map(|owner| owner.name.as_str())
    }
}

/// Every table of this module.
pub(crate) struct OwnershipTables {
    pub(crate) ownership_documents: Table<OwnershipDocumentsCols>,
    pub(crate) ownership_reporting_owners: Table<OwnershipReportingOwnersCols>,
    pub(crate) ownership_transactions: Table<OwnershipTransactionsCols>,
    pub(crate) ownership_holdings: Table<OwnershipHoldingsCols>,
    pub(crate) ownership_footnotes: Table<OwnershipFootnotesCols>,
}

impl OwnershipTables {
    pub(crate) fn new(include_fork_step: bool, encoding: &EncodeBytes) -> Self {
        Self {
            ownership_documents: Table::new(
                schema::OWNERSHIP_DOCUMENTS,
                include_fork_step,
                encoding,
            ),
            ownership_reporting_owners: Table::new(
                schema::OWNERSHIP_REPORTING_OWNERS,
                include_fork_step,
                encoding,
            ),
            ownership_transactions: Table::new(
                schema::OWNERSHIP_TRANSACTIONS,
                include_fork_step,
                encoding,
            ),
            ownership_holdings: Table::new(schema::OWNERSHIP_HOLDINGS, include_fork_step, encoding),
            ownership_footnotes: Table::new(
                schema::OWNERSHIP_FOOTNOTES,
                include_fork_step,
                encoding,
            ),
        }
    }

    /// Append the rows of one filing's body, from its proto message and the
    /// values prepared by `crate::sec::prepare::ownership::prepare`. Infallible.
    pub(crate) fn append(
        &mut self,
        ctx: &AppendCtx<'_>,
        fc: &FilingCtx<'_>,
        body: &sec::OwnershipDocument,
        prepared: &PreparedOwnership<'_>,
    ) {
        let parent = Parent::of(body, prepared);
        self.append_document(ctx, fc, body, prepared, &parent);
        self.append_owners(ctx, fc, body, &parent);
        self.append_transactions(ctx, fc, body, prepared, &parent);
        self.append_holdings(ctx, fc, body, prepared, &parent);
        self.append_footnotes(ctx, fc, body, &parent);
    }

    fn append_document(
        &mut self,
        ctx: &AppendCtx<'_>,
        fc: &FilingCtx<'_>,
        body: &sec::OwnershipDocument,
        prepared: &PreparedOwnership<'_>,
        parent: &Parent<'_>,
    ) {
        let d = &prepared.document;
        let row = self.ownership_documents.row(ctx);
        row.fc.append(fc);
        row.issuer_cik.nz(parent.issuer_cik);
        row.issuer_name.nz(parent.issuer_name);
        row.issuer_trading_symbol.nz(parent.issuer_trading_symbol);
        row.issuer_foreign_trading_symbol
            .nz(parent.issuer_foreign_trading_symbol);
        row.schema_version.nz(&body.schema_version);
        row.document_type.nz(&body.document_type);
        row.period_of_report.opt(d.period_of_report);
        row.not_subject_to_section16
            .val(body.not_subject_to_section16);
        row.aff_10b5_one.opt(body.aff_10b5_one);
        row.no_securities_owned.opt(body.no_securities_owned);
        row.form3_holdings_reported
            .opt(body.form3_holdings_reported);
        row.form4_transactions_reported
            .opt(body.form4_transactions_reported);
        row.date_of_original_submission
            .opt(d.date_of_original_submission);
        row.remarks.nz(&body.remarks);
        row.reporting_owner_count.val(d.reporting_owner_count);
        row.non_derivative_transaction_count
            .val(d.non_derivative_transaction_count);
        row.derivative_transaction_count
            .val(d.derivative_transaction_count);
        row.non_derivative_holding_count
            .val(d.non_derivative_holding_count);
        row.derivative_holding_count.val(d.derivative_holding_count);
        row.footnote_count.val(d.footnote_count);
        row.owner_signature_count.val(d.owner_signature_count);
        row.has_parse_issues.val(d.has_parse_issues);
    }

    fn append_owners(
        &mut self,
        ctx: &AppendCtx<'_>,
        fc: &FilingCtx<'_>,
        body: &sec::OwnershipDocument,
        parent: &Parent<'_>,
    ) {
        // The slice drives the zip, so the position counter never steps past
        // the checked count.
        for (owner, owner_index) in body.reporting_owners.iter().zip(0u32..) {
            let row = self.ownership_reporting_owners.row(ctx);
            row.fc.append(fc);
            row.issuer_cik.nz(parent.issuer_cik);
            row.issuer_trading_symbol.nz(parent.issuer_trading_symbol);
            row.owner_index.val(owner_index);
            row.owner_cik.nz(&owner.cik);
            row.owner_name.nz(&owner.name);
            row.owner.append(owner.address.as_ref());
            // §4.1: bools of the optional `Relationship` are NULL exactly when
            // it is absent.
            let relationship = owner.relationship.as_ref();
            row.is_director.opt(relationship.map(|r| r.is_director));
            row.is_officer.opt(relationship.map(|r| r.is_officer));
            row.is_ten_percent_owner
                .opt(relationship.map(|r| r.is_ten_percent_owner));
            row.is_other.opt(relationship.map(|r| r.is_other));
            row.officer_title
                .nz(relationship.map_or("", |r| r.officer_title.as_str()));
            row.other_text
                .nz(relationship.map_or("", |r| r.other_text.as_str()));
        }
    }

    fn append_transactions(
        &mut self,
        ctx: &AppendCtx<'_>,
        fc: &FilingCtx<'_>,
        body: &sec::OwnershipDocument,
        prepared: &PreparedOwnership<'_>,
        parent: &Parent<'_>,
    ) {
        for ((_, tx), t) in prep::transactions(body).zip(&prepared.transactions) {
            let row = self.ownership_transactions.row(ctx);
            row.fc.append(fc);
            row.issuer_cik.nz(parent.issuer_cik);
            row.issuer_name.nz(parent.issuer_name);
            row.issuer_trading_symbol.nz(parent.issuer_trading_symbol);
            row.reporting_owner_count.val(parent.reporting_owner_count);
            row.owner_ciks.items_nz(parent.owner_ciks());
            row.owner_names.items_nz(parent.owner_names());
            row.any_owner_is_director
                .val(parent.owners.any_owner_is_director);
            row.any_owner_is_officer
                .val(parent.owners.any_owner_is_officer);
            row.any_owner_is_ten_percent_owner
                .val(parent.owners.any_owner_is_ten_percent_owner);
            row.any_owner_is_other.val(parent.owners.any_owner_is_other);
            row.officer_titles
                .items(parent.owners.officer_titles.iter().copied());
            row.aff_10b5_one.opt(body.aff_10b5_one);
            row.transaction_index.val(t.transaction_index);
            row.is_derivative.val(t.is_derivative);
            row.security_title.nz(&tx.security_title);
            row.transaction_date.opt(t.transaction_date);
            row.deemed_execution_date.opt(t.deemed_execution_date);
            row.transaction_form_type.nz(&tx.transaction_form_type);
            row.transaction_code.nz(&tx.transaction_code);
            row.equity_swap_involved.val(tx.equity_swap_involved);
            row.transaction_timeliness.nz(&tx.transaction_timeliness);
            row.shares.opt(t.shares);
            row.price_per_share.opt(t.price_per_share);
            row.total_value.opt(t.total_value);
            row.acquired_disposed_code.nz(&tx.acquired_disposed_code);
            row.shares_owned_following.opt(t.shares_owned_following);
            row.value_owned_following.opt(t.value_owned_following);
            row.direct_or_indirect.nz(&tx.direct_or_indirect);
            row.nature_of_ownership.nz(&tx.nature_of_ownership);
            row.conversion_or_exercise_price
                .opt(t.conversion_or_exercise_price);
            row.exercise_date.opt(t.exercise_date);
            row.expiration_date.opt(t.expiration_date);
            row.underlying_security_title.nz(tx
                .underlying_security
                .as_ref()
                .map_or("", |security| security.title.as_str()));
            row.underlying_security_shares
                .opt(t.underlying_security_shares);
            row.underlying_security_value
                .opt(t.underlying_security_value);
            row.footnote_ids
                .items(tx.footnote_ids.iter().map(String::as_str));
            row.signed_shares.opt(t.signed_shares);
            row.value_usd.opt(t.value_usd);
            row.is_open_market.val(t.is_open_market);
            row.filing_lag_days.opt(t.filing_lag_days);
            row.has_parse_issues.val(t.has_parse_issues);
        }
    }

    fn append_holdings(
        &mut self,
        ctx: &AppendCtx<'_>,
        fc: &FilingCtx<'_>,
        body: &sec::OwnershipDocument,
        prepared: &PreparedOwnership<'_>,
        parent: &Parent<'_>,
    ) {
        for ((_, holding), h) in prep::holdings(body).zip(&prepared.holdings) {
            let row = self.ownership_holdings.row(ctx);
            row.fc.append(fc);
            row.issuer_cik.nz(parent.issuer_cik);
            row.issuer_name.nz(parent.issuer_name);
            row.issuer_trading_symbol.nz(parent.issuer_trading_symbol);
            row.reporting_owner_count.val(parent.reporting_owner_count);
            row.owner_ciks.items_nz(parent.owner_ciks());
            row.owner_names.items_nz(parent.owner_names());
            row.any_owner_is_director
                .val(parent.owners.any_owner_is_director);
            row.any_owner_is_officer
                .val(parent.owners.any_owner_is_officer);
            row.any_owner_is_ten_percent_owner
                .val(parent.owners.any_owner_is_ten_percent_owner);
            row.any_owner_is_other.val(parent.owners.any_owner_is_other);
            row.officer_titles
                .items(parent.owners.officer_titles.iter().copied());
            row.holding_index.val(h.holding_index);
            row.is_derivative.val(h.is_derivative);
            row.security_title.nz(&holding.security_title);
            row.shares_owned.opt(h.shares_owned);
            row.value_owned.opt(h.value_owned);
            row.direct_or_indirect.nz(&holding.direct_or_indirect);
            row.nature_of_ownership.nz(&holding.nature_of_ownership);
            row.conversion_or_exercise_price
                .opt(h.conversion_or_exercise_price);
            row.exercise_date.opt(h.exercise_date);
            row.expiration_date.opt(h.expiration_date);
            row.underlying_security_title.nz(holding
                .underlying_security
                .as_ref()
                .map_or("", |security| security.title.as_str()));
            row.underlying_security_shares
                .opt(h.underlying_security_shares);
            row.underlying_security_value
                .opt(h.underlying_security_value);
            row.footnote_ids
                .items(holding.footnote_ids.iter().map(String::as_str));
            row.has_parse_issues.val(h.has_parse_issues);
        }
    }

    fn append_footnotes(
        &mut self,
        ctx: &AppendCtx<'_>,
        fc: &FilingCtx<'_>,
        body: &sec::OwnershipDocument,
        parent: &Parent<'_>,
    ) {
        for (footnote, footnote_index) in body.footnotes.iter().zip(0u32..) {
            let row = self.ownership_footnotes.row(ctx);
            row.fc.append(fc);
            row.issuer_cik.nz(parent.issuer_cik);
            row.footnote_index.val(footnote_index);
            row.footnote_id.nz(&footnote.id);
            row.footnote_text.nz(&footnote.text);
        }
    }

    pub(crate) fn tables(&self) -> [&dyn SecTable; 5] {
        [
            &self.ownership_documents,
            &self.ownership_reporting_owners,
            &self.ownership_transactions,
            &self.ownership_holdings,
            &self.ownership_footnotes,
        ]
    }

    pub(crate) fn tables_mut(&mut self) -> [&mut dyn SecTable; 5] {
        [
            &mut self.ownership_documents,
            &mut self.ownership_reporting_owners,
            &mut self.ownership_transactions,
            &mut self.ownership_holdings,
            &mut self.ownership_footnotes,
        ]
    }
}

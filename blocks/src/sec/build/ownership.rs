//! Append phase of `ownership_documents`, `ownership_reporting_owners`, `ownership_transactions`, `ownership_holdings`, `ownership_footnotes` (§3.9, §3.10, §3.11, §3.12, §3.13).
//! Owned by the `ownership` group: see `/tmp/sec-fireparq/impl/contracts.md`.
//!
//! The column structs below are generated from `super::super::schema` and checked
//! against it on construction; edit them only together with the schema.

use firehose_parquet::encode::EncodeBytes;

use super::{sec_columns, AppendCtx, SecTable, Table};
#[allow(unused_imports)]
use super::{Addr, Bool, Date, Dec, Fc, ListStr, Str, I32, U32};
use crate::sec::prepare::ownership::PreparedOwnership;
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
        // Stub: no rows yet.
        let _ = (ctx, fc, body, prepared);
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

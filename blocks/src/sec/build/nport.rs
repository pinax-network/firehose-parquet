//! Append phase of `nport_reports`, `nport_monthly_returns`, `nport_monthly_activity`, `nport_holdings`, `nport_debt_reference_instruments`, `nport_debt_conversion_currencies`, `nport_derivatives`, `nport_derivative_swap_legs`, `nport_derivative_index_components` (§3.23, §3.24, §3.25, §3.26, §3.27, §3.28, §3.29, §3.30, §3.31).
//! Owned by the `nport` group: see `/tmp/sec-fireparq/impl/contracts.md`.
//!
//! The column structs below are generated from `super::super::schema` and checked
//! against it on construction; edit them only together with the schema.

use firehose_parquet::encode::EncodeBytes;

use super::{sec_columns, AppendCtx, SecTable, Table};
#[allow(unused_imports)]
use super::{Addr, Bool, Date, Dec, Fc, ListStruct, Str, U32};
use crate::sec::prepare::nport::PreparedNport;
use crate::sec::prepare::FilingCtx;
use crate::sec::proto::sec;
use crate::sec::schema;

sec_columns! {
    /// The columns of `nport_reports` (§3.23), in schema order.
    pub(crate) struct NportReportsCols {
        /// [FC] `filing_index` … `acceptance_datetime`.
        pub fc: Fc,
        pub filer_cik: Str,
        pub registrant_name: Str,
        pub registrant_file_number: Str,
        pub registrant_cik: Str,
        pub registrant_lei: Str,
        /// `registrant_street1` … `registrant_non_us_state_territory`.
        pub registrant: Addr,
        pub registrant_phone: Str,
        pub series_name: Str,
        pub series_id: Str,
        pub series_lei: Str,
        pub fiscal_year_end: Date,
        pub as_of_date: Date,
        pub is_final_filing: Bool,
        pub total_assets: Dec,
        pub total_liabilities: Dec,
        pub net_assets: Dec,
        pub assets_invested: Dec,
        pub misc_securities_assets: Dec,
        pub cash_not_reported: Dec,
        pub explanatory_notes: ListStruct,
        pub holdings_count: U32,
        pub derivative_holding_count: U32,
        pub debt_holding_count: U32,
        pub monthly_return_class_count: U32,
        pub has_parse_issues: Bool,
    }
}

sec_columns! {
    /// The columns of `nport_monthly_returns` (§3.24), in schema order.
    pub(crate) struct NportMonthlyReturnsCols {
        /// [FC] `filing_index` … `acceptance_datetime`.
        pub fc: Fc,
        pub series_id: Str,
        pub as_of_date: Date,
        pub return_index: U32,
        pub class_id: Str,
        pub return_month1: Dec,
        pub return_month2: Dec,
        pub return_month3: Dec,
        pub month1_end: Date,
        pub month2_end: Date,
        pub month3_end: Date,
        pub has_parse_issues: Bool,
    }
}

sec_columns! {
    /// The columns of `nport_monthly_activity` (§3.25), in schema order.
    pub(crate) struct NportMonthlyActivityCols {
        /// [FC] `filing_index` … `acceptance_datetime`.
        pub fc: Fc,
        pub series_id: Str,
        pub as_of_date: Date,
        pub activity_index: U32,
        pub month: U32,
        pub month_end: Date,
        pub sales: Dec,
        pub reinvestment: Dec,
        pub redemption: Dec,
        pub net_realized_gain: Dec,
        pub net_unrealized_appreciation: Dec,
        pub has_parse_issues: Bool,
    }
}

sec_columns! {
    /// The columns of `nport_holdings` (§3.26), in schema order.
    pub(crate) struct NportHoldingsCols {
        /// [FC] `filing_index` … `acceptance_datetime`.
        pub fc: Fc,
        pub registrant_name: Str,
        pub series_id: Str,
        pub series_name: Str,
        pub as_of_date: Date,
        pub holding_index: U32,
        pub issuer_name: Str,
        pub issuer_lei: Str,
        pub issuer_lei_norm: Str,
        pub issue_title: Str,
        pub cusip: Str,
        pub cusip_norm: Str,
        pub isin: Str,
        pub ticker: Str,
        pub other_identifier: Str,
        pub other_identifiers: ListStruct,
        pub balance: Dec,
        pub units: Str,
        pub units_description: Str,
        pub currency: Str,
        pub exchange_rate: Dec,
        pub value_usd: Dec,
        pub pct_value: Dec,
        pub payoff_profile: Str,
        pub asset_category: Str,
        pub asset_category_description: Str,
        pub issuer_category: Str,
        pub issuer_category_description: Str,
        pub investment_country: Str,
        pub fair_value_level: Str,
        pub is_restricted: Bool,
        pub has_debt_security: Bool,
        pub debt_maturity_date: Date,
        pub debt_coupon_kind: Str,
        pub debt_annualized_rate: Dec,
        pub debt_is_default: Bool,
        pub debt_are_interest_payments_in_arrears: Bool,
        pub debt_is_paid_in_kind: Bool,
        pub debt_is_mandatory_convertible: Bool,
        pub debt_is_contingent_convertible: Bool,
        pub debt_delta: Str,
        pub debt_reference_instrument_count: U32,
        pub debt_conversion_currency_count: U32,
        pub has_derivative: Bool,
        pub derivative_category: Str,
        pub has_security_lending: Bool,
        pub lending_is_cash_collateral: Bool,
        pub lending_is_non_cash_collateral: Bool,
        pub lending_is_loan_by_fund: Bool,
        pub lending_loan_value: Dec,
        pub lending_cash_collateral_value: Dec,
        pub has_parse_issues: Bool,
    }
}

sec_columns! {
    /// The columns of `nport_debt_reference_instruments` (§3.27), in schema order.
    pub(crate) struct NportDebtReferenceInstrumentsCols {
        /// [FC] `filing_index` … `acceptance_datetime`.
        pub fc: Fc,
        pub series_id: Str,
        pub as_of_date: Date,
        pub holding_index: U32,
        pub reference_index: U32,
        pub reference_name: Str,
        pub reference_title: Str,
        pub currency: Str,
        pub cusip: Str,
        pub cusip_norm: Str,
        pub isin: Str,
        pub ticker: Str,
        pub other_identifiers: ListStruct,
    }
}

sec_columns! {
    /// The columns of `nport_debt_conversion_currencies` (§3.28), in schema order.
    pub(crate) struct NportDebtConversionCurrenciesCols {
        /// [FC] `filing_index` … `acceptance_datetime`.
        pub fc: Fc,
        pub series_id: Str,
        pub as_of_date: Date,
        pub holding_index: U32,
        pub conversion_index: U32,
        pub currency: Str,
        pub conversion_ratio: Dec,
        pub has_parse_issues: Bool,
    }
}

sec_columns! {
    /// The columns of `nport_derivatives` (§3.29), in schema order.
    pub(crate) struct NportDerivativesCols {
        /// [FC] `filing_index` … `acceptance_datetime`.
        pub fc: Fc,
        pub series_id: Str,
        pub as_of_date: Date,
        pub holding_index: U32,
        pub nesting_level: U32,
        pub category: Str,
        pub counterparty_name: Str,
        pub counterparty_lei: Str,
        pub put_or_call: Str,
        pub written_or_purchased: Str,
        pub payoff_profile: Str,
        pub ref_instrument_name: Str,
        pub ref_instrument_title: Str,
        pub ref_cusip: Str,
        pub ref_cusip_norm: Str,
        pub ref_isin: Str,
        pub ref_ticker: Str,
        pub ref_other_identifiers: ListStruct,
        pub ref_index_name: Str,
        pub ref_index_identifier: Str,
        pub ref_index_description: Str,
        pub other_description: Str,
        pub share_no: Dec,
        pub principal_amount: Dec,
        pub notional_amount: Dec,
        pub exercise_price: Dec,
        pub exercise_currency: Str,
        pub expiration_date: Date,
        pub delta: Str,
        pub unrealized_appreciation: Dec,
        pub currency: Str,
        pub termination_date: Date,
        pub amount_currency_sold: Dec,
        pub currency_sold: Str,
        pub amount_currency_purchased: Dec,
        pub currency_purchased: Str,
        pub settlement_date: Date,
        pub upfront_payment: Dec,
        pub upfront_receipt: Dec,
        pub payment_currency: Str,
        pub receipt_currency: Str,
        pub swap_flag: Bool,
        pub swap_leg_count: U32,
        pub index_component_count: U32,
        pub has_nested: Bool,
        pub has_additional_info: Bool,
        pub addl_name: Str,
        pub addl_lei: Str,
        pub addl_title: Str,
        pub addl_cusip: Str,
        pub addl_isin: Str,
        pub addl_ticker: Str,
        pub addl_other_identifiers: ListStruct,
        pub addl_balance: Dec,
        pub addl_units: Str,
        pub addl_units_description: Str,
        pub addl_currency: Str,
        pub addl_exchange_rate: Dec,
        pub addl_value_usd: Dec,
        pub addl_pct_value: Dec,
        pub addl_asset_category: Str,
        pub addl_asset_category_description: Str,
        pub addl_issuer_category: Str,
        pub addl_issuer_category_description: Str,
        pub addl_investment_country: Str,
        pub addl_other_investment_country: Str,
        pub has_parse_issues: Bool,
    }
}

sec_columns! {
    /// The columns of `nport_derivative_swap_legs` (§3.30), in schema order.
    pub(crate) struct NportDerivativeSwapLegsCols {
        /// [FC] `filing_index` … `acceptance_datetime`.
        pub fc: Fc,
        pub series_id: Str,
        pub as_of_date: Date,
        pub holding_index: U32,
        pub nesting_level: U32,
        pub leg_index: U32,
        pub leg_kind: Str,
        pub fixed_or_floating: Str,
        pub currency: Str,
        pub amount: Dec,
        pub fixed_rate: Dec,
        pub floating_rate_index: Str,
        pub floating_rate_spread: Dec,
        pub payment_amount: Dec,
        pub description: Str,
        pub has_parse_issues: Bool,
    }
}

sec_columns! {
    /// The columns of `nport_derivative_index_components` (§3.31), in schema order.
    pub(crate) struct NportDerivativeIndexComponentsCols {
        /// [FC] `filing_index` … `acceptance_datetime`.
        pub fc: Fc,
        pub series_id: Str,
        pub as_of_date: Date,
        pub holding_index: U32,
        pub nesting_level: U32,
        pub component_index: U32,
        pub component_name: Str,
        pub cusip: Str,
        pub cusip_norm: Str,
        pub isin: Str,
        pub ticker: Str,
        pub other_identifiers: ListStruct,
        pub notional_amount: Dec,
        pub currency: Str,
        pub value: Dec,
        pub issue_currency: Str,
        pub has_parse_issues: Bool,
    }
}

/// Every table of this module.
pub(crate) struct NportTables {
    pub(crate) nport_reports: Table<NportReportsCols>,
    pub(crate) nport_monthly_returns: Table<NportMonthlyReturnsCols>,
    pub(crate) nport_monthly_activity: Table<NportMonthlyActivityCols>,
    pub(crate) nport_holdings: Table<NportHoldingsCols>,
    pub(crate) nport_debt_reference_instruments: Table<NportDebtReferenceInstrumentsCols>,
    pub(crate) nport_debt_conversion_currencies: Table<NportDebtConversionCurrenciesCols>,
    pub(crate) nport_derivatives: Table<NportDerivativesCols>,
    pub(crate) nport_derivative_swap_legs: Table<NportDerivativeSwapLegsCols>,
    pub(crate) nport_derivative_index_components: Table<NportDerivativeIndexComponentsCols>,
}

impl NportTables {
    pub(crate) fn new(include_fork_step: bool, encoding: &EncodeBytes) -> Self {
        Self {
            nport_reports: Table::new(schema::NPORT_REPORTS, include_fork_step, encoding),
            nport_monthly_returns: Table::new(
                schema::NPORT_MONTHLY_RETURNS,
                include_fork_step,
                encoding,
            ),
            nport_monthly_activity: Table::new(
                schema::NPORT_MONTHLY_ACTIVITY,
                include_fork_step,
                encoding,
            ),
            nport_holdings: Table::new(schema::NPORT_HOLDINGS, include_fork_step, encoding),
            nport_debt_reference_instruments: Table::new(
                schema::NPORT_DEBT_REFERENCE_INSTRUMENTS,
                include_fork_step,
                encoding,
            ),
            nport_debt_conversion_currencies: Table::new(
                schema::NPORT_DEBT_CONVERSION_CURRENCIES,
                include_fork_step,
                encoding,
            ),
            nport_derivatives: Table::new(schema::NPORT_DERIVATIVES, include_fork_step, encoding),
            nport_derivative_swap_legs: Table::new(
                schema::NPORT_DERIVATIVE_SWAP_LEGS,
                include_fork_step,
                encoding,
            ),
            nport_derivative_index_components: Table::new(
                schema::NPORT_DERIVATIVE_INDEX_COMPONENTS,
                include_fork_step,
                encoding,
            ),
        }
    }

    /// Append the rows of one filing's body, from its proto message and the
    /// values prepared by `crate::sec::prepare::nport::prepare`. Infallible.
    pub(crate) fn append(
        &mut self,
        ctx: &AppendCtx<'_>,
        fc: &FilingCtx<'_>,
        body: &sec::NportReport,
        prepared: &PreparedNport<'_>,
    ) {
        // Stub: no rows yet.
        let _ = (ctx, fc, body, prepared);
    }

    pub(crate) fn tables(&self) -> [&dyn SecTable; 9] {
        [
            &self.nport_reports,
            &self.nport_monthly_returns,
            &self.nport_monthly_activity,
            &self.nport_holdings,
            &self.nport_debt_reference_instruments,
            &self.nport_debt_conversion_currencies,
            &self.nport_derivatives,
            &self.nport_derivative_swap_legs,
            &self.nport_derivative_index_components,
        ]
    }

    pub(crate) fn tables_mut(&mut self) -> [&mut dyn SecTable; 9] {
        [
            &mut self.nport_reports,
            &mut self.nport_monthly_returns,
            &mut self.nport_monthly_activity,
            &mut self.nport_holdings,
            &mut self.nport_debt_reference_instruments,
            &mut self.nport_debt_conversion_currencies,
            &mut self.nport_derivatives,
            &mut self.nport_derivative_swap_legs,
            &mut self.nport_derivative_index_components,
        ]
    }
}

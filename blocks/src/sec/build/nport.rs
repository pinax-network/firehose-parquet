//! Append phase of `nport_reports`, `nport_monthly_returns`, `nport_monthly_activity`, `nport_holdings`, `nport_debt_reference_instruments`, `nport_debt_conversion_currencies`, `nport_derivatives`, `nport_derivative_swap_legs`, `nport_derivative_index_components` (§3.23, §3.24, §3.25, §3.26, §3.27, §3.28, §3.29, §3.30, §3.31).
//! Owned by the `nport` group: see `/tmp/sec-fireparq/impl/contracts.md`.
//!
//! The column structs below are generated from `super::super::schema` and checked
//! against it on construction; edit them only together with the schema.

use firehose_parquet::encode::EncodeBytes;

use super::{other_identifier_items, sec_columns, AppendCtx, SecTable, Table};
use super::{Addr, Bool, Date, Dec, Fc, ListStruct, Str, U32};
use crate::sec::parse::{cusip_norm, lei_norm};
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
        let general = body.general_info.as_ref();
        let registrant_name = general.map_or("", |g| g.reg_name.as_str());
        let series_id = general.map_or("", |g| g.series_id.as_str());
        let series_name = general.map_or("", |g| g.series_name.as_str());
        let as_of_date = prepared.report.as_of_date;

        let p = &prepared.report;
        let row = self.nport_reports.row(ctx);
        row.fc.append(fc);
        row.filer_cik.nz(&body.filer_cik);
        row.registrant_name.nz(registrant_name);
        row.registrant_file_number
            .nz(general.map_or("", |g| g.reg_file_number.as_str()));
        row.registrant_cik
            .nz(general.map_or("", |g| g.reg_cik.as_str()));
        row.registrant_lei
            .nz(general.map_or("", |g| g.reg_lei.as_str()));
        row.registrant
            .append(general.and_then(|g| g.reg_address.as_ref()));
        row.registrant_phone
            .nz(general.map_or("", |g| g.reg_phone.as_str()));
        row.series_name.nz(series_name);
        row.series_id.nz(series_id);
        row.series_lei
            .nz(general.map_or("", |g| g.series_lei.as_str()));
        row.fiscal_year_end.opt(p.fiscal_year_end);
        row.as_of_date.opt(as_of_date);
        row.is_final_filing.opt(general.map(|g| g.is_final_filing));
        row.total_assets.opt(p.total_assets);
        row.total_liabilities.opt(p.total_liabilities);
        row.net_assets.opt(p.net_assets);
        row.assets_invested.opt(p.assets_invested);
        row.misc_securities_assets.opt(p.misc_securities_assets);
        row.cash_not_reported.opt(p.cash_not_reported);
        row.explanatory_notes.utf8_items(
            body.explanatory_notes
                .iter()
                .map(|n| [n.note_item.as_str(), n.note.as_str()]),
        );
        row.holdings_count.val(p.holdings_count);
        row.derivative_holding_count.val(p.derivative_holding_count);
        row.debt_holding_count.val(p.debt_holding_count);
        row.monthly_return_class_count
            .val(p.monthly_return_class_count);
        row.has_parse_issues.val(p.has_parse_issues);

        for p in &prepared.returns {
            let row = self.nport_monthly_returns.row(ctx);
            row.fc.append(fc);
            row.series_id.nz(series_id);
            row.as_of_date.opt(as_of_date);
            row.return_index.val(p.return_index);
            row.class_id.nz(&p.source.class_id);
            row.return_month1.opt(p.return_month1);
            row.return_month2.opt(p.return_month2);
            row.return_month3.opt(p.return_month3);
            row.month1_end.opt(p.month1_end);
            row.month2_end.opt(p.month2_end);
            row.month3_end.opt(p.month3_end);
            row.has_parse_issues.val(p.has_parse_issues);
        }

        for p in &prepared.activity {
            let row = self.nport_monthly_activity.row(ctx);
            row.fc.append(fc);
            row.series_id.nz(series_id);
            row.as_of_date.opt(as_of_date);
            row.activity_index.val(p.activity_index);
            row.month.val(p.source.month);
            row.month_end.opt(p.month_end);
            row.sales.opt(p.sales);
            row.reinvestment.opt(p.reinvestment);
            row.redemption.opt(p.redemption);
            row.net_realized_gain.opt(p.net_realized_gain);
            row.net_unrealized_appreciation
                .opt(p.net_unrealized_appreciation);
            row.has_parse_issues.val(p.has_parse_issues);
        }

        for p in &prepared.holdings {
            let h = p.source;
            let debt = h.debt_security.as_ref();
            let lending = h.security_lending.as_ref();
            let row = self.nport_holdings.row(ctx);
            row.fc.append(fc);
            row.registrant_name.nz(registrant_name);
            row.series_id.nz(series_id);
            row.series_name.nz(series_name);
            row.as_of_date.opt(as_of_date);
            row.holding_index.val(p.holding_index);
            row.issuer_name.nz(&h.name);
            row.issuer_lei.nz(&h.lei);
            row.issuer_lei_norm.opt(lei_norm(&h.lei).as_deref());
            row.issue_title.nz(&h.title);
            row.cusip.nz(&h.cusip);
            row.cusip_norm.opt(cusip_norm(&h.cusip).as_deref());
            row.isin.nz(&h.isin);
            row.ticker.nz(&h.ticker);
            row.other_identifier.nz(&h.other_identifier);
            row.other_identifiers
                .utf8_items(other_identifier_items(&h.other_identifiers));
            row.balance.opt(p.balance);
            row.units.nz(&h.units);
            row.units_description.nz(&h.units_description);
            row.currency.nz(&h.currency);
            row.exchange_rate.opt(p.exchange_rate);
            row.value_usd.opt(p.value_usd);
            row.pct_value.opt(p.pct_value);
            row.payoff_profile.nz(&h.payoff_profile);
            row.asset_category.nz(&h.asset_category);
            row.asset_category_description
                .nz(&h.asset_category_description);
            row.issuer_category.nz(&h.issuer_category);
            row.issuer_category_description
                .nz(&h.issuer_category_description);
            row.investment_country.nz(&h.investment_country);
            row.fair_value_level.nz(&h.fair_value_level);
            row.is_restricted.val(h.is_restricted);
            row.has_debt_security.val(debt.is_some());
            row.debt_maturity_date.opt(p.debt_maturity_date);
            row.debt_coupon_kind
                .nz(debt.map_or("", |d| d.coupon_kind.as_str()));
            row.debt_annualized_rate.opt(p.debt_annualized_rate);
            row.debt_is_default.opt(debt.and_then(|d| d.is_default));
            row.debt_are_interest_payments_in_arrears
                .opt(debt.and_then(|d| d.are_interest_payments_in_arrears));
            row.debt_is_paid_in_kind
                .opt(debt.and_then(|d| d.is_paid_in_kind));
            row.debt_is_mandatory_convertible
                .opt(debt.and_then(|d| d.is_mandatory_convertible));
            row.debt_is_contingent_convertible
                .opt(debt.and_then(|d| d.is_contingent_convertible));
            row.debt_delta.nz(debt.map_or("", |d| d.delta.as_str()));
            row.debt_reference_instrument_count
                .val(p.debt_reference_instrument_count);
            row.debt_conversion_currency_count
                .val(p.debt_conversion_currency_count);
            row.has_derivative.val(h.derivative.is_some());
            row.derivative_category
                .nz(h.derivative.as_ref().map_or("", |d| d.category.as_str()));
            row.has_security_lending.val(lending.is_some());
            row.lending_is_cash_collateral
                .opt(lending.map(|l| l.is_cash_collateral));
            row.lending_is_non_cash_collateral
                .opt(lending.map(|l| l.is_non_cash_collateral));
            row.lending_is_loan_by_fund
                .opt(lending.map(|l| l.is_loan_by_fund));
            row.lending_loan_value.opt(p.lending_loan_value);
            row.lending_cash_collateral_value
                .opt(p.lending_cash_collateral_value);
            row.has_parse_issues.val(p.has_parse_issues);
        }

        for p in &prepared.references {
            let r = p.source;
            let row = self.nport_debt_reference_instruments.row(ctx);
            row.fc.append(fc);
            row.series_id.nz(series_id);
            row.as_of_date.opt(as_of_date);
            row.holding_index.val(p.holding_index);
            row.reference_index.val(p.reference_index);
            row.reference_name.nz(&r.name);
            row.reference_title.nz(&r.title);
            row.currency.nz(&r.currency);
            row.cusip.nz(&r.cusip);
            row.cusip_norm.opt(cusip_norm(&r.cusip).as_deref());
            row.isin.nz(&r.isin);
            row.ticker.nz(&r.ticker);
            row.other_identifiers
                .utf8_items(other_identifier_items(&r.other_identifiers));
        }

        for p in &prepared.conversions {
            let row = self.nport_debt_conversion_currencies.row(ctx);
            row.fc.append(fc);
            row.series_id.nz(series_id);
            row.as_of_date.opt(as_of_date);
            row.holding_index.val(p.holding_index);
            row.conversion_index.val(p.conversion_index);
            row.currency.nz(&p.source.currency);
            row.conversion_ratio.opt(p.conversion_ratio);
            row.has_parse_issues.val(p.has_parse_issues);
        }

        for p in &prepared.derivatives {
            let d = p.source;
            let addl = d.additional_info.as_ref();
            let addl_str =
                |field: fn(&sec::DerivativeAdditionalInfo) -> &str| addl.map_or("", field);
            let row = self.nport_derivatives.row(ctx);
            row.fc.append(fc);
            row.series_id.nz(series_id);
            row.as_of_date.opt(as_of_date);
            row.holding_index.val(p.holding_index);
            row.nesting_level.val(p.nesting_level);
            row.category.nz(&d.category);
            row.counterparty_name.nz(&d.counterparty_name);
            row.counterparty_lei.nz(&d.counterparty_lei);
            row.put_or_call.nz(&d.put_or_call);
            row.written_or_purchased.nz(&d.written_or_purchased);
            row.payoff_profile.nz(&d.payoff_profile);
            row.ref_instrument_name.nz(&d.ref_instrument_name);
            row.ref_instrument_title.nz(&d.ref_instrument_title);
            row.ref_cusip.nz(&d.ref_cusip);
            row.ref_cusip_norm.opt(cusip_norm(&d.ref_cusip).as_deref());
            row.ref_isin.nz(&d.ref_isin);
            row.ref_ticker.nz(&d.ref_ticker);
            row.ref_other_identifiers
                .utf8_items(other_identifier_items(&d.ref_other_identifiers));
            row.ref_index_name.nz(&d.ref_index_name);
            row.ref_index_identifier.nz(&d.ref_index_identifier);
            row.ref_index_description.nz(&d.ref_index_description);
            row.other_description.nz(&d.other_description);
            row.share_no.opt(p.share_no);
            row.principal_amount.opt(p.principal_amount);
            row.notional_amount.opt(p.notional_amount);
            row.exercise_price.opt(p.exercise_price);
            row.exercise_currency.nz(&d.exercise_currency);
            row.expiration_date.opt(p.expiration_date);
            row.delta.nz(&d.delta);
            row.unrealized_appreciation.opt(p.unrealized_appreciation);
            row.currency.nz(&d.currency);
            row.termination_date.opt(p.termination_date);
            row.amount_currency_sold.opt(p.amount_currency_sold);
            row.currency_sold.nz(&d.currency_sold);
            row.amount_currency_purchased
                .opt(p.amount_currency_purchased);
            row.currency_purchased.nz(&d.currency_purchased);
            row.settlement_date.opt(p.settlement_date);
            row.upfront_payment.opt(p.upfront_payment);
            row.upfront_receipt.opt(p.upfront_receipt);
            row.payment_currency.nz(&d.payment_currency);
            row.receipt_currency.nz(&d.receipt_currency);
            row.swap_flag.opt(p.swap_flag);
            row.swap_leg_count.val(p.swap_leg_count);
            row.index_component_count.val(p.index_component_count);
            row.has_nested.val(d.nested.is_some());
            row.has_additional_info.val(addl.is_some());
            row.addl_name.nz(addl_str(|a| &a.name));
            row.addl_lei.nz(addl_str(|a| &a.lei));
            row.addl_title.nz(addl_str(|a| &a.title));
            row.addl_cusip.nz(addl_str(|a| &a.cusip));
            row.addl_isin.nz(addl_str(|a| &a.isin));
            row.addl_ticker.nz(addl_str(|a| &a.ticker));
            row.addl_other_identifiers
                .utf8_items(other_identifier_items(
                    addl.map_or(&[], |a| a.other_identifiers.as_slice()),
                ));
            row.addl_balance.opt(p.addl_balance);
            row.addl_units.nz(addl_str(|a| &a.units));
            row.addl_units_description
                .nz(addl_str(|a| &a.units_description));
            row.addl_currency.nz(addl_str(|a| &a.currency));
            row.addl_exchange_rate.opt(p.addl_exchange_rate);
            row.addl_value_usd.opt(p.addl_value_usd);
            row.addl_pct_value.opt(p.addl_pct_value);
            row.addl_asset_category.nz(addl_str(|a| &a.asset_category));
            row.addl_asset_category_description
                .nz(addl_str(|a| &a.asset_category_description));
            row.addl_issuer_category
                .nz(addl_str(|a| &a.issuer_category));
            row.addl_issuer_category_description
                .nz(addl_str(|a| &a.issuer_category_description));
            row.addl_investment_country
                .nz(addl_str(|a| &a.investment_country));
            row.addl_other_investment_country
                .nz(addl_str(|a| &a.other_investment_country));
            row.has_parse_issues.val(p.has_parse_issues);
        }

        for p in &prepared.legs {
            let l = p.source;
            let row = self.nport_derivative_swap_legs.row(ctx);
            row.fc.append(fc);
            row.series_id.nz(series_id);
            row.as_of_date.opt(as_of_date);
            row.holding_index.val(p.holding_index);
            row.nesting_level.val(p.nesting_level);
            row.leg_index.val(p.leg_index);
            row.leg_kind.nz(&l.kind);
            row.fixed_or_floating.nz(&l.fixed_or_floating);
            row.currency.nz(&l.currency);
            row.amount.opt(p.amount);
            row.fixed_rate.opt(p.fixed_rate);
            row.floating_rate_index.nz(&l.floating_rate_index);
            row.floating_rate_spread.opt(p.floating_rate_spread);
            row.payment_amount.opt(p.payment_amount);
            row.description.nz(&l.description);
            row.has_parse_issues.val(p.has_parse_issues);
        }

        for p in &prepared.components {
            let c = p.source;
            let row = self.nport_derivative_index_components.row(ctx);
            row.fc.append(fc);
            row.series_id.nz(series_id);
            row.as_of_date.opt(as_of_date);
            row.holding_index.val(p.holding_index);
            row.nesting_level.val(p.nesting_level);
            row.component_index.val(p.component_index);
            row.component_name.nz(&c.name);
            row.cusip.nz(&c.cusip);
            row.cusip_norm.opt(cusip_norm(&c.cusip).as_deref());
            row.isin.nz(&c.isin);
            row.ticker.nz(&c.ticker);
            row.other_identifiers
                .utf8_items(other_identifier_items(&c.other_identifiers));
            row.notional_amount.opt(p.notional_amount);
            row.currency.nz(&c.currency);
            row.value.opt(p.value);
            row.issue_currency.nz(&c.issue_currency);
            row.has_parse_issues.val(p.has_parse_issues);
        }
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

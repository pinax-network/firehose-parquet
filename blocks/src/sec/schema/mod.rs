//! The 43 SEC tables (final specification §2–§3), declared as data.
//!
//! Each table is a [`TableSpec`]: the 7 canonical columns, the 5 filing-context
//! columns [`FC_COLUMNS`] on every filing-derived table, then its own columns,
//! and `fork_step`/`stream_ordinal` on non-final streams. The column `doc`s are
//! the schema reference descriptions (`docs/schemas/sec.md`): each starts with
//! the column's source path in `pinax.sec.v1`.
//!
//! The builders in `super::build` are constructed from these schemas and check
//! every column name, so a builder cannot drift from its schema.

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Fields, Schema};
use firehose_parquet::encode::EncodeBytes;
use firehose_parquet::traits::{
    canonical_fields_with_encoding, enum_data_type, push_fork_step_field, timestamp_millis_utc_type,
};

use super::parse::{Family, DECIMAL_PRECISION};

mod beneficial;
mod envelope;
mod form13f;
mod form144;
mod formc;
mod formd;
mod issues;
mod ncen;
mod nport;
mod npx;
mod ownership;

/// A `List<Struct>` member type: `List<Struct>` holds only `Utf8` and `Date32`
/// members (§1 principle 9).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Member {
    Utf8,
    Date32,
}

/// The Arrow type of one column.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Ty {
    Utf8,
    Boolean,
    UInt32,
    Int32,
    Int64,
    Date32,
    /// `Timestamp(Millisecond, "UTC")`.
    TimestampMs,
    Binary,
    /// `Dictionary(Int32, Utf8)`: mapper discriminators only.
    Dictionary,
    /// `Decimal128(38, family.scale())`.
    Decimal(Family),
    /// `List<Utf8>`, item field `"item"` nullable.
    ListUtf8,
    /// `List<Date32>`, item field `"item"` nullable.
    ListDate32,
    /// `List<Int32>`, item field `"item"` nullable.
    ListInt32,
    /// `List<Struct<…>>` with nullable members; item field `"item"` nullable.
    ListStruct(&'static [(&'static str, Member)]),
}

impl Ty {
    pub(crate) fn data_type(self) -> DataType {
        let item = |data_type: DataType| Arc::new(Field::new("item", data_type, true));
        match self {
            Ty::Utf8 => DataType::Utf8,
            Ty::Boolean => DataType::Boolean,
            Ty::UInt32 => DataType::UInt32,
            Ty::Int32 => DataType::Int32,
            Ty::Int64 => DataType::Int64,
            Ty::Date32 => DataType::Date32,
            Ty::TimestampMs => timestamp_millis_utc_type(),
            Ty::Binary => DataType::Binary,
            Ty::Dictionary => enum_data_type(),
            Ty::Decimal(family) => DataType::Decimal128(DECIMAL_PRECISION, family.scale() as i8),
            Ty::ListUtf8 => DataType::List(item(DataType::Utf8)),
            Ty::ListDate32 => DataType::List(item(DataType::Date32)),
            Ty::ListInt32 => DataType::List(item(DataType::Int32)),
            Ty::ListStruct(members) => {
                DataType::List(item(DataType::Struct(struct_fields(members))))
            }
        }
    }
}

/// The fields of a `List<Struct>` member list.
pub(crate) fn struct_fields(members: &[(&'static str, Member)]) -> Fields {
    members
        .iter()
        .map(|(name, member)| {
            let data_type = match member {
                Member::Utf8 => DataType::Utf8,
                Member::Date32 => DataType::Date32,
            };
            Field::new(*name, data_type, true)
        })
        .collect()
}

/// One column of a table.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Col {
    pub name: &'static str,
    pub ty: Ty,
    pub nullable: bool,
    /// Schema reference description, starting with the source path.
    pub doc: &'static str,
}

impl Col {
    pub(crate) const fn new(name: &'static str, ty: Ty, nullable: bool, doc: &'static str) -> Self {
        Self {
            name,
            ty,
            nullable,
            doc,
        }
    }

    pub(crate) fn field(&self) -> Field {
        Field::new(self.name, self.ty.data_type(), self.nullable)
    }
}

/// One table.
#[derive(Clone, Copy, Debug)]
pub(crate) struct TableSpec {
    pub name: &'static str,
    /// Schema reference description of the table.
    pub doc: &'static str,
    /// Whether the 5 filing-context columns [`FC_COLUMNS`] follow the canonical
    /// columns (every table except `blocks`, `filings` and `parse_issues`).
    pub filing_context: bool,
    /// The table's own columns, after the canonical and filing-context columns.
    pub cols: &'static [Col],
}

impl TableSpec {
    /// Every column after the canonical ones, without `fork_step` and
    /// `stream_ordinal`.
    pub(crate) fn columns(&self) -> impl Iterator<Item = &'static Col> {
        let context: &'static [Col] = if self.filing_context {
            &FC_COLUMNS
        } else {
            &[]
        };
        context.iter().chain(self.cols.iter())
    }
}

/// The filing context [FC] (§3.0) copied onto every filing-derived table.
pub(crate) const FC_COLUMNS: [Col; 5] = [
    Col::new(
        "filing_index",
        Ty::UInt32,
        false,
        "`Filing.ordinal`: the filing's 0-based position in the block; with `block_num`, the \
         filing key of `filings`.",
    ),
    Col::new(
        "accession_number",
        Ty::Utf8,
        false,
        "`Filing.accession_number`: the EDGAR accession, verbatim. Not unique across blocks \
         (re-dissemination): use the `sec_filings_first` view.",
    ),
    Col::new(
        "form_type",
        Ty::Utf8,
        false,
        "`Filing.form_type`: the EDGAR form type, verbatim (`4/A`).",
    ),
    Col::new(
        "filing_date",
        Ty::Date32,
        true,
        "`Filing.filing_date`: a copy of `filings.filing_date` (date, §4.2).",
    ),
    Col::new(
        "acceptance_datetime",
        Ty::TimestampMs,
        true,
        "`Filing.acceptance_datetime`: a copy of `filings.acceptance_datetime` (UTC).",
    ),
];

pub(crate) const BLOCKS: &str = "blocks";
pub(crate) const FILINGS: &str = "filings";
pub(crate) const FILING_RAW_XML: &str = "filing_raw_xml";
pub(crate) const FILING_PARTIES: &str = "filing_parties";
pub(crate) const FILING_DOCUMENTS: &str = "filing_documents";
pub(crate) const FILING_SERIES: &str = "filing_series";
pub(crate) const FILING_SERIES_CLASSES: &str = "filing_series_classes";
pub(crate) const FILING_SIGNATURES: &str = "filing_signatures";
pub(crate) const OWNERSHIP_DOCUMENTS: &str = "ownership_documents";
pub(crate) const OWNERSHIP_REPORTING_OWNERS: &str = "ownership_reporting_owners";
pub(crate) const OWNERSHIP_TRANSACTIONS: &str = "ownership_transactions";
pub(crate) const OWNERSHIP_HOLDINGS: &str = "ownership_holdings";
pub(crate) const OWNERSHIP_FOOTNOTES: &str = "ownership_footnotes";
pub(crate) const FORM13F_REPORTS: &str = "form13f_reports";
pub(crate) const FORM13F_OTHER_MANAGERS: &str = "form13f_other_managers";
pub(crate) const FORM13F_HOLDINGS: &str = "form13f_holdings";
pub(crate) const BENEFICIAL_REPORTS: &str = "beneficial_reports";
pub(crate) const BENEFICIAL_REPORTING_PERSONS: &str = "beneficial_reporting_persons";
pub(crate) const FORM144_NOTICES: &str = "form144_notices";
pub(crate) const FORM144_SECURITIES_INFORMATION: &str = "form144_securities_information";
pub(crate) const FORM144_SECURITIES_TO_BE_SOLD: &str = "form144_securities_to_be_sold";
pub(crate) const FORM144_SALES_PAST_3_MONTHS: &str = "form144_sales_past_3_months";
pub(crate) const NPORT_REPORTS: &str = "nport_reports";
pub(crate) const NPORT_MONTHLY_RETURNS: &str = "nport_monthly_returns";
pub(crate) const NPORT_MONTHLY_ACTIVITY: &str = "nport_monthly_activity";
pub(crate) const NPORT_HOLDINGS: &str = "nport_holdings";
pub(crate) const NPORT_DEBT_REFERENCE_INSTRUMENTS: &str = "nport_debt_reference_instruments";
pub(crate) const NPORT_DEBT_CONVERSION_CURRENCIES: &str = "nport_debt_conversion_currencies";
pub(crate) const NPORT_DERIVATIVES: &str = "nport_derivatives";
pub(crate) const NPORT_DERIVATIVE_SWAP_LEGS: &str = "nport_derivative_swap_legs";
pub(crate) const NPORT_DERIVATIVE_INDEX_COMPONENTS: &str = "nport_derivative_index_components";
pub(crate) const FORM_D_NOTICES: &str = "form_d_notices";
pub(crate) const FORM_D_CO_ISSUERS: &str = "form_d_co_issuers";
pub(crate) const FORM_D_RELATED_PERSONS: &str = "form_d_related_persons";
pub(crate) const FORM_D_SALES_RECIPIENTS: &str = "form_d_sales_recipients";
pub(crate) const NPX_REPORTS: &str = "npx_reports";
pub(crate) const NPX_VOTES: &str = "npx_votes";
pub(crate) const NPX_VOTE_RECORDS: &str = "npx_vote_records";
pub(crate) const NPX_OTHER_MANAGERS: &str = "npx_other_managers";
pub(crate) const NCEN_REPORTS: &str = "ncen_reports";
pub(crate) const FORM_C_NOTICES: &str = "form_c_notices";
pub(crate) const FORM_C_CO_ISSUERS: &str = "form_c_co_issuers";
pub(crate) const PARSE_ISSUES: &str = "parse_issues";

/// Every table, in the §2 order: `blocks` first, `parse_issues` last.
pub const TABLE_NAMES: [&str; 43] = [
    BLOCKS,
    FILINGS,
    FILING_RAW_XML,
    FILING_PARTIES,
    FILING_DOCUMENTS,
    FILING_SERIES,
    FILING_SERIES_CLASSES,
    FILING_SIGNATURES,
    OWNERSHIP_DOCUMENTS,
    OWNERSHIP_REPORTING_OWNERS,
    OWNERSHIP_TRANSACTIONS,
    OWNERSHIP_HOLDINGS,
    OWNERSHIP_FOOTNOTES,
    FORM13F_REPORTS,
    FORM13F_OTHER_MANAGERS,
    FORM13F_HOLDINGS,
    BENEFICIAL_REPORTS,
    BENEFICIAL_REPORTING_PERSONS,
    FORM144_NOTICES,
    FORM144_SECURITIES_INFORMATION,
    FORM144_SECURITIES_TO_BE_SOLD,
    FORM144_SALES_PAST_3_MONTHS,
    NPORT_REPORTS,
    NPORT_MONTHLY_RETURNS,
    NPORT_MONTHLY_ACTIVITY,
    NPORT_HOLDINGS,
    NPORT_DEBT_REFERENCE_INSTRUMENTS,
    NPORT_DEBT_CONVERSION_CURRENCIES,
    NPORT_DERIVATIVES,
    NPORT_DERIVATIVE_SWAP_LEGS,
    NPORT_DERIVATIVE_INDEX_COMPONENTS,
    FORM_D_NOTICES,
    FORM_D_CO_ISSUERS,
    FORM_D_RELATED_PERSONS,
    FORM_D_SALES_RECIPIENTS,
    NPX_REPORTS,
    NPX_VOTES,
    NPX_VOTE_RECORDS,
    NPX_OTHER_MANAGERS,
    NCEN_REPORTS,
    FORM_C_NOTICES,
    FORM_C_CO_ISSUERS,
    PARSE_ISSUES,
];

/// Every table spec, in [`TABLE_NAMES`] order.
pub(crate) const TABLES: [&TableSpec; 43] = [
    &envelope::BLOCKS,
    &envelope::FILINGS,
    &envelope::FILING_RAW_XML,
    &envelope::FILING_PARTIES,
    &envelope::FILING_DOCUMENTS,
    &envelope::FILING_SERIES,
    &envelope::FILING_SERIES_CLASSES,
    &envelope::FILING_SIGNATURES,
    &ownership::OWNERSHIP_DOCUMENTS,
    &ownership::OWNERSHIP_REPORTING_OWNERS,
    &ownership::OWNERSHIP_TRANSACTIONS,
    &ownership::OWNERSHIP_HOLDINGS,
    &ownership::OWNERSHIP_FOOTNOTES,
    &form13f::FORM13F_REPORTS,
    &form13f::FORM13F_OTHER_MANAGERS,
    &form13f::FORM13F_HOLDINGS,
    &beneficial::BENEFICIAL_REPORTS,
    &beneficial::BENEFICIAL_REPORTING_PERSONS,
    &form144::FORM144_NOTICES,
    &form144::FORM144_SECURITIES_INFORMATION,
    &form144::FORM144_SECURITIES_TO_BE_SOLD,
    &form144::FORM144_SALES_PAST_3_MONTHS,
    &nport::NPORT_REPORTS,
    &nport::NPORT_MONTHLY_RETURNS,
    &nport::NPORT_MONTHLY_ACTIVITY,
    &nport::NPORT_HOLDINGS,
    &nport::NPORT_DEBT_REFERENCE_INSTRUMENTS,
    &nport::NPORT_DEBT_CONVERSION_CURRENCIES,
    &nport::NPORT_DERIVATIVES,
    &nport::NPORT_DERIVATIVE_SWAP_LEGS,
    &nport::NPORT_DERIVATIVE_INDEX_COMPONENTS,
    &formd::FORM_D_NOTICES,
    &formd::FORM_D_CO_ISSUERS,
    &formd::FORM_D_RELATED_PERSONS,
    &formd::FORM_D_SALES_RECIPIENTS,
    &npx::NPX_REPORTS,
    &npx::NPX_VOTES,
    &npx::NPX_VOTE_RECORDS,
    &npx::NPX_OTHER_MANAGERS,
    &ncen::NCEN_REPORTS,
    &formc::FORM_C_NOTICES,
    &formc::FORM_C_CO_ISSUERS,
    &issues::PARSE_ISSUES,
];

/// The spec of `table`.
///
/// # Panics
/// If `table` is not one of [`TABLE_NAMES`].
pub(crate) fn spec(table: &str) -> &'static TableSpec {
    TABLES
        .iter()
        .find(|spec| spec.name == table)
        .copied()
        .unwrap_or_else(|| panic!("sec: unknown table `{table}`"))
}

/// The Arrow schema of `table`: canonical columns, the table's columns, then
/// `fork_step` and `stream_ordinal` when `include_fork_step`.
///
/// # Panics
/// If `table` is not one of [`TABLE_NAMES`].
pub fn table_schema(table: &str, include_fork_step: bool, encoding: &EncodeBytes) -> Schema {
    let spec = spec(table);
    let mut fields = canonical_fields_with_encoding(encoding);
    fields.extend(spec.columns().map(Col::field));
    push_fork_step_field(&mut fields, include_fork_step);
    Schema::new(fields)
}

/// `(table, description)` for the schema reference, in [`TABLE_NAMES`] order.
pub fn table_descriptions() -> impl Iterator<Item = (&'static str, &'static str)> {
    TABLES.iter().map(|spec| (spec.name, spec.doc))
}

/// `(table, column, description)` for the schema reference: every column of
/// every table, filing-context columns included.
pub fn column_descriptions() -> impl Iterator<Item = (&'static str, &'static str, &'static str)> {
    TABLES.iter().flat_map(|spec| {
        spec.columns()
            .map(move |col| (spec.name, col.name, col.doc))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    #[test]
    fn table_specs_follow_table_names() {
        let names: Vec<&str> = TABLES.iter().map(|spec| spec.name).collect();
        assert_eq!(names, TABLE_NAMES);
        assert_eq!(TABLE_NAMES[0], BLOCKS);
        assert_eq!(TABLE_NAMES[42], PARSE_ISSUES);
        assert_eq!(TABLE_NAMES.iter().collect::<BTreeSet<_>>().len(), 43);
    }

    #[test]
    fn filing_context_is_on_every_table_but_the_hub_and_parse_issues() {
        for spec in TABLES {
            assert_eq!(
                spec.filing_context,
                ![BLOCKS, FILINGS, PARSE_ISSUES].contains(&spec.name),
                "{}",
                spec.name
            );
        }
    }

    #[test]
    fn columns_are_unique_documented_and_avoid_reserved_names() {
        const RESERVED: [&str; 12] = [
            "date",
            "timestamp",
            "block_num",
            "block_id",
            "parent_num",
            "parent_id",
            "lib_num",
            "fork_step",
            "stream_ordinal",
            "signature",
            "address",
            "owner",
        ];
        // Virtual columns DuckDB's `read_parquet`/`delta_scan` add on request
        // (`filename = true`, …). A data column of that name shadows them, so
        // it is allowed only with the reader workaround in its description:
        // `filing_documents.filename` is the spec's name for the document.
        const READER_COLUMNS: [&str; 3] = ["filename", "file_row_number", "file_index"];
        for spec in TABLES {
            assert!(!spec.doc.is_empty(), "{}", spec.name);
            let mut seen = BTreeSet::new();
            for col in spec.columns() {
                assert!(seen.insert(col.name), "{}.{} twice", spec.name, col.name);
                assert!(!RESERVED.contains(&col.name), "{}.{}", spec.name, col.name);
                if READER_COLUMNS.contains(&col.name) {
                    assert!(
                        (spec.name, col.name) == ("filing_documents", "filename")
                            && col.doc.contains("Shadows DuckDB's `filename` scan column")
                            && col.doc.contains("filename = 'data_file'"),
                        "{}.{} shadows a DuckDB scan column",
                        spec.name,
                        col.name
                    );
                }
                assert!(!col.doc.is_empty(), "{}.{}", spec.name, col.name);
                assert!(
                    col.name
                        .bytes()
                        .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_'),
                    "{}.{}",
                    spec.name,
                    col.name
                );
            }
        }
    }

    #[test]
    fn no_native_decimal_is_a_profile_decimal() {
        for spec in TABLES {
            for col in spec.columns() {
                if let DataType::Decimal128(precision, scale) = col.ty.data_type() {
                    assert_eq!(precision, 38, "{}.{}", spec.name, col.name);
                    assert!(scale >= 2, "{}.{}", spec.name, col.name);
                }
            }
        }
    }

    #[test]
    fn schemas_start_canonical_and_end_with_fork_step() {
        for name in TABLE_NAMES {
            let final_only = table_schema(name, false, &EncodeBytes::Hex);
            let non_final = table_schema(name, true, &EncodeBytes::Hex);
            assert_eq!(non_final.fields().len(), final_only.fields().len() + 2);
            assert_eq!(final_only.field(0).name(), "block_num");
            assert_eq!(final_only.field(6).name(), "date");
            let n = non_final.fields().len();
            assert_eq!(non_final.field(n - 2).name(), "fork_step");
            assert_eq!(non_final.field(n - 1).name(), "stream_ordinal");
        }
    }
}

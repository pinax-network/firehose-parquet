//! Append phase of `filing_raw_xml`, `filing_parties`, `filing_documents`, `filing_series`, `filing_series_classes`, `filing_signatures` (§3.3, §3.4, §3.5, §3.6, §3.7, §3.8).
//! Owned by the `envelope` group: see `/tmp/sec-fireparq/impl/contracts.md`.
//!
//! The column structs below are generated from `super::super::schema` and checked
//! against it on construction; edit them only together with the schema.

use firehose_parquet::encode::EncodeBytes;

use super::{sec_columns, AppendCtx, SecTable, Table};
#[allow(unused_imports)]
use super::{Addr, Bin, Bool, Date, Dict, Fc, ListStruct, Str, U32};
use crate::sec::prepare::envelope::PreparedEnvelope;
use crate::sec::prepare::FilingCtx;
use crate::sec::proto::sec;
use crate::sec::schema;

sec_columns! {
    /// The columns of `filing_raw_xml` (§3.3), in schema order.
    pub(crate) struct FilingRawXmlCols {
        /// [FC] `filing_index` … `acceptance_datetime`.
        pub fc: Fc,
        pub primary_document: Str,
        pub raw_xml: Bin,
    }
}

sec_columns! {
    /// The columns of `filing_parties` (§3.4), in schema order.
    pub(crate) struct FilingPartiesCols {
        /// [FC] `filing_index` … `acceptance_datetime`.
        pub fc: Fc,
        pub party_index: U32,
        pub role: Str,
        pub cik: Str,
        pub name: Str,
        pub assigned_sic: Str,
        pub organization_name: Str,
        pub irs_number: Str,
        pub state_of_incorporation: Str,
        pub fiscal_year_end: Str,
        pub lei: Str,
        pub party_form_type: Str,
        pub act: Str,
        pub file_number: Str,
        pub film_number: Str,
        /// `business_street1` … `business_non_us_state_territory`.
        pub business: Addr,
        pub business_phone: Str,
        /// `mail_street1` … `mail_non_us_state_territory`.
        pub mail: Addr,
        pub former_names: ListStruct,
        pub has_parse_issues: Bool,
    }
}

sec_columns! {
    /// The columns of `filing_documents` (§3.5), in schema order.
    pub(crate) struct FilingDocumentsCols {
        /// [FC] `filing_index` … `acceptance_datetime`.
        pub fc: Fc,
        pub document_index: U32,
        pub sequence: Str,
        pub document_type: Str,
        pub filename: Str,
        pub description: Str,
    }
}

sec_columns! {
    /// The columns of `filing_series` (§3.6), in schema order.
    pub(crate) struct FilingSeriesCols {
        /// [FC] `filing_index` … `acceptance_datetime`.
        pub fc: Fc,
        pub series_index: U32,
        pub owner_cik: Str,
        pub series_id: Str,
        pub series_name: Str,
        pub status: Str,
        pub class_count: U32,
    }
}

sec_columns! {
    /// The columns of `filing_series_classes` (§3.7), in schema order.
    pub(crate) struct FilingSeriesClassesCols {
        /// [FC] `filing_index` … `acceptance_datetime`.
        pub fc: Fc,
        pub series_index: U32,
        pub class_index: U32,
        pub series_id: Str,
        pub class_id: Str,
        pub class_name: Str,
        pub ticker_symbol: Str,
    }
}

sec_columns! {
    /// The columns of `filing_signatures` (§3.8), in schema order.
    pub(crate) struct FilingSignaturesCols {
        /// [FC] `filing_index` … `acceptance_datetime`.
        pub fc: Fc,
        pub signature_index: U32,
        pub signature_source: Dict,
        pub signed_for: Str,
        pub signer_name: Str,
        pub signature_text: Str,
        pub title: Str,
        pub phone: Str,
        pub city: Str,
        pub state: Str,
        pub signature_date: Date,
        pub has_parse_issues: Bool,
    }
}

/// Every table of this module.
pub(crate) struct EnvelopeTables {
    pub(crate) filing_raw_xml: Table<FilingRawXmlCols>,
    pub(crate) filing_parties: Table<FilingPartiesCols>,
    pub(crate) filing_documents: Table<FilingDocumentsCols>,
    pub(crate) filing_series: Table<FilingSeriesCols>,
    pub(crate) filing_series_classes: Table<FilingSeriesClassesCols>,
    pub(crate) filing_signatures: Table<FilingSignaturesCols>,
}

impl EnvelopeTables {
    pub(crate) fn new(include_fork_step: bool, encoding: &EncodeBytes) -> Self {
        Self {
            filing_raw_xml: Table::new(schema::FILING_RAW_XML, include_fork_step, encoding),
            filing_parties: Table::new(schema::FILING_PARTIES, include_fork_step, encoding),
            filing_documents: Table::new(schema::FILING_DOCUMENTS, include_fork_step, encoding),
            filing_series: Table::new(schema::FILING_SERIES, include_fork_step, encoding),
            filing_series_classes: Table::new(
                schema::FILING_SERIES_CLASSES,
                include_fork_step,
                encoding,
            ),
            filing_signatures: Table::new(schema::FILING_SIGNATURES, include_fork_step, encoding),
        }
    }

    /// Append the envelope child rows of one filing, from its proto message and the
    /// values prepared by `crate::sec::prepare::envelope::prepare`. Infallible.
    pub(crate) fn append(
        &mut self,
        ctx: &AppendCtx<'_>,
        fc: &FilingCtx<'_>,
        filing: &sec::Filing,
        prepared: &PreparedEnvelope<'_>,
    ) {
        // Stub: no rows yet.
        let _ = (ctx, fc, filing, prepared);
    }

    pub(crate) fn tables(&self) -> [&dyn SecTable; 6] {
        [
            &self.filing_raw_xml,
            &self.filing_parties,
            &self.filing_documents,
            &self.filing_series,
            &self.filing_series_classes,
            &self.filing_signatures,
        ]
    }

    pub(crate) fn tables_mut(&mut self) -> [&mut dyn SecTable; 6] {
        [
            &mut self.filing_raw_xml,
            &mut self.filing_parties,
            &mut self.filing_documents,
            &mut self.filing_series,
            &mut self.filing_series_classes,
            &mut self.filing_signatures,
        ]
    }
}

//! Append phase of `filing_raw_xml`, `filing_parties`, `filing_documents`, `filing_series`, `filing_series_classes`, `filing_signatures` (§3.3, §3.4, §3.5, §3.6, §3.7, §3.8).
//! Owned by the `envelope` group: see `/tmp/sec-fireparq/impl/contracts.md`.
//!
//! The column structs below are generated from `super::super::schema` and checked
//! against it on construction; edit them only together with the schema.

use arrow::array::StringBuilder;
use firehose_parquet::encode::EncodeBytes;

use super::{sec_columns, AppendCtx, SecTable, Table};
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
    ///
    /// Positions come from `(0u32..)`: preflight checked every list length
    /// with `idx`, so no position can exceed `u32`.
    pub(crate) fn append(
        &mut self,
        ctx: &AppendCtx<'_>,
        fc: &FilingCtx<'_>,
        filing: &sec::Filing,
        prepared: &PreparedEnvelope<'_>,
    ) {
        // filing_raw_xml (§3.3): a row only when `raw_xml` is non-empty.
        if !filing.raw_xml.is_empty() {
            let row = self.filing_raw_xml.row(ctx);
            row.fc.append(fc);
            row.primary_document.nz(&filing.primary_document);
            row.raw_xml.val(&filing.raw_xml);
        }

        // filing_parties (§3.4).
        let mut former_name_dates = prepared.former_name_dates.iter().copied();
        for ((party_index, party), has_parse_issues) in (0u32..)
            .zip(&filing.parties)
            .zip(&prepared.party_has_parse_issues)
        {
            let row = self.filing_parties.row(ctx);
            row.fc.append(fc);
            row.party_index.val(party_index);
            row.role.nz(&party.role);
            row.cik.nz(&party.cik);
            row.name.nz(&party.name);
            row.assigned_sic.nz(&party.assigned_sic);
            row.organization_name.nz(&party.organization_name);
            row.irs_number.nz(&party.irs_number);
            row.state_of_incorporation.nz(&party.state_of_incorporation);
            row.fiscal_year_end.nz(&party.fiscal_year_end);
            row.lei.nz(&party.lei);
            row.party_form_type.nz(&party.form_type);
            row.act.nz(&party.act);
            row.file_number.nz(&party.file_number);
            row.film_number.nz(&party.film_number);
            row.business.append(party.business_address.as_ref());
            row.business_phone.nz(&party.business_phone);
            row.mail.append(party.mail_address.as_ref());
            for former in &party.former_names {
                append_nz(row.former_names.utf8(0), &former.name);
                row.former_names
                    .date(1)
                    .append_option(former_name_dates.next().flatten());
                row.former_names.end_item();
            }
            row.former_names.end_row();
            row.has_parse_issues.val(*has_parse_issues);
        }

        // filing_documents (§3.5).
        for (document_index, document) in (0u32..).zip(&filing.documents) {
            let row = self.filing_documents.row(ctx);
            row.fc.append(fc);
            row.document_index.val(document_index);
            row.sequence.nz(&document.sequence);
            row.document_type.nz(&document.r#type);
            row.filename.nz(&document.filename);
            row.description.nz(&document.description);
        }

        // filing_series (§3.6), each followed by its classes (§3.7).
        for ((series_index, series), class_count) in
            (0u32..).zip(&filing.series).zip(&prepared.class_counts)
        {
            let row = self.filing_series.row(ctx);
            row.fc.append(fc);
            row.series_index.val(series_index);
            row.owner_cik.nz(&series.owner_cik);
            row.series_id.nz(&series.series_id);
            row.series_name.nz(&series.series_name);
            row.status.nz(&series.status);
            row.class_count.val(*class_count);

            for (class_index, class) in (0u32..).zip(&series.classes) {
                let row = self.filing_series_classes.row(ctx);
                row.fc.append(fc);
                row.series_index.val(series_index);
                row.class_index.val(class_index);
                // Copy of the parent `filing_series.series_id`.
                row.series_id.nz(&series.series_id);
                row.class_id.nz(&class.class_id);
                row.class_name.nz(&class.class_name);
                row.ticker_symbol.nz(&class.ticker_symbol);
            }
        }

        // filing_signatures (§3.8), in the prepared source order.
        for (signature_index, signature) in (0u32..).zip(&prepared.signatures) {
            let text = &signature.text;
            let row = self.filing_signatures.row(ctx);
            row.fc.append(fc);
            row.signature_index.val(signature_index);
            row.signature_source.val(signature.source);
            row.signed_for.nz(text.signed_for);
            row.signer_name.nz(text.signer_name);
            row.signature_text.nz(text.signature_text);
            row.title.nz(text.title);
            row.phone.nz(text.phone);
            row.city.nz(text.city);
            row.state.nz(text.state);
            row.signature_date.opt(signature.signature_date);
            row.has_parse_issues.val(signature.has_parse_issues);
        }
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

/// A `Utf8` `List<Struct>` member: `""` → NULL, otherwise verbatim.
fn append_nz(builder: &mut StringBuilder, value: &str) {
    if value.is_empty() {
        builder.append_null();
    } else {
        builder.append_value(value);
    }
}

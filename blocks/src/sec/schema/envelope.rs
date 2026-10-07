//! Table specs generated from the final specification §3 (`final-spec.md`).
//! Edit only to fix a divergence from the specification; regenerate
//! `docs/schemas/sec.md` and re-pin the chain schema digests afterwards.

#[allow(unused_imports)]
use super::{Col, Family, Member, TableSpec, Ty};

/// §3.1 `blocks`.
pub(crate) const BLOCKS: TableSpec = TableSpec {
    name: super::BLOCKS,
    doc: "One row per 10-minute window. The only table with a row for empty windows; commits last.",
    filing_context: false,
    cols: &[
        Col::new(
            "feed_date",
            Ty::Date32,
            true,
            "`Block.header.feed_date`: date (§4.2). The producer's feed day; equal to the `date` partition by construction.",
        ),
        Col::new(
            "filing_count",
            Ty::UInt32,
            false,
            "Derived from `Block.filings[]`: length of the list. 0 for ~39% of windows.",
        ),
        Col::new(
            "has_parse_issues",
            Ty::Boolean,
            false,
            "Derived: true iff this row wrote ≥1 `parse_issues` row (§4.6).",
        ),
    ],
};

/// §3.2 `filings`.
pub(crate) const FILINGS: TableSpec = TableSpec {
    name: super::FILINGS,
    doc: "One row per filing: the envelope, resolved parties and dissemination flags. Every other table joins here on `(block_num, filing_index)`.",
    filing_context: false,
    cols: &[
        Col::new(
            "filing_index",
            Ty::UInt32,
            false,
            "`Filing.ordinal`: u32::try_from (overflow = structural error, §4.7). With `block_num`, the filing key.",
        ),
        Col::new(
            "accession_number",
            Ty::Utf8,
            false,
            "`Filing.accession_number`: verbatim. EDGAR id; NOT unique across blocks (re-dissemination).",
        ),
        Col::new(
            "form_type",
            Ty::Utf8,
            false,
            "`Filing.form_type`: verbatim (always present). `4`, `13F-HR/A`, `SCHEDULE 13G`, … (26 values in the sample).",
        ),
        Col::new(
            "base_form_type",
            Ty::Utf8,
            false,
            "Derived from `Filing.form_type`: form_type with one trailing `/A` removed. Groups originals with their amendments.",
        ),
        Col::new(
            "is_amendment",
            Ty::Boolean,
            false,
            "`Filing.is_amendment`: proto bool (false = false or absent). = form_type ends with `/A` (100% in the sample).",
        ),
        Col::new(
            "body_kind",
            Ty::Dictionary,
            true,
            "`Filing.body (oneof)`: name of the populated `body` member. `ownership` `form13f` `beneficial` `raw` `form144` `nport` `form_d` `npx` `ncen` `form_c`; NULL if unset (never seen).",
        ),
        Col::new(
            "cik",
            Ty::Utf8,
            true,
            "`Filing.cik`. Header party CIK (10-pad); its party is named by `cik_role`.",
        ),
        Col::new(
            "cik_role",
            Ty::Utf8,
            true,
            "`Filing.cik_role`. `ISSUER` (3/4/5), `SUBJECT-COMPANY` (13D/G, 144), `FILER` (13F, N-PORT, N-PX, N-CEN, D, C).",
        ),
        Col::new(
            "company_name",
            Ty::Utf8,
            true,
            "`Filing.company_name`. Conformed name of the `cik` party.",
        ),
        Col::new(
            "issuer_cik",
            Ty::Utf8,
            true,
            "Derived from `Filing.parties[]`: party rule (§4.5). Company the filing is about; NULL for 13F/N-PORT/N-PX/N-CEN.",
        ),
        Col::new(
            "issuer_name",
            Ty::Utf8,
            true,
            "Derived from `Filing.parties[]`: party rule (§4.5).",
        ),
        Col::new(
            "filer_cik",
            Ty::Utf8,
            true,
            "Derived from `Filing.parties[]`: party rule (§4.5). First REPORTING-OWNER / FILED-BY / FILER party (header order).",
        ),
        Col::new(
            "filer_name",
            Ty::Utf8,
            true,
            "Derived from `Filing.parties[]`: party rule (§4.5).",
        ),
        Col::new(
            "filing_date",
            Ty::Date32,
            true,
            "`Filing.filing_date`: date (§4.2). Legal filing date; can be years before `date` (re-dissemination).",
        ),
        Col::new(
            "period_of_report",
            Ty::Date32,
            true,
            "`Filing.period_of_report`: date (§4.2). Absent on 144, D, C, 13D/G.",
        ),
        Col::new(
            "acceptance_datetime",
            Ty::TimestampMs,
            true,
            "`Filing.acceptance_datetime`: protobuf Timestamp → ms (checked; out of range = structural error). EDGAR acceptance (UTC, whole seconds); may fall outside the block window.",
        ),
        Col::new(
            "acceptance_in_block_window",
            Ty::Boolean,
            true,
            "Derived from `Filing.acceptance_datetime`: `timestamp ≤ acceptance_datetime < timestamp + 10 min`; NULL if acceptance NULL. False for the ~5% clamped into window 0 or 143.",
        ),
        Col::new(
            "dissemination_lag_days",
            Ty::Int32,
            true,
            "Derived from `Filing.filing_date`: `date − filing_date` in days; NULL if filing_date NULL. 0–4 normally; years for re-disseminated filings.",
        ),
        Col::new(
            "primary_document",
            Ty::Utf8,
            true,
            "`Filing.primary_document`. First `.xml` document, not necessarily SEC's primary document.",
        ),
        Col::new(
            "amended_accession",
            Ty::Utf8,
            true,
            "`Filing.amended_accession`. Accession this amendment amends (Form D/A, 13D/G/A, 144/A, N-CEN/A); NULL otherwise.",
        ),
        Col::new(
            "source_path",
            Ty::Utf8,
            true,
            "`Filing.source_path`. `<feed>.gz!<accession>`.",
        ),
        Col::new(
            "dissemination_flags",
            Ty::ListUtf8,
            false,
            "`Filing.dissemination_flags[]`: verbatim items in document order; [] when empty. `CORRECTION`, `DELETION`, `PAPER`, `CONFIRMING-COPY`, `PRIVATE-TO-PUBLIC`.",
        ),
        Col::new(
            "dissemination_timestamp",
            Ty::Utf8,
            true,
            "`Filing.dissemination_timestamp`. Raw `YYYYMMDD:HHMMSS`, US-Eastern; not typed (§4.2).",
        ),
        Col::new(
            "is_deletion_notice",
            Ty::Boolean,
            false,
            "Derived from `Filing.dissemination_flags[]`: `DELETION` ∈ dissemination_flags. EDGAR notice that `accession_number` was deleted; body is `raw` with reason `deletion`.",
        ),
        Col::new(
            "group_members",
            Ty::ListUtf8,
            false,
            "`Filing.group_members[]`: verbatim items in document order; [] when empty. Legacy SC 13D/G `<GROUP-MEMBERS>` lines.",
        ),
        Col::new(
            "party_count",
            Ty::UInt32,
            false,
            "Derived from `Filing.parties[]`: length of the list.",
        ),
        Col::new(
            "document_count",
            Ty::UInt32,
            false,
            "Derived from `Filing.documents[]`: length of the list.",
        ),
        Col::new(
            "series_count",
            Ty::UInt32,
            false,
            "Derived from `Filing.series[]`: length of the list.",
        ),
        Col::new(
            "raw_reason",
            Ty::Utf8,
            true,
            "`Filing.body.raw.reason`. `no_xml`, `legacy_html`, `parse_error`, `deletion`, `unsupported_form`; NULL unless body_kind = `raw`.",
        ),
        Col::new(
            "raw_detail",
            Ty::Utf8,
            true,
            "`Filing.body.raw.detail`. Parser message for `parse_error`.",
        ),
        Col::new(
            "has_raw_xml",
            Ty::Boolean,
            false,
            "Derived from `Filing.raw_xml`: `raw_xml` non-empty. The filing has a `filing_raw_xml` row.",
        ),
        Col::new(
            "raw_xml_size",
            Ty::UInt32,
            false,
            "Derived from `Filing.raw_xml`: byte length of `raw_xml` (0 without `--include-raw`).",
        ),
        Col::new(
            "has_parse_issues",
            Ty::Boolean,
            false,
            "Derived: true iff this row wrote ≥1 `parse_issues` row (§4.6).",
        ),
    ],
};

/// §3.3 `filing_raw_xml`.
pub(crate) const FILING_RAW_XML: TableSpec = TableSpec {
    name: super::FILING_RAW_XML,
    doc: "Original primary-document XML. Gets rows only when the producer runs with `--include-raw`.",
    filing_context: true,
    cols: &[
        Col::new(
            "primary_document",
            Ty::Utf8,
            true,
            "Copy of `filings.primary_document`. The file the bytes came from.",
        ),
        Col::new(
            "raw_xml",
            Ty::Binary,
            false,
            "`Filing.raw_xml`: bytes verbatim (zero-copy `Bytes`). 13F: `primary_doc.xml`, not the information table.",
        ),
    ],
};

/// §3.4 `filing_parties`.
pub(crate) const FILING_PARTIES: TableSpec = TableSpec {
    name: super::FILING_PARTIES,
    doc: "Every party of the SGML submission header (0.13.0+): filer, filed-by, subject company, issuer and reporting owners, with SIC, state of incorporation, addresses and former names.",
    filing_context: true,
    cols: &[
        Col::new(
            "party_index",
            Ty::UInt32,
            false,
            "Derived: position in `parties`.",
        ),
        Col::new(
            "role",
            Ty::Utf8,
            true,
            "`Filing.parties[].role`. `FILER`, `FILED-BY`, `SUBJECT-COMPANY`, `ISSUER`, `REPORTING-OWNER`.",
        ),
        Col::new(
            "cik",
            Ty::Utf8,
            true,
            "`Filing.parties[].cik`. 10-pad.",
        ),
        Col::new(
            "name",
            Ty::Utf8,
            true,
            "`Filing.parties[].name`. Conformed name.",
        ),
        Col::new(
            "assigned_sic",
            Ty::Utf8,
            true,
            "`Filing.parties[].assigned_sic`. 4-digit SIC code as text (`0000` = none assigned).",
        ),
        Col::new(
            "organization_name",
            Ty::Utf8,
            true,
            "`Filing.parties[].organization_name`. SEC review office, e.g. `03 Life Sciences`.",
        ),
        Col::new(
            "irs_number",
            Ty::Utf8,
            true,
            "`Filing.parties[].irs_number`.",
        ),
        Col::new(
            "state_of_incorporation",
            Ty::Utf8,
            true,
            "`Filing.parties[].state_of_incorporation`. EDGAR state/country code.",
        ),
        Col::new(
            "fiscal_year_end",
            Ty::Utf8,
            true,
            "`Filing.parties[].fiscal_year_end`. Raw `MMDD`.",
        ),
        Col::new(
            "lei",
            Ty::Utf8,
            true,
            "`Filing.parties[].lei`.",
        ),
        Col::new(
            "party_form_type",
            Ty::Utf8,
            true,
            "`Filing.parties[].form_type`. First `<FILING-VALUES><FORM-TYPE>` (renamed: `form_type` is the filing context).",
        ),
        Col::new(
            "act",
            Ty::Utf8,
            true,
            "`Filing.parties[].act`.",
        ),
        Col::new(
            "file_number",
            Ty::Utf8,
            true,
            "`Filing.parties[].file_number`.",
        ),
        Col::new(
            "film_number",
            Ty::Utf8,
            true,
            "`Filing.parties[].film_number`.",
        ),
        Col::new(
            "business_street1",
            Ty::Utf8,
            true,
            "`Filing.parties[].business_address.street1`.",
        ),
        Col::new(
            "business_street2",
            Ty::Utf8,
            true,
            "`Filing.parties[].business_address.street2`.",
        ),
        Col::new(
            "business_city",
            Ty::Utf8,
            true,
            "`Filing.parties[].business_address.city`.",
        ),
        Col::new(
            "business_state",
            Ty::Utf8,
            true,
            "`Filing.parties[].business_address.state`. EDGAR state/country code.",
        ),
        Col::new(
            "business_zip_code",
            Ty::Utf8,
            true,
            "`Filing.parties[].business_address.zip_code`. Text (keeps leading zeros).",
        ),
        Col::new(
            "business_state_description",
            Ty::Utf8,
            true,
            "`Filing.parties[].business_address.state_description`. Filled only by Forms 3/4/5 and D.",
        ),
        Col::new(
            "business_country",
            Ty::Utf8,
            true,
            "`Filing.parties[].business_address.country`.",
        ),
        Col::new(
            "business_non_us_state_territory",
            Ty::Utf8,
            true,
            "`Filing.parties[].business_address.non_us_state_territory`.",
        ),
        Col::new(
            "business_phone",
            Ty::Utf8,
            true,
            "`Filing.parties[].business_phone`.",
        ),
        Col::new(
            "mail_street1",
            Ty::Utf8,
            true,
            "`Filing.parties[].mail_address.street1`.",
        ),
        Col::new(
            "mail_street2",
            Ty::Utf8,
            true,
            "`Filing.parties[].mail_address.street2`.",
        ),
        Col::new(
            "mail_city",
            Ty::Utf8,
            true,
            "`Filing.parties[].mail_address.city`.",
        ),
        Col::new(
            "mail_state",
            Ty::Utf8,
            true,
            "`Filing.parties[].mail_address.state`. EDGAR state/country code.",
        ),
        Col::new(
            "mail_zip_code",
            Ty::Utf8,
            true,
            "`Filing.parties[].mail_address.zip_code`. Text (keeps leading zeros).",
        ),
        Col::new(
            "mail_state_description",
            Ty::Utf8,
            true,
            "`Filing.parties[].mail_address.state_description`. Filled only by Forms 3/4/5 and D.",
        ),
        Col::new(
            "mail_country",
            Ty::Utf8,
            true,
            "`Filing.parties[].mail_address.country`.",
        ),
        Col::new(
            "mail_non_us_state_territory",
            Ty::Utf8,
            true,
            "`Filing.parties[].mail_address.non_us_state_territory`.",
        ),
        Col::new(
            "former_names",
            Ty::ListStruct(&[("name", Member::Utf8), ("date_changed", Member::Date32)]),
            false,
            "`Filing.parties[].former_names[]`: one struct per element {name ← .name ('' → NULL), date_changed ← .date_changed, date (§4.2)}; [] when empty. Header order; issues use `former_names.date_changed`.",
        ),
        Col::new(
            "has_parse_issues",
            Ty::Boolean,
            false,
            "Derived: true iff this row wrote ≥1 `parse_issues` row (§4.6).",
        ),
    ],
};

/// §3.5 `filing_documents`.
pub(crate) const FILING_DOCUMENTS: TableSpec = TableSpec {
    name: super::FILING_DOCUMENTS,
    doc: "Every document of the submission, including non-XML exhibits (bodies are not carried).",
    filing_context: true,
    cols: &[
        Col::new(
            "document_index",
            Ty::UInt32,
            false,
            "Derived: position in `documents`.",
        ),
        Col::new(
            "sequence",
            Ty::Utf8,
            true,
            "`Filing.documents[].sequence`. SGML `<SEQUENCE>`, text.",
        ),
        Col::new(
            "document_type",
            Ty::Utf8,
            true,
            "`Filing.documents[].type`. `4`, `INFORMATION TABLE`, `EX-99.1`, `GRAPHIC`… (proto `type`).",
        ),
        Col::new(
            "filename",
            Ty::Utf8,
            true,
            "`Filing.documents[].filename`.",
        ),
        Col::new(
            "description",
            Ty::Utf8,
            true,
            "`Filing.documents[].description`.",
        ),
    ],
};

/// §3.6 `filing_series`.
pub(crate) const FILING_SERIES: TableSpec = TableSpec {
    name: super::FILING_SERIES,
    doc: "Fund series named in the header of N-PORT, N-PX and N-CEN filings: the series-id ↔ name registry.",
    filing_context: true,
    cols: &[
        Col::new(
            "series_index",
            Ty::UInt32,
            false,
            "Derived: position in `series`.",
        ),
        Col::new(
            "owner_cik",
            Ty::Utf8,
            true,
            "`Filing.series[].owner_cik`. 10-pad.",
        ),
        Col::new(
            "series_id",
            Ty::Utf8,
            true,
            "`Filing.series[].series_id`. `S000…`.",
        ),
        Col::new(
            "series_name",
            Ty::Utf8,
            true,
            "`Filing.series[].series_name`.",
        ),
        Col::new(
            "status",
            Ty::Utf8,
            true,
            "`Filing.series[].status`. Enclosing block name, e.g. `EXISTING-SERIES-AND-CLASSES-CONTRACTS`.",
        ),
        Col::new(
            "class_count",
            Ty::UInt32,
            false,
            "Derived from `Filing.series[].classes[]`: length of the list.",
        ),
    ],
};

/// §3.7 `filing_series_classes`.
pub(crate) const FILING_SERIES_CLASSES: TableSpec = TableSpec {
    name: super::FILING_SERIES_CLASSES,
    doc: "Share classes and their tickers: the ticker → class → series bridge to N-PORT, N-PX and N-CEN.",
    filing_context: true,
    cols: &[
        Col::new(
            "series_index",
            Ty::UInt32,
            false,
            "Derived: position of the parent series.",
        ),
        Col::new(
            "class_index",
            Ty::UInt32,
            false,
            "Derived: position in `classes`.",
        ),
        Col::new(
            "series_id",
            Ty::Utf8,
            true,
            "Copy of `filing_series.series_id`.",
        ),
        Col::new(
            "class_id",
            Ty::Utf8,
            true,
            "`Filing.series[].classes[].class_id`. `C000…`.",
        ),
        Col::new(
            "class_name",
            Ty::Utf8,
            true,
            "`Filing.series[].classes[].class_name`.",
        ),
        Col::new(
            "ticker_symbol",
            Ty::Utf8,
            true,
            "`Filing.series[].classes[].ticker_symbol`. Fund share-class ticker (resolves a fund ticker to `series_id`).",
        ),
    ],
};

/// §3.8 `filing_signatures`.
pub(crate) const FILING_SIGNATURES: TableSpec = TableSpec {
    name: super::FILING_SIGNATURES,
    doc: "Every signature block of every form in one table (Form 144's notice signature stays on `form144_notices`).",
    filing_context: true,
    cols: &[
        Col::new(
            "signature_index",
            Ty::UInt32,
            false,
            "Derived: 0-based position over the filing's signature messages, in the source order of §3.8.",
        ),
        Col::new(
            "signature_source",
            Ty::Dictionary,
            false,
            "Derived: which proto message the row came from (§3.8). `ownership`, `form13f`, `beneficial`, `nport`, `form_d`, `npx`, `form_c_issuer`, `form_c_person`.",
        ),
        Col::new(
            "signed_for",
            Ty::Utf8,
            true,
            "Per signature source (§3.8). The entity or person the signature is given for.",
        ),
        Col::new(
            "signer_name",
            Ty::Utf8,
            true,
            "Per signature source (§3.8). Printed name of the signer.",
        ),
        Col::new(
            "signature_text",
            Ty::Utf8,
            true,
            "Per signature source (§3.8). The signature as filed (`/s/ …`); not named `signature` (Bloom name).",
        ),
        Col::new(
            "title",
            Ty::Utf8,
            true,
            "Per signature source (§3.8).",
        ),
        Col::new(
            "phone",
            Ty::Utf8,
            true,
            "`Filing.body.form13f.signature.phone`. 13F only.",
        ),
        Col::new(
            "city",
            Ty::Utf8,
            true,
            "`Filing.body.form13f.signature.city`. 13F only.",
        ),
        Col::new(
            "state",
            Ty::Utf8,
            true,
            "`Filing.body.form13f.signature.state`. 13F only; EDGAR code.",
        ),
        Col::new(
            "signature_date",
            Ty::Date32,
            true,
            "Per signature source (§3.8): date (§4.2). Formats differ by form; all parse.",
        ),
        Col::new(
            "has_parse_issues",
            Ty::Boolean,
            false,
            "Derived: true iff this row wrote ≥1 `parse_issues` row (§4.6).",
        ),
    ],
};

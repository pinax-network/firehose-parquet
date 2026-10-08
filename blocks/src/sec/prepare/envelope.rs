//! Preflight of `filing_raw_xml`, `filing_parties`, `filing_documents`, `filing_series`, `filing_series_classes`, `filing_signatures` (§3.3, §3.4, §3.5, §3.6, §3.7, §3.8).
//! Owned by the `envelope` group: see `/tmp/sec-fireparq/impl/contracts.md`.
//!
//! Fallible only on the structural invariants of §4.7 (positions through
//! `super::idx`); every typed value goes through `IssueSink::row`.
//!
//! The typed values are few: `former_names[].date_changed` on
//! `filing_parties` and `signature_date` on `filing_signatures`. Everything
//! else is a verbatim string the append copies straight from the proto.
//! `filing_signatures` reads the signature messages of every body kind here, in
//! the §3.8 source order, so no body module writes signature rows.
//!
//! Issue order (= `parse_issues` row order): parties (each party's former
//! names, in element order), then signatures. `filing_raw_xml`,
//! `filing_documents`, `filing_series` and `filing_series_classes` have no
//! typed source column.

use anyhow::Result;

use super::{idx, FilingCtx};
use crate::sec::issues::IssueSink;
use crate::sec::proto::sec;
use crate::sec::schema::{FILING_PARTIES, FILING_SIGNATURES};
use sec::filing::Body;

/// The parsed and derived values of `filing_raw_xml`, `filing_parties`, `filing_documents`, `filing_series`, `filing_series_classes`, `filing_signatures` for one filing.
#[derive(Debug, Default)]
pub(crate) struct PreparedEnvelope<'a> {
    /// `filing_parties.has_parse_issues`, one per `Filing.parties[]` element.
    pub party_has_parse_issues: Vec<bool>,
    /// `filing_parties.former_names[].date_changed` (`Date32` days) of every
    /// party, concatenated in party order, then element order.
    pub former_name_dates: Vec<Option<i32>>,
    /// `filing_series.class_count`, one per `Filing.series[]` element.
    pub class_counts: Vec<u32>,
    /// The `filing_signatures` rows, in §3.8 source order
    /// (`signature_index` = position).
    pub signatures: Vec<PreparedSignature<'a>>,
}

/// The verbatim text columns of one `filing_signatures` row, borrowed from its
/// signature message; `""` is a blank cell of the §3.8 matrix (NULL).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct SignatureText<'a> {
    pub signed_for: &'a str,
    pub signer_name: &'a str,
    pub signature_text: &'a str,
    pub title: &'a str,
    pub phone: &'a str,
    pub city: &'a str,
    pub state: &'a str,
}

/// One `filing_signatures` row.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct PreparedSignature<'a> {
    /// `signature_source`: `ownership`, `form13f`, `beneficial`, `nport`,
    /// `form_d`, `npx`, `form_c_issuer` or `form_c_person`.
    pub source: &'static str,
    pub text: SignatureText<'a>,
    /// `signature_date` (§4.2), `Date32` days.
    pub signature_date: Option<i32>,
    pub has_parse_issues: bool,
}

pub(crate) fn prepare<'a>(
    _fc: &FilingCtx<'a>,
    filing: &'a sec::Filing,
    issues: &mut IssueSink<'a>,
) -> Result<PreparedEnvelope<'a>> {
    // filing_raw_xml (§3.3): `raw_xml.len()` is checked by the hub
    // (`filings.raw_xml_size`); no typed column.

    // filing_parties (§3.4): the only typed member is
    // `former_names.date_changed`, addressed (party_index, element).
    let mut party_has_parse_issues = Vec::with_capacity(filing.parties.len());
    let mut former_name_dates = Vec::new();
    for (position, party) in filing.parties.iter().enumerate() {
        let party_index = idx(position)?;
        let mut row = issues.row(FILING_PARTIES, &[party_index]);
        for (element, former) in party.former_names.iter().enumerate() {
            former_name_dates.push(row.date_item(
                "former_names.date_changed",
                idx(element)?,
                &former.date_changed,
            ));
        }
        party_has_parse_issues.push(row.finish());
    }

    // filing_documents (§3.5), filing_series (§3.6), filing_series_classes
    // (§3.7): positions and counts only. Every position is below its list
    // length, so checking the lengths checks the positions.
    idx(filing.documents.len())?;
    idx(filing.series.len())?;
    let class_counts = filing
        .series
        .iter()
        .map(|series| idx(series.classes.len()))
        .collect::<Result<Vec<_>>>()?;

    // filing_signatures (§3.8).
    let signatures = prepare_signatures(filing.body.as_ref(), issues)?;

    Ok(PreparedEnvelope {
        party_has_parse_issues,
        former_name_dates,
        class_counts,
        signatures,
    })
}

/// The `filing_signatures` rows of one body, in the §3.8 source order
/// (`signature_index` = position). A filing has one body, so only Form C has
/// two sources: the issuer signature first, then the person signatures. A
/// singular signature message (13F, N-PORT, Form C issuer) gives a row when it
/// is present, even when every field is empty.
fn prepare_signatures<'a>(
    body: Option<&'a Body>,
    issues: &mut IssueSink<'a>,
) -> Result<Vec<PreparedSignature<'a>>> {
    let mut rows = Vec::new();
    let mut add = |source: &'static str, text: SignatureText<'a>, date: &'a str| -> Result<()> {
        let signature_index = idx(rows.len())?;
        let mut row = issues.row(FILING_SIGNATURES, &[signature_index]);
        let signature_date = row.date("signature_date", date);
        rows.push(PreparedSignature {
            source,
            text,
            signature_date,
            has_parse_issues: row.finish(),
        });
        Ok(())
    };
    match body {
        Some(Body::Ownership(body)) => {
            for s in &body.owner_signatures {
                // `Signature.name` holds the signature as filed (`/s/ …`).
                let text = SignatureText {
                    signature_text: &s.name,
                    ..SignatureText::default()
                };
                add("ownership", text, &s.date)?;
            }
        }
        Some(Body::Form13f(body)) => {
            if let Some(s) = &body.signature {
                let text = SignatureText {
                    signed_for: "",
                    signer_name: &s.name,
                    signature_text: &s.signature,
                    title: &s.title,
                    phone: &s.phone,
                    city: &s.city,
                    state: &s.state,
                };
                add("form13f", text, &s.signature_date)?;
            }
        }
        Some(Body::Beneficial(body)) => {
            for s in &body.signatures {
                let text = SignatureText {
                    signed_for: &s.reporting_person,
                    signature_text: &s.signature,
                    title: &s.title,
                    ..SignatureText::default()
                };
                add("beneficial", text, &s.date)?;
            }
        }
        Some(Body::Nport(body)) => {
            if let Some(s) = &body.signature {
                let text = SignatureText {
                    signed_for: &s.name_of_applicant,
                    signer_name: &s.signer_name,
                    signature_text: &s.signature,
                    title: &s.title,
                    ..SignatureText::default()
                };
                add("nport", text, &s.date_signed)?;
            }
        }
        Some(Body::FormD(body)) => {
            for s in &body.signatures {
                let text = SignatureText {
                    signed_for: &s.issuer_name,
                    signer_name: &s.name_of_signer,
                    signature_text: &s.signature_name,
                    title: &s.signature_title,
                    ..SignatureText::default()
                };
                add("form_d", text, &s.signature_date)?;
            }
        }
        Some(Body::Npx(body)) => {
            for s in body.cover_page.iter().flat_map(|cover| &cover.signatures) {
                let text = SignatureText {
                    signed_for: &s.reporting_person,
                    signer_name: &s.printed_signature,
                    signature_text: &s.signature,
                    title: &s.title,
                    ..SignatureText::default()
                };
                add("npx", text, &s.date)?;
            }
        }
        Some(Body::FormC(body)) => {
            let text = |s: &'a sec::FormCSignature| SignatureText {
                signed_for: &s.issuer,
                signature_text: &s.signature,
                title: &s.title,
                ..SignatureText::default()
            };
            if let Some(s) = &body.issuer_signature {
                add("form_c_issuer", text(s), &s.date)?;
            }
            for s in &body.person_signatures {
                add("form_c_person", text(s), &s.date)?;
            }
        }
        // Form 144's notice signature stays on `form144_notices` (§3.8).
        Some(Body::Raw(_) | Body::Form144(_) | Body::Ncen(_)) | None => {}
    }
    Ok(rows)
}

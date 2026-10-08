//! Table specs generated from the final specification §3 (`final-spec.md`).
//! Edit only to fix a divergence from the specification; regenerate
//! `docs/schemas/sec.md` and re-pin the chain schema digests afterwards.

#[allow(unused_imports)]
use super::{Col, Family, Member, TableSpec, Ty};

/// §3.43 `parse_issues`.
pub(crate) const PARSE_ISSUES: TableSpec = TableSpec {
    name: super::PARSE_ISSUES,
    doc: "Every source value that did not convert exactly (or was rounded), with enough keys to put it back on its row. No files on clean days.",
    filing_context: false,
    cols: &[
        Col::new(
            "filing_index",
            Ty::UInt32,
            true,
            "`Filing.ordinal`: the filing of the value; NULL for block-level fields (`blocks.feed_date`).",
        ),
        Col::new(
            "accession_number",
            Ty::Utf8,
            true,
            "`Filing.accession_number`: verbatim; NULL for block-level fields.",
        ),
        Col::new(
            "table_name",
            Ty::Dictionary,
            false,
            "Derived: table of the typed column.",
        ),
        Col::new(
            "column_name",
            Ty::Dictionary,
            false,
            "Derived: typed column; `list.member` for a List<Struct> member (`former_names.date_changed`).",
        ),
        Col::new(
            "index_1",
            Ty::UInt32,
            true,
            "Derived: first key position after `filing_index` (§4.6).",
        ),
        Col::new(
            "index_2",
            Ty::UInt32,
            true,
            "Derived: second key position.",
        ),
        Col::new(
            "index_3",
            Ty::UInt32,
            true,
            "Derived: third key position.",
        ),
        Col::new(
            "raw_value",
            Ty::Utf8,
            false,
            "The source string: verbatim; for `overflow`, the operands joined with ` * ` / ` + ` (§4.6).",
        ),
        Col::new(
            "issue",
            Ty::Dictionary,
            false,
            "Derived: `unparseable`, `sentinel`, `out_of_range`, `rounded`, `tz_dropped`, `overflow` (§4.6).",
        ),
    ],
};

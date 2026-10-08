//! Recording `parse_issues` rows (§4.6) during preflight.
//!
//! Every typed column fed from a source string goes through a [`RowIssues`]
//! recorder: it maps `""` to NULL with no issue, parses the rest with the §4
//! parsers, and records one [`PreparedIssue`] per value that does not convert
//! exactly. [`RowIssues::finish`] returns the row's `has_parse_issues` flag.
//!
//! ```ignore
//! let mut row = issues.row(OWNERSHIP_TRANSACTIONS, &[transaction_index]);
//! let shares = row.decimal("shares", &tx.shares, Family::Q6);
//! let transaction_date = row.date("transaction_date", &tx.transaction_date);
//! let has_parse_issues = row.finish();
//! ```
//!
//! Issues keep the order they are recorded in, which is the `parse_issues` row
//! order: block-level issues, then per filing the `filings` row, the envelope
//! tables and the body tables, each in mapping (row, then column) order.

use std::borrow::Cow;

use super::parse::{self, Family, IssueKind, Parsed};

/// One `parse_issues` row, before it is appended.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PreparedIssue<'a> {
    /// `parse_issues.table_name`: the table of the typed column.
    pub table: &'static str,
    /// `parse_issues.column_name`: the typed column; `<list>.<member>` for a
    /// `List<Struct>` member (`former_names.date_changed`).
    pub column: &'static str,
    /// `index_1..index_3`: the row's key positions after `filing_index`, then
    /// the list element position; unused positions are `None`.
    pub index: [Option<u32>; 3],
    /// `parse_issues.raw_value`: the verbatim source text (for `overflow`, the
    /// operands joined with ` * ` or ` + `).
    pub raw: Cow<'a, str>,
    pub kind: IssueKind,
}

/// The issues of one filing (or of the block, for block-level fields), in
/// recording order.
#[derive(Debug, Default)]
pub(crate) struct IssueSink<'a> {
    issues: Vec<PreparedIssue<'a>>,
}

impl<'a> IssueSink<'a> {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Start recording the issues of one row of `table`. `keys` are the row's
    /// key positions after `filing_index`, in key order (§4.6 addressing table):
    /// `&[]` for `blocks`, `filings` and per-filing body tables,
    /// `&[holding_index, nesting_level, leg_index]` for a swap leg.
    pub(crate) fn row<'s>(&'s mut self, table: &'static str, keys: &[u32]) -> RowIssues<'s, 'a> {
        assert!(keys.len() <= 3, "{table}: at most three key positions");
        let mut index = [None; 3];
        for (slot, key) in index.iter_mut().zip(keys) {
            *slot = Some(*key);
        }
        let start = self.issues.len();
        RowIssues {
            sink: self,
            table,
            index,
            keys: keys.len(),
            start,
        }
    }

    pub(crate) fn issues(&self) -> &[PreparedIssue<'a>] {
        &self.issues
    }

    pub(crate) fn len(&self) -> usize {
        self.issues.len()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.issues.is_empty()
    }
}

/// The issue recorder of one row. Every helper maps `""` to `None` without an
/// issue, as §8.4 requires of the callers of the parsers.
pub(crate) struct RowIssues<'s, 'a> {
    sink: &'s mut IssueSink<'a>,
    table: &'static str,
    index: [Option<u32>; 3],
    keys: usize,
    start: usize,
}

impl<'s, 'a> RowIssues<'s, 'a> {
    /// Record one issue of `column`. `element` is the 0-based position of the
    /// value inside a list column (it follows the key positions).
    pub(crate) fn record(
        &mut self,
        column: &'static str,
        element: Option<u32>,
        raw: impl Into<Cow<'a, str>>,
        kind: IssueKind,
    ) {
        let mut index = self.index;
        if let Some(element) = element {
            assert!(
                self.keys < 3,
                "{}.{column}: no index position left for the list element",
                self.table
            );
            index[self.keys] = Some(element);
        }
        self.sink.issues.push(PreparedIssue {
            table: self.table,
            column,
            index,
            raw: raw.into(),
            kind,
        });
    }

    /// Record `parsed`'s issue, if any, and return its value.
    pub(crate) fn note<T>(
        &mut self,
        column: &'static str,
        element: Option<u32>,
        raw: &'a str,
        parsed: Parsed<T>,
    ) -> Option<T> {
        if let Some(kind) = parsed.issue {
            self.record(column, element, raw, kind);
        }
        parsed.value
    }

    /// A date column (§4.2) as `Date32` days.
    pub(crate) fn date(&mut self, column: &'static str, raw: &'a str) -> Option<i32> {
        if raw.is_empty() {
            return None;
        }
        self.note(column, None, raw, parse::parse_date(raw))
    }

    /// A decimal column (§4.3) as a mantissa at `family`'s scale.
    pub(crate) fn decimal(
        &mut self,
        column: &'static str,
        raw: &'a str,
        family: Family,
    ) -> Option<i128> {
        if raw.is_empty() {
            return None;
        }
        self.note(column, None, raw, parse::parse_decimal(raw, family.scale()))
    }

    /// An `Int32` or `Int64` column (§4.3).
    pub(crate) fn int<T: TryFrom<i128>>(
        &mut self,
        column: &'static str,
        raw: &'a str,
    ) -> Option<T> {
        if raw.is_empty() {
            return None;
        }
        self.note(column, None, raw, parse::parse_int::<T>(raw))
    }

    /// A `Y`/`N` text column (§4.1).
    pub(crate) fn yn(&mut self, column: &'static str, raw: &'a str) -> Option<bool> {
        if raw.is_empty() {
            return None;
        }
        self.note(column, None, raw, parse::parse_yn(raw))
    }

    /// One element of a `List<Date32>` column, or the `Date32` member of a
    /// `List<Struct>` column (`column` = `former_names.date_changed`).
    pub(crate) fn date_item(
        &mut self,
        column: &'static str,
        element: u32,
        raw: &'a str,
    ) -> Option<i32> {
        if raw.is_empty() {
            return None;
        }
        self.note(column, Some(element), raw, parse::parse_date(raw))
    }

    /// One element of a `List<Int32>` column.
    pub(crate) fn int_item<T: TryFrom<i128>>(
        &mut self,
        column: &'static str,
        element: u32,
        raw: &'a str,
    ) -> Option<T> {
        if raw.is_empty() {
            return None;
        }
        self.note(column, Some(element), raw, parse::parse_int::<T>(raw))
    }

    /// Whether this row recorded an issue so far.
    pub(crate) fn has_issues(&self) -> bool {
        self.sink.issues.len() > self.start
    }

    /// Stop recording; the row's `has_parse_issues` value.
    pub(crate) fn finish(self) -> bool {
        self.has_issues()
    }
}

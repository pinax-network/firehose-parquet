//! The §4 parsers of the SEC table specification: total, deterministic byte
//! parsers that never fail on EDGAR content.
//!
//! This is a 1:1 port of the reference functions `parse_dec`, `parse_int`,
//! `parse_date`, `parse_yn`, `cusip_norm`, `lei_norm`, `seq_numbers` and
//! `month_end_back` of the prototype (`proto_map.py`). Two details follow the
//! reference rather than Rust defaults:
//!
//! - **Trimming** is Python's `str.strip()`: Unicode white space plus the
//!   ASCII separators `\x1c`..`\x1f` ([`is_py_space`]).
//! - **Upper-casing** of free text (`Y`/`N` flags, CUSIPs, LEIs, `Indefinite`)
//!   is Unicode upper-casing, like Python's `str.upper()`. Sentinel matching is
//!   ASCII-case-insensitive, which is equivalent for the six sentinels.
//!
//! Digits are ASCII `[0-9]` only, in every grammar (spec §4.3, decision C15):
//! non-ASCII digits such as `١٢` or `１２` are `unparseable`, and a
//! `seq_numbers` token made of them is dropped. The reference spells its
//! grammars with `[0-9]`, not Python's Unicode `\d`, for the same reading.
//!
//! `""` never reaches a parser: callers map it to NULL first, with no issue
//! ([`crate::sec::issues::RowIssues`] does this).

use std::borrow::Cow;

/// Why a source value has a `parse_issues` row (§4.6).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum IssueKind {
    /// The text does not match the column grammar; the typed value is NULL.
    Unparseable,
    /// `N/A`, `NA`, `NONE`, `NULL`, `-` or `XXXX`; the typed value is NULL.
    Sentinel,
    /// Too many integer digits, outside the integer type, an invalid calendar
    /// date or a year outside 1000..=9999; the typed value is NULL.
    OutOfRange,
    /// Non-zero digits beyond the column scale; the typed value is rounded half
    /// away from zero.
    Rounded,
    /// An ISO date with a zone suffix; the typed value is the date.
    TzDropped,
    /// A checked derived sum or product overflowed; the typed value is NULL.
    Overflow,
}

impl IssueKind {
    pub const ALL: [IssueKind; 6] = [
        IssueKind::Unparseable,
        IssueKind::Sentinel,
        IssueKind::OutOfRange,
        IssueKind::Rounded,
        IssueKind::TzDropped,
        IssueKind::Overflow,
    ];

    /// The `parse_issues.issue` label.
    pub const fn label(self) -> &'static str {
        match self {
            IssueKind::Unparseable => "unparseable",
            IssueKind::Sentinel => "sentinel",
            IssueKind::OutOfRange => "out_of_range",
            IssueKind::Rounded => "rounded",
            IssueKind::TzDropped => "tz_dropped",
            IssueKind::Overflow => "overflow",
        }
    }
}

/// A parsed value and the issue it logs, if any. `Rounded` and `TzDropped`
/// come with a value; every other issue with `None`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Parsed<T> {
    pub value: Option<T>,
    pub issue: Option<IssueKind>,
}

impl<T> Parsed<T> {
    pub const fn ok(value: T) -> Self {
        Self {
            value: Some(value),
            issue: None,
        }
    }

    pub const fn fail(issue: IssueKind) -> Self {
        Self {
            value: None,
            issue: Some(issue),
        }
    }

    pub const fn with_issue(value: T, issue: IssueKind) -> Self {
        Self {
            value: Some(value),
            issue: Some(issue),
        }
    }
}

/// Precision of every native decimal column.
pub const DECIMAL_PRECISION: u8 = 38;

/// The five `Decimal128(38, s)` scale families (§4.3).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Family {
    /// `Decimal128(38,2)`: USD with cents.
    M2,
    /// `Decimal128(38,6)`: quantities and per-unit prices.
    Q6,
    /// `Decimal128(38,10)`: N-PORT amounts and quantities.
    N10,
    /// `Decimal128(38,12)`: percents, rates and ratios.
    R12,
    /// `Decimal128(38,16)`: N-PX share counts.
    S16,
}

impl Family {
    pub const ALL: [Family; 5] = [
        Family::M2,
        Family::Q6,
        Family::N10,
        Family::R12,
        Family::S16,
    ];

    pub const fn scale(self) -> u8 {
        match self {
            Family::M2 => 2,
            Family::Q6 => 6,
            Family::N10 => 10,
            Family::R12 => 12,
            Family::S16 => 16,
        }
    }
}

/// `10^38`: decimal mantissas must stay strictly below it in magnitude.
pub const DECIMAL_LIMIT: i128 = 10i128.pow(38);

/// `10^exp` for `exp <= 38`.
pub const fn pow10(exp: u8) -> i128 {
    10i128.pow(exp as u32)
}

/// Whether a mantissa fits `Decimal128(38, s)` (fewer than 39 digits).
pub fn fits_precision(mantissa: i128) -> bool {
    mantissa > -DECIMAL_LIMIT && mantissa < DECIMAL_LIMIT
}

/// Python's `str.isspace()`: Unicode `White_Space` plus `\x1c`..`\x1f`.
pub fn is_py_space(c: char) -> bool {
    c.is_whitespace() || ('\u{1c}'..='\u{1f}').contains(&c)
}

/// Python's `str.strip()`.
pub fn trim(raw: &str) -> &str {
    raw.trim_matches(is_py_space)
}

/// Python's `str.upper()`: Unicode upper-casing, borrowed when unchanged.
pub fn upper(text: &str) -> Cow<'_, str> {
    if text.is_ascii() {
        if text.bytes().any(|b| b.is_ascii_lowercase()) {
            Cow::Owned(text.to_ascii_uppercase())
        } else {
            Cow::Borrowed(text)
        }
    } else {
        let upper = text.to_uppercase();
        if upper == text {
            Cow::Borrowed(text)
        } else {
            Cow::Owned(upper)
        }
    }
}

/// The sentinels of §4.3, matched ASCII-case-insensitively after trimming.
pub const SENTINELS: [&str; 6] = ["N/A", "NA", "NONE", "NULL", "-", "XXXX"];

/// Whether trimmed text is a sentinel.
pub fn is_sentinel(trimmed: &str) -> bool {
    SENTINELS
        .iter()
        .any(|sentinel| trimmed.eq_ignore_ascii_case(sentinel))
}

fn digit_run(bytes: &[u8], from: usize) -> usize {
    bytes[from..]
        .iter()
        .take_while(|b| b.is_ascii_digit())
        .count()
}

/// ASCII digits without leading zeros as an `i128`; `None` when they do not fit.
fn digits_value(digits: &[u8]) -> Option<i128> {
    digits.iter().try_fold(0i128, |value, digit| {
        value.checked_mul(10)?.checked_add(i128::from(digit - b'0'))
    })
}

fn strip_leading_zeros(digits: &[u8]) -> &[u8] {
    let zeros = digits.iter().take_while(|&&b| b == b'0').count();
    &digits[zeros..]
}

/// A decimal string (§4.3 grammar `^[+-]?(\d+(\.\d*)?|\.\d+)$`) as an exact
/// mantissa at `scale`. Digits beyond the scale round half away from zero and
/// log `Rounded`; more than `38 - scale` integer digits (after rounding) give
/// `OutOfRange`. `-0` is `0`.
pub fn parse_decimal(raw: &str, scale: u8) -> Parsed<i128> {
    debug_assert!(scale <= DECIMAL_PRECISION);
    let text = trim(raw);
    if is_sentinel(text) {
        return Parsed::fail(IssueKind::Sentinel);
    }
    let bytes = text.as_bytes();
    let (negative, mut at) = match bytes.first() {
        Some(b'-') => (true, 1),
        Some(b'+') => (false, 1),
        _ => (false, 0),
    };
    let int_len = digit_run(bytes, at);
    let int_digits = &bytes[at..at + int_len];
    at += int_len;
    let mut fraction: &[u8] = &[];
    if bytes.get(at) == Some(&b'.') {
        at += 1;
        let fraction_len = digit_run(bytes, at);
        fraction = &bytes[at..at + fraction_len];
        at += fraction_len;
        if int_digits.is_empty() && fraction.is_empty() {
            return Parsed::fail(IssueKind::Unparseable);
        }
    } else if int_digits.is_empty() {
        return Parsed::fail(IssueKind::Unparseable);
    }
    if at != bytes.len() {
        return Parsed::fail(IssueKind::Unparseable);
    }

    let int_digits = strip_leading_zeros(int_digits);
    if int_digits.len() > usize::from(DECIMAL_PRECISION - scale) {
        return Parsed::fail(IssueKind::OutOfRange);
    }
    let scale_len = usize::from(scale);
    let (kept, rest) = fraction.split_at(fraction.len().min(scale_len));
    // Both fit: at most 38 - scale integer digits and scale fraction digits.
    let mut mantissa = digits_value(int_digits).expect("at most 38 digits") * pow10(scale)
        + digits_value(kept).expect("at most 38 digits") * pow10(scale - kept.len() as u8);
    let mut issue = None;
    if rest.iter().any(|&b| b != b'0') {
        issue = Some(IssueKind::Rounded);
        if rest[0] >= b'5' {
            mantissa += 1;
        }
    }
    if mantissa >= DECIMAL_LIMIT {
        return Parsed::fail(IssueKind::OutOfRange);
    }
    let value = if negative { -mantissa } else { mantissa };
    Parsed {
        value: Some(value),
        issue,
    }
}

/// An integer (§4.3 grammar `^[+-]?\d+(\.0*)?$`, an all-zero fraction is
/// exact) in the range of `T` (`i32` or `i64`); outside it, `OutOfRange`.
pub fn parse_int<T: TryFrom<i128>>(raw: &str) -> Parsed<T> {
    let text = trim(raw);
    if is_sentinel(text) {
        return Parsed::fail(IssueKind::Sentinel);
    }
    let bytes = text.as_bytes();
    let (negative, mut at) = match bytes.first() {
        Some(b'-') => (true, 1),
        Some(b'+') => (false, 1),
        _ => (false, 0),
    };
    let int_len = digit_run(bytes, at);
    if int_len == 0 {
        return Parsed::fail(IssueKind::Unparseable);
    }
    let int_digits = &bytes[at..at + int_len];
    at += int_len;
    if bytes.get(at) == Some(&b'.') {
        at += 1;
        at += bytes[at..].iter().take_while(|&&b| b == b'0').count();
    }
    if at != bytes.len() {
        return Parsed::fail(IssueKind::Unparseable);
    }
    let Some(magnitude) = digits_value(strip_leading_zeros(int_digits)) else {
        return Parsed::fail(IssueKind::OutOfRange);
    };
    let value = if negative { -magnitude } else { magnitude };
    match T::try_from(value) {
        Ok(value) => Parsed::ok(value),
        Err(_) => Parsed::fail(IssueKind::OutOfRange),
    }
}

/// Days since 1970-01-01 (Arrow `Date32`) of a calendar date; `None` when the
/// date is invalid.
pub fn date32(year: i32, month: u8, day: u8) -> Option<i32> {
    let month = time::Month::try_from(month).ok()?;
    let date = time::Date::from_calendar_date(year, month, day).ok()?;
    Some(date.to_julian_day() - UNIX_EPOCH_JULIAN_DAY)
}

/// The Julian day of 1970-01-01.
const UNIX_EPOCH_JULIAN_DAY: i32 = 2_440_588;

fn date_from_days(days: i32) -> Option<time::Date> {
    time::Date::from_julian_day(days.checked_add(UNIX_EPOCH_JULIAN_DAY)?).ok()
}

/// `(year, month, day)` of a `Date32` value.
pub fn calendar(days: i32) -> Option<(i32, u8, u8)> {
    let date = date_from_days(days)?;
    Some((date.year(), date.month() as u8, date.day()))
}

/// ISO `YYYY-MM-DD` of a `Date32` value (for messages and tests).
pub fn iso_date(days: i32) -> Option<String> {
    let (year, month, day) = calendar(days)?;
    Some(format!("{year:04}-{month:02}-{day:02}"))
}

fn small_number(digits: &[u8]) -> u8 {
    digits
        .iter()
        .fold(0, |value, digit| value * 10 + (digit - b'0'))
}

/// A date (§4.2) as `Date32` days: ISO `YYYY-M{1,2}-D{1,2}` with an optional
/// `Z` / `±HH:MM` suffix (dropped, `TzDropped`), US `M{1,2}/D{1,2}/YYYY` or
/// `M{1,2}-D{1,2}-YYYY`. A year outside 1000..=9999 or an invalid calendar
/// date is `OutOfRange`; anything else `Unparseable`.
pub fn parse_date(raw: &str) -> Parsed<i32> {
    let text = trim(raw);
    if is_sentinel(text) {
        return Parsed::fail(IssueKind::Sentinel);
    }
    let bytes = text.as_bytes();
    let Some((year, month, day, zone)) = iso_parts(bytes).or_else(|| us_parts(bytes)) else {
        return Parsed::fail(IssueKind::Unparseable);
    };
    if !(1000..=9999).contains(&year) {
        return Parsed::fail(IssueKind::OutOfRange);
    }
    match date32(year, month, day) {
        Some(days) if zone => Parsed::with_issue(days, IssueKind::TzDropped),
        Some(days) => Parsed::ok(days),
        None => Parsed::fail(IssueKind::OutOfRange),
    }
}

/// `^(\d{4})-(\d{1,2})-(\d{1,2})(Z|[+-]\d{2}:\d{2})?$`.
fn iso_parts(bytes: &[u8]) -> Option<(i32, u8, u8, bool)> {
    if bytes.len() < 8 || digit_run(bytes, 0) != 4 || bytes[4] != b'-' {
        return None;
    }
    let year = bytes[..4]
        .iter()
        .fold(0i32, |value, digit| value * 10 + i32::from(digit - b'0'));
    let month_len = digit_run(bytes, 5);
    if !(1..=2).contains(&month_len) || bytes.get(5 + month_len) != Some(&b'-') {
        return None;
    }
    let day_at = 6 + month_len;
    let day_len = digit_run(bytes, day_at);
    if !(1..=2).contains(&day_len) {
        return None;
    }
    let zone = &bytes[day_at + day_len..];
    let has_zone = match zone {
        [] => false,
        [b'Z'] => true,
        [sign, h1, h2, b':', m1, m2]
            if (*sign == b'+' || *sign == b'-')
                && [h1, h2, m1, m2].iter().all(|b| b.is_ascii_digit()) =>
        {
            true
        }
        _ => return None,
    };
    Some((
        year,
        small_number(&bytes[5..5 + month_len]),
        small_number(&bytes[day_at..day_at + day_len]),
        has_zone,
    ))
}

/// `^(\d{1,2})([/-])(\d{1,2})\2(\d{4})$`.
fn us_parts(bytes: &[u8]) -> Option<(i32, u8, u8, bool)> {
    let month_len = digit_run(bytes, 0);
    if !(1..=2).contains(&month_len) {
        return None;
    }
    let separator = *bytes.get(month_len)?;
    if separator != b'/' && separator != b'-' {
        return None;
    }
    let day_at = month_len + 1;
    let day_len = digit_run(bytes, day_at);
    if !(1..=2).contains(&day_len) || bytes.get(day_at + day_len) != Some(&separator) {
        return None;
    }
    let year_at = day_at + day_len + 1;
    if bytes.len() != year_at + 4 || digit_run(bytes, year_at) != 4 {
        return None;
    }
    let year = bytes[year_at..]
        .iter()
        .fold(0i32, |value, digit| value * 10 + i32::from(digit - b'0'));
    Some((
        year,
        small_number(&bytes[..month_len]),
        small_number(&bytes[day_at..day_at + day_len]),
        false,
    ))
}

/// `Y`/`N` text (§4.1): after trim and upper-case, `Y`, `YES`, `TRUE`, `1` →
/// true; `N`, `NO`, `FALSE`, `0` → false; anything else `Unparseable`.
pub fn parse_yn(raw: &str) -> Parsed<bool> {
    match upper(trim(raw)).as_ref() {
        "Y" | "YES" | "TRUE" | "1" => Parsed::ok(true),
        "N" | "NO" | "FALSE" | "0" => Parsed::ok(false),
        _ => Parsed::fail(IssueKind::Unparseable),
    }
}

fn is_upper_alnum(text: &str, len: usize) -> bool {
    text.len() == len
        && text
            .bytes()
            .all(|b| b.is_ascii_digit() || b.is_ascii_uppercase())
}

/// The cross-form CUSIP join key (§4.4): white space removed, upper-cased, kept
/// only when it is 9 characters of `[0-9A-Z]` and not `000000000` or
/// `999999999`.
pub fn cusip_norm(raw: &str) -> Option<Cow<'_, str>> {
    if raw.is_empty() {
        return None;
    }
    let compact: Cow<'_, str> = if raw.chars().any(is_py_space) {
        Cow::Owned(raw.chars().filter(|c| !is_py_space(*c)).collect())
    } else {
        Cow::Borrowed(raw)
    };
    let value = match compact {
        Cow::Borrowed(text) => upper(text),
        Cow::Owned(text) => Cow::Owned(upper(&text).into_owned()),
    };
    (is_upper_alnum(&value, 9) && value != "000000000" && value != "999999999").then_some(value)
}

/// The N-PORT issuer LEI join key (§4.4): trimmed, upper-cased, kept only when
/// it is 20 characters of `[0-9A-Z]` and not twenty zeros.
pub fn lei_norm(raw: &str) -> Option<Cow<'_, str>> {
    if raw.is_empty() {
        return None;
    }
    let value = upper(trim(raw));
    (is_upper_alnum(&value, 20) && value.bytes().any(|b| b != b'0')).then_some(value)
}

/// `upper(trim(put_call))` when it is `PUT` or `CALL`, else `None` (§4.4).
pub fn put_call_norm(raw: &str) -> Option<&'static str> {
    let text = trim(raw);
    if text.eq_ignore_ascii_case("PUT") {
        Some("PUT")
    } else if text.eq_ignore_ascii_case("CALL") {
        Some("CALL")
    } else {
        None
    }
}

/// Form D: the raw amount is `Indefinite` (case-insensitive, trimmed).
pub fn is_indefinite(raw: &str) -> bool {
    upper(trim(raw)) == "INDEFINITE"
}

/// `form_type` with one trailing `/A` removed.
pub fn base_form_type(form_type: &str) -> &str {
    form_type.strip_suffix("/A").unwrap_or(form_type)
}

fn is_sequence_separator(c: char) -> bool {
    c == ',' || c == ';' || c == '/' || is_py_space(c)
}

/// The 13F `other_manager_ids` tokenizer (§4.4): split every item on
/// `[,;/\s]+` and keep, in order, the tokens matching `^\d+(\.0*)?$` whose
/// value is 1..=9999.
pub fn seq_numbers<S: AsRef<str>>(items: &[S]) -> Vec<i32> {
    let mut numbers = Vec::new();
    for item in items {
        for token in item.as_ref().split(is_sequence_separator) {
            let bytes = token.as_bytes();
            let int_len = digit_run(bytes, 0);
            if int_len == 0 {
                continue;
            }
            let rest = &bytes[int_len..];
            let exact = match rest.split_first() {
                None => true,
                Some((b'.', zeros)) => zeros.iter().all(|&b| b == b'0'),
                Some(_) => false,
            };
            if !exact {
                continue;
            }
            let digits = strip_leading_zeros(&bytes[..int_len]);
            if digits.len() > 4 {
                continue;
            }
            let value = digits
                .iter()
                .fold(0i32, |value, digit| value * 10 + i32::from(digit - b'0'));
            if (1..=9999).contains(&value) {
                numbers.push(value);
            }
        }
    }
    numbers
}

/// The last day of the month `months_back` months before the month of `days`
/// (`Date32`), as `Date32`. N-PORT `month1_end..month3_end` use
/// `month_end_back(as_of_date, 3 - k)`.
pub fn month_end_back(days: i32, months_back: u32) -> Option<i32> {
    let (year, month, _) = calendar(days)?;
    let months = i64::from(year) * 12 + i64::from(month) - 1 - i64::from(months_back);
    let year = i32::try_from(months.div_euclid(12)).ok()?;
    let month = (months.rem_euclid(12) + 1) as u8;
    // December ends on the 31st. Going through the next January 1st would
    // need 10000-01-01 for December 9999, which `time` cannot represent.
    if month == 12 {
        return date32(year, 12, 31);
    }
    Some(date32(year, month + 1, 1)? - 1)
}

/// The canonical text of a mantissa at `scale` (`-12.500000`), as DuckDB prints
/// a `DECIMAL(38, scale)`.
pub fn decimal_string(mantissa: i128, scale: u8) -> String {
    let digits = mantissa.unsigned_abs().to_string();
    let sign = if mantissa < 0 { "-" } else { "" };
    let scale = usize::from(scale);
    if scale == 0 {
        return format!("{sign}{digits}");
    }
    let padded = format!("{digits:0>width$}", width = scale + 1);
    let (int_part, fraction) = padded.split_at(padded.len() - scale);
    format!("{sign}{int_part}.{fraction}")
}

/// `a × b` (at scales `scale_a + scale_b`) rescaled to `scale_out`, rounding half
/// away from zero. `None` when the product overflows `i128` or the result does
/// not fit `Decimal128(38, scale_out)` (log `Overflow`).
pub fn mul_rescale(a: i128, scale_a: u8, b: i128, scale_b: u8, scale_out: u8) -> Option<i128> {
    let product = a.checked_mul(b)?;
    let scale = scale_a + scale_b;
    let value = if scale_out >= scale {
        product.checked_mul(pow10(scale_out - scale))?
    } else {
        let divisor = pow10(scale - scale_out);
        let magnitude = product.unsigned_abs();
        let divisor_u = divisor.unsigned_abs();
        let mut quotient = magnitude / divisor_u;
        if (magnitude % divisor_u) * 2 >= divisor_u {
            quotient += 1;
        }
        let quotient = i128::try_from(quotient).ok()?;
        if product < 0 {
            -quotient
        } else {
            quotient
        }
    };
    fits_precision(value).then_some(value)
}

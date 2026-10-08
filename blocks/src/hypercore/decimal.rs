//! HyperLiquid decimal strings as exact `Decimal128(38, 10)` values.
//!
//! HyperLiquid writes amounts, prices and sizes as decimal strings: `f64`
//! values rounded to at most 10 fractional digits, trailing zeros trimmed
//! (`"6426274.5300000003"`, `"0.0000004703"`, `"-1516.0"`). [`parse`] accepts
//! exactly the strings whose value a `decimal(38,10)` holds, and refuses every
//! other one instead of rounding: no exponent, sign `+`, whitespace, `NaN`,
//! leading or trailing `.`, more than 10 significant fractional digits or more
//! than 28 integer digits. Arrow's `parse_decimal` is not used: it rounds and
//! accepts exponents and whitespace.
//!
//! The value is exact; the text is not kept. Non-canonical but exact forms
//! (`"1.50"`, `"5"`, `"00.5"`, `"-0.0"`) are accepted, and [`canonical_text`]
//! renders HyperLiquid's canonical form of a value back.

use std::fmt;

/// Fractional digits of the stored value: the columns' scale.
pub const SCALE: u32 = super::schema::DECIMAL_SCALE as u32;
/// Integer digits that fit beside [`SCALE`] in the columns' precision (38).
pub const MAX_INTEGER_DIGITS: usize = super::schema::DECIMAL_PRECISION as usize - SCALE as usize;

/// Why a string is not an exact `decimal(38,10)`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DecimalError {
    /// Not `^-?[0-9]+(\.[0-9]+)?$`.
    Syntax,
    /// More than 10 fractional digits after trailing zeros are removed.
    FractionalDigits,
    /// More than 28 integer digits after leading zeros are removed.
    IntegerDigits,
}

impl fmt::Display for DecimalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            DecimalError::Syntax => "not a plain decimal number",
            DecimalError::FractionalDigits => "more than 10 fractional digits",
            DecimalError::IntegerDigits => "more than 28 integer digits",
        })
    }
}

/// Parse `s` into its value scaled by 10^10, exactly.
///
/// 1. `s` must match `^-?[0-9]+(\.[0-9]+)?$` (ASCII digits only). An empty
///    string is refused.
/// 2. Trailing zeros of the fraction are dropped; more than 10 remaining digits
///    are refused.
/// 3. Leading zeros of the integer part are dropped; more than 28 remaining
///    digits are refused.
/// 4. The value is `int * 10^10 + frac` (the fraction right-padded to 10
///    digits), negated after a `-`. `"-0"` and `"-0.0"` are 0.
///
/// Every accepted value has `|value| < 10^38`, so it fits `decimal(38,10)`.
pub fn parse(s: &str) -> Result<i128, DecimalError> {
    let (negative, unsigned) = match s.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, s),
    };
    let (integer, fraction) = match unsigned.split_once('.') {
        Some((integer, fraction)) => (integer, Some(fraction)),
        None => (unsigned, None),
    };
    let digits = |part: &str| !part.is_empty() && part.bytes().all(|b| b.is_ascii_digit());
    if !digits(integer) || !fraction.is_none_or(digits) {
        return Err(DecimalError::Syntax);
    }
    let fraction = fraction.unwrap_or("").trim_end_matches('0');
    if fraction.len() > SCALE as usize {
        return Err(DecimalError::FractionalDigits);
    }
    let integer = integer.trim_start_matches('0');
    if integer.len() > MAX_INTEGER_DIGITS {
        return Err(DecimalError::IntegerDigits);
    }
    // At most 28 + 10 = 38 digits: below 10^38 < i128::MAX, no overflow.
    let mut value: i128 = 0;
    for digit in integer.bytes() {
        value = value * 10 + i128::from(digit - b'0');
    }
    for position in 0..SCALE as usize {
        let digit = fraction.as_bytes().get(position).map_or(0, |b| b - b'0');
        value = value * 10 + i128::from(digit);
    }
    Ok(if negative { -value } else { value })
}

/// HyperLiquid's canonical text of a scaled value: the integer part without
/// leading zeros (`0` if none), `.`, the fraction without trailing zeros (`0`
/// if none), `-` before a negative value. The inverse of [`parse`] on every
/// canonical string, so the lake's values regenerate the source text.
pub fn canonical_text(value: i128) -> String {
    let scale = 10_u128.pow(SCALE);
    let magnitude = value.unsigned_abs();
    let integer = magnitude / scale;
    let fraction = format!("{:010}", magnitude % scale);
    let fraction = fraction.trim_end_matches('0');
    format!(
        "{}{integer}.{}",
        if value < 0 { "-" } else { "" },
        if fraction.is_empty() { "0" } else { fraction }
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    const ONE: i128 = 10_000_000_000;

    #[test]
    fn exact_values_are_accepted_in_any_exact_form() {
        for (text, value) in [
            ("0.0", 0),
            ("1.0", ONE),
            ("-1516.0", -1516 * ONE),
            ("6426274.5300000003", 64_262_745_300_000_003),
            ("0.0000004703", 4_703),
            ("184467440737095.53125", 1_844_674_407_370_955_312_500_000),
            // §2.4 accepted forms: not canonical, but exact.
            ("1.50", 15 * ONE / 10),
            ("5", 5 * ONE),
            ("-0.0", 0),
            ("-0", 0),
            ("0.1234567890000", 1_234_567_890),
            ("00.5", ONE / 2),
        ] {
            assert_eq!(parse(text), Ok(value), "{text}");
        }
        // 28 integer digits and 10 fractional digits: the extremes.
        let max = format!("{}.{}", "9".repeat(28), "9".repeat(10));
        assert_eq!(parse(&max), Ok(10_i128.pow(38) - 1));
        assert_eq!(parse(&format!("-{max}")), Ok(1 - 10_i128.pow(38)));
    }

    #[test]
    fn inexact_or_malformed_strings_are_refused() {
        for text in [
            "", "-", ".", ".5", "5.", "-.5", "1e-5", "1E5", "+1.0", " 1.0", "1.0 ", "NaN", "inf",
            "-inf", "1,0", "1.0.0", "--1", "0x10", "١", "1_000",
        ] {
            assert_eq!(parse(text), Err(DecimalError::Syntax), "{text:?}");
        }
        assert_eq!(parse("0.12345678901"), Err(DecimalError::FractionalDigits));
        assert_eq!(
            parse(&format!("{}.0", "1".repeat(29))),
            Err(DecimalError::IntegerDigits)
        );
        // Leading zeros do not count as integer digits, trailing zeros do not
        // count as fractional digits.
        assert!(parse(&format!("{}1.0", "0".repeat(40))).is_ok());
        assert!(parse(&format!("1.1{}", "0".repeat(40))).is_ok());
        assert_eq!(
            DecimalError::FractionalDigits.to_string(),
            "more than 10 fractional digits"
        );
        assert_eq!(
            DecimalError::IntegerDigits.to_string(),
            "more than 28 integer digits"
        );
    }

    #[test]
    fn canonical_text_inverts_canonical_strings() {
        for text in [
            "0.0",
            "1.0",
            "-1516.0",
            "6426274.5300000003",
            "0.0000004703",
            "-0.0000000001",
            "103001.0",
            "184467440737095.53125",
        ] {
            assert_eq!(canonical_text(parse(text).unwrap()), text);
        }
        assert_eq!(canonical_text(parse("-0.0").unwrap()), "0.0");
        assert_eq!(canonical_text(parse("1.50").unwrap()), "1.5");
        assert_eq!(canonical_text(parse("00.5").unwrap()), "0.5");
    }
}

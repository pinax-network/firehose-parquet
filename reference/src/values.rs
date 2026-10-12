//! Typed values from `/info` payloads.
//!
//! Each parser takes the value at a key (`None` for a missing key) and gives
//! `None`, a NULL in its typed column, for anything that does not parse
//! exactly; `payload_json` keeps the original. They are the Python job's
//! parsers, with its quirks: a number with a fraction or an exponent is also
//! text ([`parse_text`]) and a decimal ([`parse_decimal`]), an integer token
//! never a decimal. Integers outside `i64` are NULL, where Python crashed.

use std::collections::HashMap;
use std::hash::Hash;

use time::{Date, Month, PrimitiveDateTime, Time};

use crate::json::Json;

/// Fractional digits of a `decimal(38,10)`.
const SCALE: usize = 10;
/// Integer digits beside them in its precision.
const MAX_INTEGER_DIGITS: usize = 38 - SCALE;

/// 0001-01-01T00:00:00Z and 9999-12-31T23:59:59.999Z, the range of Python's
/// `datetime`, in milliseconds.
const MIN_MILLIS: i64 = -62_135_596_800_000;
const MAX_MILLIS: i64 = 253_402_300_799_999;

/// A string, or a number's text (a number with a fraction or an exponent is a
/// string in Python). An integer or a boolean is NULL.
pub fn parse_text(value: Option<&Json>) -> Option<String> {
    match value? {
        Json::Str(text) | Json::Num(text) => Some(text.clone()),
        _ => None,
    }
}

/// An integer token within `i64`; never a boolean or a number with a fraction
/// or an exponent.
pub fn parse_int(value: Option<&Json>) -> Option<i64> {
    value?.as_i64()
}

pub fn parse_bool(value: Option<&Json>) -> Option<bool> {
    match value? {
        Json::Bool(flag) => Some(*flag),
        _ => None,
    }
}

/// HyperLiquid's decimal text as a `decimal(38,10)` scaled by 10^10, exactly,
/// or `None`: the text must be `-?[0-9]+(\.[0-9]+)?` (ASCII digits) with at
/// most 10 fractional digits once trailing zeros are dropped and at most 28
/// integer digits once leading zeros are dropped. Nothing is rounded, and
/// `-0` is 0. The lake's `blocks::hypercore::decimal::parse` accepts the same
/// strings (its test `agrees_with_the_reference_parser`).
pub fn decimal_text(text: &str) -> Option<i128> {
    let (negative, unsigned) = match text.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, text),
    };
    let (integer, fraction) = match unsigned.split_once('.') {
        Some((integer, fraction)) => (integer, Some(fraction)),
        None => (unsigned, None),
    };
    let digits = |part: &str| !part.is_empty() && part.bytes().all(|b| b.is_ascii_digit());
    if !digits(integer) || !fraction.is_none_or(digits) {
        return None;
    }
    let fraction = fraction.unwrap_or("").trim_end_matches('0');
    let integer = integer.trim_start_matches('0');
    if fraction.len() > SCALE || integer.len() > MAX_INTEGER_DIGITS {
        return None;
    }
    // At most 38 digits: below 10^38, no overflow.
    let mut value: i128 = 0;
    for digit in integer.bytes() {
        value = value * 10 + i128::from(digit - b'0');
    }
    for position in 0..SCALE {
        let digit = fraction.as_bytes().get(position).map_or(0, |b| b - b'0');
        value = value * 10 + i128::from(digit);
    }
    Some(if negative { -value } else { value })
}

/// A decimal from a string or a number's text ([`decimal_text`]). An integer
/// token is NULL (`1`; HyperLiquid writes decimals as strings).
pub fn parse_decimal(value: Option<&Json>) -> Option<i128> {
    match value? {
        Json::Str(text) | Json::Num(text) => decimal_text(text),
        _ => None,
    }
}

/// A naive UTC time, `2026-02-04T08:08:57.325998778` with an optional
/// fraction of 1 to 9 digits and an optional `Z`, in microseconds since the
/// epoch; the fraction is truncated. The date and time must exist (no
/// February 30, no second 60), in years 1 to 9999 as Python's `strptime`.
/// ASCII digits only.
pub fn parse_naive_utc(value: Option<&Json>) -> Option<i64> {
    let text = value?.as_str()?;
    let text = text.strip_suffix('Z').unwrap_or(text);
    let (moment, fraction) = match text.split_once('.') {
        Some((moment, fraction)) => (moment, Some(fraction)),
        None => (text, None),
    };
    let bytes = moment.as_bytes();
    if bytes.len() != 19
        || [(4, b'-'), (7, b'-'), (10, b'T'), (13, b':'), (16, b':')]
            .iter()
            .any(|&(index, separator)| bytes[index] != separator)
    {
        return None;
    }
    // At most four ASCII digits: below 10^4.
    let number = |from: usize, to: usize| -> Option<u16> {
        bytes[from..to].iter().try_fold(0_u16, |value, b| {
            b.is_ascii_digit().then(|| value * 10 + u16::from(b - b'0'))
        })
    };
    let year = number(0, 4).filter(|year| *year >= 1)?;
    let month = Month::try_from(u8::try_from(number(5, 7)?).ok()?).ok()?;
    let day = u8::try_from(number(8, 10)?).ok()?;
    let date = Date::from_calendar_date(i32::from(year), month, day).ok()?;
    let [hour, minute, second] =
        [number(11, 13)?, number(14, 16)?, number(17, 19)?].map(|part| part as u8);
    let time = Time::from_hms(hour, minute, second).ok()?;
    let micros = match fraction {
        None => 0,
        Some(fraction)
            if (1..=9).contains(&fraction.len())
                && fraction.bytes().all(|b| b.is_ascii_digit()) =>
        {
            format!("{fraction:0<6}")[..6].parse::<i64>().ok()?
        }
        Some(_) => return None,
    };
    let seconds = PrimitiveDateTime::new(date, time)
        .assume_utc()
        .unix_timestamp();
    Some(seconds * 1_000_000 + micros)
}

/// Milliseconds since the epoch in microseconds (`1767225600063` is
/// 2026-01-01T00:00:00.063Z), or `None` outside the years 1 to 9999.
pub fn from_millis(ms: i64) -> Option<i64> {
    (MIN_MILLIS..=MAX_MILLIS).contains(&ms).then(|| ms * 1000)
}

/// `k:v|k:v` as an ordered map, each part split at its first `:` (values may
/// contain `:`, `perp:xyz:SPCX`). `None` unless the text is non-empty, every
/// part has a `:` and a non-empty key, and the keys are distinct.
pub fn parse_kv(text: Option<&str>) -> Option<Vec<(String, String)>> {
    let text = text.filter(|text| !text.is_empty())?;
    let mut pairs: Vec<(String, String)> = Vec::new();
    for part in text.split('|') {
        let (key, value) = part.split_once(':')?;
        if key.is_empty() || pairs.iter().any(|(seen, _)| seen == key) {
            return None;
        }
        pairs.push((key.to_owned(), value.to_owned()));
    }
    Some(pairs)
}

/// The perp dex of a perp coin, its prefix before the first `:`, and `""` for
/// the default dex (`BTC`); the lake's dex vocabulary.
pub fn dex_of(coin: Option<&str>) -> Option<String> {
    let coin = coin?;
    Some(coin.split_once(':').map_or("", |(dex, _)| dex).to_owned())
}

/// HyperLiquid's asset id: the index on the default dex, else
/// `100000 + dex * 10000 + index` (HIP-3). `None` on overflow.
pub fn asset_id(dex: i64, asset: i64) -> Option<i64> {
    if dex == 0 {
        return Some(asset);
    }
    dex.checked_mul(10_000)?
        .checked_add(100_000)?
        .checked_add(asset)
}

/// A list whose items are all integers within `i64` (`[]` included).
pub fn id_list(value: Option<&Json>) -> Option<Vec<i64>> {
    value?.as_arr()?.iter().map(|item| item.as_i64()).collect()
}

/// An entity id: an integer from 0 to `i64::MAX`.
pub fn valid_id(value: Option<&Json>) -> Option<i64> {
    parse_int(value).filter(|id| *id >= 0)
}

/// Python's `bool()` of a value: false for null, `false`, `0`, `""`, `[]` and
/// `{}`. A number's text is a non-empty string, so true even for `0.0`.
pub fn truthy(value: &Json) -> bool {
    match value {
        Json::Null => false,
        Json::Bool(flag) => *flag,
        Json::Int(digits) => digits != "0",
        Json::Num(_) => true,
        Json::Str(text) => !text.is_empty(),
        Json::Arr(items) => !items.is_empty(),
        Json::Obj(entries) => !entries.is_empty(),
    }
}

/// The UTC hour zlib's CRC-32 of `text` falls in: a token's daily details
/// retry, a delisted coin's daily funding read. A stable hash spreads them
/// over the day.
pub fn crc32_hour(text: &str) -> u32 {
    crc32fast::hash(text.as_bytes()) % 24
}

/// A Python dict built from a list: the first occurrence of a key fixes its
/// position, the last supplies its value. The live outcomes, the listed
/// questions, the tokens and pairs and the delisted coins are kept so: their
/// order decides membership, token-detail scheduling and row order.
#[derive(Clone, Debug)]
pub struct Ordered<K, V> {
    order: Vec<K>,
    values: HashMap<K, V>,
}

impl<K, V> Default for Ordered<K, V> {
    fn default() -> Self {
        Self {
            order: Vec::new(),
            values: HashMap::new(),
        }
    }
}

impl<K: Hash + Eq + Clone, V> Ordered<K, V> {
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds `key` at the end, or replaces its value where it already is.
    pub fn insert(&mut self, key: K, value: V) {
        if self.values.insert(key.clone(), value).is_none() {
            self.order.push(key);
        }
    }

    pub fn get(&self, key: &K) -> Option<&V> {
        self.values.get(key)
    }

    pub fn contains_key(&self, key: &K) -> bool {
        self.values.contains_key(key)
    }

    pub fn len(&self) -> usize {
        self.order.len()
    }

    pub fn is_empty(&self) -> bool {
        self.order.is_empty()
    }

    /// The entries in first-seen order.
    pub fn iter(&self) -> impl Iterator<Item = (&K, &V)> {
        self.order.iter().map(|key| (key, &self.values[key]))
    }
}

impl<K: Hash + Eq + Clone, V> FromIterator<(K, V)> for Ordered<K, V> {
    fn from_iter<I: IntoIterator<Item = (K, V)>>(entries: I) -> Self {
        let mut ordered = Self::new();
        for (key, value) in entries {
            ordered.insert(key, value);
        }
        ordered
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ONE: i128 = 10_000_000_000;

    fn text(value: &str) -> Json {
        Json::Str(value.into())
    }

    #[test]
    fn decimals_are_exact_or_null() {
        assert_eq!(parse_decimal(Some(&text("0.0000125"))), Some(125_000));
        assert_eq!(
            parse_decimal(Some(&text("-0.0002624881"))),
            Some(-2_624_881)
        );
        assert_eq!(parse_decimal(Some(&text("1.0"))), Some(ONE));
        assert_eq!(
            parse_decimal(Some(&text("2.50000000000000"))),
            Some(5 * ONE / 2)
        );
        assert_eq!(parse_decimal(Some(&text("-0.0"))), Some(0));
        assert_eq!(
            parse_decimal(Some(&text("00012.5000"))),
            Some(125 * ONE / 10)
        );
        assert_eq!(
            parse_decimal(Some(&text(&"1".repeat(28)))),
            Some("1".repeat(28).parse::<i128>().unwrap() * ONE)
        );
        let max = format!("{}.{}", "9".repeat(28), "9".repeat(10));
        assert_eq!(decimal_text(&max), Some(10_i128.pow(38) - 1));
        assert_eq!(decimal_text(&format!("-{max}")), Some(1 - 10_i128.pow(38)));
        // A number with a fraction keeps its text, so it parses (Q2).
        assert_eq!(
            parse_decimal(Some(&Json::Num("1.5".into()))),
            Some(3 * ONE / 2)
        );
        for bad in [
            "0.00000000001",
            "1e-5",
            "+1",
            ".5",
            "1.",
            "NaN",
            " 1.5",
            "1.5 ",
            "",
            "-",
            "１",
        ] {
            assert_eq!(parse_decimal(Some(&text(bad))), None, "{bad:?}");
        }
        assert_eq!(parse_decimal(Some(&text(&"1".repeat(29)))), None);
        assert_eq!(parse_decimal(Some(&Json::Num("1e-5".into()))), None);
        // An integer token is not a decimal (Q2), nor is a boolean or null.
        for bad in [Json::Int("1".into()), Json::Bool(true), Json::Null] {
            assert_eq!(parse_decimal(Some(&bad)), None, "{bad:?}");
        }
        assert_eq!(parse_decimal(None), None);
    }

    #[test]
    fn description_map() {
        let pairs = |items: &[(&str, &str)]| {
            Some(
                items
                    .iter()
                    .map(|(key, value)| (key.to_string(), value.to_string()))
                    .collect::<Vec<_>>(),
            )
        };
        assert_eq!(
            parse_kv(Some("perp:xyz:SPCX|threshold:174")),
            pairs(&[("perp", "xyz:SPCX"), ("threshold", "174")])
        );
        assert_eq!(parse_kv(Some("a:")), pairs(&[("a", "")]));
        assert_eq!(parse_kv(Some("k:v:w")), pairs(&[("k", "v:w")]));
        for bad in [
            "other", "", "a:1|a:2", ":x", "a:1|", "a:1||b:2", "a:1|b", "|",
        ] {
            assert_eq!(parse_kv(Some(bad)), None, "{bad:?}");
        }
        assert_eq!(parse_kv(None), None);
    }

    #[test]
    fn asset_ids_and_dexes() {
        assert_eq!(asset_id(0, 3), Some(3));
        assert_eq!(asset_id(1, 0), Some(110_000));
        assert_eq!(asset_id(2, 1), Some(120_001));
        assert_eq!(asset_id(10, 5), Some(200_005));
        assert_eq!(asset_id(i64::MAX / 1000, 0), None);
        assert_eq!(dex_of(Some("BTC")).as_deref(), Some(""));
        assert_eq!(dex_of(Some("xyz:XYZ100")).as_deref(), Some("xyz"));
        assert_eq!(dex_of(Some(":X")).as_deref(), Some(""));
        assert_eq!(dex_of(Some("a:b:c")).as_deref(), Some("a"));
        assert_eq!(dex_of(None), None);
    }

    #[test]
    fn times() {
        assert_eq!(from_millis(1_767_225_600_063), Some(1_767_225_600_063_000));
        assert_eq!(from_millis(-1), Some(-1000));
        assert_eq!(
            from_millis(253_402_300_799_999),
            Some(253_402_300_799_999_000)
        );
        assert_eq!(from_millis(253_402_300_800_000), None);
        assert_eq!(from_millis(MIN_MILLIS), Some(MIN_MILLIS * 1000));
        assert_eq!(from_millis(MIN_MILLIS - 1), None);
        assert_eq!(
            parse_naive_utc(Some(&text("2026-02-04T08:08:57.325998778"))),
            Some(1_770_192_537_325_998)
        );
        assert_eq!(
            parse_naive_utc(Some(&text("2024-07-09T06:32:28.719Z"))),
            Some(1_720_506_748_719_000)
        );
        assert_eq!(parse_naive_utc(None), None);
        assert_eq!(parse_naive_utc(Some(&Json::Int("1".into()))), None);
        for bad in [
            "0000-01-01T00:00:00",
            "2023-02-29T00:00:00",
            "2024-01-01T24:00:00",
            "2024-01-01T00:00:60",
            "2024-01-01 00:00:00",
            "2024-01-01T00:00:00+00:00",
            "2024-07-09T06:32:28.",
            "2024-07-09T06:32:28.1234567891",
            "2024-07-09T06:32:28ZZ",
            "2024-00-09T06:32:28",
            "２024-07-09T06:32:28",
            "é24-07-09T06:32:28",
        ] {
            assert_eq!(parse_naive_utc(Some(&text(bad))), None, "{bad:?}");
        }
    }

    #[test]
    fn ids_and_truthiness() {
        let list = Json::parse(b"[1, 2, 3]").unwrap();
        assert_eq!(id_list(Some(&list)), Some(vec![1, 2, 3]));
        assert_eq!(id_list(Some(&Json::Arr(vec![]))), Some(vec![]));
        for bad in [
            "[1, true]",
            "[1, 1.0]",
            "[99999999999999999999]",
            "{}",
            "null",
        ] {
            assert_eq!(
                id_list(Some(&Json::parse(bad.as_bytes()).unwrap())),
                None,
                "{bad}"
            );
        }
        assert_eq!(valid_id(Some(&Json::Int("0".into()))), Some(0));
        assert_eq!(valid_id(Some(&Json::Int("-1".into()))), None);
        assert_eq!(valid_id(Some(&Json::Bool(true))), None);
        for (value, expected) in [
            ("null", false),
            ("false", false),
            ("0", false),
            ("-0", false),
            ("\"\"", false),
            ("[]", false),
            ("{}", false),
            ("true", true),
            ("2", true),
            ("0.0", true),
            ("\"x\"", true),
            ("[0]", true),
        ] {
            assert_eq!(
                truthy(&Json::parse(value.as_bytes()).unwrap()),
                expected,
                "{value}"
            );
        }
        assert_eq!(crc32_hour("MATIC"), 7);
        assert_eq!(crc32_hour("flx:TSLA"), 4);
    }

    #[test]
    fn ordered_keeps_first_position_and_last_value() {
        let ordered: Ordered<i64, &str> = [(3, "a"), (1, "b"), (3, "c"), (2, "d")]
            .into_iter()
            .collect();
        assert_eq!(
            ordered
                .iter()
                .map(|(key, value)| (*key, *value))
                .collect::<Vec<_>>(),
            [(3, "c"), (1, "b"), (2, "d")]
        );
        assert_eq!(ordered.len(), 3);
        assert_eq!(ordered.get(&3), Some(&"c"));
        assert!(ordered.contains_key(&2));
        assert!(!ordered.contains_key(&4));
        assert!(Ordered::<i64, ()>::new().is_empty());
    }
}

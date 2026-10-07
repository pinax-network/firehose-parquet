//! Table-driven tests of the §4 parsers, one case per rule, with the real
//! oddities of the samples (final specification §8.5 item 1).

use super::parse::*;
use IssueKind::*;

fn dec(raw: &str, scale: u8) -> (Option<String>, Option<IssueKind>) {
    let parsed = parse_decimal(raw, scale);
    (parsed.value.map(|m| decimal_string(m, scale)), parsed.issue)
}

#[test]
fn decimals() {
    #[rustfmt::skip]
    let cases: &[(&str, u8, Option<&str>, Option<IssueKind>)] = &[
        // Grammar: accepted shapes.
        (".28", 6, Some("0.280000"), None),
        ("5.", 6, Some("5.000000"), None),
        ("100.00", 2, Some("100.00"), None),
        ("-0", 6, Some("0.000000"), None),
        ("+3.25", 2, Some("3.25"), None),
        ("  12.5 ", 6, Some("12.500000"), None),
        ("0007", 2, Some("7.00"), None),
        ("-1.5", 6, Some("-1.500000"), None),
        // Rounding, half away from zero, logged.
        ("387225.35800000001", 6, Some("387225.358000"), Some(Rounded)),
        ("387225.35800000001", 16, Some("387225.3580000000100000"), None),
        ("0.00018086269318177603", 12, Some("0.000180862693"), Some(Rounded)),
        ("0.0000005", 6, Some("0.000001"), Some(Rounded)),
        ("-0.0000005", 6, Some("-0.000001"), Some(Rounded)),
        ("0.0000004", 6, Some("0.000000"), Some(Rounded)),
        ("-0.0000001", 6, Some("0.000000"), Some(Rounded)),
        ("1.2300000", 2, Some("1.23"), None),
        ("992862.410000000030", 10, Some("992862.4100000000"), Some(Rounded)),
        ("21.367520234938123", 12, Some("21.367520234938"), Some(Rounded)),
        // Sentinels, case-insensitive after trim.
        ("N/A", 6, None, Some(Sentinel)),
        ("n/a", 6, None, Some(Sentinel)),
        (" NA ", 6, None, Some(Sentinel)),
        ("None", 6, None, Some(Sentinel)),
        ("NULL", 6, None, Some(Sentinel)),
        ("-", 6, None, Some(Sentinel)),
        ("XXXX", 6, None, Some(Sentinel)),
        ("xxxx", 10, None, Some(Sentinel)),
        // Unparseable: no guessing of commas, currency, exponents, spaces.
        ("Indefinite", 2, None, Some(Unparseable)),
        ("1,000", 2, None, Some(Unparseable)),
        ("$5", 2, None, Some(Unparseable)),
        ("1e5", 2, None, Some(Unparseable)),
        ("(5)", 2, None, Some(Unparseable)),
        ("1 000", 2, None, Some(Unparseable)),
        (".", 2, None, Some(Unparseable)),
        ("+", 2, None, Some(Unparseable)),
        ("--1", 2, None, Some(Unparseable)),
        ("1.2.3", 2, None, Some(Unparseable)),
        ("   ", 2, None, Some(Unparseable)),
        // Range: more than 38 - s integer digits.
        ("99999999999999999999999999", 12, Some("99999999999999999999999999.000000000000"), None),
        ("999999999999999999999999999", 12, None, Some(OutOfRange)),
        ("99999999999999999999999999.9999999999995", 12, None, Some(OutOfRange)),
        ("000000000000000000000000000000000000000000001", 2, Some("1.00"), None),
        ("99999999999999999999999999999999999999", 0, Some("99999999999999999999999999999999999999"), None),
    ];
    for (raw, scale, value, issue) in cases {
        assert_eq!(
            dec(raw, *scale),
            (value.map(str::to_string), *issue),
            "{raw:?} at scale {scale}"
        );
    }
}

#[test]
fn integers() {
    #[rustfmt::skip]
    let cases: &[(&str, Option<i64>, Option<IssueKind>)] = &[
        ("100", Some(100), None),
        ("100.00", Some(100), None),
        ("100.", Some(100), None),
        ("+7", Some(7), None),
        ("-0", Some(0), None),
        (" 0042 ", Some(42), None),
        ("9223372036854775807", Some(i64::MAX), None),
        ("-9223372036854775808", Some(i64::MIN), None),
        ("9223372036854775808", None, Some(OutOfRange)),
        ("123456789012345678901234567890123456789012", None, Some(OutOfRange)),
        ("1.5", None, Some(Unparseable)),
        ("1.05", None, Some(Unparseable)),
        (".0", None, Some(Unparseable)),
        ("1,000", None, Some(Unparseable)),
        ("N/A", None, Some(Sentinel)),
        ("none", None, Some(Sentinel)),
    ];
    for (raw, value, issue) in cases {
        let parsed = parse_int::<i64>(raw);
        assert_eq!((parsed.value, parsed.issue), (*value, *issue), "{raw:?}");
    }
    let parsed = parse_int::<i32>("2147483648");
    assert_eq!((parsed.value, parsed.issue), (None, Some(OutOfRange)));
    let parsed = parse_int::<i32>("-2147483648");
    assert_eq!((parsed.value, parsed.issue), (Some(i32::MIN), None));
}

#[test]
fn dates() {
    #[rustfmt::skip]
    let cases: &[(&str, Option<&str>, Option<IssueKind>)] = &[
        ("2026-08-14", Some("2026-08-14"), None),
        ("2026-8-4", Some("2026-08-04"), None),
        ("6/30/2026", Some("2026-06-30"), None),
        ("03/16/2026", Some("2026-03-16"), None),
        ("8-14-2026", Some("2026-08-14"), None),
        ("04-30-2027", Some("2027-04-30"), None),
        (" 2026-08-14 ", Some("2026-08-14"), None),
        // Zone suffixes are dropped and logged.
        ("2014-06-30-05:00", Some("2014-06-30"), Some(TzDropped)),
        ("2014-06-30+01:00", Some("2014-06-30"), Some(TzDropped)),
        ("2014-06-30Z", Some("2014-06-30"), Some(TzDropped)),
        // Implausible but valid dates are kept.
        ("05/20/1601", Some("1601-05-20"), None),
        ("11/13/1933", Some("1933-11-13"), None),
        ("2099-12-31", Some("2099-12-31"), None),
        ("9999-12-31", Some("9999-12-31"), None),
        ("1000-01-01", Some("1000-01-01"), None),
        // Invalid calendar dates and years.
        ("2026-02-30", None, Some(OutOfRange)),
        ("2026-13-01", None, Some(OutOfRange)),
        ("2026-00-10", None, Some(OutOfRange)),
        ("02/29/2025", None, Some(OutOfRange)),
        ("0999-12-31", None, Some(OutOfRange)),
        ("0999-12-31Z", None, Some(OutOfRange)),
        ("2/30/0999", None, Some(OutOfRange)),
        // Sentinels and everything else.
        ("N/A", None, Some(Sentinel)),
        ("n/a", None, Some(Sentinel)),
        ("20260814", None, Some(Unparseable)),
        ("2026-08-14T00:00:00", None, Some(Unparseable)),
        ("2026-08-14 00:00", None, Some(Unparseable)),
        ("8/14-2026", None, Some(Unparseable)),
        ("2026/08/14", None, Some(Unparseable)),
        ("14-Aug-2026", None, Some(Unparseable)),
        ("2026-008-14", None, Some(Unparseable)),
        ("2026-08-140", None, Some(Unparseable)),
        ("2026-08-14+0500", None, Some(Unparseable)),
        ("123/1/2026", None, Some(Unparseable)),
        ("1/1/20260", None, Some(Unparseable)),
        ("1/1/26", None, Some(Unparseable)),
    ];
    for (raw, value, issue) in cases {
        let parsed = parse_date(raw);
        assert_eq!(
            (parsed.value.and_then(iso_date), parsed.issue),
            (value.map(str::to_string), *issue),
            "{raw:?}"
        );
    }
    assert_eq!(parse_date("1970-01-01").value, Some(0));
    assert_eq!(parse_date("1969-12-31").value, Some(-1));
}

#[test]
fn yes_no_text() {
    #[rustfmt::skip]
    let cases: &[(&str, Option<bool>, Option<IssueKind>)] = &[
        ("Y", Some(true), None), ("yes", Some(true), None), ("TRUE", Some(true), None), ("1", Some(true), None),
        (" n ", Some(false), None), ("No", Some(false), None), ("false", Some(false), None), ("0", Some(false), None),
        ("N/A", None, Some(Unparseable)), ("maybe", None, Some(Unparseable)), ("YN", None, Some(Unparseable)),
    ];
    for (raw, value, issue) in cases {
        let parsed = parse_yn(raw);
        assert_eq!((parsed.value, parsed.issue), (*value, *issue), "{raw:?}");
    }
}

#[test]
fn cusip_and_lei_keys() {
    #[rustfmt::skip]
    let cusips: &[(&str, Option<&str>)] = &[
        ("037833100", Some("037833100")),
        ("03783310a", Some("03783310A")),
        (" 0378 33100 ", Some("037833100")),
        ("000000000", None),
        ("999999999", None),
        ("N/A", None),
        ("03783310", None),
        ("0378331001", None),
        ("03783310-", None),
        ("", None),
    ];
    for (raw, expected) in cusips {
        assert_eq!(cusip_norm(raw).as_deref(), *expected, "{raw:?}");
    }
    #[rustfmt::skip]
    let leis: &[(&str, Option<&str>)] = &[
        ("5493001KJTIIGC8Y1R12", Some("5493001KJTIIGC8Y1R12")),
        (" 5493001kjtiigc8y1r12 ", Some("5493001KJTIIGC8Y1R12")),
        ("00000000000000000000", None),
        ("N/A", None),
        ("5493001KJTIIGC8Y1R1", None),
        ("5493001KJT IGC8Y1R12", None),
        ("", None),
    ];
    for (raw, expected) in leis {
        assert_eq!(lei_norm(raw).as_deref(), *expected, "{raw:?}");
    }
}

#[test]
fn thirteen_f_sequence_numbers() {
    #[rustfmt::skip]
    let cases: &[(&[&str], &[i32])] = &[
        (&["1,5,6"], &[1, 5, 6]),
        (&["03,01"], &[3, 1]),
        (&["1.0"], &[1]),
        (&["0"], &[]),
        (&["N/A"], &[]),
        (&["NONE"], &[]),
        (&["01", "2 ; 3/4"], &[1, 2, 3, 4]),
        (&["10000", "9999"], &[9999]),
        (&["1.5", "+2", "3a"], &[]),
        (&["  7  "], &[7]),
        (&[], &[]),
    ];
    for (items, expected) in cases {
        assert_eq!(seq_numbers(items), expected.to_vec(), "{items:?}");
    }
}

#[test]
fn put_call_indefinite_and_base_form() {
    assert_eq!(put_call_norm(" Put "), Some("PUT"));
    assert_eq!(put_call_norm("call"), Some("CALL"));
    assert_eq!(put_call_norm("PUTS"), None);
    assert_eq!(put_call_norm(""), None);
    assert!(is_indefinite("Indefinite"));
    assert!(is_indefinite(" INDEFINITE "));
    assert!(!is_indefinite("1000000"));
    assert_eq!(base_form_type("13F-HR/A"), "13F-HR");
    assert_eq!(base_form_type("4"), "4");
    assert_eq!(base_form_type("A/A/A"), "A/A");
}

#[test]
fn month_ends() {
    let day = |text: &str| parse_date(text).value.unwrap();
    let back = |text: &str, k: u32| month_end_back(day(text), k).and_then(iso_date);
    assert_eq!(back("2026-06-30", 0).as_deref(), Some("2026-06-30"));
    assert_eq!(back("2026-06-30", 1).as_deref(), Some("2026-05-31"));
    assert_eq!(back("2026-06-15", 2).as_deref(), Some("2026-04-30"));
    assert_eq!(back("2026-02-10", 2).as_deref(), Some("2025-12-31"));
    assert_eq!(back("2024-03-31", 1).as_deref(), Some("2024-02-29"));
    assert_eq!(back("2026-01-31", 13).as_deref(), Some("2024-12-31"));
}

#[test]
fn decimal_arithmetic() {
    // value_usd: Q6 × Q6 at scale 12, rounded half away from zero to scale 6.
    let q6 = |text: &str| parse_decimal(text, 6).value.unwrap();
    let product = mul_rescale(q6("100"), 6, q6("12.345678"), 6, 6).unwrap();
    assert_eq!(decimal_string(product, 6), "1234.567800");
    let product = mul_rescale(q6("0.000001"), 6, q6("0.5"), 6, 6).unwrap();
    assert_eq!(decimal_string(product, 6), "0.000001");
    let product = mul_rescale(q6("-0.000001"), 6, q6("0.5"), 6, 6).unwrap();
    assert_eq!(decimal_string(product, 6), "-0.000001");
    let product = mul_rescale(q6("0.000001"), 6, q6("0.4"), 6, 6).unwrap();
    assert_eq!(decimal_string(product, 6), "0.000000");
    let big = parse_decimal("99999999999999999999999999999999", 6)
        .value
        .unwrap();
    assert_eq!(mul_rescale(big, 6, big, 6, 6), None);
    assert!(fits_precision(DECIMAL_LIMIT - 1));
    assert!(!fits_precision(DECIMAL_LIMIT));
    assert!(!fits_precision(-DECIMAL_LIMIT));
    assert_eq!(decimal_string(5, 2), "0.05");
    assert_eq!(decimal_string(-5, 2), "-0.05");
    assert_eq!(decimal_string(0, 0), "0");
}

#[test]
fn trimming_follows_python_str_strip() {
    // Python `str.isspace()` (CPython, Unicode 15): every character it accepts.
    const PY_SPACE: [u32; 29] = [
        0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x1c, 0x1d, 0x1e, 0x1f, 0x20, 0x85, 0xa0, 0x1680, 0x2000,
        0x2001, 0x2002, 0x2003, 0x2004, 0x2005, 0x2006, 0x2007, 0x2008, 0x2009, 0x200a, 0x2028,
        0x2029, 0x202f, 0x205f, 0x3000,
    ];
    let accepted: Vec<u32> = (0..=0x10ffffu32)
        .filter_map(char::from_u32)
        .filter(|c| is_py_space(*c))
        .map(u32::from)
        .collect();
    assert_eq!(accepted, PY_SPACE);
    assert_eq!(trim("\u{1c} 12 \u{a0}"), "12");
    assert_eq!(parse_decimal("\u{1f}1.5\u{3000}", 2).value, Some(150));
}

/// A documented divergence from the Python reference, whose `\d` also
/// matches non-ASCII decimal digits: the byte parsers accept ASCII digits only.
/// No numeric or date field of the samples holds a non-ASCII digit.
#[test]
fn non_ascii_digits_are_not_digits() {
    assert_eq!(parse_decimal("\u{661}\u{662}", 2).issue, Some(Unparseable));
    assert_eq!(
        parse_decimal("\u{ff11}\u{ff12}", 2).issue,
        Some(Unparseable)
    );
    assert_eq!(parse_int::<i32>("\u{661}").issue, Some(Unparseable));
    assert_eq!(parse_date("\u{661}/1/2026").issue, Some(Unparseable));
    assert!(seq_numbers(&["\u{661}"]).is_empty());
}

#[test]
fn issue_labels() {
    let labels: Vec<&str> = IssueKind::ALL.iter().map(|kind| kind.label()).collect();
    assert_eq!(
        labels,
        [
            "unparseable",
            "sentinel",
            "out_of_range",
            "rounded",
            "tz_dropped",
            "overflow"
        ]
    );
    let scales: Vec<u8> = Family::ALL.iter().map(|family| family.scale()).collect();
    assert_eq!(scales, [2, 6, 10, 12, 16]);
}

/// Cross-check against the Python reference (`proto_map.py`) on real values.
///
/// The oracle file is produced outside the repository by
/// `/tmp/sec-fireparq/impl/parse_oracle.py`: one tab-separated line
/// `kind\targ\traw\texpected` per value, with `\t`, `\n`, `\r` and `\\`
/// escaped in `raw`, and `expected` as `value|issue` (`~` for None).
///
/// `SEC_PARSE_ORACLE=/tmp/sec-fireparq/impl/parse_oracle.tsv cargo test -p blocks --lib sec::parse_tests::reference -- --ignored --nocapture`
#[test]
#[ignore = "local: needs the Python reference oracle file"]
fn reference_oracle_agrees() {
    use std::io::BufRead;
    let path = std::env::var("SEC_PARSE_ORACLE")
        .unwrap_or_else(|_| "/tmp/sec-fireparq/impl/parse_oracle.tsv".to_string());
    let file = std::fs::File::open(&path).unwrap_or_else(|e| panic!("{path}: {e}"));
    let unescape = |text: &str| {
        let mut out = String::with_capacity(text.len());
        let mut chars = text.chars();
        while let Some(c) = chars.next() {
            if c == '\\' {
                match chars.next() {
                    Some('t') => out.push('\t'),
                    Some('n') => out.push('\n'),
                    Some('r') => out.push('\r'),
                    Some('\\') => out.push('\\'),
                    other => panic!("bad escape {other:?}"),
                }
            } else {
                out.push(c);
            }
        }
        out
    };
    let issue = |issue: Option<IssueKind>| issue.map_or("~", IssueKind::label).to_string();
    let opt = |value: Option<String>| value.unwrap_or_else(|| "~".to_string());
    let mut checked = std::collections::BTreeMap::<String, usize>::new();
    let mut mismatches = Vec::new();
    for line in std::io::BufReader::new(file).lines() {
        let line = line.unwrap();
        let mut parts = line.splitn(4, '\t');
        let (kind, arg, raw, expected) = (
            parts.next().unwrap(),
            parts.next().unwrap(),
            unescape(parts.next().unwrap()),
            parts.next().unwrap(),
        );
        let actual = match kind {
            "dec" => {
                let scale: u8 = arg.parse().unwrap();
                let parsed = parse_decimal(&raw, scale);
                format!(
                    "{}|{}",
                    opt(parsed.value.map(|m| decimal_string(m, scale))),
                    issue(parsed.issue)
                )
            }
            "int" => {
                let parsed = if arg == "32" {
                    let p = parse_int::<i32>(&raw);
                    (p.value.map(i64::from), p.issue)
                } else {
                    let p = parse_int::<i64>(&raw);
                    (p.value, p.issue)
                };
                format!(
                    "{}|{}",
                    opt(parsed.0.map(|v| v.to_string())),
                    issue(parsed.1)
                )
            }
            "date" => {
                let parsed = parse_date(&raw);
                format!(
                    "{}|{}",
                    opt(parsed.value.and_then(iso_date)),
                    issue(parsed.issue)
                )
            }
            "yn" => {
                let parsed = parse_yn(&raw);
                format!(
                    "{}|{}",
                    opt(parsed.value.map(|v| v.to_string())),
                    issue(parsed.issue)
                )
            }
            "cusip" => opt(cusip_norm(&raw).map(|v| v.into_owned())),
            "lei" => opt(lei_norm(&raw).map(|v| v.into_owned())),
            "seq" => {
                let items: Vec<&str> = raw.split('\u{1}').collect();
                format!("{:?}", seq_numbers(&items))
            }
            other => panic!("unknown kind {other}"),
        };
        *checked.entry(format!("{kind}:{arg}")).or_default() += 1;
        if actual != expected {
            mismatches.push(format!(
                "{kind}:{arg} {raw:?}: rust {actual} python {expected}"
            ));
        }
    }
    println!("checked {checked:?}");
    for mismatch in mismatches.iter().take(50) {
        println!("MISMATCH {mismatch}");
    }
    assert!(mismatches.is_empty(), "{} mismatches", mismatches.len());
    assert!(checked.values().sum::<usize>() > 0);
}

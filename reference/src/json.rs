//! Lossless JSON and the canonical form that payload hashes cover.
//!
//! An `/info` answer is parsed into an owned [`Json`] tree that keeps every
//! number's text: an integer token as its digits (`-0` as `0`), and a token
//! with a fraction or an exponent as its source text, so re-serializing never
//! changes a value. The canonical form ([`Json::canonical`]) sorts object keys
//! by code point and has no whitespace. It is the text stored in
//! `payload_json`, `payload_sha256` is the SHA-256 of its UTF-8 bytes
//! ([`payload`]), and request bodies are sent in it.
//!
//! serde_json's strict tokenizer validates the document through `&RawValue`
//! (the `raw_value` feature the binary already has), and each level is then
//! converted on its own. Its `arbitrary_precision` and `preserve_order`
//! features would keep number text and key order too, but Cargo unifies
//! features across the binary, where they change delta-rs's stats parsing and
//! fireparq's control records (#698): they stay off.
//!
//! The tree's memory is bounded before it is built: dense small values take
//! far more than their text (an array of `{"a":0}` about 100 times), so the
//! cap on an answer's bytes alone would let one outgrow the job's memory.

use std::collections::BTreeMap;

use serde_json::value::RawValue;
use sha2::{Digest, Sha256};

/// More nested arrays and objects than this are invalid JSON, as for
/// serde_json's typed values. `/info` answers are a few levels deep.
const MAX_DEPTH: usize = 128;

/// Python refuses to convert an integer of more digits (`sys.int_info`).
const MAX_INTEGER_DIGITS: usize = 4300;

/// The most memory parsing a document may take, as [`tree_bytes`] counts
/// it. With the 64 MiB answer it stays well inside the CronJobs' 1 GiB; a
/// real answer, a few MB counted at 10 to 20 times its size, takes a
/// fraction of it.
pub const MAX_TREE_BYTES: usize = 256 << 20;

/// What [`tree_bytes`] counts for every value: its slot in its array or
/// object, and its entry in the list its level is parsed into, with room
/// for that list's doubling.
const VALUE_BYTES: usize = 2 * (size_of::<Json>() + size_of::<&RawValue>());
/// For every string, key and number besides its text: its allocation's
/// rounding and header.
const TEXT_BYTES: usize = 32;
/// For every object: a B-tree node of 11 entries (an empty one has none).
const NODE_BYTES: usize = 11 * (size_of::<String>() + size_of::<Json>()) + 16;
/// For every key: its share of an object's further nodes, and of the list
/// the object is collected through.
const ENTRY_BYTES: usize = size_of::<(String, Json)>();

/// A parsed JSON value.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Json {
    Null,
    Bool(bool),
    /// An integer token's digits, `-0` as `0`. It may exceed `i64`.
    Int(String),
    /// A number token with a fraction or an exponent: its source text.
    Num(String),
    Str(String),
    Arr(Vec<Json>),
    /// A repeated key keeps its last value. Keys iterate in code point order.
    Obj(BTreeMap<String, Json>),
}

/// Why bytes were not parsed. Carries no input text.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ParseError {
    /// Not JSON (strict RFC 8259, UTF-8).
    Invalid,
    /// JSON whose tree would take more memory than allowed.
    TooLarge,
}

impl Json {
    /// Parses a whole document. Refused as invalid: invalid UTF-8, a byte
    /// order mark, trailing data, `NaN` and `Infinity`, raw control characters
    /// in strings, a lone surrogate escape, an integer of more than 4300
    /// digits and nesting deeper than 128; as too large, a tree of more than
    /// [`MAX_TREE_BYTES`].
    pub fn parse(bytes: &[u8]) -> Result<Json, ParseError> {
        Json::parse_within(bytes, MAX_TREE_BYTES)
    }

    /// [`Json::parse`] with a bound of `max_tree_bytes`, checked on the
    /// validated text before any of the tree is built.
    pub fn parse_within(bytes: &[u8], max_tree_bytes: usize) -> Result<Json, ParseError> {
        let text = std::str::from_utf8(bytes).map_err(|_| ParseError::Invalid)?;
        let raw: &RawValue = serde_json::from_str(text).map_err(|_| ParseError::Invalid)?;
        if tree_bytes(raw.get()) > max_tree_bytes {
            return Err(ParseError::TooLarge);
        }
        convert(raw, 0)
    }

    /// The canonical text: keys sorted, no whitespace, numbers as parsed and
    /// strings escaped as Python's `json.dumps(ensure_ascii=False)` does.
    pub fn canonical(&self) -> String {
        let mut out = String::new();
        self.write_canonical(&mut out);
        out
    }

    fn write_canonical(&self, out: &mut String) {
        match self {
            Json::Null => out.push_str("null"),
            Json::Bool(true) => out.push_str("true"),
            Json::Bool(false) => out.push_str("false"),
            Json::Int(text) | Json::Num(text) => out.push_str(text),
            Json::Str(text) => write_string(out, text),
            Json::Arr(items) => {
                out.push('[');
                for (index, item) in items.iter().enumerate() {
                    if index > 0 {
                        out.push(',');
                    }
                    item.write_canonical(out);
                }
                out.push(']');
            }
            Json::Obj(entries) => {
                out.push('{');
                for (index, (key, value)) in entries.iter().enumerate() {
                    if index > 0 {
                        out.push(',');
                    }
                    write_string(out, key);
                    out.push(':');
                    value.write_canonical(out);
                }
                out.push('}');
            }
        }
    }

    /// The value at `key` of an object; `None` for a missing key or another
    /// kind of value.
    pub fn get(&self, key: &str) -> Option<&Json> {
        self.as_obj()?.get(key)
    }

    pub fn as_obj(&self) -> Option<&BTreeMap<String, Json>> {
        match self {
            Json::Obj(entries) => Some(entries),
            _ => None,
        }
    }

    pub fn as_arr(&self) -> Option<&[Json]> {
        match self {
            Json::Arr(items) => Some(items),
            _ => None,
        }
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            Json::Str(text) => Some(text),
            _ => None,
        }
    }

    /// An integer token within `i64`; never a boolean or a number with a
    /// fraction or an exponent.
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Json::Int(digits) => digits.parse().ok(),
            _ => None,
        }
    }

    /// An object without the given keys (a response without its entity
    /// lists); any other value as it is.
    pub fn without(&self, keys: &[&str]) -> Json {
        match self {
            Json::Obj(entries) => Json::Obj(
                entries
                    .iter()
                    .filter(|(key, _)| !keys.contains(&key.as_str()))
                    .map(|(key, value)| (key.clone(), value.clone()))
                    .collect(),
            ),
            other => other.clone(),
        }
    }

    /// An object of the given entries: a request body.
    pub fn obj<const N: usize>(entries: [(&str, Json); N]) -> Json {
        Json::Obj(
            entries
                .into_iter()
                .map(|(key, value)| (key.to_owned(), value))
                .collect(),
        )
    }

    /// The value at an RFC 6901 pointer (`/0/universe/1/maxLeverage`), to
    /// change a fixture in a test.
    #[cfg(any(test, feature = "test-support"))]
    pub fn pointer_mut(&mut self, pointer: &str) -> Option<&mut Json> {
        if pointer.is_empty() {
            return Some(self);
        }
        let mut value = self;
        for token in pointer.strip_prefix('/')?.split('/') {
            let token = token.replace("~1", "/").replace("~0", "~");
            value = match value {
                Json::Obj(entries) => entries.get_mut(&token)?,
                Json::Arr(items) => items.get_mut(token.parse::<usize>().ok()?)?,
                _ => return None,
            };
        }
        Some(value)
    }

    #[cfg(any(test, feature = "test-support"))]
    pub fn as_arr_mut(&mut self) -> Option<&mut Vec<Json>> {
        match self {
            Json::Arr(items) => Some(items),
            _ => None,
        }
    }
}

/// About the most memory [`convert`] holds at once for the validated JSON
/// `text`, from its tokens alone; it errs high. Every value counts its slot
/// and list entry, every string, key and number its copy, every object a
/// B-tree node and every key its share of the rest.
fn tree_bytes(text: &str) -> usize {
    let bytes = text.as_bytes();
    let mut total: usize = 0;
    let mut index = 0;
    while let Some(&byte) = bytes.get(index) {
        let start = index;
        index += 1;
        let cost = match byte {
            b'"' => {
                // To the closing quote; an escaped one goes with its `\`.
                while let Some(&byte) = bytes.get(index) {
                    index += if byte == b'\\' { 2 } else { 1 };
                    if byte == b'"' {
                        break;
                    }
                }
                VALUE_BYTES + TEXT_BYTES + (index - start)
            }
            b'-' | b'0'..=b'9' => {
                while bytes.get(index).is_some_and(|byte| {
                    matches!(byte, b'0'..=b'9' | b'.' | b'e' | b'E' | b'+' | b'-')
                }) {
                    index += 1;
                }
                VALUE_BYTES + TEXT_BYTES + (index - start)
            }
            b'{' => VALUE_BYTES + NODE_BYTES,
            b':' => ENTRY_BYTES,
            b'[' | b't' | b'f' | b'n' => VALUE_BYTES,
            // `,`, `]`, `}`, whitespace and the rest of `true`, `false`, `null`.
            _ => 0,
        };
        total = total.saturating_add(cost);
    }
    total
}

/// `raw` at `depth` arrays and objects deep.
fn convert(raw: &RawValue, depth: usize) -> Result<Json, ParseError> {
    let text = raw.get();
    let first = text.as_bytes().first();
    if matches!(first, Some(b'{' | b'[')) && depth >= MAX_DEPTH {
        return Err(ParseError::Invalid);
    }
    match first {
        Some(b'{') => {
            // BTreeMap keeps the last of repeated keys, as Python does.
            let entries: BTreeMap<String, &RawValue> =
                serde_json::from_str(text).map_err(|_| ParseError::Invalid)?;
            entries
                .into_iter()
                .map(|(key, value)| Ok((key, convert(value, depth + 1)?)))
                .collect::<Result<_, _>>()
                .map(Json::Obj)
        }
        Some(b'[') => {
            let items: Vec<&RawValue> =
                serde_json::from_str(text).map_err(|_| ParseError::Invalid)?;
            // Exactly as long as the list: no room for doubling to waste.
            let mut values = Vec::with_capacity(items.len());
            for item in items {
                values.push(convert(item, depth + 1)?);
            }
            Ok(Json::Arr(values))
        }
        // A lone surrogate escape is refused here: a `String` is UTF-8.
        Some(b'"') => serde_json::from_str(text)
            .map(Json::Str)
            .map_err(|_| ParseError::Invalid),
        Some(b't') => Ok(Json::Bool(true)),
        Some(b'f') => Ok(Json::Bool(false)),
        Some(b'n') => Ok(Json::Null),
        Some(b'-' | b'0'..=b'9') => number(text),
        _ => Err(ParseError::Invalid),
    }
}

/// A number token serde_json validated: `-?(0|[1-9][0-9]*)(\.[0-9]+)?([eE][-+]?[0-9]+)?`.
fn number(text: &str) -> Result<Json, ParseError> {
    if text.contains(['.', 'e', 'E']) {
        return Ok(Json::Num(text.to_owned()));
    }
    let digits = text.strip_prefix('-').unwrap_or(text);
    if digits.len() > MAX_INTEGER_DIGITS {
        return Err(ParseError::Invalid);
    }
    Ok(Json::Int(if digits == "0" {
        digits.to_owned()
    } else {
        text.to_owned()
    }))
}

/// serde_json's string escapes are exactly Python's
/// `json.dumps(ensure_ascii=False)`: `\"`, `\\`, `\b \f \n \r \t`, the other
/// controls as lowercase `\u00xx`, everything else (DEL, U+2028, `/`) raw.
fn write_string(out: &mut String, text: &str) {
    out.push_str(&serde_json::to_string(text).expect("a string always serializes"));
}

/// `(payload_json, payload_sha256)` of a value: its canonical text and that
/// text's SHA-256.
pub fn payload(value: &Json) -> (String, String) {
    let text = value.canonical();
    let digest = sha256_hex(&text);
    (text, digest)
}

/// The lowercase hex SHA-256 of `text`'s UTF-8 bytes.
pub fn sha256_hex(text: &str) -> String {
    hex::encode(Sha256::digest(text.as_bytes()))
}

/// How a request appears in logs and errors (never the URL): its type and its
/// other keys in sorted order, `settledOutcome(outcome=9975)`, or the type
/// alone, `spotMeta`.
pub fn subject(body: &Json) -> String {
    let text = |value: &Json| match value {
        Json::Str(text) | Json::Int(text) | Json::Num(text) => text.clone(),
        other => other.canonical(),
    };
    let kind = body.get("type").map(text).unwrap_or_default();
    let details: Vec<String> = body
        .as_obj()
        .into_iter()
        .flatten()
        .filter(|(key, _)| key.as_str() != "type")
        .map(|(key, value)| format!("{key}={}", text(value)))
        .collect();
    if details.is_empty() {
        kind
    } else {
        format!("{kind}({})", details.join(","))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn canonical(text: &str) -> String {
        Json::parse(text.as_bytes()).unwrap().canonical()
    }

    #[test]
    fn numbers_keep_their_text_and_keys_sort() {
        assert_eq!(
            canonical(r#"{"b": 1.50, "a": [1e-7, 2, "xé"], "c": null}"#),
            r#"{"a":[1e-7,2,"xé"],"b":1.50,"c":null}"#
        );
        assert_eq!(canonical("-0"), "0");
        assert_eq!(canonical("[-0.0, 1E+07, -5]"), "[-0.0,1E+07,-5]");
        let big = Json::parse(b"12345678901234567890123").unwrap();
        assert_eq!(big, Json::Int("12345678901234567890123".into()));
        assert_eq!(big.as_i64(), None);
        assert_eq!(Json::parse(b"-17").unwrap().as_i64(), Some(-17));
        assert_eq!(Json::parse(b"1.0").unwrap().as_i64(), None);
        assert_eq!(Json::parse(b"true").unwrap().as_i64(), None);
        // The last of repeated keys wins.
        assert_eq!(canonical(r#"{"a":1,"a":2}"#), r#"{"a":2}"#);
    }

    #[test]
    fn payload_hash_is_of_the_canonical_text() {
        let value = Json::obj([("z", Json::Int("1".into())), ("a", Json::Bool(true))]);
        let (text, digest) = payload(&value);
        assert_eq!(text, r#"{"a":true,"z":1}"#);
        assert_eq!(digest, sha256_hex(&text));
        assert_eq!(
            digest,
            "903596123ef8596d132eb70fae4c171f0133ed7cdbe9e3e6a67d01c08c5c6992"
        );
        let same = Json::parse(br#"{"a":true,"z":1}"#).unwrap();
        assert_eq!(payload(&same), (text, digest));
    }

    #[test]
    fn non_standard_json_is_refused() {
        for text in [
            r#"{"a": NaN}"#,
            r#"{"a": Infinity}"#,
            r#"{"a": -Infinity}"#,
            // Python parses a lone surrogate, then fails to hash it (Q5).
            r#""\uD800""#,
            r#"["\uDC00x"]"#,
            "\u{feff}{}",
            "{} {}",
            "[1,]",
            "{trunc",
            "\"a\u{1}b\"",
            "01",
            "+1",
            ".5",
            "1.",
            "",
        ] {
            assert_eq!(
                Json::parse(text.as_bytes()),
                Err(ParseError::Invalid),
                "{text:?}"
            );
        }
        assert_eq!(Json::parse(b"\xff"), Err(ParseError::Invalid));
        let digits = "1".repeat(MAX_INTEGER_DIGITS);
        assert!(Json::parse(digits.as_bytes()).is_ok());
        assert!(Json::parse(format!("-{digits}").as_bytes()).is_ok());
        assert_eq!(
            Json::parse(format!("{digits}1").as_bytes()),
            Err(ParseError::Invalid)
        );
        let deep = |levels: usize| format!("{}{}", "[".repeat(levels), "]".repeat(levels));
        assert!(Json::parse(deep(MAX_DEPTH).as_bytes()).is_ok());
        assert_eq!(
            Json::parse(deep(MAX_DEPTH + 1).as_bytes()),
            Err(ParseError::Invalid)
        );
    }

    #[test]
    fn dense_answers_are_refused_before_their_tree_is_built() {
        // About 2 MB of `{"a":0}` would take about 200 MB as a tree.
        let objects = |n: usize| format!("[{}]", vec![r#"{"a":0}"#; n].join(","));
        let each = tree_bytes(r#"{"a":0}"#);
        assert!(each > 100 * r#"{"a":0},"#.len(), "{each}");
        let over = MAX_TREE_BYTES / each + 1;
        assert!(over * 8 < 4 << 20, "{over} objects");
        assert_eq!(
            Json::parse(objects(over).as_bytes()),
            Err(ParseError::TooLarge)
        );
        // The bound is the tree's, not the text's: what it admits parses.
        let body = objects(1000);
        let bound = tree_bytes(&body);
        assert!(Json::parse_within(body.as_bytes(), bound).is_ok());
        assert_eq!(
            Json::parse_within(body.as_bytes(), bound - 1),
            Err(ParseError::TooLarge)
        );
        // Invalid JSON is invalid, whatever its size.
        assert_eq!(Json::parse_within(b"[0,", 0), Err(ParseError::Invalid));
        // Strings, escapes and literals are counted as tokens: a `{` or a
        // `:` inside a string is text.
        assert_eq!(tree_bytes(r#""{:\"x""#), VALUE_BYTES + TEXT_BYTES + 7);
        assert_eq!(
            tree_bytes(r#"[true, -1.5e+3, null, false]"#),
            5 * VALUE_BYTES + TEXT_BYTES + 7
        );
    }

    #[test]
    fn subjects_name_the_request() {
        let body = Json::obj([
            ("type", Json::Str("fundingHistory".into())),
            ("coin", Json::Str("xyz:XYZ100".into())),
            ("startTime", Json::Int("0".into())),
            ("endTime", Json::Int("100".into())),
        ]);
        assert_eq!(
            subject(&body),
            "fundingHistory(coin=xyz:XYZ100,endTime=100,startTime=0)"
        );
        assert_eq!(
            body.canonical(),
            r#"{"coin":"xyz:XYZ100","endTime":100,"startTime":0,"type":"fundingHistory"}"#
        );
        assert_eq!(
            subject(&Json::obj([("type", Json::Str("spotMeta".into()))])),
            "spotMeta"
        );
    }

    #[test]
    fn edits_reach_nested_values() {
        let mut value = Json::parse(br#"[{"universe":[{"a":1},{"a/b":2}]}]"#).unwrap();
        *value.pointer_mut("/0/universe/1/a~1b").unwrap() = Json::Int("20".into());
        assert_eq!(value.canonical(), r#"[{"universe":[{"a":1},{"a/b":20}]}]"#);
        assert!(value.pointer_mut("/0/missing").is_none());
        assert!(value.pointer_mut("/9").is_none());
        value.as_arr_mut().unwrap().clear();
        assert_eq!(value.canonical(), "[]");
        let meta = Json::parse(br#"{"universe":[],"x":1,"tokens":[]}"#).unwrap();
        assert_eq!(
            meta.without(&["universe", "tokens"]).canonical(),
            r#"{"x":1}"#
        );
    }
}

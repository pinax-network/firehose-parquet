use arrow::array::{Array, ArrayBuilder, BinaryBuilder, ListBuilder, StringBuilder};
use arrow::datatypes::DataType;
use sha2::{Digest, Sha256};
use std::sync::Arc;

// ---------------------------------------------------------------------------
// EncodeBytes strategy
// ---------------------------------------------------------------------------

/// How binary data (hashes, public keys, addresses) is encoded in Parquet output.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EncodeBytes {
    /// Raw bytes (default) — Arrow Binary for maximum fidelity and compact storage.
    Binary,
    /// Hex-encoded strings (0x-prefixed).
    Hex,
    /// Hex-encoded strings (no prefix).
    HexNoPrefix,
    /// Base58-encoded strings.
    Base58,
    /// Tron Base58Check-encoded strings (addresses only; falls back to hex for non-address sizes).
    TronBase58,
}

impl Default for EncodeBytes {
    fn default() -> Self {
        EncodeBytes::Binary
    }
}

/// Parse a stored encoding label into an `EncodeBytes` variant.
/// Returns `None` for legacy `"auto"` metadata so callers can resolve the
/// effective chain-specific contract themselves.
pub fn parse_encode_bytes(s: &str) -> Option<EncodeBytes> {
    match s.to_lowercase().as_str() {
        "binary" => Some(EncodeBytes::Binary),
        "hex" => Some(EncodeBytes::Hex),
        "hex_no_prefix" => Some(EncodeBytes::HexNoPrefix),
        "base58" => Some(EncodeBytes::Base58),
        "tron_base58" => Some(EncodeBytes::TronBase58),
        _ => None, // "auto" or unknown → caller resolves
    }
}

// ---------------------------------------------------------------------------
// Encoding functions
// ---------------------------------------------------------------------------

/// Encode bytes to a 0x-prefixed hex string.
pub fn encode_hex(bytes: &[u8]) -> String {
    encode_bytes(bytes, &EncodeBytes::Hex)
}

/// Encode bytes to a hex string without prefix.
pub fn encode_hex_no_prefix(bytes: &[u8]) -> String {
    encode_bytes(bytes, &EncodeBytes::HexNoPrefix)
}

/// Encode bytes to a base58 string.
pub fn encode_base58(bytes: &[u8]) -> String {
    encode_bytes(bytes, &EncodeBytes::Base58)
}

/// Decode a base58 string to bytes.
pub fn decode_base58(value: &str) -> Result<Vec<u8>, bs58::decode::Error> {
    bs58::decode(value).into_vec()
}

const TRON_VERSION_BYTE: u8 = 0x41;

/// Double-SHA256 then take first 4 bytes.
fn checksum4(data: &[u8]) -> [u8; 4] {
    let hash1 = Sha256::digest(data);
    let hash2 = Sha256::digest(hash1);
    let mut out = [0u8; 4];
    out.copy_from_slice(&hash2[..4]);
    out
}

/// Encode bytes as a Tron Base58Check address.
/// - 20 bytes → prepend version byte (0x41), append checksum, base58 encode.
/// - 21 bytes → assume version byte already present, append checksum, base58 encode.
/// - Other lengths → fall back to hex encoding without `0x`.
pub fn encode_tron_base58(bytes: &[u8]) -> String {
    encode_bytes(bytes, &EncodeBytes::TronBase58)
}

/// Encode bytes according to the given strategy.
pub fn encode_bytes(bytes: &[u8], encoding: &EncodeBytes) -> String {
    let mut out = Vec::new();
    encode_text_into(bytes, encoding, &mut out);
    String::from_utf8(out).expect("hex and base58 encodings are ASCII")
}

/// Replace the contents of `out` with the text encoding of `bytes`.
///
/// Reusing `out` across calls avoids allocating a `String` per value; this is
/// what [`BytesColumn`] and [`BytesListColumn`] do on every append.
///
/// # Panics
/// For [`EncodeBytes::Binary`], which has no text form.
fn encode_text_into(bytes: &[u8], encoding: &EncodeBytes, out: &mut Vec<u8>) {
    out.clear();
    match encoding {
        EncodeBytes::Binary => {
            unreachable!("text encoding requested for Binary encoding")
        }
        EncodeBytes::Hex => {
            out.extend_from_slice(b"0x");
            push_hex(bytes, out);
        }
        EncodeBytes::HexNoPrefix => push_hex(bytes, out),
        EncodeBytes::Base58 => push_base58(bytes, out),
        EncodeBytes::TronBase58 => push_tron_base58(bytes, out),
    }
}

/// Append lowercase hex digits of `bytes` to `out`.
fn push_hex(bytes: &[u8], out: &mut Vec<u8>) {
    let start = out.len();
    out.resize(start + bytes.len() * 2, 0);
    hex::encode_to_slice(bytes, &mut out[start..]).expect("output sized for the input");
}

/// Append the base58 encoding of `bytes` to `out`.
fn push_base58(bytes: &[u8], out: &mut Vec<u8>) {
    bs58::encode(bytes)
        .onto(out)
        .expect("a Vec target always has room");
}

/// Append a Tron Base58Check address to `out` (see [`encode_tron_base58`]).
fn push_tron_base58(bytes: &[u8], out: &mut Vec<u8>) {
    let mut data = [0u8; 25];
    let payload = match bytes.len() {
        20 => {
            data[0] = TRON_VERSION_BYTE;
            data[1..21].copy_from_slice(bytes);
            21
        }
        21 => {
            data[..21].copy_from_slice(bytes);
            21
        }
        _ => return push_hex(bytes, out),
    };
    let checksum = checksum4(&data[..payload]);
    data[payload..].copy_from_slice(&checksum);
    push_base58(&data, out);
}

/// Encode into `scratch` and return the text, for appending to a string builder.
fn encode_text<'a>(bytes: &[u8], encoding: &EncodeBytes, scratch: &'a mut Vec<u8>) -> &'a str {
    encode_text_into(bytes, encoding, scratch);
    std::str::from_utf8(scratch).expect("hex and base58 encodings are ASCII")
}

// ---------------------------------------------------------------------------
// ID re-encoding (block_id / parent_id from Firehose metadata)
// ---------------------------------------------------------------------------

/// Re-encode a hex ID string (from Firehose metadata) through the chosen encoding.
/// For Binary mode, returns the string as-is (canonical fields are always Utf8).
/// For string modes, decodes the hex string to bytes and re-encodes.
/// If the hex string has a `0x` prefix, it is stripped before decoding.
pub fn encode_id(id: &str, encoding: &EncodeBytes) -> String {
    match encoding {
        EncodeBytes::Binary => id.to_string(),
        _ => {
            let hex_str = id.strip_prefix("0x").unwrap_or(id);
            match hex::decode(hex_str) {
                Ok(bytes) => encode_bytes(&bytes, encoding),
                Err(_) => id.to_string(), // not valid hex, keep as-is
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Arrow DataType helper
// ---------------------------------------------------------------------------

/// Returns the Arrow DataType for a byte field given the encoding strategy.
pub fn bytes_data_type(encoding: &EncodeBytes) -> DataType {
    match encoding {
        EncodeBytes::Binary => DataType::Binary,
        _ => DataType::Utf8,
    }
}

// ---------------------------------------------------------------------------
// EncodedBytes — a byte value encoded once, appended many times
// ---------------------------------------------------------------------------

/// A byte value already encoded for a [`BytesColumn`], for values that repeat on
/// many rows (e.g. the canonical `block_id`), so they are encoded only once.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EncodedBytes {
    Binary(Vec<u8>),
    String(String, EncodeBytes),
}

impl EncodedBytes {
    /// Encode `bytes` the way a [`BytesColumn`] with `encoding` would.
    pub fn new(bytes: &[u8], encoding: &EncodeBytes) -> Self {
        match encoding {
            EncodeBytes::Binary => EncodedBytes::Binary(bytes.to_vec()),
            other => EncodedBytes::String(encode_bytes(bytes, other), other.clone()),
        }
    }
}

// ---------------------------------------------------------------------------
// BytesColumn — unified builder for byte fields
// ---------------------------------------------------------------------------

/// A column builder that stores either raw binary or encoded strings,
/// depending on the EncodeBytes strategy.
///
/// String columns encode into a reusable scratch buffer, so appends don't
/// allocate per value.
pub enum BytesColumn {
    Binary(BinaryBuilder),
    String(StringBuilder, EncodeBytes, Vec<u8>),
}

impl BytesColumn {
    pub fn new(encoding: &EncodeBytes) -> Self {
        match encoding {
            EncodeBytes::Binary => BytesColumn::Binary(BinaryBuilder::new()),
            other => BytesColumn::String(StringBuilder::new(), other.clone(), Vec::new()),
        }
    }

    /// Append raw bytes, encoding them according to the column strategy.
    pub fn append_value(&mut self, bytes: &[u8]) {
        match self {
            BytesColumn::Binary(b) => b.append_value(bytes),
            BytesColumn::String(b, enc, scratch) => {
                b.append_value(encode_text(bytes, enc, scratch))
            }
        }
    }

    /// Encode `bytes` once for repeated [`BytesColumn::append_encoded`] calls.
    pub fn encode(&self, bytes: &[u8]) -> EncodedBytes {
        match self {
            BytesColumn::Binary(_) => EncodedBytes::Binary(bytes.to_vec()),
            BytesColumn::String(_, enc, _) => EncodedBytes::new(bytes, enc),
        }
    }

    /// Append a value encoded by [`BytesColumn::encode`] (or [`EncodedBytes::new`])
    /// with this column's encoding.
    ///
    /// # Panics
    /// If `value` was encoded for a different encoding.
    pub fn append_encoded(&mut self, value: &EncodedBytes) {
        match (self, value) {
            (BytesColumn::Binary(b), EncodedBytes::Binary(bytes)) => b.append_value(bytes),
            (BytesColumn::String(b, enc, _), EncodedBytes::String(s, value_enc))
                if enc == value_enc =>
            {
                b.append_value(s)
            }
            (column, value) => panic!(
                "value encoded as {value:?} does not match the column encoding {:?}",
                column.encoding()
            ),
        }
    }

    /// The encoding this column writes.
    pub fn encoding(&self) -> EncodeBytes {
        match self {
            BytesColumn::Binary(_) => EncodeBytes::Binary,
            BytesColumn::String(_, enc, _) => enc.clone(),
        }
    }

    /// Append a null value.
    pub fn append_null(&mut self) {
        match self {
            BytesColumn::Binary(b) => b.append_null(),
            BytesColumn::String(b, _, _) => b.append_null(),
        }
    }

    /// Finish the builder and produce an Arrow Array.
    ///
    /// The builder is re-created with the capacity the finished batch used, so the
    /// next batch of similar size does not regrow it from empty.
    pub fn finish(&mut self) -> Arc<dyn Array> {
        match self {
            BytesColumn::Binary(b) => {
                let (items, bytes) = (b.len(), b.values_slice().len());
                let array = b.finish();
                *b = BinaryBuilder::with_capacity(items, bytes);
                Arc::new(array)
            }
            BytesColumn::String(b, _, _) => {
                let (items, bytes) = (b.len(), b.values_slice().len());
                let array = b.finish();
                *b = StringBuilder::with_capacity(items, bytes);
                Arc::new(array)
            }
        }
    }

    pub fn len(&self) -> usize {
        match self {
            BytesColumn::Binary(b) => b.len(),
            BytesColumn::String(b, _, _) => b.len(),
        }
    }

    /// Estimate in-memory byte usage (offsets + values).
    pub fn estimated_bytes(&self) -> usize {
        match self {
            BytesColumn::Binary(b) => b.values_slice().len() + (b.len() + 1) * 4,
            BytesColumn::String(b, _, _) => b.values_slice().len() + (b.len() + 1) * 4,
        }
    }
}

// ---------------------------------------------------------------------------
// BytesListColumn — unified list builder for lists of byte fields
// ---------------------------------------------------------------------------

/// A list column builder for lists of byte values (e.g. account_keys, witness).
pub enum BytesListColumn {
    Binary(ListBuilder<BinaryBuilder>),
    String(ListBuilder<StringBuilder>, EncodeBytes, Vec<u8>),
}

impl BytesListColumn {
    pub fn new(encoding: &EncodeBytes) -> Self {
        match encoding {
            EncodeBytes::Binary => BytesListColumn::Binary(ListBuilder::new(BinaryBuilder::new())),
            other => BytesListColumn::String(
                ListBuilder::new(StringBuilder::new()),
                other.clone(),
                Vec::new(),
            ),
        }
    }

    /// Append a single value to the current list entry.
    pub fn append_value(&mut self, bytes: &[u8]) {
        match self {
            BytesListColumn::Binary(b) => b.values().append_value(bytes),
            BytesListColumn::String(b, enc, scratch) => {
                b.values().append_value(encode_text(bytes, enc, scratch))
            }
        }
    }

    /// Finish the current list entry.
    pub fn append(&mut self, is_valid: bool) {
        match self {
            BytesListColumn::Binary(b) => b.append(is_valid),
            BytesListColumn::String(b, _, _) => b.append(is_valid),
        }
    }

    /// Finish the builder and produce an Arrow Array.
    pub fn finish(&mut self) -> Arc<dyn Array> {
        match self {
            BytesListColumn::Binary(b) => Arc::new(b.finish()),
            BytesListColumn::String(b, _, _) => Arc::new(b.finish()),
        }
    }

    /// Arrow DataType for the list schema field.
    pub fn data_type(encoding: &EncodeBytes) -> DataType {
        use arrow::datatypes::Field;
        DataType::List(Arc::new(Field::new(
            "item",
            bytes_data_type(encoding),
            true,
        )))
    }

    /// Estimate in-memory byte usage (offsets + inner builder).
    pub fn estimated_bytes(&mut self) -> usize {
        let inner = match self {
            BytesListColumn::Binary(b) => {
                let v = b.values();
                v.values_slice().len() + (v.len() + 1) * 4
            }
            BytesListColumn::String(b, _, _) => {
                let v = b.values();
                v.values_slice().len() + (v.len() + 1) * 4
            }
        };
        let len = match self {
            BytesListColumn::Binary(b) => b.len(),
            BytesListColumn::String(b, _, _) => b.len(),
        };
        (len + 1) * 4 + inner
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::StringArray;

    #[test]
    fn test_encode_hex() {
        assert_eq!(encode_hex(&[0xde, 0xad, 0xbe, 0xef]), "0xdeadbeef");
        assert_eq!(encode_hex(&[]), "0x");
    }

    #[test]
    fn test_encode_base58() {
        let bytes = [1u8; 32];
        let encoded = encode_base58(&bytes);
        assert!(!encoded.is_empty());
        // Base58 of 32 ones should be deterministic
        assert_eq!(encoded, bs58::encode(&[1u8; 32]).into_string());
    }

    #[test]
    fn test_encode_tron_base58_20_bytes() {
        // 20-byte address
        let addr = [0x41u8; 20];
        let encoded = encode_tron_base58(&addr);
        // Should produce a valid Tron address (starts with T typically for main addresses)
        assert!(!encoded.is_empty());
        // The result should be a valid base58 string
        assert!(bs58::decode(&encoded).into_vec().is_ok());
    }

    #[test]
    fn test_encode_tron_base58_21_bytes() {
        // 21 bytes: version byte + 20-byte body
        let mut addr = vec![TRON_VERSION_BYTE];
        addr.extend_from_slice(&[0x42u8; 20]);
        let encoded = encode_tron_base58(&addr);
        assert!(!encoded.is_empty());
        assert!(bs58::decode(&encoded).into_vec().is_ok());
    }

    #[test]
    fn test_encode_tron_base58_falls_back_to_hex() {
        // Non-address size (e.g., 32-byte hash) → falls back to hex
        let hash = [0xab; 32];
        let encoded = encode_tron_base58(&hash);
        assert!(!encoded.starts_with("0x"));
        assert_eq!(encoded, encode_hex_no_prefix(&hash));
    }

    #[test]
    fn test_encode_id_tron_base58_falls_back_to_hex_no_prefix() {
        assert_eq!(
            encode_id("0xdeadbeef", &EncodeBytes::TronBase58),
            "deadbeef"
        );
        assert_eq!(encode_id("deadbeef", &EncodeBytes::TronBase58), "deadbeef");
    }

    #[test]
    fn test_encode_hex_no_prefix() {
        assert_eq!(encode_hex_no_prefix(&[0xde, 0xad, 0xbe, 0xef]), "deadbeef");
        assert_eq!(encode_hex_no_prefix(&[]), "");
    }

    #[test]
    fn test_encode_id_hex() {
        // Without 0x prefix in input
        assert_eq!(encode_id("deadbeef", &EncodeBytes::Hex), "0xdeadbeef");
        // With 0x prefix in input
        assert_eq!(encode_id("0xdeadbeef", &EncodeBytes::Hex), "0xdeadbeef");
    }

    #[test]
    fn test_encode_id_hex_no_prefix() {
        assert_eq!(
            encode_id("0xdeadbeef", &EncodeBytes::HexNoPrefix),
            "deadbeef"
        );
        assert_eq!(encode_id("deadbeef", &EncodeBytes::HexNoPrefix), "deadbeef");
    }

    #[test]
    fn test_encode_id_binary_passthrough() {
        assert_eq!(encode_id("deadbeef", &EncodeBytes::Binary), "deadbeef");
        assert_eq!(encode_id("0xdeadbeef", &EncodeBytes::Binary), "0xdeadbeef");
    }

    #[test]
    fn test_encode_id_invalid_hex() {
        // Non-hex string should be returned as-is
        assert_eq!(encode_id("not_hex!", &EncodeBytes::Hex), "not_hex!");
    }

    #[test]
    fn test_parse_encode_bytes_hex_no_prefix() {
        assert_eq!(
            parse_encode_bytes("hex_no_prefix"),
            Some(EncodeBytes::HexNoPrefix)
        );
        assert_eq!(
            parse_encode_bytes("HEX_NO_PREFIX"),
            Some(EncodeBytes::HexNoPrefix)
        );
    }

    #[test]
    fn test_bytes_data_type() {
        assert_eq!(bytes_data_type(&EncodeBytes::Binary), DataType::Binary);
        assert_eq!(bytes_data_type(&EncodeBytes::Hex), DataType::Utf8);
        assert_eq!(bytes_data_type(&EncodeBytes::HexNoPrefix), DataType::Utf8);
        assert_eq!(bytes_data_type(&EncodeBytes::Base58), DataType::Utf8);
        assert_eq!(bytes_data_type(&EncodeBytes::TronBase58), DataType::Utf8);
    }

    #[test]
    fn test_parse_encode_bytes() {
        assert_eq!(parse_encode_bytes("binary"), Some(EncodeBytes::Binary));
        assert_eq!(parse_encode_bytes("hex"), Some(EncodeBytes::Hex));
        assert_eq!(parse_encode_bytes("base58"), Some(EncodeBytes::Base58));
        assert_eq!(
            parse_encode_bytes("tron_base58"),
            Some(EncodeBytes::TronBase58)
        );
        assert_eq!(parse_encode_bytes("auto"), None);
        assert_eq!(parse_encode_bytes("BINARY"), Some(EncodeBytes::Binary));
        assert_eq!(parse_encode_bytes("HEX"), Some(EncodeBytes::Hex));
    }

    #[test]
    fn test_bytes_column_binary() {
        let mut col = BytesColumn::new(&EncodeBytes::Binary);
        col.append_value(&[1, 2, 3]);
        col.append_value(&[4, 5]);
        assert_eq!(col.len(), 2);

        let arr = col.finish();
        assert_eq!(arr.len(), 2);
        assert_eq!(*arr.data_type(), DataType::Binary);
    }

    #[test]
    fn test_bytes_column_hex() {
        let mut col = BytesColumn::new(&EncodeBytes::Hex);
        col.append_value(&[0xde, 0xad]);
        assert_eq!(col.len(), 1);

        let arr = col.finish();
        assert_eq!(arr.len(), 1);
        assert_eq!(*arr.data_type(), DataType::Utf8);
        let string_arr = arr
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(string_arr.value(0), "0xdead");
    }

    #[test]
    fn test_bytes_column_base58() {
        let mut col = BytesColumn::new(&EncodeBytes::Base58);
        col.append_value(&[1, 2, 3]);
        let arr = col.finish();
        assert_eq!(*arr.data_type(), DataType::Utf8);
        let string_arr = arr
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(string_arr.value(0), bs58::encode(&[1, 2, 3]).into_string());
    }

    #[test]
    fn test_bytes_list_column_binary() {
        let mut col = BytesListColumn::new(&EncodeBytes::Binary);
        col.append_value(&[1, 2]);
        col.append_value(&[3, 4]);
        col.append(true);
        let arr = col.finish();
        assert_eq!(arr.len(), 1);
    }

    #[test]
    fn test_bytes_list_column_hex() {
        let mut col = BytesListColumn::new(&EncodeBytes::Hex);
        col.append_value(&[0xff]);
        col.append(true);
        let arr = col.finish();
        assert_eq!(arr.len(), 1);
    }

    /// The per-value encoders used before the scratch-buffer rewrite.
    fn reference_encode(bytes: &[u8], encoding: &EncodeBytes) -> String {
        match encoding {
            EncodeBytes::Binary => unreachable!(),
            EncodeBytes::Hex => format!("0x{}", hex::encode(bytes)),
            EncodeBytes::HexNoPrefix => hex::encode(bytes),
            EncodeBytes::Base58 => bs58::encode(bytes).into_string(),
            EncodeBytes::TronBase58 => match bytes.len() {
                20 | 21 => {
                    let mut data = Vec::with_capacity(25);
                    if bytes.len() == 20 {
                        data.push(TRON_VERSION_BYTE);
                    }
                    data.extend_from_slice(bytes);
                    let checksum = checksum4(&data);
                    data.extend_from_slice(&checksum);
                    bs58::encode(data).into_string()
                }
                _ => hex::encode(bytes),
            },
        }
    }

    /// Inputs of every length up to 70 bytes: all zeros, leading zeros, all 0xff,
    /// and pseudo-random bytes.
    fn sample_values() -> Vec<Vec<u8>> {
        let mut state = 0x9e37_79b9_7f4a_7c15_u64;
        let mut next = move || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state as u8
        };
        let mut values = Vec::new();
        for len in 0..=70 {
            values.push(vec![0u8; len]);
            values.push(vec![0xff; len]);
            let random: Vec<u8> = (0..len).map(|_| next()).collect();
            let mut leading_zeros = random.clone();
            leading_zeros.iter_mut().take(len / 3).for_each(|b| *b = 0);
            values.push(random);
            values.push(leading_zeros);
        }
        values
    }

    const TEXT_ENCODINGS: [EncodeBytes; 4] = [
        EncodeBytes::Hex,
        EncodeBytes::HexNoPrefix,
        EncodeBytes::Base58,
        EncodeBytes::TronBase58,
    ];

    #[test]
    fn test_encoders_match_the_previous_per_value_implementations() {
        let values = sample_values();
        for encoding in &TEXT_ENCODINGS {
            for value in &values {
                assert_eq!(
                    encode_bytes(value, encoding),
                    reference_encode(value, encoding),
                    "{encoding:?} of {value:02x?}"
                );
            }
        }
        assert_eq!(encode_hex(&[0xde, 0xad]), "0xdead");
        assert_eq!(encode_hex_no_prefix(&[0xde, 0xad]), "dead");
        assert_eq!(
            encode_base58(&[0, 0, 1]),
            bs58::encode([0, 0, 1]).into_string()
        );
        assert_eq!(
            encode_tron_base58(&[0x11; 20]),
            reference_encode(&[0x11; 20], &EncodeBytes::TronBase58)
        );
    }

    #[test]
    fn test_bytes_columns_reuse_the_scratch_buffer_without_leaking_values() {
        // Long and short values interleaved, so a stale scratch tail would show.
        let values = sample_values();
        for encoding in &TEXT_ENCODINGS {
            let mut column = BytesColumn::new(encoding);
            let mut list = BytesListColumn::new(encoding);
            for value in values.iter().rev().chain(values.iter()) {
                column.append_value(value);
                list.append_value(value);
                list.append(true);
            }
            let array = column.finish();
            let strings = array.as_any().downcast_ref::<StringArray>().unwrap();
            let list_array = list.finish();
            let lists = list_array
                .as_any()
                .downcast_ref::<arrow::array::ListArray>()
                .unwrap();
            for (row, value) in values.iter().rev().chain(values.iter()).enumerate() {
                let expected = reference_encode(value, encoding);
                assert_eq!(strings.value(row), expected, "{encoding:?} row {row}");
                let item = lists.value(row);
                let item = item.as_any().downcast_ref::<StringArray>().unwrap();
                assert_eq!(item.value(0), expected, "{encoding:?} list row {row}");
            }
        }
    }

    #[test]
    fn test_bytes_column_finish_keeps_capacity_and_output() {
        let mut column = BytesColumn::new(&EncodeBytes::Hex);
        for _ in 0..3 {
            column.append_value(&[0xab; 32]);
            column.append_null();
            let array = column.finish();
            assert_eq!(array.len(), 2);
            assert_eq!(array.null_count(), 1);
            let strings = array.as_any().downcast_ref::<StringArray>().unwrap();
            assert_eq!(strings.value(0), format!("0x{}", "ab".repeat(32)));
            assert_eq!(column.len(), 0);
        }
    }

    /// Per-value cost of `BytesColumn::append_value` (encode + append), with a
    /// flush every 10,000 values like a mapper would do.
    /// `cargo test --release -p firehose-parquet --lib bench_bytes_column -- --ignored --nocapture`
    #[test]
    #[ignore = "benchmark"]
    fn bench_bytes_column_append_value() {
        use std::hint::black_box;
        use std::time::Instant;

        const VALUES: usize = 2_000_000;
        const FLUSH_EVERY: usize = 10_000;
        let hash = [0xab_u8; 32];
        let address = [0x41_u8; 20];
        let cases: [(&str, EncodeBytes, &[u8]); 6] = [
            ("binary, 32 B", EncodeBytes::Binary, &hash),
            ("hex, 32 B", EncodeBytes::Hex, &hash),
            ("hex_no_prefix, 32 B", EncodeBytes::HexNoPrefix, &hash),
            ("base58, 32 B", EncodeBytes::Base58, &hash),
            (
                "tron_base58, 20 B address",
                EncodeBytes::TronBase58,
                &address,
            ),
            ("tron_base58, 32 B hash", EncodeBytes::TronBase58, &hash),
        ];
        for (label, encoding, value) in cases {
            let mut column = BytesColumn::new(&encoding);
            let start = Instant::now();
            for i in 0..VALUES {
                column.append_value(black_box(value));
                if (i + 1) % FLUSH_EVERY == 0 {
                    black_box(column.finish());
                }
            }
            let elapsed = start.elapsed();
            println!(
                "{label}: {:.1} ns/value",
                elapsed.as_nanos() as f64 / VALUES as f64
            );
        }
    }
}

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

/// Parse a CLI string into an EncodeBytes variant.
/// Returns `None` for "auto" — the caller (chain binary) is expected to resolve
/// auto to the chain-appropriate encoding before constructing the mapper.
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
    format!("0x{}", hex::encode(bytes))
}

/// Encode bytes to a hex string without prefix.
pub fn encode_hex_no_prefix(bytes: &[u8]) -> String {
    hex::encode(bytes)
}

/// Encode bytes to a base58 string.
pub fn encode_base58(bytes: &[u8]) -> String {
    bs58::encode(bytes).into_string()
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
    match bytes.len() {
        20 => {
            let mut data = Vec::with_capacity(25);
            data.push(TRON_VERSION_BYTE);
            data.extend_from_slice(bytes);
            let chk = checksum4(&data);
            data.extend_from_slice(&chk);
            bs58::encode(data).into_string()
        }
        21 => {
            let mut data = bytes.to_vec();
            let chk = checksum4(&data);
            data.extend_from_slice(&chk);
            bs58::encode(data).into_string()
        }
        _ => encode_hex_no_prefix(bytes),
    }
}

/// Encode bytes according to the given strategy.
pub fn encode_bytes(bytes: &[u8], encoding: &EncodeBytes) -> String {
    match encoding {
        EncodeBytes::Binary => {
            unreachable!("encode_bytes should not be called for Binary encoding")
        }
        EncodeBytes::Hex => encode_hex(bytes),
        EncodeBytes::HexNoPrefix => encode_hex_no_prefix(bytes),
        EncodeBytes::Base58 => encode_base58(bytes),
        EncodeBytes::TronBase58 => encode_tron_base58(bytes),
    }
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
// BytesColumn — unified builder for byte fields
// ---------------------------------------------------------------------------

/// A column builder that stores either raw binary or encoded strings,
/// depending on the EncodeBytes strategy.
pub enum BytesColumn {
    Binary(BinaryBuilder),
    String(StringBuilder, EncodeBytes),
}

impl BytesColumn {
    pub fn new(encoding: &EncodeBytes) -> Self {
        match encoding {
            EncodeBytes::Binary => BytesColumn::Binary(BinaryBuilder::new()),
            other => BytesColumn::String(StringBuilder::new(), other.clone()),
        }
    }

    /// Append raw bytes, encoding them according to the column strategy.
    pub fn append_value(&mut self, bytes: &[u8]) {
        match self {
            BytesColumn::Binary(b) => b.append_value(bytes),
            BytesColumn::String(b, enc) => {
                let s = encode_bytes(bytes, enc);
                b.append_value(&s);
            }
        }
    }

    /// Append a null value.
    pub fn append_null(&mut self) {
        match self {
            BytesColumn::Binary(b) => b.append_null(),
            BytesColumn::String(b, _) => b.append_null(),
        }
    }

    /// Finish the builder and produce an Arrow Array.
    pub fn finish(&mut self) -> Arc<dyn Array> {
        match self {
            BytesColumn::Binary(b) => Arc::new(b.finish()),
            BytesColumn::String(b, _) => Arc::new(b.finish()),
        }
    }

    pub fn len(&self) -> usize {
        match self {
            BytesColumn::Binary(b) => b.len(),
            BytesColumn::String(b, _) => b.len(),
        }
    }

    /// Estimate in-memory byte usage (offsets + values).
    pub fn estimated_bytes(&self) -> usize {
        match self {
            BytesColumn::Binary(b) => b.values_slice().len() + (b.len() + 1) * 4,
            BytesColumn::String(b, _) => b.values_slice().len() + (b.len() + 1) * 4,
        }
    }
}

// ---------------------------------------------------------------------------
// BytesListColumn — unified list builder for lists of byte fields
// ---------------------------------------------------------------------------

/// A list column builder for lists of byte values (e.g. account_keys, witness).
pub enum BytesListColumn {
    Binary(ListBuilder<BinaryBuilder>),
    String(ListBuilder<StringBuilder>, EncodeBytes),
}

impl BytesListColumn {
    pub fn new(encoding: &EncodeBytes) -> Self {
        match encoding {
            EncodeBytes::Binary => BytesListColumn::Binary(ListBuilder::new(BinaryBuilder::new())),
            other => BytesListColumn::String(ListBuilder::new(StringBuilder::new()), other.clone()),
        }
    }

    /// Append a single value to the current list entry.
    pub fn append_value(&mut self, bytes: &[u8]) {
        match self {
            BytesListColumn::Binary(b) => b.values().append_value(bytes),
            BytesListColumn::String(b, enc) => {
                let s = encode_bytes(bytes, enc);
                b.values().append_value(&s);
            }
        }
    }

    /// Finish the current list entry.
    pub fn append(&mut self, is_valid: bool) {
        match self {
            BytesListColumn::Binary(b) => b.append(is_valid),
            BytesListColumn::String(b, _) => b.append(is_valid),
        }
    }

    /// Finish the builder and produce an Arrow Array.
    pub fn finish(&mut self) -> Arc<dyn Array> {
        match self {
            BytesListColumn::Binary(b) => Arc::new(b.finish()),
            BytesListColumn::String(b, _) => Arc::new(b.finish()),
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
            BytesListColumn::String(b, _) => {
                let v = b.values();
                v.values_slice().len() + (v.len() + 1) * 4
            }
        };
        let len = match self {
            BytesListColumn::Binary(b) => b.len(),
            BytesListColumn::String(b, _) => b.len(),
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
}

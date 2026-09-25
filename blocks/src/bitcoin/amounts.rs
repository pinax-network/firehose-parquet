//! Recover output base units without relying on a floating-point SQL column.
use super::proto::btc;
use anyhow::{ensure, Context, Result};

const UNITS: f64 = 100_000_000.0;
const MAX_EXACT_INTEGER: u64 = (1_u64 << 53) - 1;

pub(super) fn transaction_satoshis(tx: &btc::Transaction) -> Result<Vec<u64>> {
    if tx.hex.is_empty() {
        return tx
            .vout
            .iter()
            .enumerate()
            .map(|(index, output)| {
                value_satoshis(output.value).with_context(|| format!("output {index}"))
            })
            .collect();
    }
    // Bitcoin-family transaction serialization stores output values directly as
    // signed 64-bit little-endian integers. The same prefix supports Litecoin;
    // do not apply Bitcoin's monetary-supply bound to that shared mapper.
    let values = serialized_output_values(&tx.hex)?;
    ensure!(
        values.len() == tx.vout.len(),
        "serialized/decoded output counts differ"
    );
    for (index, (value, output)) in values.iter().zip(&tx.vout).enumerate() {
        ensure!(
            output.n as usize == index,
            "output {index} has inconsistent index"
        );
        ensure!(
            output.value.is_finite() && canonical_coin_value(*value) == output.value,
            "output {index} has inconsistent serialized and BTC values"
        );
    }
    Ok(values)
}

/// Fallback for older payloads without serialized transaction bytes. Accept
/// only a unique integer that recreates the supplied double. At large values
/// adjacent base units can share one f64; those require raw transaction bytes.
pub(super) fn value_satoshis(value: f64) -> Result<u64> {
    ensure!(value.is_finite() && value >= 0.0 && value <= MAX_EXACT_INTEGER as f64 / UNITS,
        "output value cannot be recovered exactly from the BTC double; serialized transaction required");
    let center = (value * UNITS).round() as u64;
    let mut exact = None;
    // Below 2^53, input rounding + multiplication moves the result by less
    // than two base units. Inspect neighbors and reject ambiguous encodings.
    for candidate in center.saturating_sub(2)..=center.saturating_add(2) {
        if canonical_coin_value(candidate) == value {
            ensure!(
                exact.is_none(),
                "ambiguous BTC double; serialized transaction required"
            );
            exact = Some(candidate);
        }
    }
    let exact = exact.context("output value does not represent a whole number of satoshis")?;
    ensure!(
        exact <= MAX_EXACT_INTEGER,
        "serialized transaction required for this output value"
    );
    Ok(exact)
}

// Parse the exact base-10 coin amount, avoiding a second rounding from casting
// a serialized integer larger than 2^53 to f64 before division.
fn canonical_coin_value(satoshis: u64) -> f64 {
    format!("{}.{:08}", satoshis / 100_000_000, satoshis % 100_000_000)
        .parse()
        .expect("bounded integer decimal is a valid finite f64")
}

fn serialized_output_values(hex: &str) -> Result<Vec<u64>> {
    ensure!(
        hex.len() % 2 == 0,
        "serialized transaction hex has odd length"
    );
    fn nibble(byte: u8) -> Result<u8> {
        match byte {
            b'0'..=b'9' => Ok(byte - b'0'),
            b'a'..=b'f' => Ok(byte - b'a' + 10),
            b'A'..=b'F' => Ok(byte - b'A' + 10),
            _ => anyhow::bail!("serialized transaction contains invalid hex"),
        }
    }
    let data = hex
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| Ok(nibble(pair[0])? * 16 + nibble(pair[1])?))
        .collect::<Result<Vec<_>>>()?;
    let mut reader = Reader(&data);
    reader.take(4)?; // version
    if reader.0.first() == Some(&0) {
        reader.take(1)?; // extended serialization marker
        ensure!(
            reader.take(1)?[0] != 0,
            "serialized transaction has an empty flag"
        );
    }
    let inputs = reader.compact_size()?;
    ensure!(
        inputs <= reader.0.len() / 41,
        "serialized input count exceeds payload"
    );
    for _ in 0..inputs {
        reader.take(36)?; // previous txid and output index
        let script = reader.compact_size()?;
        reader.take(script)?;
        reader.take(4)?; // sequence
    }
    let outputs = reader.compact_size()?;
    ensure!(
        outputs <= reader.0.len() / 9,
        "serialized output count exceeds payload"
    );
    let mut amounts = Vec::with_capacity(outputs);
    for _ in 0..outputs {
        let value = i64::from_le_bytes(reader.take(8)?.try_into().unwrap());
        ensure!(value >= 0, "serialized output value is negative");
        amounts.push(value as u64);
        let script = reader.compact_size()?;
        reader.take(script)?;
    }
    // Only the output prefix is decoded. Witnesses, locktime and family-specific
    // extensions are not consensus-validated here. They cannot alter these values.
    Ok(amounts)
}

struct Reader<'a>(&'a [u8]);
impl<'a> Reader<'a> {
    fn take(&mut self, len: usize) -> Result<&'a [u8]> {
        ensure!(len <= self.0.len(), "truncated serialized transaction");
        let (value, rest) = self.0.split_at(len);
        self.0 = rest;
        Ok(value)
    }
    fn compact_size(&mut self) -> Result<usize> {
        let prefix = self.take(1)?[0];
        let value = match prefix {
            0..=252 => u64::from(prefix),
            253 => {
                let n = u16::from_le_bytes(self.take(2)?.try_into().unwrap());
                ensure!(n >= 253, "noncanonical compact size");
                u64::from(n)
            }
            254 => {
                let n = u32::from_le_bytes(self.take(4)?.try_into().unwrap());
                ensure!(n > u16::MAX as u32, "noncanonical compact size");
                u64::from(n)
            }
            255 => {
                let n = u64::from_le_bytes(self.take(8)?.try_into().unwrap());
                ensure!(n > u32::MAX as u64, "noncanonical compact size");
                n
            }
        };
        usize::try_from(value).context("compact size exceeds platform address space")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn transaction(values: &[i64], extended: bool) -> btc::Transaction {
        let mut bytes = vec![1, 0, 0, 0];
        if extended {
            bytes.extend([0, 1]);
        }
        bytes.push(1); // one input
        bytes.extend([0; 36]);
        bytes.extend([0, 255, 255, 255, 255]); // empty script + sequence
        bytes.push(values.len().try_into().unwrap());
        for value in values {
            bytes.extend(value.to_le_bytes());
            bytes.push(0);
        }
        bytes.extend([0; 4]); // locktime (prefix decoder does not interpret it)
        btc::Transaction {
            hex: bytes.iter().map(|byte| format!("{byte:02x}")).collect(),
            vout: values
                .iter()
                .enumerate()
                .map(|(index, value)| btc::Vout {
                    n: index as u32,
                    value: *value as f64 / UNITS,
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }
    }
    #[test]
    fn serialized_values_preserve_units_including_ambiguous_large_family_amounts() {
        let amounts = [0, 1, 123_456_789, 8_400_000_000_000_001];
        assert!(value_satoshis(amounts[3] as f64 / UNITS).is_err());
        for extended in [false, true] {
            assert_eq!(
                transaction_satoshis(&transaction(&amounts, extended)).unwrap(),
                amounts.map(|n| n as u64)
            );
        }
    }
    #[test]
    fn inconsistent_or_malformed_serialized_values_fail_without_float_fallback() {
        let mut mismatched = transaction(&[1], false);
        mismatched.vout[0].value = 0.1;
        assert!(transaction_satoshis(&mismatched).is_err());
        mismatched = transaction(&[1], false);
        mismatched.vout[0].n = 1;
        assert!(transaction_satoshis(&mismatched).is_err());
        mismatched = transaction(&[1], false);
        mismatched.vout.clear();
        assert!(transaction_satoshis(&mismatched).is_err());
        assert!(transaction_satoshis(&transaction(&[-1], false)).is_err());
        for hex in ["0", "zz", "0100000000", "0100000001", "01000000feffffffff"] {
            let mut tx = transaction(&[1], false);
            tx.hex = hex.into();
            assert!(transaction_satoshis(&tx).is_err(), "accepted {hex}");
        }
        let tx = transaction(&[1], false);
        for end in (0..tx.hex.len() - 10).step_by(2).skip(1) {
            let mut truncated = tx.clone();
            truncated.hex.truncate(end);
            assert!(transaction_satoshis(&truncated).is_err());
        }
    }
}

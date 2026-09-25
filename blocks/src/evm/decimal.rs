//! Decimal strings for EVM big integers.
//!
//! Firehose `BigInt` values (balances, values, gas prices, fee caps) are
//! big-endian unsigned bytes, written to Parquet as decimal strings. This runs
//! for every transaction value, gas price, call value and balance change, so it
//! is on the hot path of EVM mapping.
//!
//! - Values of up to 16 bytes are read into a `u128` and split into base-10^19
//!   chunks (at most two 128-bit divisions); each chunk is a `u64`.
//! - Longer values are read into 64-bit limbs and repeatedly divided by 10^19,
//!   limb by limb. Every step divides a 128-bit value whose high half is below
//!   10^19, so the quotient fits a `u64`.
//! - Digits are written right to left into a stack buffer, so nothing is
//!   allocated for values of up to 32 bytes (78 digits).

use arrow::array::StringBuilder;

/// 10^19, the largest power of ten below 2^64.
const CHUNK: u64 = 10_000_000_000_000_000_000;
const CHUNK_DIGITS: usize = 19;
/// Values of up to this many bytes use the stack buffer.
const STACK_BYTES: usize = 32;
/// Digits of 2^256 - 1, the largest value the stack buffer holds.
const STACK_DIGITS: usize = 78;

/// Append the decimal string of the big-endian unsigned integer `bytes` to
/// `builder`. Empty input is `"0"`.
pub(crate) fn append_decimal(builder: &mut StringBuilder, bytes: &[u8]) {
    let bytes = trim_leading_zeros(bytes);
    if bytes.len() <= STACK_BYTES {
        let mut buf = [0u8; STACK_DIGITS];
        builder.append_value(format_stack(bytes, &mut buf));
    } else {
        builder.append_value(format_heap(bytes));
    }
}

/// Decimal string of the big-endian unsigned integer `bytes`. Empty input is
/// `"0"`.
#[cfg(test)]
pub(crate) fn to_decimal(bytes: &[u8]) -> String {
    let bytes = trim_leading_zeros(bytes);
    if bytes.len() <= STACK_BYTES {
        let mut buf = [0u8; STACK_DIGITS];
        format_stack(bytes, &mut buf).to_owned()
    } else {
        format_heap(bytes)
    }
}

fn trim_leading_zeros(bytes: &[u8]) -> &[u8] {
    let first = bytes.iter().position(|&b| b != 0).unwrap_or(bytes.len());
    &bytes[first..]
}

/// Format at most 32 bytes (without leading zero bytes) into `buf`.
fn format_stack<'a>(bytes: &[u8], buf: &'a mut [u8; STACK_DIGITS]) -> &'a str {
    let start = if bytes.len() <= 16 {
        let mut value = 0u128;
        for &b in bytes {
            value = (value << 8) | u128::from(b);
        }
        write_u128(value, buf)
    } else {
        let mut limbs = [0u64; STACK_BYTES / 8];
        let count = fill_limbs(bytes, &mut limbs);
        write_limbs(&mut limbs[..count], buf)
    };
    as_str(&buf[start..])
}

/// Format more than 32 bytes. EVM `BigInt`s are at most 32 bytes, so this is
/// only here to stay correct for any input.
fn format_heap(bytes: &[u8]) -> String {
    let mut limbs = vec![0u64; bytes.len().div_ceil(8)];
    let count = fill_limbs(bytes, &mut limbs);
    // Each 64-bit limb adds at most 20 digits, and chunks are 19 wide.
    let mut buf = vec![0u8; count * 20 + CHUNK_DIGITS];
    let start = write_limbs(&mut limbs[..count], &mut buf);
    as_str(&buf[start..]).to_owned()
}

fn as_str(digits: &[u8]) -> &str {
    std::str::from_utf8(digits).expect("only ASCII digits are written")
}

/// Pack big-endian `bytes` into 64-bit limbs, most significant first. Returns
/// the number of limbs used.
fn fill_limbs(bytes: &[u8], limbs: &mut [u64]) -> usize {
    let count = bytes.len().div_ceil(8);
    let head = bytes.len() - (count - 1) * 8;
    limbs[0] = bytes[..head]
        .iter()
        .fold(0u64, |acc, &b| (acc << 8) | u64::from(b));
    for (limb, chunk) in limbs[1..count]
        .iter_mut()
        .zip(bytes[head..].chunks_exact(8))
    {
        *limb = u64::from_be_bytes(chunk.try_into().expect("8-byte chunk"));
    }
    count
}

/// Write `value` at the end of `buf`; returns the index of the first digit.
fn write_u128(value: u128, buf: &mut [u8]) -> usize {
    let chunk = u128::from(CHUNK);
    if value < chunk {
        return write_u64(value as u64, buf, buf.len());
    }
    let mut end = buf.len();
    let mut rest = value;
    while rest >= chunk {
        let quotient = rest / chunk;
        // One 128-bit division; the remainder comes from a multiplication.
        write_chunk(
            (rest - quotient * chunk) as u64,
            &mut buf[end - CHUNK_DIGITS..end],
        );
        end -= CHUNK_DIGITS;
        rest = quotient;
    }
    write_u64(rest as u64, buf, end)
}

/// Write the number held in `limbs` (most significant first) at the end of
/// `buf`, dividing the limbs in place; returns the index of the first digit.
fn write_limbs(limbs: &mut [u64], buf: &mut [u8]) -> usize {
    let mut end = buf.len();
    let mut first = 0;
    loop {
        // Divide by 10^19; the remainder is the next 19 digits.
        let mut rem = 0u64;
        for limb in &mut limbs[first..] {
            let cur = (u128::from(rem) << 64) | u128::from(*limb);
            // `rem < 10^19`, so the quotient fits a u64.
            let quotient = (cur / u128::from(CHUNK)) as u64;
            rem = (cur - u128::from(quotient) * u128::from(CHUNK)) as u64;
            *limb = quotient;
        }
        while first < limbs.len() && limbs[first] == 0 {
            first += 1;
        }
        if first == limbs.len() {
            // Most significant chunk: no zero padding.
            return write_u64(rem, buf, end);
        }
        write_chunk(rem, &mut buf[end - CHUNK_DIGITS..end]);
        end -= CHUNK_DIGITS;
    }
}

/// "00", "01", ..., "99": two digits per division when writing.
const DIGIT_PAIRS: &[u8; 200] = b"\
0001020304050607080910111213141516171819\
2021222324252627282930313233343536373839\
4041424344454647484950515253545556575859\
6061626364656667686970717273747576777879\
8081828384858687888990919293949596979899";

/// Write `value` so that it ends at `end`; returns the index of its first digit.
fn write_u64(mut value: u64, buf: &mut [u8], end: usize) -> usize {
    let mut i = end;
    while value >= 100 {
        let pair = (value % 100) as usize * 2;
        value /= 100;
        i -= 2;
        buf[i..i + 2].copy_from_slice(&DIGIT_PAIRS[pair..pair + 2]);
    }
    if value >= 10 {
        let pair = value as usize * 2;
        i -= 2;
        buf[i..i + 2].copy_from_slice(&DIGIT_PAIRS[pair..pair + 2]);
    } else {
        i -= 1;
        buf[i] = b'0' + value as u8;
    }
    i
}

/// Write a value below 10^19 as exactly 19 digits, zero padded.
fn write_chunk(mut value: u64, out: &mut [u8]) {
    let mut i = out.len();
    while i > 1 {
        let pair = (value % 100) as usize * 2;
        value /= 100;
        i -= 2;
        out[i..i + 2].copy_from_slice(&DIGIT_PAIRS[pair..pair + 2]);
    }
    out[0] = b'0' + value as u8;
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Array;

    /// The implementation this module replaced (#513), kept as the reference.
    fn legacy_to_string(bytes: &[u8]) -> String {
        if bytes.is_empty() {
            return "0".to_string();
        }
        let mut result = vec![0u8];
        for &byte in bytes {
            let mut carry = 0u16;
            for digit in result.iter_mut().rev() {
                let val = (*digit as u16) * 256 + carry;
                *digit = (val % 10) as u8;
                carry = val / 10;
            }
            while carry > 0 {
                result.insert(0, (carry % 10) as u8);
                carry /= 10;
            }
            let mut carry = byte as u16;
            for digit in result.iter_mut().rev() {
                let val = (*digit as u16) + carry;
                *digit = (val % 10) as u8;
                carry = val / 10;
            }
            while carry > 0 {
                result.insert(0, (carry % 10) as u8);
                carry /= 10;
            }
        }
        while result.len() > 1 && result[0] == 0 {
            result.remove(0);
        }
        result.into_iter().map(|d| (b'0' + d) as char).collect()
    }

    /// Deterministic xorshift64* generator, so failures are reproducible.
    struct Rng(u64);

    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 >> 12;
            self.0 ^= self.0 << 25;
            self.0 ^= self.0 >> 27;
            self.0.wrapping_mul(0x2545_f491_4f6c_dd1d)
        }

        fn bytes(&mut self, len: usize) -> Vec<u8> {
            (0..len).map(|_| self.next() as u8).collect()
        }
    }

    fn assert_matches_legacy(bytes: &[u8]) {
        let expected = legacy_to_string(bytes);
        assert_eq!(to_decimal(bytes), expected, "to_decimal({bytes:02x?})");
        let mut builder = StringBuilder::new();
        append_decimal(&mut builder, bytes);
        let array = builder.finish();
        assert_eq!(array.len(), 1);
        assert_eq!(array.value(0), expected, "append_decimal({bytes:02x?})");
    }

    #[test]
    fn test_matches_legacy_for_every_one_and_two_byte_value() {
        assert_matches_legacy(&[]);
        for value in 0..=u8::MAX {
            assert_matches_legacy(&[value]);
        }
        for value in 0..=u16::MAX {
            assert_matches_legacy(&value.to_be_bytes());
        }
    }

    #[test]
    fn test_matches_legacy_for_random_values_of_every_length() {
        let mut rng = Rng(0x9e37_79b9_7f4a_7c15);
        for len in 0..=40 {
            for _ in 0..2_000 {
                assert_matches_legacy(&rng.bytes(len));
            }
        }
    }

    #[test]
    fn test_matches_legacy_with_leading_zero_bytes() {
        let mut rng = Rng(42);
        for zeros in 0..=33 {
            for len in 0..=32 {
                let mut bytes = vec![0u8; zeros];
                bytes.extend(rng.bytes(len));
                assert_matches_legacy(&bytes);
            }
        }
    }

    #[test]
    fn test_matches_legacy_at_boundaries() {
        for len in 0..=40 {
            assert_matches_legacy(&vec![0xff; len]);
            assert_matches_legacy(&vec![0x00; len]);
            let mut power_of_two = vec![0u8; len + 1];
            power_of_two[0] = 1;
            assert_matches_legacy(&power_of_two);
        }
        // Powers of ten and their neighbours cross every chunk boundary.
        let mut power = vec![1u8];
        for _ in 0..=80 {
            for delta in [-1i32, 0, 1] {
                assert_matches_legacy(&add_small(&power, delta));
            }
            power = mul_small(&power, 10);
        }
    }

    #[test]
    fn test_known_values() {
        assert_eq!(to_decimal(&[]), "0");
        assert_eq!(to_decimal(&[0, 0, 0]), "0");
        assert_eq!(to_decimal(&[0x3b, 0x9a, 0xca, 0x00]), "1000000000");
        assert_eq!(to_decimal(&u64::MAX.to_be_bytes()), u64::MAX.to_string());
        assert_eq!(to_decimal(&u128::MAX.to_be_bytes()), u128::MAX.to_string());
        assert_eq!(
            to_decimal(&[0xff; 32]),
            "115792089237316195423570985008687907853269984665640564039457584007913129639935"
        );
    }

    #[test]
    fn appends_preserve_existing_values_nulls_and_builder_reset() {
        let mut builder = StringBuilder::new();
        builder.append_value("sentinel");
        builder.append_null();
        append_decimal(&mut builder, &[]);
        append_decimal(&mut builder, &[255; 32]);
        append_decimal(&mut builder, &[255; 256]);
        let values = builder.finish();
        assert_eq!(values.value(0), "sentinel");
        assert!(values.is_null(1));
        assert_eq!(values.value(2), "0");
        assert_eq!(values.value(3), legacy_to_string(&[255; 32]));
        assert_eq!(values.value(4), legacy_to_string(&[255; 256]));
        append_decimal(&mut builder, &[0, 42]);
        let reset = builder.finish();
        assert_eq!(reset.len(), 1);
        assert_eq!(reset.value(0), "42");
    }

    /// `bytes * factor`, big-endian.
    fn mul_small(bytes: &[u8], factor: u32) -> Vec<u8> {
        let mut out = Vec::with_capacity(bytes.len() + 1);
        let mut carry = 0u32;
        for &b in bytes.iter().rev() {
            let v = u32::from(b) * factor + carry;
            out.push(v as u8);
            carry = v >> 8;
        }
        while carry > 0 {
            out.push(carry as u8);
            carry >>= 8;
        }
        out.reverse();
        out
    }

    /// `bytes + delta` for `delta` in -1..=1, big-endian; `bytes` must be > 0
    /// when `delta` is -1.
    fn add_small(bytes: &[u8], delta: i32) -> Vec<u8> {
        let mut out = bytes.to_vec();
        match delta {
            1 => {
                for b in out.iter_mut().rev() {
                    let (v, overflow) = b.overflowing_add(1);
                    *b = v;
                    if !overflow {
                        return out;
                    }
                }
                out.insert(0, 1);
            }
            -1 => {
                for b in out.iter_mut().rev() {
                    let (v, borrow) = b.overflowing_sub(1);
                    *b = v;
                    if !borrow {
                        break;
                    }
                }
            }
            _ => {}
        }
        out
    }

    /// Timing of the old and new conversions. Run with
    /// `cargo test --release -p blocks decimal::tests::bench -- --ignored --nocapture`.
    #[test]
    #[ignore = "benchmark; run manually in release mode"]
    fn bench_decimal_conversion() {
        use std::hint::black_box;
        use std::time::Instant;

        let cases: [(&str, Vec<u8>); 5] = [
            ("4-byte gas price", vec![0x3b, 0x9a, 0xca, 0x00]),
            (
                "10-byte balance",
                vec![0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0x01, 0x23],
            ),
            ("16-byte value", vec![0xab; 16]),
            ("20-byte value", vec![0xcd; 20]),
            ("32-byte max", vec![0xff; 32]),
        ];
        let iterations = 100_000usize;
        let samples = 7;
        // Both paths include appending to a preallocated Arrow builder; construction
        // and final flush are outside the timer. Alternate order across samples.
        for (name, bytes) in &cases {
            let width = legacy_to_string(bytes).len();
            let measure = |legacy: bool| {
                let mut builder = StringBuilder::with_capacity(iterations, iterations * width);
                let start = Instant::now();
                for _ in 0..iterations {
                    if legacy {
                        builder.append_value(legacy_to_string(black_box(bytes)));
                    } else {
                        append_decimal(&mut builder, black_box(bytes));
                    }
                }
                let ns = start.elapsed().as_secs_f64() * 1e9 / iterations as f64;
                black_box(builder.finish());
                ns
            };
            measure(true);
            measure(false);
            let mut old = Vec::new();
            let mut new = Vec::new();
            for sample in 0..samples {
                if sample % 2 == 0 {
                    old.push(measure(true));
                    new.push(measure(false));
                } else {
                    new.push(measure(false));
                    old.push(measure(true));
                }
            }
            let median = |values: &[f64]| {
                let mut sorted = values.to_vec();
                sorted.sort_by(f64::total_cmp);
                sorted[sorted.len() / 2]
            };
            println!(
                "{}",
                serde_json::json!({"case":name,"input_hex":bytes.iter().map(|b|format!("{b:02x}")).collect::<String>(),"iterations":iterations,"samples":samples,"legacy_ns":old,"optimized_ns":new,"legacy_median_ns":median(&old),"optimized_median_ns":median(&new)})
            );
        }
    }
}

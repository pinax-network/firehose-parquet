//! `merkle_v2` row encoding: explicit, per-type canonical value bytes.
//!
//! Every value is encoded from its logical value, never from Arrow display
//! formatting, so an arrow-rs upgrade cannot change roots. Physical variants of
//! one logical type encode identically (`Utf8`/`LargeUtf8`/`Utf8View`, the
//! binary family, dictionaries and their value type, timestamps in any unit),
//! so a reader or writer choosing a different Arrow representation for the
//! same data does not change the root. The spec, with the per-type table, is in
//! `docs/verifiability-hash-strategy.md`; any change here must bump
//! `MERKLE_VERSION`.

use anyhow::{anyhow, Context, Result};
use arrow::array::{Array, ArrowPrimitiveType, AsArray, OffsetSizeTrait};
use arrow::buffer::NullBuffer;
use arrow::datatypes::{
    DataType, Date32Type, Float16Type, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type,
    Int8Type, TimeUnit, TimestampMicrosecondType, TimestampMillisecondType,
    TimestampNanosecondType, TimestampSecondType, UInt16Type, UInt32Type, UInt64Type, UInt8Type,
};
use arrow::record_batch::RecordBatch;
use std::fmt::Display;
use std::io::Write;

/// Value tag for a null value (no length or bytes follow).
const VALUE_NULL: u8 = 0x00;
/// Value tag for a present value, followed by `u32` LE length and canonical bytes.
const VALUE_PRESENT: u8 = 0x01;
/// Bit pattern every floating-point NaN is canonicalized to.
const CANONICAL_NAN_BITS: u64 = 0x7ff8_0000_0000_0000;

/// Writes the canonical bytes of the non-null value at a row.
type Canonical<'a> = Box<dyn Fn(usize, &mut Vec<u8>) + 'a>;

/// Encodes whole rows of one batch: for each column in schema order,
/// `u32le(len(name)) || name || value`.
pub(super) struct RowEncoder<'a> {
    columns: Vec<(&'a str, ValueEncoder<'a>)>,
}

impl<'a> RowEncoder<'a> {
    /// Resolves one encoder per column; fails on an Arrow type that has no
    /// `merkle_v2` encoding.
    pub(super) fn new(batch: &'a RecordBatch) -> Result<Self> {
        let columns = batch
            .schema_ref()
            .fields()
            .iter()
            .zip(batch.columns())
            .map(|(field, array)| {
                ValueEncoder::new(array.as_ref())
                    .map(|encoder| (field.name().as_str(), encoder))
                    .with_context(|| format!("column `{}`", field.name()))
            })
            .collect::<Result<_>>()?;
        Ok(Self { columns })
    }

    pub(super) fn encode_row(&self, row: usize, out: &mut Vec<u8>) {
        for (name, value) in &self.columns {
            out.extend_from_slice(&(name.len() as u32).to_le_bytes());
            out.extend_from_slice(name.as_bytes());
            value.encode(row, out);
        }
    }
}

/// Canonical bytes of one non-null value, or `None` for an unsupported type.
pub(super) fn canonical_value_bytes(array: &dyn Array, row: usize) -> Option<Vec<u8>> {
    let canonical = canonical_encoder(array).ok()?;
    let mut out = Vec::new();
    canonical(row, &mut out);
    Some(out)
}

/// Encodes one value as `0x00` (null) or `0x01 || u32le(len) || canonical`.
struct ValueEncoder<'a> {
    nulls: Option<NullBuffer>,
    canonical: Canonical<'a>,
}

impl<'a> ValueEncoder<'a> {
    fn new(array: &'a dyn Array) -> Result<Self> {
        Ok(Self {
            // Logical nulls also cover dictionary values and the Null type.
            nulls: array.logical_nulls(),
            canonical: canonical_encoder(array)?,
        })
    }

    fn encode(&self, row: usize, out: &mut Vec<u8>) {
        if self.nulls.as_ref().is_some_and(|nulls| nulls.is_null(row)) {
            out.push(VALUE_NULL);
            return;
        }
        out.push(VALUE_PRESENT);
        let len_at = out.len();
        out.extend_from_slice(&[0; 4]);
        (self.canonical)(row, out);
        let len = (out.len() - len_at - 4) as u32;
        out[len_at..len_at + 4].copy_from_slice(&len.to_le_bytes());
    }
}

fn canonical_encoder<'a>(array: &'a dyn Array) -> Result<Canonical<'a>> {
    Ok(match array.data_type() {
        // Every value of a Null array is null, so canonical bytes are never written.
        DataType::Null => Box::new(|_, _| {}),
        DataType::Boolean => {
            let a = array.as_boolean();
            Box::new(move |row, out| {
                out.extend_from_slice(if a.value(row) { b"true" } else { b"false" })
            })
        }
        DataType::Int8 => decimal::<Int8Type>(array),
        DataType::Int16 => decimal::<Int16Type>(array),
        DataType::Int32 => decimal::<Int32Type>(array),
        DataType::Int64 => decimal::<Int64Type>(array),
        DataType::UInt8 => decimal::<UInt8Type>(array),
        DataType::UInt16 => decimal::<UInt16Type>(array),
        DataType::UInt32 => decimal::<UInt32Type>(array),
        DataType::UInt64 => decimal::<UInt64Type>(array),
        DataType::Float16 => {
            let a = array.as_primitive::<Float16Type>();
            Box::new(move |row, out| float_bits(a.value(row).to_f64(), out))
        }
        DataType::Float32 => {
            let a = array.as_primitive::<Float32Type>();
            Box::new(move |row, out| float_bits(f64::from(a.value(row)), out))
        }
        DataType::Float64 => {
            let a = array.as_primitive::<Float64Type>();
            Box::new(move |row, out| float_bits(a.value(row), out))
        }
        DataType::Utf8 => {
            let a = array.as_string::<i32>();
            Box::new(move |row, out| out.extend_from_slice(a.value(row).as_bytes()))
        }
        DataType::LargeUtf8 => {
            let a = array.as_string::<i64>();
            Box::new(move |row, out| out.extend_from_slice(a.value(row).as_bytes()))
        }
        DataType::Utf8View => {
            let a = array.as_string_view();
            Box::new(move |row, out| out.extend_from_slice(a.value(row).as_bytes()))
        }
        DataType::Binary => {
            let a = array.as_binary::<i32>();
            Box::new(move |row, out| lower_hex(a.value(row), out))
        }
        DataType::LargeBinary => {
            let a = array.as_binary::<i64>();
            Box::new(move |row, out| lower_hex(a.value(row), out))
        }
        DataType::BinaryView => {
            let a = array.as_binary_view();
            Box::new(move |row, out| lower_hex(a.value(row), out))
        }
        DataType::FixedSizeBinary(_) => {
            let a = array.as_fixed_size_binary();
            Box::new(move |row, out| lower_hex(a.value(row), out))
        }
        DataType::Date32 => decimal::<Date32Type>(array),
        DataType::Timestamp(TimeUnit::Second, _) => {
            epoch_nanos::<TimestampSecondType>(array, 1_000_000_000)
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            epoch_nanos::<TimestampMillisecondType>(array, 1_000_000)
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            epoch_nanos::<TimestampMicrosecondType>(array, 1_000)
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            epoch_nanos::<TimestampNanosecondType>(array, 1)
        }
        DataType::Dictionary(_, _) => {
            let dict = array.as_any_dictionary();
            let values = canonical_encoder(dict.values().as_ref())?;
            // An empty dictionary means every key is null.
            let keys = if dict.values().is_empty() {
                Vec::new()
            } else {
                dict.normalized_keys()
            };
            Box::new(move |row, out| values(keys[row], out))
        }
        DataType::List(_) => list::<i32>(array)?,
        DataType::LargeList(_) => list::<i64>(array)?,
        DataType::FixedSizeList(_, _) => {
            let list = array.as_fixed_size_list();
            let child = ValueEncoder::new(list.values().as_ref())?;
            let size = list.value_length() as usize;
            Box::new(move |row, out| {
                let start = list.value_offset(row) as usize;
                out.extend_from_slice(&(size as u32).to_le_bytes());
                for idx in start..start + size {
                    child.encode(idx, out);
                }
            })
        }
        other => {
            return Err(anyhow!(
                "Arrow type {other} has no merkle_v2 encoding (supported types are listed in docs/verifiability-hash-strategy.md)"
            ))
        }
    })
}

/// Base-10 ASCII, `-` for negatives, no leading zeros or `+`.
fn decimal<'a, T>(array: &'a dyn Array) -> Canonical<'a>
where
    T: ArrowPrimitiveType,
    T::Native: Display,
{
    let a = array.as_primitive::<T>();
    Box::new(move |row, out| write_decimal(a.value(row), out))
}

/// The instant as base-10 nanoseconds since the Unix epoch; unit and timezone
/// are not encoded.
fn epoch_nanos<'a, T>(array: &'a dyn Array, nanos_per_unit: i128) -> Canonical<'a>
where
    T: ArrowPrimitiveType<Native = i64>,
{
    let a = array.as_primitive::<T>();
    Box::new(move |row, out| write_decimal(i128::from(a.value(row)) * nanos_per_unit, out))
}

fn list<'a, O: OffsetSizeTrait>(array: &'a dyn Array) -> Result<Canonical<'a>> {
    let list = array.as_list::<O>();
    let child = ValueEncoder::new(list.values().as_ref())?;
    Ok(Box::new(move |row, out| {
        let offsets = list.value_offsets();
        let (start, end) = (offsets[row].as_usize(), offsets[row + 1].as_usize());
        out.extend_from_slice(&((end - start) as u32).to_le_bytes());
        for idx in start..end {
            child.encode(idx, out);
        }
    }))
}

fn write_decimal(value: impl Display, out: &mut Vec<u8>) {
    write!(out, "{value}").expect("writing to a Vec cannot fail");
}

/// IEEE-754 binary64 bits, little-endian; every NaN maps to one bit pattern.
fn float_bits(value: f64, out: &mut Vec<u8>) {
    let bits = if value.is_nan() {
        CANONICAL_NAN_BITS
    } else {
        value.to_bits()
    };
    out.extend_from_slice(&bits.to_le_bytes());
}

fn lower_hex(bytes: &[u8], out: &mut Vec<u8>) {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    out.reserve(bytes.len() * 2);
    for byte in bytes {
        out.push(DIGITS[usize::from(byte >> 4)]);
        out.push(DIGITS[usize::from(byte & 0x0f)]);
    }
}

#[cfg(test)]
mod tests {
    use super::{RowEncoder, ValueEncoder};
    use arrow::array::{
        Array, ArrayRef, BinaryArray, BinaryViewArray, BooleanArray, Date32Array, DictionaryArray,
        FixedSizeBinaryArray, FixedSizeListArray, Float32Array, Float64Array, Int32Array,
        Int64Array, LargeBinaryArray, LargeListArray, LargeStringArray, ListArray, NullArray,
        StringArray, StringViewArray, TimestampMicrosecondArray, TimestampMillisecondArray,
        TimestampNanosecondArray, TimestampSecondArray, UInt32Array, UInt64Array, UInt8Array,
    };
    use arrow::datatypes::{
        DataType, Decimal128Type, Field, Int32Type, Schema, UInt64Type, UInt8Type,
    };
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    // Golden values were computed independently from the spec in
    // docs/verifiability-hash-strategy.md with a Python reference encoder.

    /// Hex of the full value encoding (tag, length, canonical bytes) at `row`.
    fn encoded(array: &dyn Array, row: usize) -> String {
        let mut out = Vec::new();
        ValueEncoder::new(array).unwrap().encode(row, &mut out);
        hex::encode(out)
    }

    fn encoded_all(arrays: &[ArrayRef], row: usize) -> Vec<String> {
        arrays.iter().map(|a| encoded(a.as_ref(), row)).collect()
    }

    fn assert_all_eq(arrays: &[ArrayRef], row: usize, expected: &str) {
        for (array, got) in arrays.iter().zip(encoded_all(arrays, row)) {
            assert_eq!(got, expected, "type {}", array.data_type());
        }
    }

    #[test]
    fn null_values_use_the_null_tag_for_every_type() {
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(UInt64Array::from(vec![None])),
            Arc::new(StringArray::from(vec![None::<&str>])),
            Arc::new(BinaryArray::from(vec![None::<&[u8]>])),
            Arc::new(TimestampSecondArray::from(vec![None]).with_timezone("UTC")),
            Arc::new(NullArray::new(1)),
        ];
        assert_all_eq(&arrays, 0, "00");
        // A null is distinct from the string "<null>" (the merkle_v1 sentinel).
        assert_eq!(
            encoded(&StringArray::from(vec!["<null>"]), 0),
            format!("0106000000{}", hex::encode("<null>"))
        );
    }

    #[test]
    fn boolean_golden() {
        let a = BooleanArray::from(vec![true, false]);
        assert_eq!(encoded(&a, 0), "010400000074727565");
        assert_eq!(encoded(&a, 1), "010500000066616c7365");
    }

    #[test]
    fn integers_are_decimal_and_width_independent() {
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(UInt8Array::from(vec![42])),
            Arc::new(UInt32Array::from(vec![42])),
            Arc::new(UInt64Array::from(vec![42])),
            Arc::new(Int32Array::from(vec![42])),
            Arc::new(Int64Array::from(vec![42])),
        ];
        assert_all_eq(&arrays, 0, "01020000003432");
        assert_eq!(encoded(&Int64Array::from(vec![-7]), 0), "01020000002d37");
        assert_eq!(
            encoded(&UInt64Array::from(vec![u64::MAX]), 0),
            "01140000003138343436373434303733373039353531363135"
        );
    }

    #[test]
    fn floats_are_binary64_bits_with_canonical_nan() {
        let arrays: Vec<ArrayRef> = vec![
            arrow::compute::cast(&Float64Array::from(vec![1.5]), &DataType::Float16).unwrap(),
            Arc::new(Float32Array::from(vec![1.5f32])),
            Arc::new(Float64Array::from(vec![1.5f64])),
        ];
        assert_all_eq(&arrays, 0, "0108000000000000000000f83f");

        let nans = Float64Array::from(vec![f64::NAN, f64::from_bits(0x7ff0_0000_0000_0001)]);
        assert_eq!(encoded(&nans, 0), "0108000000000000000000f87f");
        assert_eq!(encoded(&nans, 1), "0108000000000000000000f87f");
        assert_eq!(
            encoded(&Float32Array::from(vec![f32::NAN]), 0),
            "0108000000000000000000f87f"
        );
        // -0.0 and 0.0 are distinct values.
        assert_eq!(
            encoded(&Float64Array::from(vec![-0.0]), 0),
            "01080000000000000000000080"
        );
    }

    #[test]
    fn strings_encode_utf8_bytes_for_every_variant() {
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(vec!["abc"])),
            Arc::new(LargeStringArray::from(vec!["abc"])),
            Arc::new(StringViewArray::from(vec!["abc"])),
        ];
        assert_all_eq(&arrays, 0, "0103000000616263");
    }

    #[test]
    fn binary_family_encodes_lowercase_hex() {
        let bytes: &[u8] = &[0xde, 0xad];
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(BinaryArray::from(vec![bytes])),
            Arc::new(LargeBinaryArray::from(vec![bytes])),
            Arc::new(BinaryViewArray::from(vec![bytes])),
            Arc::new(FixedSizeBinaryArray::try_from_iter(std::iter::once(bytes)).unwrap()),
            // Text/binary parity: hex text equals the binary encoding.
            Arc::new(StringArray::from(vec!["dead"])),
        ];
        assert_all_eq(&arrays, 0, "010400000064656164");
    }

    #[test]
    fn date32_is_days_since_epoch() {
        // 19723 = 2024-01-01
        assert_eq!(
            encoded(&Date32Array::from(vec![19723]), 0),
            "01050000003139373233"
        );
    }

    #[test]
    fn timestamps_encode_the_instant_independent_of_unit_and_timezone() {
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(TimestampSecondArray::from(vec![1_700_000_000]).with_timezone("UTC")),
            Arc::new(TimestampMillisecondArray::from(vec![1_700_000_000_000]).with_timezone("UTC")),
            Arc::new(
                TimestampMicrosecondArray::from(vec![1_700_000_000_000_000])
                    .with_timezone("+00:00"),
            ),
            Arc::new(TimestampNanosecondArray::from(vec![
                1_700_000_000_000_000_000,
            ])),
        ];
        assert_all_eq(
            &arrays,
            0,
            "011300000031373030303030303030303030303030303030",
        );

        // Millisecond precision (#491) is kept.
        assert_eq!(
            encoded(
                &TimestampMillisecondArray::from(vec![1_700_000_000_123]).with_timezone("UTC"),
                0
            ),
            "011300000031373030303030303030313233303030303030"
        );
        assert_eq!(
            encoded(
                &TimestampSecondArray::from(vec![-1]).with_timezone("UTC"),
                0
            ),
            "010b0000002d31303030303030303030"
        );
        // Seconds at the i64 limit do not overflow.
        let max = encoded(&TimestampSecondArray::from(vec![i64::MAX]), 0);
        assert!(max.ends_with(&hex::encode(format!("{}000000000", i64::MAX))));
    }

    #[test]
    fn dictionary_encodes_the_referenced_value() {
        let dict: DictionaryArray<Int32Type> =
            vec![Some("call"), None, Some("create"), Some("call")]
                .into_iter()
                .collect();
        let plain = StringArray::from(vec!["call", "create"]);
        assert_eq!(encoded(&dict, 0), encoded(&plain, 0));
        assert_eq!(encoded(&dict, 3), encoded(&plain, 0));
        assert_eq!(encoded(&dict, 2), encoded(&plain, 1));
        assert_eq!(encoded(&dict, 1), "00");

        // A non-null key that points at a null dictionary value is null.
        let values = StringArray::from(vec![Some("a"), None]);
        let keys = Int32Array::from(vec![0, 1]);
        let dict = DictionaryArray::<Int32Type>::try_new(keys, Arc::new(values)).unwrap();
        assert_eq!(encoded(&dict, 0), encoded(&StringArray::from(vec!["a"]), 0));
        assert_eq!(encoded(&dict, 1), "00");

        // All-null dictionary with no values.
        let dict = DictionaryArray::<Int32Type>::try_new(
            Int32Array::from(vec![None]),
            Arc::new(StringArray::from(Vec::<&str>::new())),
        )
        .unwrap();
        assert_eq!(encoded(&dict, 0), "00");
    }

    #[test]
    fn list_u8_golden_and_list_variants_agree() {
        let values = vec![Some(vec![Some(1u8), Some(2), Some(255)])];
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(ListArray::from_iter_primitive::<UInt8Type, _, _>(
                values.clone(),
            )),
            Arc::new(LargeListArray::from_iter_primitive::<UInt8Type, _, _>(
                values.clone(),
            )),
            Arc::new(FixedSizeListArray::from_iter_primitive::<UInt8Type, _, _>(
                values, 3,
            )),
        ];
        assert_all_eq(
            &arrays,
            0,
            "0118000000030000000101000000310101000000320103000000323535",
        );
    }

    #[test]
    fn list_elements_carry_their_own_null_tags() {
        let list = ListArray::from_iter_primitive::<UInt64Type, _, _>(vec![
            Some(vec![]),
            Some(vec![Some(1), None]),
            None,
        ]);
        assert_eq!(encoded(&list, 0), "010400000000000000");
        assert_eq!(encoded(&list, 1), "010b0000000200000001010000003100");
        assert_eq!(encoded(&list, 2), "00");

        // List<Utf8> (Bitcoin and Solana schemas); also checks a sliced list.
        let strings = Arc::new(StringArray::from(vec!["x", "a", "bc"])) as ArrayRef;
        let offsets = arrow::buffer::OffsetBuffer::new(vec![0, 1, 3].into());
        let field = Arc::new(Field::new("item", DataType::Utf8, true));
        let list = ListArray::new(field, offsets, strings, None).slice(1, 1);
        assert_eq!(
            encoded(&list, 0),
            "01110000000200000001010000006101020000006263"
        );
    }

    #[test]
    fn unsupported_types_are_rejected() {
        let decimals = arrow::array::PrimitiveArray::<Decimal128Type>::from(vec![1i128]);
        let err = ValueEncoder::new(&decimals)
            .err()
            .expect("decimal is unsupported");
        assert!(err.to_string().contains("Decimal128"), "{err}");

        let schema = Arc::new(Schema::new(vec![Field::new(
            "amount",
            decimals.data_type().clone(),
            false,
        )]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(decimals)]).unwrap();
        let err = RowEncoder::new(&batch)
            .err()
            .expect("decimal column is unsupported");
        assert!(format!("{err:#}").contains("column `amount`"), "{err:#}");
    }

    #[test]
    fn row_golden() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("block_num", DataType::UInt64, false),
            Field::new("hash", DataType::Binary, false),
            Field::new("note", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(UInt64Array::from(vec![1])),
                Arc::new(BinaryArray::from(vec![&[0xabu8][..]])),
                Arc::new(StringArray::from(vec![None::<&str>])),
            ],
        )
        .unwrap();
        let mut out = Vec::new();
        RowEncoder::new(&batch).unwrap().encode_row(0, &mut out);
        assert_eq!(
            hex::encode(out),
            "09000000626c6f636b5f6e756d010100000031040000006861736801020000006162040000006e6f746500"
        );
    }
}

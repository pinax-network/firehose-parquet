//! Fireparq-shaped source batches (Arrow 60, the workspace's types) and their
//! mapping onto Delta types.
//!
//! The source batches use the types fireparq writes today: `UInt64`, `UInt32`,
//! `UInt8`, `Timestamp(Millisecond, "UTC")`, `Dictionary(Int32, Utf8)`,
//! `List<..>`, `Binary` and the `Date32` partition column. [`to_delta_batch`]
//! converts them with checked casts: an out-of-range value is an error, never a
//! wrapped or null value.

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BinaryBuilder, Date32Array, DictionaryArray, Int32Array, ListBuilder,
    RecordBatch, StringArray, StringBuilder, TimestampMillisecondArray, UInt32Array, UInt64Array,
    UInt64Builder, UInt8Builder,
};
use arrow::compute::{cast_with_options, CastOptions};
use arrow::datatypes::{DataType, Field, Int32Type, Schema, TimeUnit};
use arrow::error::ArrowError;
use arrow::util::display::FormatOptions;

/// How one fireparq column maps onto a Delta type.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Mapping {
    /// The Arrow type is already a Delta type (`Utf8`, `Binary`, `List<Utf8>`, ...).
    Keep,
    /// `UInt64`/`UInt32` (or a list of them) to `Int64` (`long`), checked.
    Int64,
    /// `UInt8` (or a list of them) to `Int16` (`short`).
    Int16,
    /// `UInt64` (or a list of them) to `Decimal(20,0)`: every u64 value fits.
    Decimal20,
    /// `Timestamp(Millisecond, "UTC")` to `Timestamp(Microsecond, "UTC")` (Delta `timestamp`).
    TimestampMicros,
    /// `Dictionary(Int32, Utf8)` to `Utf8` (Delta `string`).
    String,
    /// The `date` partition column. Delta keeps it in the log (`partitionValues`),
    /// so it is left out of the physical file.
    Partition,
}

/// A column of a spike table: name and mapping.
pub type ColumnMapping = (&'static str, Mapping);

/// `blocks`-like table: EVM/Solana block header columns.
pub const BLOCKS: &[ColumnMapping] = &[
    ("block_num", Mapping::Int64),
    ("block_id", Mapping::Keep),
    ("timestamp", Mapping::TimestampMicros),
    ("date", Mapping::Partition),
    // PoW mix nonce: any u64, so it can exceed i64::MAX.
    ("nonce", Mapping::Decimal20),
    ("gas_used", Mapping::Int64),
    ("num_transactions", Mapping::Int64),
    ("detail_level", Mapping::String),
];

/// `transactions`-like table: lists, binary payloads and u64 amounts.
pub const TRANSACTIONS: &[ColumnMapping] = &[
    ("block_num", Mapping::Int64),
    ("timestamp", Mapping::TimestampMicros),
    ("date", Mapping::Partition),
    ("tx_index", Mapping::Int64),
    ("status", Mapping::String),
    ("amount", Mapping::Decimal20),
    ("pre_balances", Mapping::Decimal20),
    ("accounts", Mapping::Int16),
    ("topics", Mapping::Keep),
    ("data", Mapping::Keep),
];

/// Returns the mapping table of a spike table.
pub fn table_mapping(table: &str) -> &'static [ColumnMapping] {
    match table {
        "blocks" => BLOCKS,
        "transactions" => TRANSACTIONS,
        other => panic!("unknown spike table {other}"),
    }
}

/// The Delta-compatible Arrow type for a source type under a mapping.
pub fn target_type(source: &DataType, mapping: Mapping) -> Result<DataType, ArrowError> {
    let scalar = |t: &DataType| -> Result<DataType, ArrowError> {
        Ok(match (mapping, t) {
            (Mapping::Keep, t) => t.clone(),
            (Mapping::Int64, DataType::UInt64 | DataType::UInt32 | DataType::UInt16) => {
                DataType::Int64
            }
            (Mapping::Int16, DataType::UInt8) => DataType::Int16,
            (Mapping::Decimal20, DataType::UInt64) => DataType::Decimal128(20, 0),
            (Mapping::TimestampMicros, DataType::Timestamp(TimeUnit::Millisecond, Some(tz)))
                if tz.as_ref() == "UTC" =>
            {
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
            }
            (Mapping::String, DataType::Dictionary(_, value)) if **value == DataType::Utf8 => {
                DataType::Utf8
            }
            (mapping, t) => {
                return Err(ArrowError::CastError(format!(
                    "no Delta mapping {mapping:?} for {t}"
                )))
            }
        })
    };
    match source {
        DataType::List(item) => {
            let inner = scalar(item.data_type())?;
            Ok(DataType::List(Arc::new(Field::new(
                item.name(),
                inner,
                item.is_nullable(),
            ))))
        }
        other => scalar(other),
    }
}

/// Converts a fireparq-shaped batch into the physical Delta data file batch.
///
/// Partition columns are dropped; every other column is cast with
/// `safe: false`, so a value that does not fit (for example a `UInt64` above
/// `i64::MAX` mapped to `Int64`) fails instead of becoming null or wrapping.
pub fn to_delta_batch(
    batch: &RecordBatch,
    mapping: &[ColumnMapping],
) -> Result<RecordBatch, ArrowError> {
    let options = CastOptions {
        safe: false,
        format_options: FormatOptions::default(),
    };
    let mut fields = Vec::new();
    let mut columns: Vec<ArrayRef> = Vec::new();
    for (name, map) in mapping {
        if *map == Mapping::Partition {
            continue;
        }
        let index = batch.schema().index_of(name)?;
        let source_field = batch.schema().field(index).clone();
        let target = target_type(source_field.data_type(), *map)?;
        let column = cast_with_options(batch.column(index), &target, &options)
            .map_err(|e| ArrowError::CastError(format!("column {name}: {e}")))?;
        fields.push(Field::new(*name, target, source_field.is_nullable()));
        columns.push(column);
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
}

/// Same as [`to_delta_batch`], but keeps the `date` column in the file (as
/// fireparq does today), to check how readers treat a physical partition column.
pub fn to_delta_batch_with_physical_date(
    batch: &RecordBatch,
    mapping: &[ColumnMapping],
) -> Result<RecordBatch, ArrowError> {
    let converted = to_delta_batch(batch, mapping)?;
    let index = batch.schema().index_of("date")?;
    let mut fields: Vec<Field> = converted
        .schema()
        .fields()
        .iter()
        .map(|f| f.as_ref().clone())
        .collect();
    let mut columns = converted.columns().to_vec();
    fields.push(batch.schema().field(index).clone());
    columns.push(batch.column(index).clone());
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
}

/// Casts every microsecond timestamp column back to milliseconds (a reader
/// probe: the Delta column stays `timestamp`, the file stores `TIMESTAMP_MILLIS`).
pub fn with_millis_timestamps(batch: &RecordBatch) -> RecordBatch {
    let millis = DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into()));
    let mut fields = Vec::new();
    let mut columns = Vec::new();
    for (field, column) in batch.schema().fields().iter().zip(batch.columns()) {
        if matches!(
            field.data_type(),
            DataType::Timestamp(TimeUnit::Microsecond, _)
        ) {
            fields.push(Field::new(
                field.name(),
                millis.clone(),
                field.is_nullable(),
            ));
            columns.push(arrow::compute::cast(column, &millis).expect("whole milliseconds"));
        } else {
            fields.push(field.as_ref().clone());
            columns.push(column.clone());
        }
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).expect("same rows")
}

/// Milliseconds per UTC day.
pub const DAY_MS: i64 = 86_400_000;

/// One flush of fixture rows: `blocks` consecutive blocks starting at
/// `first_block`, all on the UTC day `date` (days since the epoch).
#[derive(Clone, Copy, Debug)]
pub struct Fixture {
    pub date: i32,
    pub first_block: u64,
    pub blocks: u64,
    pub txs_per_block: u64,
}

impl Fixture {
    fn timestamp_ms(&self, block: u64) -> i64 {
        // 250 ms per block keeps up to 345,600 blocks inside one UTC day.
        i64::from(self.date) * DAY_MS + (block % 345_600) as i64 * 250
    }

    /// The `date` partition value (`YYYY-MM-DD`).
    pub fn date_string(&self) -> String {
        date_string(self.date)
    }

    /// A `blocks` batch in fireparq's current Arrow types.
    pub fn source_blocks(&self) -> RecordBatch {
        let blocks: Vec<u64> = (self.first_block..self.first_block + self.blocks).collect();
        let schema = Schema::new(vec![
            Field::new("block_num", DataType::UInt64, false),
            Field::new("block_id", DataType::Utf8, false),
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
                false,
            ),
            Field::new("date", DataType::Date32, false),
            Field::new("nonce", DataType::UInt64, false),
            Field::new("gas_used", DataType::UInt64, false),
            Field::new("num_transactions", DataType::UInt32, false),
            Field::new(
                "detail_level",
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                false,
            ),
        ]);
        let detail: DictionaryArray<Int32Type> = blocks
            .iter()
            .map(|b| if b % 2 == 0 { "BASE" } else { "EXTENDED" })
            .collect();
        let columns: Vec<ArrayRef> = vec![
            Arc::new(UInt64Array::from(blocks.clone())),
            Arc::new(StringArray::from_iter_values(
                blocks.iter().map(|b| format!("0x{b:064x}")),
            )),
            Arc::new(
                TimestampMillisecondArray::from(
                    blocks
                        .iter()
                        .map(|b| self.timestamp_ms(*b))
                        .collect::<Vec<_>>(),
                )
                .with_timezone("UTC"),
            ),
            Arc::new(Date32Array::from(vec![self.date; blocks.len()])),
            // Above i64::MAX, as PoW nonces often are.
            Arc::new(UInt64Array::from(
                blocks.iter().map(|b| u64::MAX - b).collect::<Vec<_>>(),
            )),
            Arc::new(UInt64Array::from(
                blocks.iter().map(|b| b * 21_000).collect::<Vec<_>>(),
            )),
            Arc::new(UInt32Array::from(vec![
                self.txs_per_block as u32;
                blocks.len()
            ])),
            Arc::new(detail),
        ];
        RecordBatch::try_new(Arc::new(schema), columns).expect("valid blocks fixture")
    }

    /// A `transactions` batch in fireparq's current Arrow types.
    pub fn source_transactions(&self) -> RecordBatch {
        let mut block_num = Vec::new();
        let mut timestamp = Vec::new();
        let mut tx_index = Vec::new();
        let mut status_keys = Vec::new();
        let mut amount = Vec::new();
        let mut pre_balances = ListBuilder::new(UInt64Builder::new());
        let mut accounts = ListBuilder::new(UInt8Builder::new());
        let mut topics = ListBuilder::new(StringBuilder::new());
        let mut data = BinaryBuilder::new();
        for block in self.first_block..self.first_block + self.blocks {
            for tx in 0..self.txs_per_block {
                block_num.push(block);
                timestamp.push(self.timestamp_ms(block));
                tx_index.push(tx as u32);
                status_keys.push(if tx % 3 == 2 { 1 } else { 0 });
                // The first transaction of a block moves u64::MAX base units.
                amount.push(if tx == 0 {
                    u64::MAX
                } else {
                    block * 1_000 + tx
                });
                pre_balances.values().append_value(u64::MAX - tx);
                pre_balances.values().append_value(block);
                pre_balances.append(true);
                accounts.values().append_value((tx % 256) as u8);
                accounts.values().append_value(255);
                accounts.append(true);
                if tx % 4 == 3 {
                    topics.append_null();
                } else {
                    topics.values().append_value(format!("0x{block:x}"));
                    topics.values().append_value(format!("0x{tx:x}"));
                    topics.append(true);
                }
                data.append_value([0xde, 0xad, (block % 256) as u8, (tx % 256) as u8]);
            }
        }
        let rows = block_num.len();
        let status = DictionaryArray::<Int32Type>::try_new(
            Int32Array::from(status_keys),
            Arc::new(StringArray::from(vec!["SUCCEEDED", "FAILED"])),
        )
        .expect("valid dictionary");
        let pre_balances = pre_balances.finish();
        let accounts = accounts.finish();
        let topics = topics.finish();
        let schema = Schema::new(vec![
            Field::new("block_num", DataType::UInt64, false),
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
                false,
            ),
            Field::new("date", DataType::Date32, false),
            Field::new("tx_index", DataType::UInt32, false),
            Field::new(
                "status",
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                false,
            ),
            Field::new("amount", DataType::UInt64, false),
            Field::new("pre_balances", pre_balances.data_type().clone(), true),
            Field::new("accounts", accounts.data_type().clone(), true),
            Field::new("topics", topics.data_type().clone(), true),
            Field::new("data", DataType::Binary, false),
        ]);
        let columns: Vec<ArrayRef> = vec![
            Arc::new(UInt64Array::from(block_num)),
            Arc::new(TimestampMillisecondArray::from(timestamp).with_timezone("UTC")),
            Arc::new(Date32Array::from(vec![self.date; rows])),
            Arc::new(UInt32Array::from(tx_index)),
            Arc::new(status),
            Arc::new(UInt64Array::from(amount)),
            Arc::new(pre_balances),
            Arc::new(accounts),
            Arc::new(topics),
            Arc::new(data.finish()),
        ];
        RecordBatch::try_new(Arc::new(schema), columns).expect("valid transactions fixture")
    }

    /// The source batch of a spike table.
    pub fn source(&self, table: &str) -> RecordBatch {
        match table {
            "blocks" => self.source_blocks(),
            "transactions" => self.source_transactions(),
            other => panic!("unknown spike table {other}"),
        }
    }
}

/// Formats days since the epoch as `YYYY-MM-DD` (proleptic Gregorian, UTC).
pub fn date_string(days: i32) -> String {
    // Howard Hinnant's civil_from_days.
    let z = i64::from(days) + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    format!("{year:04}-{month:02}-{day:02}")
}

/// Asserts that no column of a batch still has an unsigned or dictionary type.
pub fn assert_delta_types(batch: &RecordBatch) {
    fn check(name: &str, t: &DataType) {
        match t {
            DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
                panic!("{name} is still unsigned: {t}")
            }
            DataType::Dictionary(..) => panic!("{name} is still a dictionary: {t}"),
            DataType::Timestamp(unit, _) if *unit != TimeUnit::Microsecond => {
                panic!("{name} is not a microsecond timestamp: {t}")
            }
            DataType::List(item) => check(name, item.data_type()),
            _ => {}
        }
    }
    for field in batch.schema().fields() {
        check(field.name(), field.data_type());
    }
}

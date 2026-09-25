use arrow::array::{
    ArrayBuilder, BinaryBuilder, BooleanBuilder, Date32Builder, Float64Builder, Int32Builder,
    Int64Builder, ListBuilder, StringBuilder, TimestampMillisecondBuilder, UInt32Builder,
    UInt64Builder,
};
use arrow::datatypes::{DataType, Field, TimeUnit};
use arrow::record_batch::RecordBatch;
use std::collections::HashMap;
use std::sync::Arc;

use crate::encode::{bytes_data_type, BytesColumn, EncodeBytes, EncodedBytes};

// ---------------------------------------------------------------------------
// Arrow builder memory estimation helpers
// ---------------------------------------------------------------------------

/// Estimate memory usage of a `UInt64Builder`.
pub fn est_u64(b: &UInt64Builder) -> usize {
    b.len() * 8
}

/// Estimate memory usage of a `UInt32Builder`.
pub fn est_u32(b: &UInt32Builder) -> usize {
    b.len() * 4
}

/// Estimate memory usage of an `Int64Builder`.
pub fn est_i64(b: &Int64Builder) -> usize {
    b.len() * 8
}

/// Estimate memory usage of a `TimestampMillisecondBuilder`.
pub fn est_ts_ms(b: &TimestampMillisecondBuilder) -> usize {
    b.len() * 8
}

/// Estimate memory usage of an `Int32Builder`.
pub fn est_i32(b: &Int32Builder) -> usize {
    b.len() * 4
}

/// Estimate memory usage of a `Date32Builder`.
pub fn est_date32(b: &Date32Builder) -> usize {
    b.len() * 4
}

/// Estimate memory usage of a `Float64Builder`.
pub fn est_f64(b: &Float64Builder) -> usize {
    b.len() * 8
}

/// Estimate memory usage of a `BooleanBuilder`.
pub fn est_bool(b: &BooleanBuilder) -> usize {
    (b.len() + 7) / 8
}

/// Estimate memory usage of a `StringBuilder` (offsets + values).
pub fn est_str(b: &StringBuilder) -> usize {
    b.values_slice().len() + (b.len() + 1) * 4
}

/// Estimate memory usage of an `Option<StringBuilder>`.
pub fn est_opt_str(b: &Option<StringBuilder>) -> usize {
    b.as_ref().map_or(0, est_str)
}

/// Estimate memory usage of a `BinaryBuilder` (offsets + values).
pub fn est_bin(b: &BinaryBuilder) -> usize {
    b.values_slice().len() + (b.len() + 1) * 4
}

/// Estimate memory usage of a `ListBuilder<StringBuilder>` (offsets + inner).
pub fn est_list_str(b: &mut ListBuilder<StringBuilder>) -> usize {
    (b.len() + 1) * 4 + est_str(b.values())
}

/// Estimate memory usage of a `ListBuilder<UInt64Builder>` (offsets + inner).
pub fn est_list_u64(b: &mut ListBuilder<UInt64Builder>) -> usize {
    (b.len() + 1) * 4 + est_u64(b.values())
}

/// Estimate memory usage of a `ListBuilder<BinaryBuilder>` (offsets + inner).
pub fn est_list_bin(b: &mut ListBuilder<BinaryBuilder>) -> usize {
    (b.len() + 1) * 4 + est_bin(b.values())
}

/// Canonical block identity from Firehose BlockMetadata.
/// Added to every output table for chain-agnostic identification.
#[derive(Debug, Clone, Default)]
pub struct BlockIdentity {
    pub block_num: u64,
    pub block_id: String,
    pub parent_num: u64,
    pub parent_id: String,
    pub lib_num: u64,
    /// Block time in whole unix seconds. Drives partition routing, the `date`
    /// column and the cursor's `last_timestamp`.
    pub timestamp: i64,
    /// Sub-second part of the block time in nanoseconds, as carried by the
    /// Firehose metadata `time` (0 when absent). Only the canonical
    /// `timestamp` column uses it, via [`BlockIdentity::timestamp_millis`].
    pub timestamp_nanos: i32,
    /// Fork step: None when final_blocks_only=true, Some("NEW"/"UNDO"/"FINAL") otherwise.
    pub fork_step: Option<String>,
}

impl BlockIdentity {
    /// Block time in unix milliseconds, for the canonical `timestamp` column.
    pub fn timestamp_millis(&self) -> i64 {
        timestamp_millis(self.timestamp, self.timestamp_nanos)
    }
}

/// Combine unix seconds and a protobuf-style sub-second nanos part into unix
/// milliseconds. Nanos outside `0..1_000_000_000` are clamped.
pub fn timestamp_millis(seconds: i64, nanos: i32) -> i64 {
    let millis = i64::from(nanos.clamp(0, 999_999_999) / 1_000_000);
    seconds.saturating_mul(1_000).saturating_add(millis)
}

/// Arrow type of the canonical `timestamp` column and other block-time columns:
/// `Timestamp(Millisecond, UTC)`, written to Parquet as
/// `TIMESTAMP(MILLIS, isAdjustedToUTC=true)`. (Arrow `Timestamp(Second)` has no
/// Parquet logical type and reads back as a plain INT64 outside Arrow.)
pub fn timestamp_millis_utc_type() -> DataType {
    DataType::Timestamp(TimeUnit::Millisecond, Some(Arc::from("UTC")))
}

/// Convert a block timestamp expressed as UTC unix seconds into Arrow `Date32`
/// days since epoch.
pub fn date32_from_timestamp_seconds(timestamp_seconds: i64) -> i32 {
    timestamp_seconds
        .div_euclid(86_400)
        .try_into()
        .unwrap_or_else(|_| {
            panic!(
                "block timestamp {timestamp_seconds} exceeds Arrow Date32 range when converted to days"
            )
        })
}

/// Returns the 7 canonical identity fields to prepend to every schema.
/// Uses the given encoding to determine the data type of block_id/parent_id.
/// Pass `nullable_timestamps = true` for chains (e.g. Solana) where
/// `timestamp` and `date` may be null.
pub fn canonical_fields_with_encoding(encoding: &EncodeBytes) -> Vec<Field> {
    canonical_fields_with_encoding_nullable(encoding, false)
}

/// Returns the 7 canonical identity fields with nullable `timestamp` and `date`.
/// Use for Solana tables where `block_time` may be absent.
pub fn canonical_fields_with_nullable_timestamps(encoding: &EncodeBytes) -> Vec<Field> {
    canonical_fields_with_encoding_nullable(encoding, true)
}

fn canonical_fields_with_encoding_nullable(
    encoding: &EncodeBytes,
    nullable_timestamps: bool,
) -> Vec<Field> {
    let id_type = bytes_data_type(encoding);
    vec![
        Field::new("block_num", DataType::UInt64, false),
        Field::new("block_id", id_type.clone(), false),
        Field::new("parent_num", DataType::UInt64, false),
        Field::new("parent_id", id_type, false),
        Field::new("lib_num", DataType::UInt64, false),
        Field::new(
            "timestamp",
            timestamp_millis_utc_type(),
            nullable_timestamps,
        ),
        Field::new("date", DataType::Date32, nullable_timestamps),
    ]
}

/// Returns the 7 canonical identity fields (Utf8 block_id/parent_id).
/// Use `canonical_fields_with_encoding` when encoding matters.
pub fn canonical_fields() -> Vec<Field> {
    canonical_fields_with_encoding(&EncodeBytes::Hex)
}

/// Decode a hex block ID string to raw bytes.
pub fn decode_id_bytes(id: &str) -> Vec<u8> {
    let hex_str = id.strip_prefix("0x").unwrap_or(id);
    hex::decode(hex_str).unwrap_or_else(|_| id.as_bytes().to_vec())
}

/// The canonical identity columns of one block, with `block_id`/`parent_id`
/// already encoded and the `date` already derived.
///
/// Mappers prepare it once per block (see [`CanonicalBuilder::prepare`]) and
/// append it to every row of every table, instead of decoding and re-encoding the
/// ids on each row.
#[derive(Debug, Clone)]
pub struct PreparedIdentity {
    block_num: u64,
    parent_num: u64,
    lib_num: u64,
    /// Unix milliseconds; `None` writes a null `timestamp` and `date`.
    timestamp_millis: Option<i64>,
    date: Option<i32>,
    block_id: EncodedBytes,
    parent_id: EncodedBytes,
}

impl PreparedIdentity {
    /// Prepare `identity` with its Firehose metadata ids (hex strings, decoded by
    /// [`decode_id_bytes`]), encoded with `encoding`.
    pub fn new(identity: &BlockIdentity, encoding: &EncodeBytes) -> Self {
        Self::with_ids(
            identity,
            &decode_id_bytes(&identity.block_id),
            &decode_id_bytes(&identity.parent_id),
            encoding,
        )
    }

    /// Prepare `identity` with raw block and parent ids taken from the block
    /// itself, encoded with `encoding`.
    pub fn with_ids(
        identity: &BlockIdentity,
        block_id: &[u8],
        parent_id: &[u8],
        encoding: &EncodeBytes,
    ) -> Self {
        Self {
            block_num: identity.block_num,
            parent_num: identity.parent_num,
            lib_num: identity.lib_num,
            timestamp_millis: Some(identity.timestamp_millis()),
            date: Some(date32_from_timestamp_seconds(identity.timestamp)),
            block_id: EncodedBytes::new(block_id, encoding),
            parent_id: EncodedBytes::new(parent_id, encoding),
        }
    }

    /// Replace the block time with an optional time in whole unix seconds. `None`
    /// (e.g. a Solana block without `block_time`) writes null `timestamp` and
    /// `date` values.
    pub fn with_timestamp_seconds(mut self, timestamp_seconds: Option<i64>) -> Self {
        self.timestamp_millis = timestamp_seconds.map(|seconds| timestamp_millis(seconds, 0));
        self.date = timestamp_seconds.map(date32_from_timestamp_seconds);
        self
    }
}

/// Builder for canonical identity columns. Embed in each table builder.
pub struct CanonicalBuilder {
    pub block_num: UInt64Builder,
    block_id: BytesColumn,
    pub parent_num: UInt64Builder,
    parent_id: BytesColumn,
    pub lib_num: UInt64Builder,
    pub timestamp: TimestampMillisecondBuilder,
    pub date: Date32Builder,
}

impl CanonicalBuilder {
    pub fn new() -> Self {
        Self::with_encoding(&EncodeBytes::Hex)
    }

    /// Create a builder that encodes block_id/parent_id with the given strategy.
    pub fn with_encoding(encoding: &EncodeBytes) -> Self {
        Self {
            block_num: UInt64Builder::new(),
            block_id: BytesColumn::new(encoding),
            parent_num: UInt64Builder::new(),
            parent_id: BytesColumn::new(encoding),
            lib_num: UInt64Builder::new(),
            timestamp: TimestampMillisecondBuilder::new().with_timezone("UTC"),
            date: Date32Builder::new(),
        }
    }

    /// Prepare `identity`, with its Firehose metadata ids, in this builder's
    /// encoding. Every table of a mapper shares the canonical encoding, so the
    /// result can be appended to all of them.
    pub fn prepare(&self, identity: &BlockIdentity) -> PreparedIdentity {
        PreparedIdentity::new(identity, &self.block_id.encoding())
    }

    /// Prepare `identity` with raw block and parent ids taken from the block
    /// itself, in this builder's encoding.
    pub fn prepare_with_ids(
        &self,
        identity: &BlockIdentity,
        block_id: &[u8],
        parent_id: &[u8],
    ) -> PreparedIdentity {
        PreparedIdentity::with_ids(identity, block_id, parent_id, &self.block_id.encoding())
    }

    /// Append one row.
    ///
    /// # Panics
    /// If `id` was prepared with a different encoding than this builder's.
    pub fn append(&mut self, id: &PreparedIdentity) {
        self.block_num.append_value(id.block_num);
        self.parent_num.append_value(id.parent_num);
        self.lib_num.append_value(id.lib_num);
        self.timestamp.append_option(id.timestamp_millis);
        self.date.append_option(id.date);
        self.block_id.append_encoded(&id.block_id);
        self.parent_id.append_encoded(&id.parent_id);
    }

    pub fn finish(&mut self) -> Vec<Arc<dyn arrow::array::Array>> {
        vec![
            Arc::new(self.block_num.finish()),
            self.block_id.finish(),
            Arc::new(self.parent_num.finish()),
            self.parent_id.finish(),
            Arc::new(self.lib_num.finish()),
            Arc::new(self.timestamp.finish()),
            Arc::new(self.date.finish()),
        ]
    }

    pub fn len(&self) -> usize {
        self.block_num.len()
    }

    /// Estimate in-memory byte usage of all canonical columns.
    pub fn estimated_bytes(&self) -> usize {
        est_u64(&self.block_num)
            + self.block_id.estimated_bytes()
            + est_u64(&self.parent_num)
            + self.parent_id.estimated_bytes()
            + est_u64(&self.lib_num)
            + est_ts_ms(&self.timestamp)
            + est_date32(&self.date)
    }
}

/// Convert a Firehose ForkStep integer to a human-readable string.
pub fn fork_step_name(step: i32) -> Option<&'static str> {
    match step {
        1 => Some("NEW"),
        2 => Some("UNDO"),
        3 => Some("FINAL"),
        _ => None,
    }
}

/// Returns the fork_step field definition.
pub fn fork_step_field() -> Field {
    Field::new("fork_step", DataType::Utf8, false)
}

/// Builder for the fork_step column (a simple StringBuilder wrapper).
pub struct ForkStepBuilder {
    inner: StringBuilder,
}

impl ForkStepBuilder {
    pub fn new() -> Self {
        Self {
            inner: StringBuilder::new(),
        }
    }

    pub fn append(&mut self, value: &str) {
        self.inner.append_value(value);
    }

    pub fn finish(&mut self) -> Arc<dyn arrow::array::Array> {
        Arc::new(self.inner.finish())
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }
}

/// Trait for mapping raw protobuf block bytes into Arrow RecordBatches.
pub trait BlockMapper {
    /// Map raw protobuf bytes (from Any.value) into internal builders.
    /// Returns the number of transactions mapped for the block.
    fn map_block(
        &mut self,
        block_bytes: &[u8],
        identity: &BlockIdentity,
        fork_step: Option<&str>,
    ) -> anyhow::Result<u64>;

    /// Flush all buffered data into RecordBatches.
    fn flush(&mut self) -> anyhow::Result<HashMap<String, RecordBatch>>;

    /// Get current max rows across all tables.
    fn max_table_rows(&self) -> usize;

    /// Get total rows across all tables (sum).
    fn total_rows(&self) -> usize;

    /// Return the name and estimated in-memory byte size of the largest table.
    ///
    /// The size is the **maximum** across all per-table estimates so that
    /// `flush_bytes` controls the size of the biggest output file rather
    /// than the sum across all tables (which would produce many small files
    /// after per-table splitting and compression).
    fn largest_table(&mut self) -> (&str, usize);

    /// Estimate the in-memory byte usage of the largest single table.
    fn estimated_bytes(&mut self) -> usize {
        self.largest_table().1
    }

    /// Get table names.
    fn table_names(&self) -> Vec<&str>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{BinaryArray, Date32Array, TimestampMillisecondArray, UInt64Array};
    use arrow::record_batch::RecordBatch;
    use bytes::Bytes;
    use parquet::arrow::arrow_reader::{ArrowReaderOptions, ParquetRecordBatchReaderBuilder};
    use parquet::arrow::ArrowWriter;
    use parquet::basic::{LogicalType, TimeUnit as ParquetTimeUnit};

    /// Write one canonical-identity batch to an in-memory Parquet file.
    fn canonical_parquet_bytes(timestamp_millis: i64) -> Bytes {
        let schema = Arc::new(arrow::datatypes::Schema::new(
            canonical_fields_with_nullable_timestamps(&EncodeBytes::Binary),
        ));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(UInt64Array::from(vec![42_u64])),
                Arc::new(BinaryArray::from(vec![b"block-id".as_slice()])),
                Arc::new(UInt64Array::from(vec![41_u64])),
                Arc::new(BinaryArray::from(vec![b"parent-id".as_slice()])),
                Arc::new(UInt64Array::from(vec![40_u64])),
                Arc::new(
                    TimestampMillisecondArray::from(vec![timestamp_millis]).with_timezone("UTC"),
                ),
                Arc::new(Date32Array::from(vec![date32_from_timestamp_seconds(
                    timestamp_millis.div_euclid(1_000),
                )])),
            ],
        )
        .expect("record batch should build");

        let mut parquet_bytes = Vec::new();
        let mut writer =
            ArrowWriter::try_new(&mut parquet_bytes, Arc::clone(&schema), None).expect("writer");
        writer.write(&batch).expect("write batch");
        writer.close().expect("close writer");
        Bytes::from(parquet_bytes)
    }

    #[test]
    fn test_timestamp_millis_combines_seconds_and_nanos() {
        assert_eq!(timestamp_millis(1_700_000_000, 0), 1_700_000_000_000);
        assert_eq!(
            timestamp_millis(1_700_000_000, 500_000_000),
            1_700_000_000_500
        );
        assert_eq!(
            timestamp_millis(1_700_000_000, 999_999_999),
            1_700_000_000_999
        );
        assert_eq!(timestamp_millis(1_700_000_000, -1), 1_700_000_000_000);
        assert_eq!(
            timestamp_millis(1_700_000_000, 2_000_000_000),
            1_700_000_000_999
        );
    }

    #[test]
    fn test_date32_from_timestamp_seconds_uses_utc_days() {
        assert_eq!(date32_from_timestamp_seconds(0), 0);
        assert_eq!(date32_from_timestamp_seconds(86_399), 0);
        assert_eq!(date32_from_timestamp_seconds(86_400), 1);
        assert_eq!(date32_from_timestamp_seconds(-1), -1);
    }

    #[test]
    fn test_canonical_fields_include_date32() {
        let fields = canonical_fields();
        let date_field = fields
            .iter()
            .find(|field| field.name() == "date")
            .expect("date field should be present");

        assert_eq!(date_field.data_type(), &DataType::Date32);
        assert!(!date_field.is_nullable());
    }

    #[test]
    fn test_canonical_builder_derives_date_column_from_timestamp() {
        let mut builder = CanonicalBuilder::new();
        let identity = builder.prepare(&BlockIdentity {
            block_num: 42,
            block_id: "aa".to_string(),
            parent_num: 41,
            parent_id: "bb".to_string(),
            lib_num: 40,
            timestamp: 1_700_000_000,
            timestamp_nanos: 0,
            fork_step: None,
        });
        builder.append(&identity);

        let columns = builder.finish();
        let date_array = columns[6]
            .as_any()
            .downcast_ref::<Date32Array>()
            .expect("date column should be Date32");

        assert_eq!(
            date_array.value(0),
            date32_from_timestamp_seconds(1_700_000_000)
        );
    }

    #[test]
    fn test_canonical_fields_with_nullable_timestamps_are_nullable() {
        let fields = canonical_fields_with_nullable_timestamps(&EncodeBytes::Hex);
        let ts_field = fields.iter().find(|f| f.name() == "timestamp").unwrap();
        let date_field = fields.iter().find(|f| f.name() == "date").unwrap();
        assert!(ts_field.is_nullable());
        assert!(date_field.is_nullable());
    }

    #[test]
    fn test_prepared_identity_without_timestamp_writes_null_timestamp_and_date() {
        use arrow::array::Array;
        let mut builder = CanonicalBuilder::new();
        let identity = builder
            .prepare(&BlockIdentity {
                block_num: 100,
                block_id: "cc".to_string(),
                parent_num: 99,
                parent_id: "dd".to_string(),
                lib_num: 98,
                timestamp: 0,
                timestamp_nanos: 0,
                fork_step: None,
            })
            .with_timestamp_seconds(None);
        builder.append(&identity);

        let columns = builder.finish();
        let ts_array = columns[5]
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .expect("timestamp column should be TimestampMillisecondArray");
        let date_array = columns[6]
            .as_any()
            .downcast_ref::<Date32Array>()
            .expect("date column should be Date32");

        assert!(ts_array.is_null(0), "timestamp should be null");
        assert!(date_array.is_null(0), "date should be null");
    }

    #[test]
    fn test_prepared_identity_with_timestamp_seconds_writes_millis() {
        let mut builder = CanonicalBuilder::new();
        let identity = builder
            .prepare(&BlockIdentity {
                timestamp: 1,
                timestamp_nanos: 500_000_000,
                ..BlockIdentity::default()
            })
            .with_timestamp_seconds(Some(1_700_000_000));
        builder.append(&identity);

        let columns = builder.finish();
        let ts_array = columns[5]
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .expect("timestamp column should be TimestampMillisecondArray");
        let date_array = columns[6]
            .as_any()
            .downcast_ref::<Date32Array>()
            .expect("date column should be Date32");
        assert_eq!(ts_array.value(0), 1_700_000_000_000);
        assert_eq!(
            date_array.value(0),
            date32_from_timestamp_seconds(1_700_000_000)
        );
    }

    #[test]
    fn test_nullable_canonical_timestamp_and_date_stay_optional_in_parquet() {
        let parquet_bytes = canonical_parquet_bytes(1_700_000_000_000);

        let builder = ParquetRecordBatchReaderBuilder::try_new(parquet_bytes).expect("reader");
        let roundtrip_schema = builder.schema();
        let parquet_schema = builder.parquet_schema().root_schema();

        let timestamp_field = roundtrip_schema
            .field_with_name("timestamp")
            .expect("timestamp field should exist after roundtrip");
        let date_field = roundtrip_schema
            .field_with_name("date")
            .expect("date field should exist after roundtrip");
        assert!(timestamp_field.is_nullable());
        assert!(date_field.is_nullable());

        let parquet_timestamp = parquet_schema
            .get_fields()
            .iter()
            .find(|field| field.name() == "timestamp")
            .expect("timestamp field should exist in parquet schema");
        let parquet_date = parquet_schema
            .get_fields()
            .iter()
            .find(|field| field.name() == "date")
            .expect("date field should exist in parquet schema");
        assert!(
            parquet_timestamp.is_optional(),
            "parquet timestamp field should stay optional even without null values"
        );
        assert!(
            parquet_date.is_optional(),
            "parquet date field should stay optional even without null values"
        );
    }

    #[test]
    fn test_canonical_builder_keeps_sub_second_precision_and_second_based_date() {
        // 2023-11-14T23:59:59.500Z: the timestamp keeps the 500 ms, the date
        // stays on the UTC day of the whole second.
        let mut builder = CanonicalBuilder::new();
        let identity = builder.prepare(&BlockIdentity {
            timestamp: 1_700_006_399,
            timestamp_nanos: 500_000_000,
            ..BlockIdentity::default()
        });
        builder.append(&identity);

        let columns = builder.finish();
        let timestamps = columns[5]
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .expect("timestamp column should be TimestampMillisecondArray");
        let dates = columns[6]
            .as_any()
            .downcast_ref::<Date32Array>()
            .expect("date column should be Date32");
        assert_eq!(timestamps.value(0), 1_700_006_399_500);
        assert_eq!(dates.value(0), date32_from_timestamp_seconds(1_700_006_399));
    }

    #[test]
    fn test_canonical_timestamp_has_parquet_timestamp_millis_utc_logical_type() {
        let parquet_bytes = canonical_parquet_bytes(1_700_000_000_500);

        // What non-Arrow readers (DuckDB, Spark, Trino, ClickHouse) see.
        let builder =
            ParquetRecordBatchReaderBuilder::try_new(parquet_bytes.clone()).expect("reader");
        let descr = builder.parquet_schema();
        let column = (0..descr.num_columns())
            .map(|i| descr.column(i))
            .find(|column| column.name() == "timestamp")
            .expect("timestamp column in parquet schema");
        assert_eq!(
            column.logical_type_ref(),
            Some(&LogicalType::Timestamp {
                is_adjusted_to_u_t_c: true,
                unit: ParquetTimeUnit::MILLIS,
            })
        );

        // The Arrow type is recoverable from the Parquet type alone, without the
        // embedded Arrow schema.
        let options = ArrowReaderOptions::new().with_skip_arrow_metadata(true);
        let builder = ParquetRecordBatchReaderBuilder::try_new_with_options(parquet_bytes, options)
            .expect("reader without arrow metadata");
        let field = builder
            .schema()
            .field_with_name("timestamp")
            .expect("timestamp field")
            .clone();
        assert_eq!(field.data_type(), &timestamp_millis_utc_type());

        let batch = builder.build().unwrap().next().unwrap().unwrap();
        let values = batch
            .column_by_name("timestamp")
            .unwrap()
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .expect("timestamp column should be TimestampMillisecondArray");
        assert_eq!(values.value(0), 1_700_000_000_500);
    }

    /// The per-row path used before prepared identities: decode the metadata
    /// ids on every row and let each `BytesColumn` encode them again.
    fn legacy_append_ids(
        block_ids: &mut BytesColumn,
        parent_ids: &mut BytesColumn,
        id: &BlockIdentity,
    ) {
        block_ids.append_value(&decode_id_bytes(&id.block_id));
        parent_ids.append_value(&decode_id_bytes(&id.parent_id));
    }

    fn all_encodings() -> [EncodeBytes; 5] {
        [
            EncodeBytes::Binary,
            EncodeBytes::Hex,
            EncodeBytes::HexNoPrefix,
            EncodeBytes::Base58,
            EncodeBytes::TronBase58,
        ]
    }

    #[test]
    fn test_prepared_identity_ids_match_the_per_row_decode_and_encode_path() {
        let identities = [
            // 0x-prefixed hex (EVM-style metadata), plain hex (Antelope/Tron-style),
            // a non-hex id (kept as its UTF-8 bytes), a 21-byte Tron address-sized
            // id, and empty ids.
            (
                "0x".to_string() + &"ab".repeat(32),
                "0x".to_string() + &"cd".repeat(32),
            ),
            ("12".repeat(32), "34".repeat(32)),
            (
                "firehose-envelope-id".to_string(),
                "not hex either".to_string(),
            ),
            (
                "41".to_string() + &"aa".repeat(20),
                "41".to_string() + &"bb".repeat(20),
            ),
            (String::new(), String::new()),
        ];

        for encoding in all_encodings() {
            let mut builder = CanonicalBuilder::with_encoding(&encoding);
            let mut block_ids = BytesColumn::new(&encoding);
            let mut parent_ids = BytesColumn::new(&encoding);
            for (block_id, parent_id) in &identities {
                let identity = BlockIdentity {
                    block_id: block_id.clone(),
                    parent_id: parent_id.clone(),
                    ..BlockIdentity::default()
                };
                let prepared = builder.prepare(&identity);
                for _ in 0..3 {
                    builder.append(&prepared);
                    legacy_append_ids(&mut block_ids, &mut parent_ids, &identity);
                }
            }

            let columns = builder.finish();
            assert_eq!(
                columns[1].to_data(),
                block_ids.finish().to_data(),
                "block_id with {encoding:?}"
            );
            assert_eq!(
                columns[3].to_data(),
                parent_ids.finish().to_data(),
                "parent_id with {encoding:?}"
            );
        }
    }

    #[test]
    fn test_prepare_with_ids_matches_hex_round_trip_of_raw_ids() {
        // The per-chain helpers used to hex-encode raw ids into BlockIdentity and
        // decode them again on every row.
        let raw_ids: [&[u8]; 3] = [&[0xab; 32], &[0x01, 0x02, 0x03], &[]];
        for encoding in all_encodings() {
            let mut builder = CanonicalBuilder::with_encoding(&encoding);
            let mut block_ids = BytesColumn::new(&encoding);
            let mut parent_ids = BytesColumn::new(&encoding);
            for raw in raw_ids {
                let identity = BlockIdentity {
                    block_id: crate::encode::encode_hex(raw),
                    parent_id: crate::encode::encode_hex_no_prefix(raw),
                    ..BlockIdentity::default()
                };
                let prepared = builder.prepare_with_ids(&BlockIdentity::default(), raw, raw);
                builder.append(&prepared);
                legacy_append_ids(&mut block_ids, &mut parent_ids, &identity);
            }

            let columns = builder.finish();
            assert_eq!(
                columns[1].to_data(),
                block_ids.finish().to_data(),
                "{encoding:?}"
            );
            assert_eq!(
                columns[3].to_data(),
                parent_ids.finish().to_data(),
                "{encoding:?}"
            );
        }
    }

    #[test]
    #[should_panic(expected = "does not match the column encoding")]
    fn test_append_rejects_identity_prepared_with_another_encoding() {
        let identity = PreparedIdentity::new(&BlockIdentity::default(), &EncodeBytes::Base58);
        CanonicalBuilder::with_encoding(&EncodeBytes::Hex).append(&identity);
    }

    /// Per-row cost of the canonical identity columns, legacy path vs prepared.
    /// `cargo test --release -p firehose-parquet --lib bench_canonical_identity -- --ignored --nocapture`
    #[test]
    #[ignore = "benchmark"]
    fn bench_canonical_identity_per_row() {
        use std::hint::black_box;
        use std::time::Instant;

        const ROWS: usize = 1_000_000;
        let identity = BlockIdentity {
            block_num: 300_000_000,
            block_id: "0x".to_string() + &"ab".repeat(32),
            parent_num: 299_999_999,
            parent_id: "0x".to_string() + &"cd".repeat(32),
            lib_num: 299_999_968,
            timestamp: 1_700_000_000,
            timestamp_nanos: 500_000_000,
            fork_step: None,
        };

        for encoding in [EncodeBytes::Binary, EncodeBytes::Hex, EncodeBytes::Base58] {
            // Legacy: what `CanonicalBuilder::append(&BlockIdentity)` did per row.
            let mut block_num = UInt64Builder::with_capacity(ROWS);
            let mut parent_num = UInt64Builder::with_capacity(ROWS);
            let mut lib_num = UInt64Builder::with_capacity(ROWS);
            let mut timestamp = TimestampMillisecondBuilder::with_capacity(ROWS);
            let mut date = Date32Builder::with_capacity(ROWS);
            let mut block_ids = BytesColumn::new(&encoding);
            let mut parent_ids = BytesColumn::new(&encoding);
            let start = Instant::now();
            for _ in 0..ROWS {
                let id = black_box(&identity);
                block_num.append_value(id.block_num);
                parent_num.append_value(id.parent_num);
                lib_num.append_value(id.lib_num);
                timestamp.append_value(id.timestamp_millis());
                date.append_value(date32_from_timestamp_seconds(id.timestamp));
                legacy_append_ids(&mut block_ids, &mut parent_ids, id);
            }
            let legacy = start.elapsed();
            black_box((block_ids.finish(), parent_ids.finish()));

            let mut builder = CanonicalBuilder::with_encoding(&encoding);
            let start = Instant::now();
            let prepared = builder.prepare(black_box(&identity));
            for _ in 0..ROWS {
                builder.append(black_box(&prepared));
            }
            let fast = start.elapsed();
            black_box(builder.finish());

            println!(
                "{encoding:?}: legacy {:.0} ns/row, prepared {:.1} ns/row ({:.0}x)",
                legacy.as_nanos() as f64 / ROWS as f64,
                fast.as_nanos() as f64 / ROWS as f64,
                legacy.as_secs_f64() / fast.as_secs_f64()
            );
        }
    }
}

//! Parquet encoding of ingestion parts and the `date=YYYY-MM-DD` partition
//! contract. Every table file a `build` writes is a part of the protected
//! ingestion transaction ([`protected`]), committed to its table's Delta log
//! (#643); nothing here publishes a file on its own.
use crate::config::{BlockMetadata, Compression};
use crate::date_partition::{DatePartition, DATE_KEY};
use anyhow::Result;
use arrow::array::Array;
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use std::fs::File;
use std::path::Path;

mod local;
pub(crate) use local::create_dir_all_durable;
pub mod properties;
pub mod protected;

/// Key-value metadata to embed in every Parquet file's footer.
#[derive(Debug, Clone, Default)]
pub struct ParquetFileMetadata {
    /// Key-value pairs to store in the Parquet file metadata.
    pub entries: Vec<(String, String)>,
}

impl ParquetFileMetadata {
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    pub fn add(&mut self, key: impl Into<String>, value: impl Into<String>) {
        self.entries.push((key.into(), value.into()));
    }
}

/// The partition-relative directory, `<table>/date=YYYY-MM-DD`, of a flush
/// whose routing time is `metadata.min_timestamp`. Both routing times must be
/// valid; a flush without one has no partition.
pub fn partition_suffix(table: &str, metadata: &BlockMetadata) -> Result<String> {
    if let Some(max) = metadata.max_timestamp {
        crate::traits::checked_timestamp(max)?;
    }
    let seconds = metadata.min_timestamp.ok_or_else(|| {
        anyhow::anyhow!(
            "table `{table}` needs a routing block time: every table is partitioned by \
             date=YYYY-MM-DD"
        )
    })?;
    Ok(format!(
        "{table}/{}",
        DatePartition::from_timestamp(seconds)?
    ))
}

/// Check every row against the declared destination, without assuming row
/// order: reversible NEW/UNDO streams may visit the same partition in either
/// order. Metadata describes the whole mapper flush, not each table's exact
/// extrema. The partition key is formatted by `crate::date_partition` alone.
pub(crate) fn validate_partition(
    table: &str,
    batch: &RecordBatch,
    metadata: &BlockMetadata,
) -> Result<()> {
    let expected = partition_suffix(table, metadata)?;
    let check = |row_metadata: &BlockMetadata| -> Result<()> {
        let actual = partition_suffix(table, row_metadata)?;
        anyhow::ensure!(actual == expected,
            "table `{table}` spans partitions or disagrees with its metadata: expected `{expected}`, found `{actual}`; flush the mapper at partition boundaries");
        Ok(())
    };
    let column = batch.column_by_name("timestamp").ok_or_else(|| {
        anyhow::anyhow!("table `{table}` needs canonical timestamp for date partitioning")
    })?;
    // The mapper's `Timestamp(Millisecond, UTC)`, or the Delta data file's
    // `Timestamp(Microsecond, UTC)` (#643): whole seconds of each row.
    let seconds: Box<dyn Iterator<Item = i64> + '_> =
        if column.data_type() == &crate::traits::timestamp_millis_utc_type() {
            let millis = column
                .as_any()
                .downcast_ref::<arrow::array::TimestampMillisecondArray>()
                .expect("millisecond timestamp type checked above");
            Box::new(millis.iter().flatten().map(|value| value.div_euclid(1_000)))
        } else if column.data_type() == &crate::traits::timestamp_micros_utc_type() {
            let micros = column
                .as_any()
                .downcast_ref::<arrow::array::TimestampMicrosecondArray>()
                .expect("microsecond timestamp type checked above");
            Box::new(
                micros
                    .iter()
                    .flatten()
                    .map(|value| value.div_euclid(1_000_000)),
            )
        } else {
            anyhow::bail!(
                "table `{table}` timestamp must be Timestamp(Millisecond, UTC) or \
                 Timestamp(Microsecond, UTC)"
            )
        };
    let max = metadata.max_timestamp.ok_or_else(|| {
        anyhow::anyhow!("table `{table}` needs both minimum and maximum routing timestamps")
    })?;
    check(&BlockMetadata {
        min_timestamp: Some(max),
        ..metadata.clone()
    })?;
    let mut previous = None;
    for seconds in seconds {
        if previous == Some(seconds) {
            continue;
        }
        check(&BlockMetadata {
            min_timestamp: Some(seconds),
            ..metadata.clone()
        })?;
        previous = Some(seconds);
    }
    // Null Solana payload times deliberately use the metadata's synthetic
    // anchor.
    validate_date_column(table, batch, metadata)
}

/// A mapper `date` column must equal the `date=` partition of its rows
/// (#652). Both derive from the same checked block time, so this holds by
/// construction; the Delta data files leave the column out (#643), and the
/// partition value is the table's `date`.
fn validate_date_column(table: &str, batch: &RecordBatch, metadata: &BlockMetadata) -> Result<()> {
    let Some(column) = batch.column_by_name(DATE_KEY) else {
        return Ok(());
    };
    let dates = column
        .as_any()
        .downcast_ref::<arrow::array::Date32Array>()
        .ok_or_else(|| anyhow::anyhow!("table `{table}` date must be Date32"))?;
    let seconds = metadata
        .min_timestamp
        .expect("validate_partition requires a routing time");
    let expected = DatePartition::from_timestamp(seconds)?.date32();
    for date in dates.iter().flatten() {
        anyhow::ensure!(
            date == expected,
            "table `{table}` date {date} (days since 1970-01-01) differs from its date \
             partition; the date column and the partition come from the same block time"
        );
    }
    Ok(())
}

/// The writer properties of one part: [`properties::for_batch`] with
/// `metadata` in the footer.
pub(crate) fn writer_properties(
    compression: Compression,
    batch: &RecordBatch,
    metadata: &ParquetFileMetadata,
) -> Result<WriterProperties> {
    let metadata = (!metadata.entries.is_empty()).then(|| {
        metadata
            .entries
            .iter()
            .map(|(key, value)| {
                parquet::file::metadata::KeyValue::new(key.clone(), Some(value.clone()))
            })
            .collect()
    });
    properties::for_batch(compression, batch, metadata)
}

/// Encodes `batch` as one complete Parquet file into `sink`, with the part
/// properties and `metadata` in its footer. An in-memory part of the
/// protected transaction is encoded this way.
pub(crate) fn encode_into<W: std::io::Write + Send>(
    sink: &mut W,
    batch: &RecordBatch,
    compression: Compression,
    metadata: &ParquetFileMetadata,
) -> Result<()> {
    let mut parquet = ArrowWriter::try_new(
        sink,
        batch.schema(),
        Some(writer_properties(compression, batch, metadata)?),
    )?;
    parquet.write(batch)?;
    parquet.close()?;
    Ok(())
}

/// The Parquet file of `batch`, in memory, encoded as an in-memory `build`
/// part is ([`encode_into`]). Tests and offline tools use it to round-trip
/// mapper rows; it publishes nothing.
pub fn encode_parquet(
    batch: &RecordBatch,
    compression: Compression,
    metadata: &ParquetFileMetadata,
) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    encode_into(&mut bytes, batch, compression, metadata)?;
    Ok(bytes)
}

/// Fixed compression ratio (compressed/uncompressed) used for diagnostic
/// estimates of validated data retained after a failed table write.
pub(crate) fn compression_ratio(compression: &Compression) -> f64 {
    match compression {
        Compression::None => 0.50,   // Parquet encoding alone: ~2×
        Compression::Snappy => 0.25, // Parquet + Snappy: ~4×
        Compression::Gzip => 0.12,   // Parquet + Gzip: ~8×
        Compression::Zstd | Compression::ZstdWithLevel(_) => 0.12, // Parquet + Zstd: ~8×
    }
}

/// Parse an S3 URL into (bucket, prefix).
///
/// Supports: `s3://bucket/prefix/path` → `("bucket", "prefix/path")`
pub fn parse_s3_url(url: &str) -> Result<(String, String)> {
    let rest = url
        .strip_prefix("s3://")
        .ok_or_else(|| anyhow::anyhow!("expected s3:// URL, got: {url}"))?;
    let (bucket, prefix) = match rest.find('/') {
        Some(i) => {
            let prefix = rest[i + 1..].trim_end_matches('/');
            (rest[..i].to_string(), prefix.to_string())
        }
        None => (rest.to_string(), String::new()),
    };
    if bucket.is_empty() {
        return Err(anyhow::anyhow!("S3 URL missing bucket name: {url}"));
    }
    Ok((bucket, prefix))
}

/// Returns [`object_store::PutOptions`] with cache and content-type headers.
///
/// When `cache_control` is non-empty, sets the `Cache-Control` header so that
/// Tigris / CloudFront / any CDN caches accordingly.
pub fn s3_put_options(cache_control: &str) -> object_store::PutOptions {
    use object_store::Attribute;

    let mut attrs = object_store::Attributes::new();
    if !cache_control.is_empty() {
        attrs.insert(Attribute::CacheControl, cache_control.to_string().into());
    }
    attrs.insert(
        Attribute::ContentType,
        "application/vnd.apache.parquet".into(),
    );
    object_store::PutOptions {
        attributes: attrs,
        ..Default::default()
    }
}

/// Read a Parquet file and return all RecordBatches.
pub fn read_parquet(path: &Path) -> Result<Vec<RecordBatch>> {
    read_batches(File::open(path)?)
}

/// Every row of a Parquet file held in memory.
pub fn decode_parquet(bytes: impl Into<Bytes>) -> Result<Vec<RecordBatch>> {
    read_batches(bytes.into())
}

fn read_batches<R: parquet::file::reader::ChunkReader + 'static>(
    input: R,
) -> Result<Vec<RecordBatch>> {
    let reader = ParquetRecordBatchReaderBuilder::try_new(input)?.build()?;
    Ok(reader.collect::<std::result::Result<Vec<_>, _>>()?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    /// 2024-01-15T12:00:00Z.
    const T: i64 = 1_705_320_000;

    fn partition_batch(blocks: Vec<Option<u64>>, timestamps: Vec<Option<i64>>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("block_num", DataType::UInt64, true),
            Field::new(
                "timestamp",
                crate::traits::timestamp_millis_utc_type(),
                true,
            ),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(arrow::array::UInt64Array::from(blocks)),
                Arc::new(
                    arrow::array::TimestampMillisecondArray::from(timestamps).with_timezone("UTC"),
                ),
            ],
        )
        .unwrap()
    }

    fn routed_metadata(timestamp: Option<i64>) -> BlockMetadata {
        BlockMetadata {
            min_block_number: 500,
            max_block_number: 599,
            min_timestamp: timestamp,
            max_timestamp: timestamp,
        }
    }

    fn assert_refused(batch: RecordBatch, metadata: BlockMetadata) {
        assert!(validate_partition("blocks", &batch, &metadata).is_err());
    }

    #[test]
    fn encoded_parts_round_trip_with_their_footer_metadata() {
        let batch = partition_batch(vec![Some(42)], vec![Some(T * 1_000)]);
        let mut metadata = ParquetFileMetadata::new();
        metadata.add("firehose-parquet.block_type", "evm");
        let bytes = encode_parquet(&batch, Compression::Snappy, &metadata).unwrap();
        assert_eq!(decode_parquet(bytes.clone()).unwrap(), vec![batch]);
        let reader = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(bytes)).unwrap();
        let footer = reader
            .metadata()
            .file_metadata()
            .key_value_metadata()
            .unwrap();
        assert!(footer.iter().any(
            |kv| kv.key == "firehose-parquet.block_type" && kv.value.as_deref() == Some("evm")
        ));
    }

    #[test]
    fn test_date_partitioning() {
        assert_eq!(
            partition_suffix("blocks", &routed_metadata(Some(T))).unwrap(),
            "blocks/date=2024-01-15"
        );
        // The next UTC day is the next partition.
        assert_eq!(
            partition_suffix("blocks", &routed_metadata(Some(1_705_363_200))).unwrap(),
            "blocks/date=2024-01-16"
        );
        // Without a routing time there is no partition to write to.
        let error = partition_suffix("blocks", &routed_metadata(None))
            .unwrap_err()
            .to_string();
        assert!(error.contains("date=YYYY-MM-DD"), "{error}");
    }

    #[test]
    fn test_parse_s3_url_with_prefix() {
        let (bucket, prefix) = parse_s3_url("s3://my-bucket/some/prefix").unwrap();
        assert_eq!(bucket, "my-bucket");
        assert_eq!(prefix, "some/prefix");
    }

    #[test]
    fn test_parse_s3_url_bucket_only() {
        let (bucket, prefix) = parse_s3_url("s3://my-bucket").unwrap();
        assert_eq!(bucket, "my-bucket");
        assert_eq!(prefix, "");
    }

    #[test]
    fn test_parse_s3_url_trailing_slash() {
        let (bucket, prefix) = parse_s3_url("s3://my-bucket/path/").unwrap();
        assert_eq!(bucket, "my-bucket");
        assert_eq!(prefix, "path");
    }

    #[test]
    fn test_parse_s3_url_invalid() {
        assert!(parse_s3_url("http://not-s3").is_err());
        assert!(parse_s3_url("s3://").is_err());
    }

    #[test]
    fn test_date_partition_validation_checks_all_rows_and_metadata_endpoints() {
        let batch = partition_batch(
            vec![Some(501), Some(502), Some(503), Some(504)],
            vec![
                Some(T * 1_000),
                Some((T + 86_400 * 2) * 1_000),
                Some((T + 86_400) * 1_000),
                Some(T * 1_000),
            ],
        );
        assert_refused(batch, routed_metadata(Some(T)));
        let batch = partition_batch(vec![Some(501)], vec![Some(T * 1_000)]);
        let mut metadata = routed_metadata(Some(T));
        metadata.max_timestamp = Some(T + 86_400);
        assert_refused(batch, metadata);
    }

    #[test]
    fn test_date_partition_validation_allows_unordered_same_partition_and_null_anchor() {
        let batch = partition_batch(
            vec![Some(503), Some(501), Some(502)],
            vec![Some((T + 2) * 1_000), None, Some(T * 1_000 + 999)],
        );
        validate_partition("blocks", &batch, &routed_metadata(Some(T))).unwrap();
    }

    #[test]
    fn invalid_times_have_no_partition() {
        for timestamp in [i64::MIN, i64::MAX, 1_700_000_000_000] {
            let batch = partition_batch(vec![Some(501)], vec![Some(0)]);
            assert_refused(batch.clone(), routed_metadata(Some(timestamp)));
            let mut metadata = routed_metadata(Some(0));
            metadata.max_timestamp = Some(timestamp);
            assert_refused(batch.clone(), metadata.clone());
            assert!(partition_suffix("blocks", &routed_metadata(Some(timestamp))).is_err());
            assert!(partition_suffix("blocks", &metadata).is_err());
        }
        let batch = partition_batch(vec![Some(501)], vec![Some(i64::MAX)]);
        assert_refused(batch, routed_metadata(Some(0)));
    }

    #[test]
    fn test_negative_milliseconds_use_floor_seconds_for_partition_membership() {
        let batch = partition_batch(vec![Some(501), Some(502)], vec![Some(-1), Some(-999)]);
        let metadata = routed_metadata(Some(-1));
        validate_partition("blocks", &batch, &metadata).unwrap();
        assert_eq!(
            partition_suffix("blocks", &metadata).unwrap(),
            "blocks/date=1969-12-31"
        );
    }

    /// Null payload times (Solana without `block_time`) use the metadata's
    /// routing anchor; a flush without any routing time is refused.
    #[test]
    fn test_null_timestamps_route_by_the_metadata_anchor() {
        let batch = partition_batch(vec![Some(501)], vec![None]);
        let metadata = routed_metadata(Some(T));
        assert_eq!(
            partition_suffix("blocks", &metadata).unwrap(),
            "blocks/date=2024-01-15"
        );
        validate_partition("blocks", &batch, &metadata).unwrap();
        assert_refused(batch, routed_metadata(None));
    }

    /// The `date` column equals the `date=` partition of its rows (#652): a
    /// matching column passes, a disagreeing or mistyped one is refused, and
    /// null dates follow their null times.
    #[test]
    fn test_date_column_must_equal_its_date_partition() {
        let with_dates = |dates: Vec<Option<i32>>| {
            let base = partition_batch(
                vec![Some(501); dates.len()],
                dates.iter().map(|date| date.map(|_| T * 1_000)).collect(),
            );
            let mut fields = base.schema().fields().to_vec();
            fields.push(Arc::new(Field::new("date", DataType::Date32, true)));
            let mut columns = base.columns().to_vec();
            columns.push(Arc::new(arrow::array::Date32Array::from(dates)));
            RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
        };
        let day = crate::traits::date32_from_timestamp_seconds(T).unwrap();
        validate_partition(
            "blocks",
            &with_dates(vec![Some(day), None]),
            &routed_metadata(Some(T)),
        )
        .unwrap();
        for wrong in [day - 1, day + 1] {
            assert_refused(
                with_dates(vec![Some(day), Some(wrong)]),
                routed_metadata(Some(T)),
            );
        }
        let base = partition_batch(vec![Some(501)], vec![Some(T * 1_000)]);
        let mut fields = base.schema().fields().to_vec();
        fields.push(Arc::new(Field::new("date", DataType::Int32, false)));
        let mut columns = base.columns().to_vec();
        columns.push(Arc::new(arrow::array::Int32Array::from(vec![day])));
        assert_refused(
            RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap(),
            routed_metadata(Some(T)),
        );
    }

    #[test]
    fn test_date_partition_rejects_missing_mistyped_or_unanchored_timestamps() {
        assert_refused(
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new(
                    "block_num",
                    DataType::UInt64,
                    false,
                )])),
                vec![Arc::new(arrow::array::UInt64Array::from(vec![42]))],
            )
            .unwrap(),
            routed_metadata(Some(T)),
        );
        let wrong = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "timestamp",
                DataType::Int64,
                false,
            )])),
            vec![Arc::new(arrow::array::Int64Array::from(vec![T]))],
        )
        .unwrap();
        assert_refused(wrong, routed_metadata(Some(T)));
        let unzoned = arrow::array::TimestampMillisecondArray::from(vec![T * 1_000]);
        let wrong = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "timestamp",
                unzoned.data_type().clone(),
                false,
            )])),
            vec![Arc::new(unzoned)],
        )
        .unwrap();
        assert_refused(wrong, routed_metadata(Some(T)));
        let batch = partition_batch(vec![Some(501)], vec![Some(T * 1_000)]);
        assert_refused(batch.clone(), routed_metadata(None));
        let mut metadata = routed_metadata(Some(T));
        metadata.max_timestamp = None;
        assert_refused(batch, metadata);
    }

    #[test]
    fn test_metadata_merge() {
        let mut a = BlockMetadata {
            min_block_number: 100,
            max_block_number: 200,
            min_timestamp: Some(1000),
            max_timestamp: Some(2000),
        };
        let b = BlockMetadata {
            min_block_number: 50,
            max_block_number: 300,
            min_timestamp: Some(500),
            max_timestamp: Some(2500),
        };
        a.merge(&b);
        assert_eq!(a.min_block_number, 50);
        assert_eq!(a.max_block_number, 300);
        assert_eq!(a.min_timestamp, Some(500));
        assert_eq!(a.max_timestamp, Some(2500));
    }
}

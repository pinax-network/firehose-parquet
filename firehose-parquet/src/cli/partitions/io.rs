//! Strict partition-index loading, schema validation and publication.
use super::*;

pub fn read_partitions_build_rows(
    path: &str,
    aws: Option<&AwsConfig>,
) -> anyhow::Result<Vec<PartitionBuildRow>> {
    Ok(read_keyed_partitions_build_rows(path, aws)?
        .into_iter()
        .map(|(_, row)| row)
        .collect())
}

/// Read `partitions.parquet` rows paired with their numeric `partition` column value,
/// sorted by partition type, partition key, and block bounds.
pub(in crate::cli) fn read_keyed_partitions_build_rows(
    path: &str,
    aws: Option<&AwsConfig>,
) -> anyhow::Result<Vec<(u64, PartitionBuildRow)>> {
    let mut rows = read_partition_index_snapshot(path, aws)?.rows;
    sort_keyed_partition_rows(&mut rows);
    Ok(rows)
}

pub(in crate::cli) struct PartitionIndexSnapshot {
    pub(in crate::cli) rows: Vec<(u64, PartitionBuildRow)>,
    pub(in crate::cli) coverage: Option<PartitionCoverage>,
    pub(in crate::cli) proofs: Vec<PartitionSpanProof>,
}

/// Decode rows, coverage and boundary flags from the same file/object snapshot.
pub fn read_verified_partitions_index(
    path: &str,
    aws: Option<&AwsConfig>,
) -> anyhow::Result<VerifiedPartitionIndex> {
    verified_index_from_snapshot(read_partition_index_snapshot(path, aws)?)
}

pub(in crate::cli) fn verified_index_from_snapshot(
    snapshot: PartitionIndexSnapshot,
) -> anyhow::Result<VerifiedPartitionIndex> {
    let coverage = snapshot.coverage.ok_or_else(|| anyhow::anyhow!(
        "partition index has no verified coverage/completeness metadata; rebuild legacy indexes before resolution or resume"))?;
    anyhow::ensure!(
        snapshot.rows.len() == snapshot.proofs.len(),
        "partition index has missing span proofs"
    );
    let mut spans = snapshot
        .rows
        .into_iter()
        .zip(snapshot.proofs)
        .map(|((_, row), proof)| VerifiedPartitionSpan { row, proof })
        .collect::<Vec<_>>();
    spans.sort_by_key(|span| span.row.start_block);
    let index = VerifiedPartitionIndex { coverage, spans };
    index.validate()?;
    Ok(index)
}

pub(in crate::cli) fn read_partition_index_snapshot(
    path: &str,
    aws: Option<&AwsConfig>,
) -> anyhow::Result<PartitionIndexSnapshot> {
    let path = resolve_parquet_input_path_string(path);
    if path.starts_with("s3://") {
        use object_store::ObjectStore;
        let aws = aws.ok_or_else(|| anyhow::anyhow!("AWS config required for S3 paths"))?;
        let (bucket, key) = crate::writer::parse_s3_url(&path)?;
        let client = aws.build_s3_client(&bucket)?;
        let object_path = object_store::path::Path::from(key.as_str());
        let data = block_on_async(async { client.get(&object_path).await?.bytes().await })
            .map_err(|error| anyhow::Error::from(error).context(format!("reading {path}")))?;
        partition_index_snapshot_from_reader(data, false)
    } else {
        let file = std::fs::File::open(&path)
            .map_err(|error| anyhow::Error::from(error).context(format!("opening {path}")))?;
        partition_index_snapshot_from_reader(file, false)
    }
}

/// Strict snapshot decoder shared with native async protected ingestion reads.
/// The caller bounds the compressed byte stream before collecting it.
pub(crate) fn read_verified_partitions_index_bytes(
    data: bytes::Bytes,
) -> anyhow::Result<VerifiedPartitionIndex> {
    verified_index_from_snapshot(partition_index_snapshot_from_reader(data, true)?)
}

pub(in crate::cli) fn partition_index_snapshot_from_reader<
    T: parquet::file::reader::ChunkReader + 'static,
>(
    input: T,
    bounded: bool,
) -> anyhow::Result<PartitionIndexSnapshot> {
    use arrow::array::{
        Array, Int32Array, Int64Array, LargeStringArray, StringArray, TimestampSecondArray,
        UInt32Array, UInt64Array,
    };
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    fn read_utf8_value(column: &dyn Array, row: usize) -> anyhow::Result<Option<String>> {
        if column.is_null(row) {
            return Ok(None);
        }
        if let Some(arr) = column.as_any().downcast_ref::<StringArray>() {
            return Ok(Some(arr.value(row).to_string()));
        }
        if let Some(arr) = column.as_any().downcast_ref::<LargeStringArray>() {
            return Ok(Some(arr.value(row).to_string()));
        }
        anyhow::bail!("expected utf8 column, found {}", column.data_type())
    }

    fn read_timestamp_as_string(column: &dyn Array, row: usize) -> anyhow::Result<Option<String>> {
        if column.is_null(row) {
            return Ok(None);
        }
        if let Some(arr) = column.as_any().downcast_ref::<StringArray>() {
            return Ok(Some(arr.value(row).to_string()));
        }
        if let Some(arr) = column.as_any().downcast_ref::<LargeStringArray>() {
            return Ok(Some(arr.value(row).to_string()));
        }
        if let Some(arr) = column.as_any().downcast_ref::<TimestampSecondArray>() {
            return Ok(Some(format_partition_timestamp(arr.value(row))?));
        }
        anyhow::bail!(
            "expected utf8 or timestamp(second, UTC) column, found {}",
            column.data_type()
        )
    }

    fn read_i64_value(column: &dyn Array, row: usize) -> anyhow::Result<Option<i64>> {
        if column.is_null(row) {
            return Ok(None);
        }
        if let Some(arr) = column.as_any().downcast_ref::<Int64Array>() {
            return Ok(Some(arr.value(row)));
        }
        if let Some(arr) = column.as_any().downcast_ref::<Int32Array>() {
            return Ok(Some(arr.value(row) as i64));
        }
        if let Some(arr) = column.as_any().downcast_ref::<UInt64Array>() {
            return Ok(Some(arr.value(row) as i64));
        }
        if let Some(arr) = column.as_any().downcast_ref::<UInt32Array>() {
            return Ok(Some(arr.value(row) as i64));
        }
        anyhow::bail!("expected integer column, found {}", column.data_type())
    }

    fn read_u64_value(column: &dyn Array, row: usize) -> anyhow::Result<Option<u64>> {
        if column.is_null(row) {
            return Ok(None);
        }
        if let Some(values) = column.as_any().downcast_ref::<UInt64Array>() {
            return Ok(Some(values.value(row)));
        }
        read_i64_value(column, row)?.map_or(Ok(None), |value| {
            if value < 0 {
                anyhow::bail!("negative integer value: {value}");
            }
            Ok(Some(value as u64))
        })
    }

    /// File-level metadata carried by the canonical partitions index schema.
    #[derive(Debug, Clone, Default)]
    struct PartitionsFileContext {
        chain: Option<String>,
        partition_type: Option<String>,
        interval: Option<i64>,
        coverage: Option<PartitionCoverage>,
    }

    fn collect_rows(
        batch: &arrow::record_batch::RecordBatch,
        rows: &mut Vec<(u64, PartitionBuildRow)>,
        proofs: &mut Vec<PartitionSpanProof>,
        read_utf8_value: &impl Fn(&dyn Array, usize) -> anyhow::Result<Option<String>>,
        read_u64_value: &impl Fn(&dyn Array, usize) -> anyhow::Result<Option<u64>>,
        file_ctx: &PartitionsFileContext,
    ) -> anyhow::Result<()> {
        let schema = batch.schema();
        if file_ctx.coverage.is_some() {
            use arrow::datatypes::{DataType, Field, TimeUnit};
            for expected in [
                Field::new("partition", DataType::UInt64, false),
                Field::new("start_block", DataType::UInt64, false),
                Field::new("stop_block", DataType::UInt64, false),
                Field::new(
                    "start_time",
                    DataType::Timestamp(TimeUnit::Second, Some("UTC".into())),
                    true,
                ),
                Field::new(
                    "end_time",
                    DataType::Timestamp(TimeUnit::Second, Some("UTC".into())),
                    true,
                ),
            ] {
                anyhow::ensure!(
                    schema.field_with_name(expected.name())? == &expected,
                    "verified partition column {} has an unexpected type/nullability",
                    expected.name()
                );
            }
        }
        let partition_type = file_ctx
            .partition_type
            .as_ref()
            .ok_or_else(|| {
                anyhow::anyhow!("missing required metadata: firehose-parquet.partition")
            })?
            .clone();
        let partition_value_idx = schema
            .index_of("partition")
            .map_err(|_| anyhow::anyhow!("missing required column: partition"))?;
        let start_block_idx = schema
            .index_of("start_block")
            .map_err(|_| anyhow::anyhow!("missing required column: start_block"))?;
        let stop_block_idx = schema
            .index_of("stop_block")
            .map_err(|_| anyhow::anyhow!("missing required column: stop_block"))?;
        let chain_idx = schema.index_of("chain").ok();
        let start_time_idx = schema.index_of("start_time").ok();
        let end_time_idx = schema.index_of("end_time").ok();

        for row_index in 0..batch.num_rows() {
            let partition_column = batch.column(partition_value_idx).as_ref();
            let partition_raw = read_u64_value(partition_column, row_index)?
                .ok_or_else(|| anyhow::anyhow!("partition cannot be null"))?;
            let partition_value = if partition_type == "block_range" {
                partition_raw.to_string()
            } else {
                format_partition_timestamp(partition_raw as i64)?
            };
            let partition_start_ts = partition_value.clone();
            let partition_interval_seconds = file_ctx.interval.unwrap_or_else(|| {
                PartitionBuildType::from_cli_value(&partition_type)
                    .map(|kind| kind.interval_seconds())
                    .unwrap_or_default()
            });
            let start_block = read_u64_value(batch.column(start_block_idx).as_ref(), row_index)?
                .ok_or_else(|| anyhow::anyhow!("start_block cannot be null"))?;
            let stop_block = read_u64_value(batch.column(stop_block_idx).as_ref(), row_index)?
                .ok_or_else(|| anyhow::anyhow!("stop_block cannot be null"))?;

            let chain = if let Some(idx) = chain_idx {
                read_utf8_value(batch.column(idx).as_ref(), row_index)?
            } else {
                file_ctx.chain.clone()
            };

            let read_time = |idx: Option<usize>| -> anyhow::Result<Option<String>> {
                let result = idx
                    .map(|idx| read_timestamp_as_string(batch.column(idx).as_ref(), row_index))
                    .transpose()
                    .map(Option::flatten);
                if file_ctx.coverage.is_some() {
                    // A malformed verified timestamp must not become nullable routing context.
                    result
                } else {
                    Ok(result.unwrap_or(None))
                }
            };
            let start_time = read_time(start_time_idx)?;
            let end_time = read_time(end_time_idx)?;

            if file_ctx.coverage.is_some() {
                proofs.push(crate::partition_index::read_proof(batch, row_index)?);
            }
            rows.push((
                partition_raw,
                PartitionBuildRow {
                    partition_type: partition_type.clone(),
                    partition_interval_seconds,
                    partition_start_ts,
                    partition_value,
                    start_block,
                    stop_block,
                    start_time,
                    end_time,
                    chain,
                },
            ));
        }

        Ok(())
    }

    let mut rows = Vec::new();
    let mut proofs = Vec::new();
    let coverage;
    /// Extract canonical chain/type/interval from Parquet file-level metadata.
    fn extract_file_context(
        file_metadata: &parquet::file::metadata::FileMetaData,
    ) -> anyhow::Result<PartitionsFileContext> {
        let kv = file_metadata.key_value_metadata();
        let find = |key: &str| -> Option<String> {
            kv.as_ref().and_then(|kvs| {
                kvs.iter()
                    .find(|kv| kv.key == key)
                    .and_then(|kv| kv.value.clone())
            })
        };
        let chain = find("firehose-parquet.chain_name");
        let partition_type = find("firehose-parquet.partition")
            .and_then(|pt| canonical_partition_type_label(&pt).ok());
        let interval = find("firehose-parquet.block_range_size")
            .and_then(|v| v.parse::<i64>().ok())
            .filter(|v| *v > 0)
            .or_else(|| {
                partition_type.as_ref().and_then(|pt| {
                    PartitionBuildType::from_cli_value(pt)
                        .ok()
                        .map(|kind| kind.interval_seconds())
                })
            });
        Ok(PartitionsFileContext {
            chain,
            partition_type,
            interval,
            coverage: find(INDEX_COVERAGE_METADATA)
                .map(|value| serde_json::from_str(&value))
                .transpose()?,
        })
    }

    let builder = ParquetRecordBatchReaderBuilder::try_new(input)?;
    if bounded {
        let metadata = builder.metadata();
        anyhow::ensure!(
            metadata.file_metadata().num_rows() >= 0
                && metadata.file_metadata().num_rows() <= 1_000_000,
            "standalone index exceeds protected initialization row limit"
        );
        let uncompressed = metadata
            .row_groups()
            .iter()
            .try_fold(0_u64, |total, group| {
                anyhow::ensure!(
                    group.total_byte_size() >= 0,
                    "invalid standalone index row-group size"
                );
                total
                    .checked_add(group.total_byte_size() as u64)
                    .ok_or_else(|| anyhow::anyhow!("standalone index size overflow"))
            })?;
        anyhow::ensure!(
            uncompressed <= 512 * 1024 * 1024,
            "standalone index exceeds protected initialization decoded-size limit"
        );
    }
    validate_partitions_schema(builder.schema())?;
    validate_partitions_metadata(builder.metadata().file_metadata())?;
    let file_ctx = extract_file_context(builder.metadata().file_metadata())?;
    coverage = file_ctx.coverage.clone();
    for batch in builder.build()? {
        let batch = batch?;
        if bounded {
            anyhow::ensure!(
                batch.num_rows() <= 1_000_000_usize.saturating_sub(rows.len()),
                "standalone index exceeds protected initialization observed-row limit"
            );
        }
        collect_rows(
            &batch,
            &mut rows,
            &mut proofs,
            &read_utf8_value,
            &read_u64_value,
            &file_ctx,
        )?;
    }

    Ok(PartitionIndexSnapshot {
        rows,
        coverage,
        proofs,
    })
}

pub fn write_partitions_index(
    path: &str,
    rows: &[PartitionBuildRow],
    aws: Option<&AwsConfig>,
) -> anyhow::Result<()> {
    write_partitions_index_with_metadata(path, rows, Compression::Zstd, aws, None)
}

pub fn write_partitions_index_with_metadata(
    path: &str,
    rows: &[PartitionBuildRow],
    compression: Compression,
    aws: Option<&AwsConfig>,
    file_metadata: Option<&crate::writer::ParquetFileMetadata>,
) -> anyhow::Result<()> {
    write_partitions_index_impl(path, rows, compression, aws, file_metadata, None)
}

/// Write partitions index.
pub fn write_partitions_index_strict(
    path: &str,
    rows: &[PartitionBuildRow],
    compression: Compression,
    aws: Option<&AwsConfig>,
    file_metadata: Option<&crate::writer::ParquetFileMetadata>,
) -> anyhow::Result<()> {
    write_partitions_index_impl(path, rows, compression, aws, file_metadata, None)
}

/// Write a v2 snapshot only after coverage and every span have been validated.
pub fn write_verified_partitions_index(
    path: &str,
    index: &VerifiedPartitionIndex,
    compression: Compression,
    aws: Option<&AwsConfig>,
    file_metadata: Option<&crate::writer::ParquetFileMetadata>,
) -> anyhow::Result<()> {
    index.validate()?;
    let mut metadata = file_metadata.cloned().unwrap_or_default();
    let first = &index.spans[0].row;
    for (key, value) in [
        ("firehose-parquet.chain_name", first.chain.clone().unwrap()),
        ("firehose-parquet.partition", first.partition_type.clone()),
        (
            "firehose-parquet.block_range_size",
            if first.partition_type == "block_range" {
                first.partition_interval_seconds.to_string()
            } else {
                "0".into()
            },
        ),
    ] {
        anyhow::ensure!(
            metadata
                .entries
                .iter()
                .filter(|(existing, _)| existing == key)
                .all(|(_, existing)| existing == &value),
            "partition metadata disagrees with verified rows for {key}"
        );
        if !metadata.entries.iter().any(|(existing, _)| existing == key) {
            metadata.add(key, value);
        }
    }
    metadata
        .entries
        .retain(|(key, _)| key != INDEX_COVERAGE_METADATA);
    metadata.add(
        INDEX_COVERAGE_METADATA,
        serde_json::to_string(&index.coverage)?,
    );
    let rows = index
        .spans
        .iter()
        .map(|span| span.row.clone())
        .collect::<Vec<_>>();
    let proofs = index
        .spans
        .iter()
        .map(|span| span.proof.clone())
        .collect::<Vec<_>>();
    write_partitions_index_impl(
        path,
        &rows,
        compression,
        aws,
        Some(&metadata),
        Some(&proofs),
    )
}

pub(in crate::cli) fn write_partitions_index_impl(
    path: &str,
    rows: &[PartitionBuildRow],
    compression: Compression,
    aws: Option<&AwsConfig>,
    file_metadata: Option<&crate::writer::ParquetFileMetadata>,
    proofs: Option<&[PartitionSpanProof]>,
) -> anyhow::Result<()> {
    use arrow::array::{TimestampSecondArray, UInt64Array};
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;
    use parquet::file::metadata::KeyValue;
    use parquet::file::properties::WriterProperties;
    use std::sync::Arc;

    if rows.is_empty() {
        anyhow::bail!("cannot write an empty partitions index");
    }

    if let Some(proofs) = proofs {
        anyhow::ensure!(
            proofs.len() == rows.len(),
            "partition rows and proofs have different lengths"
        );
    }

    // Build minimal metadata from rows when no external metadata is provided.
    // This ensures chain_name and partition type are always in file metadata.
    let auto_metadata = if file_metadata.is_none() {
        let mut meta = crate::writer::ParquetFileMetadata::new();
        if let Some(chain) = rows.first().and_then(|r| r.chain.as_deref()) {
            meta.add("firehose-parquet.chain_name", chain);
        }
        if let Some(row) = rows.first() {
            meta.add("firehose-parquet.partition", &row.partition_type);
            meta.add(
                "firehose-parquet.block_range_size",
                if row.partition_type == "block_range" {
                    row.partition_interval_seconds.to_string()
                } else {
                    "0".to_string()
                },
            );
        }
        Some(meta)
    } else {
        None
    };
    let effective_metadata = file_metadata.or(auto_metadata.as_ref());

    // Lean schema: chain, type, interval live in file-level metadata only
    let mut fields = vec![
        Field::new("partition", DataType::UInt64, false),
        Field::new("start_block", DataType::UInt64, false),
        Field::new("stop_block", DataType::UInt64, false),
        Field::new(
            "start_time",
            DataType::Timestamp(TimeUnit::Second, Some(Arc::from("UTC"))),
            true,
        ),
        Field::new(
            "end_time",
            DataType::Timestamp(TimeUnit::Second, Some(Arc::from("UTC"))),
            true,
        ),
    ];
    if proofs.is_some() {
        fields.extend(crate::partition_index::proof_fields());
    }
    let schema = Arc::new(Schema::new(fields));

    // Build partition column: UInt64 — epoch seconds for time-based, start block for block_range
    let partition_values = rows
        .iter()
        .enumerate()
        .map(|(index, row)| {
            row.partition_key().map_err(|error| {
                anyhow::anyhow!("invalid partition for partition row at index {index}: {error}")
            })
        })
        .collect::<anyhow::Result<Vec<_>>>()?;

    // Build nullable start_time / end_time columns
    let start_time_values: Vec<Option<i64>> = rows
        .iter()
        .map(|row| {
            row.start_time
                .as_deref()
                .map(parse_partition_timestamp)
                .transpose()
        })
        .collect::<anyhow::Result<Vec<_>>>()?;

    let end_time_values: Vec<Option<i64>> = rows
        .iter()
        .map(|row| {
            row.end_time
                .as_deref()
                .map(parse_partition_timestamp)
                .transpose()
        })
        .collect::<anyhow::Result<Vec<_>>>()?;

    let mut columns: Vec<arrow::array::ArrayRef> = vec![
        Arc::new(UInt64Array::from(partition_values)),
        Arc::new(UInt64Array::from(
            rows.iter().map(|row| row.start_block).collect::<Vec<_>>(),
        )),
        Arc::new(UInt64Array::from(
            rows.iter().map(|row| row.stop_block).collect::<Vec<_>>(),
        )),
        Arc::new(TimestampSecondArray::from(start_time_values).with_timezone("UTC")),
        Arc::new(TimestampSecondArray::from(end_time_values).with_timezone("UTC")),
    ];
    if let Some(proofs) = proofs {
        columns.extend(crate::partition_index::proof_columns(proofs));
    }
    let batch = RecordBatch::try_new(schema.clone(), columns)?;

    let pq_compression = compression.parquet();
    let mut props_builder = WriterProperties::builder().set_compression(pq_compression);
    let mut kvs = Vec::new();
    if let Some(meta) = effective_metadata {
        kvs.extend(
            meta.entries
                .iter()
                .map(|(key, value)| KeyValue::new(key.clone(), value.clone())),
        );
    }
    if !kvs.is_empty() {
        props_builder = props_builder.set_key_value_metadata(Some(kvs));
    }
    let props = props_builder.build();

    if path.starts_with("s3://") {
        use crate::writer::parse_s3_url;
        use bytes::Bytes;
        use object_store::ObjectStore;

        let aws = aws
            .ok_or_else(|| anyhow::anyhow!("AWS config required for S3 partitions index output"))?;
        let (bucket, key) = parse_s3_url(path)?;
        // Shared routing validation and zero transport retries are required for
        // this guarded non-CAS checkpoint write, just as for data and cursors.
        let client = aws.build_s3_client_for_mutation(&bucket)?;
        let mut buf = Vec::new();
        {
            let mut writer = ArrowWriter::try_new(&mut buf, schema, Some(props))?;
            writer.write(&batch)?;
            writer.close()?;
        }

        let object_path = object_store::path::Path::from(key.as_str());
        block_on_async(async {
            client
                .put(
                    &object_path,
                    object_store::PutPayload::from(Bytes::from(buf)),
                )
                .await
        })
        .map_err(|e| anyhow::anyhow!("writing {path}: {e}"))?;
    } else {
        let output_path = std::path::Path::new(path);
        if let Some(parent) = output_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let file_name = output_path
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| anyhow::anyhow!("invalid output file name for {path}"))?;
        let temp_path = output_path.with_file_name(format!(".{file_name}.tmp"));
        let file = std::fs::File::create(&temp_path)?;
        let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;
        writer.write(&batch)?;
        writer.close()?;
        std::fs::rename(&temp_path, output_path)?;
    }

    Ok(())
}

pub(in crate::cli) fn is_utf8_like(data_type: &arrow::datatypes::DataType) -> bool {
    matches!(
        data_type,
        arrow::datatypes::DataType::Utf8 | arrow::datatypes::DataType::LargeUtf8
    )
}

pub(in crate::cli) fn is_integer_like(data_type: &arrow::datatypes::DataType) -> bool {
    matches!(
        data_type,
        arrow::datatypes::DataType::UInt64
            | arrow::datatypes::DataType::UInt32
            | arrow::datatypes::DataType::Int64
            | arrow::datatypes::DataType::Int32
    )
}

pub(in crate::cli) fn is_timestamp_second_utc(data_type: &arrow::datatypes::DataType) -> bool {
    matches!(
        data_type,
        arrow::datatypes::DataType::Timestamp(
            arrow::datatypes::TimeUnit::Second,
            Some(timezone)
        )
            if timezone.as_ref() == "UTC"
    )
}

pub(in crate::cli) fn validate_partitions_schema(
    schema: &arrow::datatypes::Schema,
) -> anyhow::Result<()> {
    if let Ok(chain) = schema.field_with_name("chain") {
        if !is_utf8_like(chain.data_type()) {
            anyhow::bail!(
                "invalid partitions.parquet column type for chain: expected Utf8/LargeUtf8, got {}",
                chain.data_type()
            );
        }
        if chain.is_nullable() {
            anyhow::bail!("invalid partitions.parquet schema: chain must be non-nullable");
        }
    }

    let partition_value = schema
        .field_with_name("partition")
        .map_err(|_| anyhow::anyhow!("missing required column: partition"))?;
    if !is_integer_like(partition_value.data_type()) {
        anyhow::bail!(
            "invalid partitions.parquet column type for partition: expected integer, got {}",
            partition_value.data_type()
        );
    }

    let start_block = schema
        .field_with_name("start_block")
        .map_err(|_| anyhow::anyhow!("missing required column: start_block"))?;
    if !is_integer_like(start_block.data_type()) {
        anyhow::bail!(
            "invalid partitions.parquet column type for start_block: expected integer, got {}",
            start_block.data_type()
        );
    }

    let stop_block = schema
        .field_with_name("stop_block")
        .map_err(|_| anyhow::anyhow!("missing required column: stop_block"))?;
    if !is_integer_like(stop_block.data_type()) {
        anyhow::bail!(
            "invalid partitions.parquet column type for stop_block: expected integer, got {}",
            stop_block.data_type()
        );
    }
    for (name, field) in [
        ("start_time", schema.field_with_name("start_time")),
        ("end_time", schema.field_with_name("end_time")),
    ] {
        if let Ok(field) = field {
            if !(is_utf8_like(field.data_type()) || is_timestamp_second_utc(field.data_type())) {
                anyhow::bail!(
                    "invalid partitions.parquet column type for {name}: expected Utf8/LargeUtf8 or Timestamp(Second, UTC), got {}",
                    field.data_type()
                );
            }
        }
    }

    Ok(())
}

pub(in crate::cli) fn validate_partitions_metadata(
    file_meta: &parquet::file::metadata::FileMetaData,
) -> anyhow::Result<()> {
    let kvs = file_meta
        .key_value_metadata()
        .ok_or_else(|| anyhow::anyhow!("missing required file metadata"))?;
    let find = |key: &str| -> Option<&str> {
        kvs.iter()
            .find(|kv| kv.key == key)
            .and_then(|kv| kv.value.as_deref())
    };

    let partition_type = find("firehose-parquet.partition")
        .ok_or_else(|| anyhow::anyhow!("missing required metadata: firehose-parquet.partition"))?;
    let partition_type = canonical_partition_type_label(partition_type)?;
    if partition_type == "block_range" {
        let block_range_size = find("firehose-parquet.block_range_size").ok_or_else(|| {
            anyhow::anyhow!("missing required metadata: firehose-parquet.block_range_size")
        })?;
        let parsed = block_range_size.parse::<u64>().map_err(|error| {
            anyhow::anyhow!(
                "invalid firehose-parquet.block_range_size metadata value `{block_range_size}`: {error}"
            )
        })?;
        if parsed == 0 {
            anyhow::bail!("firehose-parquet.block_range_size must be greater than 0");
        }
    }
    Ok(())
}

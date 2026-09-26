//! Common Parquet schema and encoding mechanics for merge and rollup.
//! Storage acquisition, discovery, journals, publication and deletion remain with callers.
use crate::config::Compression;
use anyhow::{Context, Result};
use arrow::datatypes::{Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::file::metadata::KeyValue;
use parquet::file::properties::WriterProperties;
use parquet::file::reader::ChunkReader;
use std::sync::Arc;

// A separate finite active-row-group budget preserves useful dictionaries even
// when the encoded output target is small. It never scales with the input group.
const ROW_GROUP_MEMORY_BUDGET_BYTES: usize = 32 * 1024 * 1024;

/// Buffers one encoded output part plus a separately bounded active row group.
pub(crate) struct StreamingPartWriter {
    schema: Arc<arrow::datatypes::Schema>,
    props: WriterProperties,
    flush_bytes: u64,
    row_group_memory_bytes: usize,
    flush_rows: Option<usize>,
    next_part_num: u32,
    current_writer: Option<ArrowWriter<Vec<u8>>>,
    current_rows: usize,
}

impl StreamingPartWriter {
    pub(crate) fn new(
        schema: Arc<arrow::datatypes::Schema>,
        props: WriterProperties,
        flush_bytes: u64,
        flush_rows: Option<u32>,
        initial_part_num: u32,
    ) -> Self {
        Self {
            schema,
            props,
            flush_bytes,
            row_group_memory_bytes: ROW_GROUP_MEMORY_BUDGET_BYTES,
            // Treat an explicit zero like the disabled default so merge only flushes on rows when
            // the operator provides a positive threshold.
            flush_rows: flush_rows
                .filter(|rows| *rows > 0)
                .map(|rows| rows as usize),
            next_part_num: initial_part_num,
            current_writer: None,
            current_rows: 0,
        }
    }

    pub(crate) fn write_batch<F>(&mut self, batch: &RecordBatch, flush_part: &mut F) -> Result<()>
    where
        F: FnMut(u32, Vec<u8>, usize) -> Result<()>,
    {
        if batch.num_rows() == 0 {
            return Ok(());
        }

        if self.current_writer.is_none() {
            self.current_writer = Some(ArrowWriter::try_new(
                Vec::new(),
                self.schema.clone(),
                Some(self.props.clone()),
            )?);
        }

        let writer = self.current_writer.as_mut().expect("writer must exist");
        writer.write(batch)?;
        self.current_rows = self
            .current_rows
            .checked_add(batch.num_rows())
            .context("output row count overflow")?;

        let reached_flush_rows = self
            .flush_rows
            .is_some_and(|flush_rows| self.current_rows >= flush_rows);
        // Bound the active Arrow/dictionary buffers independently of encoded
        // output. Closing only the row group preserves the compressed part
        // target rather than turning every memory-bound batch into a tiny file.
        if writer.memory_size() >= self.row_group_memory_bytes {
            writer.flush()?;
        }
        // Already encoded row groups remain in the output Vec and must count.
        let encoded = writer
            .bytes_written()
            .saturating_add(writer.in_progress_size());
        let reached_flush_bytes = self.flush_bytes > 0 && encoded as u64 >= self.flush_bytes;

        if reached_flush_rows || reached_flush_bytes {
            self.flush_current(flush_part)?;
        }

        Ok(())
    }

    pub(crate) fn finish<F>(&mut self, flush_part: &mut F) -> Result<()>
    where
        F: FnMut(u32, Vec<u8>, usize) -> Result<()>,
    {
        if self.current_writer.is_some() {
            self.flush_current(flush_part)?;
        }
        Ok(())
    }

    fn flush_current<F>(&mut self, flush_part: &mut F) -> Result<()>
    where
        F: FnMut(u32, Vec<u8>, usize) -> Result<()>,
    {
        let writer = self
            .current_writer
            .take()
            .expect("writer must exist when flushing");
        let rows = self.current_rows;
        self.current_rows = 0;
        let buf = writer.into_inner()?;
        self.next_part_num = self
            .next_part_num
            .checked_add(1)
            .context("merge part number exhausted")?;
        flush_part(self.next_part_num, buf, rows)?;
        Ok(())
    }
}

/// Describes how `other` differs from `reference`, or returns `None` when both have the same
/// columns (name, type, and nullability) in the same order.
///
/// Arrow writers and `concat_batches` pair columns by position, so merge and rollup only
/// combine files when this returns `None`; otherwise columns would be dropped or swapped.
pub(crate) fn describe_schema_mismatch(reference: &Schema, other: &Schema) -> Option<String> {
    let same_field = |a: &Field, b: &Field| {
        a.name() == b.name() && a.data_type() == b.data_type() && a.is_nullable() == b.is_nullable()
    };
    let (reference_fields, other_fields) = (reference.fields(), other.fields());
    if reference_fields.len() == other_fields.len()
        && reference_fields
            .iter()
            .zip(other_fields.iter())
            .all(|(a, b)| same_field(a, b))
    {
        return None;
    }

    let nullability = |field: &Field| {
        if field.is_nullable() {
            "nullable"
        } else {
            "non-nullable"
        }
    };
    let mut problems = Vec::new();
    for field in reference_fields {
        match other.field_with_name(field.name()) {
            Err(_) => problems.push(format!("missing column `{}`", field.name())),
            Ok(o) if o.data_type() != field.data_type() => problems.push(format!(
                "column `{}` is {} instead of {}",
                field.name(),
                o.data_type(),
                field.data_type()
            )),
            Ok(o) if o.is_nullable() != field.is_nullable() => problems.push(format!(
                "column `{}` is {} instead of {}",
                field.name(),
                nullability(o),
                nullability(field)
            )),
            Ok(_) => {}
        }
    }
    for field in other_fields {
        if reference.field_with_name(field.name()).is_err() {
            problems.push(format!("extra column `{}`", field.name()));
        }
    }
    if problems.is_empty() {
        // Same columns, different positions (or duplicate names).
        let (position, (expected, found)) = reference_fields
            .iter()
            .zip(other_fields.iter())
            .enumerate()
            .find(|(_, (a, b))| !same_field(a, b))
            .expect("schemas differ, so some position differs");
        problems.push(format!(
            "columns are in a different order (column {} is `{}` instead of `{}`)",
            position + 1,
            found.name(),
            expected.name()
        ));
    }
    Some(problems.join("; "))
}

/// Remembers the schema of the first file in a partition and reports how later files differ.
#[derive(Default)]
pub(crate) struct SchemaCheck {
    reference: Option<(String, SchemaRef)>,
}

impl SchemaCheck {
    /// Records the first file's schema. For later files, returns how `schema` differs from it.
    pub(crate) fn check(&mut self, name: &str, schema: &SchemaRef) -> Option<String> {
        match &self.reference {
            None => {
                self.reference = Some((name.to_string(), Arc::clone(schema)));
                None
            }
            Some((reference_name, reference)) => describe_schema_mismatch(reference, schema)
                .map(|diff| format!("{name} does not match {reference_name}: {diff}")),
        }
    }
}

pub(crate) fn writer_properties(
    compression: Compression,
    schema: &Schema,
    kv_metadata: Option<&[KeyValue]>,
) -> WriterProperties {
    let metadata = kv_metadata.map(|kvs| {
        kvs.iter()
            .filter(|kv| !kv.key.starts_with("fireparq.ingest.") && kv.key != "ARROW:schema")
            .cloned()
            .collect()
    });
    crate::writer::properties::for_schema(compression, schema, metadata)
}

/// Compaction/export changes physical parts, so never inherit a source transaction receipt.
pub(crate) fn strip_transaction_schema(original: Arc<Schema>) -> Arc<Schema> {
    if !original
        .metadata()
        .keys()
        .any(|key| key.starts_with("fireparq.ingest."))
    {
        return original;
    }
    let metadata: std::collections::HashMap<String, String> = original
        .metadata()
        .iter()
        .filter(|(key, _)| !key.starts_with("fireparq.ingest."))
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect();
    Arc::new(Schema::new_with_metadata(
        original.fields().clone(),
        metadata,
    ))
}

pub(crate) fn strip_transaction_metadata(batch: RecordBatch) -> Result<RecordBatch> {
    let schema = strip_transaction_schema(batch.schema());
    if Arc::ptr_eq(&schema, &batch.schema()) {
        return Ok(batch);
    }
    Ok(RecordBatch::try_new(schema, batch.columns().to_vec())?)
}

/// Merge captures the first available footer only until a nonempty batch
/// initializes its writer. Rollup instead supplies its first-file preflight
/// schema/metadata up front, including an empty first file.
struct DeferredStart {
    compression: Compression,
    flush_bytes: u64,
    flush_rows: Option<u32>,
    initial_part_num: u32,
    metadata: Option<Vec<KeyValue>>,
}

pub(crate) struct Encoder {
    deferred: Option<DeferredStart>,
    writer: Option<StreamingPartWriter>,
}

impl Encoder {
    pub(crate) fn merge(
        compression: Compression,
        flush_bytes: u64,
        flush_rows: Option<u32>,
        initial_part_num: u32,
    ) -> Self {
        Self {
            deferred: Some(DeferredStart {
                compression,
                flush_bytes,
                flush_rows,
                initial_part_num,
                metadata: None,
            }),
            writer: None,
        }
    }

    pub(crate) fn rollup(
        schema: SchemaRef,
        compression: Compression,
        metadata: Option<&[KeyValue]>,
        flush_bytes: u64,
    ) -> Self {
        let props = writer_properties(compression, schema.as_ref(), metadata);
        Self {
            deferred: None,
            writer: Some(StreamingPartWriter::new(
                schema,
                props,
                flush_bytes,
                None,
                0,
            )),
        }
    }

    /// Process one source reader in its existing batch order. Rollup passes its
    /// checked group-row counter; merge retains its existing writer-only counts.
    pub(crate) fn write_reader<R: ChunkReader + 'static, F>(
        &mut self,
        builder: ParquetRecordBatchReaderBuilder<R>,
        publish: &mut F,
        mut rollup_rows: Option<&mut usize>,
    ) -> Result<()>
    where
        F: FnMut(u32, Vec<u8>, usize) -> Result<()>,
    {
        if let Some(start) = &mut self.deferred {
            if start.metadata.is_none() {
                start.metadata = builder
                    .metadata()
                    .file_metadata()
                    .key_value_metadata()
                    .cloned();
            }
        }
        for batch in builder.build()? {
            let batch = strip_transaction_metadata(batch?)?;
            if let Some(rows) = rollup_rows.as_deref_mut() {
                *rows = rows
                    .checked_add(batch.num_rows())
                    .context("rollup row count overflow")?;
            }
            if batch.num_rows() == 0 {
                continue;
            }
            if self.writer.is_none() {
                let start = self
                    .deferred
                    .take()
                    .expect("merge encoder must have deferred initialization");
                let props = writer_properties(
                    start.compression,
                    batch.schema().as_ref(),
                    start.metadata.as_deref(),
                );
                self.writer = Some(StreamingPartWriter::new(
                    batch.schema(),
                    props,
                    start.flush_bytes,
                    start.flush_rows,
                    start.initial_part_num,
                ));
            }
            self.writer
                .as_mut()
                .expect("encoder writer must exist")
                .write_batch(&batch, publish)?;
        }
        Ok(())
    }

    pub(crate) fn initialized(&self) -> bool {
        self.writer.is_some()
    }

    pub(crate) fn finish<F>(&mut self, publish: &mut F) -> Result<()>
    where
        F: FnMut(u32, Vec<u8>, usize) -> Result<()>,
    {
        if let Some(writer) = &mut self.writer {
            writer.finish(publish)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;

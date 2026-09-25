//! Bounded lookup metadata shared by ingestion and maintenance output.
//!
//! Sorting declarations require a complete per-file proof. Streaming callers
//! use `for_schema`, which deliberately makes no ordering assertion.

use crate::config::Compression;
use anyhow::{Context, Result};
use arrow::{
    array::{Array, UInt64Array},
    datatypes::{DataType, Schema},
    record_batch::RecordBatch,
};
use parquet::{
    arrow::ArrowSchemaConverter,
    file::{
        metadata::{KeyValue, SortingColumn},
        properties::{BloomFilterPosition, WriterProperties, WriterPropertiesBuilder},
    },
    schema::types::ColumnPath,
};

/// One row group cannot exceed the Bloom sizing assumption. Smaller groups are
/// folded by Parquet 60 to avoid storing a large filter for a tiny table.
pub const ROW_GROUP_ROWS: usize = 65_536;
/// At 1% target FPP and 65,536 NDV, each filter reserves 128 KiB while writing.
/// Eight scalar columns bound active filter bitsets to 1 MiB per active writer.
pub const MAX_BLOOM_COLUMNS: usize = 8;

fn lookup_column(name: &str) -> bool {
    matches!(
        name,
        "hash"
            | "tx_hash"
            | "transaction_hash"
            | "signature"
            | "address"
            | "from"
            | "to"
            | "sender"
            | "receiver"
            | "account"
            | "account_id"
            | "account_key"
            | "pubkey"
            | "owner"
            | "mint"
            | "program_id"
            | "caller_address"
            | "transfer_to_address"
            | "contract_address"
            | "receiver_id"
            | "signer_id"
            | "block_hash"
            | "blockhash"
            | "receipt_id"
    )
}

fn scalar_bytes(data_type: &DataType) -> bool {
    match data_type {
        DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Binary
        | DataType::LargeBinary
        | DataType::FixedSizeBinary(_) => true,
        DataType::Dictionary(_, value) => scalar_bytes(value),
        _ => false,
    }
}

fn builder(
    compression: Compression,
    schema: &Schema,
    metadata: Option<Vec<KeyValue>>,
) -> WriterPropertiesBuilder {
    let mut properties = WriterProperties::builder()
        .set_compression(compression.parquet())
        .set_max_row_group_row_count(Some(ROW_GROUP_ROWS))
        .set_bloom_filter_position(BloomFilterPosition::AfterRowGroup)
        .set_key_value_metadata(metadata);
    for field in schema
        .fields()
        .iter()
        .filter(|field| lookup_column(field.name()) && scalar_bytes(field.data_type()))
        .take(MAX_BLOOM_COLUMNS)
    {
        let path = ColumnPath::new(vec![field.name().clone()]);
        properties = properties
            .set_column_bloom_filter_enabled(path.clone(), true)
            .set_column_bloom_filter_fpp(path.clone(), 0.01)
            .set_column_bloom_filter_max_ndv(path, ROW_GROUP_ROWS as u64);
    }
    properties
}

/// Properties for streaming output: future batches may reverse block order.
pub fn for_schema(
    compression: Compression,
    schema: &Schema,
    metadata: Option<Vec<KeyValue>>,
) -> WriterProperties {
    builder(compression, schema, metadata).build()
}

/// Properties for a complete output part. Null or decreasing block heights omit
/// the optional sort assertion; repeated heights are valid nondecreasing order.
pub fn for_batch(
    compression: Compression,
    batch: &RecordBatch,
    metadata: Option<Vec<KeyValue>>,
) -> Result<WriterProperties> {
    let schema = batch.schema();
    let mut properties = builder(compression, schema.as_ref(), metadata);
    if let Some(blocks) = batch
        .column_by_name("block_num")
        .and_then(|column| column.as_any().downcast_ref::<UInt64Array>())
    {
        if !blocks.is_empty()
            && blocks.null_count() == 0
            && blocks.values().windows(2).all(|pair| pair[0] <= pair[1])
        {
            // SortingColumn indexes Parquet leaves, not Arrow root fields.
            // A preceding list or struct can contribute several leaf columns.
            let parquet_schema = ArrowSchemaConverter::new().convert(schema.as_ref())?;
            let index = parquet_schema
                .columns()
                .iter()
                .position(|column| column.path().parts() == ["block_num"])
                .context("canonical block_num is missing from Parquet schema")?;
            properties = properties.set_sorting_columns(Some(vec![SortingColumn {
                column_idx: i32::try_from(index)?,
                descending: false,
                nulls_first: false,
            }]));
        }
    }
    Ok(properties.build())
}

#[cfg(test)]
mod tests;

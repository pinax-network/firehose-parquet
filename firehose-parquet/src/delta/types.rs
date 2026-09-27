//! Delta Lake column types (#643, `docs/design/delta-lake.md` §6).
//!
//! Delta has no unsigned integers, no dictionary types and one timestamp unit,
//! microseconds. The mappers keep building their own Arrow types. Each flushed
//! table is mapped once, at the flush boundary and before the ingestion
//! transaction journals anything, onto the types of its Delta data file:
//!
//! | Mapper Arrow type | Delta type | Conversion |
//! |---|---|---|
//! | `UInt64` | `long` | checked: a value above `i64::MAX` refuses the flush |
//! | `UInt64` in a chain's decimal columns | `decimal(20,0)` | every `u64` fits |
//! | `UInt32`, `UInt16` | `long` | lossless |
//! | `UInt8` | `short` | lossless |
//! | `Dictionary(_, Utf8)` | `string` | Parquet still dictionary-encodes the pages |
//! | `Timestamp(Millisecond, "UTC")` | `timestamp` | the same instant, in microseconds |
//! | `date` | partition column | left out of the data file |
//!
//! Lists and structs map element-wise and field-wise. Every cast is checked
//! (`safe: false`): a value that does not fit is an error, never a wrapped or
//! null value. Columns that are already Delta types (`Utf8`, `Binary`,
//! `Boolean`, `Int32`, `Int64`, `Float64`, ...) are kept as they are.
//!
//! Which `UInt64` columns become `decimal(20,0)` is a per-chain decision
//! (`ChainProfile::decimal_columns` in `blocks/src/chain.rs`): currency amounts,
//! balances and fees, and values a sender or signer chooses without a range
//! check. Every other `UInt64` is bounded by its protocol and becomes a checked
//! `long`.

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use anyhow::{anyhow, bail, ensure, Context, Result};
use arrow::array::{Array, ArrayRef, AsArray, Date32Array, RecordBatch, UInt64Array};
use arrow::compute::{cast_with_options, CastOptions};
use arrow::datatypes::{DataType, Field, Fields, Schema, TimeUnit};
use arrow::util::display::FormatOptions;

use crate::config::BlockMetadata;
use crate::date_partition::{DatePartition, DATE_KEY};

/// The Delta partition column: every table is partitioned by `date`, and its
/// value lives in the Delta log (`partitionValues.date`) and the
/// `date=YYYY-MM-DD` directory, not in the data files.
pub const PARTITION_COLUMN: &str = DATE_KEY;

/// Precision of the `decimal(20,0)` type that holds any `u64`
/// (`u64::MAX` has 20 digits).
pub const U64_DECIMAL_PRECISION: u8 = 20;

/// A `UInt64` column (or a list of them) stored as `decimal(20,0)` instead of
/// a checked `long`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DecimalColumn {
    pub table: &'static str,
    pub column: &'static str,
    /// Why a checked `long` does not fit: the value is a currency amount, or a
    /// sender or signer chooses it without a range check.
    pub reason: &'static str,
}

/// How a mapper column becomes its Delta column.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Conversion {
    /// `UInt64` to `long`, checked: a value above `i64::MAX` refuses the flush.
    CheckedLong,
    /// `UInt64` to `decimal(20,0)`: every value fits.
    Decimal,
    /// `UInt32` or `UInt16` to `long`, `UInt8` to `short`.
    LosslessInteger,
    /// `Dictionary(_, Utf8)` to `string`.
    DictionaryString,
    /// `Timestamp(Millisecond, "UTC")` to `timestamp` (microseconds, UTC).
    TimestampMicros,
    /// The `date` partition column, left out of the data file.
    Partition,
}

impl Conversion {
    /// One-line description of the rule, for documentation.
    pub fn rule(self) -> &'static str {
        match self {
            Conversion::CheckedLong => {
                "checked: a value above 9,223,372,036,854,775,807 (`i64::MAX`) refuses the \
                 flush before anything is written"
            }
            Conversion::Decimal => {
                "exact: every 64-bit unsigned value fits (currency amounts, and values a sender \
                 or signer chooses without a range check)"
            }
            Conversion::LosslessInteger => "lossless",
            Conversion::DictionaryString => {
                "the same labels; Parquet still dictionary-encodes the pages"
            }
            Conversion::TimestampMicros => {
                "the same instant, stored as `TIMESTAMP(MICROS, UTC)` (whole milliseconds)"
            }
            Conversion::Partition => {
                "stored in the Delta log (`partitionValues.date`) and the `date=YYYY-MM-DD` \
                 directory, not in the data files"
            }
        }
    }
}

/// The per-chain Delta type decisions: which `UInt64` columns become
/// `decimal(20,0)`. Everything else follows the fixed rules of this module.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DeltaTypes {
    decimal_columns: &'static [DecimalColumn],
}

impl DeltaTypes {
    pub const fn new(decimal_columns: &'static [DecimalColumn]) -> Self {
        Self { decimal_columns }
    }

    pub fn decimal_columns(&self) -> &'static [DecimalColumn] {
        self.decimal_columns
    }

    /// The listed decimal column `table.column`, if any.
    pub fn decimal_column(&self, table: &str, column: &str) -> Option<&'static DecimalColumn> {
        self.decimal_columns
            .iter()
            .find(|decimal| decimal.table == table && decimal.column == column)
    }

    /// The schema of `table`'s Delta data files: every field mapped onto its
    /// Delta type, and the `date` partition column left out. Every decimal
    /// column listed for `table` must be a `UInt64` column (or a list of them).
    pub fn data_schema(&self, table: &str, schema: &Schema) -> Result<Schema> {
        for decimal in self.decimal_columns.iter().filter(|d| d.table == table) {
            let field = schema.field_with_name(decimal.column).map_err(|_| {
                anyhow!(
                    "decimal(20,0) column `{table}.{}` is not a column of table `{table}`",
                    decimal.column
                )
            })?;
            ensure!(
                holds_only_u64(field.data_type()),
                "decimal(20,0) column `{table}.{}` is {}, not UInt64",
                decimal.column,
                field.data_type()
            );
        }
        let fields = schema
            .fields()
            .iter()
            .filter(|field| field.name() != PARTITION_COLUMN)
            .map(|field| self.data_field(table, field))
            .collect::<Result<Vec<_>>>()?;
        Ok(Schema::new_with_metadata(fields, schema.metadata().clone()))
    }

    fn data_field(&self, table: &str, field: &Field) -> Result<Field> {
        let decimal = self.decimal_column(table, field.name()).is_some();
        let data_type = delta_type(field.data_type(), decimal).map_err(|error| {
            anyhow!(
                "table `{table}` column `{}` ({}) {error}",
                field.name(),
                field.data_type()
            )
        })?;
        Ok(Field::new(field.name(), data_type, field.is_nullable())
            .with_metadata(field.metadata().clone()))
    }

    /// The conversions `field` of `table` goes through (none when its type is
    /// already a Delta type). A struct can combine several.
    pub fn conversions(&self, table: &str, field: &Field) -> BTreeSet<Conversion> {
        let mut conversions = BTreeSet::new();
        if field.name() == PARTITION_COLUMN {
            conversions.insert(Conversion::Partition);
        } else {
            let decimal = self.decimal_column(table, field.name()).is_some();
            collect_conversions(field.data_type(), decimal, &mut conversions);
        }
        conversions
    }

    /// Checked conversion of one flushed table into its Delta data file batch.
    ///
    /// `partition` is the table's `date=` partition in this flush. It is
    /// required when the batch has rows and a `date` column, whose non-null
    /// values must equal it (both come from the same checked block time). The
    /// column is then left out: readers get `date` from the partition value.
    /// A row whose `date` is null (a Solana block without `block_time`) takes
    /// the partition's day, the routing day of the flush.
    pub fn data_batch(
        &self,
        table: &str,
        batch: &RecordBatch,
        partition: Option<DatePartition>,
    ) -> Result<RecordBatch> {
        let schema = Arc::new(self.data_schema(table, batch.schema().as_ref())?);
        if let Some(dates) = batch.column_by_name(PARTITION_COLUMN) {
            check_dates(table, dates.as_ref(), partition)?;
        }
        let options = CastOptions {
            safe: false,
            format_options: FormatOptions::default(),
        };
        let columns = schema
            .fields()
            .iter()
            .map(|target| {
                let column = batch
                    .column_by_name(target.name())
                    .expect("the data schema keeps only batch columns");
                if column.data_type() == target.data_type() {
                    return Ok(Arc::clone(column));
                }
                cast_with_options(column, target.data_type(), &options)
                    .map_err(|error| cast_error(table, target, column.as_ref(), &error.to_string()))
            })
            .collect::<Result<Vec<ArrayRef>>>()?;
        RecordBatch::try_new(schema, columns)
            .with_context(|| format!("building the Delta data file batch of table `{table}`"))
    }

    /// [`Self::data_batch`] for every table of one mapper flush. The partition
    /// is the flush's routing day (`metadata.min_timestamp`), the same one the
    /// writer derives its `<table>/date=YYYY-MM-DD` directory from. Empty
    /// tables need no routing time.
    pub fn data_batches(
        &self,
        batches: HashMap<String, RecordBatch>,
        metadata: &BlockMetadata,
    ) -> Result<HashMap<String, RecordBatch>> {
        let partition = metadata
            .min_timestamp
            .map(DatePartition::from_timestamp)
            .transpose()?;
        let mut mapped = HashMap::with_capacity(batches.len());
        for (table, batch) in batches {
            let partition = if batch.num_rows() == 0 {
                None
            } else {
                Some(partition.with_context(|| {
                    format!(
                        "table `{table}` needs a routing block time: every table is \
                         partitioned by date=YYYY-MM-DD"
                    )
                })?)
            };
            let data = self.data_batch(&table, &batch, partition)?;
            mapped.insert(table, data);
        }
        Ok(mapped)
    }
}

/// The Delta type of one mapper type. `decimal` selects `decimal(20,0)` for
/// `UInt64` values.
fn delta_type(source: &DataType, decimal: bool) -> std::result::Result<DataType, String> {
    let field = |item: &Field| -> std::result::Result<Field, String> {
        Ok(Field::new(
            item.name(),
            delta_type(item.data_type(), decimal)?,
            item.is_nullable(),
        )
        .with_metadata(item.metadata().clone()))
    };
    Ok(match source {
        DataType::UInt64 if decimal => DataType::Decimal128(U64_DECIMAL_PRECISION, 0),
        DataType::UInt64 | DataType::UInt32 | DataType::UInt16 => DataType::Int64,
        DataType::UInt8 => DataType::Int16,
        DataType::Boolean
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::Float32
        | DataType::Float64
        | DataType::Utf8
        | DataType::Binary
        | DataType::Date32 => source.clone(),
        DataType::Decimal128(precision, scale) if *precision <= 38 && *scale >= 0 => source.clone(),
        DataType::Timestamp(TimeUnit::Millisecond | TimeUnit::Microsecond, Some(zone))
            if zone.as_ref() == "UTC" =>
        {
            DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::clone(zone)))
        }
        DataType::Dictionary(_, value) if **value == DataType::Utf8 => DataType::Utf8,
        DataType::List(item) => DataType::List(Arc::new(field(item)?)),
        DataType::Struct(fields) => DataType::Struct(
            fields
                .iter()
                .map(|item| field(item))
                .collect::<std::result::Result<Fields, String>>()?,
        ),
        _ => return Err("has no Delta type".to_string()),
    })
}

fn holds_only_u64(data_type: &DataType) -> bool {
    match data_type {
        DataType::UInt64 => true,
        DataType::List(item) => holds_only_u64(item.data_type()),
        _ => false,
    }
}

fn collect_conversions(data_type: &DataType, decimal: bool, into: &mut BTreeSet<Conversion>) {
    match data_type {
        DataType::UInt64 if decimal => {
            into.insert(Conversion::Decimal);
        }
        DataType::UInt64 => {
            into.insert(Conversion::CheckedLong);
        }
        DataType::UInt32 | DataType::UInt16 | DataType::UInt8 => {
            into.insert(Conversion::LosslessInteger);
        }
        DataType::Dictionary(..) => {
            into.insert(Conversion::DictionaryString);
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            into.insert(Conversion::TimestampMicros);
        }
        DataType::List(item) => collect_conversions(item.data_type(), decimal, into),
        DataType::Struct(fields) => {
            for field in fields {
                collect_conversions(field.data_type(), decimal, into);
            }
        }
        _ => {}
    }
}

/// Every non-null `date` must equal the flush's partition.
fn check_dates(table: &str, dates: &dyn Array, partition: Option<DatePartition>) -> Result<()> {
    let dates = dates
        .as_any()
        .downcast_ref::<Date32Array>()
        .with_context(|| format!("table `{table}` column `date` must be Date32"))?;
    if dates.null_count() == dates.len() {
        return Ok(());
    }
    let partition = partition.with_context(|| {
        format!("table `{table}` has dates but no date=YYYY-MM-DD partition to check them against")
    })?;
    let expected = partition.date32();
    if let Some(date) = dates.iter().flatten().find(|date| *date != expected) {
        bail!(
            "table `{table}` date {date} (days since 1970-01-01) differs from its partition \
             {partition}; the date column and the partition come from the same block time"
        );
    }
    Ok(())
}

/// The error of a refused cast. A `UInt64` above `i64::MAX` in a `long`
/// column is named with its value.
fn cast_error(table: &str, target: &Field, column: &dyn Array, cause: &str) -> anyhow::Error {
    let column_name = target.name();
    let target_type = delta_type_name(target.data_type());
    match first_value_above_i64(column) {
        Some(value) => anyhow!(
            "table `{table}` column `{column_name}`: value {value} does not fit the Delta type \
             `{target_type}` (at most {}); the flush was refused before anything was written. \
             A column whose values can exceed that range must be listed as decimal(20,0) in its \
             chain profile (`ChainProfile::decimal_columns` in blocks/src/chain.rs)",
            i64::MAX
        ),
        None => anyhow!(
            "table `{table}` column `{column_name}` cannot be stored as the Delta type \
             `{target_type}`: {cause}; the flush was refused before anything was written"
        ),
    }
}

/// The first `UInt64` value above `i64::MAX`, looking inside lists and structs.
fn first_value_above_i64(array: &dyn Array) -> Option<u64> {
    const LIMIT: u64 = i64::MAX as u64;
    match array.data_type() {
        DataType::UInt64 => array
            .as_any()
            .downcast_ref::<UInt64Array>()?
            .iter()
            .flatten()
            .find(|value| *value > LIMIT),
        DataType::List(_) => first_value_above_i64(array.as_list::<i32>().values().as_ref()),
        DataType::Struct(_) => array
            .as_struct()
            .columns()
            .iter()
            .find_map(|column| first_value_above_i64(column.as_ref())),
        _ => None,
    }
}

/// The Delta protocol name of a Delta-compatible Arrow type: `long`,
/// `decimal(20,0)`, `array<short>`, `struct<name: string>`, ... Other types
/// keep their Arrow spelling.
pub fn delta_type_name(data_type: &DataType) -> String {
    match data_type {
        DataType::Boolean => "boolean".into(),
        DataType::Int8 => "byte".into(),
        DataType::Int16 => "short".into(),
        DataType::Int32 => "integer".into(),
        DataType::Int64 => "long".into(),
        DataType::Float32 => "float".into(),
        DataType::Float64 => "double".into(),
        DataType::Utf8 => "string".into(),
        DataType::Binary => "binary".into(),
        DataType::Date32 => "date".into(),
        DataType::Decimal128(precision, scale) => format!("decimal({precision},{scale})"),
        DataType::Timestamp(TimeUnit::Microsecond, Some(_)) => "timestamp".into(),
        DataType::List(item) => format!("array<{}>", delta_type_name(item.data_type())),
        DataType::Struct(fields) => format!(
            "struct<{}>",
            fields
                .iter()
                .map(|field| format!("{}: {}", field.name(), delta_type_name(field.data_type())))
                .collect::<Vec<_>>()
                .join(", ")
        ),
        other => other.to_string(),
    }
}

/// Whether `data_type` is a Delta type (protocol reader 1 / writer 2, no
/// table features): no unsigned integer, dictionary, zone-less or
/// non-microsecond timestamp, at any nesting depth.
pub fn is_delta_type(data_type: &DataType) -> bool {
    match data_type {
        DataType::Boolean
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::Float32
        | DataType::Float64
        | DataType::Utf8
        | DataType::Binary
        | DataType::Date32 => true,
        DataType::Decimal128(precision, scale) => *precision <= 38 && *scale >= 0,
        DataType::Timestamp(TimeUnit::Microsecond, Some(zone)) => zone.as_ref() == "UTC",
        DataType::List(item) => is_delta_type(item.data_type()),
        DataType::Struct(fields) => fields.iter().all(|field| is_delta_type(field.data_type())),
        _ => false,
    }
}

#[cfg(test)]
mod tests;

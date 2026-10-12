//! The tables in Delta: their schemas and properties, and rows as Arrow
//! record batches.
//!
//! The Delta fields are built from the model's columns with delta-rs's kernel
//! types: `long`, `string`, `boolean`, `decimal(38,10)`, `timestamp` (UTC, not
//! `timestamp_ntz`), `map<string,string>` and arrays of `string` and `long`,
//! none of which needs a table feature at writer version 2. Batches are
//! delta-rs's Arrow 59 (`deltalake_core::arrow`), never the workspace's 60.
//! [`encode`] takes every nested field's name and nullability from the
//! table's own Arrow schema, so a batch matches the table exactly, whatever
//! names delta-rs's conversion gives map entries and list items.

use std::sync::Arc;

use deltalake_core::arrow::array::{
    new_null_array, Array, ArrayRef, AsArray, BooleanBuilder, Decimal128Builder, Int64Builder,
    ListBuilder, MapBuilder, MapFieldNames, RecordBatch, StringBuilder,
    TimestampMicrosecondBuilder,
};
use deltalake_core::arrow::datatypes::{
    DataType as ArrowType, Decimal128Type, Field, Int64Type, Schema, SchemaRef, TimeUnit,
    TimestampMicrosecondType,
};
use deltalake_core::arrow::error::ArrowError;
use deltalake_core::kernel::engine::arrow_conversion::TryIntoArrow as _;
use deltalake_core::kernel::{ArrayType, DataType, DecimalType, MapType, StructField};
use deltalake_core::{DeltaResult, DeltaTable, TableProperty};

use crate::model::{Cell, ColumnType, Row, TableSpec};

/// The precision and scale of every decimal column.
const DECIMAL_PRECISION: u8 = 38;
const DECIMAL_SCALE: u8 = 10;
const ARROW_DECIMAL: ArrowType = ArrowType::Decimal128(DECIMAL_PRECISION, DECIMAL_SCALE as i8);

/// The properties every table is created with. Each table is append-only (an
/// OPTIMIZE's removes carry `dataChange: false`), checkpointed every 20
/// commits, and keeps its log and removed files for 7 days.
pub const TABLE_PROPERTIES: [(TableProperty, &str); 4] = [
    (TableProperty::AppendOnly, "true"),
    (TableProperty::CheckpointInterval, "20"),
    (TableProperty::LogRetentionDuration, "interval 7 days"),
    (
        TableProperty::DeletedFileRetentionDuration,
        "interval 7 days",
    ),
];

/// The Delta type of a column type.
pub fn delta_type(ty: ColumnType) -> DataType {
    match ty {
        ColumnType::Long => DataType::LONG,
        ColumnType::Text => DataType::STRING,
        ColumnType::Bool => DataType::BOOLEAN,
        ColumnType::Decimal => DecimalType::try_new(DECIMAL_PRECISION, DECIMAL_SCALE)
            .expect("decimal(38,10) is a Delta type")
            .into(),
        ColumnType::Timestamp => DataType::TIMESTAMP,
        ColumnType::Kv => MapType::new(DataType::STRING, DataType::STRING, true).into(),
        ColumnType::TextList => ArrayType::new(DataType::STRING, true).into(),
        ColumnType::LongList => ArrayType::new(DataType::LONG, true).into(),
    }
}

/// The table's Delta fields, in its column order.
pub fn delta_fields(spec: &TableSpec) -> Vec<StructField> {
    spec.columns
        .iter()
        .map(|column| StructField::new(column.name, delta_type(column.ty), column.nullable))
        .collect()
}

/// The Arrow schema of the table's data files, as delta-rs converts the
/// table's current Delta schema: the schema a batch is encoded to and an
/// OPTIMIZE writes (every table is unpartitioned).
pub fn file_schema(table: &DeltaTable) -> DeltaResult<SchemaRef> {
    let schema: Schema = table.snapshot()?.schema().as_ref().try_into_arrow()?;
    Ok(Arc::new(schema))
}

fn mismatch(field: &Field, cell: &Cell) -> ArrowError {
    let kind = match cell {
        Cell::Null => "NULL",
        Cell::Long(_) => "long",
        Cell::Text(_) => "text",
        Cell::Bool(_) => "boolean",
        Cell::Decimal(_) => "decimal",
        Cell::Timestamp(_) => "timestamp",
        Cell::Kv(_) => "map",
        Cell::TextList(_) => "text list",
        Cell::LongList(_) => "long list",
    };
    ArrowError::InvalidArgumentError(format!("column {} holds a {kind} value", field.name()))
}

/// The rows as a batch of `schema`, the table's Arrow schema (its fields by
/// name; a field the spec lacks is NULL, refused if it is NOT NULL).
pub fn encode(
    schema: &SchemaRef,
    spec: &TableSpec,
    rows: &[Row],
) -> Result<RecordBatch, ArrowError> {
    if let Some(row) = rows.iter().find(|row| row.spec.name != spec.name) {
        return Err(ArrowError::InvalidArgumentError(format!(
            "a row of {} encoded as {}",
            row.spec.name, spec.name
        )));
    }
    let columns = schema
        .fields()
        .iter()
        .map(|field| match spec.position(field.name()) {
            Some(index) => encode_column(
                field,
                spec.columns[index].ty,
                rows.iter().map(|row| &row.cells[index]),
            ),
            None => Ok(new_null_array(field.data_type(), rows.len())),
        })
        .collect::<Result<Vec<_>, _>>()?;
    RecordBatch::try_new(schema.clone(), columns)
}

fn encode_column<'a>(
    field: &Field,
    ty: ColumnType,
    cells: impl Iterator<Item = &'a Cell>,
) -> Result<ArrayRef, ArrowError> {
    fn string(field: &Field) -> bool {
        field.data_type() == &ArrowType::Utf8
    }
    Ok(match (ty, field.data_type()) {
        (ColumnType::Long, ArrowType::Int64) => {
            let mut builder = Int64Builder::new();
            for cell in cells {
                match cell {
                    Cell::Null => builder.append_null(),
                    Cell::Long(value) => builder.append_value(*value),
                    other => return Err(mismatch(field, other)),
                }
            }
            Arc::new(builder.finish())
        }
        (ColumnType::Text, ArrowType::Utf8) => {
            let mut builder = StringBuilder::new();
            for cell in cells {
                match cell {
                    Cell::Null => builder.append_null(),
                    Cell::Text(value) => builder.append_value(value),
                    other => return Err(mismatch(field, other)),
                }
            }
            Arc::new(builder.finish())
        }
        (ColumnType::Bool, ArrowType::Boolean) => {
            let mut builder = BooleanBuilder::new();
            for cell in cells {
                match cell {
                    Cell::Null => builder.append_null(),
                    Cell::Bool(value) => builder.append_value(*value),
                    other => return Err(mismatch(field, other)),
                }
            }
            Arc::new(builder.finish())
        }
        (ColumnType::Decimal, decimal) if *decimal == ARROW_DECIMAL => {
            let mut builder = Decimal128Builder::new().with_data_type(field.data_type().clone());
            for cell in cells {
                match cell {
                    Cell::Null => builder.append_null(),
                    Cell::Decimal(value) => builder.append_value(*value),
                    other => return Err(mismatch(field, other)),
                }
            }
            Arc::new(builder.finish())
        }
        (ColumnType::Timestamp, ArrowType::Timestamp(TimeUnit::Microsecond, Some(_))) => {
            let mut builder =
                TimestampMicrosecondBuilder::new().with_data_type(field.data_type().clone());
            for cell in cells {
                match cell {
                    Cell::Null => builder.append_null(),
                    Cell::Timestamp(value) => builder.append_value(*value),
                    other => return Err(mismatch(field, other)),
                }
            }
            Arc::new(builder.finish())
        }
        (ColumnType::Kv, ArrowType::Map(entries, false)) => {
            let ArrowType::Struct(parts) = entries.data_type() else {
                return Err(schema_mismatch(field, ty));
            };
            if parts.len() != 2
                || !string(&parts[0])
                || parts[0].is_nullable()
                || !string(&parts[1])
            {
                return Err(schema_mismatch(field, ty));
            }
            let (key, value) = (&parts[0], &parts[1]);
            let names = MapFieldNames {
                entry: entries.name().clone(),
                key: key.name().clone(),
                value: value.name().clone(),
            };
            let mut builder =
                MapBuilder::new(Some(names), StringBuilder::new(), StringBuilder::new())
                    .with_keys_field(Arc::clone(key))
                    .with_values_field(Arc::clone(value));
            for cell in cells {
                match cell {
                    Cell::Null => builder.append(false)?,
                    Cell::Kv(pairs) => {
                        for (key, value) in pairs {
                            builder.keys().append_value(key);
                            builder.values().append_value(value);
                        }
                        builder.append(true)?;
                    }
                    other => return Err(mismatch(field, other)),
                }
            }
            Arc::new(builder.finish())
        }
        (ColumnType::TextList, ArrowType::List(element))
            if string(element) && element.is_nullable() =>
        {
            let mut builder =
                ListBuilder::new(StringBuilder::new()).with_field(Arc::clone(element));
            for cell in cells {
                match cell {
                    Cell::Null => builder.append_null(),
                    Cell::TextList(values) => {
                        for value in values {
                            builder.values().append_option(value.as_deref());
                        }
                        builder.append(true);
                    }
                    other => return Err(mismatch(field, other)),
                }
            }
            Arc::new(builder.finish())
        }
        (ColumnType::LongList, ArrowType::List(element))
            if element.data_type() == &ArrowType::Int64 && element.is_nullable() =>
        {
            let mut builder = ListBuilder::new(Int64Builder::new()).with_field(Arc::clone(element));
            for cell in cells {
                match cell {
                    Cell::Null => builder.append_null(),
                    Cell::LongList(values) => {
                        for value in values {
                            builder.values().append_option(*value);
                        }
                        builder.append(true);
                    }
                    other => return Err(mismatch(field, other)),
                }
            }
            Arc::new(builder.finish())
        }
        _ => return Err(schema_mismatch(field, ty)),
    })
}

fn schema_mismatch(field: &Field, ty: ColumnType) -> ArrowError {
    ArrowError::SchemaError(format!(
        "column {} is {}, not the {ty:?} column of this release",
        field.name(),
        field.data_type()
    ))
}

/// The cells of `columns` in each row of `batch`, read from a data file of
/// the table: a column the file lacks (written before the column was added)
/// is NULL. A column of another type than the spec's is an error.
pub fn decode(
    batch: &RecordBatch,
    spec: &TableSpec,
    columns: &[&str],
) -> Result<Vec<Vec<Cell>>, ArrowError> {
    let schema = batch.schema_ref();
    let mut rows = vec![Vec::with_capacity(columns.len()); batch.num_rows()];
    for name in columns {
        let ty = spec.columns[spec.index(name)].ty;
        match schema.index_of(name) {
            Err(_) => rows.iter_mut().for_each(|row| row.push(Cell::Null)),
            Ok(index) => {
                let cells = decode_column(schema.field(index), ty, batch.column(index).as_ref())?;
                for (row, cell) in rows.iter_mut().zip(cells) {
                    row.push(cell);
                }
            }
        }
    }
    Ok(rows)
}

fn decode_column(
    field: &Field,
    ty: ColumnType,
    array: &dyn Array,
) -> Result<Vec<Cell>, ArrowError> {
    let unexpected = || schema_mismatch(field, ty);
    let cells = |cell: &dyn Fn(usize) -> Result<Cell, ArrowError>| {
        (0..array.len())
            .map(|index| {
                if array.is_null(index) {
                    Ok(Cell::Null)
                } else {
                    cell(index)
                }
            })
            .collect::<Result<Vec<_>, _>>()
    };
    let strings = |array: &dyn Array| -> Result<Vec<Option<String>>, ArrowError> {
        let strings = array.as_string_opt::<i32>().ok_or_else(unexpected)?;
        Ok(strings
            .iter()
            .map(|value| value.map(str::to_owned))
            .collect())
    };
    match ty {
        ColumnType::Long => {
            let values = array
                .as_primitive_opt::<Int64Type>()
                .ok_or_else(unexpected)?;
            cells(&|index| Ok(Cell::Long(values.value(index))))
        }
        ColumnType::Text => {
            let values = array.as_string_opt::<i32>().ok_or_else(unexpected)?;
            cells(&|index| Ok(Cell::Text(values.value(index).to_owned())))
        }
        ColumnType::Bool => {
            let values = array.as_boolean_opt().ok_or_else(unexpected)?;
            cells(&|index| Ok(Cell::Bool(values.value(index))))
        }
        ColumnType::Decimal => {
            if *array.data_type() != ARROW_DECIMAL {
                return Err(unexpected());
            }
            let values = array
                .as_primitive_opt::<Decimal128Type>()
                .ok_or_else(unexpected)?;
            cells(&|index| Ok(Cell::Decimal(values.value(index))))
        }
        ColumnType::Timestamp => {
            let values = array
                .as_primitive_opt::<TimestampMicrosecondType>()
                .ok_or_else(unexpected)?;
            cells(&|index| Ok(Cell::Timestamp(values.value(index))))
        }
        ColumnType::Kv => {
            let maps = array.as_map_opt().ok_or_else(unexpected)?;
            cells(&|index| {
                let entries = maps.value(index);
                let keys = strings(entries.column(0).as_ref())?;
                let values = strings(entries.column(1).as_ref())?;
                keys.into_iter()
                    .zip(values)
                    .map(|pair| match pair {
                        (Some(key), Some(value)) => Ok((key, value)),
                        // The job never writes one.
                        _ => Err(ArrowError::InvalidArgumentError(format!(
                            "column {} holds a NULL map key or value",
                            field.name()
                        ))),
                    })
                    .collect::<Result<_, _>>()
                    .map(Cell::Kv)
            })
        }
        ColumnType::TextList => {
            let lists = array.as_list_opt::<i32>().ok_or_else(unexpected)?;
            cells(&|index| strings(lists.value(index).as_ref()).map(Cell::TextList))
        }
        ColumnType::LongList => {
            let lists = array.as_list_opt::<i32>().ok_or_else(unexpected)?;
            cells(&|index| {
                let list = lists.value(index);
                let values = list
                    .as_primitive_opt::<Int64Type>()
                    .ok_or_else(unexpected)?;
                Ok(Cell::LongList(values.iter().collect()))
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::json::Json;
    use crate::model::{
        outcome_meta_row, outcome_settled_row, question_live_row, removal_row, spot_meta_row,
        spot_token_row, token_details_row, ALL_TABLES, OUTCOMES, OUTCOME_META, QUESTIONS,
        SPOT_META, SPOT_TOKENS, SPOT_TOKEN_DETAILS,
    };
    use deltalake_core::kernel::StructType;

    const AT: i64 = 1_791_478_200_000_000;

    fn fixture(name: &str) -> Json {
        let path = format!("{}/tests/fixtures/{name}", env!("CARGO_MANIFEST_DIR"));
        Json::parse(&std::fs::read(path).unwrap()).unwrap()
    }

    /// The table's Arrow schema as delta-rs converts its Delta schema.
    fn schema(spec: &TableSpec) -> SchemaRef {
        let delta = StructType::try_new(delta_fields(spec)).unwrap();
        Arc::new((&delta).try_into_arrow().unwrap())
    }

    /// Encodes the rows, decodes every column back, and requires the same
    /// cells.
    fn round_trip(spec: &'static TableSpec, rows: &[Row]) -> RecordBatch {
        let batch = encode(&schema(spec), spec, rows).unwrap();
        assert_eq!(batch.num_rows(), rows.len());
        let names: Vec<&str> = spec.columns.iter().map(|column| column.name).collect();
        let decoded = decode(&batch, spec, &names).unwrap();
        let cells: Vec<Vec<Cell>> = rows.iter().map(|row| row.cells.clone()).collect();
        assert_eq!(decoded, cells, "{}", spec.name);
        batch
    }

    #[test]
    fn rows_fit_the_schemas() {
        let spot = fixture("spotMeta.json");
        let tokens: Vec<Row> = spot
            .get("tokens")
            .unwrap()
            .as_arr()
            .unwrap()
            .iter()
            .map(|token| spot_token_row(token, AT))
            .collect();
        round_trip(&SPOT_TOKENS, &tokens);
        let details: Vec<Row> = ["HSEI", "USDC", "ASI"]
            .iter()
            .map(|name| {
                token_details_row(0, "0x1", &fixture(&format!("tokenDetails_{name}.json")), AT)
            })
            .collect();
        round_trip(&SPOT_TOKEN_DETAILS, &details);
        let meta = fixture("outcomeMeta_t0.json");
        round_trip(&OUTCOME_META, &[outcome_meta_row(&meta, AT)]);
        round_trip(&SPOT_META, &[spot_meta_row(&spot, AT)]);
        let mut outcomes: Vec<Row> = [0, 7768, 9174]
            .iter()
            .map(|id| outcome_settled_row(&fixture(&format!("settledOutcome_{id}.json")), AT))
            .collect();
        outcomes.push(removal_row(&outcomes[1], AT + 1, "outcomeMeta"));
        let batch = round_trip(&OUTCOMES, &outcomes);
        // NULL lists and maps, and a removal row's NULL payload, stay NULL.
        assert_eq!(
            batch.column_by_name("payload_json").unwrap().null_count(),
            1
        );
        let question = &meta.get("questions").unwrap().as_arr().unwrap()[0];
        round_trip(&QUESTIONS, &[question_live_row(question, AT)]);
    }

    #[test]
    fn nested_fields_take_the_tables_names() {
        let schema = schema(&OUTCOMES);
        let ArrowType::Map(entries, false) = schema
            .field_with_name("description_kv")
            .unwrap()
            .data_type()
        else {
            panic!("not a map")
        };
        assert_eq!(entries.name(), "key_value");
        let ArrowType::List(element) = schema.field_with_name("side_names").unwrap().data_type()
        else {
            panic!("not a list")
        };
        assert_eq!(element.name(), "element");
        assert_eq!(
            schema.field_with_name("observed_at").unwrap().data_type(),
            &ArrowType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        );
        assert_eq!(
            schema
                .field_with_name("settle_fraction")
                .unwrap()
                .data_type(),
            &ArrowType::Decimal128(38, 10)
        );
    }

    #[test]
    fn files_written_before_a_column_read_it_as_null() {
        let spot = fixture("spotMeta.json");
        let row = spot_meta_row(&spot, AT);
        let batch = encode(&schema(&SPOT_META), &SPOT_META, std::slice::from_ref(&row)).unwrap();
        let older = batch.project(&[0, 1, 2]).unwrap();
        let cells = decode(
            &older,
            &SPOT_META,
            &["request", "observed_at", "payload_json"],
        )
        .unwrap();
        assert_eq!(
            cells,
            vec![vec![
                row.get("request").clone(),
                row.get("observed_at").clone(),
                Cell::Null
            ]]
        );
    }

    #[test]
    fn mismatched_rows_and_schemas_are_refused() {
        let spot = fixture("spotMeta.json");
        let mut row = spot_meta_row(&spot, AT);
        row.set("observed_at", Cell::Text("now".into()));
        assert!(encode(&schema(&SPOT_META), &SPOT_META, &[row]).is_err());
        // A NULL in a NOT NULL column.
        let mut row = spot_meta_row(&spot, AT);
        row.set("source", Cell::Null);
        assert!(encode(&schema(&SPOT_META), &SPOT_META, &[row]).is_err());
        // A row of another table.
        let row = spot_meta_row(&spot, AT);
        assert!(encode(&schema(&OUTCOME_META), &OUTCOME_META, &[row]).is_err());
        // A file whose column has another type.
        let batch = encode(&schema(&SPOT_META), &SPOT_META, &[spot_meta_row(&spot, AT)]).unwrap();
        let renamed = RecordBatch::try_new(
            Arc::new(deltalake_core::arrow::datatypes::Schema::new(vec![
                Field::new("listed", ArrowType::Utf8, false),
            ])),
            vec![batch.column(0).clone()],
        )
        .unwrap();
        assert!(decode(&renamed, &SPOT_META, &["listed"]).is_err());
    }

    #[test]
    fn every_table_converts_to_arrow() {
        for spec in ALL_TABLES {
            assert_eq!(
                schema(spec).fields().len(),
                spec.columns.len(),
                "{}",
                spec.name
            );
            round_trip(spec, &[]);
        }
    }
}

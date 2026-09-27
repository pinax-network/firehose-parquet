use super::*;
use arrow::array::{
    BooleanArray, Decimal128Array, DictionaryArray, Int16Array, Int64Array, ListArray, ListBuilder,
    StringArray, StructArray, TimestampMicrosecondArray, TimestampMillisecondArray, UInt16Array,
    UInt32Array, UInt64Builder, UInt8Array,
};
use arrow::datatypes::Int32Type;

/// 2024-01-15T12:00:00Z.
const T: i64 = 1_705_320_000;
const DECIMALS: &[DecimalColumn] = &[
    DecimalColumn {
        table: "blocks",
        column: "nonce",
        reason: "any 64-bit value",
    },
    DecimalColumn {
        table: "blocks",
        column: "balances",
        reason: "currency amounts",
    },
];
const TYPES: DeltaTypes = DeltaTypes::new(DECIMALS);

fn partition() -> DatePartition {
    DatePartition::from_timestamp(T).unwrap()
}

fn metadata(timestamp: Option<i64>) -> BlockMetadata {
    BlockMetadata {
        min_block_number: 1,
        max_block_number: 2,
        min_timestamp: timestamp,
        max_timestamp: timestamp,
    }
}

fn u64_list(values: Vec<Option<Vec<u64>>>) -> ListArray {
    let mut builder = ListBuilder::new(UInt64Builder::new());
    for value in values {
        match value {
            Some(items) => {
                for item in items {
                    builder.values().append_value(item);
                }
                builder.append(true);
            }
            None => builder.append(false),
        }
    }
    builder.finish()
}

/// A table with every kind of mapper column: two rows, the second with nulls.
fn source(gas_used: u64, sequence: u64) -> RecordBatch {
    let sequence_field = Arc::new(Field::new("sequence", DataType::UInt64, false));
    let denom_field = Arc::new(Field::new("denom", DataType::Utf8, true));
    let signer = StructArray::from(vec![
        (
            Arc::clone(&sequence_field),
            Arc::new(UInt64Array::from(vec![sequence, 7])) as ArrayRef,
        ),
        (
            Arc::clone(&denom_field),
            Arc::new(StringArray::from(vec![Some("uatom"), None])) as ArrayRef,
        ),
    ]);
    let status: DictionaryArray<Int32Type> = vec![Some("SUCCEEDED"), None].into_iter().collect();
    let accounts = {
        let mut builder = ListBuilder::new(arrow::array::UInt8Builder::new())
            .with_field(Field::new("item", DataType::UInt8, false));
        builder.values().append_value(0);
        builder.values().append_value(255);
        builder.append(true);
        builder.append(true);
        builder.finish()
    };
    let columns: Vec<(&str, ArrayRef, bool)> = vec![
        (
            "block_num",
            Arc::new(UInt64Array::from(vec![100, 101])),
            false,
        ),
        (
            "timestamp",
            Arc::new(
                TimestampMillisecondArray::from(vec![Some(T * 1_000 + 250), None])
                    .with_timezone("UTC"),
            ),
            true,
        ),
        (
            "date",
            Arc::new(Date32Array::from(vec![Some(partition().date32()), None])),
            true,
        ),
        (
            "gas_used",
            Arc::new(UInt64Array::from(vec![Some(gas_used), None])),
            true,
        ),
        (
            "nonce",
            Arc::new(UInt64Array::from(vec![u64::MAX, 0])),
            false,
        ),
        (
            "balances",
            Arc::new(u64_list(vec![Some(vec![u64::MAX, 1]), None])),
            true,
        ),
        (
            "indices",
            Arc::new(u64_list(vec![Some(vec![3, 4]), Some(vec![])])),
            true,
        ),
        (
            "tx_index",
            Arc::new(UInt32Array::from(vec![u32::MAX, 0])),
            false,
        ),
        (
            "port",
            Arc::new(UInt16Array::from(vec![u16::MAX, 0])),
            false,
        ),
        ("flags", Arc::new(UInt8Array::from(vec![255, 0])), false),
        ("accounts", Arc::new(accounts), false),
        ("status", Arc::new(status), true),
        ("signer", Arc::new(signer), false),
        (
            "hash",
            Arc::new(StringArray::from(vec!["0xaa", "0xbb"])),
            false,
        ),
        (
            "success",
            Arc::new(BooleanArray::from(vec![true, false])),
            false,
        ),
        ("expiry", Arc::new(Int64Array::from(vec![-1, 1])), false),
    ];
    let schema = Schema::new(
        columns
            .iter()
            .map(|(name, array, nullable)| Field::new(*name, array.data_type().clone(), *nullable))
            .collect::<Vec<_>>(),
    );
    RecordBatch::try_new(
        Arc::new(schema),
        columns.into_iter().map(|(_, array, _)| array).collect(),
    )
    .unwrap()
}

#[test]
fn every_mapper_type_maps_onto_its_delta_type() {
    let batch = source(21_000, 5);
    let schema = TYPES
        .data_schema("blocks", batch.schema().as_ref())
        .unwrap();
    let types: Vec<(String, String, bool)> = schema
        .fields()
        .iter()
        .map(|field| {
            (
                field.name().clone(),
                delta_type_name(field.data_type()),
                field.is_nullable(),
            )
        })
        .collect();
    let expected = [
        ("block_num", "long", false),
        ("timestamp", "timestamp", true),
        ("gas_used", "long", true),
        ("nonce", "decimal(20,0)", false),
        ("balances", "array<decimal(20,0)>", true),
        ("indices", "array<long>", true),
        ("tx_index", "long", false),
        ("port", "long", false),
        ("flags", "short", false),
        ("accounts", "array<short>", false),
        ("status", "string", true),
        ("signer", "struct<sequence: long, denom: string>", false),
        ("hash", "string", false),
        ("success", "boolean", false),
        ("expiry", "long", false),
    ]
    .map(|(name, delta, nullable)| (name.to_string(), delta.to_string(), nullable));
    assert_eq!(types, expected);
    // The `date` partition column is left out; nullability of list items and
    // struct fields is kept.
    assert!(schema.field_with_name(PARTITION_COLUMN).is_err());
    let DataType::List(item) = schema.field_with_name("accounts").unwrap().data_type() else {
        panic!("accounts is a list");
    };
    assert!(!item.is_nullable());
    let DataType::Struct(fields) = schema.field_with_name("signer").unwrap().data_type() else {
        panic!("signer is a struct");
    };
    assert!(!fields[0].is_nullable() && fields[1].is_nullable());
    assert!(schema
        .fields()
        .iter()
        .all(|field| is_delta_type(field.data_type())));
    assert!(!is_delta_type(batch.schema().field(0).data_type()));

    // The batch conversion builds exactly that schema.
    let data = TYPES
        .data_batch("blocks", &batch, Some(partition()))
        .unwrap();
    assert_eq!(data.schema().as_ref(), &schema);
    assert_eq!(data.num_rows(), 2);
}

#[test]
fn conversions_keep_every_value_and_null() {
    let data = TYPES
        .data_batch("blocks", &source(21_000, 5), Some(partition()))
        .unwrap();
    let column = |name: &str| data.column_by_name(name).unwrap();
    let block_num = column("block_num").as_primitive::<arrow::datatypes::Int64Type>();
    assert_eq!(block_num.values(), &[100, 101]);
    let timestamp = column("timestamp")
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .unwrap();
    assert_eq!(timestamp.value(0), (T * 1_000 + 250) * 1_000);
    assert!(timestamp.is_null(1));
    assert_eq!(timestamp.timezone(), Some("UTC"));
    let gas = column("gas_used").as_primitive::<arrow::datatypes::Int64Type>();
    assert_eq!((gas.value(0), gas.is_null(1)), (21_000, true));
    let nonce = column("nonce")
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    assert_eq!(nonce.value(0), i128::from(u64::MAX));
    assert_eq!(nonce.value_as_string(0), "18446744073709551615");
    let balances = column("balances").as_list::<i32>();
    assert!(balances.is_null(1));
    let first = balances.value(0);
    let first = first.as_any().downcast_ref::<Decimal128Array>().unwrap();
    assert_eq!(first.values(), &[i128::from(u64::MAX), 1]);
    let tx_index = column("tx_index").as_primitive::<arrow::datatypes::Int64Type>();
    assert_eq!(tx_index.value(0), i64::from(u32::MAX));
    let port = column("port").as_primitive::<arrow::datatypes::Int64Type>();
    assert_eq!(port.value(0), i64::from(u16::MAX));
    let flags = column("flags")
        .as_any()
        .downcast_ref::<Int16Array>()
        .unwrap();
    assert_eq!(flags.values(), &[255, 0]);
    let accounts = column("accounts").as_list::<i32>().value(0);
    let accounts = accounts.as_any().downcast_ref::<Int16Array>().unwrap();
    assert_eq!(accounts.values(), &[0, 255]);
    let status = column("status").as_string::<i32>();
    assert_eq!((status.value(0), status.is_null(1)), ("SUCCEEDED", true));
    let signer = column("signer").as_struct();
    let sequence = signer
        .column(0)
        .as_primitive::<arrow::datatypes::Int64Type>();
    assert_eq!(sequence.values(), &[5, 7]);
    assert_eq!(column("hash").as_string::<i32>().value(1), "0xbb");
}

#[test]
fn columns_that_already_have_a_delta_type_are_shared_not_copied() {
    let batch = source(21_000, 5);
    let data = TYPES
        .data_batch("blocks", &batch, Some(partition()))
        .unwrap();
    for name in ["hash", "success", "expiry"] {
        assert!(
            Arc::ptr_eq(
                data.column_by_name(name).unwrap(),
                batch.column_by_name(name).unwrap()
            ),
            "{name}"
        );
    }
}

#[test]
fn a_value_above_i64_max_in_a_long_column_refuses_the_flush() {
    let too_big = i64::MAX as u64 + 1;
    for (batch, column) in [
        (source(too_big, 5), "gas_used"),
        (source(21_000, u64::MAX), "signer"),
    ] {
        let error = TYPES
            .data_batch("blocks", &batch, Some(partition()))
            .unwrap_err()
            .to_string();
        assert!(
            error.contains(&format!("table `blocks` column `{column}`"))
                && error.contains("does not fit the Delta type")
                && error.contains("refused before anything was written")
                && error.contains("decimal(20,0)"),
            "{error}"
        );
        assert!(
            error.contains(&too_big.to_string()) || error.contains(&u64::MAX.to_string()),
            "{error}"
        );
    }
    // Inside a list of longs too.
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "indices",
            u64_list(vec![]).data_type().clone(),
            true,
        )])),
        vec![Arc::new(u64_list(vec![Some(vec![1, u64::MAX])]))],
    )
    .unwrap();
    let error = TYPES
        .data_batch("logs", &batch, None)
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("column `indices`: value 18446744073709551615"),
        "{error}"
    );
    // i64::MAX itself fits.
    let data = TYPES
        .data_batch("blocks", &source(i64::MAX as u64, 5), Some(partition()))
        .unwrap();
    let gas = data
        .column_by_name("gas_used")
        .unwrap()
        .as_primitive::<arrow::datatypes::Int64Type>();
    assert_eq!(gas.value(0), i64::MAX);
}

#[test]
fn the_date_column_leaves_the_file_and_must_equal_its_partition() {
    let batch = source(21_000, 5);
    // A null date (a Solana block without a time) takes the partition's day.
    assert!(TYPES
        .data_batch("blocks", &batch, Some(partition()))
        .is_ok());
    for other in [
        DatePartition::from_timestamp(T - 86_400).unwrap(),
        DatePartition::from_timestamp(T + 86_400).unwrap(),
    ] {
        let error = TYPES
            .data_batch("blocks", &batch, Some(other))
            .unwrap_err()
            .to_string();
        assert!(error.contains("differs from its partition"), "{error}");
    }
    let error = TYPES
        .data_batch("blocks", &batch, None)
        .unwrap_err()
        .to_string();
    assert!(error.contains("no date=YYYY-MM-DD partition"), "{error}");
    let wrong = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "date",
            DataType::Int32,
            false,
        )])),
        vec![Arc::new(arrow::array::Int32Array::from(vec![1]))],
    )
    .unwrap();
    let error = TYPES
        .data_batch("logs", &wrong, Some(partition()))
        .unwrap_err()
        .to_string();
    assert!(error.contains("must be Date32"), "{error}");
}

#[test]
fn flush_batches_are_mapped_with_the_routing_day() {
    let batch = source(21_000, 5);
    let empty = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("block_num", DataType::UInt64, false),
            Field::new("date", DataType::Date32, false),
        ])),
        vec![
            Arc::new(UInt64Array::from(Vec::<u64>::new())),
            Arc::new(Date32Array::from(Vec::<i32>::new())),
        ],
    )
    .unwrap();
    let batches = HashMap::from([
        ("blocks".to_string(), batch.clone()),
        ("logs".to_string(), empty.clone()),
    ]);
    let mapped = TYPES.data_batches(batches, &metadata(Some(T))).unwrap();
    assert_eq!(mapped["blocks"].num_rows(), 2);
    assert_eq!(mapped["logs"].num_rows(), 0);
    assert!(mapped["logs"].schema().field_with_name("date").is_err());
    // The routing day is the flush's minimum time.
    let error = TYPES
        .data_batches(
            HashMap::from([("blocks".to_string(), batch.clone())]),
            &metadata(Some(T + 86_400)),
        )
        .unwrap_err()
        .to_string();
    assert!(error.contains("differs from its partition"), "{error}");
    // Empty tables need no routing time; tables with rows do.
    assert!(TYPES
        .data_batches(
            HashMap::from([("logs".to_string(), empty)]),
            &metadata(None)
        )
        .is_ok());
    let error = TYPES
        .data_batches(
            HashMap::from([("blocks".to_string(), batch)]),
            &metadata(None),
        )
        .unwrap_err()
        .to_string();
    assert!(error.contains("needs a routing block time"), "{error}");
}

#[test]
fn listed_decimal_columns_must_be_u64_columns_of_their_table() {
    let schema = source(21_000, 5).schema();
    for (column, expected) in [
        ("missing", "is not a column of table `blocks`"),
        ("tx_index", "is UInt32, not UInt64"),
        ("signer", "not UInt64"),
    ] {
        let decimals: &'static [DecimalColumn] = Box::leak(Box::new([DecimalColumn {
            table: "blocks",
            column,
            reason: "test",
        }]));
        let error = DeltaTypes::new(decimals)
            .data_schema("blocks", schema.as_ref())
            .unwrap_err()
            .to_string();
        assert!(error.contains(expected), "{column}: {error}");
    }
    // A decimal column of another table does not apply.
    assert!(TYPES.data_schema("logs", schema.as_ref()).is_ok());
    let schema = TYPES.data_schema("logs", schema.as_ref()).unwrap();
    assert_eq!(
        schema.field_with_name("nonce").unwrap().data_type(),
        &DataType::Int64
    );
}

#[test]
fn types_without_a_delta_type_are_refused() {
    for data_type in [
        DataType::LargeUtf8,
        DataType::Timestamp(TimeUnit::Millisecond, None),
        DataType::Timestamp(TimeUnit::Millisecond, Some("+01:00".into())),
        DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
        DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Binary)),
        DataType::FixedSizeBinary(4),
    ] {
        let schema = Schema::new(vec![Field::new("value", data_type.clone(), true)]);
        let error = TYPES.data_schema("logs", &schema).unwrap_err().to_string();
        assert!(
            error.contains("table `logs` column `value`") && error.contains("has no Delta type"),
            "{data_type}: {error}"
        );
    }
}

#[test]
fn conversions_describe_each_column_for_the_docs() {
    let schema = source(21_000, 5).schema();
    let conversions = |name: &str| {
        TYPES
            .conversions("blocks", schema.field_with_name(name).unwrap())
            .into_iter()
            .collect::<Vec<_>>()
    };
    assert_eq!(conversions("block_num"), [Conversion::CheckedLong]);
    assert_eq!(conversions("nonce"), [Conversion::Decimal]);
    assert_eq!(conversions("balances"), [Conversion::Decimal]);
    assert_eq!(conversions("indices"), [Conversion::CheckedLong]);
    assert_eq!(conversions("tx_index"), [Conversion::LosslessInteger]);
    assert_eq!(conversions("accounts"), [Conversion::LosslessInteger]);
    assert_eq!(conversions("status"), [Conversion::DictionaryString]);
    assert_eq!(conversions("timestamp"), [Conversion::TimestampMicros]);
    assert_eq!(conversions("date"), [Conversion::Partition]);
    assert_eq!(conversions("signer"), [Conversion::CheckedLong]);
    assert!(conversions("hash").is_empty());
    assert!(conversions("expiry").is_empty());
}

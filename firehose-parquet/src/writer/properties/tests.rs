use super::*;
use arrow::{
    array::{ArrayRef, BinaryArray, StringArray, StructArray},
    compute::concat_batches,
    datatypes::Field,
};
use bytes::Bytes;
use parquet::arrow::{arrow_reader::ParquetRecordBatchReaderBuilder, ArrowWriter};
use std::sync::Arc;

fn encoded(batch: &RecordBatch, properties: WriterProperties) -> Bytes {
    let mut writer = ArrowWriter::try_new(Vec::new(), batch.schema(), Some(properties)).unwrap();
    writer.write(batch).unwrap();
    writer.into_inner().unwrap().into()
}

#[test]
fn sorting_is_proved_per_complete_part_and_uses_parquet_leaf_ordinal() {
    for (numbers, expected) in [
        (vec![Some(1), Some(1), Some(2)], true),
        (vec![Some(2), Some(1), Some(2)], false),
        (vec![Some(1), None, Some(2)], false),
        (vec![], false),
    ] {
        let n = numbers.len();
        let nested = StructArray::from(vec![
            (
                Arc::new(Field::new("a", DataType::Utf8, false)),
                Arc::new(StringArray::from(vec!["x"; n])) as ArrayRef,
            ),
            (
                Arc::new(Field::new("b", DataType::Utf8, false)),
                Arc::new(StringArray::from(vec!["y"; n])) as ArrayRef,
            ),
        ]);
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("nested", nested.data_type().clone(), false),
                Field::new("block_num", DataType::UInt64, true),
            ])),
            vec![Arc::new(nested), Arc::new(UInt64Array::from(numbers))],
        )
        .unwrap();
        let props = for_batch(Compression::Zstd, &batch, None).unwrap();
        assert_eq!(props.sorting_columns().is_some(), expected);
        if expected {
            assert_eq!(props.sorting_columns().unwrap()[0].column_idx, 2);
        }
        let reader = ParquetRecordBatchReaderBuilder::try_new(encoded(&batch, props)).unwrap();
        for group in reader.metadata().row_groups() {
            assert_eq!(group.sorting_columns().is_some(), expected);
            if expected {
                assert_eq!(group.sorting_columns().unwrap()[0].column_idx, 2);
            }
        }
        assert!(for_schema(Compression::Zstd, batch.schema().as_ref(), None)
            .sorting_columns()
            .is_none());
    }
}

#[test]
fn lookup_filters_have_no_false_negatives_across_nulls_encodings_and_row_groups() {
    let n = ROW_GROUP_ROWS + 33;
    let hashes: Vec<Option<String>> = (0..n)
        .map(|i| (i % 101 != 0).then(|| format!("hash-é-{i:08}")))
        .collect();
    let addresses: Vec<Option<Vec<u8>>> = (0..n)
        .map(|i| (i % 103 != 0).then(|| (i as u64).to_le_bytes().to_vec()))
        .collect();
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("block_num", DataType::UInt64, false),
            Field::new("tx_hash", DataType::Utf8, true),
            Field::new("address", DataType::Binary, true),
            Field::new("data", DataType::Binary, true),
        ])),
        vec![
            Arc::new(UInt64Array::from_iter_values((0..n).map(|i| i as u64))),
            Arc::new(StringArray::from_iter(hashes.iter().map(|x| x.as_deref()))),
            Arc::new(BinaryArray::from_iter(
                addresses.iter().map(|x| x.as_deref()),
            )),
            Arc::new(BinaryArray::from_iter(
                addresses.iter().map(|x| x.as_deref()),
            )),
        ],
    )
    .unwrap();
    for compression in [
        Compression::None,
        Compression::Snappy,
        Compression::Gzip,
        Compression::Zstd,
        Compression::ZstdWithLevel(parquet::basic::ZstdLevel::try_new(6).unwrap()),
    ] {
        let props = for_batch(compression, &batch, None).unwrap();
        assert_eq!(
            props.compression(&ColumnPath::from("tx_hash")),
            compression.parquet()
        );
        let bytes = encoded(&batch, props);
        let builder = ParquetRecordBatchReaderBuilder::try_new(bytes).unwrap();
        assert_eq!(builder.metadata().num_row_groups(), 2);
        for group in 0..2 {
            let hash_filter = builder
                .get_row_group_column_bloom_filter(group, 1)
                .unwrap()
                .unwrap();
            let address_filter = builder
                .get_row_group_column_bloom_filter(group, 2)
                .unwrap()
                .unwrap();
            assert!(builder
                .get_row_group_column_bloom_filter(group, 3)
                .unwrap()
                .is_none());
            for row in group * ROW_GROUP_ROWS..n.min((group + 1) * ROW_GROUP_ROWS) {
                // Null predicates must use column null statistics, never Bloom absence.
                if let Some(hash) = &hashes[row] {
                    assert!(hash_filter.check(hash.as_str()));
                }
                if let Some(address) = &addresses[row] {
                    assert!(address_filter.check(address.as_slice()));
                }
            }
        }
        let actual = concat_batches(
            &batch.schema(),
            &builder
                .build()
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(actual, batch);
    }
}

#[test]
fn filters_are_bounded_and_never_added_to_nested_payloads() {
    let names = [
        "hash",
        "tx_hash",
        "signature",
        "address",
        "from",
        "to",
        "sender",
        "receiver",
        "account",
        "owner",
    ];
    let schema = Schema::new(
        names
            .iter()
            .map(|name| Field::new(*name, DataType::Utf8, false))
            .collect::<Vec<_>>(),
    );
    let props = for_schema(Compression::Zstd, &schema, None);
    assert_eq!(
        names
            .iter()
            .filter(|name| props
                .bloom_filter_properties(&ColumnPath::from(**name))
                .is_some())
            .count(),
        MAX_BLOOM_COLUMNS
    );
    assert_eq!(
        props.bloom_filter_position(),
        BloomFilterPosition::AfterRowGroup
    );
    let schema = Schema::new(vec![Field::new(
        "address",
        DataType::List(Arc::new(Field::new("item", DataType::Binary, true))),
        false,
    )]);
    let props = for_schema(Compression::Zstd, &schema, None);
    assert!(props
        .bloom_filter_properties(&ColumnPath::from("address"))
        .is_none());
}

#[test]
fn fixed_binary_and_dictionary_identity_filters_index_values_not_dictionary_ids() {
    use arrow::{
        array::{FixedSizeBinaryArray, StringDictionaryBuilder},
        datatypes::Int32Type,
    };
    let mut dictionary = StringDictionaryBuilder::<Int32Type>::new();
    dictionary.append("same").unwrap();
    dictionary.append_null();
    dictionary.append("other").unwrap();
    dictionary.append("same").unwrap();
    let dict = dictionary.finish();
    let fixed = FixedSizeBinaryArray::try_from_sparse_iter_with_size(
        [Some([0, 255]), None, Some([128, 1]), Some([0, 255])].into_iter(),
        2,
    )
    .unwrap();
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("hash", fixed.data_type().clone(), true),
            Field::new("address", dict.data_type().clone(), true),
        ])),
        vec![Arc::new(fixed), Arc::new(dict)],
    )
    .unwrap();
    let reader = ParquetRecordBatchReaderBuilder::try_new(encoded(
        &batch,
        for_batch(Compression::Zstd, &batch, None).unwrap(),
    ))
    .unwrap();
    let fixed_filter = reader
        .get_row_group_column_bloom_filter(0, 0)
        .unwrap()
        .unwrap();
    assert!(fixed_filter.check(&[0u8, 255][..]));
    assert!(fixed_filter.check(&[128u8, 1][..]));
    let dict_filter = reader
        .get_row_group_column_bloom_filter(0, 1)
        .unwrap()
        .unwrap();
    assert!(dict_filter.check("same"));
    assert!(dict_filter.check("other"));
    let actual = concat_batches(
        &batch.schema(),
        &reader
            .build()
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(actual, batch);
}

#[test]
fn direct_library_default_levels_write_identical_level_three_files() {
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("hash", DataType::Utf8, false)])),
        vec![Arc::new(StringArray::from(vec!["a repeated value"; 1000]))],
    )
    .unwrap();
    let expected = encoded(&batch, for_batch(Compression::Zstd, &batch, None).unwrap());
    for level in [0, 3] {
        let compression =
            Compression::ZstdWithLevel(parquet::basic::ZstdLevel::try_new(level).unwrap());
        assert_eq!(compression.to_string(), "zstd");
        assert_eq!(
            encoded(&batch, for_batch(compression, &batch, None).unwrap()),
            expected
        );
    }
}

//! Value semantics that must survive mapping, flushing, and Parquet round trips.
use super::*;
use arrow::datatypes::DataType;
use firehose_parquet::config::{BlockMetadata, Compression, Partition};
use firehose_parquet::encode::encode_bytes;
use firehose_parquet::writer::{read_parquet, ParquetTableWriter};

fn map(block: &beacon::Block, encoding: EncodeBytes) -> HashMap<String, RecordBatch> {
    let mut mapper = BeaconBlockMapper::new(false, encoding);
    mapper
        .map_block(&block.encode_to_vec(), &BlockIdentity::default(), None)
        .unwrap();
    let batches = mapper.flush().unwrap();
    for (table, batch) in &batches {
        if batch.num_rows() > 0 {
            round_trip(table, batch);
        }
    }
    batches
}

fn round_trip(table: &str, batch: &RecordBatch) {
    let dir = tempfile::tempdir().unwrap();
    let mut writer = ParquetTableWriter::new(dir.path(), Partition::None, Compression::Zstd);
    let metadata = BlockMetadata {
        min_block_number: 0,
        max_block_number: 0,
        min_timestamp: None,
        max_timestamp: None,
    };
    let (path, _) = writer.write_batch(table, batch, &metadata).unwrap();
    let batches = read_parquet(&path).unwrap();
    let actual = arrow::compute::concat_batches(&batches[0].schema(), &batches).unwrap();
    assert_eq!(&actual, batch, "Parquet round trip: {table}");
}

fn payload_block(fork: beacon::Spec, bytes: Vec<u8>) -> beacon::Block {
    use beacon::block::Body;
    let payload = beacon::DenebExecutionPayload {
        base_fee_per_gas: bytes.clone().into(),
        ..Default::default()
    };
    let body = match fork {
        beacon::Spec::Bellatrix => Body::Bellatrix(beacon::BellatrixBody {
            execution_payload: Some(beacon::BellatrixExecutionPayload {
                base_fee_per_gas: bytes.into(),
                ..Default::default()
            }),
            ..Default::default()
        }),
        beacon::Spec::Capella => Body::Capella(beacon::CapellaBody {
            execution_payload: Some(beacon::CapellaExecutionPayload {
                base_fee_per_gas: bytes.into(),
                ..Default::default()
            }),
            ..Default::default()
        }),
        beacon::Spec::Deneb => Body::Deneb(beacon::DenebBody {
            execution_payload: Some(payload),
            ..Default::default()
        }),
        beacon::Spec::Electra => Body::Electra(beacon::ElectraBody {
            execution_payload: Some(payload),
            ..Default::default()
        }),
        beacon::Spec::Fusaka => Body::Fusaka(beacon::ElectraBody {
            execution_payload: Some(payload),
            ..Default::default()
        }),
        _ => unreachable!(),
    };
    beacon::Block {
        spec: fork as i32,
        body: Some(body),
        ..Default::default()
    }
}

fn fee(batch: &RecordBatch) -> &str {
    batch
        .column_by_name("base_fee_per_gas")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .value(0)
}

#[test]
fn every_payload_type_uses_its_producer_byte_order_and_full_uint256_range() {
    let maximum = "115792089237316195423570985008687907853269984665640564039457584007913129639935";
    for fork in [
        beacon::Spec::Bellatrix,
        beacon::Spec::Capella,
        beacon::Spec::Deneb,
        beacon::Spec::Electra,
        beacon::Spec::Fusaka,
    ] {
        let little = matches!(fork, beacon::Spec::Bellatrix | beacon::Spec::Capella);
        for (big, expected) in [(vec![], "0"), (vec![1, 2], "258"), (vec![255; 32], maximum)] {
            let bytes = if little {
                let mut b = big.clone();
                b.reverse();
                b.resize(32, 0);
                b
            } else {
                big
            };
            for encoding in [EncodeBytes::Binary, EncodeBytes::Hex] {
                let batches = map(&payload_block(fork, bytes.clone()), encoding);
                assert_eq!(fee(&batches["execution_payload"]), expected, "{fork:?}");
            }
        }
    }
    // Retained, cursor-stripped live Firehose samples from #504.
    assert_eq!(
        fee(&map(
            &payload_block(beacon::Spec::Deneb, vec![3, 0x27, 0x35, 0x70, 0xed]),
            EncodeBytes::Hex
        )["execution_payload"]),
        "13542715629"
    );
    assert_eq!(
        fee(&map(
            &payload_block(beacon::Spec::Fusaka, vec![6, 0x7b, 0x36, 0xe4]),
            EncodeBytes::Hex
        )["execution_payload"]),
        "108738276"
    );
}

#[test]
fn malformed_fee_rejects_whole_block_without_mutating_buffered_tables() {
    for (fork, lengths) in [
        (beacon::Spec::Bellatrix, vec![0, 1, 31, 33]),
        (beacon::Spec::Capella, vec![0, 31, 33]),
        (beacon::Spec::Deneb, vec![33]),
        (beacon::Spec::Electra, vec![33]),
        (beacon::Spec::Fusaka, vec![33]),
    ] {
        let mut mapper = BeaconBlockMapper::new(false, EncodeBytes::Hex);
        let valid = payload_block(beacon::Spec::Deneb, vec![1]);
        mapper
            .map_block(&valid.encode_to_vec(), &BlockIdentity::default(), None)
            .unwrap();
        for length in lengths {
            let invalid = payload_block(fork, vec![0; length]);
            let error = mapper
                .map_block(&invalid.encode_to_vec(), &BlockIdentity::default(), None)
                .unwrap_err();
            assert!(error.to_string().contains("base_fee_per_gas"));
        }
        let batches = mapper.flush().unwrap();
        assert_eq!(batches["blocks"].num_rows(), 1);
        assert_eq!(batches["execution_payload"].num_rows(), 1);
        assert_eq!(fee(&batches["execution_payload"]), "1");
        assert!(batches
            .iter()
            .filter(|(name, _)| !matches!(name.as_str(), "blocks" | "execution_payload"))
            .all(|(_, batch)| batch.num_rows() == 0));
    }
}

#[test]
fn generated_spec_names_are_dictionary_encoded_with_distinct_unknown() {
    let mut mapper = BeaconBlockMapper::new(false, EncodeBytes::Hex);
    for spec in [-1, 0, 1, 2, 3, 4, 5, 6, 7, 999] {
        mapper
            .map_block(
                &beacon::Block {
                    spec,
                    ..Default::default()
                }
                .encode_to_vec(),
                &BlockIdentity::default(),
                None,
            )
            .unwrap();
    }
    let batches = mapper.flush().unwrap();
    round_trip("blocks", &batches["blocks"]);
    let array = batches["blocks"]
        .column_by_name("spec")
        .unwrap()
        .as_any()
        .downcast_ref::<DictionaryArray<Int32Type>>()
        .unwrap();
    let values = array
        .values()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let actual: Vec<_> = array
        .keys()
        .values()
        .iter()
        .map(|key| values.value(*key as usize))
        .collect();
    assert_eq!(
        actual,
        [
            "UNKNOWN",
            "UNSPECIFIED",
            "PHASE0",
            "ALTAIR",
            "BELLATRIX",
            "CAPELLA",
            "DENEB",
            "ELECTRA",
            "FUSAKA",
            "UNKNOWN"
        ]
    );
    assert_eq!(values.len(), 9);
    assert_eq!(mapper.flush().unwrap()["blocks"].num_rows(), 0);
}

#[test]
fn full_size_blobs_remain_binary_in_every_encoding_and_after_flush() {
    let blob: Vec<u8> = (0..131_072).map(|i| (i % 251) as u8).collect();
    for encoding in [
        EncodeBytes::Binary,
        EncodeBytes::Hex,
        EncodeBytes::HexNoPrefix,
        EncodeBytes::Base58,
        EncodeBytes::TronBase58,
    ] {
        let block = beacon::Block {
            body: Some(beacon::block::Body::Deneb(beacon::DenebBody {
                embedded_blobs: vec![beacon::Blob {
                    blob: blob.clone().into(),
                    kzg_commitment: vec![0xa5; 48].into(),
                    kzg_proof: vec![0xb6; 48].into(),
                    index: 7,
                    ..Default::default()
                }],
                ..Default::default()
            })),
            ..Default::default()
        };
        let mut mapper = BeaconBlockMapper::new(false, encoding.clone());
        for _ in 0..2 {
            mapper
                .map_block(&block.encode_to_vec(), &BlockIdentity::default(), None)
                .unwrap();
            let (table, estimate) = mapper.largest_table();
            assert_eq!(table, "blob_sidecars");
            assert!(
                (131_072..132_500).contains(&estimate),
                "{encoding:?}: {estimate}"
            );
            let batches = mapper.flush().unwrap();
            let batch = &batches["blob_sidecars"];
            round_trip("blob_sidecars", batch);
            assert_eq!(
                batch
                    .column_by_name("blob")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<BinaryArray>()
                    .unwrap()
                    .value(0),
                blob
            );
            let commitment = batch.column_by_name("kzg_commitment").unwrap();
            if encoding == EncodeBytes::Binary {
                assert_eq!(
                    commitment
                        .as_any()
                        .downcast_ref::<BinaryArray>()
                        .unwrap()
                        .value(0),
                    &[0xa5; 48]
                );
            } else {
                assert_eq!(
                    commitment
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap()
                        .value(0),
                    encode_bytes(&[0xa5; 48], &encoding)
                );
            }
            assert_eq!(mapper.flush().unwrap()["blob_sidecars"].num_rows(), 0);
        }
    }
}

#[test]
fn missing_nested_messages_are_null_but_present_zero_and_empty_values_survive() {
    let checkpoints = beacon::AttestationData {
        source: Some(Default::default()),
        target: Some(Default::default()),
        ..Default::default()
    };
    let block = beacon::Block {
        body: Some(beacon::block::Body::Deneb(beacon::DenebBody {
            attestations: vec![
                Default::default(),
                beacon::Attestation {
                    data: Some(Default::default()),
                    ..Default::default()
                },
                beacon::Attestation {
                    data: Some(checkpoints.clone()),
                    ..Default::default()
                },
            ],
            deposits: vec![
                Default::default(),
                beacon::Deposit {
                    data: Some(Default::default()),
                    ..Default::default()
                },
            ],
            proposer_slashings: vec![
                Default::default(),
                beacon::ProposerSlashing {
                    signed_header_1: Some(Default::default()),
                    signed_header_2: Some(Default::default()),
                },
                beacon::ProposerSlashing {
                    signed_header_1: Some(beacon::SignedBeaconBlockHeader {
                        message: Some(Default::default()),
                        ..Default::default()
                    }),
                    signed_header_2: Some(beacon::SignedBeaconBlockHeader {
                        message: Some(Default::default()),
                        ..Default::default()
                    }),
                },
            ],
            attester_slashings: vec![
                Default::default(),
                beacon::AttesterSlashing {
                    attestation_1: Some(Default::default()),
                    attestation_2: Some(Default::default()),
                },
                beacon::AttesterSlashing {
                    attestation_1: Some(beacon::IndexedAttestation {
                        data: Some(Default::default()),
                        ..Default::default()
                    }),
                    attestation_2: Some(beacon::IndexedAttestation {
                        data: Some(Default::default()),
                        ..Default::default()
                    }),
                },
                beacon::AttesterSlashing {
                    attestation_1: Some(beacon::IndexedAttestation {
                        data: Some(checkpoints.clone()),
                        ..Default::default()
                    }),
                    attestation_2: Some(beacon::IndexedAttestation {
                        data: Some(checkpoints),
                        ..Default::default()
                    }),
                },
            ],
            voluntary_exits: vec![
                Default::default(),
                beacon::SignedVoluntaryExit {
                    message: Some(Default::default()),
                    ..Default::default()
                },
            ],
            bls_to_execution_changes: vec![
                Default::default(),
                beacon::SignedBlsToExecutionChange {
                    message: Some(Default::default()),
                    ..Default::default()
                },
            ],
            ..Default::default()
        })),
        ..Default::default()
    };
    for encoding in [EncodeBytes::Binary, EncodeBytes::Hex] {
        let batches = map(&block, encoding);
        let check = |table: &str, fields: &[&str], nulls: &[bool]| {
            for field in fields {
                let b = &batches[table];
                assert!(b.schema().field_with_name(field).unwrap().is_nullable());
                let array = b.column_by_name(field).unwrap();
                assert_eq!(
                    (0..array.len())
                        .map(|i| array.is_null(i))
                        .collect::<Vec<_>>(),
                    nulls,
                    "{table}.{field}"
                );
                for (i, null) in nulls.iter().enumerate() {
                    if !null {
                        match array.data_type() {
                            DataType::UInt64 => assert_eq!(
                                array
                                    .as_any()
                                    .downcast_ref::<UInt64Array>()
                                    .unwrap()
                                    .value(i),
                                0
                            ),
                            DataType::Binary => assert!(array
                                .as_any()
                                .downcast_ref::<BinaryArray>()
                                .unwrap()
                                .value(i)
                                .is_empty()),
                            DataType::Utf8 => assert_eq!(
                                array
                                    .as_any()
                                    .downcast_ref::<StringArray>()
                                    .unwrap()
                                    .value(i),
                                "0x"
                            ),
                            DataType::List(_) => assert!(array
                                .as_any()
                                .downcast_ref::<ListArray>()
                                .unwrap()
                                .value(i)
                                .is_empty()),
                            other => panic!("unexpected {other}"),
                        }
                    }
                }
            }
        };
        check(
            "attestations",
            &["slot", "committee_index", "beacon_block_root"],
            &[true, false, false],
        );
        check(
            "attestations",
            &["source_epoch", "source_root", "target_epoch", "target_root"],
            &[true, true, false],
        );
        check(
            "deposits",
            &["pubkey", "withdrawal_credentials", "amount", "signature"],
            &[true, false],
        );
        for n in [1, 2] {
            for field in [
                "slot",
                "proposer_index",
                "parent_root",
                "state_root",
                "body_root",
            ] {
                check(
                    "proposer_slashings",
                    &[&format!("header_{n}_{field}")],
                    &[true, true, false],
                );
            }
            for field in ["slot", "committee_index", "beacon_block_root"] {
                check(
                    "attester_slashings",
                    &[&format!("attestation_{n}_{field}")],
                    &[true, true, false, false],
                );
            }
            for field in ["source_epoch", "source_root", "target_epoch", "target_root"] {
                check(
                    "attester_slashings",
                    &[&format!("attestation_{n}_{field}")],
                    &[true, true, true, false],
                );
            }
            check(
                "attester_slashings",
                &[&format!("attestation_{n}_attesting_indices")],
                &[true, false, false, false],
            );
        }
        check(
            "voluntary_exits",
            &["epoch", "validator_index"],
            &[true, false],
        );
        check(
            "bls_to_execution_changes",
            &["validator_index", "from_bls_pubkey", "to_execution_address"],
            &[true, false],
        );
        for table in [
            "attestations",
            "voluntary_exits",
            "bls_to_execution_changes",
        ] {
            assert_eq!(
                batches[table]
                    .column_by_name("signature")
                    .unwrap()
                    .null_count(),
                0
            );
        }
        assert_eq!(batches["execution_payload"].num_rows(), 0);
        for batch in batches.values() {
            assert_eq!(batch.column_by_name("block_num").unwrap().null_count(), 0);
        }
    }
    let absent = map(&beacon::Block::default(), EncodeBytes::Hex);
    assert!(absent["blocks"]
        .column_by_name("graffiti")
        .unwrap()
        .is_null(0));
    assert!(absent
        .iter()
        .filter(|(name, _)| *name != "blocks")
        .all(|(_, batch)| batch.num_rows() == 0));
}

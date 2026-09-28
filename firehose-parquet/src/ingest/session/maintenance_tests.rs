//! Exercise the public session boundary, not only the maintenance predicates.
use super::*;
use crate::dataset_lock::MutationScope;
use crate::durable_state::CONTROL_DIRECTORY;
use arrow::array::UInt64Array;
use arrow::datatypes::{DataType, Field, Schema};
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;

fn mapper() -> MapperSemantics {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "block_num",
        DataType::UInt64,
        false,
    )]));
    let batch =
        RecordBatch::try_new(schema, vec![Arc::new(UInt64Array::from(Vec::<u64>::new()))]).unwrap();
    MapperSemantics {
        chain: "mainnet".into(),
        family: BlockFamily::Evm,
        bytes_encoding: "binary".into(),
        extended: false,
        with_votes: false,
        include_failed_transactions: true,
        tables: declare_inventory(
            &[("blocks".into(), batch)].into(),
            &["blocks"],
            &DeltaTypes::default(),
        )
        .unwrap(),
        delta_types: DeltaTypes::default(),
    }
}
fn config(output: &Path) -> Config {
    Config {
        output: output.into(),
        start_block: Some(100),
        final_blocks_only: true,
        ..Default::default()
    }
}
async fn own(config: &Config) -> DatasetOwnership {
    DatasetOwnership::acquire(
        "session-order-test",
        vec![MutationScope::directory(config.output.to_string_lossy())],
        None,
    )
    .await
    .unwrap()
}
fn snapshot(root: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
    fn walk(root: &Path, path: &Path, result: &mut BTreeMap<PathBuf, Vec<u8>>) {
        for entry in fs::read_dir(path).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                walk(root, &path, result);
            } else {
                result.insert(
                    path.strip_prefix(root).unwrap().into(),
                    fs::read(path).unwrap(),
                );
            }
        }
    }
    let mut result = BTreeMap::new();
    walk(root, root, &mut result);
    result
}

#[tokio::test]
async fn session_refuses_ancestor_and_descendant_authority_before_initialization() {
    let temp = tempfile::tempdir().unwrap();
    let protected = temp.path().join("protected");
    let original = config(&protected);
    let owner = own(&original).await;
    drop(
        IngestionSession::open(&original, mapper(), &owner, None, None)
            .await
            .unwrap(),
    );
    owner.release().await.unwrap();
    let before = snapshot(&protected);

    for selected in [protected.join("missing-child"), temp.path().to_path_buf()] {
        let selected_config = config(&selected);
        let owner = own(&selected_config).await;
        let error = IngestionSession::open(&selected_config, mapper(), &owner, None, None)
            .await
            .err()
            .unwrap();
        assert!(
            error
                .to_string()
                .contains("overlaps another protected root"),
            "{error:#}"
        );
        assert_eq!(snapshot(&protected), before);
        assert!(!protected.join("missing-child").exists());
        assert!(!temp.path().join(CONTROL_DIRECTORY).exists());
        owner.release().await.unwrap();
    }
}

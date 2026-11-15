//! The parts of `fireparq recovery rebind` that need no storage: which roots
//! may be paired, where the cursor mirror moves, and the rebound authority.
//! `ingest/session/tests/rebind.rs` rebinds real datasets.

use super::*;
use crate::ingest::state::tests::{descriptor, event};
use crate::ingest::state::{AuthorityState, Digest, RoutingPolicy};

fn aws() -> AwsConfig {
    AwsConfig {
        aws_access_key_id: None,
        aws_secret_access_key: None,
        aws_session_token: None,
        aws_region: None,
        aws_endpoint_url: None,
    }
}

fn root(path: &str) -> Root {
    Root::resolve(path, &aws()).unwrap()
}

fn message<T>(result: Result<T>) -> String {
    format!("{:#}", result.err().expect("refused"))
}

#[test]
fn prefixes_nest_on_path_segments_and_the_bucket_root_holds_every_prefix() {
    assert!(nested("", "eth"));
    assert!(nested("eth", ""));
    assert!(nested("eth", "eth"));
    assert!(nested("eth", "eth/v1"));
    assert!(nested("eth/v1", "eth"));
    assert!(!nested("eth", "eth-v1"));
    assert!(!nested("eth/v1", "eth/v2"));
}

#[test]
fn roots_pair_on_one_kind_of_storage_and_one_service_without_overlap() {
    let dir = tempfile::tempdir().unwrap();
    let local = |name: &str| root(dir.path().join(name).to_str().unwrap());

    check_roots(&root("s3://eth-v1-datasets"), &root("s3://eth")).unwrap();
    check_roots(&root("s3://data/eth-v1"), &root("s3://data/eth")).unwrap();
    check_roots(&root("s3://other/eth"), &root("s3://data/eth")).unwrap();
    check_roots(&local("eth-v1"), &local("eth")).unwrap();

    for (to, from) in [
        (root("s3://data/eth/v1"), root("s3://data/eth")),
        (root("s3://data"), root("s3://data/eth")),
        (local("eth/v1"), local("eth")),
        (local("eth"), local("eth/v1")),
    ] {
        let refused = message(check_roots(&to, &from));
        assert!(refused.contains("overlap"), "{refused}");
    }
    let refused = message(check_roots(&local("eth"), &root("s3://data/eth")));
    assert!(refused.contains("different kinds of storage"), "{refused}");

    // Another endpoint or region is another service.
    let mut elsewhere = root("s3://eth");
    let StorageIdentity::S3 { service, .. } = &mut elsewhere.identity else {
        unreachable!()
    };
    *service = Digest::hash("s3-service-v1", &"elsewhere").unwrap();
    let refused = message(check_roots(&root("s3://eth-v1-datasets"), &elsewhere));
    assert!(refused.contains("different S3 services"), "{refused}");
}

fn bound(root: &Root, mirror: MirrorBinding) -> StreamDescriptor {
    StreamDescriptor {
        output: root.identity.clone(),
        mirror,
        ..descriptor(RoutingPolicy::GenesisLookaheadV1)
    }
}

fn s3_mirror(bucket: &str, key: &str) -> MirrorBinding {
    resolve_mirror_binding(&format!("s3://{bucket}"), Some(key), &aws()).unwrap()
}

#[test]
fn an_s3_mirror_moves_to_the_same_key_under_the_new_root() {
    for (from, to, key, moved) in [
        (
            "s3://eth",
            "s3://eth-v1-datasets",
            "_fireparq/cursor.parquet",
            ("eth-v1-datasets", "_fireparq/cursor.parquet"),
        ),
        (
            "s3://data/eth",
            "s3://data/eth-v1",
            "eth/_fireparq/cursor.parquet",
            ("data", "eth-v1/_fireparq/cursor.parquet"),
        ),
        (
            "s3://data/eth",
            "s3://eth-v1-datasets",
            "eth/cursor.parquet",
            ("eth-v1-datasets", "cursor.parquet"),
        ),
    ] {
        let (from, to) = (root(from), root(to));
        let bucket = match &from.identity {
            StorageIdentity::S3 { bucket, .. } => bucket.clone(),
            StorageIdentity::Local { .. } => unreachable!(),
        };
        let previous = bound(&from, s3_mirror(&bucket, key));
        let next = relocate(&previous, &from, &to, &aws()).unwrap();
        assert!(next.output == to.identity);
        assert!(next.mirror == s3_mirror(moved.0, moved.1));
        // Every other field is the stream's.
        assert!(
            StreamDescriptor {
                output: previous.output.clone(),
                mirror: previous.mirror.clone(),
                ..next.clone()
            } == previous
        );
        // The way back is exact: a rerun finds the previous descriptor.
        assert!(relocate(&next, &to, &from, &aws()).unwrap() == previous);
    }
    let (from, to) = (root("s3://eth"), root("s3://eth-v1-datasets"));
    let disabled = relocate(&bound(&from, MirrorBinding::Disabled), &from, &to, &aws()).unwrap();
    assert!(disabled.mirror == MirrorBinding::Disabled);
}

#[test]
fn a_mirror_outside_the_root_is_refused() {
    let from = root("s3://data/eth");
    let to = root("s3://data/eth-v1");
    for mirror in [
        s3_mirror("data", "cursors/eth.parquet"),
        s3_mirror("data", "ethereum/cursor.parquet"),
        s3_mirror("cursors", "eth/cursor.parquet"),
        MirrorBinding::Local {
            absolute_path: "/var/lib/fireparq/cursor.parquet".into(),
        },
    ] {
        let refused = message(relocate(&bound(&from, mirror), &from, &to, &aws()));
        assert!(refused.contains("outside that root"), "{refused}");
    }
    let refused = message(relocate(
        &bound(&root("s3://elsewhere"), MirrorBinding::Disabled),
        &from,
        &to,
        &aws(),
    ));
    assert!(refused.contains("not bound to s3://data/eth"), "{refused}");
}

#[test]
fn a_local_mirror_moves_inside_the_new_root_whichever_spelling_bound_it() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(dir.path().join("eth")).unwrap();
    let from_path = dir.path().join("eth");
    let from = root(from_path.to_str().unwrap());
    let to = root(dir.path().join("eth-v1").to_str().unwrap());
    let moved = resolve_mirror_binding(&to.path, Some("_fireparq/cursor.parquet"), &aws()).unwrap();
    // As `build` bound it from the path it was given, and from the canonical
    // root (a temporary directory may be behind a symbolic link).
    let StorageIdentity::Local { canonical_root } = &from.identity else {
        unreachable!()
    };
    for mirror_root in [from_path.clone(), PathBuf::from(canonical_root)] {
        let mirror = MirrorBinding::Local {
            absolute_path: mirror_root
                .join("_fireparq/cursor.parquet")
                .to_str()
                .unwrap()
                .into(),
        };
        let next = relocate(&bound(&from, mirror), &from, &to, &aws()).unwrap();
        assert!(next.mirror == moved);
    }
    let outside = MirrorBinding::Local {
        absolute_path: dir.path().join("cursor.parquet").to_str().unwrap().into(),
    };
    let refused = message(relocate(&bound(&from, outside), &from, &to, &aws()));
    assert!(refused.contains("outside that root"), "{refused}");
}

#[test]
fn a_rebound_authority_keeps_its_progress_under_the_new_stream_and_changes_nothing_else() {
    let from = root("s3://eth");
    let to = root("s3://eth-v1-datasets");
    let previous = bound(&from, s3_mirror("eth", "_fireparq/cursor.parquet"));
    let authority = AuthorityState::sealed(previous.clone(), 3, event(102, 1), 103).unwrap();
    let next = relocate(&previous, &from, &to, &aws()).unwrap();

    let rebound = authority.rebound(next.clone()).unwrap();
    rebound.validate().unwrap();
    let (old, new) = (&authority.checkpoint, &rebound.checkpoint);
    assert!(rebound.descriptor == next);
    assert_eq!(new.previous.as_ref(), Some(&old.id));
    assert_ne!(new.id, old.id);
    assert_eq!(
        (new.ordinal, &new.event, &new.routing, new.completed_stop),
        (old.ordinal, &old.event, &old.routing, old.completed_stop)
    );
    // The previous head does not validate under the new stream.
    assert!(old.validate(&next).is_err());

    // Only the output and the mirror may change, and the output must.
    let other_chain = StreamDescriptor {
        chain: "sepolia".into(),
        ..next.clone()
    };
    let refused = message(authority.rebound(other_chain));
    assert!(
        refused.contains("only its output and cursor mirror"),
        "{refused}"
    );
    let refused = message(authority.rebound(previous.clone()));
    assert!(refused.contains("changes its output root"), "{refused}");

    // Nothing accepted, nothing to relocate.
    let empty = AuthorityState::initial(previous).unwrap();
    let refused = message(empty.rebound(next));
    assert!(refused.contains("accepted nothing"), "{refused}");
}

#[test]
fn the_binding_refusal_names_both_roots_and_the_rebind_or_the_service() {
    let bound = root("s3://eth").identity;
    let hint = super::super::binding::rebind_hint(&bound, &root("s3://eth-v1-datasets").identity);
    assert!(
        hint.contains("bound to s3://eth, not to s3://eth-v1-datasets")
            && hint.contains("fireparq recovery rebind s3://eth-v1-datasets --from s3://eth"),
        "{hint}"
    );
    let mut elsewhere = root("s3://eth").identity;
    let StorageIdentity::S3 { service, .. } = &mut elsewhere else {
        unreachable!()
    };
    *service = Digest::hash("s3-service-v1", &"elsewhere").unwrap();
    let hint = super::super::binding::rebind_hint(&bound, &elsewhere);
    assert!(
        hint.contains("bound to s3://eth on another S3 endpoint or region")
            && !hint.contains("recovery rebind"),
        "{hint}"
    );
}

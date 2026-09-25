use super::*;
use crate::dataset_lock::LocalOwnership;
use std::time::Duration;

fn fixture(root: &Path) -> std::path::PathBuf {
    let data = root.join("source/mainnet/blocks");
    write_block_nums(&data.join("day=1/part-0.parquet"), &[1, 2]);
    data
}

#[test]
fn source_and_every_artifact_scope_conflict_before_any_publication() {
    for default_registry in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let data = fixture(dir.path());
        let registry = if default_registry {
            data.parent().unwrap().join(MERKLE_ROOTS_FILENAME)
        } else {
            dir.path().join("external-registry/roots.parquet")
        };
        let report = dir.path().join("local-report/report.json");
        let published = dir.path().join("published/report.json");
        let mut opts = roots_opts(&registry);
        if default_registry {
            opts.registry_path = None;
        }
        opts.report_json = Some(report.clone());
        opts.publish_report_path = Some(published.to_string_lossy().into_owned());
        let scopes = [
            data.clone(),
            registry.parent().unwrap().to_path_buf(),
            report.parent().unwrap().to_path_buf(),
            published.parent().unwrap().to_path_buf(),
        ];
        for scope in &scopes {
            let held = LocalOwnership::acquire(&[scope.clone()]).unwrap();
            let error = verify_parquet(data.to_str().unwrap(), None, &opts).unwrap_err();
            assert!(format!("{error:#}").contains("ownership"), "{error:#}");
            assert!(!registry.exists());
            assert!(!report.exists());
            assert!(!published.exists());
            drop(held);
        }
        assert!(
            verify_parquet(data.to_str().unwrap(), None, &opts)
                .unwrap()
                .summary
                .wrote_registry
        );
        assert!(registry.exists() && report.exists() && published.exists());
        LocalOwnership::acquire(&scopes).unwrap();
    }
}

#[test]
fn all_scopes_stay_owned_until_registry_and_reports_finish() {
    let dir = tempfile::tempdir().unwrap();
    let data = fixture(dir.path());
    let registry = dir.path().join("registry/roots.parquet");
    let report = dir.path().join("report/report.json");
    let published = dir.path().join("published/report.json");
    std::fs::create_dir_all(registry.parent().unwrap()).unwrap();
    std::fs::create_dir_all(report.parent().unwrap()).unwrap();
    let legacy_lock = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(super::super::sibling_path(&registry, ".lock"))
        .unwrap();
    legacy_lock.lock().unwrap();
    let mut opts = roots_opts(&registry);
    opts.report_json = Some(report.clone());
    opts.publish_report_path = Some(published.to_string_lossy().into_owned());
    let run_id = uuid::Uuid::new_v4().to_string();
    let plan =
        super::super::VerifyMutationPlan::discover(data.to_str().unwrap(), None, &opts, &run_id)
            .unwrap();
    let ownership =
        crate::dataset_lock::DatasetOwnership::acquire_blocking("verify", plan.scopes, None)
            .unwrap();
    let source = data.clone();
    let (send, receive) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        let result = super::super::with_verify_ownership(Some(ownership), |ownership| {
            super::super::verify_owned(
                source.to_string_lossy().into_owned(),
                None,
                &opts,
                time::OffsetDateTime::now_utc(),
                run_id,
                Some(&plan.chain_root),
                ownership,
            )
        });
        send.send(result).unwrap();
    });
    for scope in [
        &data,
        registry.parent().unwrap(),
        report.parent().unwrap(),
        published.parent().unwrap(),
    ] {
        assert!(LocalOwnership::acquire(&[scope.to_path_buf()]).is_err());
    }
    assert!(!registry.exists() && !report.exists() && !published.exists());
    // Allow the existing legacy registry lock to release the paused commit.
    // The common guard must remain held across the following report writes.
    drop(legacy_lock);
    let result = receive
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    worker.join().unwrap();
    assert!(result.summary.wrote_registry);
    assert!(registry.exists() && report.exists() && published.exists());
    LocalOwnership::acquire(&[dir.path().to_path_buf()]).unwrap();
}

#[test]
fn authoritative_scan_relists_after_scope_discovery() {
    let dir = tempfile::tempdir().unwrap();
    let data = fixture(dir.path());
    let opts = roots_opts(&dir.path().join("registry/roots.parquet"));
    let run_id = uuid::Uuid::new_v4().to_string();
    let plan =
        super::super::VerifyMutationPlan::discover(data.to_str().unwrap(), None, &opts, &run_id)
            .unwrap();
    // A writer finished between planning and ownership acquisition. The first
    // listing is not a snapshot and must not supply the authoritative roots.
    write_block_nums(&data.join("day=2/part-0.parquet"), &[3, 4]);
    let report = super::super::verify_with_plan(
        data.to_string_lossy().into_owned(),
        None,
        &opts,
        time::OffsetDateTime::now_utc(),
        run_id,
        Some(plan),
    )
    .unwrap();
    assert_eq!(report.summary.partitions_scanned, 2);
    assert_eq!(report.findings.len(), 2);
    assert_eq!(
        load_registry(&report.registry_path, None)
            .unwrap()
            .rows
            .len(),
        2
    );
}

#[test]
fn changed_layout_after_planning_refuses_artifacts_and_releases_local_scopes() {
    let dir = tempfile::tempdir().unwrap();
    let data = fixture(dir.path());
    let registry = dir.path().join("registry/roots.parquet");
    let opts = roots_opts(&registry);
    let run_id = uuid::Uuid::new_v4().to_string();
    let mut plan =
        super::super::VerifyMutationPlan::discover(data.to_str().unwrap(), None, &opts, &run_id)
            .unwrap();
    plan.chain_root.push_str("/different-dataset");
    let error = super::super::verify_with_plan(
        data.to_string_lossy().into_owned(),
        None,
        &opts,
        time::OffsetDateTime::now_utc(),
        run_id,
        Some(plan),
    )
    .unwrap_err();
    assert!(error.to_string().contains("layout changed"));
    assert!(!registry.exists());
    LocalOwnership::acquire(&[dir.path().to_path_buf()]).unwrap();
}

#[test]
fn replaced_artifact_directory_is_revalidated_before_first_publication() {
    let dir = tempfile::tempdir().unwrap();
    let data = fixture(dir.path());
    let registry = dir.path().join("registry/roots.parquet");
    let reports = dir.path().join("reports");
    std::fs::create_dir_all(registry.parent().unwrap()).unwrap();
    std::fs::create_dir(&reports).unwrap();
    let mut opts = roots_opts(&registry);
    opts.report_json = Some(reports.join("report.json"));
    let run_id = uuid::Uuid::new_v4().to_string();
    let plan =
        super::super::VerifyMutationPlan::discover(data.to_str().unwrap(), None, &opts, &run_id)
            .unwrap();
    let ownership =
        crate::dataset_lock::DatasetOwnership::acquire_blocking("verify", plan.scopes, None)
            .unwrap();
    // Deliberately bypass cooperating commands to replace a held path's inode.
    std::fs::rename(&reports, dir.path().join("old-reports")).unwrap();
    std::fs::create_dir(&reports).unwrap();
    let error = super::super::with_verify_ownership(Some(ownership), |ownership| {
        super::super::verify_owned(
            data.to_string_lossy().into_owned(),
            None,
            &opts,
            time::OffsetDateTime::now_utc(),
            run_id,
            Some(&plan.chain_root),
            ownership,
        )
    })
    .unwrap_err();
    assert!(format!("{error:#}").contains("ownership"), "{error:#}");
    assert!(!registry.exists());
    assert!(!reports.join("report.json").exists());
}

#[test]
fn protocol_only_without_outputs_stays_read_only_but_reports_require_ownership() {
    let dir = tempfile::tempdir().unwrap();
    let data = fixture(dir.path());
    let mut opts = base_opts();
    opts.checks = vec![VerifyCheck::Protocol];
    let held = LocalOwnership::acquire(&[data.clone()]).unwrap();
    verify_parquet(data.to_str().unwrap(), None, &opts).unwrap();
    opts.publish_report = true;
    let error = verify_parquet(data.to_str().unwrap(), None, &opts).unwrap_err();
    assert!(format!("{error:#}").contains("ownership"));
    drop(held);
    let report = verify_parquet(data.to_str().unwrap(), None, &opts).unwrap();
    assert!(!report.summary.wrote_registry);
    assert!(Path::new(report.published_report_path.as_ref().unwrap()).exists());
}

#[derive(Debug, Default)]
struct ArtifactStore {
    inner: object_store::memory::InMemory,
    // 0 = success, 1 = published but lost response, 2 = unsupported write.
    fault: std::sync::atomic::AtomicU8,
    writes: std::sync::Mutex<Vec<object_store::PutMode>>,
}

impl std::fmt::Display for ArtifactStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("synthetic artifact store")
    }
}

#[async_trait::async_trait]
impl object_store::ObjectStore for ArtifactStore {
    async fn put_opts(
        &self,
        key: &object_store::path::Path,
        payload: object_store::PutPayload,
        opts: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        let artifact = matches!(key.as_ref(), "registry.parquet" | "report.json");
        let fault = if artifact {
            self.writes.lock().unwrap().push(opts.mode.clone());
            self.fault.load(std::sync::atomic::Ordering::SeqCst)
        } else {
            0
        };
        if fault == 2 {
            return Err(object_store::Error::NotImplemented);
        }
        let result = self.inner.put_opts(key, payload, opts).await?;
        if fault == 1 {
            Err(object_store::Error::Generic {
                store: "synthetic",
                source: "accepted artifact lost its response".into(),
            })
        } else {
            Ok(result)
        }
    }
    async fn get_opts(
        &self,
        key: &object_store::path::Path,
        opts: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        self.inner.get_opts(key, opts).await
    }
    async fn delete(&self, key: &object_store::path::Path) -> object_store::Result<()> {
        self.inner.delete(key).await
    }
    async fn put_multipart_opts(
        &self,
        key: &object_store::path::Path,
        opts: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.inner.put_multipart_opts(key, opts).await
    }
    fn list(
        &self,
        prefix: Option<&object_store::path::Path>,
    ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>> {
        self.inner.list(prefix)
    }
    async fn list_with_delimiter(
        &self,
        prefix: Option<&object_store::path::Path>,
    ) -> object_store::Result<object_store::ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }
    async fn copy(
        &self,
        from: &object_store::path::Path,
        to: &object_store::path::Path,
    ) -> object_store::Result<()> {
        self.inner.copy(from, to).await
    }
    async fn copy_if_not_exists(
        &self,
        from: &object_store::path::Path,
        to: &object_store::path::Path,
    ) -> object_store::Result<()> {
        self.inner.copy_if_not_exists(from, to).await
    }
}

#[test]
fn remote_registry_and_report_failures_never_retry_or_release_the_owner() {
    use crate::dataset_lock::DatasetOwnership;
    use crate::dataset_lock_s3::{OwnerState, S3Ownership};
    use object_store::ObjectStore;
    for registry in [false, true] {
        for fault in [0, 1, 2] {
            let fake = Arc::new(ArtifactStore::default());
            let store: Arc<dyn ObjectStore> = fake.clone();
            let guard = super::super::block_on_async(S3Ownership::acquire(
                store.clone(),
                "verify",
                vec!["".into()],
            ))
            .unwrap();
            let expected = guard.record().clone();
            let ownership = DatasetOwnership::from_remote_for_test("bucket", guard);
            fake.fault.store(fault, std::sync::atomic::Ordering::SeqCst);
            let location = object_store::path::Path::from(if registry {
                "registry.parquet"
            } else {
                "report.json"
            });
            let result = super::super::with_verify_ownership(Some(ownership), |_| {
                if registry {
                    super::super::commit_registry_to_store(
                        store.as_ref(),
                        &location,
                        &super::super::RegistrySnapshot::default(),
                        &[fill(registry_row("mainnet", "day=1", "aa"))],
                    )
                } else {
                    super::super::write_report_to_store(
                        store.as_ref(),
                        &location,
                        b"synthetic report",
                    )
                }
            });
            assert_eq!(result.is_ok(), fault == 0);
            assert_eq!(fake.writes.lock().unwrap().len(), 1);
            if registry {
                assert!(matches!(
                    fake.writes.lock().unwrap()[0],
                    object_store::PutMode::Create
                ));
            }
            let owner = super::super::block_on_async(S3Ownership::status(&store))
                .unwrap()
                .unwrap();
            if fault == 0 {
                assert_eq!(owner.state(), OwnerState::Released);
            } else {
                assert_eq!(owner, expected);
                assert!(super::super::block_on_async(S3Ownership::acquire(
                    store.clone(),
                    "verify",
                    vec!["".into()]
                ))
                .is_err());
            }
            // Losing the acknowledgement does not undo accepted publication.
            assert_eq!(
                super::super::block_on_async(store.get(&location)).is_ok(),
                fault != 2
            );
        }
    }
}

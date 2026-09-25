//! Hierarchical directory-inode ownership on supported local filesystems.

#[cfg(any(target_os = "linux", target_os = "macos"))]
mod supported {
    use anyhow::{bail, Context, Result};
    use std::collections::BTreeMap;
    use std::fs::{self, File};
    use std::os::unix::fs::MetadataExt;
    use std::path::{Component, Path, PathBuf};
    use std::sync::{Mutex, MutexGuard};

    #[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
    struct Identity(u64, u64);

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum Mode {
        Shared,
        Exclusive,
    }

    struct Requirement {
        path: PathBuf,
        mode: Mode,
    }

    struct Snapshot {
        roots: Vec<PathBuf>,
        directories: BTreeMap<Identity, Requirement>,
    }

    /// Holds exclusive directory locks at the reduced mutation roots and shared
    /// locks on their ancestors. Keep this value alive for the whole operation.
    ///
    /// Each supplied path is a directory scope; callers passing a file must
    /// instead supply its parent. Missing directories are durably created while
    /// their existing ancestors are protected. Acquisition is fail-fast: no
    /// write may proceed after a conflict or partial acquisition failure.
    pub struct LocalOwnership {
        roots: Vec<PathBuf>,
        scopes: Vec<PathBuf>,
        held: BTreeMap<Identity, (File, Mode)>,
        control_mutation: Mutex<()>,
    }

    impl LocalOwnership {
        /// Guard an intended output without creating it before validation.
        /// Missing roots conservatively own their nearest existing ancestor.
        /// All upgrades are calculated before acquiring any inode lock.
        pub fn acquire_without_creation(scopes: &[PathBuf]) -> Result<Self> {
            let absolute = scopes
                .iter()
                .map(|scope| {
                    if scope.is_absolute() {
                        Ok(scope.clone())
                    } else {
                        Ok(std::env::current_dir()?.join(scope))
                    }
                })
                .collect::<Result<Vec<_>>>()?;
            let mut plan = snapshot(&absolute)?;
            for root in &plan.roots {
                if root.is_dir() {
                    continue;
                }
                let ancestor = root
                    .ancestors()
                    .find(|path| path.is_dir())
                    .context("missing output has no existing directory ancestor")?;
                let identity = identity_of(&fs::metadata(ancestor)?);
                plan.directories
                    .get_mut(&identity)
                    .context("existing output ancestor was not included in ownership plan")?
                    .mode = Mode::Exclusive;
            }
            let mut guard = Self {
                roots: plan.roots.clone(),
                scopes: absolute.clone(),
                held: BTreeMap::new(),
                control_mutation: Mutex::new(()),
            };
            guard.acquire_missing(&plan)?;
            validate_handles(&guard, &plan)?;
            // Detect path/alias changes between planning and locking. Existing
            // roots may have appeared, but no path may resolve to another graph.
            if snapshot(&absolute)?.roots != plan.roots {
                bail!("ownership scope changed during acquisition; retry after stabilizing paths");
            }
            Ok(guard)
        }

        pub fn acquire(scopes: &[PathBuf]) -> Result<Self> {
            let absolute = scopes
                .iter()
                .map(|scope| {
                    if scope.is_absolute() {
                        Ok(scope.clone())
                    } else {
                        Ok(std::env::current_dir()?.join(scope))
                    }
                })
                .collect::<Result<Vec<_>>>()?;
            let before = snapshot(&absolute)?;
            let mut guard = Self {
                roots: before.roots.clone(),
                scopes: absolute.clone(),
                held: BTreeMap::new(),
                control_mutation: Mutex::new(()),
            };
            guard.acquire_missing(&before)?;
            validate_handles(&guard, &before)?;

            // Parents are already shared-locked, or covered by an exclusive
            // scope. Concurrent creation of the same empty directory is benign;
            // only the winner of its final exclusive lock may publish data.
            for scope in &absolute {
                fs::create_dir_all(scope)
                    .with_context(|| format!("creating ownership scope {}", scope.display()))?;
            }

            let after = snapshot(&absolute)?;
            if before.roots != after.roots {
                bail!("ownership scope changed while directories were being created; retry after stabilizing the paths");
            }
            guard.acquire_missing(&after)?;
            validate_handles(&guard, &after)?;
            for requirement in after.directories.values() {
                File::open(&requirement.path)
                    .and_then(|directory| directory.sync_all())
                    .with_context(|| {
                        format!("syncing ownership directory {}", requirement.path.display())
                    })?;
            }
            Ok(guard)
        }

        /// Recheck aliases and inode coverage before a command publishes data.
        /// Newly created directories remain covered by a held exclusive ancestor.
        pub fn revalidate(&self) -> Result<()> {
            let current = snapshot(&self.scopes)?;
            if current.roots != self.roots {
                bail!("local mutation scope changed after ownership acquisition");
            }
            for (identity, requirement) in &current.directories {
                if self.held.contains_key(identity) {
                    continue;
                }
                let mut covered = false;
                for ancestor in requirement.path.ancestors().skip(1) {
                    let metadata = fs::metadata(ancestor)?;
                    if self
                        .held
                        .get(&identity_of(&metadata))
                        .is_some_and(|(_, mode)| *mode == Mode::Exclusive)
                    {
                        covered = true;
                        break;
                    }
                }
                if !covered {
                    bail!("local mutation ancestry changed after ownership acquisition");
                }
            }
            Ok(())
        }

        /// Canonical roots after aliases and nested scopes have been reduced.
        pub fn roots(&self) -> &[PathBuf] {
            &self.roots
        }

        // Other stores may borrow this same OS guard. Serialize their version
        // validation and publication as one critical section too.
        pub(crate) fn lock_control_mutation(&self) -> Result<MutexGuard<'_, ()>> {
            self.control_mutation.lock().map_err(|_| {
                anyhow::anyhow!(
                    "control mutation lock was poisoned; stop and recover before continuing"
                )
            })
        }

        fn acquire_missing(&mut self, snapshot: &Snapshot) -> Result<()> {
            for (identity, requirement) in &snapshot.directories {
                if let Some((_, held_mode)) = self.held.get(identity) {
                    if *held_mode == Mode::Shared && requirement.mode == Mode::Exclusive {
                        bail!("ownership scope changed during acquisition; retry the complete scope set");
                    }
                    continue;
                }
                let file = File::open(&requirement.path).with_context(|| {
                    format!("opening ownership directory {}", requirement.path.display())
                })?;
                let metadata = file.metadata()?;
                if !metadata.is_dir() || identity_of(&metadata) != *identity {
                    bail!(
                        "ownership directory changed: {}",
                        requirement.path.display()
                    );
                }
                let locked = match requirement.mode {
                    Mode::Shared => file.try_lock_shared(),
                    Mode::Exclusive => file.try_lock(),
                };
                locked.with_context(|| {
                    format!(
                        "cannot acquire {:?} dataset ownership at {}; another mutation may be running, or this filesystem does not support directory locks",
                        requirement.mode,
                        requirement.path.display()
                    )
                })?;
                self.held.insert(*identity, (file, requirement.mode));
            }
            Ok(())
        }
    }

    fn identity_of(metadata: &fs::Metadata) -> Identity {
        Identity(metadata.dev(), metadata.ino())
    }

    fn canonical_scope(scope: &Path) -> Result<PathBuf> {
        let mut existing = scope;
        loop {
            match fs::canonicalize(existing) {
                Ok(mut resolved) => {
                    if !resolved.is_dir() {
                        bail!("ownership scope is not a directory: {}", existing.display());
                    }
                    for component in scope.strip_prefix(existing)?.components() {
                        match component {
                            Component::Normal(name) => resolved.push(name),
                            Component::CurDir => {}
                            _ => bail!(
                                "ambiguous missing ownership path {}; create or simplify its parent directories first",
                                scope.display()
                            ),
                        }
                    }
                    return Ok(resolved);
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    existing = existing
                        .parent()
                        .with_context(|| format!("no existing ancestor for {}", scope.display()))?;
                }
                Err(error) => {
                    return Err(error).with_context(|| {
                        format!("resolving ownership directory {}", existing.display())
                    });
                }
            }
        }
    }

    fn snapshot(scopes: &[PathBuf]) -> Result<Snapshot> {
        let mut roots = scopes
            .iter()
            .map(|scope| canonical_scope(scope))
            .collect::<Result<Vec<_>>>()?;
        roots.sort();
        roots.dedup();
        let mut reduced: Vec<PathBuf> = Vec::new();
        for root in roots {
            if !reduced.iter().any(|parent| root.starts_with(parent)) {
                reduced.push(root);
            }
        }
        let mut directories: BTreeMap<Identity, Requirement> = BTreeMap::new();
        // Retain lexical ancestry too: deleting an alias from its containing
        // directory is a mutation even when the target inode remains unchanged.
        for root in reduced.iter().chain(scopes) {
            for ancestor in root.ancestors() {
                let canonical = match fs::canonicalize(ancestor) {
                    Ok(path) => path,
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                    Err(error) => return Err(error).context("resolving ownership ancestry"),
                };
                let metadata = fs::metadata(&canonical)?;
                if !metadata.is_dir() {
                    bail!(
                        "ownership ancestor is not a directory: {}",
                        canonical.display()
                    );
                }
                let mode = if reduced.contains(&canonical) {
                    Mode::Exclusive
                } else {
                    Mode::Shared
                };
                let entry = directories
                    .entry(identity_of(&metadata))
                    .or_insert(Requirement {
                        path: canonical,
                        mode,
                    });
                if mode == Mode::Exclusive {
                    entry.mode = mode;
                }
            }
        }
        Ok(Snapshot {
            roots: reduced,
            directories,
        })
    }

    fn validate_handles(guard: &LocalOwnership, snapshot: &Snapshot) -> Result<()> {
        for (identity, requirement) in &snapshot.directories {
            let metadata = fs::metadata(&requirement.path).with_context(|| {
                format!("rechecking ownership path {}", requirement.path.display())
            })?;
            if identity_of(&metadata) != *identity || !guard.held.contains_key(identity) {
                bail!(
                    "ownership directory changed: {}",
                    requirement.path.display()
                );
            }
        }
        Ok(())
    }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
pub use supported::LocalOwnership;

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
pub struct LocalOwnership;

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
impl LocalOwnership {
    pub fn acquire_without_creation(scopes: &[std::path::PathBuf]) -> anyhow::Result<Self> {
        Self::acquire(scopes)
    }

    pub fn acquire(_scopes: &[std::path::PathBuf]) -> anyhow::Result<Self> {
        anyhow::bail!(
            "local dataset ownership requires supported macOS/Linux directory inode locks"
        )
    }

    pub fn revalidate(&self) -> anyhow::Result<()> {
        anyhow::bail!("local dataset ownership is unsupported on this platform")
    }

    pub fn roots(&self) -> &[std::path::PathBuf] {
        &[]
    }

    pub(crate) fn lock_control_mutation(&self) -> anyhow::Result<std::sync::MutexGuard<'_, ()>> {
        anyhow::bail!("local dataset ownership is unsupported on this platform")
    }
}

#[cfg(all(test, any(target_os = "linux", target_os = "macos")))]
mod tests {
    use super::LocalOwnership;
    use std::fs;
    use std::os::unix::fs::symlink;
    use std::path::PathBuf;
    use std::process::{Command, Stdio};
    use std::time::{Duration, Instant};

    const CHILD_TEST: &str = "dataset_lock::local::tests::ownership_child";

    fn child(scope: &std::path::Path, expected: &str) -> Command {
        let mut command = Command::new(std::env::current_exe().unwrap());
        command
            .args(["--exact", CHILD_TEST, "--ignored", "--nocapture"])
            .env("FIREPARQ_OWNERSHIP_TEST_SCOPE", scope)
            .env("FIREPARQ_OWNERSHIP_TEST_EXPECTED", expected)
            .stdout(Stdio::null());
        command
    }

    #[test]
    #[ignore = "subprocess entry point, invoked by the ownership tests"]
    fn ownership_child() {
        let scope = PathBuf::from(std::env::var_os("FIREPARQ_OWNERSHIP_TEST_SCOPE").unwrap());
        let expected = std::env::var("FIREPARQ_OWNERSHIP_TEST_EXPECTED").unwrap();
        let acquired = LocalOwnership::acquire(&[scope]);
        if expected == "blocked" {
            assert!(
                acquired.is_err(),
                "overlapping ownership unexpectedly succeeded"
            );
            return;
        }
        let _held = acquired.unwrap();
        if expected == "hold" {
            let ready = std::env::var_os("FIREPARQ_OWNERSHIP_TEST_READY").unwrap();
            fs::write(ready, b"ready").unwrap();
            loop {
                std::thread::park();
            }
        }
    }

    #[test]
    fn overlapping_scopes_conflict_but_siblings_can_progress() {
        let dir = tempfile::tempdir().unwrap();
        let parent = dir.path().join("data");
        let first = parent.join("first");
        let nested = first.join("table");
        fs::create_dir_all(&nested).unwrap();
        let guard = LocalOwnership::acquire(&[first.clone()]).unwrap();
        for scope in [&parent, &first, &nested] {
            assert!(child(scope, "blocked").status().unwrap().success());
        }
        assert!(child(&parent.join("second"), "free")
            .status()
            .unwrap()
            .success());
        drop(guard);
        assert!(child(&first, "free").status().unwrap().success());
    }

    #[test]
    fn nested_alias_and_external_cursor_scopes_do_not_self_deadlock() {
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().join("data");
        let state = dir.path().join("state");
        fs::create_dir_all(&data).unwrap();
        let alias = dir.path().join("alias");
        symlink(&data, &alias).unwrap();
        let guard = LocalOwnership::acquire(&[
            data.clone(),
            data.join("new/nested"),
            alias.join("new"),
            state.clone(),
        ])
        .unwrap();
        assert_eq!(guard.roots().len(), 2);
        assert!(child(&alias, "blocked").status().unwrap().success());
        assert!(child(&state, "blocked").status().unwrap().success());
        assert!(data.join("new/nested").is_dir());
    }

    #[test]
    fn failed_acquisition_releases_its_partial_scope_set() {
        let dir = tempfile::tempdir().unwrap();
        let first = dir.path().join("a");
        let second = dir.path().join("b");
        let held = LocalOwnership::acquire(&[second.clone()]).unwrap();
        assert!(LocalOwnership::acquire(&[first.clone(), second]).is_err());
        assert!(child(&first, "free").status().unwrap().success());
        drop(held);
    }

    #[test]
    fn alias_parent_is_protected_alongside_target_ancestry() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("target/data");
        let aliases = dir.path().join("aliases");
        fs::create_dir_all(&target).unwrap();
        fs::create_dir_all(&aliases).unwrap();
        let alias = aliases.join("data");
        symlink(&target, &alias).unwrap();
        let _guard = LocalOwnership::acquire(&[alias]).unwrap();
        assert!(child(&aliases, "blocked").status().unwrap().success());
        assert!(child(&dir.path().join("target"), "blocked")
            .status()
            .unwrap()
            .success());
    }

    #[test]
    fn owner_process_death_releases_directory_inode_locks() {
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().join("data");
        let ready = dir.path().join("ready");
        let mut owner = child(&data, "hold")
            .env("FIREPARQ_OWNERSHIP_TEST_READY", &ready)
            .spawn()
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        while !ready.exists() {
            assert!(
                owner.try_wait().unwrap().is_none(),
                "owner exited before locking"
            );
            if Instant::now() > deadline {
                owner.kill().unwrap();
                owner.wait().unwrap();
                panic!("owner did not acquire its lock");
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        assert!(LocalOwnership::acquire(&[data.clone()]).is_err());
        owner.kill().unwrap();
        owner.wait().unwrap();
        let _recovered = LocalOwnership::acquire(&[data]).unwrap();
    }
}

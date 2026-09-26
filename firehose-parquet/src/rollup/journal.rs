//! Crash safety for `rollup`, following the merge journal ([`crate::merge_journal`]).
//!
//! Rolling up a target partition writes new parts and then removes earlier copies or deletes
//! the sources. A crash in between used to leave both, and the next run rolled the sources up
//! again next to the orphaned outputs, so their rows were stored twice for good. Each target
//! partition is now journaled in `_fireparq_rollup.json` in its output directory:
//!
//! 1. The journal is created exclusively with state `writing`, the run id, the source root and
//!    every source path before any output is written.
//! 2. The outputs are written (locally to an fsynced temporary file, then linked into place)
//!    and the output directory is synced.
//! 3. The journal is rewritten with state `committed` and the output names.
//! 4. Earlier copies are removed, and with `--delete-source` the sources are deleted and their
//!    directories synced. Then the journal is removed.
//!
//! The next rollup of the output finishes or undoes interrupted partitions before it discovers
//! sources: a `writing` journal is rolled back (that run's outputs are deleted; the sources were
//! never touched), and a `committed` one is rolled forward (earlier copies are removed and the
//! remaining recorded sources deleted). `merge` refuses partitions at or below a directory that
//! still holds a rollup journal, because renaming them would hide outputs or sources from the
//! recovery.

use crate::merge_journal::JournalState;
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

/// Journal file name inside a rollup output directory.
pub(crate) const ROLLUP_JOURNAL_FILE: &str = "_fireparq_rollup.json";

/// How errors name the journal.
pub(super) const WHAT: &str = "rollup journal";

const VERSION: u32 = 1;

/// The record of one target partition's rollup.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct RollupJournal {
    pub version: u32,
    /// The run's output-name id: outputs are `part-<run_id>-*` or `part-rollup-<run_id>-*`.
    pub run_id: String,
    pub started_at: String,
    pub state: JournalState,
    /// Written without `--delete-source`: outputs are copies and sources are kept.
    pub copy: bool,
    /// Root that `sources` are relative to (a canonical local path or `s3://bucket/prefix`).
    pub source_root: String,
    /// Every source file of the partition, relative to `source_root`.
    pub sources: Vec<String>,
    /// Output file names in the output directory, recorded at commit.
    #[serde(default)]
    pub outputs: Vec<String>,
}

impl RollupJournal {
    pub(super) fn writing(
        run_id: &str,
        copy: bool,
        source_root: &str,
        sources: Vec<String>,
    ) -> Self {
        Self {
            version: VERSION,
            run_id: run_id.to_string(),
            started_at: time::OffsetDateTime::now_utc()
                .format(&time::format_description::well_known::Rfc3339)
                .unwrap_or_default(),
            state: JournalState::Writing,
            copy,
            source_root: source_root.to_string(),
            sources,
            outputs: Vec::new(),
        }
    }

    pub(super) fn committed(&self, outputs: Vec<String>) -> Self {
        Self {
            state: JournalState::Committed,
            outputs,
            ..self.clone()
        }
    }

    /// File-name prefix of this run's outputs.
    pub(super) fn output_prefix(&self) -> String {
        output_prefix(&self.run_id, self.copy)
    }

    /// Whether `name` in the output directory was written, or was being written, by this run.
    pub(super) fn is_run_file(&self, name: &str) -> bool {
        (name.starts_with(&self.output_prefix()) && name.ends_with(".parquet"))
            || (name.starts_with('.') && name.ends_with(&format!(".{}.tmp", self.run_id)))
    }

    pub(super) fn encode(&self) -> Result<Vec<u8>> {
        self.validate()?;
        let bytes = serde_json::to_vec_pretty(self)?;
        anyhow::ensure!(
            bytes.len() <= crate::durable_state::MAX_CONTROL_BYTES,
            "the rollup journal of {} source file(s) exceeds the control-record byte limit; \
             roll up to a finer target first",
            self.sources.len()
        );
        Ok(bytes)
    }

    pub(super) fn decode(data: &[u8], location: &str) -> Result<Self> {
        anyhow::ensure!(
            data.len() <= crate::durable_state::MAX_CONTROL_BYTES,
            "rollup journal exceeds the control-record byte limit"
        );
        let journal: Self = serde_json::from_slice(data)
            .with_context(|| format!("parsing rollup journal {location}"))?;
        anyhow::ensure!(
            journal.version == VERSION,
            "rollup journal {location} has version {}, but this build understands {VERSION}",
            journal.version
        );
        journal
            .validate()
            .with_context(|| format!("rollup journal {location}"))?;
        Ok(journal)
    }

    fn validate(&self) -> Result<()> {
        anyhow::ensure!(
            !self.run_id.is_empty()
                && self.run_id.len() <= 64
                && self
                    .run_id
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
                && self.started_at.len() <= 64,
            "rollup journal identity fields are invalid"
        );
        anyhow::ensure!(
            !self.source_root.is_empty()
                && self.source_root.len() <= 4096
                && !self.source_root.contains('\0'),
            "rollup journal source root is invalid"
        );
        anyhow::ensure!(
            match self.state {
                JournalState::Writing => self.outputs.is_empty(),
                JournalState::Committed => !self.outputs.is_empty(),
            } && !self.sources.is_empty(),
            "rollup journal phase disagrees with its inventory"
        );
        let prefix = self.output_prefix();
        anyhow::ensure!(
            self.outputs.iter().all(|name| {
                name.len() <= 255
                    && !name.contains(['/', '\\', '\0'])
                    && name.starts_with(&prefix)
                    && name.ends_with(".parquet")
            }),
            "rollup journal records outputs this run could not have written"
        );
        anyhow::ensure!(
            self.sources.iter().all(|source| valid_source(source)),
            "rollup journal records an invalid source path"
        );
        let unique_sources: HashSet<_> = self.sources.iter().collect();
        let unique_outputs: HashSet<_> = self.outputs.iter().collect();
        anyhow::ensure!(
            unique_sources.len() == self.sources.len()
                && unique_outputs.len() == self.outputs.len(),
            "rollup journal records duplicate files"
        );
        Ok(())
    }
}

/// File-name prefix of the outputs of run `run_id`.
pub(super) fn output_prefix(run_id: &str, copy: bool) -> String {
    if copy {
        format!("{}{run_id}-", super::COPY_OUTPUT_PREFIX)
    } else {
        format!("part-{run_id}-")
    }
}

/// A relative, normalized path to a Parquet data file below the source root.
fn valid_source(source: &str) -> bool {
    !source.is_empty()
        && source.len() <= 4096
        && !source.contains(['\0', '\\'])
        && source.ends_with(".parquet")
        && source
            .split('/')
            .all(|part| !part.is_empty() && part != "." && part != "..")
        && !crate::artifacts::is_reserved_artifact_path(source)
        && !crate::artifacts::is_control_path(source)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn journal() -> RollupJournal {
        RollupJournal::writing(
            "0a1b2c3d",
            false,
            "/data/mainnet",
            vec![
                "blocks/day=01/hour=00/part-000001.parquet".into(),
                "blocks/day=01/hour=01/part-000001.parquet".into(),
            ],
        )
    }

    #[test]
    fn round_trips_and_tracks_this_runs_files() {
        let writing = journal();
        let decoded = RollupJournal::decode(&writing.encode().unwrap(), "test").unwrap();
        assert_eq!(decoded, writing);
        let committed = writing.committed(vec!["part-0a1b2c3d-000001.parquet".into()]);
        assert_eq!(
            RollupJournal::decode(&committed.encode().unwrap(), "test").unwrap(),
            committed
        );
        assert!(committed.is_run_file("part-0a1b2c3d-000002.parquet"));
        assert!(committed.is_run_file(".part-0a1b2c3d-000002.parquet.0a1b2c3d.tmp"));
        assert!(!committed.is_run_file("part-ffffffff-000001.parquet"));
        assert!(!committed.is_run_file("part-rollup-0a1b2c3d-000001.parquet"));
        assert!(!committed.is_run_file("part-000001.parquet"));
        let copy = RollupJournal {
            copy: true,
            ..journal()
        };
        assert!(copy.is_run_file("part-rollup-0a1b2c3d-000001.parquet"));
        assert!(!copy.is_run_file("part-0a1b2c3d-000001.parquet"));
    }

    #[test]
    fn rejects_inconsistent_or_unsafe_records() {
        let mut cases = Vec::new();
        cases.push(journal().committed(Vec::new()));
        cases.push(RollupJournal {
            outputs: vec!["part-0a1b2c3d-000001.parquet".into()],
            ..journal()
        });
        cases.push(journal().committed(vec!["part-ffffffff-000001.parquet".into()]));
        cases.push(journal().committed(vec!["../part-0a1b2c3d-000001.parquet".into()]));
        for source in [
            "/abs/part.parquet",
            "blocks/../other/part.parquet",
            "blocks//part.parquet",
            "blocks/part.json",
            "cursor.parquet",
            ".fireparq-ingest/state.parquet",
        ] {
            cases.push(RollupJournal {
                sources: vec![source.into()],
                ..journal()
            });
        }
        cases.push(RollupJournal {
            sources: Vec::new(),
            ..journal()
        });
        cases.push(RollupJournal {
            run_id: "bad id".into(),
            ..journal()
        });
        cases.push(RollupJournal {
            source_root: String::new(),
            ..journal()
        });
        let duplicate = journal().sources[0].clone();
        cases.push(RollupJournal {
            sources: vec![duplicate.clone(), duplicate],
            ..journal()
        });
        for case in cases {
            assert!(case.encode().is_err(), "{case:?}");
            let bytes = serde_json::to_vec(&case).unwrap();
            assert!(RollupJournal::decode(&bytes, "test").is_err(), "{case:?}");
        }
        let mut unknown: serde_json::Value = serde_json::to_value(journal()).unwrap();
        unknown["extra"] = serde_json::Value::Bool(true);
        assert!(RollupJournal::decode(&serde_json::to_vec(&unknown).unwrap(), "test").is_err());
        let mut future: serde_json::Value = serde_json::to_value(journal()).unwrap();
        future["version"] = serde_json::Value::from(2);
        assert!(RollupJournal::decode(&serde_json::to_vec(&future).unwrap(), "test").is_err());
    }
}

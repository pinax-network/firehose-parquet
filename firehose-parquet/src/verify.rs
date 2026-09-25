use crate::artifacts::{is_reserved_artifact_path, MERKLE_ROOTS_FILENAME, VERIFY_RUNS_DIR};
use crate::cli::{block_on_async, resolve_parquet_input_path_string, AwsConfig};
use crate::cursor::{parse_cursor, CURSOR_PARQUET_FILENAME};
use crate::dataset_lock::{DatasetOwnership, MutationScope};
use crate::writer::parse_s3_url;
use anyhow::{anyhow, Context, Result};
use arrow::array::{Array, AsArray, Int64Array, LargeStringArray, StringArray, UInt64Array};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use object_store::ObjectStore;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::arrow::ProjectionMask;
use serde::Serialize;
use sha2::{Digest as ShaDigest, Sha256};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;
use tiny_keccak::{Hasher, Keccak};

mod row_encoding;

const VERIFY_REPORT_SCHEMA_VERSION: &str = "2.0.0";

/// Row-to-root construction recorded as `merkle_version` in the registry and
/// report. Bump it whenever row encoding or tree construction changes, so roots
/// computed by an older algorithm are detected instead of silently compared.
const MERKLE_VERSION: &str = "merkle_v2";
/// Version assumed for registry files written before `merkle_version` existed.
const LEGACY_MERKLE_VERSION: &str = "merkle_v1";

/// Domain-separation prefixes for `merkle_v2` leaf, interior node, and
/// row-count commitment hashes.
const MERKLE_LEAF_PREFIX: u8 = 0x00;
const MERKLE_NODE_PREFIX: u8 = 0x01;
const MERKLE_ROOT_PREFIX: u8 = 0x02;

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum HashStrategy {
    Keccak256,
    Sha256,
}

impl HashStrategy {
    fn as_str(self) -> &'static str {
        match self {
            HashStrategy::Keccak256 => "keccak256",
            HashStrategy::Sha256 => "sha256",
        }
    }

    fn hash(self, bytes: &[u8]) -> [u8; 32] {
        match self {
            HashStrategy::Keccak256 => {
                let mut output = [0u8; 32];
                let mut hasher = Keccak::v256();
                hasher.update(bytes);
                hasher.finalize(&mut output);
                output
            }
            HashStrategy::Sha256 => {
                let digest = Sha256::digest(bytes);
                let mut output = [0u8; 32];
                output.copy_from_slice(&digest);
                output
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, clap::ValueEnum, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum VerifyCheck {
    Roots,
    Protocol,
    Continuity,
    Completeness,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum VerifyProfile {
    Quick,
    Standard,
    Deep,
}

impl VerifyProfile {
    fn default_checks(self) -> BTreeSet<VerifyCheck> {
        let mut checks = BTreeSet::new();
        checks.insert(VerifyCheck::Roots);
        match self {
            VerifyProfile::Quick => checks,
            VerifyProfile::Standard => {
                checks.insert(VerifyCheck::Protocol);
                checks
            }
            VerifyProfile::Deep => {
                checks.insert(VerifyCheck::Protocol);
                checks.insert(VerifyCheck::Continuity);
                checks.insert(VerifyCheck::Completeness);
                checks
            }
        }
    }
}

fn parse_hash_strategy(value: &str) -> Result<Option<HashStrategy>> {
    let lowered = value.to_ascii_lowercase();
    match lowered.as_str() {
        "auto" => Ok(None),
        "keccak256" => Ok(Some(HashStrategy::Keccak256)),
        "sha256" => Ok(Some(HashStrategy::Sha256)),
        other => Err(anyhow!(
            "invalid hash strategy '{other}': expected auto, keccak256, or sha256"
        )),
    }
}

fn default_hash_strategy_for_chain(chain: &str) -> HashStrategy {
    match chain.to_ascii_lowercase().as_str() {
        "evm" => HashStrategy::Keccak256,
        "bitcoin" | "solana" => HashStrategy::Sha256,
        _ => HashStrategy::Sha256,
    }
}

fn resolve_hash_strategy(chain: &str, configured: Option<&str>) -> Result<HashStrategy> {
    if let Some(raw) = configured {
        if let Some(parsed) = parse_hash_strategy(raw)? {
            return Ok(parsed);
        }
    }
    Ok(default_hash_strategy_for_chain(chain))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum VerifyScope {
    Chain,
    Table,
    Partition,
    Run,
}

#[derive(Debug, Clone)]
pub struct VerifyOptions {
    /// Chain family (`evm`, `bitcoin`, ...). `None` infers it from the
    /// `firehose-parquet.block_type` file metadata.
    pub chain: Option<String>,
    /// Table name. `None` infers it from the `<chain_root>/<table>/...` layout.
    pub table: Option<String>,
    pub hash_strategy: Option<String>,
    pub checks: Vec<VerifyCheck>,
    pub profile: VerifyProfile,
    pub scope: VerifyScope,
    pub no_fail_fast: bool,
    pub report_json: Option<PathBuf>,
    pub publish_report: bool,
    pub publish_report_path: Option<String>,
    pub registry_path: Option<String>,
    pub update_registry: bool,
}

impl VerifyOptions {
    pub fn effective_checks(&self) -> BTreeSet<VerifyCheck> {
        if self.checks.is_empty() {
            return self.profile.default_checks();
        }

        self.checks.iter().copied().collect()
    }

    fn runs_roots(&self) -> bool {
        self.effective_checks().contains(&VerifyCheck::Roots)
    }

    fn runs_protocol(&self) -> bool {
        self.effective_checks().contains(&VerifyCheck::Protocol)
    }
}

fn verify_check_name(check: VerifyCheck) -> &'static str {
    match check {
        VerifyCheck::Roots => "roots",
        VerifyCheck::Protocol => "protocol",
        VerifyCheck::Continuity => "continuity",
        VerifyCheck::Completeness => "completeness",
    }
}

fn verify_profile_name(profile: VerifyProfile) -> &'static str {
    match profile {
        VerifyProfile::Quick => "quick",
        VerifyProfile::Standard => "standard",
        VerifyProfile::Deep => "deep",
    }
}

fn verify_scope_name(scope: VerifyScope) -> &'static str {
    match scope {
        VerifyScope::Chain => "chain",
        VerifyScope::Table => "table",
        VerifyScope::Partition => "partition",
        VerifyScope::Run => "run",
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FindingStatus {
    Match,
    MissingExpected,
    Mismatch,
    /// The registry root differed and `--update-registry` replaced it.
    Updated,
    /// The partition may still receive rows from `build`; not compared or recorded.
    Open,
}

#[derive(Debug, Clone, Serialize)]
pub struct VerifyFinding {
    pub chain: String,
    pub table: String,
    pub partition: String,
    pub status: FindingStatus,
    pub expected_root: Option<String>,
    pub computed_root: String,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct VerifySummary {
    pub partitions_scanned: usize,
    pub matches: usize,
    pub missing_expected: usize,
    pub mismatches: usize,
    pub updated: usize,
    pub open_partitions: usize,
    pub protocol_passed: usize,
    pub protocol_failed: usize,
    pub protocol_not_verifiable: usize,
    pub capability_not_verifiable: usize,
    pub wrote_registry: bool,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CapabilityStatus {
    NotVerifiable,
}

#[derive(Debug, Clone, Serialize)]
pub struct CapabilityFinding {
    pub chain: String,
    pub table: String,
    pub check: VerifyCheck,
    pub status: CapabilityStatus,
    pub details: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProtocolCheckStatus {
    Pass,
    Fail,
    NotVerifiable,
}

#[derive(Debug, Clone, Serialize)]
pub struct ProtocolCheckFinding {
    pub chain: String,
    pub table: String,
    pub partition: String,
    pub check: String,
    pub status: ProtocolCheckStatus,
    pub block_num: Option<u64>,
    pub details: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct VerifyReport {
    pub report_schema_version: String,
    pub run_id: String,
    pub started_at: String,
    pub finished_at: String,
    pub duration_ms: u64,
    pub tool_version: String,
    pub chain: String,
    pub table: String,
    pub network: Option<String>,
    pub scope: VerifyScope,
    pub requested_checks: Vec<VerifyCheck>,
    pub effective_checks: Vec<VerifyCheck>,
    pub profile: VerifyProfile,
    pub data_path: String,
    pub registry_path: String,
    pub suggested_run_report_path: String,
    pub report_json_path: Option<String>,
    pub published_report_path: Option<String>,
    pub algorithm: String,
    pub merkle_version: String,
    pub warnings: Vec<String>,
    pub summary: VerifySummary,
    pub findings: Vec<VerifyFinding>,
    pub protocol_findings: Vec<ProtocolCheckFinding>,
    pub capability_findings: Vec<CapabilityFinding>,
}

impl VerifyReport {
    pub fn is_valid(&self) -> bool {
        self.summary.mismatches == 0 && self.summary.protocol_failed == 0
    }

    pub fn print(&self) {
        println!("Verifying {}:{}", self.chain, self.table);
        if let Some(ref network) = self.network {
            println!("  network:       {}", network);
        }
        println!("  report schema: {}", self.report_schema_version);
        println!("  run id:        {}", self.run_id);
        println!("  started at:    {}", self.started_at);
        println!("  finished at:   {}", self.finished_at);
        println!("  duration ms:   {}", self.duration_ms);
        println!("  tool version:  {}", self.tool_version);
        println!("  scope:         {}", verify_scope_name(self.scope));
        println!("  profile:       {}", verify_profile_name(self.profile));
        println!(
            "  checks:        {}",
            self.effective_checks
                .iter()
                .map(|check| verify_check_name(*check).to_string())
                .collect::<Vec<_>>()
                .join(",")
        );
        println!("  data path:     {}", self.data_path);
        println!("  registry path: {}", self.registry_path);
        println!("  suggested rpt: {}", self.suggested_run_report_path);
        if let Some(ref report_path) = self.report_json_path {
            println!("  report json:   {}", report_path);
        }
        if let Some(ref report_path) = self.published_report_path {
            println!("  published rpt: {}", report_path);
        }
        println!("  algorithm:     {}", self.algorithm);
        println!("  merkle ver:    {}", self.merkle_version);
        println!("  partitions:    {}", self.summary.partitions_scanned);
        println!("  matches:       {}", self.summary.matches);
        println!("  missing roots: {}", self.summary.missing_expected);
        println!("  mismatches:    {}", self.summary.mismatches);
        println!("  updated:       {}", self.summary.updated);
        println!("  open parts:    {}", self.summary.open_partitions);
        println!("  protocol pass: {}", self.summary.protocol_passed);
        println!("  protocol fail: {}", self.summary.protocol_failed);
        println!("  protocol n/v:  {}", self.summary.protocol_not_verifiable);
        println!(
            "  capability n/v:{}",
            self.summary.capability_not_verifiable
        );
        println!(
            "  registry write:{}",
            if self.summary.wrote_registry {
                " yes"
            } else {
                " no"
            }
        );

        if !self.warnings.is_empty() {
            println!("\nWarnings:");
            for warning in &self.warnings {
                println!("  - {}", warning);
            }
        }

        if self.findings.is_empty() {
            return;
        }

        println!("\nFindings:");
        for f in &self.findings {
            let status = match f.status {
                FindingStatus::Match => "match",
                FindingStatus::MissingExpected => "missing_expected",
                FindingStatus::Mismatch => "mismatch",
                FindingStatus::Updated => "updated",
                FindingStatus::Open => "open",
            };
            println!("  - partition={} status={}", f.partition, status);
            if let Some(ref expected) = f.expected_root {
                println!("    expected={}", expected);
            }
            println!("    computed={}", f.computed_root);
            if let Some(ref err) = f.error {
                println!("    error={}", err);
            }
        }

        if !self.protocol_findings.is_empty() {
            println!("\nProtocol checks:");
            for p in &self.protocol_findings {
                let status = match p.status {
                    ProtocolCheckStatus::Pass => "pass",
                    ProtocolCheckStatus::Fail => "fail",
                    ProtocolCheckStatus::NotVerifiable => "not_verifiable",
                };
                println!(
                    "  - partition={} check={} status={}",
                    p.partition, p.check, status
                );
                if let Some(block_num) = p.block_num {
                    println!("    block_num={}", block_num);
                }
                println!("    details={}", p.details);
            }
        }

        if !self.capability_findings.is_empty() {
            println!("\nCapability checks:");
            for c in &self.capability_findings {
                println!(
                    "  - check={} status=not_verifiable",
                    verify_check_name(c.check)
                );
                println!("    details={}", c.details);
            }
        }
    }
}

#[derive(Debug, Clone)]
struct ScanOutput {
    target: Target,
    partition_roots: BTreeMap<String, String>,
    /// Partitions read in full (also counted when roots are not computed).
    partitions_scanned: usize,
    protocol_findings: Vec<ProtocolCheckFinding>,
    /// Highest `block_num` per partition, to find partitions still being written.
    partition_max_block: HashMap<String, u64>,
    /// Partition whose files were only partly read when fail-fast stopped the scan.
    truncated_partition: Option<String>,
}

/// How a scanned partition's root relates to the registry.
enum RootOutcome {
    Open(String),
    Missing,
    Match(String),
    Differs {
        reason: Option<String>,
        key: String,
        row: Box<RegistryRow>,
    },
}

#[derive(Debug, Clone, Default)]
struct ProtocolPartitionState {
    hash_matches_block_id: CheckAccumulator,
    parent_hash_matches_parent_id: CheckAccumulator,
    number_matches_block_num: CheckAccumulator,
    transactions_block_number_matches_block_num: CheckAccumulator,
    logs_block_number_matches_block_num: CheckAccumulator,
    calls_block_number_matches_block_num: CheckAccumulator,
}

#[derive(Debug, Clone, Default)]
struct CheckAccumulator {
    evaluated: u64,
    first_failure: Option<(u64, String)>,
    not_verifiable_reason: Option<String>,
}

impl CheckAccumulator {
    fn mark_not_verifiable(&mut self, reason: impl Into<String>) {
        if self.not_verifiable_reason.is_none() {
            self.not_verifiable_reason = Some(reason.into());
        }
    }

    fn observe(&mut self, ok: bool, block_num: u64, details: impl Into<String>) {
        self.evaluated += 1;
        if !ok && self.first_failure.is_none() {
            self.first_failure = Some((block_num, details.into()));
        }
    }
}

#[derive(Debug, Clone)]
struct RegistryRow {
    network: String,
    chain: String,
    table: String,
    partition: String,
    algorithm: String,
    merkle_version: String,
    merkle_root: String,
    updated_at: String,
}

pub fn verify_parquet(
    path: &str,
    aws: Option<&AwsConfig>,
    opts: &VerifyOptions,
) -> Result<VerifyReport> {
    let resolved_path = resolve_parquet_input_path_string(path);
    let run_started = OffsetDateTime::now_utc();
    let run_id = uuid::Uuid::new_v4().to_string();
    // Validate options before discovery or persistent ownership acquisition.
    if let Some(raw) = opts.hash_strategy.as_deref() {
        parse_hash_strategy(raw)?;
    }
    let plan = if opts.runs_roots()
        || opts.report_json.is_some()
        || opts.publish_report
        || opts.publish_report_path.is_some()
    {
        Some(VerifyMutationPlan::discover(
            &resolved_path,
            aws,
            opts,
            &run_id,
        )?)
    } else {
        // Protocol-only verification without artifact output remains read-only,
        // including access to public S3 buckets without write credentials.
        None
    };
    verify_with_plan(resolved_path, aws, opts, run_started, run_id, plan)
}

struct VerifyMutationPlan {
    chain_root: String,
    scopes: Vec<MutationScope>,
}

impl VerifyMutationPlan {
    fn discover(
        path: &str,
        aws: Option<&AwsConfig>,
        opts: &VerifyOptions,
        run_id: &str,
    ) -> Result<Self> {
        // Only discover paths here. Rows, footers, roots and registry contents
        // are authoritatively re-read after every source/destination is owned.
        let first = if path.starts_with("s3://") {
            let aws = aws.context("AWS config required for S3 paths")?;
            let (bucket, _, _, objects) = list_verify_objects(path, aws)?;
            format!("s3://{bucket}/{}", objects[0].location)
        } else {
            list_verify_files(path)?.1[0].clone()
        };
        let chain_root = file_layout(&first).chain_root;
        let mut scopes = vec![MutationScope::input(path)?];
        if opts.runs_roots() {
            // Missing roots are inserted even without --update-registry.
            scopes.push(MutationScope::file(
                opts.registry_path
                    .clone()
                    .unwrap_or_else(|| join_artifact_path(&chain_root, MERKLE_ROOTS_FILENAME)),
            ));
        }
        if let Some(path) = &opts.report_json {
            // This option is a local PathBuf, even if its literal spelling
            // begins with "s3://". Resolve it locally before scope routing.
            let local = if path.is_absolute() {
                path.clone()
            } else {
                std::env::current_dir()?.join(path)
            };
            scopes.push(MutationScope::file(local.to_string_lossy()));
        }
        if opts.publish_report || opts.publish_report_path.is_some() {
            scopes.push(MutationScope::file(
                opts.publish_report_path.clone().unwrap_or_else(|| {
                    join_artifact_path(
                        &chain_root,
                        &format!("{VERIFY_RUNS_DIR}/{run_id}/report.json"),
                    )
                }),
            ));
        }
        Ok(Self { chain_root, scopes })
    }
}

fn verify_with_plan(
    resolved_path: String,
    aws: Option<&AwsConfig>,
    opts: &VerifyOptions,
    run_started: OffsetDateTime,
    run_id: String,
    plan: Option<VerifyMutationPlan>,
) -> Result<VerifyReport> {
    let (ownership, expected_root) = match plan {
        Some(plan) => (
            Some(DatasetOwnership::acquire_blocking(
                "verify",
                plan.scopes,
                aws,
            )?),
            Some(plan.chain_root),
        ),
        None => (None, None),
    };
    with_verify_ownership(ownership, |ownership| {
        verify_owned(
            resolved_path,
            aws,
            opts,
            run_started,
            run_id,
            expected_root.as_deref(),
            ownership,
        )
    })
}

fn with_verify_ownership<T>(
    ownership: Option<DatasetOwnership>,
    operation: impl FnOnce(Option<&DatasetOwnership>) -> Result<T>,
) -> Result<T> {
    match operation(ownership.as_ref()) {
        Ok(report) => {
            if let Some(ownership) = ownership {
                ownership.release_blocking()?;
            }
            Ok(report)
        }
        Err(error) => {
            // An error may follow an accepted remote write. Never release or
            // replace an uncertain owner merely because a later read succeeds.
            if let Some(ownership) = &ownership {
                ownership.mark_remote_mutations_uncertain();
            }
            Err(error)
        }
    }
}

fn verify_owned(
    resolved_path: String,
    aws: Option<&AwsConfig>,
    opts: &VerifyOptions,
    run_started: OffsetDateTime,
    run_id: String,
    expected_root: Option<&str>,
    ownership: Option<&DatasetOwnership>,
) -> Result<VerifyReport> {
    let effective_checks = opts.effective_checks();
    let runs_roots = opts.runs_roots();
    let runs_protocol = opts.runs_protocol();
    // Fail on a bad --hash-strategy before scanning.
    if let Some(raw) = opts.hash_strategy.as_deref() {
        parse_hash_strategy(raw)?;
    }

    let scan_output = if resolved_path.starts_with("s3://") {
        let aws = aws.ok_or_else(|| anyhow!("AWS config required for S3 paths"))?;
        collect_partition_roots_s3(&resolved_path, aws, opts)?
    } else {
        collect_partition_roots_local(&resolved_path, opts)?
    };

    let target = scan_output.target;
    anyhow::ensure!(
        expected_root.is_none_or(|root| root == target.chain_root),
        "verify dataset layout changed while ownership was acquired; no artifact was written"
    );
    let partition_roots = scan_output.partition_roots;
    let algorithm = target.hash_strategy.as_str().to_string();
    let registry_path = opts
        .registry_path
        .clone()
        .unwrap_or_else(|| join_artifact_path(&target.chain_root, MERKLE_ROOTS_FILENAME));
    let suggested_run_report_path = join_artifact_path(
        &target.chain_root,
        &format!("{VERIFY_RUNS_DIR}/{run_id}/report.json"),
    );

    let mut warnings = Vec::new();
    if runs_roots && opts.registry_path.is_none() {
        let data_path = if resolved_path.starts_with("s3://") {
            resolved_path.clone()
        } else {
            absolute_local_path(&resolved_path)?
                .to_string_lossy()
                .into_owned()
        };
        let legacy = legacy_default_registry_path(&data_path, &target.chain, &target.table);
        if legacy != registry_path && artifact_exists(&legacy, aws) {
            warnings.push(format!(
                "a registry exists at the old default location {legacy} and is ignored; verify now keeps one registry per network at {registry_path}. Move or delete the old file (see \"Moving a registry from the old default location\" in docs/verifiability-artifact-runbook.md)"
            ));
        }
    }

    let mut protocol_passed = 0usize;
    let mut protocol_failed = 0usize;
    let mut protocol_not_verifiable = 0usize;
    let protocol_findings = if runs_protocol {
        scan_output.protocol_findings
    } else {
        Vec::new()
    };

    for finding in &protocol_findings {
        match finding.status {
            ProtocolCheckStatus::Pass => protocol_passed += 1,
            ProtocolCheckStatus::Fail => protocol_failed += 1,
            ProtocolCheckStatus::NotVerifiable => protocol_not_verifiable += 1,
        }
    }

    if let Some(partition) = &scan_output.truncated_partition {
        warnings.push(format!(
            "the scan stopped at the first protocol failure (fail-fast) inside partition {partition}; its root would be incomplete, so it was not compared. Re-run with --no-fail-fast to scan every file"
        ));
    }

    let mut findings = Vec::new();
    let mut matches = 0usize;
    let mut missing_expected = 0usize;
    let mut mismatches = 0usize;
    let mut updated = 0usize;
    let mut open_count = 0usize;
    let wrote_registry = if runs_roots {
        let open = open_partitions(
            &target,
            &scan_output.partition_max_block,
            aws,
            &mut warnings,
        );
        let snapshot = load_registry(&registry_path, aws)?;
        let network = target.network.clone().unwrap_or_default();

        let mut outcomes = Vec::new();
        for (partition, computed_root) in &partition_roots {
            if let Some(reason) = open.get(partition) {
                outcomes.push((partition, computed_root, RootOutcome::Open(reason.clone())));
                continue;
            }
            let outcome = match lookup_registry_row(
                &snapshot.rows,
                &network,
                &target.chain,
                &target.table,
                partition,
            ) {
                None => RootOutcome::Missing,
                Some((key, row))
                    if row.algorithm != algorithm || row.merkle_version != MERKLE_VERSION =>
                {
                    RootOutcome::Differs {
                        reason: Some(incomparable_root_error(row, &algorithm)),
                        key,
                        row: Box::new(row.clone()),
                    }
                }
                Some((_, row)) if row.merkle_root == *computed_root => {
                    RootOutcome::Match(row.merkle_root.clone())
                }
                Some((key, row)) => RootOutcome::Differs {
                    reason: None,
                    key,
                    row: Box::new(row.clone()),
                },
            };
            // A differing root fails the run unless --update-registry accepts it.
            let stop = matches!(outcome, RootOutcome::Differs { .. })
                && !opts.update_registry
                && !opts.no_fail_fast;
            outcomes.push((partition, computed_root, outcome));
            if stop {
                break;
            }
        }

        let differing = outcomes
            .iter()
            .filter(|(_, _, outcome)| matches!(outcome, RootOutcome::Differs { .. }))
            .count();
        let blocked = if protocol_failed > 0 || scan_output.truncated_partition.is_some() {
            Some("protocol checks failed".to_string())
        } else if differing > 0 && !opts.update_registry {
            Some(format!(
                "{differing} partition root(s) differ from the registry; re-run with --update-registry to accept the current data"
            ))
        } else {
            None
        };

        let new_row = |partition: &str, computed_root: &str| RegistryRow {
            network: network.clone(),
            chain: target.chain.clone(),
            table: target.table.clone(),
            partition: partition.to_string(),
            algorithm: algorithm.clone(),
            merkle_version: MERKLE_VERSION.to_string(),
            merkle_root: computed_root.to_string(),
            updated_at: now_rfc3339(),
        };
        let changes: Vec<RegistryChange> = outcomes
            .iter()
            .filter_map(|(partition, computed_root, outcome)| {
                let key = registry_key(&network, &target.chain, &target.table, partition);
                match outcome {
                    RootOutcome::Missing => Some(RegistryChange {
                        compared_key: key.clone(),
                        expected: None,
                        key,
                        row: new_row(partition, computed_root),
                    }),
                    RootOutcome::Differs {
                        key: compared_key,
                        row,
                        ..
                    } if opts.update_registry => Some(RegistryChange {
                        compared_key: compared_key.clone(),
                        expected: Some(row.as_ref().clone()),
                        key,
                        row: new_row(partition, computed_root),
                    }),
                    _ => None,
                }
            })
            .collect();

        let wrote = match (&blocked, changes.is_empty()) {
            (_, true) => false,
            (Some(reason), false) => {
                warnings.push(format!("the registry was not updated: {reason}"));
                false
            }
            (None, false) => {
                ownership
                    .context("registry publication requires dataset ownership")?
                    .revalidate_local_paths()?;
                commit_registry(&registry_path, aws, &snapshot, &changes)?;
                true
            }
        };

        for (partition, computed_root, outcome) in outcomes {
            let (status, expected_root, error) = match outcome {
                RootOutcome::Open(reason) => {
                    open_count += 1;
                    (FindingStatus::Open, None, Some(reason))
                }
                RootOutcome::Missing => {
                    missing_expected += 1;
                    (FindingStatus::MissingExpected, None, None)
                }
                RootOutcome::Match(root) => {
                    matches += 1;
                    (FindingStatus::Match, Some(root), None)
                }
                RootOutcome::Differs { reason, row, .. } if wrote && opts.update_registry => {
                    updated += 1;
                    (FindingStatus::Updated, Some(row.merkle_root), reason)
                }
                RootOutcome::Differs { reason, row, .. } => {
                    mismatches += 1;
                    (FindingStatus::Mismatch, Some(row.merkle_root), reason)
                }
            };
            findings.push(VerifyFinding {
                chain: target.chain.clone(),
                table: target.table.clone(),
                partition: partition.clone(),
                status,
                expected_root,
                computed_root: computed_root.clone(),
                error,
            });
        }
        wrote
    } else {
        false
    };

    let mut capability_findings = Vec::new();
    if effective_checks.contains(&VerifyCheck::Continuity) {
        capability_findings.push(CapabilityFinding {
            chain: target.chain.clone(),
            table: target.table.clone(),
            check: VerifyCheck::Continuity,
            status: CapabilityStatus::NotVerifiable,
            details: "continuity checks are not yet implemented under verify; use `validate` for sequence integrity checks"
                .to_string(),
        });
    }
    if effective_checks.contains(&VerifyCheck::Completeness) {
        capability_findings.push(CapabilityFinding {
            chain: target.chain.clone(),
            table: target.table.clone(),
            check: VerifyCheck::Completeness,
            status: CapabilityStatus::NotVerifiable,
            details: "completeness checks are not yet implemented under verify".to_string(),
        });
    }

    let run_finished = OffsetDateTime::now_utc();
    let duration_ms = (run_finished - run_started).whole_milliseconds().max(0) as u64;
    let published_report_path = if opts.publish_report || opts.publish_report_path.is_some() {
        Some(
            opts.publish_report_path
                .clone()
                .unwrap_or_else(|| suggested_run_report_path.clone()),
        )
    } else {
        None
    };

    let report = VerifyReport {
        report_schema_version: VERIFY_REPORT_SCHEMA_VERSION.to_string(),
        run_id,
        started_at: format_rfc3339(run_started),
        finished_at: format_rfc3339(run_finished),
        duration_ms,
        tool_version: env!("CARGO_PKG_VERSION").to_string(),
        chain: target.chain.clone(),
        table: target.table.clone(),
        network: target.network.clone(),
        scope: opts.scope,
        requested_checks: opts.checks.clone(),
        effective_checks: effective_checks.iter().copied().collect(),
        profile: opts.profile,
        data_path: resolved_path,
        registry_path,
        suggested_run_report_path,
        report_json_path: opts
            .report_json
            .as_ref()
            .map(|path| path.display().to_string()),
        published_report_path,
        algorithm,
        merkle_version: MERKLE_VERSION.to_string(),
        warnings,
        summary: VerifySummary {
            partitions_scanned: scan_output.partitions_scanned,
            matches,
            missing_expected,
            mismatches,
            updated,
            open_partitions: open_count,
            protocol_passed,
            protocol_failed,
            protocol_not_verifiable,
            capability_not_verifiable: capability_findings.len(),
            wrote_registry,
        },
        findings,
        protocol_findings,
        capability_findings,
    };

    let bytes = serde_json::to_vec_pretty(&report)?;

    if let Some(ref report_path) = opts.report_json {
        ownership
            .context("report publication requires dataset ownership")?
            .revalidate_local_paths()?;
        write_file_atomic(report_path, &bytes)
            .with_context(|| format!("writing JSON report to {}", report_path.display()))?;
    }

    if let Some(ref publish_path) = report.published_report_path {
        ownership
            .context("report publication requires dataset ownership")?
            .revalidate_local_paths()?;
        write_report_bytes(publish_path, aws, &bytes)?;
    }

    Ok(report)
}

/// Registry location used before registries moved to `<chain_root>` (v0.7.1
/// and earlier). Only used to warn about an old registry that is now ignored.
fn legacy_default_registry_path(data_path: &str, chain: &str, table: &str) -> String {
    if data_path.starts_with("s3://") {
        if let Ok((bucket, prefix)) = parse_s3_url(data_path) {
            let mut segments: Vec<&str> = prefix.split('/').filter(|s| !s.is_empty()).collect();
            if let Some(pos) = segments.iter().position(|s| *s == table) {
                segments.truncate(pos);
            }
            if segments.len() < 2
                || segments[segments.len() - 2] != chain
                || segments[segments.len() - 1] != "mainnet"
            {
                segments = vec![chain, "mainnet"];
            }
            if segments.is_empty() {
                format!("s3://{}/merkle_roots.parquet", bucket)
            } else {
                format!(
                    "s3://{}/{}/merkle_roots.parquet",
                    bucket,
                    segments.join("/")
                )
            }
        } else {
            format!("s3://{}/{}/mainnet/merkle_roots.parquet", "unknown", chain)
        }
    } else {
        let mut root = PathBuf::from(data_path);
        if root.is_file() {
            root = root.parent().unwrap_or(Path::new(".")).to_path_buf();
        }

        let root_text = root.to_string_lossy().to_string();
        if let Some(idx) = root_text.find(&format!("/{table}")) {
            root = PathBuf::from(&root_text[..idx]);
        }

        let root_text = root.to_string_lossy();
        if !root_text.ends_with(&format!("/{chain}/mainnet")) {
            root.push(chain);
            root.push("mainnet");
        }
        root.push("merkle_roots.parquet");
        root.to_string_lossy().to_string()
    }
}

/// What `verify` checks, resolved from the scanned files and the explicit flags.
#[derive(Debug, Clone)]
struct Target {
    /// Chain family (`evm`, `bitcoin`, ...): registry key, default hash
    /// strategy, and protocol checks.
    chain: String,
    table: String,
    /// Network (`firehose-parquet.chain_name`, else the chain root directory name).
    network: Option<String>,
    /// `<output>/<chain_name>` directory (local path or `s3://bucket/prefix`)
    /// holding the table directories, `merkle_roots.parquet` and `verify_runs/`.
    chain_root: String,
    hash_strategy: HashStrategy,
}

/// Dataset identity recorded in a file's `firehose-parquet.*` footer metadata.
#[derive(Debug, Default)]
struct FooterIdentity {
    block_type: Option<String>,
    chain_name: Option<String>,
}

impl FooterIdentity {
    fn from_metadata(metadata: &parquet::file::metadata::ParquetMetaData) -> Self {
        let value = |key: &str| {
            metadata
                .file_metadata()
                .key_value_metadata()?
                .iter()
                .find(|kv| kv.key == key)?
                .value
                .clone()
                .filter(|v| !v.is_empty())
        };
        Self {
            block_type: value("firehose-parquet.block_type"),
            chain_name: value("firehose-parquet.chain_name"),
        }
    }
}

/// Where a data file sits in the `build` layout
/// `<chain_root>/<table>/[<k>=<v>/...]<file>.parquet`.
#[derive(Debug, PartialEq, Eq)]
struct FileLayout {
    chain_root: String,
    table: Option<String>,
    /// Name of the chain root directory (the network under `build` output).
    chain_root_name: Option<String>,
}

/// Splits a `/`-separated data file path (absolute local path or
/// `s3://bucket/key`) into its layout by path components: the table is the
/// nearest ancestor directory that is not a Hive partition (`k=v`), and the
/// chain root is that directory's parent.
fn file_layout(file_path: &str) -> FileLayout {
    let (prefix, rest) = match file_path.strip_prefix("s3://") {
        Some(url) => {
            let (bucket, key) = url.split_once('/').unwrap_or((url, ""));
            (format!("s3://{bucket}"), key)
        }
        None => (String::new(), file_path),
    };
    let mut dirs: Vec<&str> = rest.split('/').filter(|c| !c.is_empty()).collect();
    dirs.pop(); // file name
    while dirs.last().is_some_and(|dir| dir.contains('=')) {
        dirs.pop();
    }
    let table = dirs.pop().map(str::to_string);
    let chain_root_name = dirs.last().map(|dir| dir.to_string());
    let chain_root = match (prefix.is_empty(), dirs.is_empty()) {
        (true, true) => "/".to_string(),
        (false, true) => prefix,
        _ => format!("{prefix}/{}", dirs.join("/")),
    };
    FileLayout {
        chain_root,
        table,
        chain_root_name,
    }
}

/// Joins a `/`-separated artifact path onto a chain root.
fn join_artifact_path(chain_root: &str, artifact: &str) -> String {
    format!("{}/{artifact}", chain_root.trim_end_matches('/'))
}

/// Resolves the [`Target`] from the first scanned file and checks that every
/// later file belongs to the same table of the same dataset.
struct TargetResolver<'a> {
    opts: &'a VerifyOptions,
    target: Option<Target>,
    /// First `firehose-parquet.chain_name` seen, to reject mixed networks.
    seen_chain_name: Option<String>,
}

impl<'a> TargetResolver<'a> {
    fn new(opts: &'a VerifyOptions) -> Self {
        Self {
            opts,
            target: None,
            seen_chain_name: None,
        }
    }

    fn observe(&mut self, file_path: &str, footer: &FooterIdentity) -> Result<&Target> {
        let layout = file_layout(file_path);
        match &self.target {
            None => self.target = Some(self.resolve(file_path, &layout, footer)?),
            Some(target) => check_same_target(target, file_path, &layout, footer)?,
        }
        if let Some(chain_name) = &footer.chain_name {
            match &self.seen_chain_name {
                Some(seen) if seen != chain_name => {
                    return Err(anyhow!(
                        "{file_path} was written for network `{chain_name}`, but earlier files are from `{seen}`; verify one network at a time"
                    ))
                }
                Some(_) => {}
                None => self.seen_chain_name = Some(chain_name.clone()),
            }
        }
        Ok(self.target.as_ref().expect("target resolved above"))
    }

    fn resolve(
        &self,
        file_path: &str,
        layout: &FileLayout,
        footer: &FooterIdentity,
    ) -> Result<Target> {
        let chain = match (self.opts.chain.as_deref(), footer.block_type.as_deref()) {
            (Some(flag), Some(meta)) if !flag.eq_ignore_ascii_case(meta) => {
                return Err(anyhow!(
                    "--chain {flag} conflicts with {file_path}, which has firehose-parquet.block_type={meta}; drop --chain or point verify at {flag} data"
                ))
            }
            (_, Some(meta)) => meta.to_string(),
            (Some(flag), None) => flag.to_string(),
            (None, None) => {
                return Err(anyhow!(
                    "cannot infer the chain: {file_path} has no firehose-parquet.block_type metadata; pass --chain"
                ))
            }
        };
        let table = match (self.opts.table.as_deref(), layout.table.as_deref()) {
            (Some(flag), Some(dir)) if flag != dir => {
                return Err(anyhow!(
                    "--table {flag} conflicts with the directory layout: {file_path} is in table directory `{dir}`"
                ))
            }
            (_, Some(dir)) => dir.to_string(),
            (Some(flag), None) => flag.to_string(),
            (None, None) => {
                return Err(anyhow!(
                    "cannot infer the table: {file_path} is not inside a table directory; pass --table"
                ))
            }
        };
        let hash_strategy = resolve_hash_strategy(&chain, self.opts.hash_strategy.as_deref())?;
        Ok(Target {
            chain,
            table,
            network: footer
                .chain_name
                .clone()
                .or_else(|| layout.chain_root_name.clone()),
            chain_root: layout.chain_root.clone(),
            hash_strategy,
        })
    }

    fn finish(self) -> Option<Target> {
        self.target
    }
}

fn check_same_target(
    target: &Target,
    file_path: &str,
    layout: &FileLayout,
    footer: &FooterIdentity,
) -> Result<()> {
    if layout.chain_root != target.chain_root || layout.table.as_deref() != Some(&target.table) {
        return Err(anyhow!(
            "the verify path holds more than one table: {file_path} is not in {}; verify one table directory at a time",
            join_artifact_path(&target.chain_root, &target.table)
        ));
    }
    if let Some(block_type) = footer.block_type.as_deref() {
        if !block_type.eq_ignore_ascii_case(&target.chain) {
            return Err(anyhow!(
                "{file_path} has firehose-parquet.block_type={block_type}, but the chain is `{}`",
                target.chain
            ));
        }
    }
    Ok(())
}

/// Absolute form of a local verify path (symlinks and `..` resolved when the
/// path exists), so layout and registry paths do not depend on the working
/// directory.
fn absolute_local_path(path: &str) -> Result<PathBuf> {
    std::fs::canonicalize(path)
        .or_else(|_| std::path::absolute(path))
        .with_context(|| format!("resolving {path}"))
}

/// Path of `file` relative to the scan base, `/`-separated.
fn relative_path(file: &str, base: &str) -> String {
    file.strip_prefix(base)
        .unwrap_or(file)
        .trim_start_matches('/')
        .to_string()
}

fn collect_partition_roots_local(path: &str, opts: &VerifyOptions) -> Result<ScanOutput> {
    let (base, files) = list_verify_files(path)?;
    let partitions: Vec<String> = files
        .iter()
        .map(|file| detect_partition(file, &base))
        .collect();
    let mut scan = ScanAccumulator::new(opts);
    for (index, file_path) in files.iter().enumerate() {
        let file = File::open(file_path).with_context(|| format!("opening {file_path}"))?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
        let next_partition = partitions.get(index + 1).map(String::as_str);
        if !scan.add_file(builder, file_path, &partitions[index], next_partition)? {
            break;
        }
    }
    scan.finish()
}

fn list_verify_files(path: &str) -> Result<(String, Vec<String>)> {
    let pathbuf = absolute_local_path(path)?;
    let mut files = Vec::new();

    if pathbuf.is_dir() {
        collect_parquet_files(&pathbuf, &mut files)?;
    } else if pathbuf
        .extension()
        .is_some_and(|ext| ext.eq_ignore_ascii_case("parquet"))
    {
        files.push(pathbuf.clone());
    }

    let base = if pathbuf.is_dir() {
        pathbuf.clone()
    } else {
        pathbuf.parent().unwrap_or(Path::new("/")).to_path_buf()
    };
    let base = base.to_string_lossy().to_string();
    let mut files: Vec<String> = files
        .iter()
        .map(|file| file.to_string_lossy().to_string())
        .filter(|file| !is_reserved_artifact_path(&relative_path(file, &base)))
        .collect();
    files.sort();
    if files.is_empty() {
        return Err(anyhow!("no parquet files found in {}", path));
    }

    Ok((base, files))
}

fn collect_partition_roots_s3(
    path: &str,
    aws: &AwsConfig,
    opts: &VerifyOptions,
) -> Result<ScanOutput> {
    let (bucket, prefix, client, objects) = list_verify_objects(path, aws)?;
    let partitions: Vec<String> = objects
        .iter()
        .map(|obj| detect_partition(obj.location.as_ref(), &prefix))
        .collect();
    let mut prefetcher = Prefetcher::spawn(
        Arc::new(client),
        objects.clone(),
        PREFETCH_MAX_IN_FLIGHT,
        PREFETCH_BUDGET_BYTES,
    );
    let mut scan = ScanAccumulator::new(opts);
    for (index, obj) in objects.iter().enumerate() {
        let location = &obj.location;
        let object = prefetcher
            .next_object()
            .ok_or_else(|| anyhow!("S3 prefetch ended before s3://{bucket}/{location}"))?
            .with_context(|| format!("reading s3://{bucket}/{location}"))?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(object.data.clone())?;
        let file_path = format!("s3://{bucket}/{location}");
        let next_partition = partitions.get(index + 1).map(String::as_str);
        if !scan.add_file(builder, &file_path, &partitions[index], next_partition)? {
            break;
        }
    }
    scan.finish()
}

fn list_verify_objects(
    path: &str,
    aws: &AwsConfig,
) -> Result<(
    String,
    String,
    object_store::aws::AmazonS3,
    Vec<object_store::ObjectMeta>,
)> {
    let (bucket, prefix) = parse_s3_url(path)?;
    let client = aws.build_s3_client(&bucket)?;

    let list_prefix = if prefix.is_empty() {
        None
    } else {
        Some(object_store::path::Path::from(prefix.as_str()))
    };

    let mut objects: Vec<object_store::ObjectMeta> = block_on_async(async {
        use futures::TryStreamExt;
        client.list(list_prefix.as_ref()).try_collect().await
    })
    .map_err(|e| anyhow!("listing S3 objects: {e}"))?;

    objects.retain(|obj| {
        let key = obj.location.as_ref();
        key.ends_with(".parquet") && !is_reserved_artifact_path(&relative_path(key, &prefix))
    });
    objects.sort_by(|a, b| a.location.cmp(&b.location));
    if objects.is_empty() {
        return Err(anyhow!("no parquet files found in {}", path));
    }

    Ok((bucket, prefix, client, objects))
}

/// Per-partition Merkle trees, protocol state and block ranges across the
/// files of one scan, in file order.
struct ScanAccumulator<'a> {
    opts: &'a VerifyOptions,
    resolver: TargetResolver<'a>,
    partition_trees: HashMap<String, MerkleAccumulator>,
    protocol_state: HashMap<String, ProtocolPartitionState>,
    partition_max_block: HashMap<String, u64>,
    truncated_partition: Option<String>,
}

impl<'a> ScanAccumulator<'a> {
    fn new(opts: &'a VerifyOptions) -> Self {
        Self {
            opts,
            resolver: TargetResolver::new(opts),
            partition_trees: HashMap::new(),
            protocol_state: HashMap::new(),
            partition_max_block: HashMap::new(),
            truncated_partition: None,
        }
    }

    /// Scans one file. Returns `false` when fail-fast stops the scan at a
    /// protocol failure; the partition is then marked truncated if more of its
    /// files (`next_partition`) were left unread.
    fn add_file<R: parquet::file::reader::ChunkReader + 'static>(
        &mut self,
        builder: ParquetRecordBatchReaderBuilder<R>,
        file_path: &str,
        partition: &str,
        next_partition: Option<&str>,
    ) -> Result<bool> {
        let footer = FooterIdentity::from_metadata(builder.metadata());
        let target = self.resolver.observe(file_path, &footer)?;
        let state = self
            .protocol_state
            .entry(partition.to_string())
            .or_default();

        if self.opts.runs_roots() {
            let tree = self
                .partition_trees
                .entry(partition.to_string())
                .or_insert_with(|| MerkleAccumulator::new(target.hash_strategy));
            let max_block = hash_parquet_file(builder, file_path, self.opts, target, state, tree)?;
            if let Some(max_block) = max_block {
                let max = self
                    .partition_max_block
                    .entry(partition.to_string())
                    .or_insert(max_block);
                *max = (*max).max(max_block);
            }
        } else if self.opts.runs_protocol() {
            check_parquet_file(builder, target, state)?;
        }

        if self.opts.runs_protocol() && !self.opts.no_fail_fast && has_protocol_failure(state) {
            if next_partition == Some(partition) {
                self.truncated_partition = Some(partition.to_string());
            }
            return Ok(false);
        }
        Ok(true)
    }

    fn finish(self) -> Result<ScanOutput> {
        let target = self
            .resolver
            .finish()
            .ok_or_else(|| anyhow!("no parquet files were scanned"))?;
        let mut roots = BTreeMap::new();
        for (partition, tree) in self.partition_trees {
            // A partial partition's root is meaningless; never compare or record it.
            if self.truncated_partition.as_ref() == Some(&partition) {
                continue;
            }
            roots.insert(partition, hex::encode(tree.root()));
        }
        let partitions_scanned =
            self.protocol_state.len() - usize::from(self.truncated_partition.is_some());
        let protocol_findings = if self.opts.runs_protocol() {
            finalize_protocol_findings(&target, &self.protocol_state)
        } else {
            Vec::new()
        };
        Ok(ScanOutput {
            target,
            partition_roots: roots,
            partitions_scanned,
            protocol_findings,
            partition_max_block: self.partition_max_block,
            truncated_partition: self.truncated_partition,
        })
    }
}

/// Runs protocol checks on one file and streams its rows into the partition
/// tree. Returns the file's highest `block_num`, when the column exists.
fn hash_parquet_file<R: parquet::file::reader::ChunkReader + 'static>(
    builder: ParquetRecordBatchReaderBuilder<R>,
    file_path: &str,
    opts: &VerifyOptions,
    target: &Target,
    protocol_state: &mut ProtocolPartitionState,
    tree: &mut MerkleAccumulator,
) -> Result<Option<u64>> {
    let reader = builder.build()?;
    let mut max_block: Option<u64> = None;
    for maybe_batch in reader {
        let batch = maybe_batch?;
        if opts.runs_protocol() {
            run_protocol_checks_for_batch(target, &batch, protocol_state);
        }
        let batch_max = batch
            .column_by_name("block_num")
            .and_then(|column| column.as_any().downcast_ref::<UInt64Array>())
            .and_then(arrow::compute::max);
        max_block = max_block.max(batch_max);
        append_batch_leaves(&batch, target.hash_strategy, tree)
            .with_context(|| format!("hashing rows of {file_path}"))?;
    }
    Ok(max_block)
}

/// Protocol checks for a run without roots: reads only the columns the checks
/// use (see [`protocol_columns`]), and no rows when no check applies.
fn check_parquet_file<R: parquet::file::reader::ChunkReader + 'static>(
    builder: ParquetRecordBatchReaderBuilder<R>,
    target: &Target,
    protocol_state: &mut ProtocolPartitionState,
) -> Result<()> {
    let columns = protocol_columns(target);
    if columns.is_empty() {
        return Ok(());
    }
    let projection = ProjectionMask::columns(builder.parquet_schema(), columns.iter().copied());
    for maybe_batch in builder.with_projection(projection).build()? {
        run_protocol_checks_for_batch(target, &maybe_batch?, protocol_state);
    }
    Ok(())
}

/// Columns [`run_protocol_checks_for_batch`] reads for a target. Keep the two
/// in sync: a protocol-only run reads nothing else.
fn protocol_columns(target: &Target) -> &'static [&'static str] {
    if !target.chain.eq_ignore_ascii_case("evm") {
        return &[];
    }
    match target.table.to_ascii_lowercase().as_str() {
        "blocks" => &[
            "block_num",
            "number",
            "block_id",
            "hash",
            "parent_id",
            "parent_hash",
        ],
        "transactions" | "logs" | "calls" => &["block_num", "block_number"],
        _ => &[],
    }
}

/// Adds one `merkle_v2` leaf per row, `H(0x00 || encoded_row)` with the row
/// encoded by [`row_encoding::RowEncoder`], to `out`.
fn append_batch_leaves(
    batch: &RecordBatch,
    hash_strategy: HashStrategy,
    out: &mut impl Extend<[u8; 32]>,
) -> Result<()> {
    let encoder = row_encoding::RowEncoder::new(batch)?;
    let mut encoded = Vec::new();
    out.extend((0..batch.num_rows()).map(|row| {
        encoded.clear();
        encoded.push(MERKLE_LEAF_PREFIX);
        encoder.encode_row(row, &mut encoded);
        hash_strategy.hash(&encoded)
    }));
    Ok(())
}

/// Most S3 objects fetched ahead of the scan at once.
const PREFETCH_MAX_IN_FLIGHT: usize = 4;
/// Most object bytes held ahead of the scan: fetched or in flight, and not
/// yet scanned. An object larger than the budget takes all of it.
const PREFETCH_BUDGET_BYTES: u64 = 256 * 1024 * 1024;

/// S3 objects fetched concurrently on a worker thread and handed to the scan
/// in listing order. Each object holds its share of the byte budget until it
/// is dropped, so memory stays bounded however far the fetches run ahead.
struct Prefetcher {
    receiver: Option<tokio::sync::mpsc::Receiver<Result<PrefetchedObject>>>,
    budget: Arc<tokio::sync::Semaphore>,
    worker: Option<std::thread::JoinHandle<()>>,
}

struct PrefetchedObject {
    data: bytes::Bytes,
    _budget: tokio::sync::OwnedSemaphorePermit,
}

impl Prefetcher {
    fn spawn(
        store: Arc<dyn ObjectStore>,
        objects: Vec<object_store::ObjectMeta>,
        max_in_flight: usize,
        budget_bytes: u64,
    ) -> Self {
        // Budget accounting in KiB keeps permit counts well within u32.
        let budget_units = budget_bytes.div_ceil(1024).max(1);
        let budget = Arc::new(tokio::sync::Semaphore::new(budget_units as usize));
        let max_in_flight = max_in_flight.max(1);
        let (sender, receiver) = tokio::sync::mpsc::channel(max_in_flight);
        let worker_budget = budget.clone();
        let worker = std::thread::spawn(move || {
            let runtime = match tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                Ok(runtime) => runtime,
                Err(err) => {
                    let _ = sender
                        .blocking_send(Err(anyhow!("starting the S3 prefetch runtime: {err}")));
                    return;
                }
            };
            runtime.block_on(async move {
                use futures::StreamExt;
                let fetches = futures::stream::iter(objects)
                    .map(|object| {
                        let (store, budget) = (store.clone(), worker_budget.clone());
                        async move {
                            let units = object.size.div_ceil(1024).clamp(1, budget_units) as u32;
                            let permit = budget
                                .acquire_many_owned(units)
                                .await
                                .map_err(|_| anyhow!("S3 prefetch stopped"))?;
                            let data =
                                async { store.get(&object.location).await?.bytes().await }.await?;
                            Ok(PrefetchedObject {
                                data,
                                _budget: permit,
                            })
                        }
                    })
                    .buffered(max_in_flight);
                futures::pin_mut!(fetches);
                while let Some(object) = fetches.next().await {
                    // A closed channel means the scan stopped early.
                    if sender.send(object).await.is_err() {
                        break;
                    }
                }
            });
        });
        Self {
            receiver: Some(receiver),
            budget,
            worker: Some(worker),
        }
    }

    /// The next object in listing order, or `None` once all were delivered.
    fn next_object(&mut self) -> Option<Result<PrefetchedObject>> {
        let receiver = self.receiver.as_mut()?;
        block_on_async(receiver.recv())
    }
}

impl Drop for Prefetcher {
    fn drop(&mut self) {
        // Closing the budget and the channel unblocks the worker wherever it
        // waits, so joining it cannot hang after an early stop.
        self.budget.close();
        drop(self.receiver.take());
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

fn has_protocol_failure(state: &ProtocolPartitionState) -> bool {
    state.hash_matches_block_id.first_failure.is_some()
        || state.parent_hash_matches_parent_id.first_failure.is_some()
        || state.number_matches_block_num.first_failure.is_some()
        || state
            .transactions_block_number_matches_block_num
            .first_failure
            .is_some()
        || state
            .logs_block_number_matches_block_num
            .first_failure
            .is_some()
        || state
            .calls_block_number_matches_block_num
            .first_failure
            .is_some()
}

fn finalize_protocol_findings(
    target: &Target,
    state_by_partition: &HashMap<String, ProtocolPartitionState>,
) -> Vec<ProtocolCheckFinding> {
    let mut partitions: Vec<String> = state_by_partition.keys().cloned().collect();
    partitions.sort();

    let mut findings = Vec::new();
    for partition in partitions {
        let state = match state_by_partition.get(&partition) {
            Some(s) => s,
            None => continue,
        };

        if target.chain.eq_ignore_ascii_case("evm") && target.table.eq_ignore_ascii_case("blocks") {
            add_check_finding(
                &mut findings,
                target,
                &partition,
                "evm_hash_matches_block_id",
                &state.hash_matches_block_id,
            );
            add_check_finding(
                &mut findings,
                target,
                &partition,
                "evm_parent_hash_matches_parent_id",
                &state.parent_hash_matches_parent_id,
            );
            add_check_finding(
                &mut findings,
                target,
                &partition,
                "evm_number_matches_block_num",
                &state.number_matches_block_num,
            );
            findings.push(ProtocolCheckFinding {
                chain: target.chain.clone(),
                table: target.table.clone(),
                partition: partition.clone(),
                check: "evm_transactions_root_trie_recompute".to_string(),
                status: ProtocolCheckStatus::NotVerifiable,
                block_num: None,
                details: "requires transaction trie materialization from transactions table"
                    .to_string(),
            });
            findings.push(ProtocolCheckFinding {
                chain: target.chain.clone(),
                table: target.table.clone(),
                partition: partition.clone(),
                check: "evm_receipt_root_trie_recompute".to_string(),
                status: ProtocolCheckStatus::NotVerifiable,
                block_num: None,
                details: "requires receipt trie materialization from receipts/logs data"
                    .to_string(),
            });
            findings.push(ProtocolCheckFinding {
                chain: target.chain.clone(),
                table: target.table.clone(),
                partition,
                check: "evm_state_root_recompute".to_string(),
                status: ProtocolCheckStatus::NotVerifiable,
                block_num: None,
                details: "requires full state transition execution and account/storage tries"
                    .to_string(),
            });
        } else if target.chain.eq_ignore_ascii_case("evm")
            && target.table.eq_ignore_ascii_case("transactions")
        {
            add_check_finding(
                &mut findings,
                target,
                &partition,
                "evm_transactions_block_number_matches_block_num",
                &state.transactions_block_number_matches_block_num,
            );
            findings.push(ProtocolCheckFinding {
                chain: target.chain.clone(),
                table: target.table.clone(),
                partition,
                check: "evm_transactions_root_inclusion".to_string(),
                status: ProtocolCheckStatus::NotVerifiable,
                block_num: None,
                details: "requires canonical transaction RLP encoding + trie indexing by transaction position".to_string(),
            });
        } else if target.chain.eq_ignore_ascii_case("evm")
            && target.table.eq_ignore_ascii_case("logs")
        {
            add_check_finding(
                &mut findings,
                target,
                &partition,
                "evm_logs_block_number_matches_block_num",
                &state.logs_block_number_matches_block_num,
            );
            findings.push(ProtocolCheckFinding {
                chain: target.chain.clone(),
                table: target.table.clone(),
                partition,
                check: "evm_logs_receipt_inclusion".to_string(),
                status: ProtocolCheckStatus::NotVerifiable,
                block_num: None,
                details: "requires receipt reconstruction and trie inclusion proofs".to_string(),
            });
        } else if target.chain.eq_ignore_ascii_case("evm")
            && target.table.eq_ignore_ascii_case("calls")
        {
            add_check_finding(
                &mut findings,
                target,
                &partition,
                "evm_calls_block_number_matches_block_num",
                &state.calls_block_number_matches_block_num,
            );
            findings.push(ProtocolCheckFinding {
                chain: target.chain.clone(),
                table: target.table.clone(),
                partition,
                check: "evm_calls_receipt_correlation".to_string(),
                status: ProtocolCheckStatus::NotVerifiable,
                block_num: None,
                details: "requires transaction execution traces correlated with canonical receipts"
                    .to_string(),
            });
        } else {
            findings.push(ProtocolCheckFinding {
                chain: target.chain.clone(),
                table: target.table.clone(),
                partition,
                check: "protocol_checks_coverage".to_string(),
                status: ProtocolCheckStatus::NotVerifiable,
                block_num: None,
                details: "protocol checks are currently implemented for evm blocks/transactions/logs/calls only".to_string(),
            });
        }
    }

    findings
}

fn add_check_finding(
    findings: &mut Vec<ProtocolCheckFinding>,
    target: &Target,
    partition: &str,
    check: &str,
    acc: &CheckAccumulator,
) {
    let (status, block_num, details) = if let Some((block_num, details)) = &acc.first_failure {
        (ProtocolCheckStatus::Fail, Some(*block_num), details.clone())
    } else if let Some(reason) = &acc.not_verifiable_reason {
        (ProtocolCheckStatus::NotVerifiable, None, reason.clone())
    } else if acc.evaluated == 0 {
        (
            ProtocolCheckStatus::NotVerifiable,
            None,
            "no rows evaluated".to_string(),
        )
    } else {
        (
            ProtocolCheckStatus::Pass,
            None,
            format!("validated {} rows", acc.evaluated),
        )
    };

    findings.push(ProtocolCheckFinding {
        chain: target.chain.clone(),
        table: target.table.clone(),
        partition: partition.to_string(),
        check: check.to_string(),
        status,
        block_num,
        details,
    });
}

fn run_protocol_checks_for_batch(
    target: &Target,
    batch: &RecordBatch,
    state: &mut ProtocolPartitionState,
) {
    if !target.chain.eq_ignore_ascii_case("evm") {
        return;
    }

    if target.table.eq_ignore_ascii_case("transactions") {
        check_block_number_alignment(
            batch,
            "block_number",
            &mut state.transactions_block_number_matches_block_num,
        );
        return;
    }

    if target.table.eq_ignore_ascii_case("logs") {
        check_block_number_alignment(
            batch,
            "block_number",
            &mut state.logs_block_number_matches_block_num,
        );
        return;
    }

    if target.table.eq_ignore_ascii_case("calls") {
        check_block_number_alignment(
            batch,
            "block_number",
            &mut state.calls_block_number_matches_block_num,
        );
        return;
    }

    if !target.table.eq_ignore_ascii_case("blocks") {
        return;
    }

    let schema = batch.schema();
    let idx_block_id = match schema.index_of("block_id") {
        Ok(v) => v,
        Err(_) => {
            state
                .hash_matches_block_id
                .mark_not_verifiable("missing required column: block_id");
            state
                .parent_hash_matches_parent_id
                .mark_not_verifiable("missing required column: block_id");
            return;
        }
    };
    let idx_hash = match schema.index_of("hash") {
        Ok(v) => v,
        Err(_) => {
            state
                .hash_matches_block_id
                .mark_not_verifiable("missing required column: hash");
            return;
        }
    };
    let idx_parent_id = match schema.index_of("parent_id") {
        Ok(v) => v,
        Err(_) => {
            state
                .parent_hash_matches_parent_id
                .mark_not_verifiable("missing required column: parent_id");
            return;
        }
    };
    let idx_parent_hash = match schema.index_of("parent_hash") {
        Ok(v) => v,
        Err(_) => {
            state
                .parent_hash_matches_parent_id
                .mark_not_verifiable("missing required column: parent_hash");
            return;
        }
    };
    let idx_block_num = match schema.index_of("block_num") {
        Ok(v) => v,
        Err(_) => {
            state
                .number_matches_block_num
                .mark_not_verifiable("missing required column: block_num");
            return;
        }
    };
    let idx_number = match schema.index_of("number") {
        Ok(v) => v,
        Err(_) => {
            state
                .number_matches_block_num
                .mark_not_verifiable("missing required column: number");
            return;
        }
    };

    for row in 0..batch.num_rows() {
        let canonical_num = u64_cell(batch.column(idx_block_num).as_ref(), row);
        let evm_num = u64_cell(batch.column(idx_number).as_ref(), row);
        let block_num_for_error = canonical_num.or(evm_num).unwrap_or(0);

        match (canonical_num, evm_num) {
            (Some(a), Some(b)) => state.number_matches_block_num.observe(
                a == b,
                block_num_for_error,
                format!("block_num={} does not match number={}", a, b),
            ),
            _ => state.number_matches_block_num.observe(
                false,
                block_num_for_error,
                "block_num/number is null or unsupported type",
            ),
        }

        let canonical_id = comparable_bytes(batch.column(idx_block_id).as_ref(), row);
        let evm_hash = comparable_bytes(batch.column(idx_hash).as_ref(), row);
        match (canonical_id, evm_hash) {
            (Some(a), Some(b)) => state.hash_matches_block_id.observe(
                a == b,
                block_num_for_error,
                "block_id does not match hash",
            ),
            _ => state.hash_matches_block_id.observe(
                false,
                block_num_for_error,
                "block_id/hash is null or unsupported type",
            ),
        }

        let canonical_parent = comparable_bytes(batch.column(idx_parent_id).as_ref(), row);
        let evm_parent = comparable_bytes(batch.column(idx_parent_hash).as_ref(), row);
        match (canonical_parent, evm_parent) {
            (Some(a), Some(b)) => state.parent_hash_matches_parent_id.observe(
                a == b,
                block_num_for_error,
                "parent_id does not match parent_hash",
            ),
            _ => state.parent_hash_matches_parent_id.observe(
                false,
                block_num_for_error,
                "parent_id/parent_hash is null or unsupported type",
            ),
        }
    }
}

fn check_block_number_alignment(
    batch: &RecordBatch,
    table_column: &str,
    acc: &mut CheckAccumulator,
) {
    let schema = batch.schema();
    let idx_block_num = match schema.index_of("block_num") {
        Ok(v) => v,
        Err(_) => {
            acc.mark_not_verifiable("missing required column: block_num");
            return;
        }
    };
    let idx_table_block_num = match schema.index_of(table_column) {
        Ok(v) => v,
        Err(_) => {
            acc.mark_not_verifiable(format!("missing required column: {table_column}"));
            return;
        }
    };

    for row in 0..batch.num_rows() {
        let canonical_num = u64_cell(batch.column(idx_block_num).as_ref(), row);
        let table_num = u64_cell(batch.column(idx_table_block_num).as_ref(), row);
        let block_num_for_error = canonical_num.or(table_num).unwrap_or(0);

        match (canonical_num, table_num) {
            (Some(a), Some(b)) => acc.observe(
                a == b,
                block_num_for_error,
                format!("block_num={} does not match {}={}", a, table_column, b),
            ),
            _ => acc.observe(
                false,
                block_num_for_error,
                format!("block_num/{} is null or unsupported type", table_column),
            ),
        }
    }
}

fn u64_cell(array: &dyn Array, row: usize) -> Option<u64> {
    if array.is_null(row) {
        return None;
    }
    if let Some(a) = array.as_any().downcast_ref::<UInt64Array>() {
        return Some(a.value(row));
    }
    if let Some(a) = array.as_any().downcast_ref::<Int64Array>() {
        let v = a.value(row);
        return if v >= 0 { Some(v as u64) } else { None };
    }
    if let Some(a) = array.as_any().downcast_ref::<StringArray>() {
        return a.value(row).parse::<u64>().ok();
    }
    if let Some(a) = array.as_any().downcast_ref::<LargeStringArray>() {
        return a.value(row).parse::<u64>().ok();
    }
    None
}

/// Raw bytes of a string or binary cell for identity comparisons, falling back
/// to the `merkle_v2` canonical bytes for other types.
fn comparable_bytes(array: &dyn Array, row: usize) -> Option<Vec<u8>> {
    if array.is_null(row) {
        return None;
    }
    let bytes = match array.data_type() {
        DataType::Binary => array.as_binary::<i32>().value(row),
        DataType::LargeBinary => array.as_binary::<i64>().value(row),
        DataType::BinaryView => array.as_binary_view().value(row),
        DataType::FixedSizeBinary(_) => array.as_fixed_size_binary().value(row),
        DataType::Utf8 => array.as_string::<i32>().value(row).as_bytes(),
        DataType::LargeUtf8 => array.as_string::<i64>().value(row).as_bytes(),
        DataType::Utf8View => array.as_string_view().value(row).as_bytes(),
        _ => return row_encoding::canonical_value_bytes(array, row),
    };
    Some(bytes.to_vec())
}

/// Streaming `merkle_v2` partition root.
///
/// Interior nodes are `H(0x01 || left || right)` and an odd trailing node is
/// promoted to the next level unchanged instead of being paired with itself.
/// The tree root (`H("")` for an empty partition) is then bound to the row
/// count as `H(0x02 || row_count_u64_le || tree_root)`, so `[a, b, c]` and
/// `[a, b, c, c]` never share a root.
///
/// Leaves are folded in as they arrive: the accumulator keeps only the roots
/// of completed perfect subtrees, at most one per height, so memory is
/// O(log n) instead of one 32-byte leaf per row. Pairing a level left to
/// right and promoting its odd last node yields exactly those perfect subtrees
/// joined from the smallest to the largest, which is what [`Self::root`] does.
#[derive(Debug, Clone)]
struct MerkleAccumulator {
    hash_strategy: HashStrategy,
    /// Completed perfect subtrees as `(height, root)`, tallest (leftmost) first.
    subtrees: Vec<(u32, [u8; 32])>,
    leaves: u64,
}

impl MerkleAccumulator {
    fn new(hash_strategy: HashStrategy) -> Self {
        Self {
            hash_strategy,
            subtrees: Vec::new(),
            leaves: 0,
        }
    }

    fn push(&mut self, leaf: [u8; 32]) {
        self.leaves += 1;
        let (mut height, mut node) = (0, leaf);
        while let Some(&(top_height, top)) = self.subtrees.last() {
            if top_height != height {
                break;
            }
            self.subtrees.pop();
            node = merkle_node(self.hash_strategy, &top, &node);
            height += 1;
        }
        self.subtrees.push((height, node));
    }

    fn root(&self) -> [u8; 32] {
        let tree_root = match self.subtrees.split_last() {
            None => self.hash_strategy.hash(&[]),
            Some((&(_, last), rest)) => rest.iter().rev().fold(last, |right, (_, left)| {
                merkle_node(self.hash_strategy, left, &right)
            }),
        };

        let mut committed = [0u8; 41];
        committed[0] = MERKLE_ROOT_PREFIX;
        committed[1..9].copy_from_slice(&self.leaves.to_le_bytes());
        committed[9..].copy_from_slice(&tree_root);
        self.hash_strategy.hash(&committed)
    }
}

impl Extend<[u8; 32]> for MerkleAccumulator {
    fn extend<I: IntoIterator<Item = [u8; 32]>>(&mut self, leaves: I) {
        for leaf in leaves {
            self.push(leaf);
        }
    }
}

/// Interior node `H(0x01 || left || right)`.
fn merkle_node(hash_strategy: HashStrategy, left: &[u8; 32], right: &[u8; 32]) -> [u8; 32] {
    let mut combined = [0u8; 65];
    combined[0] = MERKLE_NODE_PREFIX;
    combined[1..33].copy_from_slice(left);
    combined[33..].copy_from_slice(right);
    hash_strategy.hash(&combined)
}

/// `merkle_v2` root of a complete leaf list.
#[cfg(test)]
fn merkle_root(leaves: &[[u8; 32]], hash_strategy: HashStrategy) -> [u8; 32] {
    let mut tree = MerkleAccumulator::new(hash_strategy);
    tree.extend(leaves.iter().copied());
    tree.root()
}

fn collect_parquet_files(dir: &PathBuf, out: &mut Vec<PathBuf>) -> Result<()> {
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            collect_parquet_files(&path, out)?;
        } else if path
            .extension()
            .map_or(false, |ext| ext.eq_ignore_ascii_case("parquet"))
        {
            out.push(path);
        }
    }
    Ok(())
}

fn detect_partition(file_path: &str, base_path: &str) -> String {
    let rel = file_path.strip_prefix(base_path).unwrap_or(file_path);
    let rel = rel.trim_start_matches('/');
    let segments: Vec<&str> = rel.split('/').collect();
    let parts: Vec<&str> = segments
        .into_iter()
        .filter(|seg| seg.contains('=') && !seg.ends_with(".parquet"))
        .collect();

    if parts.is_empty() {
        "unpartitioned".to_string()
    } else {
        parts.join("/")
    }
}

fn now_rfc3339() -> String {
    time::OffsetDateTime::now_utc()
        .format(&Rfc3339)
        .unwrap_or_else(|_| "".to_string())
}

fn format_rfc3339(timestamp: OffsetDateTime) -> String {
    timestamp
        .format(&Rfc3339)
        .unwrap_or_else(|_| "".to_string())
}

/// Registry row key. `network` is empty for rows written before the registry
/// had a `network` column; such rows apply to any network.
fn registry_key(network: &str, chain: &str, table: &str, partition: &str) -> String {
    format!("{network}|{chain}|{table}|{partition}")
}

/// Finds the registry row for a partition: the row for this network, else a
/// row without a network (written before the `network` column existed).
/// Returns the row's key with it.
fn lookup_registry_row<'a>(
    rows: &'a HashMap<String, RegistryRow>,
    network: &str,
    chain: &str,
    table: &str,
    partition: &str,
) -> Option<(String, &'a RegistryRow)> {
    let key = registry_key(network, chain, table, partition);
    if let Some(row) = rows.get(&key) {
        return Some((key, row));
    }
    if network.is_empty() {
        return None;
    }
    let key = registry_key("", chain, table, partition);
    rows.get(&key).map(|row| (key, row))
}

/// Explains why a registry root cannot be compared with the computed root
/// because it was produced by a different hash algorithm or Merkle version.
fn incomparable_root_error(row: &RegistryRow, algorithm: &str) -> String {
    let mut reasons = Vec::new();
    if row.algorithm != algorithm {
        reasons.push(format!(
            "algorithm mismatch: registry={} runtime={}",
            row.algorithm, algorithm
        ));
    }
    if row.merkle_version != MERKLE_VERSION {
        reasons.push(format!(
            "merkle version mismatch: registry={} runtime={}; roots from different Merkle versions are not comparable, rebuild the registry from trusted data with --update-registry",
            row.merkle_version, MERKLE_VERSION
        ));
    }
    reasons.join("; ")
}

/// Registry contents as read before comparing, with the object version used
/// for a conditional S3 write.
#[derive(Debug, Clone, Default)]
struct RegistrySnapshot {
    exists: bool,
    rows: HashMap<String, RegistryRow>,
    e_tag: Option<String>,
    version: Option<String>,
}

/// One registry row this run writes. It is applied only if the row it was
/// compared against (`expected` at `compared_key`) is still there, so a
/// concurrent run's update is never silently overwritten.
#[derive(Debug, Clone)]
struct RegistryChange {
    compared_key: String,
    expected: Option<RegistryRow>,
    key: String,
    row: RegistryRow,
}

fn same_registry_root(a: &RegistryRow, b: &RegistryRow) -> bool {
    a.network == b.network
        && a.algorithm == b.algorithm
        && a.merkle_version == b.merkle_version
        && a.merkle_root == b.merkle_root
}

/// Applies this run's changes to freshly read registry rows. A change whose
/// compared row changed in the meantime is skipped when the registry already
/// holds the same root, and is a conflict otherwise.
fn apply_registry_changes(
    rows: &mut HashMap<String, RegistryRow>,
    changes: &[RegistryChange],
) -> Result<()> {
    for change in changes {
        let unchanged = match (rows.get(&change.compared_key), &change.expected) {
            (None, None) => true,
            (Some(current), Some(expected)) => same_registry_root(current, expected),
            _ => false,
        };
        if unchanged {
            if change.compared_key != change.key {
                rows.remove(&change.compared_key);
            }
            rows.insert(change.key.clone(), change.row.clone());
        } else if rows
            .get(&change.key)
            .is_some_and(|current| same_registry_root(current, &change.row))
        {
            continue;
        } else {
            return Err(anyhow!(
                "the registry row for {}:{} partition {} changed while verify was running (another verify run updated it); re-run verify",
                change.row.chain,
                change.row.table,
                change.row.partition
            ));
        }
    }
    Ok(())
}

fn load_registry(path: &str, aws: Option<&AwsConfig>) -> Result<RegistrySnapshot> {
    if path.starts_with("s3://") {
        let aws = aws.ok_or_else(|| anyhow!("AWS config required for S3 registry path"))?;
        let (bucket, key) = parse_s3_url(path)?;
        let client = aws.build_s3_client(&bucket)?;
        let location = object_store::path::Path::from(key.as_str());
        return load_registry_from_store(&client, &location)
            .with_context(|| format!("reading registry {path}"));
    }
    match std::fs::read(path) {
        Ok(data) => Ok(RegistrySnapshot {
            exists: true,
            rows: parse_registry(bytes::Bytes::from(data))
                .with_context(|| format!("reading registry {path}"))?,
            ..Default::default()
        }),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(RegistrySnapshot::default()),
        Err(err) => Err(anyhow::Error::new(err).context(format!("reading registry {path}"))),
    }
}

fn load_registry_from_store(
    store: &dyn ObjectStore,
    location: &object_store::path::Path,
) -> Result<RegistrySnapshot> {
    let result = match block_on_async(store.get(location)) {
        Ok(result) => result,
        Err(object_store::Error::NotFound { .. }) => return Ok(RegistrySnapshot::default()),
        Err(err) => return Err(err.into()),
    };
    let e_tag = result.meta.e_tag.clone();
    let version = result.meta.version.clone();
    let data = block_on_async(result.bytes())?;
    Ok(RegistrySnapshot {
        exists: true,
        rows: parse_registry(data)?,
        e_tag,
        version,
    })
}

fn parse_registry(data: bytes::Bytes) -> Result<HashMap<String, RegistryRow>> {
    let builder = ParquetRecordBatchReaderBuilder::try_new(data)?;
    let reader = builder.build()?;

    let mut rows = HashMap::new();
    for maybe_batch in reader {
        let batch = maybe_batch?;
        let schema = batch.schema();

        // Registries written before the network key have no `network` column.
        let idx_network = schema.index_of("network").ok();
        let idx_chain = schema.index_of("chain")?;
        let idx_table = schema.index_of("table")?;
        let idx_partition = schema.index_of("partition")?;
        let idx_algorithm = schema.index_of("algorithm")?;
        // Registries written before merkle_v2 have no `merkle_version` column.
        let idx_merkle_version = schema.index_of("merkle_version").ok();
        let idx_merkle_root = schema.index_of("merkle_root")?;
        let idx_updated_at = schema.index_of("updated_at")?;

        for row in 0..batch.num_rows() {
            let network = match idx_network {
                Some(idx) => required_string(batch.column(idx).as_ref(), row, "network")?,
                None => String::new(),
            };
            let chain = required_string(batch.column(idx_chain).as_ref(), row, "chain")?;
            let table = required_string(batch.column(idx_table).as_ref(), row, "table")?;
            let partition =
                required_string(batch.column(idx_partition).as_ref(), row, "partition")?;
            let algorithm =
                required_string(batch.column(idx_algorithm).as_ref(), row, "algorithm")?;
            let merkle_version = match idx_merkle_version {
                Some(idx) => required_string(batch.column(idx).as_ref(), row, "merkle_version")?,
                None => LEGACY_MERKLE_VERSION.to_string(),
            };
            let merkle_root =
                required_string(batch.column(idx_merkle_root).as_ref(), row, "merkle_root")?;
            let updated_at =
                required_string(batch.column(idx_updated_at).as_ref(), row, "updated_at")?;

            rows.insert(
                registry_key(&network, &chain, &table, &partition),
                RegistryRow {
                    network,
                    chain,
                    table,
                    partition,
                    algorithm,
                    merkle_version,
                    merkle_root,
                    updated_at,
                },
            );
        }
    }

    Ok(rows)
}

fn required_string(array: &dyn Array, row: usize, field: &str) -> Result<String> {
    if let Some(a) = array.as_any().downcast_ref::<StringArray>() {
        if a.is_null(row) {
            return Err(anyhow!("registry field '{}' is NULL", field));
        }
        return Ok(a.value(row).to_string());
    }
    if let Some(a) = array.as_any().downcast_ref::<LargeStringArray>() {
        if a.is_null(row) {
            return Err(anyhow!("registry field '{}' is NULL", field));
        }
        return Ok(a.value(row).to_string());
    }

    Err(anyhow!("registry field '{}' must be Utf8", field))
}

fn encode_registry(rows: &HashMap<String, RegistryRow>) -> Result<Vec<u8>> {
    let mut ordered: Vec<&RegistryRow> = rows.values().collect();
    ordered.sort_by(|a, b| {
        (&a.network, &a.chain, &a.table, &a.partition).cmp(&(
            &b.network,
            &b.chain,
            &b.table,
            &b.partition,
        ))
    });

    let column = |value: fn(&RegistryRow) -> &str| {
        Arc::new(StringArray::from(
            ordered.iter().map(|row| value(row)).collect::<Vec<&str>>(),
        )) as arrow::array::ArrayRef
    };
    let batch = RecordBatch::try_from_iter_with_nullable(vec![
        ("network", column(|r| r.network.as_str()), false),
        ("chain", column(|r| r.chain.as_str()), false),
        ("table", column(|r| r.table.as_str()), false),
        ("partition", column(|r| r.partition.as_str()), false),
        ("algorithm", column(|r| r.algorithm.as_str()), false),
        (
            "merkle_version",
            column(|r| r.merkle_version.as_str()),
            false,
        ),
        ("merkle_root", column(|r| r.merkle_root.as_str()), false),
        ("updated_at", column(|r| r.updated_at.as_str()), false),
    ])?;

    let mut data = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut data, batch.schema(), None)?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(data)
}

/// Writes this run's changes to the registry without losing concurrent
/// updates: locally under a lock file with an atomic replace, on S3 with a
/// single conditional put on the version read before comparing. Any conflict
/// or ambiguous response stops the owning operation without a fallback write.
fn commit_registry(
    path: &str,
    aws: Option<&AwsConfig>,
    snapshot: &RegistrySnapshot,
    changes: &[RegistryChange],
) -> Result<()> {
    if path.starts_with("s3://") {
        let aws = aws.ok_or_else(|| anyhow!("AWS config required for S3 registry path"))?;
        let (bucket, key) = parse_s3_url(path)?;
        let client = aws.build_s3_client_for_mutation(&bucket)?;
        let location = object_store::path::Path::from(key.as_str());
        commit_registry_to_store(&client, &location, snapshot, changes)
            .with_context(|| format!("writing registry {path}"))
    } else {
        commit_registry_local(Path::new(path), changes)
    }
}

fn commit_registry_local(path: &Path, changes: &[RegistryChange]) -> Result<()> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    std::fs::create_dir_all(parent)
        .with_context(|| format!("creating registry dir {}", parent.display()))?;

    // Serializes read-modify-write between verify runs; released on drop.
    let lock_path = sibling_path(path, ".lock");
    let lock = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(&lock_path)
        .with_context(|| format!("opening registry lock {}", lock_path.display()))?;
    lock.lock()
        .with_context(|| format!("locking registry lock {}", lock_path.display()))?;

    let mut rows = load_registry(&path.to_string_lossy(), None)?.rows;
    apply_registry_changes(&mut rows, changes)?;
    write_file_atomic(path, &encode_registry(&rows)?)
}

fn commit_registry_to_store(
    store: &dyn ObjectStore,
    location: &object_store::path::Path,
    snapshot: &RegistrySnapshot,
    changes: &[RegistryChange],
) -> Result<()> {
    let mut rows = snapshot.rows.clone();
    apply_registry_changes(&mut rows, changes)?;
    let mode = if !snapshot.exists {
        object_store::PutMode::Create
    } else {
        let version = object_store::UpdateVersion {
            e_tag: snapshot.e_tag.clone(),
            version: snapshot.version.clone(),
        };
        anyhow::ensure!(
            crate::dataset_lock_s3::usable_version(&version),
            "S3 registry has no usable version; refusing an unconditional overwrite"
        );
        object_store::PutMode::Update(version)
    };
    let payload = object_store::PutPayload::from(encode_registry(&rows)?);
    block_on_async(store.put_opts(location, payload, mode.into())).context(
        "conditional registry publication failed; no retry or unconditional fallback was attempted",
    )?;
    Ok(())
}

/// `path` with `suffix` appended to its file name.
fn sibling_path(path: &Path, suffix: &str) -> PathBuf {
    let mut name = path.file_name().unwrap_or_default().to_os_string();
    name.push(suffix);
    path.with_file_name(name)
}

/// Replaces `path` atomically: writes a unique temporary sibling, fsyncs it,
/// renames it over `path`, then fsyncs the directory. A crash leaves either
/// the old or the new file, never a partial one.
fn write_file_atomic(path: &Path, data: &[u8]) -> Result<()> {
    use std::io::Write;

    let tmp_path = sibling_path(path, &format!(".{}.tmp", uuid::Uuid::new_v4()));
    let write_tmp = || -> Result<()> {
        let mut file =
            File::create(&tmp_path).with_context(|| format!("creating {}", tmp_path.display()))?;
        file.write_all(data)
            .with_context(|| format!("writing {}", tmp_path.display()))?;
        file.sync_all()
            .with_context(|| format!("syncing {}", tmp_path.display()))?;
        std::fs::rename(&tmp_path, path)
            .with_context(|| format!("renaming {} to {}", tmp_path.display(), path.display()))
    };
    if let Err(err) = write_tmp() {
        let _ = std::fs::remove_file(&tmp_path);
        return Err(err);
    }
    #[cfg(unix)]
    if let Some(dir) = path.parent().filter(|dir| !dir.as_os_str().is_empty()) {
        File::open(dir)
            .and_then(|dir| dir.sync_all())
            .with_context(|| format!("syncing directory {}", dir.display()))?;
    }
    Ok(())
}

/// Partitions that `fireparq build` may still be writing, with the reason.
///
/// When the chain root has a `cursor.parquet` whose stream has not reached its
/// stop block (live mode or an interrupted build), the newest partition (the
/// highest `block_num`) and every partition holding rows beyond the cursor
/// block can still change, so their roots are provisional.
fn open_partitions(
    target: &Target,
    max_block: &HashMap<String, u64>,
    aws: Option<&AwsConfig>,
    warnings: &mut Vec<String>,
) -> HashMap<String, String> {
    let cursor_path = join_artifact_path(&target.chain_root, CURSOR_PARQUET_FILENAME);
    let state = match read_optional_bytes(&cursor_path, aws)
        .and_then(|data| data.map(|data| parse_cursor(data.into())).transpose())
    {
        Ok(Some(Some(state))) => state,
        Ok(_) => return HashMap::new(),
        Err(err) => {
            warnings.push(format!(
                "could not read {cursor_path} ({err:#}); partitions were not checked for ongoing writes"
            ));
            return HashMap::new();
        }
    };
    let cursor_block = state.last_block_num;
    if state
        .stop_block
        .is_some_and(|stop| cursor_block.saturating_add(1) >= stop)
    {
        return HashMap::new();
    }
    if max_block.is_empty() {
        warnings.push(format!(
            "{cursor_path} is at block {cursor_block} and its stream is not finished, but the table has no block_num column to tell which partition is still being written"
        ));
        return HashMap::new();
    }

    let reason = format!(
        "open: at or after the build cursor (block {cursor_block}) of an unfinished stream; not compared or recorded"
    );
    let mut open = HashMap::new();
    if let Some((newest, _)) = max_block
        .iter()
        .max_by(|a, b| a.1.cmp(b.1).then_with(|| a.0.cmp(b.0)))
    {
        open.insert(newest.clone(), reason.clone());
    }
    for (partition, block) in max_block {
        if *block > cursor_block {
            open.insert(partition.clone(), reason.clone());
        }
    }
    let mut names: Vec<&str> = open.keys().map(String::as_str).collect();
    names.sort_unstable();
    warnings.push(format!(
        "{} partition(s) may still receive rows from `fireparq build` ({cursor_path} is at block {cursor_block} and has not reached its stop block): {}; they were not compared or recorded",
        open.len(),
        names.join(", ")
    ));
    open
}

fn write_report_bytes(path: &str, aws: Option<&AwsConfig>, data: &[u8]) -> Result<()> {
    if path.starts_with("s3://") {
        let aws = aws.ok_or_else(|| anyhow!("AWS config required for S3 report publish path"))?;
        let (bucket, key) = parse_s3_url(path)?;
        let client = aws.build_s3_client_for_mutation(&bucket)?;
        let location = object_store::path::Path::from(key.as_str());
        write_report_to_store(&client, &location, data)
            .map_err(|e| anyhow!("writing report to s3://{bucket}/{}: {e}", location))?;
    } else {
        let file_path = PathBuf::from(path);
        if let Some(parent) = file_path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("creating report dir {}", parent.display()))?;
        }
        write_file_atomic(&file_path, data)?;
    }

    Ok(())
}

fn write_report_to_store(
    store: &dyn ObjectStore,
    location: &object_store::path::Path,
    data: &[u8],
) -> Result<()> {
    let payload = object_store::PutPayload::from(bytes::Bytes::copy_from_slice(data));
    block_on_async(store.put(location, payload))?;
    Ok(())
}

/// Whether a local file or S3 object exists; lookup errors count as absent.
fn artifact_exists(path: &str, aws: Option<&AwsConfig>) -> bool {
    if !path.starts_with("s3://") {
        return Path::new(path).is_file();
    }
    let (Some(aws), Ok((bucket, key))) = (aws, parse_s3_url(path)) else {
        return false;
    };
    let Ok(client) = aws.build_s3_client(&bucket) else {
        return false;
    };
    let location = object_store::path::Path::from(key.as_str());
    block_on_async(async { client.head(&location).await }).is_ok()
}

/// Reads a local file or S3 object, or `None` when it does not exist.
fn read_optional_bytes(path: &str, aws: Option<&AwsConfig>) -> Result<Option<Vec<u8>>> {
    if path.starts_with("s3://") {
        let aws = aws.ok_or_else(|| anyhow!("AWS config required for S3 path {path}"))?;
        let (bucket, key) = parse_s3_url(path)?;
        let client = aws.build_s3_client(&bucket)?;
        let location = object_store::path::Path::from(key.as_str());
        match block_on_async(async { client.get(&location).await?.bytes().await }) {
            Ok(data) => Ok(Some(data.to_vec())),
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(err) => Err(anyhow!("reading {path}: {err}")),
        }
    } else {
        match std::fs::read(path) {
            Ok(data) => Ok(Some(data)),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(err) => Err(anyhow::Error::new(err).context(format!("reading {path}"))),
        }
    }
}

#[cfg(test)]
mod tests {
    mod ownership;
    use super::{
        append_batch_leaves, commit_registry_local, commit_registry_to_store, file_layout,
        join_artifact_path, legacy_default_registry_path, load_registry, load_registry_from_store,
        merkle_root, registry_key, verify_parquet, FileLayout, FindingStatus, HashStrategy,
        MerkleAccumulator, Prefetcher, RegistryChange, RegistryRow, VerifyCheck, VerifyOptions,
        VerifyProfile, VerifyReport, VerifyScope, MERKLE_ROOTS_FILENAME,
    };
    use crate::cursor::{save_cursor_parquet, CursorState};
    use anyhow::Result;
    use arrow::array::{
        ArrayRef, StringArray, TimestampMillisecondArray, TimestampSecondArray, UInt64Array,
    };
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;
    use parquet::file::metadata::KeyValue;
    use parquet::file::properties::WriterProperties;
    use std::collections::HashMap;
    use std::path::Path;
    use std::sync::Arc;

    fn write_block_nums(path: &Path, values: &[u64]) {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "block_num",
            DataType::UInt64,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(UInt64Array::from(values.to_vec()))],
        )
        .unwrap();
        let mut writer = ArrowWriter::try_new(std::fs::File::create(path).unwrap(), schema, None)
            .expect("parquet writer");
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    fn roots_opts(registry_path: &Path) -> VerifyOptions {
        let mut opts = base_opts();
        opts.checks = vec![VerifyCheck::Roots];
        opts.registry_path = Some(registry_path.display().to_string());
        opts
    }

    #[test]
    fn merkle_root_distinguishes_duplicated_trailing_leaf() {
        let (a, b, c) = ([1u8; 32], [2u8; 32], [3u8; 32]);
        for strategy in [HashStrategy::Keccak256, HashStrategy::Sha256] {
            assert_ne!(
                merkle_root(&[a, b, c], strategy),
                merkle_root(&[a, b, c, c], strategy)
            );
            assert_ne!(merkle_root(&[a], strategy), merkle_root(&[a, a], strategy));
        }
    }

    #[test]
    fn merkle_root_matches_golden_vectors() {
        // Independently computed with Python hashlib from the merkle_v2 spec in
        // docs/verifiability-hash-strategy.md.
        let leaves = [[0x11u8; 32], [0x22u8; 32], [0x33u8; 32]];
        let root = |leaves: &[[u8; 32]]| hex::encode(merkle_root(leaves, HashStrategy::Sha256));

        assert_eq!(
            root(&leaves),
            "79bb7e7bb65485d80aa1d3dff65e289bf1bb3b61d907f4e74479ca86f5e9284f"
        );
        assert_eq!(
            root(&[leaves[0], leaves[1], leaves[2], leaves[2]]),
            "c8a726a66e34b05683e336c825e06e6c0344a7374461742b971826398bbe4071"
        );
        assert_eq!(
            root(&leaves[..1]),
            "fa7177e96fa95228912cf7bf30ef2d53380818778322c98117119b33fbce0069"
        );
        assert_eq!(
            root(&[]),
            "f0c1e0b4cd1983b9c92909f8145cc102993e4c797489c0ba98639fd93056b82f"
        );
    }

    #[test]
    fn verify_detects_duplicated_trailing_row() {
        let dir = tempfile::TempDir::new().unwrap();
        let data = dir.path().join("blocks");
        let file = data.join("date=2024-01-01").join("part-0.parquet");
        let registry = dir.path().join("merkle_roots.parquet");
        let opts = roots_opts(&registry);

        write_block_nums(&file, &[1, 2, 3]);
        let first = verify_parquet(data.to_str().unwrap(), None, &opts).unwrap();
        assert_eq!(first.summary.missing_expected, 1);
        assert!(first.summary.wrote_registry);

        let second = verify_parquet(data.to_str().unwrap(), None, &opts).unwrap();
        assert_eq!(second.summary.matches, 1);
        assert!(second.is_valid());

        // A final block re-emitted on resume duplicates the trailing row.
        write_block_nums(&file, &[1, 2, 3, 3]);
        let tampered = verify_parquet(data.to_str().unwrap(), None, &opts).unwrap();
        assert_eq!(tampered.summary.mismatches, 1);
        assert!(matches!(
            tampered.findings[0].status,
            FindingStatus::Mismatch
        ));
        assert!(!tampered.is_valid());
    }

    #[test]
    fn row_leaf_is_domain_separated_from_nodes() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "block_num",
            DataType::UInt64,
            false,
        )]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(UInt64Array::from(vec![1u64]))]).unwrap();
        let mut leaves = Vec::new();
        append_batch_leaves(&batch, HashStrategy::Sha256, &mut leaves).unwrap();

        // sha256(0x00 || u32le(9) || "block_num" || 0x01 || u32le(1) || "1")
        assert_eq!(
            hex::encode(leaves[0]),
            "39bb9c5c843e0b59d89e2f1df4f973d681e687aad5a87b4557e1ab30fa6037fd"
        );
    }

    /// Writes a registry in the pre-merkle_v2 layout (no `merkle_version` column).
    fn write_legacy_registry(path: &Path, rows: &[(&str, &str)]) {
        let columns = [
            "chain",
            "table",
            "partition",
            "algorithm",
            "merkle_root",
            "updated_at",
        ];
        let schema = Arc::new(Schema::new(
            columns
                .iter()
                .map(|name| Field::new(*name, DataType::Utf8, false))
                .collect::<Vec<_>>(),
        ));
        let column = |values: Vec<&str>| Arc::new(StringArray::from(values)) as ArrayRef;
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                column(rows.iter().map(|_| "evm").collect()),
                column(rows.iter().map(|_| "blocks").collect()),
                column(rows.iter().map(|(partition, _)| *partition).collect()),
                column(rows.iter().map(|_| "keccak256").collect()),
                column(rows.iter().map(|(_, root)| *root).collect()),
                column(rows.iter().map(|_| "2026-01-01T00:00:00Z").collect()),
            ],
        )
        .unwrap();
        let mut writer =
            ArrowWriter::try_new(std::fs::File::create(path).unwrap(), schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    /// Writes `columns` as one parquet file and returns its verified root.
    fn root_of(columns: Vec<(&str, ArrayRef)>) -> String {
        let dir = tempfile::TempDir::new().unwrap();
        let file = dir.path().join("data").join("part-0.parquet");
        std::fs::create_dir_all(file.parent().unwrap()).unwrap();
        let batch = RecordBatch::try_from_iter(columns).unwrap();
        let mut writer =
            ArrowWriter::try_new(std::fs::File::create(&file).unwrap(), batch.schema(), None)
                .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let opts = roots_opts(&dir.path().join("merkle_roots.parquet"));
        let data = dir.path().join("data");
        let report = verify_parquet(data.to_str().unwrap(), None, &opts).unwrap();
        report.findings[0].computed_root.clone()
    }

    #[test]
    fn verify_roots_distinguish_null_from_sentinel_string() {
        let null = root_of(vec![(
            "memo",
            Arc::new(StringArray::from(vec![None::<&str>])) as ArrayRef,
        )]);
        let sentinel = root_of(vec![(
            "memo",
            Arc::new(StringArray::from(vec!["<null>"])) as ArrayRef,
        )]);
        assert_ne!(null, sentinel);
    }

    #[test]
    fn verify_roots_are_stable_across_timestamp_units() {
        // The canonical `timestamp` column moves from seconds to milliseconds
        // (#491); identical instants must keep identical roots.
        let seconds = root_of(vec![(
            "timestamp",
            Arc::new(TimestampSecondArray::from(vec![1_700_000_000]).with_timezone("UTC"))
                as ArrayRef,
        )]);
        let millis = root_of(vec![(
            "timestamp",
            Arc::new(TimestampMillisecondArray::from(vec![1_700_000_000_000]).with_timezone("UTC"))
                as ArrayRef,
        )]);
        assert_eq!(seconds, millis);

        let later = root_of(vec![(
            "timestamp",
            Arc::new(TimestampMillisecondArray::from(vec![1_700_000_000_001]).with_timezone("UTC"))
                as ArrayRef,
        )]);
        assert_ne!(millis, later);
    }

    #[test]
    fn legacy_registry_rows_are_flagged_and_rebuilt_on_update() {
        let dir = tempfile::TempDir::new().unwrap();
        let data = dir.path().join("blocks");
        write_block_nums(
            &data.join("date=2024-01-01").join("part-0.parquet"),
            &[1, 2, 3],
        );
        let registry = dir.path().join("merkle_roots.parquet");
        let legacy_root = "11".repeat(32);
        write_legacy_registry(
            &registry,
            &[
                ("date=2023-12-31", legacy_root.as_str()),
                ("date=2024-01-01", legacy_root.as_str()),
            ],
        );
        let registry_str = registry.display().to_string();
        let mut opts = roots_opts(&registry);

        let rows = load_registry(&registry_str, None).unwrap().rows;
        assert!(rows.values().all(|r| r.merkle_version == "merkle_v1"));
        assert!(rows.values().all(|r| r.network.is_empty()));

        let legacy = verify_parquet(data.to_str().unwrap(), None, &opts).unwrap();
        assert_eq!(legacy.merkle_version, "merkle_v2");
        assert_eq!(legacy.summary.mismatches, 1);
        assert!(!legacy.summary.wrote_registry);
        assert!(!legacy.is_valid());
        let error = legacy.findings[0].error.as_deref().unwrap();
        assert!(
            error.starts_with("merkle version mismatch: registry=merkle_v1 runtime=merkle_v2"),
            "unexpected error: {error}"
        );
        assert!(error.contains("--update-registry"));

        // The rebuild accepts the current data: the row is `updated` and the run passes.
        opts.update_registry = true;
        let rebuild = verify_parquet(data.to_str().unwrap(), None, &opts).unwrap();
        assert!(rebuild.summary.wrote_registry);
        assert_eq!(rebuild.summary.updated, 1);
        assert_eq!(rebuild.summary.mismatches, 0);
        assert!(matches!(rebuild.findings[0].status, FindingStatus::Updated));
        assert_eq!(
            rebuild.findings[0].expected_root.as_deref(),
            Some(legacy_root.as_str())
        );
        assert!(rebuild.is_valid());

        let network = rebuild.network.clone().unwrap_or_default();
        let rows = load_registry(&registry_str, None).unwrap().rows;
        let rebuilt = &rows[&registry_key(&network, "evm", "blocks", "date=2024-01-01")];
        assert_eq!(rebuilt.merkle_version, "merkle_v2");
        assert_eq!(rebuilt.merkle_root, rebuild.findings[0].computed_root);
        // The legacy row it replaced is gone.
        assert!(!rows.contains_key(&registry_key("", "evm", "blocks", "date=2024-01-01")));
        // Rows outside the scanned data keep an explicit legacy label.
        let untouched = &rows[&registry_key("", "evm", "blocks", "date=2023-12-31")];
        assert_eq!(untouched.merkle_version, "merkle_v1");
        assert_eq!(untouched.merkle_root, legacy_root);

        opts.update_registry = false;
        let after = verify_parquet(data.to_str().unwrap(), None, &opts).unwrap();
        assert_eq!(after.summary.matches, 1);
        assert!(after.is_valid());
    }

    fn base_opts() -> VerifyOptions {
        VerifyOptions {
            // Test fixtures have no firehose-parquet.block_type metadata.
            chain: Some("evm".to_string()),
            table: None,
            hash_strategy: None,
            checks: vec![],
            profile: VerifyProfile::Standard,
            scope: VerifyScope::Table,
            no_fail_fast: false,
            report_json: None,
            publish_report: false,
            publish_report_path: None,
            registry_path: None,
            update_registry: false,
        }
    }

    #[test]
    fn profile_standard_defaults_to_roots_and_protocol() {
        let opts = base_opts();
        let checks = opts.effective_checks();

        assert!(checks.contains(&VerifyCheck::Roots));
        assert!(checks.contains(&VerifyCheck::Protocol));
        assert_eq!(checks.len(), 2);
    }

    #[test]
    fn explicit_checks_override_profile_defaults() {
        let mut opts = base_opts();
        opts.profile = VerifyProfile::Deep;
        opts.checks = vec![VerifyCheck::Roots];

        let checks = opts.effective_checks();
        assert!(checks.contains(&VerifyCheck::Roots));
        assert_eq!(checks.len(), 1);
    }

    #[test]
    fn file_layout_splits_chain_root_and_table_per_network() {
        let layout =
            file_layout("/out/mainnet/blocks/year=2025/month=12/date=13/part-a-000001.parquet");
        assert_eq!(
            layout,
            FileLayout {
                chain_root: "/out/mainnet".to_string(),
                table: Some("blocks".to_string()),
                chain_root_name: Some("mainnet".to_string()),
            }
        );
        assert_eq!(
            file_layout("/out/sepolia/blocks/year=2025/part-a-000001.parquet").chain_root,
            "/out/sepolia"
        );
        // Unpartitioned table: files sit directly in the table directory.
        assert_eq!(
            file_layout("/out/mainnet/blocks/part-a-000001.parquet").table,
            Some("blocks".to_string())
        );

        let s3 = file_layout("s3://bucket/data/mainnet/transactions/date=2024-01-01/x.parquet");
        assert_eq!(s3.chain_root, "s3://bucket/data/mainnet");
        assert_eq!(s3.table, Some("transactions".to_string()));
        let bucket_root = file_layout("s3://bucket/blocks/x.parquet");
        assert_eq!(bucket_root.chain_root, "s3://bucket");
        assert_eq!(bucket_root.chain_root_name, None);
        assert_eq!(file_layout("s3://bucket/x.parquet").table, None);

        assert_eq!(
            join_artifact_path("s3://bucket/data/mainnet", MERKLE_ROOTS_FILENAME),
            "s3://bucket/data/mainnet/merkle_roots.parquet"
        );
        assert_eq!(
            join_artifact_path("/", MERKLE_ROOTS_FILENAME),
            "/merkle_roots.parquet"
        );
    }

    #[test]
    fn file_layout_matches_path_components_not_substrings() {
        // The old substring match cut paths at the first "/blocks".
        let layout = file_layout("/data/blocks-archive/mainnet/transactions/date=1/x.parquet");
        assert_eq!(layout.chain_root, "/data/blocks-archive/mainnet");
        assert_eq!(layout.table, Some("transactions".to_string()));
        assert_eq!(
            file_layout("/out/mainnet/blocks_v2/x.parquet").table,
            Some("blocks_v2".to_string())
        );
    }

    #[test]
    fn legacy_default_registry_paths_are_the_old_shared_locations() {
        assert_eq!(
            legacy_default_registry_path("/out/mainnet/blocks", "evm", "blocks"),
            "/out/mainnet/evm/mainnet/merkle_roots.parquet"
        );
        // Every S3 network mapped to the same object.
        for network in ["mainnet", "sepolia"] {
            assert_eq!(
                legacy_default_registry_path(
                    &format!("s3://bucket/{network}/blocks"),
                    "evm",
                    "blocks"
                ),
                "s3://bucket/evm/mainnet/merkle_roots.parquet"
            );
        }
    }

    /// Writes a `build`-style table file with `firehose-parquet.*` metadata.
    fn write_table_file(
        path: &Path,
        values: &[u64],
        block_type: Option<&str>,
        chain_name: Option<&str>,
    ) {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let mut kvs = Vec::new();
        if let Some(block_type) = block_type {
            kvs.push(KeyValue::new(
                "firehose-parquet.block_type".to_string(),
                block_type.to_string(),
            ));
        }
        if let Some(chain_name) = chain_name {
            kvs.push(KeyValue::new(
                "firehose-parquet.chain_name".to_string(),
                chain_name.to_string(),
            ));
        }
        let props = WriterProperties::builder()
            .set_key_value_metadata(Some(kvs))
            .build();
        let batch = RecordBatch::try_from_iter(vec![(
            "block_num",
            Arc::new(UInt64Array::from(values.to_vec())) as ArrayRef,
        )])
        .unwrap();
        let mut writer = ArrowWriter::try_new(
            std::fs::File::create(path).unwrap(),
            batch.schema(),
            Some(props),
        )
        .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    /// Roots-only options that infer everything from the dataset.
    fn inferred_opts() -> VerifyOptions {
        let mut opts = base_opts();
        opts.chain = None;
        opts.checks = vec![VerifyCheck::Roots];
        opts
    }

    fn table_file(root: &Path, network: &str, table: &str) -> std::path::PathBuf {
        root.join(network)
            .join(table)
            .join("date=2024-01-01")
            .join("part-0.parquet")
    }

    fn verify_dir(dir: &Path, opts: &VerifyOptions) -> Result<VerifyReport> {
        verify_parquet(dir.to_str().unwrap(), None, opts)
    }

    #[test]
    fn target_is_inferred_from_metadata_and_layout() {
        let dir = tempfile::TempDir::new().unwrap();
        let root = std::fs::canonicalize(dir.path()).unwrap();
        write_table_file(
            &table_file(&root, "mainnet", "blocks"),
            &[1, 2],
            Some("evm"),
            Some("mainnet"),
        );

        let report = verify_dir(&root.join("mainnet/blocks"), &inferred_opts()).unwrap();
        assert_eq!(report.chain, "evm");
        assert_eq!(report.table, "blocks");
        assert_eq!(report.network.as_deref(), Some("mainnet"));
        assert_eq!(report.algorithm, "keccak256");
        let chain_root = root.join("mainnet").display().to_string();
        assert_eq!(
            report.registry_path,
            format!("{chain_root}/merkle_roots.parquet")
        );
        assert_eq!(
            report.suggested_run_report_path,
            format!("{chain_root}/verify_runs/{}/report.json", report.run_id)
        );

        // Matching explicit flags are accepted.
        let mut opts = inferred_opts();
        opts.chain = Some("EVM".to_string());
        opts.table = Some("blocks".to_string());
        assert!(verify_dir(&root.join("mainnet/blocks"), &opts).is_ok());
    }

    #[test]
    fn explicit_flags_that_conflict_with_the_dataset_are_errors() {
        let dir = tempfile::TempDir::new().unwrap();
        let root = dir.path();
        write_table_file(
            &table_file(root, "mainnet", "blocks"),
            &[1],
            Some("evm"),
            Some("mainnet"),
        );
        let data = root.join("mainnet/blocks");

        let mut opts = inferred_opts();
        opts.chain = Some("bitcoin".to_string());
        let err = verify_dir(&data, &opts).unwrap_err().to_string();
        assert!(err.contains("--chain bitcoin conflicts"), "{err}");

        let mut opts = inferred_opts();
        opts.table = Some("transactions".to_string());
        let err = verify_dir(&data, &opts).unwrap_err().to_string();
        assert!(err.contains("--table transactions conflicts"), "{err}");

        // Without metadata, the chain must be named.
        write_table_file(&table_file(root, "legacy", "blocks"), &[1], None, None);
        let err = verify_dir(&root.join("legacy/blocks"), &inferred_opts())
            .unwrap_err()
            .to_string();
        assert!(err.contains("pass --chain"), "{err}");
    }

    #[test]
    fn verify_rejects_paths_that_mix_tables_or_networks() {
        let dir = tempfile::TempDir::new().unwrap();
        let root = dir.path();
        write_table_file(
            &table_file(root, "mainnet", "blocks"),
            &[1],
            Some("evm"),
            Some("mainnet"),
        );
        write_table_file(
            &table_file(root, "mainnet", "transactions"),
            &[1],
            Some("evm"),
            Some("mainnet"),
        );
        let err = verify_dir(&root.join("mainnet"), &inferred_opts())
            .unwrap_err()
            .to_string();
        assert!(err.contains("more than one table"), "{err}");

        let mixed = root.join("mixed/blocks/date=2024-01-01");
        write_table_file(
            &mixed.join("part-0.parquet"),
            &[1],
            Some("evm"),
            Some("mainnet"),
        );
        write_table_file(
            &mixed.join("part-1.parquet"),
            &[2],
            Some("evm"),
            Some("sepolia"),
        );
        let err = verify_dir(&root.join("mixed/blocks"), &inferred_opts())
            .unwrap_err()
            .to_string();
        assert!(err.contains("verify one network at a time"), "{err}");
    }

    #[test]
    fn registries_are_per_network_and_never_scanned_as_data() {
        let dir = tempfile::TempDir::new().unwrap();
        let root = dir.path();
        for (network, values) in [("mainnet", [1u64, 2]), ("sepolia", [7, 8])] {
            write_table_file(
                &table_file(root, network, "blocks"),
                &values,
                Some("evm"),
                Some(network),
            );
            // Reserved artifacts beside the table and inside the table directory.
            write_block_nums(&root.join(network).join("cursor.parquet"), &[99]);
            write_block_nums(&root.join(network).join("partitions.parquet"), &[99]);
            write_block_nums(
                &root
                    .join(network)
                    .join("blocks")
                    .join("merkle_roots.parquet"),
                &[99],
            );
            write_block_nums(
                &root.join(network).join("verify_runs/run-0/roots.parquet"),
                &[99],
            );
        }

        let mut roots = Vec::new();
        for network in ["mainnet", "sepolia"] {
            // This chain root holds a single table, so it can be verified directly;
            // three runs show the registry it writes is never hashed as data.
            let data = root.join(network);
            let runs: Vec<VerifyReport> = (0..3)
                .map(|_| verify_dir(&data, &inferred_opts()).unwrap())
                .collect();
            assert_eq!(runs[0].summary.missing_expected, 1);
            assert!(runs[0].summary.wrote_registry);
            for run in &runs[1..] {
                assert_eq!(run.summary.partitions_scanned, 1);
                assert_eq!(run.summary.matches, 1);
                assert!(!run.summary.wrote_registry);
                assert!(run.is_valid());
            }
            assert!(runs[0]
                .registry_path
                .ends_with(&format!("/{network}/merkle_roots.parquet")));
            roots.push(runs[0].findings[0].computed_root.clone());
        }
        assert_ne!(roots[0], roots[1]);

        let mainnet = load_registry(
            &root
                .join("mainnet/merkle_roots.parquet")
                .display()
                .to_string(),
            None,
        )
        .unwrap()
        .rows;
        assert_eq!(mainnet.len(), 1);
        assert_eq!(
            mainnet[&registry_key("mainnet", "evm", "blocks", "date=2024-01-01")].merkle_root,
            roots[0]
        );
    }

    #[test]
    fn tables_of_one_network_share_a_registry_without_colliding() {
        let dir = tempfile::TempDir::new().unwrap();
        let root = dir.path();
        write_table_file(
            &table_file(root, "mainnet", "blocks"),
            &[1],
            Some("evm"),
            Some("mainnet"),
        );
        write_table_file(
            &table_file(root, "mainnet", "transactions"),
            &[2],
            Some("evm"),
            Some("mainnet"),
        );
        for _ in 0..2 {
            for table in ["blocks", "transactions"] {
                let report =
                    verify_dir(&root.join("mainnet").join(table), &inferred_opts()).unwrap();
                assert!(report.is_valid());
            }
        }
        let rows = load_registry(
            &root
                .join("mainnet/merkle_roots.parquet")
                .display()
                .to_string(),
            None,
        )
        .unwrap()
        .rows;
        assert_eq!(rows.len(), 2);
        for table in ["blocks", "transactions"] {
            assert!(rows.contains_key(&registry_key("mainnet", "evm", table, "date=2024-01-01")));
        }
    }

    #[test]
    fn registry_at_old_default_location_is_reported() {
        let dir = tempfile::TempDir::new().unwrap();
        let root = std::fs::canonicalize(dir.path()).unwrap();
        write_table_file(
            &table_file(&root, "mainnet", "blocks"),
            &[1],
            Some("evm"),
            Some("mainnet"),
        );
        let legacy = root.join("mainnet/evm/mainnet/merkle_roots.parquet");
        std::fs::create_dir_all(legacy.parent().unwrap()).unwrap();
        write_legacy_registry(&legacy, &[("date=2024-01-01", &"11".repeat(32))]);

        let report = verify_dir(&root.join("mainnet/blocks"), &inferred_opts()).unwrap();
        assert_eq!(report.warnings.len(), 1, "{:?}", report.warnings);
        assert!(report.warnings[0].contains(&legacy.display().to_string()));
        assert!(report.warnings[0].contains("old default location"));
        // The old registry is not used: the partition is a missing root in the new one.
        assert_eq!(report.summary.missing_expected, 1);
        assert!(report
            .registry_path
            .ends_with("/mainnet/merkle_roots.parquet"));

        // Data laid out as `<root>/evm/mainnet/<table>` already used the new
        // location, so there is nothing to warn about.
        write_table_file(
            &table_file(&root.join("evm"), "mainnet", "blocks"),
            &[1],
            Some("evm"),
            Some("mainnet"),
        );
        for _ in 0..2 {
            let report = verify_dir(&root.join("evm/mainnet/blocks"), &inferred_opts()).unwrap();
            assert!(report.warnings.is_empty(), "{:?}", report.warnings);
            assert!(report
                .registry_path
                .ends_with("/evm/mainnet/merkle_roots.parquet"));
        }

        // An explicit --registry-path is the operator's choice: no warning.
        let mut opts = inferred_opts();
        opts.registry_path = Some(legacy.display().to_string());
        assert!(verify_dir(&root.join("mainnet/blocks"), &opts)
            .unwrap()
            .warnings
            .is_empty());
    }

    /// Writes an EVM `transactions` file; a row whose `block_number` differs
    /// from `block_num` fails the protocol check.
    fn write_tx_file(path: &Path, rows: &[(u64, u64)]) {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let kvs = vec![
            KeyValue::new("firehose-parquet.block_type".to_string(), "evm".to_string()),
            KeyValue::new(
                "firehose-parquet.chain_name".to_string(),
                "mainnet".to_string(),
            ),
        ];
        let props = WriterProperties::builder()
            .set_key_value_metadata(Some(kvs))
            .build();
        let column = |values: Vec<u64>| Arc::new(UInt64Array::from(values)) as ArrayRef;
        let batch = RecordBatch::try_from_iter(vec![
            ("block_num", column(rows.iter().map(|r| r.0).collect())),
            ("block_number", column(rows.iter().map(|r| r.1).collect())),
        ])
        .unwrap();
        let mut writer = ArrowWriter::try_new(
            std::fs::File::create(path).unwrap(),
            batch.schema(),
            Some(props),
        )
        .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    #[test]
    fn fail_fast_partial_partition_is_never_recorded() {
        let dir = tempfile::TempDir::new().unwrap();
        let data = dir.path().join("mainnet/transactions");
        let day1 = data.join("date=2024-01-01");
        write_tx_file(&day1.join("part-0.parquet"), &[(1, 2)]); // protocol failure
        write_tx_file(&day1.join("part-1.parquet"), &[(3, 3)]);
        write_tx_file(&data.join("date=2024-01-02/part-0.parquet"), &[(4, 4)]);
        let registry = dir.path().join("mainnet/merkle_roots.parquet");
        let mut opts = base_opts();
        opts.chain = None;

        // Fail-fast stops after part-0: day 1 is only partly read.
        let report = verify_dir(&data, &opts).unwrap();
        assert!(report.summary.protocol_failed > 0);
        assert!(!report.is_valid());
        assert!(report.findings.is_empty(), "{:?}", report.findings);
        assert!(!report.summary.wrote_registry);
        assert!(!registry.exists());
        assert!(
            report.warnings.iter().any(|w| {
                w.contains("stopped at the first protocol failure") && w.contains("date=2024-01-01")
            }),
            "{:?}",
            report.warnings
        );

        // With every file read, roots are complete, but a failing run still writes nothing.
        opts.no_fail_fast = true;
        let report = verify_dir(&data, &opts).unwrap();
        assert_eq!(report.summary.missing_expected, 2);
        assert!(!report.summary.wrote_registry);
        assert!(!registry.exists());
        assert!(report
            .warnings
            .iter()
            .any(|w| w.contains("the registry was not updated: protocol checks failed")));
    }

    #[test]
    fn differing_roots_block_registry_writes_unless_update_registry() {
        let dir = tempfile::TempDir::new().unwrap();
        let root = dir.path();
        let data = root.join("mainnet/blocks");
        let registry = root
            .join("mainnet/merkle_roots.parquet")
            .display()
            .to_string();
        let day1 = table_file(root, "mainnet", "blocks");
        write_table_file(&day1, &[1, 2], Some("evm"), Some("mainnet"));
        assert!(
            verify_dir(&data, &inferred_opts())
                .unwrap()
                .summary
                .wrote_registry
        );

        // Day 1 changes and day 2 appears.
        write_table_file(&day1, &[1, 2, 2], Some("evm"), Some("mainnet"));
        write_table_file(
            &data.join("date=2024-01-02/part-0.parquet"),
            &[3],
            Some("evm"),
            Some("mainnet"),
        );
        let mut opts = inferred_opts();
        opts.no_fail_fast = true;
        let failing = verify_dir(&data, &opts).unwrap();
        assert_eq!(failing.summary.mismatches, 1);
        assert_eq!(failing.summary.missing_expected, 1);
        assert!(!failing.summary.wrote_registry);
        assert!(!failing.is_valid());
        assert!(failing
            .warnings
            .iter()
            .any(|w| w.contains("1 partition root(s) differ from the registry")));
        assert_eq!(load_registry(&registry, None).unwrap().rows.len(), 1);

        // --update-registry accepts the data (even with fail-fast): the run passes.
        let mut opts = inferred_opts();
        opts.update_registry = true;
        let accepted = verify_dir(&data, &opts).unwrap();
        assert_eq!(accepted.summary.updated, 1);
        assert_eq!(accepted.summary.missing_expected, 1);
        assert_eq!(accepted.summary.mismatches, 0);
        assert!(accepted.summary.wrote_registry);
        assert!(accepted.is_valid());

        let after = verify_dir(&data, &inferred_opts()).unwrap();
        assert_eq!(after.summary.matches, 2);
        assert!(after.is_valid());
    }

    fn save_cursor(chain_root: &Path, last_block_num: u64, stop_block: Option<u64>) {
        let state = CursorState {
            cursor: "cursor".to_string(),
            last_block_num,
            stop_block,
            ..Default::default()
        };
        save_cursor_parquet(&chain_root.join("cursor.parquet"), &state).unwrap();
    }

    fn open_partitions_of(report: &VerifyReport) -> Vec<&str> {
        let mut open: Vec<&str> = report
            .findings
            .iter()
            .filter(|f| matches!(f.status, FindingStatus::Open))
            .map(|f| f.partition.as_str())
            .collect();
        open.sort_unstable();
        open
    }

    #[test]
    fn partitions_still_being_written_are_not_compared_or_recorded() {
        let dir = tempfile::TempDir::new().unwrap();
        let root = dir.path();
        let chain_root = root.join("mainnet");
        let data = chain_root.join("blocks");
        let registry = chain_root
            .join("merkle_roots.parquet")
            .display()
            .to_string();
        for (day, blocks) in [
            ("2024-01-01", [1u64, 2]),
            ("2024-01-02", [3, 4]),
            ("2024-01-03", [5, 6]),
        ] {
            write_table_file(
                &data.join(format!("date={day}")).join("part-0.parquet"),
                &blocks,
                Some("evm"),
                Some("mainnet"),
            );
        }

        // A live build at block 6 may still append to the newest partition.
        save_cursor(&chain_root, 6, None);
        let report = verify_dir(&data, &inferred_opts()).unwrap();
        assert_eq!(open_partitions_of(&report), ["date=2024-01-03"]);
        assert_eq!(report.summary.open_partitions, 1);
        assert_eq!(report.summary.missing_expected, 2);
        assert!(report.is_valid());
        assert!(report
            .warnings
            .iter()
            .any(|w| w.contains("may still receive rows")));
        let rows = load_registry(&registry, None).unwrap().rows;
        assert_eq!(rows.len(), 2);
        assert!(!rows.values().any(|r| r.partition == "date=2024-01-03"));

        // Rows written after the last cursor save (block 2) are open too.
        save_cursor(&chain_root, 2, Some(100));
        let report = verify_dir(&data, &inferred_opts()).unwrap();
        assert_eq!(
            open_partitions_of(&report),
            ["date=2024-01-02", "date=2024-01-03"]
        );
        assert_eq!(report.summary.matches, 1);

        // A bounded build that reached its stop block has nothing open.
        save_cursor(&chain_root, 6, Some(7));
        let report = verify_dir(&data, &inferred_opts()).unwrap();
        assert!(open_partitions_of(&report).is_empty());
        assert_eq!(report.summary.matches, 2);
        assert_eq!(report.summary.missing_expected, 1);
        assert!(report.summary.wrote_registry);
    }

    #[test]
    fn a_shared_registry_keeps_networks_apart() {
        let dir = tempfile::TempDir::new().unwrap();
        let root = dir.path();
        let registry = root.join("shared/merkle_roots.parquet");
        let mut opts = inferred_opts();
        opts.registry_path = Some(registry.display().to_string());
        for (network, blocks) in [("mainnet", [1u64, 2]), ("sepolia", [7, 8])] {
            write_table_file(
                &table_file(root, network, "blocks"),
                &blocks,
                Some("evm"),
                Some(network),
            );
        }
        // Same chain, table and partition names: only the network tells them apart.
        for _ in 0..2 {
            for network in ["mainnet", "sepolia"] {
                let report = verify_dir(&root.join(network).join("blocks"), &opts).unwrap();
                assert!(report.is_valid(), "{network}: {:?}", report.findings);
            }
        }
        let rows = load_registry(&registry.display().to_string(), None)
            .unwrap()
            .rows;
        assert_eq!(rows.len(), 2);
        for network in ["mainnet", "sepolia"] {
            assert!(rows.contains_key(&registry_key(network, "evm", "blocks", "date=2024-01-01")));
        }
    }

    #[test]
    fn rows_without_a_network_still_match() {
        let dir = tempfile::TempDir::new().unwrap();
        let root = dir.path();
        write_table_file(
            &table_file(root, "mainnet", "blocks"),
            &[1, 2],
            Some("evm"),
            Some("mainnet"),
        );
        let data = root.join("mainnet/blocks");
        let registry = root.join("mainnet/merkle_roots.parquet");
        let computed = verify_dir(&data, &inferred_opts()).unwrap().findings[0]
            .computed_root
            .clone();

        // Rewrite the registry in the layout from before the `network` column.
        let schema = Arc::new(Schema::new(
            [
                "chain",
                "table",
                "partition",
                "algorithm",
                "merkle_version",
                "merkle_root",
                "updated_at",
            ]
            .iter()
            .map(|name| Field::new(*name, DataType::Utf8, false))
            .collect::<Vec<_>>(),
        ));
        let values = [
            "evm",
            "blocks",
            "date=2024-01-01",
            "keccak256",
            "merkle_v2",
            computed.as_str(),
            "2026-01-01T00:00:00Z",
        ];
        let batch = RecordBatch::try_new(
            schema.clone(),
            values
                .iter()
                .map(|v| Arc::new(StringArray::from(vec![*v])) as ArrayRef)
                .collect(),
        )
        .unwrap();
        let mut writer =
            ArrowWriter::try_new(std::fs::File::create(&registry).unwrap(), schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let report = verify_dir(&data, &inferred_opts()).unwrap();
        assert_eq!(report.summary.matches, 1);
        assert!(!report.summary.wrote_registry);
    }

    fn registry_row(network: &str, partition: &str, root: &str) -> RegistryRow {
        RegistryRow {
            network: network.to_string(),
            chain: "evm".to_string(),
            table: "blocks".to_string(),
            partition: partition.to_string(),
            algorithm: "keccak256".to_string(),
            merkle_version: "merkle_v2".to_string(),
            merkle_root: root.to_string(),
            updated_at: "2026-01-01T00:00:00Z".to_string(),
        }
    }

    /// A missing-root fill planned against a registry without that row.
    fn fill(row: RegistryRow) -> RegistryChange {
        let key = registry_key(&row.network, &row.chain, &row.table, &row.partition);
        RegistryChange {
            compared_key: key.clone(),
            expected: None,
            key,
            row,
        }
    }

    #[test]
    fn local_commits_merge_concurrent_runs_and_reject_conflicts() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("merkle_roots.parquet");
        let path_str = path.display().to_string();

        // Eight runs planned against the same empty registry commit at once.
        let handles: Vec<_> = (0..8)
            .map(|i| {
                let path = path.clone();
                std::thread::spawn(move || {
                    let change = fill(registry_row("mainnet", &format!("day={i}"), "aa"));
                    commit_registry_local(&path, &[change]).unwrap();
                })
            })
            .collect();
        for handle in handles {
            handle.join().unwrap();
        }
        assert_eq!(load_registry(&path_str, None).unwrap().rows.len(), 8);

        // Replaying a change another run already applied is a no-op.
        commit_registry_local(&path, &[fill(registry_row("mainnet", "day=0", "aa"))]).unwrap();
        // A different root for a row that appeared meanwhile is a conflict.
        let err = commit_registry_local(&path, &[fill(registry_row("mainnet", "day=0", "bb"))])
            .unwrap_err()
            .to_string();
        assert!(err.contains("changed while verify was running"), "{err}");

        // Writes are atomic: only the registry and its lock file remain.
        let mut names: Vec<String> = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        names.sort();
        assert_eq!(names, ["merkle_roots.parquet", "merkle_roots.parquet.lock"]);
    }

    #[test]
    fn store_commits_reject_stale_and_missing_versions_without_retry() {
        let store = object_store::memory::InMemory::new();
        let location = object_store::path::Path::from("mainnet/merkle_roots.parquet");
        let rows = |store: &object_store::memory::InMemory| -> HashMap<String, RegistryRow> {
            load_registry_from_store(store, &location).unwrap().rows
        };

        // A stale absent snapshot cannot overwrite a newly created registry.
        let empty = load_registry_from_store(&store, &location).unwrap();
        assert!(!empty.exists);
        let first = fill(registry_row("mainnet", "day=1", "aa"));
        commit_registry_to_store(&store, &location, &empty, &[first]).unwrap();
        let second = fill(registry_row("mainnet", "day=2", "bb"));
        assert!(commit_registry_to_store(&store, &location, &empty, &[second]).is_err());
        assert_eq!(rows(&store).len(), 1);

        // Same with an existing object: the second write's ETag is stale.
        let stale = load_registry_from_store(&store, &location).unwrap();
        assert!(stale.e_tag.is_some());
        let third = fill(registry_row("mainnet", "day=3", "cc"));
        commit_registry_to_store(&store, &location, &stale, &[third]).unwrap();
        let fourth = fill(registry_row("mainnet", "day=4", "dd"));
        assert!(commit_registry_to_store(&store, &location, &stale, &[fourth.clone()]).is_err());
        assert_eq!(rows(&store).len(), 2);

        let mut missing_version = load_registry_from_store(&store, &location).unwrap();
        missing_version.e_tag = None;
        missing_version.version = None;
        assert!(
            commit_registry_to_store(&store, &location, &missing_version, &[fourth])
                .unwrap_err()
                .to_string()
                .contains("no usable version")
        );
        assert_eq!(rows(&store).len(), 2);

        // A conflicting root is reported instead of overwriting the other run.
        let change = fill(registry_row("mainnet", "day=1", "ff"));
        let err = commit_registry_to_store(&store, &location, &stale, &[change])
            .unwrap_err()
            .to_string();
        assert!(err.contains("changed while verify was running"), "{err}");
    }

    /// The level-by-level `merkle_v2` construction the streaming accumulator
    /// replaced, kept as the reference it must match.
    fn reference_merkle_root(leaves: &[[u8; 32]], hash_strategy: HashStrategy) -> [u8; 32] {
        let tree_root = if leaves.is_empty() {
            hash_strategy.hash(&[])
        } else {
            let mut level = leaves.to_vec();
            while level.len() > 1 {
                let mut next = Vec::with_capacity(level.len().div_ceil(2));
                for pair in level.chunks(2) {
                    match pair {
                        [left, right] => {
                            let mut combined = [0u8; 65];
                            combined[0] = 0x01;
                            combined[1..33].copy_from_slice(left);
                            combined[33..].copy_from_slice(right);
                            next.push(hash_strategy.hash(&combined));
                        }
                        [odd] => next.push(*odd),
                        _ => unreachable!(),
                    }
                }
                level = next;
            }
            level[0]
        };
        let mut committed = [0u8; 41];
        committed[0] = 0x02;
        committed[1..9].copy_from_slice(&(leaves.len() as u64).to_le_bytes());
        committed[9..].copy_from_slice(&tree_root);
        hash_strategy.hash(&committed)
    }

    #[test]
    fn streaming_accumulator_matches_the_level_by_level_tree() {
        let leaf = |i: u64| HashStrategy::Sha256.hash(&i.to_le_bytes());
        let sizes = (0..=300u64).chain([511, 512, 513, 1000, 1023, 1024, 1025, 4097]);
        for n in sizes {
            let leaves: Vec<[u8; 32]> = (0..n).map(leaf).collect();
            for strategy in [HashStrategy::Keccak256, HashStrategy::Sha256] {
                let mut tree = MerkleAccumulator::new(strategy);
                for leaf in &leaves {
                    tree.push(*leaf);
                    // One pending subtree per set bit of the leaf count.
                    assert_eq!(tree.subtrees.len() as u32, tree.leaves.count_ones());
                }
                assert_eq!(
                    tree.root(),
                    reference_merkle_root(&leaves, strategy),
                    "n={n} {strategy:?}"
                );
            }
        }
    }

    fn write_blocks_file(path: &Path, rows: &[(u64, u64, &str, &str)]) {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let kvs = vec![
            KeyValue::new("firehose-parquet.block_type".to_string(), "evm".to_string()),
            KeyValue::new(
                "firehose-parquet.chain_name".to_string(),
                "mainnet".to_string(),
            ),
        ];
        let props = WriterProperties::builder()
            .set_key_value_metadata(Some(kvs))
            .build();
        let u64s = |f: fn(&(u64, u64, &str, &str)) -> u64| {
            Arc::new(UInt64Array::from(rows.iter().map(f).collect::<Vec<_>>())) as ArrayRef
        };
        let strs = |f: fn(&(u64, u64, &str, &str)) -> String| {
            Arc::new(StringArray::from(rows.iter().map(f).collect::<Vec<_>>())) as ArrayRef
        };
        let batch = RecordBatch::try_from_iter(vec![
            ("block_num", u64s(|r| r.0)),
            ("block_id", strs(|r| r.2.to_string())),
            ("parent_id", strs(|r| r.3.to_string())),
            ("number", u64s(|r| r.1)),
            ("hash", strs(|r| r.2.to_string())),
            ("parent_hash", strs(|r| r.3.to_string())),
            ("gas_used", u64s(|r| r.0 * 7)),
        ])
        .unwrap();
        let mut writer = ArrowWriter::try_new(
            std::fs::File::create(path).unwrap(),
            batch.schema(),
            Some(props),
        )
        .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    #[test]
    fn protocol_only_runs_skip_hashing_and_match_full_runs() {
        let dir = tempfile::TempDir::new().unwrap();
        let root = dir.path();
        // blocks: block 3 has number=4 (a failure); transactions: block 5 fails.
        write_blocks_file(
            &root.join("mainnet/blocks/date=2024-01-01/part-0.parquet"),
            &[(1, 1, "a", "z"), (2, 2, "b", "a")],
        );
        write_blocks_file(
            &root.join("mainnet/blocks/date=2024-01-02/part-0.parquet"),
            &[(3, 4, "c", "b")],
        );
        write_tx_file(
            &root.join("mainnet/transactions/date=2024-01-01/part-0.parquet"),
            &[(1, 1), (5, 6)],
        );

        for table in ["blocks", "transactions"] {
            let data = root.join("mainnet").join(table);
            let mut full = base_opts();
            full.chain = None;
            full.no_fail_fast = true;
            full.checks = vec![VerifyCheck::Roots, VerifyCheck::Protocol];
            full.registry_path = Some(
                root.join(format!("{table}-registry.parquet"))
                    .display()
                    .to_string(),
            );
            let mut protocol_only = full.clone();
            protocol_only.checks = vec![VerifyCheck::Protocol];

            let full = verify_dir(&data, &full).unwrap();
            let protocol_only = verify_dir(&data, &protocol_only).unwrap();
            assert!(protocol_only.summary.protocol_failed > 0, "{table}");
            assert_eq!(
                serde_json::to_value(&protocol_only.protocol_findings).unwrap(),
                serde_json::to_value(&full.protocol_findings).unwrap(),
                "{table}"
            );
            assert_eq!(
                protocol_only.summary.partitions_scanned,
                full.summary.partitions_scanned
            );
            assert!(protocol_only.findings.is_empty());
            assert!(!protocol_only.summary.wrote_registry);
        }
    }

    #[test]
    fn prefetcher_delivers_objects_in_order_within_its_budget() {
        use object_store::ObjectStore as _;
        let store = Arc::new(object_store::memory::InMemory::new());
        let mut objects = Vec::new();
        for i in 0..12u8 {
            let location = object_store::path::Path::from(format!("t/part-{i:02}.parquet"));
            let data = vec![i; (usize::from(i) + 1) * 1000];
            super::block_on_async(store.put(&location, data.into())).unwrap();
            objects.push(super::block_on_async(store.head(&location)).unwrap());
        }

        // A 4 KiB budget: the larger objects each take the whole budget in turn.
        let mut prefetcher = Prefetcher::spawn(store.clone(), objects.clone(), 3, 4096);
        for i in 0..12u8 {
            let object = prefetcher.next_object().unwrap().unwrap();
            assert_eq!(object.data.len(), (usize::from(i) + 1) * 1000);
            assert!(object.data.iter().all(|b| *b == i));
        }
        assert!(prefetcher.next_object().is_none());

        // Stopping early (fail-fast) while holding an object does not hang.
        let mut early = Prefetcher::spawn(store, objects, 3, 4096);
        let _held = early.next_object().unwrap().unwrap();
        drop(early);
    }

    #[test]
    fn protocol_only_runs_never_encode_rows() {
        // A column without a merkle_v2 encoding fails hashing, so a protocol-only
        // run succeeds only if it neither reads nor hashes that column.
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir
            .path()
            .join("mainnet/transactions/date=2024-01-01/part-0.parquet");
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let kvs = vec![KeyValue::new(
            "firehose-parquet.block_type".to_string(),
            "evm".to_string(),
        )];
        let props = WriterProperties::builder()
            .set_key_value_metadata(Some(kvs))
            .build();
        let decimals = arrow::array::Decimal128Array::from(vec![7i128])
            .with_precision_and_scale(10, 2)
            .unwrap();
        let batch = RecordBatch::try_from_iter(vec![
            (
                "block_num",
                Arc::new(UInt64Array::from(vec![1u64])) as ArrayRef,
            ),
            (
                "block_number",
                Arc::new(UInt64Array::from(vec![1u64])) as ArrayRef,
            ),
            ("fee", Arc::new(decimals) as ArrayRef),
        ])
        .unwrap();
        let mut writer = ArrowWriter::try_new(
            std::fs::File::create(&path).unwrap(),
            batch.schema(),
            Some(props),
        )
        .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        let data = dir.path().join("mainnet/transactions");

        let mut opts = base_opts();
        opts.chain = None;
        opts.checks = vec![VerifyCheck::Protocol];
        let report = verify_dir(&data, &opts).unwrap();
        assert_eq!(report.summary.protocol_passed, 1);

        opts.checks = vec![VerifyCheck::Roots];
        let err = format!("{:#}", verify_dir(&data, &opts).unwrap_err());
        assert!(err.contains("has no merkle_v2 encoding"), "{err}");
    }
}

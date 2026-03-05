use crate::cli::{block_on_async, AwsConfig};
use crate::writer::parse_s3_url;
use anyhow::{anyhow, Context, Result};
use arrow::array::{
    Array, BinaryArray, BooleanArray, Float32Array, Float64Array, Int32Array, Int64Array,
    LargeBinaryArray, LargeStringArray, StringArray, UInt32Array, UInt64Array,
};
use arrow::record_batch::RecordBatch;
use arrow::util::display::ArrayFormatter;
use object_store::ObjectStore;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use serde::Serialize;
use sha2::{Digest as ShaDigest, Sha256};
use std::collections::{BTreeMap, HashMap};
use std::fs::File;
use std::path::{Path, PathBuf};
use time::format_description::well_known::Rfc3339;
use tiny_keccak::{Hasher, Keccak};

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

#[derive(Debug, Clone)]
pub struct VerifyOptions {
    pub chain: String,
    pub table: String,
    pub hash_strategy: Option<String>,
    pub no_fail_fast: bool,
    pub report_json: Option<PathBuf>,
    pub registry_path: Option<String>,
    pub update_registry: bool,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FindingStatus {
    Match,
    MissingExpected,
    Mismatch,
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
    pub protocol_passed: usize,
    pub protocol_failed: usize,
    pub protocol_not_verifiable: usize,
    pub wrote_registry: bool,
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
    pub chain: String,
    pub table: String,
    pub data_path: String,
    pub registry_path: String,
    pub algorithm: String,
    pub summary: VerifySummary,
    pub findings: Vec<VerifyFinding>,
    pub protocol_findings: Vec<ProtocolCheckFinding>,
}

impl VerifyReport {
    pub fn is_valid(&self) -> bool {
        self.summary.mismatches == 0 && self.summary.protocol_failed == 0
    }

    pub fn print(&self) {
        println!("Verifying {}:{}", self.chain, self.table);
        println!("  data path:     {}", self.data_path);
        println!("  registry path: {}", self.registry_path);
        println!("  algorithm:     {}", self.algorithm);
        println!("  partitions:    {}", self.summary.partitions_scanned);
        println!("  matches:       {}", self.summary.matches);
        println!("  missing roots: {}", self.summary.missing_expected);
        println!("  mismatches:    {}", self.summary.mismatches);
        println!("  protocol pass: {}", self.summary.protocol_passed);
        println!("  protocol fail: {}", self.summary.protocol_failed);
        println!("  protocol n/v:  {}", self.summary.protocol_not_verifiable);
        println!(
            "  registry write:{}",
            if self.summary.wrote_registry {
                " yes"
            } else {
                " no"
            }
        );

        if self.findings.is_empty() {
            return;
        }

        println!("\nFindings:");
        for f in &self.findings {
            let status = match f.status {
                FindingStatus::Match => "match",
                FindingStatus::MissingExpected => "missing_expected",
                FindingStatus::Mismatch => "mismatch",
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
    }
}

#[derive(Debug, Clone)]
struct ScanOutput {
    partition_roots: BTreeMap<String, String>,
    protocol_findings: Vec<ProtocolCheckFinding>,
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
    chain: String,
    table: String,
    partition: String,
    algorithm: String,
    merkle_root: String,
    updated_at: String,
}

pub fn verify_parquet(
    path: &str,
    aws: Option<&AwsConfig>,
    opts: &VerifyOptions,
) -> Result<VerifyReport> {
    let hash_strategy = resolve_hash_strategy(&opts.chain, opts.hash_strategy.as_deref())?;
    let algorithm = hash_strategy.as_str().to_string();

    let registry_path = opts
        .registry_path
        .clone()
        .unwrap_or_else(|| derive_registry_path(path, &opts.chain, &opts.table));

    let scan_output = if path.starts_with("s3://") {
        let aws = aws.ok_or_else(|| anyhow!("AWS config required for S3 paths"))?;
        collect_partition_roots_s3(path, aws, opts, hash_strategy)?
    } else {
        collect_partition_roots_local(path, opts, hash_strategy)?
    };

    let partition_roots = scan_output.partition_roots;

    if partition_roots.is_empty() {
        return Err(anyhow!("no parquet files found in {}", path));
    }

    let (registry_exists, mut registry) = load_registry(&registry_path, aws)?;

    let mut findings = Vec::new();
    let mut matches = 0usize;
    let mut missing_expected = 0usize;
    let mut mismatches = 0usize;
    let mut needs_registry_write = !registry_exists;

    for (partition, computed_root) in &partition_roots {
        let key = registry_key(&opts.chain, &opts.table, partition);
        match registry.get(&key) {
            Some(row) if row.algorithm.as_str() != algorithm.as_str() => {
                mismatches += 1;
                findings.push(VerifyFinding {
                    chain: opts.chain.clone(),
                    table: opts.table.clone(),
                    partition: partition.clone(),
                    status: FindingStatus::Mismatch,
                    expected_root: Some(row.merkle_root.clone()),
                    computed_root: computed_root.clone(),
                    error: Some(format!(
                        "algorithm mismatch: registry={} runtime={}",
                        row.algorithm, algorithm
                    )),
                });
                if opts.update_registry {
                    needs_registry_write = true;
                    registry.insert(
                        key,
                        RegistryRow {
                            chain: opts.chain.clone(),
                            table: opts.table.clone(),
                            partition: partition.clone(),
                            algorithm: algorithm.clone(),
                            merkle_root: computed_root.clone(),
                            updated_at: now_rfc3339(),
                        },
                    );
                }
                if !opts.no_fail_fast {
                    break;
                }
            }
            Some(row) if row.merkle_root == *computed_root => {
                matches += 1;
                findings.push(VerifyFinding {
                    chain: opts.chain.clone(),
                    table: opts.table.clone(),
                    partition: partition.clone(),
                    status: FindingStatus::Match,
                    expected_root: Some(row.merkle_root.clone()),
                    computed_root: computed_root.clone(),
                    error: None,
                });
            }
            Some(row) => {
                mismatches += 1;
                findings.push(VerifyFinding {
                    chain: opts.chain.clone(),
                    table: opts.table.clone(),
                    partition: partition.clone(),
                    status: FindingStatus::Mismatch,
                    expected_root: Some(row.merkle_root.clone()),
                    computed_root: computed_root.clone(),
                    error: None,
                });
                if opts.update_registry {
                    needs_registry_write = true;
                    registry.insert(
                        key,
                        RegistryRow {
                            chain: opts.chain.clone(),
                            table: opts.table.clone(),
                            partition: partition.clone(),
                            algorithm: algorithm.clone(),
                            merkle_root: computed_root.clone(),
                            updated_at: now_rfc3339(),
                        },
                    );
                }
                if !opts.no_fail_fast {
                    break;
                }
            }
            None => {
                missing_expected += 1;
                findings.push(VerifyFinding {
                    chain: opts.chain.clone(),
                    table: opts.table.clone(),
                    partition: partition.clone(),
                    status: FindingStatus::MissingExpected,
                    expected_root: None,
                    computed_root: computed_root.clone(),
                    error: None,
                });
                needs_registry_write = true;
                registry.insert(
                    key,
                    RegistryRow {
                        chain: opts.chain.clone(),
                        table: opts.table.clone(),
                        partition: partition.clone(),
                        algorithm: algorithm.clone(),
                        merkle_root: computed_root.clone(),
                        updated_at: now_rfc3339(),
                    },
                );
            }
        }
    }

    let wrote_registry = if needs_registry_write {
        write_registry(&registry_path, aws, &registry)?;
        true
    } else {
        false
    };

    let mut protocol_passed = 0usize;
    let mut protocol_failed = 0usize;
    let mut protocol_not_verifiable = 0usize;
    for finding in &scan_output.protocol_findings {
        match finding.status {
            ProtocolCheckStatus::Pass => protocol_passed += 1,
            ProtocolCheckStatus::Fail => protocol_failed += 1,
            ProtocolCheckStatus::NotVerifiable => protocol_not_verifiable += 1,
        }
    }

    let report = VerifyReport {
        chain: opts.chain.clone(),
        table: opts.table.clone(),
        data_path: path.to_string(),
        registry_path,
        algorithm,
        summary: VerifySummary {
            partitions_scanned: partition_roots.len(),
            matches,
            missing_expected,
            mismatches,
            protocol_passed,
            protocol_failed,
            protocol_not_verifiable,
            wrote_registry,
        },
        findings,
        protocol_findings: scan_output.protocol_findings,
    };

    if let Some(ref report_path) = opts.report_json {
        let bytes = serde_json::to_vec_pretty(&report)?;
        std::fs::write(report_path, bytes)
            .with_context(|| format!("writing JSON report to {}", report_path.display()))?;
    }

    Ok(report)
}

fn derive_registry_path(data_path: &str, chain: &str, table: &str) -> String {
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

fn collect_partition_roots_local(
    path: &str,
    opts: &VerifyOptions,
    hash_strategy: HashStrategy,
) -> Result<ScanOutput> {
    let pathbuf = PathBuf::from(path);
    let mut files = Vec::new();

    if pathbuf.is_dir() {
        collect_parquet_files(&pathbuf, &mut files)?;
    } else if pathbuf
        .extension()
        .map_or(false, |ext| ext.eq_ignore_ascii_case("parquet"))
    {
        files.push(pathbuf.clone());
    }

    files.sort();

    let base = if pathbuf.is_dir() {
        pathbuf
    } else {
        pathbuf.parent().unwrap_or(Path::new(".")).to_path_buf()
    };

    let mut partition_leaves: HashMap<String, Vec<[u8; 32]>> = HashMap::new();
    let mut protocol_state: HashMap<String, ProtocolPartitionState> = HashMap::new();

    for file_path in files {
        let partition = detect_partition(&file_path.to_string_lossy(), &base.to_string_lossy());
        let partition_state = protocol_state.entry(partition.clone()).or_default();
        let leaves = read_parquet_leaves_local(&file_path, opts, partition_state, hash_strategy)?;
        partition_leaves
            .entry(partition)
            .or_default()
            .extend(leaves);

        if !opts.no_fail_fast && has_protocol_failure(partition_state) {
            break;
        }
    }

    let mut roots = BTreeMap::new();
    for (partition, leaves) in partition_leaves {
        roots.insert(partition, hex::encode(merkle_root(&leaves, hash_strategy)));
    }
    Ok(ScanOutput {
        partition_roots: roots,
        protocol_findings: finalize_protocol_findings(opts, &protocol_state),
    })
}

fn collect_partition_roots_s3(
    path: &str,
    aws: &AwsConfig,
    opts: &VerifyOptions,
    hash_strategy: HashStrategy,
) -> Result<ScanOutput> {
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

    objects.retain(|obj| obj.location.as_ref().ends_with(".parquet"));
    objects.sort_by(|a, b| a.location.cmp(&b.location));

    let mut partition_leaves: HashMap<String, Vec<[u8; 32]>> = HashMap::new();
    let mut protocol_state: HashMap<String, ProtocolPartitionState> = HashMap::new();

    for obj in objects {
        let partition = detect_partition(obj.location.as_ref(), &prefix);
        let partition_state = protocol_state.entry(partition.clone()).or_default();
        let leaves = read_parquet_leaves_s3(
            &client,
            &bucket,
            &obj.location,
            opts,
            partition_state,
            hash_strategy,
        )?;
        partition_leaves
            .entry(partition)
            .or_default()
            .extend(leaves);

        if !opts.no_fail_fast && has_protocol_failure(partition_state) {
            break;
        }
    }

    let mut roots = BTreeMap::new();
    for (partition, leaves) in partition_leaves {
        roots.insert(partition, hex::encode(merkle_root(&leaves, hash_strategy)));
    }
    Ok(ScanOutput {
        partition_roots: roots,
        protocol_findings: finalize_protocol_findings(opts, &protocol_state),
    })
}

fn read_parquet_leaves_local(
    path: &Path,
    opts: &VerifyOptions,
    protocol_state: &mut ProtocolPartitionState,
    hash_strategy: HashStrategy,
) -> Result<Vec<[u8; 32]>> {
    let file = File::open(path).with_context(|| format!("opening {}", path.display()))?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let reader = builder.build()?;

    let mut leaves = Vec::new();
    for maybe_batch in reader {
        let batch = maybe_batch?;
        run_protocol_checks_for_batch(opts, &batch, protocol_state);
        for row in 0..batch.num_rows() {
            let mut encoded = Vec::new();
            for (idx, field) in batch.schema().fields().iter().enumerate() {
                append_len_prefixed(&mut encoded, field.name().as_bytes());
                let value = array_value_bytes(batch.column(idx).as_ref(), row);
                append_len_prefixed(&mut encoded, &value);
            }
            leaves.push(hash_strategy.hash(&encoded));
        }
    }

    Ok(leaves)
}

fn read_parquet_leaves_s3(
    client: &object_store::aws::AmazonS3,
    bucket: &str,
    location: &object_store::path::Path,
    opts: &VerifyOptions,
    protocol_state: &mut ProtocolPartitionState,
    hash_strategy: HashStrategy,
) -> Result<Vec<[u8; 32]>> {
    let data = block_on_async(async { client.get(location).await?.bytes().await })
        .map_err(|e| anyhow!("reading s3://{bucket}/{}: {e}", location))?;

    let builder = ParquetRecordBatchReaderBuilder::try_new(data)?;
    let reader = builder.build()?;

    let mut leaves = Vec::new();
    for maybe_batch in reader {
        let batch = maybe_batch?;
        run_protocol_checks_for_batch(opts, &batch, protocol_state);
        for row in 0..batch.num_rows() {
            let mut encoded = Vec::new();
            for (idx, field) in batch.schema().fields().iter().enumerate() {
                append_len_prefixed(&mut encoded, field.name().as_bytes());
                let value = array_value_bytes(batch.column(idx).as_ref(), row);
                append_len_prefixed(&mut encoded, &value);
            }
            leaves.push(hash_strategy.hash(&encoded));
        }
    }

    Ok(leaves)
}

fn append_len_prefixed(out: &mut Vec<u8>, data: &[u8]) {
    out.extend_from_slice(&(data.len() as u32).to_le_bytes());
    out.extend_from_slice(data);
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
    opts: &VerifyOptions,
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

        if opts.chain.eq_ignore_ascii_case("evm") && opts.table.eq_ignore_ascii_case("blocks") {
            add_check_finding(
                &mut findings,
                opts,
                &partition,
                "evm_hash_matches_block_id",
                &state.hash_matches_block_id,
            );
            add_check_finding(
                &mut findings,
                opts,
                &partition,
                "evm_parent_hash_matches_parent_id",
                &state.parent_hash_matches_parent_id,
            );
            add_check_finding(
                &mut findings,
                opts,
                &partition,
                "evm_number_matches_block_num",
                &state.number_matches_block_num,
            );
            findings.push(ProtocolCheckFinding {
                chain: opts.chain.clone(),
                table: opts.table.clone(),
                partition: partition.clone(),
                check: "evm_transactions_root_trie_recompute".to_string(),
                status: ProtocolCheckStatus::NotVerifiable,
                block_num: None,
                details: "requires transaction trie materialization from transactions table"
                    .to_string(),
            });
            findings.push(ProtocolCheckFinding {
                chain: opts.chain.clone(),
                table: opts.table.clone(),
                partition: partition.clone(),
                check: "evm_receipt_root_trie_recompute".to_string(),
                status: ProtocolCheckStatus::NotVerifiable,
                block_num: None,
                details: "requires receipt trie materialization from receipts/logs data"
                    .to_string(),
            });
            findings.push(ProtocolCheckFinding {
                chain: opts.chain.clone(),
                table: opts.table.clone(),
                partition,
                check: "evm_state_root_recompute".to_string(),
                status: ProtocolCheckStatus::NotVerifiable,
                block_num: None,
                details: "requires full state transition execution and account/storage tries"
                    .to_string(),
            });
        } else if opts.chain.eq_ignore_ascii_case("evm")
            && opts.table.eq_ignore_ascii_case("transactions")
        {
            add_check_finding(
                &mut findings,
                opts,
                &partition,
                "evm_transactions_block_number_matches_block_num",
                &state.transactions_block_number_matches_block_num,
            );
            findings.push(ProtocolCheckFinding {
                chain: opts.chain.clone(),
                table: opts.table.clone(),
                partition,
                check: "evm_transactions_root_inclusion".to_string(),
                status: ProtocolCheckStatus::NotVerifiable,
                block_num: None,
                details: "requires canonical transaction RLP encoding + trie indexing by transaction position".to_string(),
            });
        } else if opts.chain.eq_ignore_ascii_case("evm") && opts.table.eq_ignore_ascii_case("logs")
        {
            add_check_finding(
                &mut findings,
                opts,
                &partition,
                "evm_logs_block_number_matches_block_num",
                &state.logs_block_number_matches_block_num,
            );
            findings.push(ProtocolCheckFinding {
                chain: opts.chain.clone(),
                table: opts.table.clone(),
                partition,
                check: "evm_logs_receipt_inclusion".to_string(),
                status: ProtocolCheckStatus::NotVerifiable,
                block_num: None,
                details: "requires receipt reconstruction and trie inclusion proofs".to_string(),
            });
        } else if opts.chain.eq_ignore_ascii_case("evm") && opts.table.eq_ignore_ascii_case("calls")
        {
            add_check_finding(
                &mut findings,
                opts,
                &partition,
                "evm_calls_block_number_matches_block_num",
                &state.calls_block_number_matches_block_num,
            );
            findings.push(ProtocolCheckFinding {
                chain: opts.chain.clone(),
                table: opts.table.clone(),
                partition,
                check: "evm_calls_receipt_correlation".to_string(),
                status: ProtocolCheckStatus::NotVerifiable,
                block_num: None,
                details: "requires transaction execution traces correlated with canonical receipts"
                    .to_string(),
            });
        } else {
            findings.push(ProtocolCheckFinding {
                chain: opts.chain.clone(),
                table: opts.table.clone(),
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
    opts: &VerifyOptions,
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
        chain: opts.chain.clone(),
        table: opts.table.clone(),
        partition: partition.to_string(),
        check: check.to_string(),
        status,
        block_num,
        details,
    });
}

fn run_protocol_checks_for_batch(
    opts: &VerifyOptions,
    batch: &RecordBatch,
    state: &mut ProtocolPartitionState,
) {
    if !opts.chain.eq_ignore_ascii_case("evm") {
        return;
    }

    if opts.table.eq_ignore_ascii_case("transactions") {
        check_block_number_alignment(
            batch,
            "block_number",
            &mut state.transactions_block_number_matches_block_num,
        );
        return;
    }

    if opts.table.eq_ignore_ascii_case("logs") {
        check_block_number_alignment(
            batch,
            "block_number",
            &mut state.logs_block_number_matches_block_num,
        );
        return;
    }

    if opts.table.eq_ignore_ascii_case("calls") {
        check_block_number_alignment(
            batch,
            "block_number",
            &mut state.calls_block_number_matches_block_num,
        );
        return;
    }

    if !opts.table.eq_ignore_ascii_case("blocks") {
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

fn comparable_bytes(array: &dyn Array, row: usize) -> Option<Vec<u8>> {
    if array.is_null(row) {
        return None;
    }
    if let Some(a) = array.as_any().downcast_ref::<BinaryArray>() {
        return Some(a.value(row).to_vec());
    }
    if let Some(a) = array.as_any().downcast_ref::<LargeBinaryArray>() {
        return Some(a.value(row).to_vec());
    }
    if let Some(a) = array.as_any().downcast_ref::<StringArray>() {
        return Some(a.value(row).as_bytes().to_vec());
    }
    if let Some(a) = array.as_any().downcast_ref::<LargeStringArray>() {
        return Some(a.value(row).as_bytes().to_vec());
    }
    Some(array_value_bytes(array, row))
}

fn array_value_bytes(array: &dyn Array, row: usize) -> Vec<u8> {
    if array.is_null(row) {
        return b"<null>".to_vec();
    }

    if let Some(a) = array.as_any().downcast_ref::<UInt64Array>() {
        return a.value(row).to_string().into_bytes();
    }
    if let Some(a) = array.as_any().downcast_ref::<UInt32Array>() {
        return a.value(row).to_string().into_bytes();
    }
    if let Some(a) = array.as_any().downcast_ref::<Int64Array>() {
        return a.value(row).to_string().into_bytes();
    }
    if let Some(a) = array.as_any().downcast_ref::<Int32Array>() {
        return a.value(row).to_string().into_bytes();
    }
    if let Some(a) = array.as_any().downcast_ref::<Float64Array>() {
        return a.value(row).to_string().into_bytes();
    }
    if let Some(a) = array.as_any().downcast_ref::<Float32Array>() {
        return a.value(row).to_string().into_bytes();
    }
    if let Some(a) = array.as_any().downcast_ref::<BooleanArray>() {
        return a.value(row).to_string().into_bytes();
    }
    if let Some(a) = array.as_any().downcast_ref::<StringArray>() {
        return a.value(row).as_bytes().to_vec();
    }
    if let Some(a) = array.as_any().downcast_ref::<LargeStringArray>() {
        return a.value(row).as_bytes().to_vec();
    }
    if let Some(a) = array.as_any().downcast_ref::<BinaryArray>() {
        return hex::encode(a.value(row)).into_bytes();
    }
    if let Some(a) = array.as_any().downcast_ref::<LargeBinaryArray>() {
        return hex::encode(a.value(row)).into_bytes();
    }

    match ArrayFormatter::try_new(array, &Default::default()) {
        Ok(formatter) => formatter.value(row).to_string().into_bytes(),
        Err(_) => b"<unsupported>".to_vec(),
    }
}

fn merkle_root(leaves: &[[u8; 32]], hash_strategy: HashStrategy) -> [u8; 32] {
    if leaves.is_empty() {
        return hash_strategy.hash(&[]);
    }

    let mut level = leaves.to_vec();
    while level.len() > 1 {
        let mut next = Vec::with_capacity((level.len() + 1) / 2);
        let mut i = 0usize;
        while i < level.len() {
            let left = level[i];
            let right = if i + 1 < level.len() {
                level[i + 1]
            } else {
                level[i]
            };
            let mut combined = [0u8; 64];
            combined[..32].copy_from_slice(&left);
            combined[32..].copy_from_slice(&right);
            next.push(hash_strategy.hash(&combined));
            i += 2;
        }
        level = next;
    }
    level[0]
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

fn registry_key(chain: &str, table: &str, partition: &str) -> String {
    format!("{chain}|{table}|{partition}")
}

fn load_registry(
    path: &str,
    aws: Option<&AwsConfig>,
) -> Result<(bool, HashMap<String, RegistryRow>)> {
    let data = read_registry_bytes(path, aws)?;
    let Some(data) = data else {
        return Ok((false, HashMap::new()));
    };

    let builder = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(data))?;
    let reader = builder.build()?;

    let mut rows = HashMap::new();
    for maybe_batch in reader {
        let batch = maybe_batch?;
        let schema = batch.schema();

        let idx_chain = schema.index_of("chain")?;
        let idx_table = schema.index_of("table")?;
        let idx_partition = schema.index_of("partition")?;
        let idx_algorithm = schema.index_of("algorithm")?;
        let idx_merkle_root = schema.index_of("merkle_root")?;
        let idx_updated_at = schema.index_of("updated_at")?;

        for row in 0..batch.num_rows() {
            let chain = required_string(batch.column(idx_chain).as_ref(), row, "chain")?;
            let table = required_string(batch.column(idx_table).as_ref(), row, "table")?;
            let partition =
                required_string(batch.column(idx_partition).as_ref(), row, "partition")?;
            let algorithm =
                required_string(batch.column(idx_algorithm).as_ref(), row, "algorithm")?;
            let merkle_root =
                required_string(batch.column(idx_merkle_root).as_ref(), row, "merkle_root")?;
            let updated_at =
                required_string(batch.column(idx_updated_at).as_ref(), row, "updated_at")?;

            rows.insert(
                registry_key(&chain, &table, &partition),
                RegistryRow {
                    chain,
                    table,
                    partition,
                    algorithm,
                    merkle_root,
                    updated_at,
                },
            );
        }
    }

    Ok((true, rows))
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

fn write_registry(
    path: &str,
    aws: Option<&AwsConfig>,
    rows: &HashMap<String, RegistryRow>,
) -> Result<()> {
    let mut ordered: Vec<&RegistryRow> = rows.values().collect();
    ordered.sort_by(|a, b| {
        (&a.chain, &a.table, &a.partition).cmp(&(&b.chain, &b.table, &b.partition))
    });

    let chain_values: Vec<&str> = ordered.iter().map(|r| r.chain.as_str()).collect();
    let table_values: Vec<&str> = ordered.iter().map(|r| r.table.as_str()).collect();
    let partition_values: Vec<&str> = ordered.iter().map(|r| r.partition.as_str()).collect();
    let algorithm_values: Vec<&str> = ordered.iter().map(|r| r.algorithm.as_str()).collect();
    let merkle_values: Vec<&str> = ordered.iter().map(|r| r.merkle_root.as_str()).collect();
    let updated_values: Vec<&str> = ordered.iter().map(|r| r.updated_at.as_str()).collect();

    let schema = arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("chain", arrow::datatypes::DataType::Utf8, false),
        arrow::datatypes::Field::new("table", arrow::datatypes::DataType::Utf8, false),
        arrow::datatypes::Field::new("partition", arrow::datatypes::DataType::Utf8, false),
        arrow::datatypes::Field::new("algorithm", arrow::datatypes::DataType::Utf8, false),
        arrow::datatypes::Field::new("merkle_root", arrow::datatypes::DataType::Utf8, false),
        arrow::datatypes::Field::new("updated_at", arrow::datatypes::DataType::Utf8, false),
    ]);

    let batch = arrow::record_batch::RecordBatch::try_new(
        std::sync::Arc::new(schema),
        vec![
            std::sync::Arc::new(StringArray::from(chain_values)),
            std::sync::Arc::new(StringArray::from(table_values)),
            std::sync::Arc::new(StringArray::from(partition_values)),
            std::sync::Arc::new(StringArray::from(algorithm_values)),
            std::sync::Arc::new(StringArray::from(merkle_values)),
            std::sync::Arc::new(StringArray::from(updated_values)),
        ],
    )?;

    let mut data = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut data, batch.schema(), None)?;
    writer.write(&batch)?;
    writer.close()?;

    if path.starts_with("s3://") {
        let aws = aws.ok_or_else(|| anyhow!("AWS config required for S3 registry path"))?;
        let (bucket, key) = parse_s3_url(path)?;
        let client = aws.build_s3_client(&bucket)?;
        let location = object_store::path::Path::from(key.as_str());
        let payload = object_store::PutPayload::from(bytes::Bytes::from(data));
        block_on_async(async { client.put(&location, payload).await })
            .map_err(|e| anyhow!("writing registry to s3://{bucket}/{}: {e}", location))?;
    } else {
        let file_path = PathBuf::from(path);
        if let Some(parent) = file_path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("creating registry dir {}", parent.display()))?;
        }
        std::fs::write(&file_path, data)
            .with_context(|| format!("writing registry {}", file_path.display()))?;
    }

    Ok(())
}

fn read_registry_bytes(path: &str, aws: Option<&AwsConfig>) -> Result<Option<Vec<u8>>> {
    if path.starts_with("s3://") {
        let aws = aws.ok_or_else(|| anyhow!("AWS config required for S3 registry path"))?;
        let (bucket, key) = parse_s3_url(path)?;
        let client = aws.build_s3_client(&bucket)?;
        let location = object_store::path::Path::from(key.as_str());
        let result = block_on_async(async { client.get(&location).await });
        match result {
            Ok(obj) => {
                let bytes = block_on_async(async { obj.bytes().await })
                    .map_err(|e| anyhow!("reading registry s3://{bucket}/{}: {e}", location))?;
                Ok(Some(bytes.to_vec()))
            }
            Err(err) => {
                let msg = err.to_string().to_lowercase();
                if msg.contains("not found") {
                    Ok(None)
                } else {
                    Err(anyhow!(
                        "reading registry s3://{bucket}/{}: {err}",
                        location
                    ))
                }
            }
        }
    } else {
        let pathbuf = PathBuf::from(path);
        if !pathbuf.exists() {
            return Ok(None);
        }
        let data = std::fs::read(&pathbuf)
            .with_context(|| format!("reading registry {}", pathbuf.display()))?;
        Ok(Some(data))
    }
}

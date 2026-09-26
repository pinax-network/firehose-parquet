//! Equivalence of the `ChainKind`-based ingestion helpers with the
//! `block_type` string helpers they replaced (#526).
//!
//! `legacy` holds verbatim copies of the origin/main `9372f99` functions from
//! `blocks/src/bin/main.rs`; `legacy_inline` reproduces the inline setup and
//! auto-detection decisions from `ingestion/{setup,runtime}.rs`. They are
//! oracles only: every test compares the current helpers with them over
//! requested types, endpoint metadata, cursor metadata and flag combinations.

use crate::*;
use firehose_parquet::networks::KNOWN_NETWORK_NAMES;

#[allow(dead_code, clippy::all)]
mod legacy {
    use crate::*;
    use firehose_parquet::ingest::BlockFamily;

    #[derive(Debug, Clone, PartialEq, Eq)]
    pub(crate) struct OutputEncodingPolicy {
        bytes_encoding: EncodeBytes,
        block_id_encoding: &'static str,
        allow_endpoint_block_id_hint: bool,
    }

    pub(crate) fn build_file_metadata(
        block_type: &str,
        encoding: &firehose_parquet::encode::EncodeBytes,
        endpoint: &str,
        compression: Compression,
        endpoint_info: &Option<EndpointInfo>,
    ) -> ParquetFileMetadata {
        let mut meta = ParquetFileMetadata::new();
        add_common_file_metadata(
            &mut meta,
            Some(block_type),
            Some(encoding),
            endpoint,
            endpoint_info,
        );
        meta.add("firehose-parquet.compression", compression.to_string());
        meta
    }

    pub(crate) fn add_common_file_metadata(
        meta: &mut ParquetFileMetadata,
        block_type: Option<&str>,
        encoding: Option<&EncodeBytes>,
        endpoint: &str,
        endpoint_info: &Option<EndpointInfo>,
    ) {
        meta.add("firehose-parquet.version", env!("CARGO_PKG_VERSION"));
        if let Some(block_type) = block_type {
            meta.add("firehose-parquet.block_type", block_type);
        }
        if let Some(encoding) = encoding {
            meta.add(
                "firehose-parquet.bytes_encoding",
                encode_bytes_label(encoding),
            );
        }
        meta.add("firehose-parquet.endpoint", endpoint);
        if let Some(ref ei) = endpoint_info {
            if !ei.chain_name.is_empty() {
                meta.add("firehose-parquet.chain_name", &ei.chain_name);
            }
            if !ei.chain_name_aliases.is_empty() {
                meta.add(
                    "firehose-parquet.chain_name_aliases",
                    ei.chain_name_aliases.join(","),
                );
            }
            if !ei.first_streamable_block_id.is_empty() {
                meta.add(
                    "firehose-parquet.first_streamable_block_id",
                    &ei.first_streamable_block_id,
                );
                meta.add(
                    "firehose-parquet.first_streamable_block_num",
                    ei.first_streamable_block_num.to_string(),
                );
            } else if ei.first_streamable_block_num > 0 {
                meta.add(
                    "firehose-parquet.first_streamable_block_num",
                    ei.first_streamable_block_num.to_string(),
                );
            }
            if !ei.block_features.is_empty() {
                meta.add(
                    "firehose-parquet.block_features",
                    ei.block_features.join(","),
                );
            }
        }
        if let Some(encoding) = encoding {
            if let Some(block_id_encoding) = output_block_id_encoding_label(encoding) {
                meta.add("firehose-parquet.block_id_encoding", block_id_encoding);
            }
        } else if let Some(ref ei) = endpoint_info {
            if ei.block_id_encoding > 0 {
                meta.add(
                    "firehose-parquet.block_id_encoding",
                    block_id_encoding_label(ei.block_id_encoding),
                );
            }
        }
    }

    pub(crate) fn maybe_add_synthetic_timestamp_metadata(
        meta: &mut ParquetFileMetadata,
        block_type: &str,
        synthetic_partition_routing: bool,
    ) {
        if block_type_has_nullable_timestamps(block_type) && synthetic_partition_routing {
            meta.add("firehose-parquet.synthetic_timestamps", "true");
            meta.add(
                "firehose-parquet.synthetic_timestamp_policy",
                "last_known_partition_routing",
            );
        }
    }

    pub(crate) fn build_cursor_file_metadata(
        block_type: Option<&str>,
        encoding: Option<&EncodeBytes>,
        endpoint: &str,
        compression: Compression,
        partition: &firehose_parquet::config::Partition,
        endpoint_info: &Option<EndpointInfo>,
        extended: bool,
        final_blocks_only: bool,
        include_failed_transactions: bool,
    ) -> ParquetFileMetadata {
        let mut meta = ParquetFileMetadata::new();
        add_common_file_metadata(&mut meta, block_type, encoding, endpoint, endpoint_info);
        meta.add("firehose-parquet.compression", compression.to_string());
        meta.add("firehose-parquet.partition", partition.to_string());
        meta.add(
            "firehose-parquet.block_range_size",
            match partition {
                firehose_parquet::config::Partition::BlockRange { size, .. } => size.to_string(),
                _ => "0".to_string(),
            },
        );
        add_cursor_compatibility_metadata(
            &mut meta,
            extended,
            final_blocks_only,
            include_failed_transactions,
        );
        meta
    }

    pub(crate) fn build_partitions_file_metadata(
        endpoint: &str,
        chain: &str,
        partition: &str,
        compression: Compression,
        endpoint_info: &Option<EndpointInfo>,
        block_range_size: Option<u64>,
    ) -> ParquetFileMetadata {
        let inferred_block_type = infer_partitions_block_type(chain, endpoint_info);
        let tron_style_evm_profile = chain_uses_tron_style_evm_profile(chain, endpoint_info);
        let encoding =
            resolve_auto_encode_bytes(inferred_block_type, endpoint_info, tron_style_evm_profile);

        let mut meta = ParquetFileMetadata::new();
        add_common_file_metadata(
            &mut meta,
            inferred_block_type,
            Some(&encoding),
            endpoint,
            endpoint_info,
        );
        if let Some(info) = endpoint_info {
            if !info.chain_name.is_empty() {
                // already set by `add_common_file_metadata`
            } else {
                meta.add("firehose-parquet.chain_name", chain);
            }
        } else {
            meta.add("firehose-parquet.chain_name", chain);
        }
        meta.add("firehose-parquet.partition", partition);
        meta.add(
            "firehose-parquet.block_range_size",
            block_range_size.unwrap_or(0).to_string(),
        );
        meta.add("firehose-parquet.compression", compression.to_string());
        meta
    }

    pub(crate) fn maybe_add_solana_with_votes_metadata(
        meta: &mut ParquetFileMetadata,
        block_type: Option<&str>,
        with_votes: bool,
    ) {
        if block_type == Some("solana") {
            meta.add("firehose-parquet.with_votes", with_votes.to_string());
        }
    }

    pub(crate) fn block_type_has_nullable_timestamps(block_type: &str) -> bool {
        block_type == "solana"
    }

    pub(crate) fn use_last_known_timestamp_partition_routing(
        block_type: &str,
        partition: &Partition,
    ) -> bool {
        block_type_has_nullable_timestamps(block_type) && partition_requires_timestamp(partition)
    }

    pub(crate) fn protected_block_family(label: &str) -> Result<BlockFamily> {
        Ok(match label {
            "evm" => BlockFamily::Evm,
            "bitcoin" => BlockFamily::Bitcoin,
            "solana" => BlockFamily::Solana,
            "near" => BlockFamily::Near,
            "antelope" => BlockFamily::Antelope,
            "cosmos" => BlockFamily::Cosmos,
            "tron" => BlockFamily::Tron,
            "beacon" => BlockFamily::Beacon,
            _ => return Err(anyhow!("unsupported resolved mapper family")),
        })
    }

    pub(crate) fn detect_block_type(type_url: &str) -> Result<String> {
        if type_url.contains("ethereum") {
            Ok("evm".to_string())
        } else if type_url.contains("bitcoin") {
            Ok("bitcoin".to_string())
        } else if type_url.contains("solana") {
            Ok("solana".to_string())
        } else if type_url.contains("near") {
            Ok("near".to_string())
        } else if type_url.contains("antelope") {
            Ok("antelope".to_string())
        } else if type_url.contains("cosmos") {
            Ok("cosmos".to_string())
        } else if type_url.contains("tron") {
            Ok("tron".to_string())
        } else if type_url.contains("beacon") {
            Ok("beacon".to_string())
        } else {
            Err(anyhow!(
                "unable to auto-detect block type from type_url: {type_url}"
            ))
        }
    }

    /// Chains whose block numbers can legitimately have gaps (skipped slots or
    /// heights), so a bounded range may end below `stop_block - 1`.
    pub(crate) fn block_type_allows_block_number_gaps(block_type: &str) -> bool {
        matches!(block_type, "solana" | "near" | "beacon")
    }

    pub(crate) fn output_encoding_policy(
        block_type: &str,
        tron_style_evm_profile: bool,
    ) -> Option<OutputEncodingPolicy> {
        match block_type {
            "evm" if tron_style_evm_profile => Some(OutputEncodingPolicy {
                bytes_encoding: EncodeBytes::TronBase58,
                block_id_encoding: "hex_no_prefix",
                allow_endpoint_block_id_hint: false,
            }),
            "evm" | "bitcoin" | "cosmos" | "beacon" => Some(OutputEncodingPolicy {
                bytes_encoding: EncodeBytes::Hex,
                block_id_encoding: "hex_0x",
                allow_endpoint_block_id_hint: false,
            }),
            "antelope" => Some(OutputEncodingPolicy {
                bytes_encoding: EncodeBytes::HexNoPrefix,
                block_id_encoding: "hex_no_prefix",
                allow_endpoint_block_id_hint: false,
            }),
            "solana" | "near" => Some(OutputEncodingPolicy {
                bytes_encoding: EncodeBytes::Base58,
                block_id_encoding: "base58",
                allow_endpoint_block_id_hint: false,
            }),
            "tron" => Some(OutputEncodingPolicy {
                bytes_encoding: EncodeBytes::TronBase58,
                block_id_encoding: "hex_no_prefix",
                allow_endpoint_block_id_hint: false,
            }),
            _ => None,
        }
    }

    pub(crate) fn resolve_auto_encode_bytes(
        block_type: Option<&str>,
        endpoint_info: &Option<EndpointInfo>,
        tron_style_evm_profile: bool,
    ) -> EncodeBytes {
        if let Some(block_type) = block_type {
            if let Some(policy) = output_encoding_policy(block_type, tron_style_evm_profile) {
                if policy.allow_endpoint_block_id_hint {
                    return endpoint_info
                        .as_ref()
                        .and_then(|ei| encode_bytes_from_block_id_encoding(ei.block_id_encoding))
                        .unwrap_or(policy.bytes_encoding);
                }

                return policy.bytes_encoding;
            }
        }

        endpoint_info
            .as_ref()
            .and_then(|ei| encode_bytes_from_block_id_encoding(ei.block_id_encoding))
            .unwrap_or(EncodeBytes::Hex)
    }

    pub(crate) fn inferred_block_type_from_endpoint_info(
        endpoint_info: &Option<EndpointInfo>,
    ) -> Option<&'static str> {
        let info = endpoint_info.as_ref()?;

        if !info.chain_name.is_empty() {
            return infer_partitions_block_type(&info.chain_name, endpoint_info);
        }

        info.chain_name_aliases
            .iter()
            .find_map(|alias| infer_partitions_block_type(alias, endpoint_info))
    }

    /// Resolve whether failed/reverted transactions are written (#494).
    ///
    /// - `--exclude-failed-transactions` always drops them.
    /// - EVM writes them by default, with only their persistent state changes.
    ///   `--include-failed-transactions` is a deprecated no-op there. A resumed EVM
    ///   cursor that was written with failed transactions excluded (the default
    ///   before #494) keeps excluding them, so one output does not mix both modes;
    ///   `--cursor-override` opts out of that.
    /// - Other chains exclude them unless `--include-failed-transactions` is set.
    ///
    /// Returns the effective value and the warnings to log.
    pub(crate) fn resolve_include_failed_transactions(
        block_type: Option<&str>,
        include_flag: bool,
        exclude_flag: bool,
        cursor_state: Option<&CursorState>,
        cursor_override: bool,
    ) -> (bool, Vec<String>) {
        let mut warnings = Vec::new();
        if exclude_flag {
            if include_flag {
                warnings.push(
                    "--include-failed-transactions is ignored because --exclude-failed-transactions is set"
                        .to_string(),
                );
            }
            return (false, warnings);
        }
        if block_type != Some("evm") {
            return (include_flag, warnings);
        }
        if include_flag {
            warnings.push(
                "--include-failed-transactions is deprecated and has no effect on EVM: failed transactions are included by default; use --exclude-failed-transactions to drop them"
                    .to_string(),
            );
        }
        if let Some(cursor_state) = cursor_state.filter(|_| !cursor_override) {
            if !cursor_state.include_failed_transactions {
                // The only departure from the verbatim copy: the validation
                // follow-up replaced this message's stale --cursor-override
                // advice. The decision logic under comparison is unchanged.
                warnings.push(crate::EVM_FAILED_TRANSACTIONS_EXCLUDED_WARNING.to_string());
                return (false, warnings);
            }
        }
        (true, warnings)
    }

    pub(crate) fn infer_partitions_block_type(
        chain: &str,
        endpoint_info: &Option<EndpointInfo>,
    ) -> Option<&'static str> {
        let mut candidates = vec![chain.to_ascii_lowercase()];
        if let Some(info) = endpoint_info {
            if !info.chain_name.is_empty() {
                candidates.push(info.chain_name.to_ascii_lowercase());
            }
            candidates.extend(
                info.chain_name_aliases
                    .iter()
                    .map(|alias| alias.to_ascii_lowercase()),
            );
        }

        for candidate in candidates {
            if candidate.eq_ignore_ascii_case("tron-evm") {
                return Some("evm");
            }
            if candidate.contains("beacon") {
                return Some("beacon");
            }
            if candidate.contains("solana") {
                return Some("solana");
            }
            if candidate.contains("bitcoin") {
                return Some("bitcoin");
            }
            if candidate.contains("near") {
                return Some("near");
            }
            if candidate.contains("antelope") || candidate.contains("eos") {
                return Some("antelope");
            }
            if candidate.contains("cosmos") {
                return Some("cosmos");
            }
            if candidate.contains("tron") {
                return Some("tron");
            }
            if candidate.contains("ethereum") || candidate.contains("evm") || candidate == "mainnet"
            {
                return Some("evm");
            }
        }

        None
    }

    pub(crate) fn chain_name_is_solana(name: &str) -> bool {
        let normalized = name.to_ascii_lowercase();
        normalized == "solana" || normalized.starts_with("solana-")
    }

    pub(crate) fn chain_name_is_antelope(name: &str) -> bool {
        let normalized = name.to_ascii_lowercase();
        normalized == "antelope" || normalized.starts_with("antelope-") || normalized == "eos"
    }

    pub(crate) fn endpoint_chain_is_solana(endpoint_info: &Option<EndpointInfo>) -> bool {
        endpoint_info.as_ref().is_some_and(|ei| {
            chain_name_is_solana(&ei.chain_name)
                || ei
                    .chain_name_aliases
                    .iter()
                    .any(|alias| chain_name_is_solana(alias))
        })
    }

    pub(crate) fn endpoint_chain_is_antelope(endpoint_info: &Option<EndpointInfo>) -> bool {
        endpoint_info.as_ref().is_some_and(|ei| {
            chain_name_is_antelope(&ei.chain_name)
                || ei
                    .chain_name_aliases
                    .iter()
                    .any(|alias| chain_name_is_antelope(alias))
        })
    }

    pub(crate) fn cursor_metadata_block_type<'a>(
        cursor_state: Option<&'a CursorState>,
    ) -> Option<&'a str> {
        cursor_state.and_then(|state| state.get_metadata("firehose-parquet.block_type"))
    }

    pub(crate) fn cursor_chain_is_solana(cursor_state: Option<&CursorState>) -> bool {
        cursor_metadata_block_type(cursor_state).is_some_and(chain_name_is_solana)
            || cursor_state.is_some_and(|state| {
                state
                    .get_metadata("firehose-parquet.chain_name")
                    .is_some_and(chain_name_is_solana)
                    || state
                        .get_metadata("firehose-parquet.chain_name_aliases")
                        .is_some_and(|aliases| aliases.split(',').any(chain_name_is_solana))
            })
    }

    pub(crate) fn cursor_chain_is_antelope(cursor_state: Option<&CursorState>) -> bool {
        cursor_metadata_block_type(cursor_state).is_some_and(chain_name_is_antelope)
            || cursor_state.is_some_and(|state| {
                state
                    .get_metadata("firehose-parquet.chain_name")
                    .is_some_and(chain_name_is_antelope)
                    || state
                        .get_metadata("firehose-parquet.chain_name_aliases")
                        .is_some_and(|aliases| aliases.split(',').any(chain_name_is_antelope))
            })
    }

    pub(crate) fn chain_is_solana(
        requested_block_type: &str,
        endpoint_info: &Option<EndpointInfo>,
        cursor_state: Option<&CursorState>,
    ) -> bool {
        requested_block_type == "solana"
            || (requested_block_type == "auto"
                && (endpoint_chain_is_solana(endpoint_info)
                    || cursor_chain_is_solana(cursor_state)))
    }

    pub(crate) fn chain_is_antelope(
        requested_block_type: &str,
        endpoint_info: &Option<EndpointInfo>,
        cursor_state: Option<&CursorState>,
    ) -> bool {
        requested_block_type == "antelope"
            || (requested_block_type == "auto"
                && (endpoint_chain_is_antelope(endpoint_info)
                    || cursor_chain_is_antelope(cursor_state)))
    }

    pub(crate) fn chain_is_known_non_solana(
        requested_block_type: &str,
        endpoint_info: &Option<EndpointInfo>,
        cursor_state: Option<&CursorState>,
    ) -> bool {
        match requested_block_type {
            "auto" => {
                if endpoint_info.is_some() {
                    !endpoint_chain_is_solana(endpoint_info)
                } else {
                    cursor_metadata_block_type(cursor_state)
                        .is_some_and(|block_type| !chain_name_is_solana(block_type))
                }
            }
            "solana" => false,
            _ => true,
        }
    }

    pub(crate) fn unsupported_chain_feature_flag_warnings(
        requested_block_type: &str,
        endpoint_info: &Option<EndpointInfo>,
        cursor_state: Option<&CursorState>,
        without_extended: bool,
        without_votes: bool,
    ) -> Vec<&'static str> {
        let mut warnings = Vec::new();
        let solana_chain = chain_is_solana(requested_block_type, endpoint_info, cursor_state);
        let antelope_chain = chain_is_antelope(requested_block_type, endpoint_info, cursor_state);
        let known_non_solana_chain =
            chain_is_known_non_solana(requested_block_type, endpoint_info, cursor_state);

        if without_extended && (solana_chain || antelope_chain) {
            warnings.push(without_extended_warning_message());
        }
        if without_votes && known_non_solana_chain {
            warnings.push(without_votes_warning_message());
        }

        warnings
    }
}

/// Inline decisions from origin/main `ingestion/setup.rs` and `runtime.rs`,
/// reproduced with the legacy string helpers.
mod legacy_inline {
    use super::legacy;
    use crate::*;

    pub struct SetupDecisions {
        pub solana_chain: bool,
        pub antelope_chain: bool,
        pub known_non_solana_chain: bool,
        pub extended: bool,
        pub initial_block_type: Option<&'static str>,
        pub failed_transactions_block_type: Option<String>,
        pub include_failed_transactions: (bool, Vec<String>),
        pub initial_bytes_encoding: EncodeBytes,
        pub use_synthetic_partition_routing: bool,
    }

    /// `IngestionSetup::resolve` and `MapperState::new` for a validated
    /// lowercase `block_type` (`auto` or a family label).
    #[allow(clippy::too_many_arguments)]
    pub fn setup(
        block_type: &'static str,
        endpoint_info: &Option<EndpointInfo>,
        existing_cursor_state: Option<&CursorState>,
        without_extended: bool,
        include_failed: bool,
        exclude_failed: bool,
        cursor_override: bool,
        dry_run: bool,
        partition: &Partition,
    ) -> SetupDecisions {
        let mut extended = !without_extended;
        let solana_chain =
            legacy::chain_is_solana(block_type, endpoint_info, existing_cursor_state);
        let antelope_chain =
            legacy::chain_is_antelope(block_type, endpoint_info, existing_cursor_state);
        let known_non_solana_chain =
            legacy::chain_is_known_non_solana(block_type, endpoint_info, existing_cursor_state);
        if solana_chain {
            extended = false;
        } else if antelope_chain {
            extended = false;
        } else if known_non_solana_chain {
            extended = resolve_extended_mode(extended, without_extended, endpoint_info);
        }
        if !dry_run && block_type != "evm" {
            extended = false;
        }
        let tron_style_evm_profile = endpoint_uses_tron_style_evm_profile(endpoint_info);
        let initial_block_type = if block_type != "auto" {
            Some(block_type)
        } else {
            legacy::inferred_block_type_from_endpoint_info(endpoint_info)
        };
        let failed_transactions_block_type = initial_block_type.map(str::to_string).or_else(|| {
            legacy::cursor_metadata_block_type(existing_cursor_state).map(str::to_string)
        });
        let include_failed_transactions = legacy::resolve_include_failed_transactions(
            failed_transactions_block_type.as_deref(),
            include_failed,
            exclude_failed,
            existing_cursor_state,
            cursor_override,
        );
        let initial_bytes_encoding = legacy::resolve_auto_encode_bytes(
            initial_block_type,
            endpoint_info,
            tron_style_evm_profile,
        );
        SetupDecisions {
            solana_chain,
            antelope_chain,
            known_non_solana_chain,
            extended,
            initial_block_type,
            failed_transactions_block_type,
            include_failed_transactions,
            initial_bytes_encoding,
            use_synthetic_partition_routing: legacy::use_last_known_timestamp_partition_routing(
                block_type, partition,
            ),
        }
    }

    /// `IngestionRuntime::ensure_mapper` extended/failed-transaction choices.
    pub fn detected_extended(
        detected: &str,
        extended: bool,
        without_extended: bool,
        endpoint_info: &Option<EndpointInfo>,
    ) -> bool {
        if detected == "solana" {
            false
        } else if detected == "antelope" {
            false
        } else {
            resolve_extended_mode(extended, without_extended, endpoint_info)
        }
    }
}

const LABELS: [&str; 8] = [
    "evm", "bitcoin", "solana", "near", "antelope", "cosmos", "tron", "beacon",
];

fn requested_types() -> Vec<(&'static str, Option<ChainKind>)> {
    std::iter::once(("auto", None))
        .chain(
            ChainKind::ALL
                .into_iter()
                .map(|kind| (kind.label(), Some(kind))),
        )
        .collect()
}

fn endpoint(
    chain_name: &str,
    aliases: &[&str],
    block_id_encoding: i32,
    extended: bool,
) -> EndpointInfo {
    EndpointInfo {
        chain_name: chain_name.to_string(),
        chain_name_aliases: aliases.iter().map(|alias| alias.to_string()).collect(),
        first_streamable_block_num: 0,
        first_streamable_block_id: String::new(),
        block_id_encoding,
        block_features: if extended {
            vec!["base".to_string(), "extended".to_string()]
        } else {
            vec![]
        },
    }
}

/// Endpoint metadata: none, every built-in network name, and names that
/// exercise strict/substring disagreements, aliases and empty chain names.
fn endpoint_corpus() -> Vec<Option<EndpointInfo>> {
    let mut endpoints = vec![None];
    for (index, name) in KNOWN_NETWORK_NAMES.iter().enumerate() {
        endpoints.push(Some(endpoint(
            name,
            &[],
            (index % 4) as i32,
            index % 3 == 0,
        )));
    }
    let special: &[(&str, &[&str])] = &[
        ("", &[]),
        ("", &["solana-mainnet-beta"]),
        ("", &["eos", "mainnet"]),
        ("", &["unknown", "tron-evm"]),
        ("mystery", &["near-mainnet"]),
        ("mystery", &["solana"]),
        ("mystery", &["antelope-jungle4", "solana-devnet"]),
        ("Solana-Mainnet-Beta", &[]),
        ("solanaish", &[]),
        ("EOS", &["wax"]),
        ("eosio", &[]),
        ("tron-evm", &["tron-mainnet"]),
        ("tron", &[]),
        ("mainnet-cl", &["eth-beacon"]),
        ("gnosis-cl", &[]),
        ("cosmoshub-4", &[]),
        ("ethereum", &["evm"]),
        ("linear", &[]),
    ];
    for (index, (name, aliases)) in special.iter().enumerate() {
        for extended in [false, true] {
            endpoints.push(Some(endpoint(name, aliases, (index % 4) as i32, extended)));
        }
    }
    endpoints
}

fn cursor(entries: &[(&str, &str)], include_failed_transactions: bool) -> CursorState {
    let mut state = CursorState {
        include_failed_transactions,
        ..CursorState::default()
    };
    for (key, value) in entries {
        state.file_metadata.add(*key, *value);
    }
    state
}

/// Cursor metadata: missing, every family label, legacy or unknown labels,
/// and chain names/aliases that only strict matching recognizes.
fn cursor_corpus() -> Vec<Option<CursorState>> {
    let mut cursors = vec![None, Some(cursor(&[], true)), Some(cursor(&[], false))];
    let block_types = LABELS.iter().copied().chain([
        "auto",
        "ethereum",
        "Solana",
        "EVM",
        "eos",
        "solana-mainnet-beta",
        "",
    ]);
    for (index, block_type) in block_types.enumerate() {
        cursors.push(Some(cursor(
            &[("firehose-parquet.block_type", block_type)],
            index % 2 == 0,
        )));
    }
    let names: &[&[(&str, &str)]] = &[
        &[("firehose-parquet.chain_name", "solana-mainnet-beta")],
        &[("firehose-parquet.chain_name", "eos")],
        &[("firehose-parquet.chain_name", "mainnet")],
        &[("firehose-parquet.chain_name_aliases", "foo,solana")],
        &[("firehose-parquet.chain_name_aliases", "antelope-x,bar")],
        &[("firehose-parquet.chain_name_aliases", "")],
        &[
            ("firehose-parquet.block_type", "evm"),
            ("firehose-parquet.chain_name", "solana"),
        ],
        &[
            ("firehose-parquet.block_type", "antelope"),
            ("firehose-parquet.chain_name_aliases", "solana-devnet"),
        ],
    ];
    for (index, entries) in names.iter().enumerate() {
        cursors.push(Some(cursor(entries, index % 2 == 1)));
    }
    cursors
}

fn partitions() -> [Partition; 4] {
    [
        Partition::None,
        Partition::Date,
        Partition::Second,
        Partition::BlockRange {
            size: 100,
            start_block: Some(0),
        },
    ]
}

#[test]
fn pre_stream_setup_decisions_match_legacy_string_resolution() {
    let endpoints = endpoint_corpus();
    let cursors = cursor_corpus();
    let mut checked = 0_usize;
    for (label, requested) in requested_types() {
        for endpoint_info in &endpoints {
            for cursor_state in &cursors {
                let cursor_state = cursor_state.as_ref();
                let features =
                    PreStreamChainFeatures::resolve(requested, endpoint_info, cursor_state);
                for bits in 0..32_u8 {
                    let without_extended = bits & 1 != 0;
                    let include_failed = bits & 2 != 0;
                    let exclude_failed = bits & 4 != 0;
                    let cursor_override = bits & 8 != 0;
                    let dry_run = bits & 16 != 0;
                    let partition = &partitions()[usize::from(bits) % 4];
                    let context = format!(
                        "{label} {endpoint_info:?} {:?} bits={bits:05b}",
                        cursor_state.map(|state| &state.file_metadata.entries)
                    );
                    let expected = legacy_inline::setup(
                        label,
                        endpoint_info,
                        cursor_state,
                        without_extended,
                        include_failed,
                        exclude_failed,
                        cursor_override,
                        dry_run,
                        partition,
                    );
                    assert_eq!(
                        features.vote_transactions, expected.solana_chain,
                        "{context}"
                    );
                    assert_eq!(
                        features.extended_unsupported,
                        expected.solana_chain || expected.antelope_chain,
                        "{context}"
                    );
                    assert_eq!(
                        features.known_without_votes, expected.known_non_solana_chain,
                        "{context}"
                    );
                    // Dry-run cursor validation branch: Solana, then Antelope.
                    let branch = if features.vote_transactions {
                        "votes"
                    } else if features.extended_unsupported {
                        "drop-extended"
                    } else {
                        "none"
                    };
                    let expected_branch = if expected.solana_chain {
                        "votes"
                    } else if expected.antelope_chain {
                        "drop-extended"
                    } else {
                        "none"
                    };
                    assert_eq!(branch, expected_branch, "{context}");
                    assert_eq!(
                        resolve_pre_stream_extended(
                            features,
                            requested,
                            !without_extended,
                            without_extended,
                            endpoint_info,
                            dry_run,
                        ),
                        expected.extended,
                        "{context}"
                    );
                    let (initial, failed_transactions) =
                        pre_stream_block_types(requested, endpoint_info, cursor_state);
                    assert_eq!(
                        initial.map(ChainKind::label),
                        expected.initial_block_type,
                        "{context}"
                    );
                    assert_eq!(
                        resolve_include_failed_transactions(
                            failed_transactions,
                            include_failed,
                            exclude_failed,
                            cursor_state,
                            cursor_override,
                        ),
                        expected.include_failed_transactions,
                        "{context}"
                    );
                    let tron_style = endpoint_uses_tron_style_evm_profile(endpoint_info);
                    assert_eq!(
                        resolve_auto_encode_bytes(initial, endpoint_info, tron_style),
                        expected.initial_bytes_encoding,
                        "{context}"
                    );
                    assert_eq!(
                        requested.is_some_and(|kind| {
                            use_last_known_timestamp_partition_routing(kind, partition)
                        }),
                        expected.use_synthetic_partition_routing,
                        "{context}"
                    );
                    // Auto-detection re-resolves failed transactions only when the
                    // detected family differs from the pre-stream one.
                    for detected in ChainKind::ALL {
                        assert_eq!(
                            failed_transactions != Some(detected),
                            expected.failed_transactions_block_type.as_deref()
                                != Some(detected.label()),
                            "{context} detected={detected}"
                        );
                    }
                    checked += 1;
                }
                assert_eq!(
                    unsupported_chain_feature_flag_warnings(
                        requested,
                        endpoint_info,
                        cursor_state,
                        true,
                        true
                    ),
                    legacy::unsupported_chain_feature_flag_warnings(
                        label,
                        endpoint_info,
                        cursor_state,
                        true,
                        true
                    ),
                    "{label} {endpoint_info:?}"
                );
            }
        }
    }
    assert!(checked > 500_000, "{checked}");
}

#[test]
fn auto_detected_family_decisions_match_legacy() {
    let endpoints = endpoint_corpus();
    let cursors = cursor_corpus();
    let type_urls = ChainKind::ALL.map(|kind| {
        format!(
            "type.googleapis.com/sf.{}.type.v1.Block",
            kind.profile().type_url_marker
        )
    });
    for type_url in &type_urls {
        let detected = detect_block_type(type_url).unwrap();
        let label = legacy::detect_block_type(type_url).unwrap();
        assert_eq!(detected.label(), label);
        assert_eq!(
            detected.profile().family,
            legacy::protected_block_family(&label).unwrap()
        );
        for endpoint_info in &endpoints {
            for cursor_state in &cursors {
                assert_eq!(
                    unsupported_chain_feature_flag_warnings(
                        Some(detected),
                        endpoint_info,
                        cursor_state.as_ref(),
                        true,
                        true
                    ),
                    legacy::unsupported_chain_feature_flag_warnings(
                        &label,
                        endpoint_info,
                        cursor_state.as_ref(),
                        true,
                        true
                    )
                );
            }
            for extended in [false, true] {
                for without_extended in [false, true] {
                    assert_eq!(
                        resolve_detected_extended(
                            detected,
                            extended,
                            without_extended,
                            endpoint_info
                        ),
                        legacy_inline::detected_extended(
                            &label,
                            extended,
                            without_extended,
                            endpoint_info
                        ),
                        "{label} {endpoint_info:?}"
                    );
                }
            }
            let tron_style = endpoint_uses_tron_style_evm_profile(endpoint_info);
            assert_eq!(
                resolve_auto_encode_bytes(Some(detected), endpoint_info, tron_style),
                legacy::resolve_auto_encode_bytes(Some(&label), endpoint_info, tron_style)
            );
        }
        assert_eq!(
            detected.profile().block_number_gaps,
            legacy::block_type_allows_block_number_gaps(&label)
        );
    }
    for unknown in ["type.googleapis.com/sf.unknown.type.v1.Block", ""] {
        assert_eq!(
            detect_block_type(unknown).unwrap_err().to_string(),
            legacy::detect_block_type(unknown).unwrap_err().to_string()
        );
    }
}

#[test]
fn endpoint_and_partition_inference_match_legacy() {
    let endpoints = endpoint_corpus();
    let chains: Vec<&str> = KNOWN_NETWORK_NAMES
        .iter()
        .copied()
        .chain(["", "tron-evm", "Mystery", "eos", "solana-devnet", "mainnet"])
        .collect();
    for endpoint_info in &endpoints {
        assert_eq!(
            inferred_block_type_from_endpoint_info(endpoint_info).map(ChainKind::label),
            legacy::inferred_block_type_from_endpoint_info(endpoint_info),
            "{endpoint_info:?}"
        );
        assert_eq!(
            endpoint_chain_has(endpoint_info, has_nullable_timestamps),
            legacy::endpoint_chain_is_solana(endpoint_info),
            "partition routing policy {endpoint_info:?}"
        );
        for chain in &chains {
            assert_eq!(
                infer_partitions_block_type(chain, endpoint_info).map(ChainKind::label),
                legacy::infer_partitions_block_type(chain, endpoint_info),
                "{chain} {endpoint_info:?}"
            );
            for compression in [Compression::Zstd, Compression::Snappy] {
                let current = build_partitions_file_metadata(
                    "https://example.com:443",
                    chain,
                    "date",
                    compression,
                    endpoint_info,
                    Some(100),
                );
                let expected = legacy::build_partitions_file_metadata(
                    "https://example.com:443",
                    chain,
                    "date",
                    compression,
                    endpoint_info,
                    Some(100),
                );
                assert_eq!(
                    current.entries, expected.entries,
                    "{chain} {endpoint_info:?}"
                );
            }
        }
    }
}

#[test]
fn table_and_cursor_file_metadata_match_legacy() {
    let endpoints = endpoint_corpus();
    let encodings = [
        EncodeBytes::Binary,
        EncodeBytes::Hex,
        EncodeBytes::HexNoPrefix,
        EncodeBytes::Base58,
        EncodeBytes::TronBase58,
    ];
    for endpoint_info in endpoints.iter().step_by(3) {
        for (label, requested) in requested_types() {
            for encoding in &encodings {
                for flags in 0..16_u8 {
                    let (with_votes, synthetic, extended, include_failed) = (
                        flags & 1 != 0,
                        flags & 2 != 0,
                        flags & 4 != 0,
                        flags & 8 != 0,
                    );
                    let partition = &partitions()[usize::from(flags) % 4];
                    if let Some(kind) = requested {
                        let mut current = build_file_metadata(
                            kind,
                            encoding,
                            "https://example.com:443",
                            Compression::Zstd,
                            endpoint_info,
                        );
                        maybe_add_with_votes_metadata(&mut current, kind, with_votes);
                        maybe_add_synthetic_timestamp_metadata(&mut current, kind, synthetic);
                        let mut expected = legacy::build_file_metadata(
                            label,
                            encoding,
                            "https://example.com:443",
                            Compression::Zstd,
                            endpoint_info,
                        );
                        legacy::maybe_add_solana_with_votes_metadata(
                            &mut expected,
                            Some(label),
                            with_votes,
                        );
                        legacy::maybe_add_synthetic_timestamp_metadata(
                            &mut expected,
                            label,
                            synthetic,
                        );
                        assert_eq!(current.entries, expected.entries, "{label} {encoding:?}");
                    }
                    for cursor_encoding in [None, Some(encoding)] {
                        let current = build_cursor_file_metadata(
                            requested,
                            cursor_encoding,
                            "https://example.com:443",
                            Compression::Zstd,
                            partition,
                            endpoint_info,
                            extended,
                            flags & 2 != 0,
                            include_failed,
                        );
                        let expected = legacy::build_cursor_file_metadata(
                            requested.map(|_| label),
                            cursor_encoding,
                            "https://example.com:443",
                            Compression::Zstd,
                            partition,
                            endpoint_info,
                            extended,
                            flags & 2 != 0,
                            include_failed,
                        );
                        assert_eq!(current.entries, expected.entries, "{label} cursor");
                    }
                }
            }
        }
    }
}

//! Equivalence of the chain profiles with the string-based helpers they
//! replaced (#526). The `legacy` module holds verbatim copies of the
//! pre-#526 `blocks/src/bin/main.rs` functions; they exist only as oracles.

use super::*;
use arrow::datatypes::Schema;
use firehose_parquet::networks::KNOWN_NETWORK_NAMES;
use sha2::{Digest as _, Sha256};

#[allow(clippy::all)]
mod legacy {
    use super::*;
    use anyhow::{anyhow, Result};
    use firehose_parquet::grpc::EndpointInfo;

    pub const BLOCK_TYPES: &[&str] = &[
        "auto", "evm", "bitcoin", "solana", "near", "antelope", "cosmos", "tron", "beacon",
    ];

    pub fn output_encoding_policy_bytes(
        block_type: &str,
        tron_style_evm_profile: bool,
    ) -> Option<EncodeBytes> {
        match block_type {
            "evm" if tron_style_evm_profile => Some(EncodeBytes::TronBase58),
            "evm" | "bitcoin" | "cosmos" | "beacon" => Some(EncodeBytes::Hex),
            "antelope" => Some(EncodeBytes::HexNoPrefix),
            "solana" | "near" => Some(EncodeBytes::Base58),
            "tron" => Some(EncodeBytes::TronBase58),
            _ => None,
        }
    }

    pub fn infer_partitions_block_type(
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

    pub fn protected_block_family(label: &str) -> Result<BlockFamily> {
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

    pub fn detect_block_type(type_url: &str) -> Result<String> {
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

    pub fn block_type_allows_block_number_gaps(block_type: &str) -> bool {
        matches!(block_type, "solana" | "near" | "beacon")
    }

    pub fn block_type_has_nullable_timestamps(block_type: &str) -> bool {
        block_type == "solana"
    }

    pub fn chain_name_is_solana(name: &str) -> bool {
        let normalized = name.to_ascii_lowercase();
        normalized == "solana" || normalized.starts_with("solana-")
    }

    pub fn chain_name_is_antelope(name: &str) -> bool {
        let normalized = name.to_ascii_lowercase();
        normalized == "antelope" || normalized.starts_with("antelope-") || normalized == "eos"
    }

    /// `block_type == "solana"` / `"antelope"` / `!= "evm"` decisions made
    /// inline in setup, auto-detection and `resolve_include_failed_transactions`.
    pub fn forces_extended_off_before_streaming(block_type: &str) -> bool {
        block_type == "solana" || block_type == "antelope"
    }
    pub fn keeps_extended_output(block_type: &str) -> bool {
        block_type == "evm"
    }
    pub fn has_vote_transactions(block_type: &str) -> bool {
        block_type == "solana"
    }
    pub fn failed_transactions_by_default(block_type: &str) -> bool {
        block_type == "evm"
    }

    pub fn create_mapper(
        block_type: &str,
        extended: bool,
        with_votes: bool,
        include_fork_step: bool,
        encode_bytes: EncodeBytes,
        synthetic_partition_routing: bool,
        include_failed_transactions: bool,
    ) -> Result<Box<dyn BlockMapper>> {
        match block_type {
            "evm" => Ok(Box::new(EvmBlockMapper::new(
                extended,
                include_fork_step,
                encode_bytes,
                include_failed_transactions,
            ))),
            "bitcoin" => Ok(Box::new(BitcoinBlockMapper::new(
                include_fork_step,
                encode_bytes.clone(),
            ))),
            "solana" => Ok(Box::new(SolanaBlockMapper::new(
                with_votes,
                include_fork_step,
                encode_bytes,
                synthetic_partition_routing,
                include_failed_transactions,
            ))),
            "near" => Ok(Box::new(NearBlockMapper::new(
                include_fork_step,
                encode_bytes,
                include_failed_transactions,
            ))),
            "antelope" => Ok(Box::new(AntelopeBlockMapper::new(
                include_fork_step,
                encode_bytes,
                include_failed_transactions,
            ))),
            "cosmos" => Ok(Box::new(CosmosBlockMapper::new(
                include_fork_step,
                encode_bytes,
                include_failed_transactions,
            ))),
            "tron" => Ok(Box::new(TronBlockMapper::new(
                include_fork_step,
                encode_bytes,
                include_failed_transactions,
            ))),
            "beacon" => Ok(Box::new(BeaconBlockMapper::new(
                include_fork_step,
                encode_bytes,
            ))),
            other => Err(anyhow!(
                "unsupported block type: {other}. Supported: {}",
                BLOCK_TYPES.join(", ")
            )),
        }
    }
}

const TYPE_URLS: &[&str] = &[
    "type.googleapis.com/sf.ethereum.type.v2.Block",
    "type.googleapis.com/sf.bitcoin.type.v1.Block",
    "type.googleapis.com/sf.solana.type.v1.Block",
    "type.googleapis.com/sf.near.type.v1.Block",
    "type.googleapis.com/sf.antelope.type.v1.Block",
    "type.googleapis.com/sf.cosmos.type.v2.Block",
    "type.googleapis.com/sf.tron.type.v1.Block",
    "type.googleapis.com/sf.beacon.type.v1.Block",
    "type.googleapis.com/sf.unknown.type.v1.Block",
    "type.googleapis.com/sf.firehose.v2.Response",
    "",
];

/// Every inference keyword, alone and in every ordered pair, so rule
/// precedence is exercised as well as each rule.
const KEYWORDS: &[&str] = &[
    "beacon", "solana", "bitcoin", "near", "antelope", "eos", "cosmos", "tron", "ethereum", "evm",
    "mainnet", "tron-evm", "btc", "cl", "eth",
];

/// Names that exercise strict matching, case folding and substring traps.
const EXTRA_NAMES: &[&str] = &[
    "",
    "unknown",
    "Solana",
    "SOLANA-MAINNET-BETA",
    "solana-devnet",
    "solanaish",
    "solana_mainnet",
    "antelope-jungle4",
    "Antelope",
    "EOS",
    "eos-mainnet",
    "eosio",
    "wax",
    "TRON-EVM",
    "Tron",
    "tron-mainnet",
    "Mainnet",
    "mainnet-cl",
    "eth-mainnet",
    "ethereum-beacon",
    "near-mainnet",
    "linear",
    "geoscience",
    "cosmoshub-4",
    "gnosis-cl",
];

fn name_corpus() -> Vec<String> {
    let mut names: Vec<String> = KNOWN_NETWORK_NAMES
        .iter()
        .chain(EXTRA_NAMES)
        .chain(KEYWORDS)
        .map(|name| name.to_string())
        .collect();
    for first in KEYWORDS {
        for second in KEYWORDS {
            names.push(format!("{first}-{second}"));
            names.push(format!("{}{}", first.to_ascii_uppercase(), second));
        }
    }
    names.extend(
        ChainKind::ALL
            .iter()
            .flat_map(|kind| [kind.label().to_string(), kind.label().to_ascii_uppercase()]),
    );
    names
}

#[test]
fn labels_and_all_match_the_legacy_block_type_list() {
    let labels: Vec<&str> = ChainKind::ALL.iter().map(|kind| kind.label()).collect();
    assert_eq!(labels, legacy::BLOCK_TYPES[1..]);
    for label in legacy::BLOCK_TYPES {
        assert_eq!(
            ChainKind::from_label(label).map(ChainKind::label),
            (*label != "auto").then_some(*label)
        );
    }
    // Labels are exact, as before: callers lowercase `--block-type` first and
    // cursor metadata comparisons were exact string comparisons.
    for name in name_corpus() {
        assert_eq!(
            ChainKind::from_label(&name).map(ChainKind::label),
            legacy::BLOCK_TYPES[1..]
                .contains(&name.as_str())
                .then_some(name.as_str()),
            "{name}"
        );
    }
    for kind in ChainKind::ALL {
        assert_eq!(kind.to_string(), kind.label());
    }
}

#[test]
fn type_url_detection_matches_legacy_substring_order() {
    let mut inputs: Vec<String> = TYPE_URLS.iter().map(|url| url.to_string()).collect();
    inputs.extend(name_corpus());
    for input in inputs {
        assert_eq!(
            ChainKind::from_type_url(&input).map(ChainKind::label),
            legacy::detect_block_type(&input).ok().as_deref(),
            "{input}"
        );
    }
}

#[test]
fn chain_name_inference_matches_legacy_for_registry_and_synthetic_names() {
    let names = name_corpus();
    assert!(names.len() > 250, "corpus should cover the registry");
    for name in &names {
        assert_eq!(
            ChainKind::infer_from_chain_name(name).map(ChainKind::label),
            legacy::infer_partitions_block_type(name, &None),
            "{name}"
        );
    }
    // Multi-name inference keeps first-match order across names.
    for first in &names {
        for second in ["", "solana-mainnet-beta", "eos", "mainnet"] {
            assert_eq!(
                ChainKind::infer_from_chain_names([first.as_str(), second]).map(ChainKind::label),
                legacy::infer_partitions_block_type(
                    first,
                    &Some(firehose_parquet::grpc::EndpointInfo {
                        chain_name: second.to_string(),
                        chain_name_aliases: vec![],
                        first_streamable_block_num: 0,
                        first_streamable_block_id: String::new(),
                        block_id_encoding: 0,
                        block_features: vec![],
                    })
                ),
                "{first} then {second}"
            );
        }
    }
}

#[test]
fn strict_chain_name_matching_matches_legacy_solana_and_antelope_helpers() {
    for name in name_corpus() {
        assert_eq!(
            ChainKind::Solana.matches_chain_name(&name),
            legacy::chain_name_is_solana(&name),
            "{name}"
        );
        assert_eq!(
            ChainKind::Antelope.matches_chain_name(&name),
            legacy::chain_name_is_antelope(&name),
            "{name}"
        );
        for kind in ChainKind::ALL {
            if !matches!(kind, ChainKind::Solana | ChainKind::Antelope) {
                assert!(!kind.matches_chain_name(&name), "{kind} {name}");
            }
        }
    }
}

#[test]
fn strict_names_exist_for_every_family_resolved_before_streaming() {
    for kind in ChainKind::ALL {
        let profile = kind.profile();
        let resolved_before_streaming = profile.vote_transactions
            || profile.nullable_timestamps
            || profile.extended == ExtendedOutput::Unsupported;
        assert_eq!(
            resolved_before_streaming,
            !profile.strict_chain_names.is_empty(),
            "{kind}"
        );
        for name in profile
            .strict_chain_names
            .iter()
            .chain(profile.strict_chain_name_prefixes)
        {
            assert_eq!(*name, name.to_ascii_lowercase(), "{kind} {name}");
        }
    }
}

#[test]
fn profile_facts_match_the_legacy_string_checks() {
    for kind in ChainKind::ALL {
        let label = kind.label();
        let profile = kind.profile();
        assert_eq!(
            profile.family,
            legacy::protected_block_family(label).unwrap(),
            "{label}"
        );
        // The protected mirror and journal spell families with serde's
        // snake_case names; they must equal the metadata labels.
        assert_eq!(
            serde_json::to_string(&profile.family).unwrap(),
            format!("\"{label}\"")
        );
        assert_eq!(
            profile.block_number_gaps,
            legacy::block_type_allows_block_number_gaps(label),
            "{label}"
        );
        assert_eq!(
            profile.nullable_timestamps,
            legacy::block_type_has_nullable_timestamps(label),
            "{label}"
        );
        assert_eq!(
            profile.extended == ExtendedOutput::Unsupported,
            legacy::forces_extended_off_before_streaming(label),
            "{label}"
        );
        assert_eq!(
            profile.extended == ExtendedOutput::Supported,
            legacy::keeps_extended_output(label),
            "{label}"
        );
        assert_eq!(
            profile.vote_transactions,
            legacy::has_vote_transactions(label),
            "{label}"
        );
        assert_eq!(
            profile.failed_transactions_by_default,
            legacy::failed_transactions_by_default(label),
            "{label}"
        );
        assert_eq!(profile.type_url_marker, profile.type_url_marker.trim());
        for tron_style in [false, true] {
            assert_eq!(
                Some(kind.default_bytes_encoding(tron_style)),
                legacy::output_encoding_policy_bytes(label, tron_style),
                "{label} tron_style={tron_style}"
            );
        }
    }
    assert!(legacy::protected_block_family("auto").is_err());
}

fn all_encodings() -> [EncodeBytes; 5] {
    [
        EncodeBytes::Binary,
        EncodeBytes::Hex,
        EncodeBytes::HexNoPrefix,
        EncodeBytes::Base58,
        EncodeBytes::TronBase58,
    ]
}

/// Every mapper option combination, as `(options, legacy positional args)`.
fn option_matrix() -> Vec<MapperOptions> {
    let mut options = Vec::new();
    for encode_bytes in all_encodings() {
        for bits in 0..32_u8 {
            options.push(MapperOptions {
                extended: bits & 1 != 0,
                with_votes: bits & 2 != 0,
                include_fork_step: bits & 4 != 0,
                encode_bytes: encode_bytes.clone(),
                synthetic_partition_routing: bits & 8 != 0,
                include_failed_transactions: bits & 16 != 0,
            });
        }
    }
    options
}

/// The table inventory and complete Arrow schemas of one empty mapper, in
/// declared and sorted-table order.
fn inventory_lines(
    kind: ChainKind,
    options: &MapperOptions,
    mapper: &mut dyn BlockMapper,
) -> Vec<String> {
    let context = format!(
        "{}|{:?}|ext={}|votes={}|fork={}|synth={}|failed={}",
        kind.label(),
        options.encode_bytes,
        options.extended,
        options.with_votes,
        options.include_fork_step,
        options.synthetic_partition_routing,
        options.include_failed_transactions
    );
    let mut lines = vec![format!("{context}|names={:?}", mapper.table_names())];
    let batches = mapper.flush().unwrap();
    let mut tables: Vec<_> = batches.iter().collect();
    tables.sort_by(|left, right| left.0.cmp(right.0));
    for (table, batch) in tables {
        let schema: &Schema = &batch.schema();
        lines.push(format!("{context}|{table}|{schema:?}"));
    }
    lines
}

#[test]
fn create_mapper_matches_the_legacy_constructor_dispatch_for_every_option() {
    for kind in ChainKind::ALL {
        for options in option_matrix() {
            let mut current = kind.create_mapper(options.clone());
            let mut previous = legacy::create_mapper(
                kind.label(),
                options.extended,
                options.with_votes,
                options.include_fork_step,
                options.encode_bytes.clone(),
                options.synthetic_partition_routing,
                options.include_failed_transactions,
            )
            .unwrap();
            assert_eq!(
                inventory_lines(kind, &options, current.as_mut()),
                inventory_lines(kind, &options, previous.as_mut())
            );
        }
    }
    assert!(legacy::create_mapper(
        "unknown",
        false,
        false,
        false,
        EncodeBytes::Hex,
        false,
        false
    )
    .is_err());
}

/// SHA-256 over every family's table inventory and complete Arrow schemas for
/// all 160 option/encoding combinations. The pinned value was produced by the
/// same loop over the pre-#526 `create_mapper` on origin/main `9372f99`, so it
/// also covers the shared fork-step and enum helpers moved into `traits.rs`.
#[test]
fn every_mapper_schema_matches_the_pre_526_digest() {
    let mut hasher = Sha256::new();
    for kind in ChainKind::ALL {
        for options in option_matrix() {
            let mut mapper = kind.create_mapper(options.clone());
            for line in inventory_lines(kind, &options, mapper.as_mut()) {
                hasher.update(line.as_bytes());
                hasher.update(b"\n");
            }
        }
    }
    let digest: String = hasher
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    assert_eq!(digest, PRE_526_SCHEMA_DIGEST);
}

const PRE_526_SCHEMA_DIGEST: &str =
    "68e8859576f696910042452f8815a6e7f9002c9357e4dd2a26edf30c61249dde";

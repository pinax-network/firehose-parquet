//! Chain profiles: the per-family facts that ingestion, file metadata and
//! partition tooling need, kept in one place instead of `block_type` string
//! comparisons at every call site.
//!
//! To add a chain family:
//!
//! 1. Add a [`ChainKind`] variant and append it to [`ChainKind::ALL`]. The order
//!    of `ALL` is also the `type_url` detection order.
//! 2. Describe it with a [`ChainProfile`] in [`ChainKind::profile`].
//! 3. Construct its mapper in [`ChainKind::create_mapper`].
//! 4. Add any chain-name inference rule to [`CHAIN_NAME_RULES`] (ordered).
//!
//! The compiler reports every exhaustive `match` that needs the new variant,
//! including the protected [`BlockFamily`] mapping.

use std::fmt;

use firehose_parquet::encode::EncodeBytes;
use firehose_parquet::ingest::BlockFamily;
use firehose_parquet::traits::BlockMapper;

use crate::antelope::mapper::AntelopeBlockMapper;
use crate::beacon::mapper::BeaconBlockMapper;
use crate::bitcoin::mapper::BitcoinBlockMapper;
use crate::cosmos::mapper::CosmosBlockMapper;
use crate::evm::mapper::EvmBlockMapper;
use crate::near::mapper::NearBlockMapper;
use crate::solana::mapper::SolanaBlockMapper;
use crate::tron::mapper::TronBlockMapper;

/// A supported Firehose block family (`--block-type` other than `auto`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ChainKind {
    Evm,
    Bitcoin,
    Solana,
    Near,
    Antelope,
    Cosmos,
    Tron,
    Beacon,
}

/// How a family treats extended output and `--without-extended`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExtendedOutput {
    /// The mapper writes extended tables while extended output is enabled.
    Supported,
    /// The mapper has no extended tables. The endpoint's advertised capability
    /// is still logged; protected output records `extended=false`.
    NotMapped,
    /// Extended output is always disabled before streaming, and
    /// `--without-extended` warns that it had no effect.
    Unsupported,
}

/// Static description of one [`ChainKind`].
#[derive(Debug)]
pub struct ChainProfile {
    /// `--block-type` value and `firehose-parquet.block_type` metadata value.
    pub label: &'static str,
    /// Substring of the Firehose `Any.type_url` that identifies the family.
    pub type_url_marker: &'static str,
    /// Protected ingestion identity.
    pub family: BlockFamily,
    /// Output bytes encoding contract for `bytes_encoding=auto`.
    pub bytes_encoding: EncodeBytes,
    /// Encoding contract override on Tron-style endpoints (`tron`, `tron-evm`).
    pub tron_style_bytes_encoding: Option<EncodeBytes>,
    /// Blocks may lack timestamps: canonical `timestamp`/`date` are nullable
    /// and time partitions route by the last known timestamp.
    pub nullable_timestamps: bool,
    /// Block numbers can legitimately skip (slots or heights), so a bounded
    /// range may end below `stop_block - 1`.
    pub block_number_gaps: bool,
    /// Extended output behavior.
    pub extended: ExtendedOutput,
    /// The mapper can write `vote_transactions` (`--without-votes`).
    pub vote_transactions: bool,
    /// Failed transactions are written by default (#494). Other families
    /// only write them with `--include-failed-transactions`.
    pub failed_transactions_by_default: bool,
    /// Lowercase chain names that strictly identify the family in endpoint
    /// or cursor metadata before the first block, for `--block-type auto`.
    /// Needed for families with votes, nullable timestamps or unsupported
    /// extended output; substring inference uses [`CHAIN_NAME_RULES`] instead.
    pub strict_chain_names: &'static [&'static str],
    /// Lowercase chain-name prefixes with the same role as `strict_chain_names`.
    pub strict_chain_name_prefixes: &'static [&'static str],
}

const EVM: ChainProfile = ChainProfile {
    label: "evm",
    type_url_marker: "ethereum",
    family: BlockFamily::Evm,
    bytes_encoding: EncodeBytes::Hex,
    tron_style_bytes_encoding: Some(EncodeBytes::TronBase58),
    nullable_timestamps: false,
    block_number_gaps: false,
    extended: ExtendedOutput::Supported,
    vote_transactions: false,
    failed_transactions_by_default: true,
    strict_chain_names: &[],
    strict_chain_name_prefixes: &[],
};

const BITCOIN: ChainProfile = ChainProfile {
    label: "bitcoin",
    type_url_marker: "bitcoin",
    family: BlockFamily::Bitcoin,
    bytes_encoding: EncodeBytes::Hex,
    tron_style_bytes_encoding: None,
    nullable_timestamps: false,
    block_number_gaps: false,
    extended: ExtendedOutput::NotMapped,
    vote_transactions: false,
    failed_transactions_by_default: false,
    strict_chain_names: &[],
    strict_chain_name_prefixes: &[],
};

const SOLANA: ChainProfile = ChainProfile {
    label: "solana",
    type_url_marker: "solana",
    family: BlockFamily::Solana,
    bytes_encoding: EncodeBytes::Base58,
    tron_style_bytes_encoding: None,
    nullable_timestamps: true,
    block_number_gaps: true,
    extended: ExtendedOutput::Unsupported,
    vote_transactions: true,
    failed_transactions_by_default: false,
    strict_chain_names: &["solana"],
    strict_chain_name_prefixes: &["solana-"],
};

const NEAR: ChainProfile = ChainProfile {
    label: "near",
    type_url_marker: "near",
    family: BlockFamily::Near,
    bytes_encoding: EncodeBytes::Base58,
    tron_style_bytes_encoding: None,
    nullable_timestamps: false,
    block_number_gaps: true,
    extended: ExtendedOutput::NotMapped,
    vote_transactions: false,
    failed_transactions_by_default: false,
    strict_chain_names: &[],
    strict_chain_name_prefixes: &[],
};

const ANTELOPE: ChainProfile = ChainProfile {
    label: "antelope",
    type_url_marker: "antelope",
    family: BlockFamily::Antelope,
    bytes_encoding: EncodeBytes::HexNoPrefix,
    tron_style_bytes_encoding: None,
    nullable_timestamps: false,
    block_number_gaps: false,
    extended: ExtendedOutput::Unsupported,
    vote_transactions: false,
    failed_transactions_by_default: false,
    strict_chain_names: &["antelope", "eos"],
    strict_chain_name_prefixes: &["antelope-"],
};

const COSMOS: ChainProfile = ChainProfile {
    label: "cosmos",
    type_url_marker: "cosmos",
    family: BlockFamily::Cosmos,
    bytes_encoding: EncodeBytes::Hex,
    tron_style_bytes_encoding: None,
    nullable_timestamps: false,
    block_number_gaps: false,
    extended: ExtendedOutput::NotMapped,
    vote_transactions: false,
    failed_transactions_by_default: false,
    strict_chain_names: &[],
    strict_chain_name_prefixes: &[],
};

const TRON: ChainProfile = ChainProfile {
    label: "tron",
    type_url_marker: "tron",
    family: BlockFamily::Tron,
    bytes_encoding: EncodeBytes::TronBase58,
    tron_style_bytes_encoding: None,
    nullable_timestamps: false,
    block_number_gaps: false,
    extended: ExtendedOutput::NotMapped,
    vote_transactions: false,
    failed_transactions_by_default: false,
    strict_chain_names: &[],
    strict_chain_name_prefixes: &[],
};

const BEACON: ChainProfile = ChainProfile {
    label: "beacon",
    type_url_marker: "beacon",
    family: BlockFamily::Beacon,
    bytes_encoding: EncodeBytes::Hex,
    tron_style_bytes_encoding: None,
    nullable_timestamps: false,
    block_number_gaps: true,
    extended: ExtendedOutput::NotMapped,
    vote_transactions: false,
    failed_transactions_by_default: false,
    strict_chain_names: &[],
    strict_chain_name_prefixes: &[],
};

/// One ordered chain-name inference rule, matched against a lowercase name.
#[derive(Clone, Copy, Debug)]
pub enum NameRule {
    Exact(&'static str),
    Contains(&'static str),
}

impl NameRule {
    fn matches(self, name: &str) -> bool {
        match self {
            NameRule::Exact(value) => name == value,
            NameRule::Contains(value) => name.contains(value),
        }
    }
}

/// Substring inference from endpoint/network chain names. The first matching
/// rule wins, so the order is part of the contract: for example `tron-evm`
/// resolves to EVM before the `tron` rule is tried.
pub const CHAIN_NAME_RULES: &[(NameRule, ChainKind)] = &[
    (NameRule::Exact("tron-evm"), ChainKind::Evm),
    (NameRule::Contains("beacon"), ChainKind::Beacon),
    (NameRule::Contains("solana"), ChainKind::Solana),
    (NameRule::Contains("bitcoin"), ChainKind::Bitcoin),
    (NameRule::Contains("near"), ChainKind::Near),
    (NameRule::Contains("antelope"), ChainKind::Antelope),
    (NameRule::Contains("eos"), ChainKind::Antelope),
    (NameRule::Contains("cosmos"), ChainKind::Cosmos),
    (NameRule::Contains("tron"), ChainKind::Tron),
    (NameRule::Contains("ethereum"), ChainKind::Evm),
    (NameRule::Contains("evm"), ChainKind::Evm),
    (NameRule::Exact("mainnet"), ChainKind::Evm),
];

/// Options that select a mapper's tables and encodings.
#[derive(Clone, Debug)]
pub struct MapperOptions {
    /// EVM extended tables.
    pub extended: bool,
    /// Solana `vote_transactions`.
    pub with_votes: bool,
    /// Append the `fork_step` column (non-final streams).
    pub include_fork_step: bool,
    pub encode_bytes: EncodeBytes,
    /// Solana last-known timestamp partition routing.
    pub synthetic_partition_routing: bool,
    pub include_failed_transactions: bool,
}

impl ChainKind {
    /// Every family, in `type_url` detection and `--block-type` help order.
    pub const ALL: [ChainKind; 8] = [
        ChainKind::Evm,
        ChainKind::Bitcoin,
        ChainKind::Solana,
        ChainKind::Near,
        ChainKind::Antelope,
        ChainKind::Cosmos,
        ChainKind::Tron,
        ChainKind::Beacon,
    ];

    pub fn profile(self) -> &'static ChainProfile {
        match self {
            ChainKind::Evm => &EVM,
            ChainKind::Bitcoin => &BITCOIN,
            ChainKind::Solana => &SOLANA,
            ChainKind::Near => &NEAR,
            ChainKind::Antelope => &ANTELOPE,
            ChainKind::Cosmos => &COSMOS,
            ChainKind::Tron => &TRON,
            ChainKind::Beacon => &BEACON,
        }
    }

    pub fn label(self) -> &'static str {
        self.profile().label
    }

    /// Parse an exact `--block-type`/`firehose-parquet.block_type` label.
    /// `auto` and unknown labels are `None`.
    pub fn from_label(label: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|kind| kind.label() == label)
    }

    /// Detect the family from a Firehose `Any.type_url` (first marker in
    /// [`ChainKind::ALL`] order).
    pub fn from_type_url(type_url: &str) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|kind| type_url.contains(kind.profile().type_url_marker))
    }

    /// Infer the family from one chain name or alias with [`CHAIN_NAME_RULES`].
    pub fn infer_from_chain_name(name: &str) -> Option<Self> {
        let name = name.to_ascii_lowercase();
        CHAIN_NAME_RULES
            .iter()
            .find(|(rule, _)| rule.matches(&name))
            .map(|(_, kind)| *kind)
    }

    /// Infer the family from the first name that matches any rule.
    pub fn infer_from_chain_names<'a>(names: impl IntoIterator<Item = &'a str>) -> Option<Self> {
        names.into_iter().find_map(Self::infer_from_chain_name)
    }

    /// Strict chain-name match (exact name or `name-` prefix), without the
    /// substring inference of [`ChainKind::infer_from_chain_name`].
    pub fn matches_chain_name(self, name: &str) -> bool {
        let name = name.to_ascii_lowercase();
        let profile = self.profile();
        profile.strict_chain_names.contains(&name.as_str())
            || profile
                .strict_chain_name_prefixes
                .iter()
                .any(|prefix| name.starts_with(prefix))
    }

    /// Output bytes encoding contract for `bytes_encoding=auto`.
    pub fn default_bytes_encoding(self, tron_style_evm_profile: bool) -> EncodeBytes {
        let profile = self.profile();
        match (&profile.tron_style_bytes_encoding, tron_style_evm_profile) {
            (Some(encoding), true) => encoding.clone(),
            _ => profile.bytes_encoding.clone(),
        }
    }

    /// Construct this family's mapper.
    pub fn create_mapper(self, options: MapperOptions) -> Box<dyn BlockMapper> {
        let MapperOptions {
            extended,
            with_votes,
            include_fork_step,
            encode_bytes,
            synthetic_partition_routing,
            include_failed_transactions,
        } = options;
        match self {
            ChainKind::Evm => Box::new(EvmBlockMapper::new(
                extended,
                include_fork_step,
                encode_bytes,
                include_failed_transactions,
            )),
            ChainKind::Bitcoin => {
                Box::new(BitcoinBlockMapper::new(include_fork_step, encode_bytes))
            }
            ChainKind::Solana => Box::new(SolanaBlockMapper::new(
                with_votes,
                include_fork_step,
                encode_bytes,
                synthetic_partition_routing,
                include_failed_transactions,
            )),
            ChainKind::Near => Box::new(NearBlockMapper::new(
                include_fork_step,
                encode_bytes,
                include_failed_transactions,
            )),
            ChainKind::Antelope => Box::new(AntelopeBlockMapper::new(
                include_fork_step,
                encode_bytes,
                include_failed_transactions,
            )),
            ChainKind::Cosmos => Box::new(CosmosBlockMapper::new(
                include_fork_step,
                encode_bytes,
                include_failed_transactions,
            )),
            ChainKind::Tron => Box::new(TronBlockMapper::new(
                include_fork_step,
                encode_bytes,
                include_failed_transactions,
            )),
            ChainKind::Beacon => Box::new(BeaconBlockMapper::new(include_fork_step, encode_bytes)),
        }
    }
}

impl fmt::Display for ChainKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.label())
    }
}

#[cfg(test)]
mod tests;

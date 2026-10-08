//! Chain profiles: the per-family facts that ingestion, file metadata and
//! partition tooling need, kept in one place instead of `block_type` string
//! comparisons at every call site.
//!
//! To add a chain family:
//!
//! 1. Add a [`ChainKind`] variant and append it to [`ChainKind::ALL`]. The order
//!    of `ALL` is also the `type_url` detection order.
//! 2. Describe it with a [`ChainProfile`] in [`ChainKind::profile`], including
//!    the `UInt64` columns its Delta tables store as `decimal(20,0)`
//!    (`decimal_columns`).
//! 3. Construct its mapper in [`ChainKind::create_mapper`].
//! 4. Add any chain-name inference rule to [`CHAIN_NAME_RULES`] (ordered).
//!
//! The compiler reports every exhaustive `match` that needs the new variant,
//! including the protected [`BlockFamily`] mapping.

use std::fmt;

use firehose_parquet::delta::types::{DecimalColumn, DeltaTypes};
use firehose_parquet::encode::EncodeBytes;
use firehose_parquet::ingest::BlockFamily;
use firehose_parquet::traits::BlockMapper;

use crate::antelope::mapper::AntelopeBlockMapper;
use crate::beacon::mapper::BeaconBlockMapper;
use crate::bitcoin::mapper::BitcoinBlockMapper;
use crate::cosmos::mapper::CosmosBlockMapper;
use crate::evm::mapper::EvmBlockMapper;
use crate::hypercore::mapper::HypercoreBlockMapper;
use crate::near::mapper::NearBlockMapper;
use crate::sec::mapper::SecBlockMapper;
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
    Sec,
    Hypercore,
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
    /// Block numbers can legitimately skip (slots or heights). This does not
    /// relax bounded completion: protected builds and dry runs both require the
    /// accepted boundary to reach `stop_block - 1` on every chain.
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
    /// `UInt64` columns stored as Delta `decimal(20,0)` instead of a checked
    /// `long` (#643, `docs/design/delta-lake.md` §6): currency amounts,
    /// balances and fees, and values a sender or signer chooses without a range
    /// check. Every other `UInt64` column is bounded by its protocol, and a
    /// value above `i64::MAX` in it refuses the flush
    /// (`firehose_parquet::delta::types`). Bitcoin satoshis stay `long`:
    /// consensus caps them at 2.1·10^15.
    pub decimal_columns: &'static [DecimalColumn],
    /// The Firehose block id is text, not a hash: HyperCore's is the decimal
    /// block number, SEC's the decimal window number. The canonical `block_id`
    /// and `parent_id` hold that text verbatim under every text encoding (its
    /// ASCII bytes under `binary`, `PreparedIdentity::with_text_ids`), the file
    /// metadata records `firehose-parquet.block_id_encoding = decimal`, and
    /// `docs/schemas/` types the two columns `string` (decimal).
    pub block_id_text: bool,
    /// `build` settings whose generic default does not suit the family.
    pub build_defaults: BuildDefaults,
}

/// `build` defaults of one family, each applied only when the operator set
/// neither the flag nor its environment variable. `None` keeps the generic
/// default.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct BuildDefaults {
    /// `--grpc-max-message-bytes`.
    pub grpc_max_message_bytes: Option<u32>,
    /// `--flush-idle-secs`.
    pub flush_idle_secs: Option<u64>,
    /// `--stream-idle-timeout-secs`.
    pub stream_idle_timeout_secs: Option<u64>,
    /// `--metrics-stale-after-secs`.
    pub metrics_stale_after_secs: Option<u64>,
}

impl BuildDefaults {
    /// Every generic default.
    pub const GENERIC: Self = Self {
        grpc_max_message_bytes: None,
        flush_idle_secs: None,
        stream_idle_timeout_secs: None,
        metrics_stale_after_secs: None,
    };
}

/// firesec delivers one EDGAR daily feed as a burst of 144 windows, then
/// nothing for about a day; the window of a 13F or N-PX deadline day can
/// exceed the generic 128 MiB message limit (143.9 MB on 2026-08-14).
const SEC_BUILD_DEFAULTS: BuildDefaults = BuildDefaults {
    // 3.7x the largest sampled window. The limit only bounds a message; the
    // writer's memory follows the windows actually received.
    grpc_max_message_bytes: Some(512 * 1024 * 1024),
    // Commit each burst a minute after it ends instead of holding it, unread
    // and uncommitted, until the next feed day's first window.
    flush_idle_secs: Some(60),
    // A day without messages is normal: reconnect only after 26 hours (one
    // feed day plus firesec's hourly poll and margin). HTTP/2 keepalive still
    // detects a dead connection within a minute.
    stream_idle_timeout_secs: Some(26 * 3600),
    // `/ready` fails only when a whole feed day is missing (36 hours).
    metrics_stale_after_secs: Some(36 * 3600),
};

impl ChainProfile {
    /// The Delta type decisions for this family's tables.
    pub fn delta_types(&self) -> DeltaTypes {
        DeltaTypes::new(self.decimal_columns)
    }
}

/// A `decimal(20,0)` column and why a checked `long` does not fit it.
const fn decimal(table: &'static str, column: &'static str, reason: &'static str) -> DecimalColumn {
    DecimalColumn {
        table,
        column,
        reason,
    }
}

const LAMPORTS: &str = "lamports: a currency amount";
const GWEI: &str = "Gwei: a currency amount";
const SIGNED_SLOT: &str = "slashing evidence: the protocol checks the signature over the \
                           header, not the range of the slot it names";
const SIGNED_ATTESTATION: &str = "slashing evidence: the protocol checks the signature over \
                                  the attestation, not the range of its data";

const EVM_DECIMALS: &[DecimalColumn] = &[
    decimal(
        "blocks",
        "nonce",
        "PoW block nonce: any 64-bit value, often above i64::MAX",
    ),
    decimal(
        "set_code_authorizations",
        "nonce",
        "EIP-7702 authorization nonce: chosen by the signer and not range-checked at inclusion",
    ),
    decimal("withdrawals", "amount_gwei", GWEI),
];

const SOLANA_DECIMALS: &[DecimalColumn] = &[
    decimal("transactions", "fee", LAMPORTS),
    decimal("transactions", "pre_balances", LAMPORTS),
    decimal("transactions", "post_balances", LAMPORTS),
    decimal("vote_transactions", "fee", LAMPORTS),
    decimal("vote_transactions", "pre_balances", LAMPORTS),
    decimal("vote_transactions", "post_balances", LAMPORTS),
    decimal("rewards", "post_balance", LAMPORTS),
];

const BEACON_DECIMALS: &[DecimalColumn] = &[
    decimal("deposits", "amount", GWEI),
    decimal("withdrawals", "amount", GWEI),
    decimal("deposit_requests", "amount", GWEI),
    decimal("withdrawal_requests", "amount", GWEI),
    decimal("proposer_slashings", "header_1_slot", SIGNED_SLOT),
    decimal("proposer_slashings", "header_2_slot", SIGNED_SLOT),
    decimal(
        "attester_slashings",
        "attestation_1_slot",
        SIGNED_ATTESTATION,
    ),
    decimal(
        "attester_slashings",
        "attestation_1_committee_index",
        SIGNED_ATTESTATION,
    ),
    decimal(
        "attester_slashings",
        "attestation_1_source_epoch",
        SIGNED_ATTESTATION,
    ),
    decimal(
        "attester_slashings",
        "attestation_1_target_epoch",
        SIGNED_ATTESTATION,
    ),
    decimal(
        "attester_slashings",
        "attestation_2_slot",
        SIGNED_ATTESTATION,
    ),
    decimal(
        "attester_slashings",
        "attestation_2_committee_index",
        SIGNED_ATTESTATION,
    ),
    decimal(
        "attester_slashings",
        "attestation_2_source_epoch",
        SIGNED_ATTESTATION,
    ),
    decimal(
        "attester_slashings",
        "attestation_2_target_epoch",
        SIGNED_ATTESTATION,
    ),
];

const ANTELOPE_DECIMALS: &[DecimalColumn] = &[decimal(
    "actions",
    "error_code",
    "`eosio_assert_code` error code: the contract passes any uint64",
)];

const COSMOS_DECIMALS: &[DecimalColumn] = &[
    decimal(
        "transactions",
        "timeout_height",
        "chosen by the sender and not range-checked",
    ),
    decimal(
        "transactions",
        "fee_gas_limit",
        "chosen by the sender; unbounded when a chain's maximum block gas is -1",
    ),
];

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
    decimal_columns: EVM_DECIMALS,
    block_id_text: false,
    build_defaults: BuildDefaults::GENERIC,
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
    decimal_columns: &[],
    block_id_text: false,
    build_defaults: BuildDefaults::GENERIC,
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
    decimal_columns: SOLANA_DECIMALS,
    block_id_text: false,
    build_defaults: BuildDefaults::GENERIC,
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
    decimal_columns: &[],
    block_id_text: false,
    build_defaults: BuildDefaults::GENERIC,
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
    decimal_columns: ANTELOPE_DECIMALS,
    block_id_text: false,
    build_defaults: BuildDefaults::GENERIC,
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
    decimal_columns: COSMOS_DECIMALS,
    block_id_text: false,
    build_defaults: BuildDefaults::GENERIC,
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
    decimal_columns: &[],
    block_id_text: false,
    build_defaults: BuildDefaults::GENERIC,
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
    decimal_columns: BEACON_DECIMALS,
    block_id_text: false,
    build_defaults: BuildDefaults::GENERIC,
};

const SEC: ChainProfile = ChainProfile {
    label: "sec",
    // The Any type URL is `type.googleapis.com/pinax.sec.v1.Block`; a bare
    // `sec` marker would be a needless substring trap.
    type_url_marker: "pinax.sec.",
    family: BlockFamily::Sec,
    // No SEC field is hash or address bytes: the encoding only makes
    // `block_id`/`parent_id` text, written verbatim (decimal window numbers).
    bytes_encoding: EncodeBytes::Hex,
    tron_style_bytes_encoding: None,
    nullable_timestamps: false,
    // firesec emits every 10-minute window, empty ones included.
    block_number_gaps: false,
    extended: ExtendedOutput::NotMapped,
    vote_transactions: false,
    failed_transactions_by_default: false,
    strict_chain_names: &[],
    strict_chain_name_prefixes: &[],
    // No UInt64 domain column and no native Decimal128(20,0).
    decimal_columns: &[],
    // The mapper writes the decimal window numbers with
    // `PreparedIdentity::with_text_ids` (`sec::prepare::prepare_block`).
    block_id_text: true,
    build_defaults: SEC_BUILD_DEFAULTS,
};

const HYPERCORE: ChainProfile = ChainProfile {
    label: "hypercore",
    type_url_marker: "hypercore",
    family: BlockFamily::Hypercore,
    bytes_encoding: EncodeBytes::Hex,
    tron_style_bytes_encoding: None,
    nullable_timestamps: false,
    // The endpoint lacks blocks 846903300–846903312, a hole in the source data
    // rather than a chain property (`docs/chains/hypercore.md`).
    block_number_gaps: false,
    extended: ExtendedOutput::NotMapped,
    vote_transactions: false,
    failed_transactions_by_default: false,
    strict_chain_names: &[],
    strict_chain_name_prefixes: &[],
    // Every `UInt64` fits a checked `long`: `order_id` (about 40 bits), the
    // trade id `transaction_id` (a 50-bit hash), `twap_id`, `slot_id`, and
    // `nonce` (at most 51 bits; HyperLiquid range-checks user-signed nonces
    // against the block time, and HyperEVM-originated ones are a sequence).
    // A larger value refuses the block (R5) before any flush.
    decimal_columns: &[],
    block_id_text: true,
    build_defaults: BuildDefaults::GENERIC,
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
    // `Contains("sec")` would also catch names such as `secret-*`.
    (NameRule::Exact("sec"), ChainKind::Sec),
    (NameRule::Contains("beacon"), ChainKind::Beacon),
    (NameRule::Contains("solana"), ChainKind::Solana),
    (NameRule::Contains("bitcoin"), ChainKind::Bitcoin),
    (NameRule::Contains("near"), ChainKind::Near),
    (NameRule::Contains("antelope"), ChainKind::Antelope),
    (NameRule::Contains("eos"), ChainKind::Antelope),
    (NameRule::Contains("cosmos"), ChainKind::Cosmos),
    (NameRule::Contains("tron"), ChainKind::Tron),
    // Not `hyper`: `hyper-evm` (HyperEVM) stays EVM through the `evm` rule.
    (NameRule::Contains("hypercore"), ChainKind::Hypercore),
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
    /// Append the `fork_step` and `stream_ordinal` columns (non-final streams).
    pub include_fork_step: bool,
    pub encode_bytes: EncodeBytes,
    /// Solana last-known timestamp partition routing.
    pub synthetic_partition_routing: bool,
    pub include_failed_transactions: bool,
}

impl ChainKind {
    /// Every family, in `type_url` detection and `--block-type` help order.
    pub const ALL: [ChainKind; 10] = [
        ChainKind::Evm,
        ChainKind::Bitcoin,
        ChainKind::Solana,
        ChainKind::Near,
        ChainKind::Antelope,
        ChainKind::Cosmos,
        ChainKind::Tron,
        ChainKind::Beacon,
        ChainKind::Sec,
        ChainKind::Hypercore,
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
            ChainKind::Sec => &SEC,
            ChainKind::Hypercore => &HYPERCORE,
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
            ChainKind::Sec => Box::new(SecBlockMapper::new(include_fork_step, encode_bytes)),
            ChainKind::Hypercore => {
                Box::new(HypercoreBlockMapper::new(include_fork_step, encode_bytes))
            }
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

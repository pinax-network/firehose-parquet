use crate::networks::BuiltinNetwork;

pub const GENERATED_NETWORKS: &[BuiltinNetwork] = &[
    BuiltinNetwork {
        canonical: "mainnet",
        aliases: &["mainnet", "eth"],
        default_endpoint: "https://eth.firehose.pinax.network:443",
    },
    BuiltinNetwork {
        canonical: "solana-mainnet-beta",
        aliases: &["solana-mainnet-beta", "solana"],
        default_endpoint: "https://solana.firehose.pinax.network:443",
    },
    BuiltinNetwork {
        canonical: "tron",
        aliases: &["tron"],
        default_endpoint: "https://tron.firehose.pinax.network:443",
    },
    BuiltinNetwork {
        canonical: "tronevm",
        aliases: &["tronevm"],
        default_endpoint: "https://tronevm.firehose.pinax.network:443",
    },
];

pub const GENERATED_NETWORK_ALIASES: &[&str] = &[
    "mainnet",
    "eth",
    "solana-mainnet-beta",
    "solana",
    "tron",
    "tronevm",
];

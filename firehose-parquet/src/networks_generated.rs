use crate::networks::BuiltinNetwork;

pub const GENERATED_NETWORKS: &[BuiltinNetwork] = &[
    BuiltinNetwork {
        chain_name: "arbitrum-nova",
        default_endpoint: "https://arbnova.firehose.pinax.network:443",
    },
    BuiltinNetwork {
        chain_name: "arbitrum-one",
        default_endpoint: "https://arbone.firehose.pinax.network:443",
    },
    BuiltinNetwork {
        chain_name: "arbitrum-sepolia",
        default_endpoint: "https://arbsepolia.firehose.pinax.network:443",
    },
    BuiltinNetwork {
        chain_name: "avalanche",
        default_endpoint: "https://avalanche.firehose.pinax.network:443",
    },
    BuiltinNetwork {
        chain_name: "base",
        default_endpoint: "https://base.firehose.pinax.network:443",
    },
    BuiltinNetwork {
        chain_name: "base-sepolia",
        default_endpoint: "https://basesepolia.firehose.pinax.network:443",
    },
    BuiltinNetwork {
        chain_name: "blast-mainnet",
        default_endpoint: "https://blast.firehose.pinax.network:443",
    },
    BuiltinNetwork {
        chain_name: "bnb-op",
        default_endpoint: "https://opbnb.firehose.pinax.network:443",
    },
    BuiltinNetwork {
        chain_name: "bnb-svm",
        default_endpoint: "https://svmbnb.firehose.pinax.network:443",
    },
    BuiltinNetwork {
        chain_name: "bsc",
        default_endpoint: "https://bsc.firehose.pinax.network:443",
    },
    BuiltinNetwork {
        chain_name: "btc",
        default_endpoint: "https://bitcoin.firehose.pinax.network:443",
    },
    BuiltinNetwork {
        chain_name: "celo",
        default_endpoint: "https://celo.firehose.pinax.network:443",
    },
    BuiltinNetwork {
        chain_name: "chapel",
        default_endpoint: "https://chapel.firehose.pinax.network:443",
    },
    BuiltinNetwork {
        chain_name: "eos",
        default_endpoint: "https://eos.firehose.pinax.network:443",
    },
    BuiltinNetwork {
        chain_name: "fantom",
        default_endpoint: "https://fantom.firehose.pinax.network:443",
    },
    BuiltinNetwork {
        chain_name: "fuse",
        default_endpoint: "https://fuse.firehose.pinax.network:443",
    },
    BuiltinNetwork {
        chain_name: "gnosis-chiado-cl",
        default_endpoint: "https://chiado-cl.firehose.pinax.network:443",
    },
    BuiltinNetwork {
        chain_name: "gnosis-cl",
        default_endpoint: "https://gnosis-cl.firehose.pinax.network:443",
    },
    BuiltinNetwork {
        chain_name: "hoodi",
        default_endpoint: "https://hoodi.firehose.pinax.network:443",
    },
    BuiltinNetwork {
        chain_name: "hoodi-cl",
        default_endpoint: "https://hoodi-cl.firehose.pinax.network:443",
    },
    BuiltinNetwork {
        chain_name: "hyper-evm",
        default_endpoint: "https://hyperevm.firehose.pinax.network:443",
    },
    BuiltinNetwork {
        chain_name: "ink",
        default_endpoint: "https://ink.firehose.pinax.network:443",
    },
    BuiltinNetwork {
        chain_name: "jungle4",
        default_endpoint: "https://jungle4.firehose.pinax.network:443",
    },
    BuiltinNetwork {
        chain_name: "linea",
        default_endpoint: "https://linea.firehose.pinax.network:443",
    },
    BuiltinNetwork {
        chain_name: "linea-sepolia",
        default_endpoint: "https://lineasepolia.firehose.pinax.network:443",
    },
    BuiltinNetwork {
        chain_name: "litecoin",
        default_endpoint: "https://litecoin.firehose.pinax.network:443",
    },
    BuiltinNetwork {
        chain_name: "mainnet",
        default_endpoint: "https://eth.firehose.pinax.network:443",
    },
    BuiltinNetwork {
        chain_name: "mainnet-cl",
        default_endpoint: "https://eth-cl.firehose.pinax.network:443",
    },
    BuiltinNetwork {
        chain_name: "matic",
        default_endpoint: "https://polygon.firehose.pinax.network:443",
    },
    BuiltinNetwork {
        chain_name: "mode-mainnet",
        default_endpoint: "https://mode.firehose.pinax.network:443",
    },
    BuiltinNetwork {
        chain_name: "moonbeam",
        default_endpoint: "https://moonbeam.firehose.pinax.network:443",
    },
    BuiltinNetwork {
        chain_name: "moonriver",
        default_endpoint: "https://moonriver.firehose.pinax.network:443",
    },
    BuiltinNetwork {
        chain_name: "near-mainnet",
        default_endpoint: "https://near.firehose.pinax.network:443",
    },
    BuiltinNetwork {
        chain_name: "near-testnet",
        default_endpoint: "https://neartest.firehose.pinax.network:443",
    },
    BuiltinNetwork {
        chain_name: "optimism",
        default_endpoint: "https://optimism.firehose.pinax.network:443",
    },
    BuiltinNetwork {
        chain_name: "optimism-sepolia",
        default_endpoint: "https://opsepolia.firehose.pinax.network:443",
    },
    BuiltinNetwork {
        chain_name: "polygon-amoy",
        default_endpoint: "https://amoy.firehose.pinax.network:443",
    },
    BuiltinNetwork {
        chain_name: "ronin",
        default_endpoint: "https://ronin.firehose.pinax.network:443",
    },
    BuiltinNetwork {
        chain_name: "scroll",
        default_endpoint: "https://scroll.firehose.pinax.network:443",
    },
    BuiltinNetwork {
        chain_name: "scroll-sepolia",
        default_endpoint: "https://scrsepolia.firehose.pinax.network:443",
    },
    BuiltinNetwork {
        chain_name: "sepolia",
        default_endpoint: "https://sepolia.firehose.pinax.network:443",
    },
    BuiltinNetwork {
        chain_name: "sepolia-cl",
        default_endpoint: "https://sepolia-cl.firehose.pinax.network:443",
    },
    BuiltinNetwork {
        chain_name: "solana-devnet",
        default_endpoint: "https://soldev.firehose.pinax.network:443",
    },
    BuiltinNetwork {
        chain_name: "solana-mainnet-beta",
        default_endpoint: "https://solana.firehose.pinax.network:443",
    },
    BuiltinNetwork {
        chain_name: "soneium",
        default_endpoint: "https://soneium.firehose.pinax.network:443",
    },
    BuiltinNetwork {
        chain_name: "soneium-testnet",
        default_endpoint: "https://minato.firehose.pinax.network:443",
    },
    BuiltinNetwork {
        chain_name: "telos",
        default_endpoint: "https://telos.firehose.pinax.network:443",
    },
    BuiltinNetwork {
        chain_name: "telos-testnet",
        default_endpoint: "https://telostest.firehose.pinax.network:443",
    },
    BuiltinNetwork {
        chain_name: "tron",
        default_endpoint: "https://tron.firehose.pinax.network:443",
    },
    BuiltinNetwork {
        chain_name: "tron-evm",
        default_endpoint: "https://tronevm.firehose.pinax.network:443",
    },
    BuiltinNetwork {
        chain_name: "unichain",
        default_endpoint: "https://unichain.firehose.pinax.network:443",
    },
    BuiltinNetwork {
        chain_name: "unichain-testnet",
        default_endpoint: "https://unisepolia.firehose.pinax.network:443",
    },
    BuiltinNetwork {
        chain_name: "wax",
        default_endpoint: "https://wax.firehose.pinax.network:443",
    },
    BuiltinNetwork {
        chain_name: "wax-testnet",
        default_endpoint: "https://waxtest.firehose.pinax.network:443",
    },
    BuiltinNetwork {
        chain_name: "zora",
        default_endpoint: "https://zora.firehose.pinax.network:443",
    },
];

pub const GENERATED_NETWORK_NAMES: &[&str] = &[
    "arbitrum-nova",
    "arbitrum-one",
    "arbitrum-sepolia",
    "avalanche",
    "base",
    "base-sepolia",
    "blast-mainnet",
    "bnb-op",
    "bnb-svm",
    "bsc",
    "btc",
    "celo",
    "chapel",
    "eos",
    "fantom",
    "fuse",
    "gnosis-chiado-cl",
    "gnosis-cl",
    "hoodi",
    "hoodi-cl",
    "hyper-evm",
    "ink",
    "jungle4",
    "linea",
    "linea-sepolia",
    "litecoin",
    "mainnet",
    "mainnet-cl",
    "matic",
    "mode-mainnet",
    "moonbeam",
    "moonriver",
    "near-mainnet",
    "near-testnet",
    "optimism",
    "optimism-sepolia",
    "polygon-amoy",
    "ronin",
    "scroll",
    "scroll-sepolia",
    "sepolia",
    "sepolia-cl",
    "solana-devnet",
    "solana-mainnet-beta",
    "soneium",
    "soneium-testnet",
    "telos",
    "telos-testnet",
    "tron",
    "tron-evm",
    "unichain",
    "unichain-testnet",
    "wax",
    "wax-testnet",
    "zora",
];

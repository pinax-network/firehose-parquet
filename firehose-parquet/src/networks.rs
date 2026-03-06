use anyhow::anyhow;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BuiltinNetwork {
    pub canonical: &'static str,
    pub aliases: &'static [&'static str],
    pub default_endpoint: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EndpointSource {
    Builtin,
    EnvOverride { env_var: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedNetworkEndpoint {
    pub requested: String,
    pub canonical: &'static str,
    pub endpoint: String,
    pub source: EndpointSource,
}

pub const KNOWN_NETWORK_ALIASES: &[&str] = &[
    "mainnet",
    "eth",
    "solana-mainnet-beta",
    "solana",
    "tron",
    "tronevm",
];

const BUILTIN_NETWORKS: &[BuiltinNetwork] = &[
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

pub fn normalize_network_name(name: &str) -> String {
    name.trim().to_ascii_lowercase()
}

pub fn normalize_network_for_env(name: &str) -> String {
    let mut normalized = String::new();
    let mut last_was_separator = false;

    for ch in name.trim().chars() {
        if ch.is_ascii_alphanumeric() {
            normalized.push(ch.to_ascii_uppercase());
            last_was_separator = false;
        } else if !normalized.is_empty() && !last_was_separator {
            normalized.push('_');
            last_was_separator = true;
        }
    }

    while normalized.ends_with('_') {
        normalized.pop();
    }

    normalized
}

pub fn network_override_env_var(name: &str) -> String {
    format!("FIREHOSE_ENDPOINT_{}", normalize_network_for_env(name))
}

pub fn resolve_network_endpoint(name: &str) -> anyhow::Result<ResolvedNetworkEndpoint> {
    let requested = normalize_network_name(name);
    let network = BUILTIN_NETWORKS
        .iter()
        .find(|network| network.aliases.iter().any(|alias| *alias == requested))
        .ok_or_else(|| {
            anyhow!(
                "unsupported network `{}`; known values: {}",
                name.trim(),
                KNOWN_NETWORK_ALIASES.join(", ")
            )
        })?;

    for env_var in candidate_env_vars(&requested, network.canonical) {
        if let Ok(endpoint) = std::env::var(&env_var) {
            let endpoint = endpoint.trim();
            if !endpoint.is_empty() {
                return Ok(ResolvedNetworkEndpoint {
                    requested,
                    canonical: network.canonical,
                    endpoint: endpoint.to_string(),
                    source: EndpointSource::EnvOverride { env_var },
                });
            }
        }
    }

    Ok(ResolvedNetworkEndpoint {
        requested,
        canonical: network.canonical,
        endpoint: network.default_endpoint.to_string(),
        source: EndpointSource::Builtin,
    })
}

fn candidate_env_vars(requested: &str, canonical: &str) -> Vec<String> {
    let mut vars = vec![network_override_env_var(requested)];
    if canonical != requested {
        vars.push(network_override_env_var(canonical));
    }
    vars
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;

    #[test]
    fn test_normalize_network_for_env() {
        assert_eq!(normalize_network_for_env("mainnet"), "MAINNET");
        assert_eq!(normalize_network_for_env("eth"), "ETH");
        assert_eq!(
            normalize_network_for_env("solana-mainnet-beta"),
            "SOLANA_MAINNET_BETA"
        );
        assert_eq!(
            normalize_network_for_env("solana/mainnet beta"),
            "SOLANA_MAINNET_BETA"
        );
        assert_eq!(normalize_network_for_env("  tron.evm  "), "TRON_EVM");
    }

    #[test]
    #[serial]
    fn test_resolve_network_endpoint_builtin_aliases() {
        unsafe {
            std::env::remove_var("FIREHOSE_ENDPOINT_ETH");
            std::env::remove_var("FIREHOSE_ENDPOINT_MAINNET");
            std::env::remove_var("FIREHOSE_ENDPOINT_SOLANA");
            std::env::remove_var("FIREHOSE_ENDPOINT_SOLANA_MAINNET_BETA");
            std::env::remove_var("FIREHOSE_ENDPOINT_TRON");
            std::env::remove_var("FIREHOSE_ENDPOINT_TRONEVM");
        }

        let eth = resolve_network_endpoint("eth").expect("eth should resolve");
        assert_eq!(eth.canonical, "mainnet");
        assert_eq!(eth.endpoint, "https://eth.firehose.pinax.network:443");
        assert_eq!(eth.source, EndpointSource::Builtin);

        let solana = resolve_network_endpoint("solana").expect("solana should resolve");
        assert_eq!(solana.canonical, "solana-mainnet-beta");
        assert_eq!(solana.endpoint, "https://solana.firehose.pinax.network:443");
        assert_eq!(solana.source, EndpointSource::Builtin);

        let tronevm = resolve_network_endpoint("tronevm").expect("tronevm should resolve");
        assert_eq!(tronevm.canonical, "tronevm");
        assert_eq!(
            tronevm.endpoint,
            "https://tronevm.firehose.pinax.network:443"
        );
        assert_eq!(tronevm.source, EndpointSource::Builtin);
    }

    #[test]
    fn test_resolve_network_endpoint_unknown_network() {
        let err = resolve_network_endpoint("unknown-network").expect_err("unknown should fail");
        assert!(err
            .to_string()
            .contains("unsupported network `unknown-network`"));
        assert!(err
            .to_string()
            .contains("mainnet, eth, solana-mainnet-beta, solana, tron, tronevm"));
    }

    #[test]
    #[serial]
    fn test_resolve_network_endpoint_uses_requested_alias_override_first() {
        unsafe {
            std::env::set_var(
                "FIREHOSE_ENDPOINT_ETH",
                "https://override-eth.example.com:443",
            );
            std::env::set_var(
                "FIREHOSE_ENDPOINT_MAINNET",
                "https://override-mainnet.example.com:443",
            );
        }

        let resolved = resolve_network_endpoint("eth").expect("eth override should resolve");
        assert_eq!(resolved.endpoint, "https://override-eth.example.com:443");
        assert_eq!(
            resolved.source,
            EndpointSource::EnvOverride {
                env_var: "FIREHOSE_ENDPOINT_ETH".to_string()
            }
        );

        unsafe {
            std::env::remove_var("FIREHOSE_ENDPOINT_ETH");
            std::env::remove_var("FIREHOSE_ENDPOINT_MAINNET");
        }
    }

    #[test]
    #[serial]
    fn test_resolve_network_endpoint_falls_back_to_canonical_override() {
        unsafe {
            std::env::set_var(
                "FIREHOSE_ENDPOINT_SOLANA_MAINNET_BETA",
                "https://override-solana.example.com:443",
            );
        }

        let resolved =
            resolve_network_endpoint("solana").expect("canonical solana override should resolve");
        assert_eq!(resolved.endpoint, "https://override-solana.example.com:443");
        assert_eq!(
            resolved.source,
            EndpointSource::EnvOverride {
                env_var: "FIREHOSE_ENDPOINT_SOLANA_MAINNET_BETA".to_string()
            }
        );

        unsafe {
            std::env::remove_var("FIREHOSE_ENDPOINT_SOLANA_MAINNET_BETA");
        }
    }
}

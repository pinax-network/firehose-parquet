use anyhow::anyhow;

use crate::networks_generated;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BuiltinNetwork {
    pub chain_name: &'static str,
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
    pub chain_name: &'static str,
    pub endpoint: String,
    pub source: EndpointSource,
}

pub const KNOWN_NETWORK_NAMES: &[&str] = networks_generated::GENERATED_NETWORK_NAMES;

const BUILTIN_NETWORKS: &[BuiltinNetwork] = networks_generated::GENERATED_NETWORKS;

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
        .find(|network| network.chain_name == requested)
        .ok_or_else(|| {
            anyhow!(
                "unsupported network `{}`; known values: {}",
                name.trim(),
                KNOWN_NETWORK_NAMES.join(", ")
            )
        })?;

    for env_var in candidate_env_vars(&requested, network.chain_name) {
        if let Ok(endpoint) = std::env::var(&env_var) {
            let endpoint = endpoint.trim();
            if !endpoint.is_empty() {
                return Ok(ResolvedNetworkEndpoint {
                    requested,
                    chain_name: network.chain_name,
                    endpoint: endpoint.to_string(),
                    source: EndpointSource::EnvOverride { env_var },
                });
            }
        }
    }

    Ok(ResolvedNetworkEndpoint {
        requested,
        chain_name: network.chain_name,
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
    fn test_resolve_network_endpoint_builtin_names() {
        unsafe {
            std::env::remove_var("FIREHOSE_ENDPOINT_MAINNET");
            std::env::remove_var("FIREHOSE_ENDPOINT_SOLANA_MAINNET_BETA");
            std::env::remove_var("FIREHOSE_ENDPOINT_TRON");
            std::env::remove_var("FIREHOSE_ENDPOINT_TRONEVM");
        }

        let mainnet = resolve_network_endpoint("mainnet").expect("mainnet should resolve");
        assert_eq!(mainnet.chain_name, "mainnet");
        assert_eq!(mainnet.endpoint, "https://eth.firehose.pinax.network:443");
        assert_eq!(mainnet.source, EndpointSource::Builtin);

        let solana =
            resolve_network_endpoint("solana-mainnet-beta").expect("solana should resolve");
        assert_eq!(solana.chain_name, "solana-mainnet-beta");
        assert_eq!(solana.endpoint, "https://solana.firehose.pinax.network:443");
        assert_eq!(solana.source, EndpointSource::Builtin);

        let tronevm = resolve_network_endpoint("tron-evm").expect("tron-evm should resolve");
        assert_eq!(tronevm.chain_name, "tron-evm");
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
        assert!(err.to_string().contains("mainnet"));
    }

    #[test]
    #[serial]
    fn test_resolve_network_endpoint_uses_requested_name_override() {
        unsafe {
            std::env::set_var(
                "FIREHOSE_ENDPOINT_MAINNET",
                "https://override-mainnet.example.com:443",
            );
        }

        let resolved =
            resolve_network_endpoint("mainnet").expect("mainnet override should resolve");
        assert_eq!(resolved.endpoint, "https://override-mainnet.example.com:443");
        assert_eq!(
            resolved.source,
            EndpointSource::EnvOverride {
                env_var: "FIREHOSE_ENDPOINT_MAINNET".to_string()
            }
        );

        unsafe {
            std::env::remove_var("FIREHOSE_ENDPOINT_MAINNET");
        }
    }

    #[test]
    #[serial]
    fn test_resolve_network_endpoint_name_normalizes_for_env() {
        unsafe {
            std::env::set_var(
                "FIREHOSE_ENDPOINT_SOLANA_MAINNET_BETA",
                "https://override-solana.example.com:443",
            );
        }

        let resolved = resolve_network_endpoint("solana-mainnet-beta")
            .expect("solana override should resolve");
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

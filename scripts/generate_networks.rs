use std::fs;
use std::path::PathBuf;

use anyhow::{anyhow, Context, Result};
use serde::Deserialize;

/// Provider whose Firehose endpoint a built-in alias uses when the registry lists one.
const DEFAULT_PROVIDER: &str = "pinax.network";

/// Networks the default provider no longer serves, kept on another Firehose
/// provider that the registry lists for them. Each entry was checked live by
/// streaming blocks with a StreamingFast-compatible token (#535). The default provider
/// still wins if the registry lists it again.
const FALLBACK_PROVIDERS: &[(&str, &str)] = &[
    ("near-mainnet", "streamingfast.io"),
    ("near-testnet", "streamingfast.io"),
    ("tron", "streamingfast.io"),
    ("tron-evm", "streamingfast.io"),
];

/// Networks whose registry endpoint is known to be broken. They get no built-in
/// alias until the endpoint works again.
const EXCLUDED_NETWORKS: &[(&str, &str)] = &[(
    "robinhood-sepolia",
    "robsepolia.firehose.pinax.network has no DNS record (checked 2026-09-24)",
)];

/// Pinax-served Firehose networks that the registry does not list yet, as
/// `(alias, endpoint, reason)`. Hand-maintained: each entry is a reviewed
/// addition whose endpoint answers `scripts/check_network_endpoints.sh`.
/// They are appended to the registry's aliases. Once the registry lists the
/// same name, the registry entry wins and the generator warns until the entry
/// is dropped from this list.
const PINAX_NETWORKS: &[(&str, &str, &str)] = &[(
    "hypercore",
    "https://hypercore.firehose.pinax.network:443",
    "HyperLiquid L1 (HyperCore); not in The Graph networks registry",
)];

#[derive(Debug, Deserialize)]
struct Registry {
    #[serde(default)]
    version: Option<String>,
    #[serde(default)]
    #[serde(rename = "updatedAt")]
    updated_at: Option<String>,
    #[serde(default)]
    networks: Vec<RegistryNetwork>,
}

#[derive(Debug, Default, Deserialize)]
struct RegistryNetwork {
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    short_name: Option<String>,
    #[serde(default)]
    #[serde(rename = "shortName")]
    short_name_alt: Option<String>,
    #[serde(default)]
    services: RegistryServices,
}

#[derive(Debug, Default, Deserialize)]
struct RegistryServices {
    #[serde(default)]
    firehose: Vec<String>,
}

#[derive(Debug)]
struct BuiltNetwork {
    chain_name: String,
    default_endpoint: String,
    /// The `PINAX_NETWORKS` reason of an internal alias; `None` for an alias
    /// from the registry.
    internal_reason: Option<String>,
}

fn main() -> Result<()> {
    let mut args = std::env::args().skip(1);
    let input = args
        .next()
        .map(PathBuf::from)
        .ok_or_else(|| anyhow!("usage: cargo run -p firehose-parquet --bin generate-networks -- <registry.json> [output.rs]"))?;
    let output = args
        .next()
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("firehose-parquet/src/networks_generated.rs"));

    let raw = fs::read_to_string(&input)
        .with_context(|| format!("failed to read registry file {}", input.display()))?;
    let registry: Registry = serde_json::from_str(&raw)
        .with_context(|| format!("failed to parse registry file {}", input.display()))?;

    let (built, warnings) = build_networks(&registry.networks, PINAX_NETWORKS)?;
    for warning in &warnings {
        eprintln!("warning: {warning}");
    }
    for (network, provider) in FALLBACK_PROVIDERS {
        if !built.iter().any(|b| b.chain_name == *network) {
            eprintln!("warning: fallback network `{network}` has no `{provider}` Firehose endpoint in the registry");
        }
    }

    let rendered = render(&built, &snapshot_label(&registry));
    fs::write(&output, rendered)
        .with_context(|| format!("failed to write generated file {}", output.display()))?;

    eprintln!("wrote {} networks to {}", built.len(), output.display());
    Ok(())
}

/// The registry's aliases plus the `internal` ones it does not list, sorted by
/// name, and a warning for every internal entry the registry now lists (the
/// registry entry wins).
fn build_networks(
    networks: &[RegistryNetwork],
    internal: &[(&str, &str, &str)],
) -> Result<(Vec<BuiltNetwork>, Vec<String>)> {
    let mut built = Vec::new();
    for network in networks {
        let Some(chain_name) = canonical_name(network) else {
            if let Some(endpoint) = provider_endpoint(network, DEFAULT_PROVIDER) {
                return Err(anyhow!(
                    "missing canonical network name for endpoint {endpoint}"
                ));
            }
            continue;
        };
        if let Some((_, reason)) = EXCLUDED_NETWORKS
            .iter()
            .find(|(name, _)| *name == chain_name)
        {
            eprintln!("skipping `{chain_name}`: {reason}");
            continue;
        }
        if let Some(endpoint) = select_endpoint(&chain_name, network) {
            built.push(BuiltNetwork {
                chain_name,
                default_endpoint: endpoint,
                internal_reason: None,
            });
        }
    }

    let mut warnings = Vec::new();
    for (alias, endpoint, reason) in internal {
        if let Some(listed) = built.iter().find(|b| b.chain_name == *alias) {
            warnings.push(format!(
                "internal network `{alias}` is now in the registry ({}); the registry entry wins, drop `{alias}` from PINAX_NETWORKS",
                listed.default_endpoint
            ));
            continue;
        }
        built.push(BuiltNetwork {
            chain_name: alias.to_string(),
            default_endpoint: endpoint.to_string(),
            internal_reason: Some(reason.to_string()),
        });
    }

    built.sort_by(|a, b| a.chain_name.cmp(&b.chain_name));
    Ok((built, warnings))
}

/// Picks the default provider's endpoint, or the fallback provider's endpoint
/// for networks listed in `FALLBACK_PROVIDERS`.
fn select_endpoint(chain_name: &str, network: &RegistryNetwork) -> Option<String> {
    provider_endpoint(network, DEFAULT_PROVIDER).or_else(|| {
        FALLBACK_PROVIDERS
            .iter()
            .find(|(name, _)| *name == chain_name)
            .and_then(|(_, provider)| provider_endpoint(network, provider))
    })
}

fn provider_endpoint(network: &RegistryNetwork, provider: &str) -> Option<String> {
    network
        .services
        .firehose
        .iter()
        .find(|endpoint| endpoint.to_ascii_lowercase().contains(provider))
        .map(|endpoint| with_https(endpoint))
}

fn canonical_name(network: &RegistryNetwork) -> Option<String> {
    network
        .id
        .as_deref()
        .map(normalize_alias)
        .filter(|value| !value.is_empty())
        .or_else(|| {
            network
                .short_name_alt
                .as_deref()
                .or(network.short_name.as_deref())
                .map(normalize_alias)
        })
        .filter(|value| !value.is_empty())
}

fn with_https(value: &str) -> String {
    let trimmed = value.trim();
    if trimmed.starts_with("http://") || trimmed.starts_with("https://") {
        trimmed.to_string()
    } else {
        format!("https://{trimmed}")
    }
}

fn normalize_alias(value: &str) -> String {
    let mut normalized = String::new();
    let mut last_dash = false;
    for ch in value.trim().chars() {
        if ch.is_ascii_alphanumeric() {
            normalized.push(ch.to_ascii_lowercase());
            last_dash = false;
        } else if !normalized.is_empty() && !last_dash {
            normalized.push('-');
            last_dash = true;
        }
    }
    normalized.trim_matches('-').to_string()
}

fn snapshot_label(registry: &Registry) -> String {
    let version = registry.version.as_deref().unwrap_or("unknown");
    match registry.updated_at.as_deref() {
        Some(updated_at) => format!("v{version}, updatedAt {updated_at}"),
        None => format!("v{version}"),
    }
}

fn render(networks: &[BuiltNetwork], snapshot: &str) -> String {
    let mut out = String::new();
    out.push_str("// @generated by scripts/generate_networks.rs. Do not edit by hand.\n");
    out.push_str(&format!(
        "// Source: The Graph networks registry ({snapshot}), plus PINAX_NETWORKS\n"
    ));
    out.push_str(
        "// (scripts/generate_networks.rs), Pinax networks the registry does not list yet.\n",
    );
    out.push_str("// Refresh steps: docs/network-registry-integration.md\n\n");
    out.push_str("use crate::networks::BuiltinNetwork;\n\n");
    out.push_str("pub const GENERATED_NETWORKS: &[BuiltinNetwork] = &[\n");
    for network in networks {
        if let Some(reason) = &network.internal_reason {
            out.push_str(&format!("    // PINAX_NETWORKS: {reason}\n"));
        }
        out.push_str("    BuiltinNetwork {\n");
        out.push_str(&format!("        chain_name: {:?},\n", network.chain_name));
        out.push_str(&format!(
            "        default_endpoint: {:?},\n",
            network.default_endpoint
        ));
        out.push_str("    },\n");
    }
    out.push_str("];\n\n");
    out.push_str("pub const GENERATED_NETWORK_NAMES: &[&str] = &[\n");
    for network in networks {
        out.push_str(&format!("    {:?},\n", network.chain_name));
    }
    out.push_str("];\n");
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn network(id: &str, firehose: &[&str]) -> RegistryNetwork {
        RegistryNetwork {
            id: Some(id.to_string()),
            services: RegistryServices {
                firehose: firehose.iter().map(|value| value.to_string()).collect(),
            },
            ..RegistryNetwork::default()
        }
    }

    fn endpoints(built: &[BuiltNetwork]) -> Vec<(&str, &str)> {
        built
            .iter()
            .map(|b| (b.chain_name.as_str(), b.default_endpoint.as_str()))
            .collect()
    }

    /// The registry's aliases alone, without internal networks or warnings.
    fn registry_only(networks: &[RegistryNetwork]) -> Vec<BuiltNetwork> {
        let (built, warnings) = build_networks(networks, &[]).unwrap();
        assert!(warnings.is_empty(), "{warnings:?}");
        built
    }

    const HYPERCORE: (&str, &str, &str) = (
        "hypercore",
        "https://hypercore.firehose.pinax.network:443",
        "HyperLiquid L1 (HyperCore); not in The Graph networks registry",
    );

    #[test]
    fn test_build_networks_prefers_default_provider() {
        let built = registry_only(&[network(
            "mainnet",
            &[
                "mainnet.eth.streamingfast.io:443",
                "eth.firehose.pinax.network:443",
            ],
        )]);
        assert_eq!(
            endpoints(&built),
            vec![("mainnet", "https://eth.firehose.pinax.network:443")]
        );
    }

    #[test]
    fn test_build_networks_uses_fallback_provider_only_for_listed_networks() {
        let built = registry_only(&[
            network("tron", &["mainnet.tron.streamingfast.io:443"]),
            network("monad", &["mainnet.monad.streamingfast.io:443"]),
        ]);
        assert_eq!(
            endpoints(&built),
            vec![("tron", "https://mainnet.tron.streamingfast.io:443")]
        );
    }

    #[test]
    fn test_build_networks_skips_excluded_networks_and_sorts() {
        let built = registry_only(&[
            network("zora", &["zora.firehose.pinax.network:443"]),
            network(
                "robinhood-sepolia",
                &["robsepolia.firehose.pinax.network:443"],
            ),
            network("arc", &["arc.firehose.pinax.network:443"]),
            network("scroll", &[]),
        ]);
        assert_eq!(
            endpoints(&built),
            vec![
                ("arc", "https://arc.firehose.pinax.network:443"),
                ("zora", "https://zora.firehose.pinax.network:443"),
            ]
        );
    }

    #[test]
    fn test_build_networks_appends_internal_networks_the_registry_lacks() {
        let (built, warnings) = build_networks(
            &[
                network("zora", &["zora.firehose.pinax.network:443"]),
                network("arc", &["arc.firehose.pinax.network:443"]),
                // The registry's HyperEVM is another network: no conflict.
                network("hyper-evm", &["hyperevm.firehose.pinax.network:443"]),
            ],
            &[HYPERCORE],
        )
        .unwrap();
        assert!(warnings.is_empty(), "{warnings:?}");
        assert_eq!(
            endpoints(&built),
            vec![
                ("arc", "https://arc.firehose.pinax.network:443"),
                ("hyper-evm", "https://hyperevm.firehose.pinax.network:443"),
                ("hypercore", "https://hypercore.firehose.pinax.network:443"),
                ("zora", "https://zora.firehose.pinax.network:443"),
            ]
        );
        let reasons: Vec<_> = built
            .iter()
            .map(|b| (b.chain_name.as_str(), b.internal_reason.as_deref()))
            .collect();
        assert_eq!(
            reasons,
            vec![
                ("arc", None),
                ("hyper-evm", None),
                ("hypercore", Some(HYPERCORE.2)),
                ("zora", None),
            ]
        );
    }

    #[test]
    fn test_build_networks_prefers_the_registry_over_an_internal_entry_and_warns() {
        let (built, warnings) = build_networks(
            &[network(
                "HyperCore",
                &["hyperliquid.firehose.pinax.network:443"],
            )],
            &[HYPERCORE],
        )
        .unwrap();
        assert_eq!(
            endpoints(&built),
            vec![(
                "hypercore",
                "https://hyperliquid.firehose.pinax.network:443"
            )]
        );
        assert_eq!(built[0].internal_reason, None);
        assert_eq!(
            warnings,
            vec![
                "internal network `hypercore` is now in the registry (https://hyperliquid.firehose.pinax.network:443); the registry entry wins, drop `hypercore` from PINAX_NETWORKS"
            ]
        );
    }

    #[test]
    fn test_pinax_networks_are_secure_pinax_endpoints_with_a_reason() {
        let mut names = Vec::new();
        for (alias, endpoint, reason) in PINAX_NETWORKS {
            assert_eq!(normalize_alias(alias), *alias, "alias must be normalized");
            let host = endpoint
                .strip_prefix("https://")
                .and_then(|rest| rest.strip_suffix(":443"))
                .unwrap_or_else(|| panic!("{alias}: {endpoint} must be https://<host>:443"));
            assert!(host.ends_with(".pinax.network"), "{alias}: {endpoint}");
            assert!(!reason.trim().is_empty(), "{alias}: a reason is required");
            assert!(
                !EXCLUDED_NETWORKS.iter().any(|(name, _)| name == alias)
                    && !FALLBACK_PROVIDERS.iter().any(|(name, _)| name == alias),
                "{alias} is also a registry policy entry"
            );
            names.push(*alias);
        }
        let count = names.len();
        names.sort_unstable();
        names.dedup();
        assert_eq!(names.len(), count, "duplicate PINAX_NETWORKS alias");
    }

    #[test]
    fn test_render_names_both_sources_and_marks_internal_entries() {
        let (built, _) = build_networks(
            &[network("arc", &["arc.firehose.pinax.network:443"])],
            &[HYPERCORE],
        )
        .unwrap();
        let rendered = render(&built, "v0.8.4, updatedAt 2026-09-24T21:50:40.258Z");
        assert!(rendered.contains(
            "// Source: The Graph networks registry (v0.8.4, updatedAt 2026-09-24T21:50:40.258Z), plus PINAX_NETWORKS\n// (scripts/generate_networks.rs), Pinax networks the registry does not list yet.\n"
        ));
        assert!(rendered.contains(
            "    // PINAX_NETWORKS: HyperLiquid L1 (HyperCore); not in The Graph networks registry\n    BuiltinNetwork {\n        chain_name: \"hypercore\",\n"
        ));
        assert_eq!(rendered.matches("PINAX_NETWORKS:").count(), 1);
        assert!(rendered.contains(
            "pub const GENERATED_NETWORK_NAMES: &[&str] = &[\n    \"arc\",\n    \"hypercore\",\n];\n"
        ));
    }
}

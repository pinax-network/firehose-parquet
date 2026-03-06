use std::collections::BTreeSet;
use std::fs;
use std::path::PathBuf;

use anyhow::{anyhow, Context, Result};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Registry {
    #[serde(default)]
    networks: Vec<RegistryNetwork>,
}

#[derive(Debug, Deserialize)]
struct RegistryNetwork {
    #[serde(default)]
    caip2: Option<String>,
    #[serde(default)]
    short_name: Option<String>,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    aliases: Vec<String>,
    #[serde(default)]
    services: Vec<RegistryService>,
    #[serde(default)]
    firehose_endpoints: Vec<RegistryEndpoint>,
}

#[derive(Debug, Deserialize)]
struct RegistryService {
    #[serde(default)]
    service_type: Option<String>,
    #[serde(default)]
    provider: Option<String>,
    #[serde(default)]
    url: Option<String>,
}

#[derive(Debug, Deserialize)]
struct RegistryEndpoint {
    #[serde(default)]
    provider: Option<String>,
    #[serde(default)]
    url: Option<String>,
}

#[derive(Debug)]
struct BuiltNetwork {
    canonical: String,
    aliases: Vec<String>,
    default_endpoint: String,
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

    let mut built = Vec::new();
    for network in registry.networks {
        if let Some(endpoint) = pinax_firehose_endpoint(&network) {
            let canonical = canonical_name(&network)
                .ok_or_else(|| anyhow!("missing canonical network name for endpoint {endpoint}"))?;
            let aliases = aliases_for(&network, &canonical);
            built.push(BuiltNetwork {
                canonical,
                aliases,
                default_endpoint: endpoint,
            });
        }
    }

    built.sort_by(|a, b| a.canonical.cmp(&b.canonical));
    let rendered = render(&built);
    fs::write(&output, rendered)
        .with_context(|| format!("failed to write generated file {}", output.display()))?;

    eprintln!("wrote {} networks to {}", built.len(), output.display());
    Ok(())
}

fn pinax_firehose_endpoint(network: &RegistryNetwork) -> Option<String> {
    for endpoint in &network.firehose_endpoints {
        if is_pinax(endpoint.provider.as_deref(), endpoint.url.as_deref()) {
            if let Some(url) = endpoint.url.as_ref() {
                return Some(url.trim().to_string());
            }
        }
    }

    for service in &network.services {
        let kind = service.service_type.as_deref().unwrap_or_default().to_ascii_lowercase();
        if kind.contains("firehose") && is_pinax(service.provider.as_deref(), service.url.as_deref()) {
            if let Some(url) = service.url.as_ref() {
                return Some(url.trim().to_string());
            }
        }
    }

    None
}

fn is_pinax(provider: Option<&str>, url: Option<&str>) -> bool {
    provider
        .map(|value| value.to_ascii_lowercase().contains("pinax"))
        .unwrap_or(false)
        || url
            .map(|value| value.to_ascii_lowercase().contains("pinax.network"))
            .unwrap_or(false)
}

fn canonical_name(network: &RegistryNetwork) -> Option<String> {
    network
        .short_name
        .as_deref()
        .map(normalize_alias)
        .filter(|value| !value.is_empty())
        .or_else(|| network.caip2.as_deref().map(normalize_caip2))
        .or_else(|| network.name.as_deref().map(normalize_alias))
        .filter(|value| !value.is_empty())
}

fn aliases_for(network: &RegistryNetwork, canonical: &str) -> Vec<String> {
    let mut aliases = BTreeSet::new();
    aliases.insert(canonical.to_string());

    if let Some(short_name) = network.short_name.as_deref() {
        let alias = normalize_alias(short_name);
        if !alias.is_empty() {
            aliases.insert(alias);
        }
    }

    if let Some(caip2) = network.caip2.as_deref() {
        let alias = normalize_caip2(caip2);
        if !alias.is_empty() {
            aliases.insert(alias);
        }
    }

    if let Some(name) = network.name.as_deref() {
        let alias = normalize_alias(name);
        if !alias.is_empty() {
            aliases.insert(alias);
        }
    }

    for alias in &network.aliases {
        let alias = normalize_alias(alias);
        if !alias.is_empty() {
            aliases.insert(alias);
        }
    }

    let endpoint_aliases = endpoint_derived_aliases(&network.services, &network.firehose_endpoints);
    for alias in endpoint_aliases {
        aliases.insert(alias);
    }

    if canonical == "mainnet" {
        aliases.insert("eth".to_string());
    }

    aliases.into_iter().collect()
}

fn endpoint_derived_aliases(
    services: &[RegistryService],
    endpoints: &[RegistryEndpoint],
) -> Vec<String> {
    let mut aliases = BTreeSet::new();
    for value in services.iter().filter_map(|service| service.url.as_deref()) {
        if let Some(alias) = alias_from_pinax_url(value) {
            aliases.insert(alias);
        }
    }
    for value in endpoints.iter().filter_map(|endpoint| endpoint.url.as_deref()) {
        if let Some(alias) = alias_from_pinax_url(value) {
            aliases.insert(alias);
        }
    }
    aliases.into_iter().collect()
}

fn alias_from_pinax_url(url: &str) -> Option<String> {
    let trimmed = url.trim();
    let host = trimmed
        .strip_prefix("https://")
        .or_else(|| trimmed.strip_prefix("http://"))?
        .split('/')
        .next()?
        .split(':')
        .next()?;
    let prefix = host.strip_suffix(".firehose.pinax.network")?;
    let alias = normalize_alias(prefix);
    if alias.is_empty() { None } else { Some(alias) }
}

fn normalize_caip2(value: &str) -> String {
    let suffix = value.split(':').nth(1).unwrap_or(value);
    normalize_alias(suffix)
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

fn render(networks: &[BuiltNetwork]) -> String {
    let mut aliases = Vec::new();
    for network in networks {
        aliases.extend(network.aliases.iter().cloned());
    }
    aliases.sort();
    aliases.dedup();

    let mut out = String::new();
    out.push_str("use super::BuiltinNetwork;\n\n");
    out.push_str("pub const GENERATED_NETWORKS: &[BuiltinNetwork] = &[\n");
    for network in networks {
        out.push_str("    BuiltinNetwork {\n");
        out.push_str(&format!("        canonical: {:?},\n", network.canonical));
        out.push_str("        aliases: &[");
        for (idx, alias) in network.aliases.iter().enumerate() {
            if idx > 0 {
                out.push_str(", ");
            }
            out.push_str(&format!("{:?}", alias));
        }
        out.push_str("],\n");
        out.push_str(&format!(
            "        default_endpoint: {:?},\n",
            network.default_endpoint
        ));
        out.push_str("    },\n");
    }
    out.push_str("];\n\n");
    out.push_str("pub const GENERATED_NETWORK_ALIASES: &[&str] = &[\n");
    for alias in aliases {
        out.push_str(&format!("    {:?},\n", alias));
    }
    out.push_str("];\n");
    out
}

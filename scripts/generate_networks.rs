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
            let chain_name = canonical_name(&network)
                .ok_or_else(|| anyhow!("missing canonical network name for endpoint {endpoint}"))?;
            built.push(BuiltNetwork {
                chain_name,
                default_endpoint: endpoint,
            });
        }
    }

    built.sort_by(|a, b| a.chain_name.cmp(&b.chain_name));
    let rendered = render(&built);
    fs::write(&output, rendered)
        .with_context(|| format!("failed to write generated file {}", output.display()))?;

    eprintln!("wrote {} networks to {}", built.len(), output.display());
    Ok(())
}

fn pinax_firehose_endpoint(network: &RegistryNetwork) -> Option<String> {
    for endpoint in &network.services.firehose {
        if endpoint.to_ascii_lowercase().contains("pinax.network") {
            return Some(with_https(endpoint));
        }
    }

    None
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

fn render(networks: &[BuiltNetwork]) -> String {
    let mut out = String::new();
    out.push_str("use crate::networks::BuiltinNetwork;\n\n");
    out.push_str("pub const GENERATED_NETWORKS: &[BuiltinNetwork] = &[\n");
    for network in networks {
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

//! Select credentials using the actual destination, after network overrides.
use anyhow::{Context, Result};
use tonic::transport::Endpoint;
use tracing::info;

/// Resolved secrets for one endpoint. Deliberately does not implement Debug.
pub struct EndpointCredentials {
    pub api_key: Option<String>,
    pub jwt_token: Option<String>,
}

/// Explicit env-var selectors authorize that credential for this endpoint.
/// Otherwise, only known HTTPS provider hosts receive ambient credentials.
pub fn resolve_credentials(
    endpoint: &str,
    api_key_envvar: Option<&str>,
    api_token_envvar: Option<&str>,
) -> Result<EndpointCredentials> {
    resolve_with(endpoint, api_key_envvar, api_token_envvar, |name| {
        std::env::var(name).ok()
    })
}

pub(crate) fn resolve_with(
    endpoint: &str,
    api_key_envvar: Option<&str>,
    api_token_envvar: Option<&str>,
    read: impl Fn(&str) -> Option<String>,
) -> Result<EndpointCredentials> {
    // Never include the raw URL in errors/logs: it might contain user info.
    let endpoint = Endpoint::from_shared(endpoint.to_owned())
        .context("invalid Firehose endpoint URI for credential selection")?;
    let uri = endpoint.uri();
    let host = uri.host().unwrap_or_default().to_ascii_lowercase();
    let known_host = crate::networks_generated::GENERATED_NETWORKS
        .iter()
        .any(|network| {
            Endpoint::from_static(network.default_endpoint).uri().host() == Some(host.as_str())
        });
    let secure = uri.scheme_str() == Some("https")
        && (uri.port_u16() == Some(443)
            || uri
                .authority()
                .is_some_and(|a| a.as_str().eq_ignore_ascii_case(&host)))
        && !uri.authority().is_some_and(|a| a.as_str().contains('@'));
    let provider = if secure && known_host && host.ends_with(".pinax.network") {
        "pinax"
    } else if secure && known_host && host.ends_with(".streamingfast.io") {
        "streamingfast"
    } else {
        "custom"
    };

    let (key_defaults, token_defaults): (&[&str], &[&str]) = match provider {
        "pinax" => (
            &["PINAX_API_KEY", "SUBSTREAMS_API_KEY"],
            &["PINAX_API_TOKEN", "SUBSTREAMS_API_TOKEN"],
        ),
        "streamingfast" => (&["STREAMINGFAST_API_KEY"], &["STREAMINGFAST_API_TOKEN"]),
        _ => (&[], &[]),
    };
    let select = |explicit: Option<&str>, defaults: &[&str]| {
        let names = explicit.map_or_else(|| defaults.to_vec(), |name| vec![name]);
        names.into_iter().find_map(|name| {
            read(name)
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty())
                .map(|value| (name.to_string(), value))
        })
    };
    let key = select(api_key_envvar, key_defaults);
    let token = select(api_token_envvar, token_defaults);
    info!(
        host = %host,
        provider,
        api_key_envvar = key.as_ref().map(|(name, _)| name.as_str()).unwrap_or("none"),
        api_token_envvar = token.as_ref().map(|(name, _)| name.as_str()).unwrap_or("none"),
        "selected Firehose authentication"
    );
    Ok(EndpointCredentials {
        api_key: key.map(|(_, value)| value),
        jwt_token: token.map(|(_, value)| value),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const PINAX: &str = "https://eth.firehose.pinax.network:443";
    const STREAMINGFAST: &str = "https://mainnet.tron.streamingfast.io:443";

    fn read(name: &str) -> Option<String> {
        match name {
            "PINAX_API_KEY" => Some(" pinax-key\n".into()),
            "PINAX_API_TOKEN" => Some("pinax-token".into()),
            "SUBSTREAMS_API_KEY" => Some("legacy-key".into()),
            "SUBSTREAMS_API_TOKEN" => Some("legacy-token".into()),
            "STREAMINGFAST_API_TOKEN" => Some("streamingfast-token".into()),
            _ => None,
        }
    }

    #[test]
    fn scopes_ambient_credentials_to_each_builtin_destination() {
        for network in crate::networks_generated::GENERATED_NETWORKS {
            let credentials = resolve_with(network.default_endpoint, None, None, read).unwrap();
            if network.default_endpoint.contains(".pinax.network") {
                assert_eq!(credentials.api_key.as_deref(), Some("pinax-key"));
                assert_eq!(credentials.jwt_token.as_deref(), Some("pinax-token"));
            } else {
                assert_eq!(credentials.api_key, None);
                assert_eq!(
                    credentials.jwt_token.as_deref(),
                    Some("streamingfast-token")
                );
            }
        }
    }

    #[test]
    fn legacy_credentials_are_only_a_pinax_fallback() {
        let legacy = |name: &str| {
            if name.starts_with("SUBSTREAMS_") {
                read(name)
            } else {
                Some(" \n".into())
            }
        };
        let pinax = resolve_with(PINAX, None, None, legacy).unwrap();
        assert_eq!(pinax.api_key.as_deref(), Some("legacy-key"));
        assert_eq!(pinax.jwt_token.as_deref(), Some("legacy-token"));
        let other = resolve_with(STREAMINGFAST, None, None, legacy).unwrap();
        assert_eq!(other.api_key, None);
        assert_eq!(other.jwt_token, None);
    }

    #[test]
    fn unknown_or_insecure_destinations_receive_no_ambient_credentials() {
        for endpoint in [
            "https://example.com",
            "http://eth.firehose.pinax.network",
            "https://eth.firehose.pinax.network:8443",
            "https://eth.firehose.pinax.network:abc",
            "https://eth.firehose.pinax.network:65536",
            "https://eth.firehose.pinax.network.evil.example",
            "https://unknown.firehose.pinax.network",
            "https://mainnet.tron.streamingfast.io.evil.example",
            "https://eth.firehose.pinax.network@evil.example",
            "https://evil.example@eth.firehose.pinax.network",
            "http://localhost:9000",
        ] {
            let credentials = resolve_with(endpoint, None, None, read).unwrap();
            assert!(credentials.api_key.is_none(), "{endpoint}");
            assert!(credentials.jwt_token.is_none(), "{endpoint}");
        }
    }

    #[test]
    fn explicit_names_authorize_one_header_without_falling_back_when_unset() {
        let credentials = resolve_with(
            "https://custom.example",
            Some("SUBSTREAMS_API_KEY"),
            None,
            read,
        )
        .unwrap();
        assert_eq!(credentials.api_key.as_deref(), Some("legacy-key"));
        assert_eq!(credentials.jwt_token, None);
        let credentials = resolve_with(PINAX, Some("UNSET"), Some("UNSET"), read).unwrap();
        assert_eq!(credentials.api_key, None);
        assert_eq!(credentials.jwt_token, None);
    }

    #[test]
    fn startup_log_names_credentials_without_values() {
        // Tracing caches callsite interest process-wide. Isolate this capture
        // from concurrent resolver tests that hit the same callsite without a
        // subscriber, while still asserting the actual production log output.
        const CHILD: &str = "FIREPARQ_AUTH_LOG_TEST_CHILD";
        if std::env::var_os(CHILD).is_none() {
            let child = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "auth::tests::startup_log_names_credentials_without_values",
                    "--nocapture",
                ])
                .env(CHILD, "1")
                .output()
                .unwrap();
            assert!(
                child.status.success()
                    && String::from_utf8_lossy(&child.stdout).contains("1 passed; 0 failed"),
                "isolated log test failed: {}{}",
                String::from_utf8_lossy(&child.stdout),
                String::from_utf8_lossy(&child.stderr)
            );
            return;
        }
        let output = tempfile::NamedTempFile::new().unwrap();
        let subscriber = tracing_subscriber::fmt()
            .without_time()
            .with_ansi(false)
            .with_writer(std::sync::Mutex::new(output.reopen().unwrap()))
            .finish();
        tracing::subscriber::with_default(subscriber, || {
            resolve_with(STREAMINGFAST, None, None, read).unwrap();
        });
        let log = std::fs::read_to_string(output.path()).unwrap();
        assert!(log.contains("mainnet.tron.streamingfast.io"));
        assert!(log.contains("STREAMINGFAST_API_TOKEN"));
        assert!(log.contains("api_key_envvar=\"none\""));
        for secret in [
            "pinax-key",
            "pinax-token",
            "legacy-key",
            "legacy-token",
            "streamingfast-token",
        ] {
            assert!(!log.contains(secret));
        }
    }

    #[test]
    fn default_https_port_and_host_case_resolve_consistently() {
        let credentials =
            resolve_with("https://ETH.FIREHOSE.PINAX.NETWORK", None, None, read).unwrap();
        assert_eq!(credentials.api_key.as_deref(), Some("pinax-key"));
    }
}

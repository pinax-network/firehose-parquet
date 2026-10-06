//! Select credentials using the actual destination, after network overrides.
use anyhow::{Context, Result};
use tonic::transport::Endpoint;
use tracing::{info, warn};

/// Ambient API key variables for built-in Pinax hosts, in priority order.
pub const PINAX_API_KEY_ENV_VARS: &[&str] = &["PINAX_API_KEY", "SUBSTREAMS_API_KEY"];
/// Ambient bearer token variables for built-in Pinax hosts, in priority order.
pub const PINAX_API_TOKEN_ENV_VARS: &[&str] = &["PINAX_API_TOKEN", "SUBSTREAMS_API_TOKEN"];
/// Ambient API key variables for built-in StreamingFast hosts.
pub const STREAMINGFAST_API_KEY_ENV_VARS: &[&str] = &["STREAMINGFAST_API_KEY"];
/// Ambient bearer token variables for built-in StreamingFast hosts.
pub const STREAMINGFAST_API_TOKEN_ENV_VARS: &[&str] = &["STREAMINGFAST_API_TOKEN"];

/// Why an explicitly selected credential deserves a startup warning.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExplicitScopeWarning {
    /// A Pinax (or legacy `SUBSTREAMS_*`) credential leaves Pinax, for example
    /// a global `API_KEY_ENVVAR=SUBSTREAMS_API_KEY` reaching StreamingFast.
    PinaxCredentialToOtherHost,
    /// Any other explicitly selected credential reaches a non-Pinax host.
    CredentialToNonPinaxHost,
}

/// Explicit selectors bypass provider scoping (#562), so an explicitly selected
/// credential sent to a non-Pinax host is logged. A provider-scoped
/// StreamingFast name sent to a StreamingFast host is its normal destination.
///
/// `pinax_host` is a secure (`https`, port 443, no userinfo) `*.pinax.network`
/// destination, built-in or not. A Pinax host outside the network registry
/// (for example `hypercore.firehose.pinax.network`) gets no ambient credential,
/// so it needs the selector, but the credential does not leave Pinax.
fn explicit_scope_warning(
    provider: &str,
    pinax_host: bool,
    name: &str,
) -> Option<ExplicitScopeWarning> {
    let streamingfast_name = STREAMINGFAST_API_KEY_ENV_VARS
        .iter()
        .chain(STREAMINGFAST_API_TOKEN_ENV_VARS)
        .any(|candidate| *candidate == name);
    if provider == "pinax" || pinax_host || (provider == "streamingfast" && streamingfast_name) {
        return None;
    }
    let pinax_name = PINAX_API_KEY_ENV_VARS
        .iter()
        .chain(PINAX_API_TOKEN_ENV_VARS)
        .any(|candidate| *candidate == name);
    Some(if pinax_name {
        ExplicitScopeWarning::PinaxCredentialToOtherHost
    } else {
        ExplicitScopeWarning::CredentialToNonPinaxHost
    })
}

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
    // Ambient credentials stay registry-scoped: only a built-in Pinax host is
    // provider "pinax". `pinax_host` only quiets the explicit-selector warning.
    let pinax_host = secure && host.ends_with(".pinax.network");
    let provider = if pinax_host && known_host {
        "pinax"
    } else if secure && known_host && host.ends_with(".streamingfast.io") {
        "streamingfast"
    } else {
        "custom"
    };

    let (key_defaults, token_defaults): (&[&str], &[&str]) = match provider {
        "pinax" => (PINAX_API_KEY_ENV_VARS, PINAX_API_TOKEN_ENV_VARS),
        "streamingfast" => (
            STREAMINGFAST_API_KEY_ENV_VARS,
            STREAMINGFAST_API_TOKEN_ENV_VARS,
        ),
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
    for (selector, selected) in [
        (
            "--api-key-envvar / API_KEY_ENVVAR",
            api_key_envvar.and(key.as_ref()),
        ),
        (
            "--api-token-envvar / API_TOKEN_ENVVAR",
            api_token_envvar.and(token.as_ref()),
        ),
    ] {
        let Some((name, _)) = selected else {
            continue;
        };
        match explicit_scope_warning(provider, pinax_host, name) {
            Some(ExplicitScopeWarning::PinaxCredentialToOtherHost) => warn!(
                host = %host,
                provider,
                envvar = name.as_str(),
                selector,
                "explicitly selected Pinax credential is sent to a non-Pinax host; \
                 unset the selector to use provider-scoped credentials, or move the \
                 secret to the destination provider's variable"
            ),
            Some(ExplicitScopeWarning::CredentialToNonPinaxHost) => warn!(
                host = %host,
                provider,
                envvar = name.as_str(),
                selector,
                "explicitly selected credential is sent to a non-Pinax host; \
                 confirm this destination should receive it"
            ),
            None => {}
        }
    }
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
    /// A Pinax host that is not in the network registry.
    const UNLISTED_PINAX: &str = "https://hypercore.firehose.pinax.network:443";

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
            UNLISTED_PINAX,
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
    fn explicit_credentials_leaving_pinax_are_classified_for_warning() {
        use ExplicitScopeWarning::*;
        for name in [
            "SUBSTREAMS_API_KEY",
            "SUBSTREAMS_API_TOKEN",
            "PINAX_API_KEY",
            "PINAX_API_TOKEN",
        ] {
            assert_eq!(
                explicit_scope_warning("streamingfast", false, name),
                Some(PinaxCredentialToOtherHost),
                "{name}"
            );
            assert_eq!(
                explicit_scope_warning("custom", false, name),
                Some(PinaxCredentialToOtherHost),
                "{name}"
            );
            assert_eq!(explicit_scope_warning("pinax", false, name), None, "{name}");
            // A secure Pinax host outside the registry is still Pinax.
            assert_eq!(explicit_scope_warning("custom", true, name), None, "{name}");
        }
        assert_eq!(
            explicit_scope_warning("custom", false, "INTERNAL_FIREHOSE_API_KEY"),
            Some(CredentialToNonPinaxHost)
        );
        assert_eq!(
            explicit_scope_warning("streamingfast", false, "INTERNAL_FIREHOSE_API_KEY"),
            Some(CredentialToNonPinaxHost)
        );
        assert_eq!(
            explicit_scope_warning("pinax", false, "INTERNAL_FIREHOSE_API_KEY"),
            None
        );
        assert_eq!(
            explicit_scope_warning("custom", true, "INTERNAL_FIREHOSE_API_KEY"),
            None
        );
        assert_eq!(
            explicit_scope_warning("streamingfast", false, "STREAMINGFAST_API_TOKEN"),
            None
        );
        assert_eq!(
            explicit_scope_warning("custom", false, "STREAMINGFAST_API_TOKEN"),
            Some(CredentialToNonPinaxHost)
        );
    }

    #[test]
    fn explicit_legacy_selector_warns_when_sent_to_streamingfast() {
        // Same process isolation as the startup log test: tracing caches
        // callsite interest process-wide.
        const CHILD: &str = "FIREPARQ_AUTH_WARN_TEST_CHILD";
        if std::env::var_os(CHILD).is_none() {
            let child = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "auth::tests::explicit_legacy_selector_warns_when_sent_to_streamingfast",
                    "--nocapture",
                ])
                .env(CHILD, "1")
                .output()
                .unwrap();
            assert!(
                child.status.success()
                    && String::from_utf8_lossy(&child.stdout).contains("1 passed; 0 failed"),
                "isolated warning test failed: {}{}",
                String::from_utf8_lossy(&child.stdout),
                String::from_utf8_lossy(&child.stderr)
            );
            return;
        }
        let capture = |endpoint: &str, key: Option<&str>, token: Option<&str>| {
            let output = tempfile::NamedTempFile::new().unwrap();
            let subscriber = tracing_subscriber::fmt()
                .without_time()
                .with_ansi(false)
                .with_writer(std::sync::Mutex::new(output.reopen().unwrap()))
                .finish();
            let credentials = tracing::subscriber::with_default(subscriber, || {
                resolve_with(endpoint, key, token, read).unwrap()
            });
            (credentials, std::fs::read_to_string(output.path()).unwrap())
        };

        // The migration hazard: a global legacy selector still authorizes the
        // Pinax key for StreamingFast, but now says so loudly.
        let (credentials, log) = capture(STREAMINGFAST, Some("SUBSTREAMS_API_KEY"), None);
        assert_eq!(credentials.api_key.as_deref(), Some("legacy-key"));
        assert!(log.contains("WARN"), "{log}");
        assert!(
            log.contains("explicitly selected Pinax credential is sent to a non-Pinax host"),
            "{log}"
        );
        assert!(log.contains("envvar=\"SUBSTREAMS_API_KEY\""), "{log}");
        assert!(log.contains("mainnet.tron.streamingfast.io"), "{log}");
        assert!(!log.contains("legacy-key"), "{log}");

        let (_, log) = capture(
            "https://custom.example",
            None,
            Some("INTERNAL_FIREHOSE_API_TOKEN"),
        );
        // Unset explicit variables transmit nothing, so there is nothing to warn about.
        assert!(!log.contains("WARN"), "{log}");
        let (_, log) = capture("https://custom.example", None, Some("PINAX_API_TOKEN"));
        assert!(
            log.contains("explicitly selected Pinax credential is sent to a non-Pinax host"),
            "{log}"
        );
        assert!(!log.contains("pinax-token"), "{log}");

        // Pinax destinations and ambient provider-scoped selection stay quiet.
        for (endpoint, key) in [
            (PINAX, Some("SUBSTREAMS_API_KEY")),
            (PINAX, None),
            (STREAMINGFAST, None),
        ] {
            let (_, log) = capture(endpoint, key, None);
            assert!(!log.contains("WARN"), "{endpoint}: {log}");
        }

        // A secure Pinax host outside the registry needs the selector (no
        // ambient credential) and is not warned about: the key stays at Pinax.
        let (credentials, log) = capture(UNLISTED_PINAX, Some("PINAX_API_KEY"), None);
        assert_eq!(credentials.api_key.as_deref(), Some("pinax-key"));
        assert!(!log.contains("WARN"), "{log}");
        assert!(log.contains("provider=\"custom\""), "{log}");
        // The same host over plaintext or another port is not secure: still warned.
        for endpoint in [
            "http://hypercore.firehose.pinax.network",
            "https://hypercore.firehose.pinax.network:8443",
        ] {
            let (_, log) = capture(endpoint, Some("PINAX_API_KEY"), None);
            assert!(
                log.contains("explicitly selected Pinax credential is sent to a non-Pinax host"),
                "{endpoint}: {log}"
            );
        }
    }

    #[test]
    fn default_https_port_and_host_case_resolve_consistently() {
        let credentials =
            resolve_with("https://ETH.FIREHOSE.PINAX.NETWORK", None, None, read).unwrap();
        assert_eq!(credentials.api_key.as_deref(), Some("pinax-key"));
    }
}

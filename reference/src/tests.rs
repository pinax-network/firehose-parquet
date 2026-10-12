//! The Python job's `test_cli.py` at the library: [`run_with`] over the fake
//! `/info` and clock, the environment a closure. The binary's own contract
//! (clap, the process environment, stderr) is tested in `blocks/tests`.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use serde_json::{json, Value};

use super::*;
use crate::jobs::tests::no_aws;
use crate::json::Json;
use crate::testing::{FakeClock, FakeInfo, MemoryLog};

/// A shared buffer the run writes its JSON lines to.
#[derive(Clone, Default)]
struct Output(Arc<Mutex<Vec<u8>>>);

impl Write for Output {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// What a run printed and returned.
struct Ran {
    status: i32,
    text: String,
    events: Vec<Value>,
}

impl Ran {
    fn first(&self) -> &Value {
        &self.events[0]
    }

    fn last(&self) -> &Value {
        self.events.last().unwrap()
    }
}

struct Cli {
    _dir: tempfile::TempDir,
    root: PathBuf,
    clock: Arc<FakeClock>,
    fake: Arc<FakeInfo>,
}

impl Cli {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().canonicalize().unwrap();
        Self {
            _dir: dir,
            root,
            clock: Arc::new(FakeClock::new()),
            fake: Arc::new(FakeInfo::new()),
        }
    }

    /// `job` on this root, with the defaults.
    fn settings(&self, job: Job) -> Settings {
        Settings {
            root: Some(self.root.display().to_string()),
            ..Settings::new(job)
        }
    }

    async fn invoke(&self, settings: Settings, env: &[(&str, &str)]) -> Ran {
        let env: HashMap<String, String> = env
            .iter()
            .map(|(name, value)| ((*name).to_owned(), (*value).to_owned()))
            .collect();
        let lookup = move |name: &str| env.get(name).cloned();
        let output = Output::default();
        let runtime = Runtime {
            clock: self.clock.clone(),
            transport: Some(self.fake.clone()),
            jitter: Box::new(|| 0.0),
            funding_page: info::FUNDING_PAGE,
        };
        let status = run_with(settings, &lookup, runtime, Box::new(output.clone())).await;
        let text = String::from_utf8(output.0.lock().unwrap().clone()).unwrap();
        let events = text
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        Ran {
            status,
            text,
            events,
        }
    }
}

fn outcomes(sweep_from: i64) -> Job {
    Job::Outcomes(OutcomesOptions {
        sweep_from,
        ..OutcomesOptions::default()
    })
}

fn s3(key: Option<&str>, secret: Option<&str>) -> AwsConfig {
    AwsConfig {
        aws_access_key_id: key.map(str::to_owned),
        aws_secret_access_key: secret.map(str::to_owned),
        ..no_aws()
    }
}

#[tokio::test]
async fn runs_and_reports() {
    let cli = Cli::new();
    let ran = cli.invoke(cli.settings(outcomes(9975)), &[]).await;
    assert_eq!(ran.status, 0, "{}", ran.text);
    let start = ran.first();
    assert_eq!(
        (&start["event"], &ran.last()["event"]),
        (&json!("start"), &json!("done"))
    );
    assert_eq!(start["info"], "https://api.hyperliquid.xyz");
    assert_eq!(start["mode"], "outcomes");
    assert_eq!(start["root"], json!(cli.root.display().to_string()));
    assert_eq!(start["weight_per_minute"], json!(400));
    assert_eq!(start["time_budget_seconds"], json!(240.0));
    assert_eq!(start["version"], env!("CARGO_PKG_VERSION"));
    assert_eq!(start["deltalake"], deltalake_core::crate_version());
    let run_id = start["run_id"].as_str().unwrap();
    let (prefix, random) = run_id.rsplit_once('-').unwrap();
    assert_eq!(prefix, "outcomes-20261008T120000Z");
    assert!(
        random.len() == 6
            && random
                .chars()
                .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
    );
    let done = ran.last();
    assert_eq!(done["failed"], json!([]));
    assert_eq!(done["run_id"], json!(run_id));
    assert_eq!(done["summary"]["probed"], json!(3));
    assert_eq!(done["requests"], json!(4));
    assert_eq!(done["weight"], json!(80));
    let ran = cli.invoke(cli.settings(Job::Verify), &[]).await;
    assert_eq!(ran.status, 0, "{}", ran.text);
}

#[tokio::test]
async fn failures_exit_1_and_never_log_the_url() {
    let cli = Cli::new();
    cli.fake.edit(|state| {
        state
            .statuses
            .insert("outcomeMeta".into(), vec![503; 10].into())
    });
    let settings = Settings {
        info_url: Some("https://gw.example/v1/SECRETKEY/info?key=SECRETQUERY".into()),
        ..cli.settings(outcomes(0))
    };
    let ran = cli.invoke(settings, &[]).await;
    assert_eq!(ran.status, 1);
    assert_eq!(ran.first()["info"], "https://gw.example");
    assert_ne!(ran.last()["failed"], json!([]));
    assert!(!ran.text.contains("SECRETKEY"), "{}", ran.text);
    assert!(!ran.text.contains("SECRETQUERY"), "{}", ran.text);
}

#[tokio::test]
async fn configuration_errors_exit_2() {
    let cli = Cli::new();
    let meta = || Job::Meta {
        allow_mass_removal: false,
    };
    let s3_root = |aws: AwsConfig| Settings {
        root: Some("s3://hypercore-ref".into()),
        aws,
        ..Settings::new(meta())
    };
    let refused = |ran: &Ran, message: &str| {
        assert_eq!(ran.status, 2, "{}", ran.text);
        assert_eq!(ran.events.len(), 1, "{}", ran.text);
        assert_eq!(
            ran.last(),
            &json!({"event": "config_error", "message": message})
        );
    };
    let keys = "an s3:// root needs AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY";
    // Empty keys are missing keys.
    refused(
        &cli.invoke(s3_root(s3(Some(""), Some(""))), &[]).await,
        keys,
    );
    refused(&cli.invoke(s3_root(s3(None, None)), &[]).await, keys);
    refused(&cli.invoke(s3_root(s3(Some("id"), None)), &[]).await, keys);
    let ran = cli
        .invoke(
            s3_root(s3(Some("id"), Some("secret"))),
            &[("AWS_S3_ALLOW_UNSAFE_RENAME", "true")],
        )
        .await;
    refused(
        &ran,
        "AWS_S3_ALLOW_UNSAFE_RENAME is refused: commits must be conditional puts",
    );
    assert!(!ran.text.contains("secret\""), "{}", ran.text);
    // No root.
    refused(
        &cli.invoke(Settings::new(meta()), &[]).await,
        "--root or HLREF_ROOT is required",
    );
    let empty_root = Settings {
        root: Some(String::new()),
        ..Settings::new(meta())
    };
    refused(
        &cli.invoke(empty_root, &[]).await,
        "--root or HLREF_ROOT is required",
    );
    for url in ["file:///etc/passwd", "https:///info"] {
        let settings = Settings {
            info_url: Some(url.into()),
            ..cli.settings(meta())
        };
        let ran = cli.invoke(settings, &[]).await;
        refused(&ran, "HL_INFO_URL must be an http(s) URL");
        assert!(!ran.text.contains("passwd"));
    }
    let settings = Settings {
        info_url: Some("http://example.com/info".into()),
        ..cli.settings(meta())
    };
    refused(
        &cli.invoke(settings, &[]).await,
        "HL_INFO_URL must use https (plain http only for a loopback host)",
    );
    let settings = Settings {
        weight_per_minute: 19,
        ..cli.settings(meta())
    };
    refused(
        &cli.invoke(settings, &[]).await,
        "weight budget must be at least 20 per minute",
    );
    for budget in [-1.0, f64::NAN, f64::INFINITY] {
        let settings = Settings {
            time_budget_seconds: budget,
            ..cli.settings(meta())
        };
        refused(
            &cli.invoke(settings, &[]).await,
            "time budget must be a finite number of seconds, 0 or more",
        );
    }
    let maintain = cli.settings(Job::Maintain {
        tables: Some(vec!["hl_outcomes".into(), "hl_nothing".into()]),
    });
    refused(
        &cli.invoke(maintain, &[]).await,
        "--tables names no table of this release: hl_nothing",
    );
    // All before any request, and before any table was created.
    assert!(cli.fake.calls().is_empty());
    assert_eq!(std::fs::read_dir(&cli.root).unwrap().count(), 0);
}

#[tokio::test]
async fn empty_environment_values_are_unset() {
    let cli = Cli::new();
    let settings = Settings {
        info_url: Some(String::new()),
        ..cli.settings(outcomes(9975))
    };
    let ran = cli.invoke(settings, &[]).await;
    assert_eq!(ran.status, 0, "{}", ran.text);
    assert_eq!(ran.first()["info"], "https://api.hyperliquid.xyz");
    let settings = Settings {
        root: Some(String::new()),
        aws: AwsConfig {
            aws_access_key_id: Some(String::new()),
            aws_secret_access_key: Some("secret".into()),
            aws_session_token: Some(String::new()),
            aws_region: Some(String::new()),
            aws_endpoint_url: Some(String::new()),
        },
        ..Settings::new(Job::Verify)
    }
    .normalized();
    assert_eq!(settings.root, None);
    assert_eq!(settings.aws.aws_access_key_id, None);
    assert_eq!(settings.aws.aws_session_token, None);
    assert_eq!(settings.aws.aws_region, None);
    assert_eq!(settings.aws.aws_endpoint_url, None);
    assert_eq!(settings.secrets(), ["secret"]);
    // The URL and its query are secrets, but a URL that is only what the
    // output shows of it is not hidden from the output.
    let with_url = |url: &str| Settings {
        info_url: Some(url.into()),
        ..Settings::new(Job::Verify)
    };
    assert_eq!(
        with_url("https://gw.example/info?key=k").secrets(),
        ["https://gw.example/info?key=k", "key=k"]
    );
    assert!(with_url("https://gw.example").secrets().is_empty());
}

#[test]
fn a_stale_endpoint_variable_is_refused_only_when_it_differs() {
    let endpoint = "https://datasets-rgw.example";
    let log = MemoryLog::new();
    let clock: Arc<dyn clock::Clock> = Arc::new(FakeClock::new());
    let open = |env: &[(&str, &str)]| {
        let env: HashMap<String, String> = env
            .iter()
            .map(|(name, value)| ((*name).to_owned(), (*value).to_owned()))
            .collect();
        let lookup = move |name: &str| env.get(name).cloned();
        let aws = AwsConfig {
            aws_endpoint_url: Some(endpoint.into()),
            ..s3(Some("id"), Some("secret"))
        };
        Store::new(
            Root::parse("s3://hypercore-ref").unwrap(),
            &aws,
            &lookup,
            "test".into(),
            "test",
            log.logger(),
            clock.clone(),
        )
        .map(|_| ())
        .map_err(|error| error.0)
    };
    let stale = "S3_ENDPOINT and AWS_ENDPOINT_URL are not read by fireparq hypercore-reference; \
                 set AWS_ENDPOINT_URL_S3 (same value)";
    assert_eq!(open(&[]), Ok(()));
    assert_eq!(open(&[("S3_ENDPOINT", endpoint)]), Ok(()));
    assert_eq!(open(&[("AWS_ENDPOINT_URL", endpoint)]), Ok(()));
    assert_eq!(open(&[("S3_ENDPOINT", "")]), Ok(()));
    assert_eq!(
        open(&[("S3_ENDPOINT", "https://s3.amazonaws.com")]),
        Err(stale.into())
    );
    assert_eq!(
        open(&[("AWS_ENDPOINT_URL", "https://other.example")]),
        Err(stale.into())
    );
}

#[tokio::test]
async fn a_stale_endpoint_variable_without_the_new_one_is_refused() {
    let cli = Cli::new();
    let settings = Settings {
        root: Some("s3://hypercore-ref".into()),
        aws: s3(Some("id"), Some("secret")),
        ..Settings::new(Job::Verify)
    };
    let ran = cli
        .invoke(settings, &[("S3_ENDPOINT", "https://datasets-rgw.example")])
        .await;
    assert_eq!(ran.status, 2);
    assert_eq!(ran.last()["event"], "config_error");
}

/// outcomeMeta with 42 live outcomes, or none.
fn many(count: i64) -> Json {
    let mut meta = FakeInfo::new().edit(|state| state.outcome_meta.clone());
    let outcomes = meta.pointer_mut("/outcomes").unwrap().as_arr_mut().unwrap();
    let template = outcomes[0].clone();
    *outcomes = (0..count)
        .map(|index| {
            let mut copy = template.clone();
            *copy.pointer_mut("/outcome").unwrap() = Json::Int((2000 + index).to_string());
            copy
        })
        .collect();
    meta
}

#[tokio::test]
async fn mass_removal_rows_need_the_flag() {
    let cli = Cli::new();
    let job = |allow_mass_removal| {
        Job::Outcomes(OutcomesOptions {
            sweep_from: 9978,
            allow_mass_removal,
            ..OutcomesOptions::default()
        })
    };
    cli.fake.edit(|state| state.outcome_meta = many(42));
    assert_eq!(cli.invoke(cli.settings(job(false)), &[]).await.status, 0);
    cli.fake.edit(|state| state.outcome_meta = many(0));
    let removed = || async {
        let store = Store::new(
            Root::Local(cli.root.clone()),
            &no_aws(),
            &|_| None,
            "test".into(),
            "test",
            MemoryLog::new().logger(),
            cli.clock.clone(),
        )
        .unwrap();
        let rows = crate::jobs::tests::rows_of(&store, &model::OUTCOMES).await;
        rows.iter()
            .filter(|row| row.listed() == Some(false))
            .count()
    };
    cli.clock.advance(300.0);
    let ran = cli.invoke(cli.settings(job(false)), &[]).await;
    assert_eq!(ran.status, 1);
    let failed = ran.last()["failed"][0].as_str().unwrap().to_owned();
    assert!(
        failed.starts_with("hl_outcomes: the snapshot lacks 42 of the 42"),
        "{failed}"
    );
    assert_eq!(removed().await, 0);
    cli.clock.advance(300.0);
    let ran = cli.invoke(cli.settings(job(true)), &[]).await;
    assert_eq!(ran.status, 0, "{}", ran.text);
    assert_eq!(removed().await, 42);
}

#[tokio::test]
async fn a_zero_budget_writes_the_snapshot_and_exits_0() {
    let cli = Cli::new();
    let settings = Settings {
        time_budget_seconds: 0.0,
        ..cli.settings(outcomes(9970))
    };
    let ran = cli.invoke(settings, &[]).await;
    assert_eq!(ran.status, 0, "{}", ran.text);
    let summary = &ran.last()["summary"];
    assert_eq!(summary["probe_budget_exhausted"], json!(true));
    assert_eq!(summary["probed"], json!(0));
    assert_eq!(summary["hl_outcome_meta"], json!(1));
    assert_eq!(summary["hl_outcomes"], json!(5));
    assert_eq!(cli.fake.count("settledOutcome"), 0);
    assert_eq!(cli.fake.count("outcomeMeta"), 1);
}

#[tokio::test]
async fn the_start_event_shows_the_normalized_root() {
    let cli = Cli::new();
    let settings = Settings {
        root: Some(format!("{}/x/../y/", cli.root.display())),
        ..cli.settings(outcomes(9975))
    };
    let ran = cli.invoke(settings, &[]).await;
    assert_eq!(ran.status, 0, "{}", ran.text);
    assert_eq!(
        ran.first()["root"],
        json!(cli.root.join("y").display().to_string())
    );
    assert!(cli.root.join("y/hl_outcomes").is_dir());
}

#[tokio::test]
async fn schema_drift_is_a_configuration_error_without_done() {
    let cli = Cli::new();
    let log_store = firehose_parquet::delta::store::DeltaStore::local(&cli.root)
        .unwrap()
        .log_store(model::OUTCOMES.name)
        .unwrap();
    deltalake_core::operations::create::CreateBuilder::new()
        .with_log_store(log_store)
        .with_columns([deltalake_core::kernel::StructField::new(
            "outcome_id",
            deltalake_core::kernel::DataType::LONG,
            true,
        )])
        .await
        .unwrap();
    let ran = cli.invoke(cli.settings(outcomes(9975)), &[]).await;
    assert_eq!(ran.status, 2, "{}", ran.text);
    assert_eq!(ran.first()["event"], "start");
    assert_eq!(
        ran.last(),
        &json!({"event": "config_error", "message":
            "hl_outcomes: the table's schema differs from this release's \
             (only appended nullable columns can be added)"})
    );
    assert!(!ran.events.iter().any(|event| event["event"] == "done"));
    assert!(cli.fake.calls().is_empty());
}

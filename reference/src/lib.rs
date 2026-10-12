//! HyperLiquid `/info` reference tables (`hl_*`), the library behind
//! `fireparq hypercore-reference`: a port of k8s-parquet PR #82's Python job.
//!
//! Names, labels and parameters that HyperCore blocks do not carry (token and
//! pair symbols, perp dexes and assets, HIP-4 outcomes and questions, funding
//! history) come from HyperLiquid's `POST /info`. The job snapshots them into
//! append-only Delta tables in their own root, never a fireparq root: a row is
//! appended only when an entity's payload changed, so each table is a change
//! log, and every row keeps the entity's canonical JSON and its SHA-256.
//!
//! - [`json`]: the lossless JSON tree and the canonical text payload hashes
//!   cover;
//! - [`values`]: the typed values parsed from payloads (decimals, times,
//!   `k:v|k:v` descriptions, ids);
//! - [`model`]: the eleven tables' columns and the rows built from payloads;
//! - [`arrow`]: their Delta schemas and properties, and the rows as Arrow
//!   record batches (delta-rs's Arrow 59);
//! - [`info`]: the rate-limited, retrying `/info` client, which never shows
//!   the endpoint URL;
//! - [`store`]: the Delta root, its append-only commits, reads, OPTIMIZE and
//!   VACUUM;
//! - [`jobs`]: outcomes, meta, funding, maintain and verify;
//! - [`clock`] and [`log`]: time, and the JSON lines on stdout.
//!
//! [`run_with`] runs one job: the configuration checks, `start`, the job,
//! `done`, and the exit status (0 done, an exhausted time budget included; 1
//! a request, commit or invariant failed; 2 configuration).
//!
//! Nothing here links DataFusion, and no serde_json feature beyond the
//! binary's `raw_value` is enabled (#698).

pub mod arrow;
pub mod clock;
mod compact;
pub mod info;
pub mod jobs;
pub mod json;
pub mod log;
pub mod model;
pub mod store;
#[cfg(any(test, feature = "test-support"))]
pub mod testing;
mod vacuum;
pub mod values;

use std::fmt;
use std::io::Write;
use std::sync::Arc;

use firehose_parquet::s3::AwsConfig;
use serde_json::json;
use time::OffsetDateTime;
use uuid::Uuid;

use crate::clock::{Clock, SystemClock};
use crate::info::{describe_endpoint, info_url, HttpTransport, InfoClient, Transport};
use crate::jobs::{
    run_funding, run_maintain, run_meta, run_outcomes, run_verify, JobError, Options, Run,
    FUNDING_START,
};
use crate::log::Logger;
use crate::model::ALL_TABLES;
use crate::store::{Root, Store};

/// Reads a setting from the environment: the process's in the binary, a
/// closure in tests.
pub type Env<'a> = &'a dyn Fn(&str) -> Option<String>;

/// A setting that makes the job unsafe or impossible: exit status 2, before
/// any request. The message names settings, never their values.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConfigError(pub String);

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for ConfigError {}

/// The outcomes job's options.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OutcomesOptions {
    pub max_probes: u32,
    pub reprobe_after_minutes: u32,
    pub sweep_from: i64,
    pub sweep_to: Option<i64>,
    /// Probe exactly these ids; none or an empty list: the sweep.
    pub outcome_ids: Option<Vec<i64>>,
    pub allow_mass_removal: bool,
}

impl Default for OutcomesOptions {
    fn default() -> Self {
        Self {
            max_probes: 80,
            reprobe_after_minutes: 60,
            sweep_from: 0,
            sweep_to: None,
            outcome_ids: None,
            allow_mass_removal: false,
        }
    }
}

/// The funding job's options; times in µs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FundingOptions {
    pub funding_start: i64,
    /// Set: repair `[funding_start, funding_end)`.
    pub funding_end: Option<i64>,
    /// None or an empty list: every perp coin.
    pub coins: Option<Vec<String>>,
    pub commit_rows: usize,
}

impl Default for FundingOptions {
    fn default() -> Self {
        Self {
            funding_start: FUNDING_START,
            funding_end: None,
            coins: None,
            commit_rows: 50_000,
        }
    }
}

/// The job to run.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Job {
    Outcomes(OutcomesOptions),
    Meta {
        allow_mass_removal: bool,
    },
    Funding(FundingOptions),
    /// OPTIMIZE and VACUUM now; none or an empty list: every table.
    Maintain {
        tables: Option<Vec<String>>,
    },
    Verify,
}

impl Job {
    /// The command: the run id's prefix and the events' `mode`.
    pub fn name(&self) -> &'static str {
        match self {
            Job::Outcomes(_) => "outcomes",
            Job::Meta { .. } => "meta",
            Job::Funding(_) => "funding",
            Job::Maintain { .. } => "maintain",
            Job::Verify => "verify",
        }
    }
}

/// A run's settings. An empty value is unset, as in the Python job: clap
/// passes an empty environment value through, and [`run_with`] drops it. No
/// `Debug`: it holds the AWS credentials.
#[derive(Clone)]
pub struct Settings {
    /// `s3://<bucket>[/<prefix>]` or a local directory; required.
    pub root: Option<String>,
    /// The `/info` endpoint; unset, the public one. It may embed a key.
    pub info_url: Option<String>,
    pub weight_per_minute: u32,
    pub time_budget_seconds: f64,
    /// Whether a run without failures compacts and vacuums its tables.
    pub maintenance: bool,
    pub aws: AwsConfig,
    pub job: Job,
}

impl Settings {
    /// `job` with the defaults: no root, the public `/info`, 400 weight per
    /// minute, a 240 s budget, maintenance on, no AWS settings.
    pub fn new(job: Job) -> Self {
        Self {
            root: None,
            info_url: None,
            weight_per_minute: 400,
            time_budget_seconds: 240.0,
            maintenance: true,
            aws: AwsConfig {
                aws_access_key_id: None,
                aws_secret_access_key: None,
                aws_session_token: None,
                aws_region: None,
                aws_endpoint_url: None,
            },
            job,
        }
    }

    /// Every empty value unset: an empty `HL_INFO_URL` is the public URL, an
    /// empty key is missing, an empty token, region or endpoint is not sent.
    fn normalized(mut self) -> Self {
        let nonempty = |value: Option<String>| value.filter(|value| !value.is_empty());
        self.root = nonempty(self.root);
        self.info_url = nonempty(self.info_url);
        let aws = &mut self.aws;
        aws.aws_access_key_id = nonempty(aws.aws_access_key_id.take());
        aws.aws_secret_access_key = nonempty(aws.aws_secret_access_key.take());
        aws.aws_session_token = nonempty(aws.aws_session_token.take());
        aws.aws_region = nonempty(aws.aws_region.take());
        aws.aws_endpoint_url = nonempty(aws.aws_endpoint_url.take());
        self
    }

    /// What the output never shows: the credentials, and the `/info` URL and
    /// its query (only its scheme and host are ever shown, and never hidden).
    fn secrets(&self) -> Vec<String> {
        let aws = &self.aws;
        let mut secrets: Vec<String> = [
            &aws.aws_access_key_id,
            &aws.aws_secret_access_key,
            &aws.aws_session_token,
        ]
        .into_iter()
        .flatten()
        .cloned()
        .collect();
        if let Some(url) = &self.info_url {
            let shown = info_url(Some(url))
                .ok()
                .map(|parsed| describe_endpoint(&parsed));
            if shown.as_deref() != Some(url.as_str()) {
                secrets.push(url.clone());
            }
            if let Some((_, query)) = url.split_once('?') {
                secrets.push(query.to_owned());
            }
        }
        secrets
    }

    /// The jobs' options.
    fn options(&self, funding_page: usize) -> Options {
        let mut options = Options {
            maintenance: self.maintenance,
            funding_page,
            ..Options::default()
        };
        match &self.job {
            Job::Outcomes(outcomes) => {
                options.max_probes = outcomes.max_probes;
                options.reprobe_after_us = i64::from(outcomes.reprobe_after_minutes) * 60_000_000;
                options.sweep_from = outcomes.sweep_from;
                options.sweep_to = outcomes.sweep_to;
                options.outcome_ids = outcomes.outcome_ids.clone();
                options.allow_mass_removal = outcomes.allow_mass_removal;
            }
            Job::Meta { allow_mass_removal } => options.allow_mass_removal = *allow_mass_removal,
            Job::Funding(funding) => {
                options.funding_start = funding.funding_start;
                options.funding_end = funding.funding_end;
                options.coins = funding.coins.clone();
                options.commit_rows = funding.commit_rows;
            }
            Job::Maintain { .. } | Job::Verify => {}
        }
        options
    }
}

/// What a run needs from its process: injected by tests.
pub struct Runtime {
    pub clock: Arc<dyn Clock>,
    /// `None`: HTTPS (or loopback HTTP) to the configured `/info`.
    pub transport: Option<Arc<dyn Transport>>,
    /// In `[0, 1)`, added to every backoff.
    pub jitter: Box<dyn FnMut() -> f64 + Send>,
    /// The most fundingHistory items in one answer (500).
    pub funding_page: usize,
}

impl Runtime {
    /// The process's clocks, HTTP and random jitter.
    pub fn system() -> Self {
        Self {
            clock: Arc::new(SystemClock::new()),
            transport: None,
            jitter: Box::new(random_unit),
            funding_page: info::FUNDING_PAGE,
        }
    }
}

/// A uniform value in `[0, 1)` from 53 of a v4 UUID's random bits (no `rand`).
fn random_unit() -> f64 {
    let bits = Uuid::new_v4().as_u128() as u64 & ((1 << 53) - 1);
    bits as f64 / (1u64 << 53) as f64
}

/// `<job>-<UTC %Y%m%dT%H%M%SZ>-<6 lowercase hex>`.
fn run_id(job: &str, now_micros: i64) -> String {
    let moment = OffsetDateTime::from_unix_timestamp_nanos(i128::from(now_micros) * 1000)
        .unwrap_or(OffsetDateTime::UNIX_EPOCH);
    let random = Uuid::new_v4().simple().to_string();
    format!(
        "{job}-{:04}{:02}{:02}T{:02}{:02}{:02}Z-{}",
        moment.year(),
        u8::from(moment.month()),
        moment.day(),
        moment.hour(),
        moment.minute(),
        moment.second(),
        &random[..6]
    )
}

/// Runs `settings.job`, writing JSON lines to `out`: the exit status.
///
/// The configuration is checked before any request or S3 call, in the Python
/// job's order: the root; for an `s3://` root its settings and keys; the
/// `/info` URL; the weight budget; then the time budget and `maintain`'s
/// table names. A failure is one `config_error` line and exit status 2. Then
/// `start`, the job, and `done` with the failures and the summary: exit 1 when
/// anything failed. A table whose schema this release cannot extend is a
/// `config_error` after `start`, exit 2 with no `done`.
pub async fn run_with(
    settings: Settings,
    env: Env<'_>,
    rt: Runtime,
    out: Box<dyn Write + Send>,
) -> i32 {
    let started = rt.clock.monotonic();
    let settings = settings.normalized();
    let mode = settings.job.name();
    let run_id = run_id(mode, rt.clock.now_micros());
    let log = Logger::new(out, settings.secrets());
    let config_error = |message: &str| {
        log.emit("config_error", json!({"message": message}));
        2
    };

    let Some(root) = settings.root.as_deref() else {
        return config_error("--root or HLREF_ROOT is required");
    };
    let root = match Root::parse(root) {
        Ok(root) => root,
        Err(error) => return config_error(&error.0),
    };
    let store = match Store::new(
        root,
        &settings.aws,
        env,
        run_id.clone(),
        mode,
        log.clone(),
        Arc::clone(&rt.clock),
    ) {
        Ok(store) => store,
        Err(error) => return config_error(&error.0),
    };
    let url = match info_url(settings.info_url.as_deref()) {
        Ok(url) => url,
        Err(error) => return config_error(&error.0),
    };
    let transport = match rt.transport {
        Some(transport) => transport,
        None => match HttpTransport::new(url.clone()) {
            Ok(transport) => Arc::new(transport),
            Err(error) => return config_error(&error.0),
        },
    };
    let mut info = match InfoClient::new(
        transport,
        settings.weight_per_minute,
        Arc::clone(&rt.clock),
        rt.jitter,
        log.clone(),
    ) {
        Ok(info) => info,
        Err(error) => return config_error(&error.0),
    };
    let budget = settings.time_budget_seconds;
    if !budget.is_finite() || budget < 0.0 {
        return config_error("time budget must be a finite number of seconds, 0 or more");
    }
    if let Job::Maintain {
        tables: Some(names),
    } = &settings.job
    {
        if let Some(unknown) = names
            .iter()
            .find(|name| !ALL_TABLES.iter().any(|spec| spec.name == name.as_str()))
        {
            return config_error(&format!(
                "--tables names no table of this release: {unknown}"
            ));
        }
    }

    info.deadline = Some(rt.clock.monotonic() + budget);
    log.emit(
        "start",
        json!({
            "version": env!("CARGO_PKG_VERSION"),
            "run_id": run_id,
            "mode": mode,
            "root": store.root().display(),
            "info": describe_endpoint(&url),
            "weight_per_minute": settings.weight_per_minute,
            "time_budget_seconds": budget,
            "deltalake": deltalake_core::crate_version(),
        }),
    );
    let options = settings.options(rt.funding_page);
    let mut run = Run::new(
        &store,
        &mut info,
        options,
        log.clone(),
        run_id.clone(),
        Arc::clone(&rt.clock),
    );
    let result = match &settings.job {
        Job::Outcomes(_) => run_outcomes(&mut run).await,
        Job::Meta { .. } => run_meta(&mut run).await,
        Job::Funding(_) => run_funding(&mut run).await,
        Job::Maintain { tables } => run_maintain(&mut run, tables.as_deref()).await,
        Job::Verify => run_verify(&mut run).await,
    };
    match result {
        Ok(()) => {}
        Err(JobError::Fatal(message)) => run.fail(message),
        Err(JobError::Config(message)) => return config_error(&message),
    }
    let (failures, summary) = (run.failures, run.summary);
    let seconds = ((rt.clock.monotonic() - started) * 10.0).round() / 10.0;
    log.emit(
        "done",
        json!({
            "run_id": run_id,
            "mode": mode,
            "failed": failures,
            "summary": summary,
            "requests": info.requests,
            "retries": info.retries,
            "weight": info.used_total(),
            "seconds": seconds,
        }),
    );
    i32::from(!failures.is_empty())
}

#[cfg(test)]
mod tests;

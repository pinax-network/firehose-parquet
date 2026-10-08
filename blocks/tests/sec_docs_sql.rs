//! The SQL of `docs/chains/sec.md` runs: every ```` ```sql ```` block of its
//! "Shipped views" and "Example queries" sections is executed with DuckDB
//! (`delta_scan`) on a SEC dataset that the real `fireparq build
//! --block-type sec` wrote from a mock Firehose serving the real fixture
//! blocks (`tests/fixtures/sec-v013/`, see its README). The views must bind
//! and decide the fixture's cases as documented (spec §8.5 item 6): every
//! distinct `how_voted` gets a label, the one-holding 13F keeps the filing-date
//! rule, the RESTATEMENT replaces its original, and the deleted accession is
//! `is_deleted`. Like the other DuckDB checks, it is skipped locally without a
//! DuckDB 1.5 CLI and required in CI (`tests/common/mod.rs`).
use blocks::sec::schema::TABLE_NAMES;
use firehose_protos::firehose;
use serde_json::{json, Value};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tonic::codegen::{http, BoxFuture, Service};

mod common;
mod sec_fixture;

const TYPE_URL: &str = "type.googleapis.com/pinax.sec.v1.Block";
/// firesec's first streamable window (2003-06-30).
const FIRST_STREAMABLE: u64 = 1_761_552;
/// Where the docs' table registration reads from; the test substitutes the
/// dataset root.
const DOC_ROOT: &str = "s3://…/sec";

#[derive(Clone)]
struct Info;
impl tonic::server::UnaryService<firehose::InfoRequest> for Info {
    type Response = firehose::InfoResponse;
    type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
    fn call(&mut self, _: tonic::Request<firehose::InfoRequest>) -> Self::Future {
        Box::pin(async {
            Ok(tonic::Response::new(firehose::InfoResponse {
                chain_name: "sec".into(),
                first_streamable_block_num: FIRST_STREAMABLE,
                ..Default::default()
            }))
        })
    }
}

/// Serves the fixture windows once, final, with firesec's metadata.
#[derive(Clone)]
struct Stream {
    responses: Arc<Vec<firehose::Response>>,
    first: u64,
}
impl tonic::server::ServerStreamingService<firehose::Request> for Stream {
    type Response = firehose::Response;
    type ResponseStream = std::pin::Pin<
        Box<dyn futures::Stream<Item = Result<Self::Response, tonic::Status>> + Send>,
    >;
    type Future = BoxFuture<tonic::Response<Self::ResponseStream>, tonic::Status>;
    fn call(&mut self, request: tonic::Request<firehose::Request>) -> Self::Future {
        let request = request.into_inner();
        assert_eq!(request.start_block_num, self.first as i64);
        assert!(request.final_blocks_only);
        assert!(request.cursor.is_empty());
        let responses: Vec<_> = self.responses.iter().cloned().map(Ok).collect();
        Box::pin(async move {
            Ok(tonic::Response::new(
                Box::pin(futures::stream::iter(responses)) as Self::ResponseStream,
            ))
        })
    }
}

macro_rules! service {
    ($type:ty, $name:literal, $method:ident) => {
        impl tonic::server::NamedService for $type {
            const NAME: &'static str = $name;
        }
        impl Service<http::Request<tonic::body::Body>> for $type {
            type Response = http::Response<tonic::body::Body>;
            type Error = std::convert::Infallible;
            type Future = BoxFuture<Self::Response, Self::Error>;
            fn poll_ready(
                &mut self,
                _: &mut std::task::Context<'_>,
            ) -> std::task::Poll<Result<(), Self::Error>> {
                std::task::Poll::Ready(Ok(()))
            }
            fn call(&mut self, request: http::Request<tonic::body::Body>) -> Self::Future {
                let service = self.clone();
                Box::pin(async move {
                    let mut grpc = tonic::server::Grpc::new(tonic_prost::ProstCodec::default());
                    Ok(grpc.$method(service, request).await)
                })
            }
        }
    };
}
service!(Info, "sf.firehose.v2.EndpointInfo", unary);
service!(Stream, "sf.firehose.v2.Stream", server_streaming);

/// One FINAL Firehose response per fixture window, with the metadata firesec
/// serves: decimal ids, `parent = lib = n - 1`, time = window start.
fn responses(blocks: &[sec_fixture::FixtureBlock]) -> Vec<firehose::Response> {
    blocks
        .iter()
        .enumerate()
        .map(|(ordinal, block)| {
            let id = &block.identity;
            firehose::Response {
                block: Some(prost_types::Any {
                    type_url: TYPE_URL.into(),
                    value: block.payload.clone(),
                }),
                step: 3,
                cursor: format!("event-{ordinal}"),
                metadata: Some(firehose::BlockMetadata {
                    num: id.block_num,
                    id: id.block_id.clone(),
                    parent_num: id.parent_num,
                    parent_id: id.parent_id.clone(),
                    lib_num: id.lib_num,
                    time: Some(prost_types::Timestamp {
                        seconds: id.timestamp,
                        nanos: 0,
                    }),
                }),
            }
        })
        .collect()
}

/// `fireparq build --block-type sec` of every fixture window into `root`.
pub async fn build_fixture_dataset(cwd: &Path, root: &Path) {
    let blocks = sec_fixture::blocks();
    let first = blocks.first().unwrap().identity.block_num;
    let stop = blocks.last().unwrap().identity.block_num + 1;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let incoming = futures::stream::unfold(listener, |listener| async {
        Some((listener.accept().await.map(|(socket, _)| socket), listener))
    });
    let stream = Stream {
        responses: Arc::new(responses(&blocks)),
        first,
    };
    let server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(Info)
            .add_service(stream)
            .serve_with_incoming(incoming)
            .await
            .unwrap();
    });
    let output = tokio::time::timeout(
        Duration::from_secs(120),
        tokio::process::Command::new(env!("CARGO_BIN_EXE_fireparq"))
            .kill_on_drop(true)
            .env_clear()
            .current_dir(cwd)
            .args(["build", "--endpoint", &endpoint, "--block-type", "sec"])
            .args(["--start-block", &first.to_string()])
            .args(["--stop-block", &stop.to_string()])
            .args(["--stream-idle-timeout-secs", "0"])
            .arg("--output")
            .arg(root)
            .output(),
    )
    .await
    .expect("fireparq timed out")
    .unwrap();
    server.abort();
    let logs = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(output.status.success(), "{logs}");
    // The SEC build defaults fill what the command leaves unset; the explicit
    // `--stream-idle-timeout-secs 0` wins over its default.
    for expected in [
        "grpc_max_message_bytes 536870912",
        "flush_idle         60s",
        "stream_idle_timeout disabled",
        "applied the block family's build defaults to settings left unset",
        "grpc_max_message_bytes=536870912 flush_idle_secs=60 metrics_stale_after_secs=129600",
    ] {
        assert!(logs.contains(expected), "{expected}\n{logs}");
    }
}

/// The ```` ```sql ```` blocks of the `## {title}` section of
/// `docs/chains/sec.md`.
fn section_sql(title: &str) -> Vec<String> {
    let doc = std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../docs/chains/sec.md"
    ))
    .unwrap();
    let section = doc
        .split(&format!("\n## {title}\n"))
        .nth(1)
        .unwrap_or_else(|| panic!("docs/chains/sec.md has no `## {title}` section"))
        .split("\n## ")
        .next()
        .unwrap();
    section
        .split("```sql\n")
        .skip(1)
        .map(|block| block.split("\n```").next().unwrap().to_string())
        .collect()
}

/// DuckDB versions differ in whether JSON output quotes some numbers, so
/// scalars are compared as text.
fn scalars_as_text(value: &Value) -> Value {
    match value {
        Value::Number(number) => Value::String(number.to_string()),
        Value::Array(items) => Value::Array(items.iter().map(scalars_as_text).collect()),
        Value::Object(fields) => Value::Object(
            fields
                .iter()
                .map(|(name, field)| (name.clone(), scalars_as_text(field)))
                .collect(),
        ),
        other => other.clone(),
    }
}

/// A documented query rerun with `from` replaced by `to`, and its rows.
type Variant = (&'static str, &'static str, Vec<Value>);

/// The documented SQL, bound to one dataset.
struct Docs<'a> {
    duckdb: &'a common::DuckDb,
    /// The registration and the views, with the dataset root substituted.
    setup: String,
}

impl Docs<'_> {
    /// The rows of `select` (one statement, no trailing `;`), after the
    /// documented setup, scalars as text.
    fn rows(&self, select: &str) -> Vec<Value> {
        self.duckdb
            .query(&format!(
                "{}\nSELECT 'rows' AS q, * FROM ({select}\n);",
                self.setup
            ))
            .remove("rows")
            .unwrap_or_default()
            .into_iter()
            .map(|mut row| {
                row.as_object_mut().unwrap().remove("q");
                scalars_as_text(&row)
            })
            .collect()
    }

    fn check(&self, select: &str, expected: Vec<Value>) {
        let expected: Vec<Value> = expected.iter().map(scalars_as_text).collect();
        assert_eq!(self.rows(select), expected, "{select}");
    }

    /// [`Docs::check`] ignoring row order (ties of the query's `ORDER BY`).
    fn check_unordered(&self, select: &str, expected: Vec<Value>) {
        let sorted = |rows: Vec<Value>| {
            let mut rows: Vec<String> = rows.iter().map(Value::to_string).collect();
            rows.sort();
            rows
        };
        let expected: Vec<Value> = expected.iter().map(scalars_as_text).collect();
        assert_eq!(sorted(self.rows(select)), sorted(expected), "{select}");
    }
}

/// One statement of the shipped views, found by its leading comment.
fn shipped_view(comment: &str) -> String {
    let views = &section_sql("Shipped views")[1];
    let start = views
        .find(comment)
        .unwrap_or_else(|| panic!("no shipped view `{comment}`"));
    let statement = &views[start..];
    let end = statement.find(";\n").map_or(statement.len(), |end| end + 1);
    statement[..end].to_string()
}

/// `sec_13f_units` on synthetic filings shaped like the real ones the review
/// found (#8): a summary total in another unit than the rows, and the 4-row
/// DAILY JOURNAL report whose rows are in dollars on the thousands date rule.
#[test]
fn sec_13f_units_reads_the_summary_unit_and_breaks_ties_with_it() {
    let dir = tempfile::tempdir().unwrap();
    let cwd = std::fs::canonicalize(dir.path()).unwrap();
    let Some(duckdb) = common::DuckDb::open(&cwd) else {
        return;
    };
    // (accession, rule, qualifying rows, value per share, summary / rows).
    // Each row is 1,000,000 shares, so its value is 10^6 × the value per share.
    let filings: [(&str, i32, u32, f64, Option<f64>); 8] = [
        // DAILY JOURNAL 0001437749-14-014916: 4 rows of $48/share on the
        // thousands rule, summary in thousands (1/1000 of the rows).
        ("a-daily-journal", 1000, 4, 48.0, Some(0.001)),
        // The same 4 rows with a summary that agrees: the guard holds.
        ("b-four-rows-agree", 1000, 4, 48.0, Some(1.0)),
        // TRS of Texas 0000796848-26-000010: rows in dollars, summary in
        // thousands.
        ("c-summary-thousands", 1, 6, 76.0, Some(0.001)),
        // HRT 0001475597-26-000199: rows in thousands after 2023, summary in
        // dollars.
        ("d-rows-thousands", 1, 6, 0.033, Some(1000.0)),
        // 2014 rows in thousands, summary in millions (Cullen and others).
        ("e-summary-millions", 1000, 6, 0.05, Some(0.001)),
        // Agreeing units, and a 10x disagreement no unit explains.
        ("f-agree", 1, 6, 76.0, Some(1.0)),
        ("g-unexplained", 1, 6, 76.0, Some(10.0)),
        // No information table: no evidence against the date rule.
        ("h-no-rows", 1000, 0, 0.0, None),
    ];
    let mut sql = String::from(
        "CREATE TABLE form13f_holdings (block_num BIGINT, filing_index BIGINT, \
         shares_or_principal_type VARCHAR, put_call_norm VARCHAR, \
         shares_or_principal_amount BIGINT, value BIGINT);\n\
         CREATE TABLE form13f_reports (block_num BIGINT, filing_index BIGINT, \
         accession_number VARCHAR, value_multiplier_rule INTEGER, table_value_total BIGINT, \
         holdings_value_sum BIGINT);\n",
    );
    for (index, (accession, rule, rows, per_share, total_to_rows)) in filings.iter().enumerate() {
        let value = (per_share * 1_000_000.0).round() as i64;
        for _ in 0..*rows {
            sql.push_str(&format!(
                "INSERT INTO form13f_holdings VALUES (1, {index}, 'SH', NULL, 1000000, {value});\n"
            ));
        }
        let sum = value * i64::from(*rows);
        let (total, sum) = match total_to_rows {
            Some(ratio) => (((sum as f64) * ratio).round().to_string(), sum.to_string()),
            None => ("1000".to_string(), "NULL".to_string()),
        };
        sql.push_str(&format!(
            "INSERT INTO form13f_reports VALUES (1, {index}, '{accession}', {rule}, {total}, {sum});\n"
        ));
    }
    sql.push_str(&shipped_view("-- 2. 13F value units per filing."));
    sql.push_str(
        "\nSELECT 'u' AS q, accession_number, value_multiplier, value_unit_source, \
         table_value_multiplier, table_value_total_usd FROM sec_13f_units ORDER BY accession_number;",
    );
    let rows: Vec<Value> = duckdb.query(&sql)["u"]
        .iter()
        .map(|row| {
            let mut row = row.clone();
            row.as_object_mut().unwrap().remove("q");
            scalars_as_text(&row)
        })
        .collect();
    let expected: Vec<Value> = [
        // Rows in dollars ($192 M), and the summary of 192,000 thousands.
        (
            "a-daily-journal",
            1,
            "median_and_total_override",
            1000,
            "192000000",
        ),
        (
            "b-four-rows-agree",
            1000,
            "filing_date_rule",
            1000,
            "192000000000",
        ),
        (
            "c-summary-thousands",
            1,
            "filing_date_rule",
            1000,
            "456000000",
        ),
        ("d-rows-thousands", 1000, "median_override", 1, "198000000"),
        (
            "e-summary-millions",
            1000,
            "filing_date_rule",
            1_000_000,
            "300000000",
        ),
        ("f-agree", 1, "filing_date_rule", 1, "456000000"),
        ("g-unexplained", 1, "filing_date_rule", 1, "4560000000"),
        ("h-no-rows", 1000, "filing_date_rule", 1000, "1000000"),
    ]
    .iter()
    .map(
        |(accession, multiplier, source, table_multiplier, total_usd)| {
            scalars_as_text(&json!({
                "accession_number": accession, "value_multiplier": multiplier,
                "value_unit_source": source, "table_value_multiplier": table_multiplier,
                "table_value_total_usd": total_usd,
            }))
        },
    )
    .collect();
    assert_eq!(rows, expected);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn documented_views_and_queries_run_on_the_fixture_dataset() {
    let shipped = section_sql("Shipped views");
    let examples = section_sql("Example queries");
    assert_eq!(
        shipped.len(),
        2,
        "registration, then the views: {shipped:?}"
    );
    assert_eq!(examples.len(), 6, "{examples:?}");
    // The registration names every SEC table once, in TABLE_NAMES order.
    let registered: Vec<String> = shipped[0]
        .lines()
        .map(|line| {
            let table = line
                .strip_prefix("CREATE OR REPLACE VIEW ")
                .and_then(|rest| rest.split(' ').next())
                .unwrap_or_else(|| panic!("registration line: {line}"));
            assert_eq!(
                line,
                format!("CREATE OR REPLACE VIEW {table} AS SELECT * FROM delta_scan('{DOC_ROOT}/{table}');"),
            );
            table.to_string()
        })
        .collect();
    assert_eq!(registered, TABLE_NAMES);
    assert!(shipped[1].contains("version sec-views-v1"));

    let dir = tempfile::tempdir().unwrap();
    let cwd = std::fs::canonicalize(dir.path()).unwrap();
    let Some(duckdb) = common::DuckDb::open(&cwd) else {
        return;
    };
    let root = cwd.join("sec");
    build_fixture_dataset(&cwd, &root).await;
    let docs = Docs {
        duckdb: &duckdb,
        setup: shipped.join("\n").replace(DOC_ROOT, root.to_str().unwrap()),
    };

    // Every table reads, with the fixture's row counts (filing_raw_xml has no
    // Delta files: a table that never gets rows is still created).
    let counts = docs.rows(
        &TABLE_NAMES
            .iter()
            .map(|table| format!("SELECT '{table}' AS t, count(*) AS n FROM {table}"))
            .collect::<Vec<_>>()
            .join(" UNION ALL "),
    );
    assert_eq!(counts.len(), TABLE_NAMES.len());
    for row in &counts {
        let table = row["t"].as_str().unwrap();
        let expected = std::fs::read_to_string(sec_fixture::expected_path(table)).unwrap();
        let expected: Value = serde_json::from_str(&expected).unwrap();
        assert_eq!(
            row["n"],
            json!(expected["rows"].as_array().unwrap().len().to_string()),
            "{table}"
        );
    }

    // sec_filings_first: one row per accession, never a deletion notice; the
    // deleted accession's original is flagged.
    docs.check(
        "SELECT count(*) AS n, count(DISTINCT accession_number) AS accessions, \
                count(*) FILTER (WHERE is_deletion_notice) AS notices, \
                count(*) FILTER (WHERE is_redisseminated) AS redisseminated \
         FROM sec_filings_first",
        vec![json!({"n": 31, "accessions": 31, "notices": 0, "redisseminated": 0})],
    );
    docs.check(
        "SELECT accession_number, form_type, dissemination_count, is_deleted, is_corrected, is_redisseminated \
         FROM sec_filings_first WHERE is_deleted",
        vec![json!({
            "accession_number": "0002147005-26-000004", "form_type": "SCHEDULE 13G",
            "dissemination_count": 1, "is_deleted": true, "is_corrected": false,
            "is_redisseminated": false
        })],
    );

    // sec_13f_units: the date rule, the >= 5-row guard and the override.
    docs.check(
        "SELECT accession_number, value_multiplier_rule, qualifying_rows, value_multiplier, value_unit_source \
         FROM sec_13f_units ORDER BY block_num, filing_index",
        vec![
            json!({"accession_number": "0001323255-14-000015", "value_multiplier_rule": 1000, "qualifying_rows": 0, "value_multiplier": 1000, "value_unit_source": "filing_date_rule"}),
            json!({"accession_number": "0000950123-14-008258", "value_multiplier_rule": 1000, "qualifying_rows": 5, "value_multiplier": 1000, "value_unit_source": "filing_date_rule"}),
            json!({"accession_number": "0001811513-26-000014", "value_multiplier_rule": 1, "qualifying_rows": 6, "value_multiplier": 1000, "value_unit_source": "median_override"}),
            json!({"accession_number": "0000950103-26-012367", "value_multiplier_rule": 1, "qualifying_rows": 5, "value_multiplier": 1, "value_unit_source": "filing_date_rule"}),
            json!({"accession_number": "0001595082-26-000063", "value_multiplier_rule": 1, "qualifying_rows": 1, "value_multiplier": 1, "value_unit_source": "filing_date_rule"}),
            json!({"accession_number": "0000950103-26-912367", "value_multiplier_rule": 1, "qualifying_rows": 4, "value_multiplier": 1, "value_unit_source": "filing_date_rule"}),
            json!({"accession_number": "0001104659-26-097111", "value_multiplier_rule": 1, "qualifying_rows": 1, "value_multiplier": 1, "value_unit_source": "filing_date_rule"}),
        ],
    );
    docs.check(
        "SELECT count(*) AS n, count(*) FILTER (WHERE value_usd = value * value_multiplier) AS exact \
         FROM sec_13f_holdings_usd",
        vec![json!({"n": 22, "exact": 22})],
    );

    // sec_13f_effective_reports: the RESTATEMENT (912367) replaces its
    // original (012367); the 13F-NT is not a holdings report, and the NEW
    // HOLDINGS amendment has no base report in the fixture, so neither counts.
    docs.check(
        "SELECT accession_number, manager_cik, period_of_report, effective_role \
         FROM sec_13f_effective_reports ORDER BY manager_cik, period_of_report, effective_role, accession_number",
        vec![
            json!({"accession_number": "0000950123-14-008258", "manager_cik": "0001439589", "period_of_report": "2014-06-30", "effective_role": "base"}),
            json!({"accession_number": "0000950103-26-912367", "manager_cik": "0001671122", "period_of_report": "2026-06-30", "effective_role": "base"}),
            json!({"accession_number": "0001811513-26-000014", "manager_cik": "0001811513", "period_of_report": "2026-06-30", "effective_role": "base"}),
            json!({"accession_number": "0001104659-26-097111", "manager_cik": "0001947083", "period_of_report": "2026-06-30", "effective_role": "base"}),
        ],
    );

    // sec_how_voted_norm labels every distinct fixture `how_voted`.
    docs.check(
        "SELECT how_voted, sec_how_voted_norm(how_voted) AS norm, count(*) AS n \
         FROM npx_vote_records GROUP BY ALL ORDER BY how_voted",
        vec![
            json!({"how_voted": "1 YEAR", "norm": "FREQUENCY_1Y", "n": 1}),
            json!({"how_voted": "FOR", "norm": "FOR", "n": 7}),
            json!({"how_voted": "TAKE NO ACTION", "norm": "DID_NOT_VOTE", "n": 6}),
        ],
    );
    docs.check(
        "SELECT count(*) FILTER (WHERE how_voted IS NOT NULL AND how_voted_norm IS NULL) AS unlabeled \
         FROM sec_npx_vote_records_norm",
        vec![json!({"unlabeled": 0})],
    );

    // sec_nport_effective_reports: one report per fund series and as-of date.
    docs.check(
        "SELECT count(*) AS n FROM sec_nport_effective_reports",
        vec![json!({"n": 6})],
    );

    // Every example query binds and returns the fixture's rows. The sample-day
    // constants of Q2–Q5 match nothing in the fixture, so each also runs with
    // a fixture value substituted. The expected rows are what the same SQL
    // returns on the reference prototype's tables of the same blocks.
    let q1 = vec![
        json!({"issuer_trading_symbol": "NWIB(OB)", "issuer_name": "NORTHWEST INDIANA BANCORP",
               "owner_names": ["DEGUILIO JON E"], "officer_titles": ["Executive Vice-President"],
               "transaction_date": "2004-06-18", "shares": "86.000000", "price_per_share": "31.080000",
               "value_usd": "2672.880000", "aff_10b5_one": null, "accession_number": "0001209191-04-032033"}),
        // The joint filing: both owners, only the officer's title, and a
        // footnote-only price, so no dollar value.
        json!({"issuer_trading_symbol": "TBCV", "issuer_name": "Thunder Bridge Capital Partners V, Ltd.",
               "owner_names": ["Simanson Gary A", "TBCP V, LLC"], "officer_titles": ["Chief Executive Officer"],
               "transaction_date": "2026-08-12", "shares": "447000.000000", "price_per_share": null,
               "value_usd": null, "aff_10b5_one": false, "accession_number": "0001339459-26-000007"}),
    ];
    // TRINET in the Atairos report: only the effective RESTATEMENT counts.
    let q2 = vec![
        json!({"quarter": "2026-06-30", "managers": 1, "shares": "18085773",
                         "value_usd": "895064906", "rows_unit_override_or_ambiguous": 0}),
    ];
    // TAKE NO ACTION and the frequency vote are undecidable.
    let q3 = vec![
        json!({"filer_cik": "0001554913", "reporting_person": "Pamplona Capital Management, LLC",
               "against_mgmt": 0, "decided": 6, "pct_against": 0.0}),
        json!({"filer_cik": "0002053459", "reporting_person": "Grey Rock Energy Management, LLC",
               "against_mgmt": 0, "decided": 1, "pct_against": 0.0}),
    ];
    let q4 = vec![
        json!({"as_of_date": "2026-06-30", "registrant_name": "VANGUARD VARIABLE INSURANCE FUNDS",
                         "series_name": "INTERNATIONAL PORTFOLIO", "series_id": "S000004403",
                         "shares": "100217.0000000000", "value_usd": "4546845.2900000000",
                         "pct_value": "0.133011585677", "accession_number": "0000857490-26-000627"}),
    ];
    // Both amendments have an `Indefinite` target, so the target sum is NULL.
    let q5 = vec![
        json!({"industry_group": "Pooled Investment Fund", "month": "2026-03-01 00:00:00",
                         "new_offerings": 2, "offering_target_usd": null, "indefinite_targets": 2,
                         "amount_sold_usd": "254126785.00", "rule_506c": 0}),
    ];
    let q6: Vec<Value> = [
        ("-1.172935447009", "-1.1729354470093785"),
        ("-1.056776020146", "-1.0567760201464529"),
        ("-0.202581557532", "-0.20258155753198523"),
    ]
    .into_iter()
    .map(|(typed, raw)| {
        json!({"accession_number": "0002000324-26-004161",
               "series_name": "Defiance Daily Target 2x Long Dram ETF", "issuer_name": "N/A",
               "pct_value": typed, "raw_value": raw})
    })
    .collect();
    let cases: [(Vec<Value>, Option<Variant>); 6] = [
        (q1, None),
        (vec![], Some(("'037833100'", "'896288107'", q2))),
        (
            vec![],
            Some(("HAVING decided >= 100", "HAVING decided >= 1", q3)),
        ),
        (vec![], Some(("'67066G104'", "'M98068105'", q4))),
        (
            vec![],
            Some(("d.form_type = 'D'", "d.form_type = 'D/A'", q5)),
        ),
        (q6, None),
    ];
    for (index, (query, (expected, variant))) in examples.iter().zip(cases).enumerate() {
        let query = query.trim_end().trim_end_matches(';');
        assert!(query.starts_with(&format!("-- Q{}.", index + 1)), "{query}");
        docs.check(query, expected);
        if let Some((from, to, expected)) = variant {
            assert!(query.contains(from), "Q{}: {from}", index + 1);
            // Q3's fixture rows tie on `pct_against`.
            docs.check_unordered(&query.replace(from, to), expected);
        }
    }
}

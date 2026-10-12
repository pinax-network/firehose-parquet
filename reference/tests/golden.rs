//! Parity with the Python job: `tests/fixtures/golden-vectors.json`, which
//! running k8s-parquet PR #82's job on the fixtures produced
//! (`tests/fixtures/README.md`). Every row's typed cells, `payload_json` byte
//! for byte and `payload_sha256`, the canonical JSON, the request bodies, the
//! value parsers, the CRC-32 hours, the Delta schemas and the table properties
//! must match it. Only the public API is used.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use deltalake_core::kernel::StructType;
use firehose_parquet::s3::AwsConfig;
use fireparq_reference::arrow::{delta_fields, TABLE_PROPERTIES};
use fireparq_reference::clock::SystemClock;
use fireparq_reference::info::{InfoClient, Transport, TransportError};
use fireparq_reference::json::{sha256_hex, subject, Json};
use fireparq_reference::log::Logger;
use fireparq_reference::model::{
    funding_row, membership, outcome_live_row, outcome_meta_row, outcome_settled_row,
    perp_asset_row, perp_dex_row, question_live_row, question_settled_row, removal_row,
    spot_meta_row, spot_pair_row, spot_token_row, token_details_row, Cell, ColumnType, Row,
    ALL_TABLES,
};
use fireparq_reference::store::{OpenMode, Root, Store};
use fireparq_reference::values::{
    asset_id, crc32_hour, dex_of, from_millis, parse_decimal, parse_kv, parse_naive_utc, valid_id,
    Ordered,
};
use futures::future::{self, BoxFuture};
use futures::FutureExt as _;
use serde_json::Value;

/// 2026-10-08T16:50:00Z, every golden row's `observed_at`.
const AT: i64 = 1_791_478_200_000_000;

fn golden() -> Value {
    serde_json::from_str(include_str!("fixtures/golden-vectors.json")).unwrap()
}

fn fixture(name: &str) -> Json {
    let path = format!("{}/tests/fixtures/{name}", env!("CARGO_MANIFEST_DIR"));
    Json::parse(&std::fs::read(path).unwrap()).unwrap()
}

fn list<'a>(value: &'a Json, key: &str) -> &'a [Json] {
    value.get(key).and_then(Json::as_arr).unwrap()
}

/// The item of `items` whose `key` is `id`.
fn with_id<'a>(items: &'a [Json], key: &str, id: i64) -> &'a Json {
    items
        .iter()
        .find(|item| item.get(key).and_then(Json::as_i64) == Some(id))
        .unwrap_or_else(|| panic!("no {key} {id}"))
}

fn number(text: &str) -> i64 {
    text.parse().unwrap()
}

/// The entities of a list keyed by their valid id, as the jobs key them.
fn keyed<'a>(items: &'a [Json], key: &str) -> Ordered<i64, &'a Json> {
    items
        .iter()
        .filter_map(|item| Some((valid_id(item.get(key))?, item)))
        .collect()
}

/// The fixtures every row is built from.
struct Fixtures {
    spot: Json,
    dexs: Json,
    metas: Json,
    meta: BTreeMap<&'static str, Json>,
}

impl Fixtures {
    fn load() -> Self {
        Self {
            spot: fixture("spotMeta.json"),
            dexs: fixture("perpDexs.json"),
            metas: fixture("allPerpMetas.json"),
            meta: [
                ("t0", fixture("outcomeMeta_t0.json")),
                ("t1", fixture("outcomeMeta_t1.json")),
            ]
            .into_iter()
            .collect(),
        }
    }

    /// The row a golden label names, built as the Python generator built it.
    fn row(&self, label: &str) -> Row {
        let tokens = keyed(list(&self.spot, "tokens"), "index");
        let words: Vec<&str> = label.split(' ').collect();
        match words.as_slice() {
            ["spotMeta"] => spot_meta_row(&self.spot, AT),
            ["token", index] => spot_token_row(tokens.get(&number(index)).unwrap(), AT),
            ["pair", index] => spot_pair_row(
                with_id(list(&self.spot, "universe"), "index", number(index)),
                &tokens,
                AT,
            ),
            ["tokenDetails", name] => {
                let index = match *name {
                    "HSEI" => 437,
                    "USDC" => 0,
                    "ASI" => 40,
                    other => panic!("no token {other}"),
                };
                let token_id = tokens.get(&index).unwrap().get("tokenId").unwrap();
                let response = fixture(&format!("tokenDetails_{name}.json"));
                token_details_row(index, token_id.as_str().unwrap(), &response, AT)
            }
            ["dex", index] => self.dex(number(index), &tokens),
            ["asset", position] => {
                let (dex, asset) = position.split_once('/').unwrap();
                let (dex, asset) = (number(dex), number(asset));
                let name = match self.dex(dex, &tokens).get("name") {
                    Cell::Text(name) => Some(name.clone()),
                    _ => None,
                };
                let universe = list(&self.metas.as_arr().unwrap()[dex as usize], "universe");
                perp_asset_row(dex, name.as_deref(), asset, &universe[asset as usize], AT)
            }
            ["outcomeMeta", time] => outcome_meta_row(&self.meta[*time], AT),
            ["live", "question", id, time] => {
                let meta = &self.meta[time.trim_matches(['(', ')'])];
                question_live_row(with_id(list(meta, "questions"), "question", number(id)), AT)
            }
            ["live", "outcome", id, time] => {
                let meta = &self.meta[time.trim_matches(['(', ')'])];
                let members = membership(&keyed(list(meta, "questions"), "question"));
                let id = number(id);
                outcome_live_row(
                    with_id(list(meta, "outcomes"), "outcome", id),
                    members.get(&id).copied(),
                    AT,
                )
            }
            ["settled", "outcome", id] => {
                outcome_settled_row(&fixture(&format!("settledOutcome_{id}.json")), AT)
            }
            ["settled", "question", "from", "outcome", id] => {
                question_settled_row(&fixture(&format!("settledOutcome_{id}.json")), AT)
            }
            ["funding", coin, time] => {
                let entries = fixture(&format!("fundingHistory_{}.json", coin.replace(':', "_")));
                let entry = with_id(entries.as_arr().unwrap(), "time", number(time));
                funding_row(entry, AT)
            }
            ["removal", "of", "live", "outcome", "1473", "(question", "198)"] => {
                let live = &list(&self.meta["t0"], "outcomes")[1];
                removal_row(&outcome_live_row(live, Some(198), AT), AT, "outcomeMeta")
            }
            _ => panic!("unknown golden label {label}"),
        }
    }

    fn dex(&self, index: i64, tokens: &Ordered<i64, &Json>) -> Row {
        let position = index as usize;
        perp_dex_row(
            index,
            &self.dexs.as_arr().unwrap()[position],
            &self.metas.as_arr().unwrap()[position],
            tokens,
            AT,
        )
    }
}

/// A golden value as the cell of a column of type `ty`.
fn cell(ty: ColumnType, value: &Value) -> Cell {
    if value.is_null() {
        return Cell::Null;
    }
    match ty {
        ColumnType::Long => Cell::Long(value.as_i64().unwrap()),
        ColumnType::Text => Cell::Text(value.as_str().unwrap().into()),
        ColumnType::Bool => Cell::Bool(value.as_bool().unwrap()),
        ColumnType::Decimal => {
            Cell::Decimal(value["unscaled_i128"].as_str().unwrap().parse().unwrap())
        }
        ColumnType::Timestamp => Cell::Timestamp(value["timestamp_us"].as_i64().unwrap()),
        ColumnType::Kv => Cell::Kv(
            value
                .as_array()
                .unwrap()
                .iter()
                .map(|pair| {
                    (
                        pair[0].as_str().unwrap().into(),
                        pair[1].as_str().unwrap().into(),
                    )
                })
                .collect(),
        ),
        ColumnType::TextList => Cell::TextList(
            value
                .as_array()
                .unwrap()
                .iter()
                .map(|item| item.as_str().map(Into::into))
                .collect(),
        ),
        ColumnType::LongList => Cell::LongList(
            value
                .as_array()
                .unwrap()
                .iter()
                .map(Value::as_i64)
                .collect(),
        ),
    }
}

#[test]
fn golden_rows_match_the_python_job() {
    let golden = golden();
    let fixtures = Fixtures::load();
    let vectors = golden["rows"].as_array().unwrap();
    assert_eq!(vectors.len(), 76);
    for vector in vectors {
        let label = vector["label"].as_str().unwrap();
        let row = fixtures.row(label);
        assert_eq!(row.spec.name, vector["table"], "{label}");
        let expected = vector["row"].as_object().unwrap();
        let columns: BTreeSet<&str> = row.spec.columns.iter().map(|column| column.name).collect();
        assert_eq!(
            expected.keys().map(String::as_str).collect::<BTreeSet<_>>(),
            columns,
            "{label}: columns"
        );
        for (index, column) in row.spec.columns.iter().enumerate() {
            assert_eq!(
                row.cells[index],
                cell(column.ty, &expected[column.name]),
                "{label}: {}",
                column.name
            );
        }
        // The hash is of the stored text.
        if let Cell::Text(text) = row.get("payload_json") {
            assert_eq!(
                row.payload_sha256(),
                Some(sha256_hex(text).as_str()),
                "{label}"
            );
        }
    }
}

#[test]
fn canonical_vectors_match() {
    let golden = golden();
    let vectors = golden["canonical"].as_array().unwrap();
    assert_eq!(vectors.len(), 22);
    for vector in vectors {
        let input = vector["input"].as_str().unwrap();
        let canonical = Json::parse(input.as_bytes()).unwrap().canonical();
        assert_eq!(canonical, vector["canonical"], "{input}");
        assert_eq!(sha256_hex(&canonical), vector["sha256"], "{input}");
    }
}

/// Records each body and answers it with the smallest valid response of its
/// request type.
#[derive(Default)]
struct Recorder {
    bodies: Mutex<Vec<Vec<u8>>>,
}

impl Transport for Recorder {
    fn post<'a>(&'a self, body: Vec<u8>) -> BoxFuture<'a, Result<(u16, Bytes), TransportError>> {
        let kind = Json::parse(&body)
            .unwrap()
            .get("type")
            .unwrap()
            .as_str()
            .unwrap()
            .to_owned();
        self.bodies.lock().unwrap().push(body);
        let answer = match kind.as_str() {
            "spotMeta" => r#"{"tokens":[{}],"universe":[{}]}"#,
            "perpDexs" => "[null]",
            "allPerpMetas" => r#"[{"universe":[{}]}]"#,
            "outcomeMeta" => r#"{"outcomes":[],"questions":[]}"#,
            _ => "null",
        };
        future::ready(Ok((200, Bytes::from_static(answer.as_bytes())))).boxed()
    }
}

#[tokio::test]
async fn request_bodies_and_subjects_match() {
    let recorder = Arc::new(Recorder::default());
    let mut info = InfoClient::new(
        recorder.clone(),
        100_000,
        Arc::new(SystemClock::new()),
        Box::new(|| 0.0),
        Logger::new(Box::new(std::io::sink()), Vec::new()),
    )
    .unwrap();
    info.spot_meta().await.unwrap();
    info.perp_dexs().await.unwrap();
    info.all_perp_metas().await.unwrap();
    info.outcome_meta().await.unwrap();
    info.settled_outcome(9975).await.unwrap();
    info.token_details("0x4519b5c4a2fd0beb66306336f63c7b99", 3)
        .await
        .unwrap();
    info.funding_history("BTC", 1_767_225_600_000, None)
        .await
        .unwrap();
    info.funding_history("xyz:XYZ100", 0, Some(100))
        .await
        .unwrap();
    let bodies = recorder.bodies.lock().unwrap().clone();
    let golden = golden();
    let vectors = golden["request_bodies"].as_array().unwrap();
    assert_eq!(bodies.len(), vectors.len());
    for (body, vector) in bodies.iter().zip(vectors) {
        let body = std::str::from_utf8(body).unwrap();
        assert_eq!(body, vector["body"]);
        assert_eq!(
            subject(&Json::parse(body.as_bytes()).unwrap()),
            vector["subject"]
        );
    }
}

/// `value` rounded to 28 significant digits, half to even: what the
/// generator's default `decimal` context made of a scaled value.
fn python_context(value: i128) -> i128 {
    let digits = value.unsigned_abs().to_string().len() as u32;
    if digits <= 28 {
        return value;
    }
    let scale = 10_i128.pow(digits - 28);
    let (quotient, remainder) = (value / scale, value % scale);
    let up = remainder.abs() * 2 > scale || (remainder.abs() * 2 == scale && quotient % 2 != 0);
    (quotient + if up { value.signum() } else { 0 }) * scale
}

#[test]
fn parser_vectors_match() {
    let golden = golden();
    let decimals = golden["parse_decimal"].as_array().unwrap();
    assert_eq!(decimals.len(), 29);
    let mut rounded = Vec::new();
    for vector in decimals {
        let input = vector["input"].as_str().unwrap();
        let value = match vector["json_kind"].as_str().unwrap() {
            "string" => Json::Str(input.into()),
            _ => Json::parse(input.as_bytes()).unwrap(),
        };
        let parsed = parse_decimal(Some(&value));
        let expected = vector["result"]
            .get("unscaled_i128")
            .map(|unscaled| unscaled.as_str().unwrap().parse::<i128>().unwrap());
        // The job's value is exact; the generator scaled it in 28 digits.
        if parsed.is_some_and(|parsed| python_context(parsed) != parsed) {
            rounded.push(input);
            assert_eq!(parsed.map(python_context), expected, "{input}");
        } else {
            assert_eq!(parsed, expected, "{input}");
        }
    }
    assert_eq!(rounded, ["9999999999999999999999999999.9999999999"]);
    assert_eq!(
        parse_decimal(Some(&Json::Str(rounded[0].into()))),
        Some(10_i128.pow(38) - 1)
    );

    let times = golden["parse_naive_utc"].as_array().unwrap();
    assert_eq!(times.len(), 14);
    for vector in times {
        let input = vector["input"].as_str().unwrap();
        assert_eq!(
            parse_naive_utc(Some(&Json::Str(input.into()))),
            vector["result"]["timestamp_us"].as_i64(),
            "{input}"
        );
    }
    let millis = golden["from_millis"].as_array().unwrap();
    assert_eq!(millis.len(), 4);
    for vector in millis {
        assert_eq!(
            from_millis(vector["input"].as_i64().unwrap()),
            vector["result"]["timestamp_us"].as_i64(),
            "{vector}"
        );
    }
    let maps = golden["parse_kv"].as_array().unwrap();
    assert_eq!(maps.len(), 12);
    for vector in maps {
        let input = vector["input"].as_str().unwrap();
        let expected = vector["result"].as_array().map(|pairs| {
            pairs
                .iter()
                .map(|pair| {
                    (
                        pair[0].as_str().unwrap().to_owned(),
                        pair[1].as_str().unwrap().to_owned(),
                    )
                })
                .collect::<Vec<_>>()
        });
        assert_eq!(parse_kv(Some(input)), expected, "{input}");
    }
    let ids = golden["asset_id"].as_array().unwrap();
    assert_eq!(ids.len(), 4);
    for vector in ids {
        assert_eq!(
            asset_id(
                vector["dex_index"].as_i64().unwrap(),
                vector["asset_index"].as_i64().unwrap()
            ),
            vector["asset_id"].as_i64(),
            "{vector}"
        );
    }
    let dexes = golden["dex_of"].as_object().unwrap();
    assert_eq!(dexes.len(), 5);
    for (coin, dex) in dexes {
        assert_eq!(dex_of(Some(coin)).as_deref(), dex.as_str(), "{coin}");
    }
}

#[test]
fn crc32_hours_match() {
    let golden = golden();
    let coins = golden["crc32_hours"]["delisted_hour"].as_object().unwrap();
    let tokens = golden["crc32_hours"]["token_hour"].as_object().unwrap();
    assert_eq!((coins.len(), tokens.len()), (10, 9));
    for (text, hour) in coins.iter().chain(tokens) {
        assert_eq!(
            u64::from(crc32_hour(text)),
            hour.as_u64().unwrap(),
            "{text}"
        );
    }
}

#[test]
fn delta_schemas_match_the_python_job() {
    let golden = golden();
    let schemas = golden["delta_schemas"].as_object().unwrap();
    assert_eq!(schemas.len(), ALL_TABLES.len());
    for spec in ALL_TABLES {
        let expected = &schemas[spec.name];
        assert_eq!(
            expected["key"],
            serde_json::json!(spec.key),
            "{}",
            spec.name
        );
        assert_eq!(expected["snapshot"], spec.snapshot(), "{}", spec.name);
        assert_eq!(
            expected["delta_table_name"],
            spec.delta_name(),
            "{}",
            spec.name
        );
        let schema = StructType::try_new(delta_fields(spec)).unwrap();
        assert_eq!(
            serde_json::to_value(&schema).unwrap(),
            expected["schema"],
            "{}",
            spec.name
        );
    }
}

#[test]
fn table_properties_match() {
    let properties: serde_json::Map<String, Value> = TABLE_PROPERTIES
        .iter()
        .map(|(property, value)| (property.as_ref().to_owned(), Value::from(*value)))
        .collect();
    assert_eq!(Value::Object(properties), golden()["table_properties"]);
}

/// The golden rows appended to their tables and read back: every cell as
/// built, `payload_json` byte for byte, and each entity's latest row its last
/// (every golden row has the same `observed_at`, and a tie goes to the later
/// row).
#[tokio::test]
async fn golden_rows_survive_a_delta_round_trip() {
    let golden = golden();
    let fixtures = Fixtures::load();
    let dir = tempfile::tempdir().unwrap();
    let aws = AwsConfig {
        aws_access_key_id: None,
        aws_secret_access_key: None,
        aws_session_token: None,
        aws_region: None,
        aws_endpoint_url: None,
    };
    let store = Store::new(
        Root::Local(dir.path().canonicalize().unwrap()),
        &aws,
        &|_| None,
        "golden".into(),
        "golden",
        Logger::new(Box::new(std::io::sink()), Vec::new()),
        Arc::new(SystemClock::new()),
    )
    .unwrap();
    let mut tables: BTreeMap<&str, Vec<Row>> = BTreeMap::new();
    for vector in golden["rows"].as_array().unwrap() {
        let row = fixtures.row(vector["label"].as_str().unwrap());
        tables.entry(row.spec.name).or_default().push(row);
    }
    assert_eq!(tables.len(), ALL_TABLES.len() - 1);
    for (name, rows) in tables {
        let spec = rows[0].spec;
        let mut table = store.open(spec, OpenMode::Write).await.unwrap().unwrap();
        store.append(&mut table, rows.clone()).await.unwrap();
        let columns: Vec<&str> = spec.columns.iter().map(|column| column.name).collect();
        let mut read = Vec::new();
        store
            .for_each_row(&table, &columns, None, &mut |cells| {
                read.push(cells.to_vec())
            })
            .await
            .unwrap();
        let built: Vec<Vec<Cell>> = rows.iter().map(|row| row.cells.clone()).collect();
        assert_eq!(read, built, "{name}");
        let mut last = HashMap::new();
        for row in rows {
            last.insert(row.key(), row);
        }
        assert_eq!(
            store.latest_with(&table, &columns).await.unwrap(),
            last,
            "{name}"
        );
    }
}

//! Offline, warm-cache Parquet property comparison. No provider or S3 requests.
//! Run the entire process under the shared benchmark/Cargo lock, not just compilation.
use anyhow::{bail, ensure, Context, Result};
use arrow::{
    array::{
        Array, ArrayRef, BinaryArray, BinaryBuilder, FixedSizeBinaryArray, LargeBinaryArray,
        LargeStringArray, StringArray, StringBuilder, StringDictionaryBuilder, UInt64Array,
    },
    compute::{cast, concat_batches},
    datatypes::{DataType, Field, Int32Type, Schema},
    record_batch::RecordBatch,
    row::{RowConverter, SortField},
};
use clap::Parser;
use firehose_parquet::{config::Compression, writer::properties};
use parquet::{
    arrow::{arrow_reader::ParquetRecordBatchReaderBuilder, ArrowWriter, ProjectionMask},
    file::properties::WriterProperties,
    schema::types::ColumnPath,
};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeSet,
    fs::{self, File},
    path::{Path, PathBuf},
    sync::Arc,
    time::Instant,
};

#[derive(Parser)]
struct Args {
    #[arg(long, default_value = "/tmp/fireparq-469-live-20260925/mainnet")]
    evm_root: PathBuf,
    #[arg(long, default_value = "/tmp/fireparq-550-before")]
    solana_root: PathBuf,
    #[arg(long, default_value_t = 262_144)]
    synthetic_rows: usize,
    #[arg(long, default_value_t = 3)]
    repetitions: usize,
    #[arg(long, default_value_t = 3)]
    query_repetitions: usize,
    #[arg(long)]
    only: Option<String>,
    #[arg(long)]
    output: PathBuf,
}
struct Corpus {
    name: String,
    batch: RecordBatch,
    columns: Vec<String>,
    dictionary_off: Vec<String>,
    sources: Vec<Value>,
    metadata_variants: usize,
    synthetic: bool,
}
#[derive(Clone)]
struct Probe {
    column: String,
    label: String,
    value: Option<Vec<u8>>,
    expected: usize,
}
const VARIANTS: [&str; 5] = [
    "original",
    "control_65536",
    "for_batch",
    "for_schema",
    "identity_dictionary_off",
];
fn sha(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}
fn parquet_files(path: &Path, files: &mut Vec<PathBuf>) -> Result<()> {
    for entry in fs::read_dir(path)? {
        let path = entry?.path();
        if path.is_dir() {
            parquet_files(&path, files)?;
        } else if path.extension().is_some_and(|x| x == "parquet") {
            files.push(path);
        }
    }
    Ok(())
}
fn read_all(path: &Path) -> Result<RecordBatch> {
    let builder = ParquetRecordBatchReaderBuilder::try_new(File::open(path)?)?;
    let schema = builder.schema().clone();
    let batches = builder
        .with_batch_size(16_384)
        .build()?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    Ok(concat_batches(&schema, batches.iter())?)
}
fn corpus(name: &str, path: &Path, columns: &[&str], dictionary_off: &[&str]) -> Result<Corpus> {
    let mut files = Vec::new();
    parquet_files(path, &mut files)?;
    files.sort();
    ensure!(
        !files.is_empty(),
        "no retained Parquet input at {}",
        path.display()
    );
    let mut batches = Vec::new();
    let mut sources = Vec::new();
    let mut schema = None;
    let mut metadata_variants = 0;
    for path in files {
        let bytes = fs::read(&path)?;
        let batch = read_all(&path)?;
        let chosen = schema.get_or_insert_with(|| batch.schema()).clone();
        ensure!(
            chosen.fields() == batch.schema().fields(),
            "source fields differ"
        );
        metadata_variants += usize::from(chosen.metadata() != batch.schema().metadata());
        sources.push(json!({"file": path.file_name().unwrap().to_string_lossy(), "rows": batch.num_rows(), "bytes": bytes.len(), "sha256": sha(&bytes)}));
        // All typed fields and values are retained. Per-source file footer metadata
        // can differ; the first file's complete schema metadata is used consistently.
        batches.push(RecordBatch::try_new(chosen, batch.columns().to_vec())?);
    }
    let batch = concat_batches(&schema.unwrap(), batches.iter())?;
    for column in columns {
        batch.schema().index_of(column)?;
    }
    Ok(Corpus {
        name: name.into(),
        batch,
        columns: columns.iter().map(|s| s.to_string()).collect(),
        dictionary_off: dictionary_off.iter().map(|s| s.to_string()).collect(),
        sources,
        metadata_variants,
        synthetic: false,
    })
}
fn synthetic(binary: bool, rows: usize) -> Result<Corpus> {
    let mut hashes_b = BinaryBuilder::new();
    let mut addresses_b = BinaryBuilder::new();
    let mut hashes_s = StringBuilder::new();
    let mut addresses_s = StringBuilder::new();
    let mut status = StringDictionaryBuilder::<Int32Type>::new();
    for row in 0..rows {
        let hash = Sha256::digest(format!("fireparq-519-hash-v1:{row}"));
        let address = Sha256::digest(format!("fireparq-519-address-v1:{}", row % 1024));
        if binary {
            hashes_b.append_value(hash);
            if row % 97 == 0 {
                addresses_b.append_null();
            } else {
                addresses_b.append_value(&address[..20]);
            }
        } else {
            hashes_s.append_value(format!("0x{}", hex::encode(hash)));
            if row % 97 == 0 {
                addresses_s.append_null();
            } else {
                addresses_s.append_value(format!("0x{}", hex::encode(&address[..20])));
            }
        }
        status.append(if row % 5 == 0 { "FAILED" } else { "SUCCESS" })?;
    }
    let data_type = if binary {
        DataType::Binary
    } else {
        DataType::Utf8
    };
    let schema = Arc::new(Schema::new(vec![
        Field::new("block_num", DataType::UInt64, false),
        Field::new("hash", data_type.clone(), false),
        Field::new("address", data_type, true),
        Field::new(
            "status",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            false,
        ),
    ]));
    let hashes: ArrayRef = if binary {
        Arc::new(hashes_b.finish())
    } else {
        Arc::new(hashes_s.finish())
    };
    let addresses: ArrayRef = if binary {
        Arc::new(addresses_b.finish())
    } else {
        Arc::new(addresses_s.finish())
    };
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(UInt64Array::from_iter_values(0..rows as u64)),
            hashes,
            addresses,
            Arc::new(status.finish()),
        ],
    )?;
    Ok(Corpus {
        name: format!("synthetic-{}", if binary { "binary" } else { "utf8" }),
        batch,
        columns: vec!["hash".into(), "address".into()],
        dictionary_off: vec!["hash".into()],
        sources: vec![],
        metadata_variants: 0,
        synthetic: true,
    })
}
fn logical(batch: &RecordBatch) -> Result<RecordBatch> {
    let mut fields = Vec::new();
    let mut columns = Vec::new();
    for (field, column) in batch.schema().fields().iter().zip(batch.columns()) {
        if let DataType::Dictionary(_, value) = field.data_type() {
            fields.push(
                field
                    .as_ref()
                    .clone()
                    .with_data_type(value.as_ref().clone()),
            );
            columns.push(cast(column, value)?);
        } else {
            fields.push(field.as_ref().clone());
            columns.push(column.clone());
        }
    }
    Ok(RecordBatch::try_new(
        Arc::new(Schema::new_with_metadata(
            fields,
            batch.schema().metadata().clone(),
        )),
        columns,
    )?)
}
fn row_hash(batch: &RecordBatch) -> Result<String> {
    let batch = logical(batch)?;
    let converter = RowConverter::new(
        batch
            .schema()
            .fields()
            .iter()
            .map(|field| SortField::new(field.data_type().clone()))
            .collect(),
    )?;
    let rows = converter.convert_columns(batch.columns())?;
    let mut digest = Sha256::new();
    digest.update(b"fireparq-519-arrow-row-v1\0");
    for row in rows.iter() {
        let bytes = row.as_ref();
        digest.update((bytes.len() as u64).to_be_bytes());
        digest.update(bytes);
    }
    Ok(hex::encode(digest.finalize()))
}

fn plain(array: &ArrayRef) -> Result<ArrayRef> {
    match array.data_type() {
        DataType::Dictionary(_, value) => Ok(cast(array, value)?),
        _ => Ok(array.clone()),
    }
}
fn value(array: &dyn Array, i: usize) -> &[u8] {
    match array.data_type() {
        DataType::Utf8 => array
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(i)
            .as_bytes(),
        DataType::LargeUtf8 => array
            .as_any()
            .downcast_ref::<LargeStringArray>()
            .unwrap()
            .value(i)
            .as_bytes(),
        DataType::Binary => array
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap()
            .value(i),
        DataType::LargeBinary => array
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .unwrap()
            .value(i),
        DataType::FixedSizeBinary(_) => array
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap()
            .value(i),
        other => panic!("unsupported equality data type: {other}"),
    }
}
fn exact_count(array: &dyn Array, key: Option<&[u8]>) -> usize {
    (0..array.len())
        .filter(|&i| match key {
            None => array.is_null(i),
            Some(key) => !array.is_null(i) && value(array, i) == key,
        })
        .count()
}
fn probes(corpus: &Corpus) -> Result<Vec<Probe>> {
    let mut out = Vec::new();
    for column in &corpus.columns {
        let array = plain(
            corpus
                .batch
                .column_by_name(column)
                .context("missing lookup column")?,
        )?;
        let keys: BTreeSet<Vec<u8>> = (0..array.len())
            .filter(|&i| !array.is_null(i))
            .map(|i| value(array.as_ref(), i).to_vec())
            .collect();
        ensure!(keys.len() > 1, "lookup corpus needs distinct keys");
        let sorted: Vec<_> = keys.iter().collect();
        let mut queries: Vec<(String, Option<Vec<u8>>)> = [0, sorted.len() / 2, sorted.len() - 1]
            .into_iter()
            .enumerate()
            .map(|(i, at)| (format!("present_{i}"), Some(sorted[at].clone())))
            .collect();
        // Mutate a middle key until it is absent yet within the true min/max.
        let middle = sorted[sorted.len() / 2];
        let mut absent = None;
        'search: for pos in (0..middle.len()).rev() {
            for c in b"123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz" {
                let mut candidate = middle.clone();
                candidate[pos] = *c;
                if candidate > **sorted.first().unwrap()
                    && candidate < **sorted.last().unwrap()
                    && !keys.contains(&candidate)
                {
                    absent = Some(candidate);
                    break 'search;
                }
            }
        }
        ensure!(absent.is_some(), "cannot construct an in-range absent key");
        queries.push(("absent_in_range".into(), absent));
        let outside = vec![0; middle.len()];
        ensure!(
            outside < **sorted.first().unwrap() && !keys.contains(&outside),
            "no lower absent probe"
        );
        queries.push(("absent_outside_range".into(), Some(outside)));
        queries.push(("is_null".into(), None));
        for (label, key) in queries {
            out.push(Probe {
                column: column.clone(),
                label,
                expected: exact_count(array.as_ref(), key.as_deref()),
                value: key,
            });
        }
    }
    Ok(out)
}
fn writer_properties(variant: &str, corpus: &Corpus) -> Result<WriterProperties> {
    Ok(match variant {
        "original" => WriterProperties::builder()
            .set_compression(Compression::Zstd.parquet())
            .build(),
        "control_65536" => WriterProperties::builder()
            .set_compression(Compression::Zstd.parquet())
            .set_max_row_group_row_count(Some(65_536))
            .build(),
        "for_batch" => properties::for_batch(Compression::Zstd, &corpus.batch, None)?,
        "for_schema" => properties::for_schema(Compression::Zstd, &corpus.batch.schema(), None),
        "identity_dictionary_off" => {
            let mut builder =
                properties::for_batch(Compression::Zstd, &corpus.batch, None)?.into_builder();
            for column in &corpus.dictionary_off {
                builder = builder
                    .set_column_dictionary_enabled(ColumnPath::new(vec![column.clone()]), false);
            }
            builder.build()
        }
        _ => bail!("unknown variant"),
    })
}
fn query(path: &Path, probe: &Probe) -> Result<Value> {
    let start = Instant::now();
    let builder = ParquetRecordBatchReaderBuilder::try_new(File::open(path)?)?;
    let leaf = builder
        .parquet_schema()
        .columns()
        .iter()
        .position(|c| c.path().parts() == [probe.column.as_str()])
        .context("lookup is not a scalar Parquet leaf")?;
    let total_groups = builder.metadata().num_row_groups();
    let mut selected = Vec::new();
    let mut stats_pruned = 0;
    let mut bloom_pruned = 0;
    let mut bloom_checked = 0;
    for group in 0..total_groups {
        let row_group = builder.metadata().row_group(group);
        let stats = row_group.column(leaf).statistics();
        let excluded = match &probe.value {
            None => stats.is_some_and(|s| s.null_count_opt() == Some(0)),
            Some(key) => stats.is_some_and(|s| {
                s.null_count_opt() == Some(row_group.num_rows() as u64)
                    || (s.min_is_exact()
                        && s.min_bytes_opt().is_some_and(|min| key.as_slice() < min))
                    || (s.max_is_exact()
                        && s.max_bytes_opt().is_some_and(|max| key.as_slice() > max))
            }),
        };
        if excluded {
            stats_pruned += 1;
            continue;
        }
        // A NULL predicate must never be excluded by a Bloom membership check.
        if let Some(key) = &probe.value {
            if let Some(bloom) = builder.get_row_group_column_bloom_filter(group, leaf)? {
                bloom_checked += 1;
                if !bloom.check(key.as_slice()) {
                    bloom_pruned += 1;
                    continue;
                }
            }
        }
        selected.push(group);
    }
    let selected_groups = selected.len();
    let projection = ProjectionMask::leaves(builder.parquet_schema(), [leaf]);
    let reader = builder
        .with_row_groups(selected)
        .with_projection(projection)
        .with_batch_size(16_384)
        .build()?;
    let mut rows = 0;
    let mut matches = 0;
    for batch in reader {
        let batch = batch?;
        rows += batch.num_rows();
        let column = plain(batch.column(0))?;
        matches += exact_count(column.as_ref(), probe.value.as_deref());
    }
    let ns = start.elapsed().as_nanos();
    ensure!(
        matches == probe.expected,
        "query result differs: {} {}",
        probe.column,
        probe.label
    );
    Ok(
        json!({"column": probe.column, "probe": probe.label, "expected_matches": probe.expected, "matches": matches, "wall_ns": ns, "row_groups": total_groups, "groups_selected": selected_groups, "stats_pruned": stats_pruned, "bloom_checked": bloom_checked, "bloom_pruned": bloom_pruned, "rows_decoded": rows}),
    )
}
fn run(args: Args) -> Result<()> {
    ensure!(
        args.repetitions > 0 && args.query_repetitions > 0,
        "repetitions must be positive"
    );
    let specs = [
        (
            "evm-transactions",
            args.evm_root.join("transactions"),
            vec!["hash", "to"],
            vec!["hash"],
        ),
        (
            "evm-logs",
            args.evm_root.join("logs"),
            vec!["tx_hash", "address", "topic1"],
            vec![],
        ),
        (
            "solana-binary-transactions",
            args.solana_root
                .join("binary-failedtrue-votestrue-eachfalse/transactions"),
            vec!["signature"],
            vec!["signature"],
        ),
        (
            "solana-binary-account-lookups",
            args.solana_root
                .join("binary-failedtrue-votestrue-eachfalse/account_lookups"),
            vec!["account_key"],
            vec![],
        ),
        (
            "solana-base58-transactions",
            args.solana_root
                .join("base58-failedtrue-votestrue-eachfalse/transactions"),
            vec!["signature"],
            vec!["signature"],
        ),
        (
            "solana-base58-account-lookups",
            args.solana_root
                .join("base58-failedtrue-votestrue-eachfalse/account_lookups"),
            vec!["account_key"],
            vec![],
        ),
    ];
    let mut corpora = Vec::new();
    for (name, path, columns, off) in specs {
        if args.only.as_ref().is_none_or(|only| only == name) {
            corpora.push(corpus(name, &path, &columns, &off)?);
        }
    }
    if args.synthetic_rows > 0 {
        for binary in [true, false] {
            let c = synthetic(binary, args.synthetic_rows)?;
            if args.only.as_ref().is_none_or(|only| only == &c.name) {
                corpora.push(c);
            }
        }
    }
    ensure!(!corpora.is_empty(), "no corpus selected");
    let temp = tempfile::tempdir()?;
    let mut results = Vec::new();
    for corpus in corpora {
        let expected = logical(&corpus.batch)?;
        let input_hash = row_hash(&corpus.batch)?;
        let probes = probes(&corpus)?;
        let mut samples = Vec::new();
        // Initialize codec/allocator state once for every corpus and variant.
        // Warmup is intentionally absent from the recorded encode samples.
        for variant in VARIANTS {
            let props = writer_properties(variant, &corpus)?;
            let mut writer = ArrowWriter::try_new(Vec::new(), corpus.batch.schema(), Some(props))?;
            writer.write(&corpus.batch)?;
            std::hint::black_box(writer.into_inner()?);
        }
        for rotation in 0..args.repetitions {
            let mut paths = Vec::new();
            let mut run_samples = Vec::new();
            for offset in 0..VARIANTS.len() {
                let variant = VARIANTS[(rotation + offset) % VARIANTS.len()];
                let props = writer_properties(variant, &corpus)?;
                let start = Instant::now();
                let mut writer =
                    ArrowWriter::try_new(Vec::new(), corpus.batch.schema(), Some(props))?;
                writer.write(&corpus.batch)?;
                let bytes = writer.into_inner()?;
                let elapsed = start.elapsed().as_nanos();
                let path = temp
                    .path()
                    .join(format!("{}-{rotation}-{variant}.parquet", corpus.name));
                fs::write(&path, &bytes)?;
                let output = read_all(&path)?;
                ensure!(
                    output.schema() == corpus.batch.schema(),
                    "schema differs: {} {variant}",
                    corpus.name
                );
                ensure!(
                    logical(&output)? == expected,
                    "full rows differ: {} {variant}",
                    corpus.name
                );
                let output_hash = row_hash(&output)?;
                ensure!(input_hash == output_hash, "full logical-row hash differs");
                paths.push((variant, path));
                run_samples.push(json!({"variant": variant, "rotation": rotation, "file_bytes": bytes.len(), "output_file_sha256": sha(&bytes), "encode_ns": elapsed, "full_rows_equal": true, "full_schema_equal": true, "output_rows_sha256": output_hash, "queries": []}));
            }
            // Initialize the exact pruning/decoder paths before measuring.
            for (_, path) in &paths {
                for probe in &probes {
                    std::hint::black_box(query(path, probe)?);
                }
            }
            for repeat in 0..args.query_repetitions {
                for offset in 0..paths.len() {
                    let index = (rotation + repeat + offset) % paths.len();
                    let (_, path) = &paths[index];
                    for offset in 0..probes.len() {
                        let probe = &probes[(repeat + offset) % probes.len()];
                        let mut result = query(path, probe)?;
                        result["repeat"] = json!(repeat);
                        run_samples[index]["queries"]
                            .as_array_mut()
                            .unwrap()
                            .push(result);
                    }
                }
            }
            samples.extend(run_samples);
            for (_, path) in paths {
                fs::remove_file(path)?;
            }
        }
        eprintln!(
            "qualified {}: {} rows, {} samples",
            corpus.name,
            corpus.batch.num_rows(),
            samples.len()
        );
        results.push(json!({"corpus": corpus.name, "synthetic": corpus.synthetic, "rows": corpus.batch.num_rows(), "columns": corpus.batch.num_columns(), "source_files": corpus.sources, "source_schema_metadata_differences_rebound_to_first": corpus.metadata_variants, "input_rows_sha256": input_hash, "dictionary_off_columns": corpus.dictionary_off, "samples": samples}));
    }
    let result = json!({"row_digest_format": "fireparq-519-arrow-row-v1: SHA256 of domain plus big-endian u64 length and Arrow60 RowConverter default-sort bytes per row, after top-level dictionary normalization; exact full schema compared separately", "properties_sha256": sha(include_bytes!("../src/writer/properties.rs")), "driver_sha256": sha(include_bytes!("bench_lookup_properties.rs")), "notes": "Offline warm-cache local equality scans. All variants use exact row-group min/max bounds and null statistics before optional Bloom pruning; inexact/truncated bounds never exclude a group, then decode/filter only the equality column. NULL never checks Bloom. One unrecorded encode warmup per corpus/variant and one query warmup per file/probe precede samples. Encode timing writes a complete in-memory Parquet file; filesystem publication, fsync, full-schema/row validation and fixture construction are outside that timing. Source file SHA256 and output_file_sha256 identify physical artifacts. Source footer metadata is rebound to the first file when combining; all typed fields and row values retained. Synthetic hash keys are deterministic and unique, addresses have 1024 values plus NULL, status has two labels; these are not live throughput measurements.", "repetitions": args.repetitions, "query_repetitions": args.query_repetitions, "results": results});
    fs::write(args.output, serde_json::to_vec_pretty(&result)?)?;
    Ok(())
}
fn main() -> Result<()> {
    run(Args::parse())
}

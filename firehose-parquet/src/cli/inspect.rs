//! Single-file Parquet inspection: footer metadata, schema, row groups and
//! column chunks of one file (a table's data file, a Delta checkpoint, the
//! cursor mirror). It never lists a directory.
use super::*;

// ---------------------------------------------------------------------------
// Inspect
// ---------------------------------------------------------------------------

/// Inspect a single parquet file's metadata.
///
/// Displays file-level key-value metadata, Arrow schema, row group details,
/// and per-column chunk information.
/// Supports local filesystem paths and S3 URIs (`s3://bucket/key.parquet`).
/// Non-URI relative paths resolve locally first; when no local path exists and
/// `S3_BUCKET` is configured, they fall back to `s3://<bucket>/<path>`.
pub fn inspect_parquet(
    path: &str,
    schema_only: bool,
    json: bool,
    aws: Option<&AwsConfig>,
) -> anyhow::Result<()> {
    match resolve_parquet_input_path(path) {
        ParquetInputPath::S3(path) => inspect_parquet_s3(
            &path,
            schema_only,
            json,
            aws.ok_or_else(|| anyhow::anyhow!("AWS config required for S3 paths"))?,
        ),
        ParquetInputPath::Local(path) => {
            inspect_parquet_local(path.to_string_lossy().as_ref(), schema_only, json)
        }
    }
}

/// Inspect a local parquet file.
pub(in crate::cli) fn inspect_parquet_local(
    path: &str,
    schema_only: bool,
    json: bool,
) -> anyhow::Result<()> {
    use parquet::file::reader::FileReader;
    use parquet::file::serialized_reader::SerializedFileReader;
    use std::fs;

    let file = fs::File::open(path).map_err(|e| anyhow::anyhow!("opening {path}: {e}"))?;
    let file_size = file.metadata()?.len();
    anyhow::ensure!(
        !file.metadata()?.is_dir(),
        "{path} is a directory: inspect reads one Parquet file; read a table through its \
         Delta log (delta_scan, scan_delta) or validate it with `fireparq validate`"
    );
    let reader = SerializedFileReader::new(file)?;
    let metadata = reader.metadata();

    print_inspect(path, file_size, metadata, schema_only, json)?;
    Ok(())
}

/// Inspect an S3 parquet file.
pub(in crate::cli) fn inspect_parquet_s3(
    path: &str,
    schema_only: bool,
    json: bool,
    aws: &AwsConfig,
) -> anyhow::Result<()> {
    use crate::writer::parse_s3_url;
    use parquet::file::reader::FileReader;
    use parquet::file::serialized_reader::SerializedFileReader;

    let (bucket, key) = parse_s3_url(path)?;

    let client = crate::s3::build_s3_store(
        aws,
        &bucket,
        crate::s3::S3Operation::ReadOnly,
        crate::s3::CredentialPolicy::ProviderChain,
    )?;

    let obj_path = object_store::path::Path::from(key.as_str());
    let data = block_on_async(crate::maintenance::discovery::read_object_bytes(
        &client, &obj_path,
    ))
    .map_err(|e| anyhow::anyhow!("reading {path}: {e}"))?;

    let file_size = data.len() as u64;
    let reader = SerializedFileReader::new(bytes::Bytes::from(data))
        .map_err(|e| anyhow::anyhow!("parsing parquet from {path}: {e}"))?;
    let metadata = reader.metadata();

    print_inspect(path, file_size, metadata, schema_only, json)?;
    Ok(())
}

/// Print the full inspection output for a parquet file.
pub(in crate::cli) fn print_inspect(
    path: &str,
    file_size: u64,
    metadata: &parquet::file::metadata::ParquetMetaData,
    schema_only: bool,
    json: bool,
) -> anyhow::Result<()> {
    let file_meta = metadata.file_metadata();
    let num_row_groups = metadata.num_row_groups();
    let total_rows: i64 = metadata.row_groups().iter().map(|rg| rg.num_rows()).sum();
    let num_columns = file_meta.schema().get_fields().len();
    let schema = build_schema_fields(file_meta.schema().get_fields());

    if json {
        let output = if schema_only {
            serde_json::json!({
                "path": path,
                "schema": schema,
            })
        } else {
            serde_json::json!({
                "path": path,
                "file_size_bytes": file_size,
                "rows": total_rows,
                "row_groups": num_row_groups,
                "columns": num_columns,
                "created_by": file_meta.created_by(),
                "version": file_meta.version(),
                "schema": schema,
            })
        };
        println!("{}", serde_json::to_string_pretty(&output)?);
        return Ok(());
    }

    if schema_only {
        println!("{}", "═".repeat(72));
        println!("  Schema: {}", path);
        println!("{}", "─".repeat(72));
        for field in file_meta.schema().get_fields() {
            print_schema_field(field, 1);
        }
        println!("{}", "═".repeat(72));
        return Ok(());
    }

    // Header
    println!("{}", "═".repeat(72));
    println!("  {}", path);
    println!("{}", "─".repeat(72));
    println!(
        "  rows: {}  row_groups: {}  columns: {}  size: {}",
        total_rows,
        num_row_groups,
        num_columns,
        format_bytes(file_size),
    );
    if let Some(created_by) = file_meta.created_by() {
        println!("  created_by: {}", created_by);
    }
    println!("  version: {}", file_meta.version());

    // File-level key-value metadata
    if let Some(kv_meta) = file_meta.key_value_metadata() {
        if !kv_meta.is_empty() {
            println!("\n{}", "─".repeat(72));
            println!("  File Metadata ({} entries)", kv_meta.len());
            println!("{}", "─".repeat(72));
            let max_key_len = kv_meta.iter().map(|kv| kv.key.len()).max().unwrap_or(0);
            for kv in kv_meta {
                let value = kv.value.as_deref().unwrap_or("(null)");
                // Truncate very long values (e.g. serialized Arrow schema)
                let display_value = if value.len() > 120 {
                    format!("{}… ({} bytes)", &value[..120], value.len())
                } else {
                    value.to_string()
                };
                println!(
                    "  {:width$}  {}",
                    kv.key,
                    display_value,
                    width = max_key_len
                );
            }
        }
    }

    // Schema
    println!("\n{}", "─".repeat(72));
    println!("  Schema");
    println!("{}", "─".repeat(72));

    // Use the parquet schema for detailed type info.
    for field in file_meta.schema().get_fields() {
        print_schema_field(field, 1);
    }

    // Row groups
    println!("\n{}", "─".repeat(72));
    println!("  Row Groups");
    println!("{}", "─".repeat(72));

    for (i, rg) in metadata.row_groups().iter().enumerate() {
        let compressed = rg.compressed_size();
        let uncompressed = rg.total_byte_size();
        let ratio = if uncompressed > 0 {
            format!("{:.1}x", uncompressed as f64 / compressed as f64)
        } else {
            "N/A".to_string()
        };
        println!(
            "  [{}]  rows: {}  compressed: {}  uncompressed: {}  ratio: {}",
            i,
            rg.num_rows(),
            format_bytes(compressed as u64),
            format_bytes(uncompressed as u64),
            ratio,
        );
    }

    // Column details (from first row group for encoding/compression info)
    if num_row_groups > 0 {
        let rg = metadata.row_groups().first().unwrap();
        println!("\n{}", "─".repeat(72));
        println!("  Column Details (row group 0)");
        println!("{}", "─".repeat(72));

        let max_col_name = rg
            .columns()
            .iter()
            .map(|c| c.column_path().string().len())
            .max()
            .unwrap_or(0);

        for col in rg.columns() {
            let col_path = col.column_path().string();
            let compression = format!("{:?}", col.compression());
            let encodings: Vec<String> = col.encodings().map(|e| format!("{:?}", e)).collect();
            let compressed = col.compressed_size();
            let uncompressed = col.uncompressed_size();
            let ratio = if uncompressed > 0 {
                format!("{:.1}x", uncompressed as f64 / compressed as f64)
            } else {
                "N/A".to_string()
            };
            println!(
                "  {:width$}  {}  {}  compressed: {}  uncompressed: {}  ratio: {}",
                col_path,
                compression,
                encodings.join("+"),
                format_bytes(compressed as u64),
                format_bytes(uncompressed as u64),
                ratio,
                width = max_col_name,
            );
        }
    }

    println!("\n{}", "═".repeat(72));
    Ok(())
}

#[derive(Debug, Clone, serde::Serialize)]
pub(in crate::cli) struct InspectSchemaField {
    pub(in crate::cli) name: String,
    pub(in crate::cli) kind: String,
    pub(in crate::cli) physical_type: Option<String>,
    pub(in crate::cli) logical_type: Option<String>,
    pub(in crate::cli) repetition: String,
    pub(in crate::cli) nullable: bool,
    pub(in crate::cli) type_length: Option<i32>,
    pub(in crate::cli) children: Vec<InspectSchemaField>,
}

pub(in crate::cli) fn build_schema_fields(
    fields: &[parquet::schema::types::TypePtr],
) -> Vec<InspectSchemaField> {
    fields
        .iter()
        .map(|field| build_schema_field(field))
        .collect()
}

pub(in crate::cli) fn build_schema_field(
    field: &parquet::schema::types::Type,
) -> InspectSchemaField {
    use parquet::basic::Repetition;
    use parquet::schema::types::Type;

    match field {
        Type::PrimitiveType {
            basic_info,
            physical_type,
            type_length,
            ..
        } => InspectSchemaField {
            name: basic_info.name().to_string(),
            kind: "primitive".to_string(),
            physical_type: Some(format!("{:?}", physical_type)),
            logical_type: basic_info
                .logical_type_ref()
                .map(|value| format!("{:?}", value)),
            repetition: format!("{:?}", basic_info.repetition()).to_lowercase(),
            nullable: matches!(basic_info.repetition(), Repetition::OPTIONAL),
            type_length: (*type_length > 0).then_some(*type_length),
            children: Vec::new(),
        },
        Type::GroupType {
            basic_info, fields, ..
        } => InspectSchemaField {
            name: basic_info.name().to_string(),
            kind: "group".to_string(),
            physical_type: None,
            logical_type: basic_info
                .logical_type_ref()
                .map(|value| format!("{:?}", value)),
            repetition: format!("{:?}", basic_info.repetition()).to_lowercase(),
            nullable: matches!(basic_info.repetition(), Repetition::OPTIONAL),
            type_length: None,
            children: build_schema_fields(fields),
        },
    }
}

/// Print a parquet schema field with indentation (supports nested types).
pub(in crate::cli) fn print_schema_field(field: &parquet::schema::types::Type, indent: usize) {
    use parquet::basic::Repetition;
    use parquet::schema::types::Type;

    let prefix = "  ".repeat(indent);
    match field {
        Type::PrimitiveType {
            basic_info,
            physical_type,
            type_length,
            ..
        } => {
            let repetition = format!("{:?}", basic_info.repetition());
            let nullable = matches!(basic_info.repetition(), Repetition::OPTIONAL);
            let logical = basic_info
                .logical_type_ref()
                .map(|lt| format!(" ({:?})", lt))
                .unwrap_or_default();
            let len_info = if *type_length > 0 {
                format!("({})", type_length)
            } else {
                String::new()
            };
            println!(
                "{}{:30} {:?}{}{}  {} nullable={}",
                prefix,
                basic_info.name(),
                physical_type,
                len_info,
                logical,
                repetition.to_lowercase(),
                nullable,
            );
        }
        Type::GroupType {
            basic_info, fields, ..
        } => {
            let repetition = format!("{:?}", basic_info.repetition());
            let nullable = matches!(basic_info.repetition(), Repetition::OPTIONAL);
            let logical = basic_info
                .logical_type_ref()
                .map(|lt| format!(" ({:?})", lt))
                .unwrap_or_default();
            println!(
                "{}{:30} group{}  {} nullable={}",
                prefix,
                basic_info.name(),
                logical,
                repetition.to_lowercase(),
                nullable,
            );
            for f in fields {
                print_schema_field(f, indent + 1);
            }
        }
    }
}

/// Human-readable byte size formatting.
pub fn format_bytes(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = 1024 * KB;
    const GB: u64 = 1024 * MB;
    if bytes >= GB {
        format!("{:.2} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.2} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.2} KB", bytes as f64 / KB as f64)
    } else {
        format!("{bytes} B")
    }
}

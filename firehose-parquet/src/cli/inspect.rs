//! Read-only Parquet scan, sampling and file inspection.
use super::*;

#[derive(clap::ValueEnum, Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScanOrder {
    Asc,
    Desc,
}

/// Scan and display parquet files at the given path.
///
/// Supports local filesystem paths and S3 URIs (`s3://bucket/prefix`).
/// Non-URI relative paths resolve locally first; when no local path exists and
/// `S3_BUCKET` is configured, they fall back to `s3://<bucket>/<path>`.
/// If `path` is a file, inspects that single file.
/// If `path` is a directory, recursively finds all `.parquet` files.
pub fn scan_parquet(
    path: &str,
    rows: usize,
    offset: usize,
    order: ScanOrder,
    schema_only: bool,
    vertical: bool,
    json: bool,
    aws: Option<&AwsConfig>,
) -> anyhow::Result<()> {
    let resolved_path = resolve_parquet_input_path(path);
    let files = match &resolved_path {
        ParquetInputPath::S3(path) => collect_scan_parquet_s3(
            path,
            rows,
            offset,
            order,
            schema_only,
            aws.ok_or_else(|| anyhow::anyhow!("AWS config required for S3 paths"))?,
        )?,
        ParquetInputPath::Local(path) => {
            collect_scan_parquet_local(path, rows, offset, order, schema_only)?
        }
    };

    if files.is_empty() {
        if json {
            println!(
                "{}",
                serde_json::to_string_pretty(&ScanJsonOutput {
                    files_scanned: 0,
                    files: Vec::new(),
                })?
            );
        } else {
            println!("No .parquet files found in {path}");
        }
        return Ok(());
    }

    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&ScanJsonOutput {
                files_scanned: files.len(),
                files,
            })?
        );
        return Ok(());
    }

    let row_mode = if vertical {
        ScanRowDisplayMode::Vertical
    } else {
        ScanRowDisplayMode::Table
    };
    render_scan_results(&files, row_mode, !schema_only && rows > 0);
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::cli) enum ScanRowDisplayMode {
    Table,
    Vertical,
}

#[derive(Debug, Clone, serde::Serialize)]
pub(in crate::cli) struct ScanSchemaColumn {
    pub(in crate::cli) name: String,
    pub(in crate::cli) data_type: String,
    pub(in crate::cli) nullable: bool,
}

#[derive(Debug, Clone, serde::Serialize)]
pub(in crate::cli) struct ScanRowCell {
    pub(in crate::cli) name: String,
    pub(in crate::cli) value: String,
}

#[derive(Debug, Clone, serde::Serialize)]
pub(in crate::cli) struct ScanRow {
    pub(in crate::cli) row_number: usize,
    pub(in crate::cli) cells: Vec<ScanRowCell>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub(in crate::cli) struct ScanFileResult {
    pub(in crate::cli) path: String,
    pub(in crate::cli) total_rows: i64,
    pub(in crate::cli) row_groups: usize,
    pub(in crate::cli) columns: usize,
    pub(in crate::cli) size_bytes: u64,
    pub(in crate::cli) size_human: String,
    pub(in crate::cli) schema: Vec<ScanSchemaColumn>,
    pub(in crate::cli) sample_rows: Vec<ScanRow>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub(in crate::cli) struct ScanJsonOutput {
    pub(in crate::cli) files_scanned: usize,
    pub(in crate::cli) files: Vec<ScanFileResult>,
}

/// Scan parquet files from the local filesystem.
pub(in crate::cli) fn collect_scan_parquet_local(
    path: &std::path::Path,
    rows: usize,
    offset: usize,
    order: ScanOrder,
    schema_only: bool,
) -> anyhow::Result<Vec<ScanFileResult>> {
    let mut files: Vec<PathBuf> = Vec::new();
    let single_file = if path.is_file() {
        files.push(path.to_path_buf());
        true
    } else if path.is_dir() {
        crate::maintenance::discovery::collect_local(
            path,
            crate::maintenance::discovery::LocalPolicy::PARQUET,
            &mut files,
        )?;
        files.sort();
        false
    } else {
        anyhow::bail!("path does not exist: {}", path.display());
    };

    let mut results = Vec::with_capacity(files.len());
    let mut remaining_rows = rows;
    let mut remaining_offset = offset;
    for file_path in &files {
        let display_path = if single_file {
            file_path.display().to_string()
        } else {
            file_path
                .strip_prefix(path)
                .unwrap_or(file_path)
                .display()
                .to_string()
        };
        let result = build_scan_file_result_from_local(
            file_path,
            display_path,
            remaining_rows,
            remaining_offset,
            order,
            schema_only,
        )?;
        update_scan_progress(
            &mut remaining_rows,
            &mut remaining_offset,
            result.total_rows,
            result.sample_rows.len(),
            rows,
            schema_only,
        )?;
        results.push(result);
        if !schema_only && remaining_rows == 0 {
            break;
        }
    }
    Ok(results)
}

pub(in crate::cli) fn build_scan_file_result_from_local(
    file_path: &std::path::Path,
    display_path: String,
    rows: usize,
    offset: usize,
    order: ScanOrder,
    schema_only: bool,
) -> anyhow::Result<ScanFileResult> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use std::fs;

    let file = fs::File::open(file_path)?;
    let file_size = file.metadata()?.len();
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let metadata = builder.metadata();
    let total_rows: i64 = metadata.row_groups().iter().map(|rg| rg.num_rows()).sum();
    let row_groups = metadata.num_row_groups();
    let columns = metadata.file_metadata().schema().get_fields().len();
    let schema = builder.schema().clone();
    let sample_rows = if schema_only || rows == 0 {
        Vec::new()
    } else {
        let file = fs::File::open(file_path)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
        let reader = builder.build()?;
        let total_rows_for_sampling = scan_total_rows_for_sampling(total_rows)?;
        collect_sample_rows(
            &schema,
            reader,
            total_rows_for_sampling,
            rows,
            offset,
            order,
        )
    };

    Ok(ScanFileResult {
        path: display_path,
        total_rows,
        row_groups,
        columns,
        size_bytes: file_size,
        size_human: format_bytes(file_size),
        schema: build_scan_schema(&schema),
        sample_rows,
    })
}

/// Scan parquet files from an S3 bucket.
pub(in crate::cli) fn collect_scan_parquet_s3(
    path: &str,
    rows: usize,
    offset: usize,
    order: ScanOrder,
    schema_only: bool,
    aws: &AwsConfig,
) -> anyhow::Result<Vec<ScanFileResult>> {
    use crate::writer::parse_s3_url;

    let (bucket, prefix) = parse_s3_url(path)?;
    let client = aws.build_s3_client(&bucket)?;
    let (parquet_objects, exact_object_path) =
        block_on_async(collect_scan_s3_parquet_objects(&client, &prefix))
            .map_err(|e| anyhow::anyhow!("listing S3 objects: {e}"))?;

    let mut results = Vec::with_capacity(parquet_objects.len());
    let mut remaining_rows = rows;
    let mut remaining_offset = offset;
    for obj in &parquet_objects {
        let data = block_on_async(crate::maintenance::discovery::read_object_bytes(
            &client,
            &obj.location,
        ))
        .map_err(|e| anyhow::anyhow!("reading s3://{bucket}/{}: {e}", obj.location))?;
        let display_key = scan_s3_display_key(obj.location.as_ref(), &prefix, exact_object_path);
        let result = build_scan_file_result_from_bytes(
            data,
            display_key,
            remaining_rows,
            remaining_offset,
            order,
            schema_only,
        )?;
        update_scan_progress(
            &mut remaining_rows,
            &mut remaining_offset,
            result.total_rows,
            result.sample_rows.len(),
            rows,
            schema_only,
        )?;
        results.push(result);
        if !schema_only && remaining_rows == 0 {
            break;
        }
    }

    Ok(results)
}

pub(in crate::cli) fn build_scan_file_result_from_bytes(
    data: bytes::Bytes,
    display_path: String,
    rows: usize,
    offset: usize,
    order: ScanOrder,
    schema_only: bool,
) -> anyhow::Result<ScanFileResult> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    let file_size = data.len() as u64;
    let builder = ParquetRecordBatchReaderBuilder::try_new(data.clone())?;
    let metadata = builder.metadata();
    let total_rows: i64 = metadata.row_groups().iter().map(|rg| rg.num_rows()).sum();
    let row_groups = metadata.num_row_groups();
    let columns = metadata.file_metadata().schema().get_fields().len();
    let schema = builder.schema().clone();
    let sample_rows = if schema_only || rows == 0 {
        Vec::new()
    } else {
        let builder = ParquetRecordBatchReaderBuilder::try_new(data)?;
        let reader = builder.build()?;
        let total_rows_for_sampling = scan_total_rows_for_sampling(total_rows)?;
        collect_sample_rows(
            &schema,
            reader,
            total_rows_for_sampling,
            rows,
            offset,
            order,
        )
    };

    Ok(ScanFileResult {
        path: display_path,
        total_rows,
        row_groups,
        columns,
        size_bytes: file_size,
        size_human: format_bytes(file_size),
        schema: build_scan_schema(&schema),
        sample_rows,
    })
}

pub(in crate::cli) async fn collect_scan_s3_parquet_objects(
    store: &dyn object_store::ObjectStore,
    prefix: &str,
) -> anyhow::Result<(Vec<object_store::ObjectMeta>, bool)> {
    if prefix.ends_with(".parquet") && !prefix.is_empty() {
        let object_path = object_store::path::Path::from(prefix);
        match store.head(&object_path).await {
            Ok(meta) => return Ok((vec![meta], true)),
            Err(object_store::Error::NotFound { .. }) => {}
            Err(err) => {
                return Err(anyhow::anyhow!(
                    "reading object metadata for {prefix}: {err}"
                ))
            }
        }
    }

    let objects = crate::maintenance::discovery::list_objects(store, prefix).await?;
    let mut parquet_objects: Vec<_> = objects
        .into_iter()
        .filter(|obj| obj.location.as_ref().ends_with(".parquet"))
        .collect();
    parquet_objects.sort_by(|a, b| a.location.cmp(&b.location));
    Ok((parquet_objects, false))
}

pub(in crate::cli) fn scan_s3_display_key(
    location: &str,
    prefix: &str,
    exact_object_path: bool,
) -> String {
    if exact_object_path {
        return location.to_string();
    }
    crate::maintenance::discovery::relative_key(prefix, location).to_string()
}

pub(in crate::cli) fn build_scan_schema(
    schema: &arrow::datatypes::SchemaRef,
) -> Vec<ScanSchemaColumn> {
    schema
        .fields()
        .iter()
        .map(|field| ScanSchemaColumn {
            name: field.name().clone(),
            data_type: field.data_type().to_string(),
            nullable: field.is_nullable(),
        })
        .collect()
}

pub(in crate::cli) fn collect_sample_rows(
    schema: &arrow::datatypes::SchemaRef,
    reader: impl Iterator<Item = Result<arrow::record_batch::RecordBatch, arrow::error::ArrowError>>,
    total_rows: usize,
    rows: usize,
    offset: usize,
    order: ScanOrder,
) -> Vec<ScanRow> {
    let Some((start_row, end_row)) = scan_sample_row_bounds(total_rows, rows, offset, order) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    let mut absolute_row = 0usize;

    'outer: for batch_result in reader {
        let batch = match batch_result {
            Ok(b) => b,
            Err(e) => {
                eprintln!("  error reading batch: {e}");
                break;
            }
        };
        let batch_end_row = absolute_row.saturating_add(batch.num_rows());
        if batch_end_row < start_row {
            absolute_row = batch_end_row;
            continue;
        }
        for row_idx in 0..batch.num_rows() {
            absolute_row += 1;
            if absolute_row < start_row {
                continue;
            }
            if absolute_row > end_row {
                break 'outer;
            }
            out.push(ScanRow {
                row_number: absolute_row,
                cells: schema
                    .fields()
                    .iter()
                    .enumerate()
                    .map(|(col_idx, field)| ScanRowCell {
                        name: field.name().clone(),
                        value: format_array_value(batch.column(col_idx).as_ref(), row_idx),
                    })
                    .collect(),
            });
        }
    }

    if matches!(order, ScanOrder::Desc) {
        out.reverse();
    }

    out
}

pub(in crate::cli) fn scan_total_rows_for_sampling(total_rows: i64) -> anyhow::Result<usize> {
    usize::try_from(total_rows).map_err(|_| {
        anyhow::anyhow!(
            "parquet row count {total_rows} exceeds supported scan preview size on this platform"
        )
    })
}

pub(in crate::cli) fn update_scan_progress(
    remaining_rows: &mut usize,
    remaining_offset: &mut usize,
    total_rows: i64,
    sampled_rows: usize,
    requested_rows: usize,
    schema_only: bool,
) -> anyhow::Result<()> {
    if schema_only || requested_rows == 0 {
        return Ok(());
    }

    *remaining_offset = remaining_offset.saturating_sub(scan_total_rows_for_sampling(total_rows)?);
    *remaining_rows = remaining_rows.saturating_sub(sampled_rows);
    Ok(())
}

/// Compute the 1-based inclusive absolute row bounds to sample for `scan`.
///
/// `offset` and `rows` are interpreted relative to the requested display
/// `order`: ascending starts from the beginning of the file, while descending
/// starts from the end of the file. Returns `None` when the requested window
/// falls outside the available rows or when there are no rows to display.
pub(in crate::cli) fn scan_sample_row_bounds(
    total_rows: usize,
    rows: usize,
    offset: usize,
    order: ScanOrder,
) -> Option<(usize, usize)> {
    if total_rows == 0 || rows == 0 || offset >= total_rows {
        return None;
    }

    match order {
        ScanOrder::Asc => {
            let start_row = offset.saturating_add(1);
            let end_row = total_rows.min(offset.saturating_add(rows));
            (start_row <= end_row).then_some((start_row, end_row))
        }
        ScanOrder::Desc => {
            let end_row = total_rows.saturating_sub(offset);
            // For descending order, start from the last visible row and walk
            // backward `rows - 1` positions, clamping to the first row.
            let start_row = end_row.saturating_sub(rows.saturating_sub(1)).max(1);
            (start_row <= end_row).then_some((start_row, end_row))
        }
    }
}

pub(in crate::cli) fn render_scan_results(
    files: &[ScanFileResult],
    row_mode: ScanRowDisplayMode,
    show_rows: bool,
) {
    for file in files {
        println!("\n{}", "═".repeat(72));
        println!("  {}", file.path);
        println!("{}", "─".repeat(72));
        println!(
            "  rows: {}  row_groups: {}  columns: {}  size: {}",
            file.total_rows, file.row_groups, file.columns, file.size_human
        );
        println!("{}", "─".repeat(72));

        for field in &file.schema {
            println!(
                "  {:30} {:20} {}",
                field.name,
                field.data_type,
                if field.nullable {
                    "nullable"
                } else {
                    "not null"
                }
            );
        }

        if show_rows {
            render_scan_rows(file, row_mode);
        }
    }

    if files.len() > 1 {
        println!("\n{}", "═".repeat(72));
        println!("  {} parquet files scanned", files.len());
    }
}

pub(in crate::cli) fn render_scan_rows(file: &ScanFileResult, row_mode: ScanRowDisplayMode) {
    if file.sample_rows.is_empty() {
        println!("\n  (empty)");
        return;
    }

    let rendered = match row_mode {
        ScanRowDisplayMode::Table => format_scan_rows_table(file),
        ScanRowDisplayMode::Vertical => format_scan_rows_vertical(file),
    };
    println!("\n{rendered}");

    let shown = file.sample_rows.len();
    if file.total_rows > shown as i64 {
        println!("\n  {shown} rows shown of {} total.", file.total_rows);
    } else {
        println!("\n  {shown} rows in set.");
    }
}

pub(in crate::cli) fn format_scan_rows_vertical(file: &ScanFileResult) -> String {
    let max_name_len = file
        .schema
        .iter()
        .map(|field| field.name.len())
        .max()
        .unwrap_or(0);
    let mut out = String::new();

    for row in &file.sample_rows {
        if !out.is_empty() {
            out.push('\n');
        }
        out.push_str(&format!("Row {}:\n", row.row_number));
        out.push_str("──────");
        for cell in &row.cells {
            out.push('\n');
            out.push_str(&format!(
                "  {:width$}  {}",
                cell.name,
                cell.value,
                width = max_name_len
            ));
        }
        out.push('\n');
    }

    out.trim_end().to_string()
}

pub(in crate::cli) fn format_scan_rows_table(file: &ScanFileResult) -> String {
    let headers = file
        .schema
        .iter()
        .map(|field| field.name.clone())
        .collect::<Vec<_>>();
    let mut widths = headers
        .iter()
        .map(|header| header.chars().count())
        .collect::<Vec<_>>();
    for row in &file.sample_rows {
        for (index, cell) in row.cells.iter().enumerate() {
            widths[index] = widths[index].max(cell.value.chars().count());
        }
    }

    let row_number_width = file
        .sample_rows
        .last()
        .map(|row| row.row_number.to_string().len())
        .unwrap_or(1);
    let table_indent = " ".repeat(row_number_width + 2);
    let mut out = String::new();

    out.push_str(&scan_table_border('┌', '┬', '┐', &widths, &table_indent));
    out.push('\n');
    out.push_str(&table_indent);
    out.push_str(&scan_table_row(
        &headers.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
        &widths,
    ));
    out.push('\n');
    out.push_str(&scan_table_border('├', '┼', '┤', &widths, &table_indent));

    for row in &file.sample_rows {
        out.push('\n');
        out.push_str(&format!(
            "{:>width$}. {}",
            row.row_number,
            scan_table_row(
                &row.cells
                    .iter()
                    .map(|cell| cell.value.as_str())
                    .collect::<Vec<_>>(),
                &widths
            ),
            width = row_number_width
        ));
    }

    out.push('\n');
    out.push_str(&scan_table_border('└', '┴', '┘', &widths, &table_indent));
    out
}

pub(in crate::cli) fn scan_table_border(
    left: char,
    middle: char,
    right: char,
    widths: &[usize],
    indent: &str,
) -> String {
    let mut out = indent.to_string();
    out.push(left);
    for (index, width) in widths.iter().enumerate() {
        if index > 0 {
            out.push(middle);
        }
        out.push_str(&"─".repeat(*width + 2));
    }
    out.push(right);
    out
}

pub(in crate::cli) fn scan_table_row(values: &[&str], widths: &[usize]) -> String {
    let mut out = String::from("│");
    for (index, value) in values.iter().enumerate() {
        if index > 0 {
            out.push('│');
        }
        out.push(' ');
        out.push_str(value);
        let padding = widths[index].saturating_sub(value.chars().count());
        out.push_str(&" ".repeat(padding + 1));
    }
    out.push('│');
    out
}

/// Format a single cell value from an Arrow array for vertical display.
pub(in crate::cli) fn format_array_value(array: &dyn arrow::array::Array, row: usize) -> String {
    use arrow::array::*;
    use arrow::datatypes::{DataType, TimeUnit};

    if array.is_null(row) {
        return "NULL".to_string();
    }

    match array.data_type() {
        DataType::UInt64 => {
            let v = array
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .value(row);
            format_number_with_hint(v as i128)
        }
        DataType::UInt32 => {
            let v = array
                .as_any()
                .downcast_ref::<UInt32Array>()
                .unwrap()
                .value(row);
            v.to_string()
        }
        DataType::Int64 => {
            let v = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(row);
            format_number_with_hint(v as i128)
        }
        DataType::Int32 => {
            let v = array
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(row);
            v.to_string()
        }
        DataType::Float64 => {
            let v = array
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(row);
            format!("{v}")
        }
        DataType::Boolean => {
            let v = array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap()
                .value(row);
            v.to_string()
        }
        DataType::Utf8 => {
            let v = array
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(row);
            truncate_str(v, 80)
        }
        DataType::LargeUtf8 => {
            let v = array
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .unwrap()
                .value(row);
            truncate_str(v, 80)
        }
        DataType::Binary => {
            let v = array
                .as_any()
                .downcast_ref::<BinaryArray>()
                .unwrap()
                .value(row);
            truncate_str(&format!("0x{}", hex::encode(v)), 80)
        }
        DataType::LargeBinary => {
            let v = array
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .unwrap()
                .value(row);
            truncate_str(&format!("0x{}", hex::encode(v)), 80)
        }
        DataType::Timestamp(unit, timezone) => {
            let timezone_label = timezone.as_deref().filter(|tz| !tz.is_empty());
            match unit {
                TimeUnit::Second => format_timestamp_value(
                    array
                        .as_any()
                        .downcast_ref::<TimestampSecondArray>()
                        .unwrap()
                        .value(row),
                    0,
                    timezone_label,
                    1_000_000_000,
                ),
                TimeUnit::Millisecond => format_timestamp_value(
                    array
                        .as_any()
                        .downcast_ref::<TimestampMillisecondArray>()
                        .unwrap()
                        .value(row),
                    3,
                    timezone_label,
                    1_000_000,
                ),
                TimeUnit::Microsecond => format_timestamp_value(
                    array
                        .as_any()
                        .downcast_ref::<TimestampMicrosecondArray>()
                        .unwrap()
                        .value(row),
                    6,
                    timezone_label,
                    1_000,
                ),
                TimeUnit::Nanosecond => format_timestamp_value(
                    array
                        .as_any()
                        .downcast_ref::<TimestampNanosecondArray>()
                        .unwrap()
                        .value(row),
                    9,
                    timezone_label,
                    1,
                ),
            }
        }
        DataType::List(_) => {
            let list = array.as_any().downcast_ref::<ListArray>().unwrap();
            let inner = list.value(row);
            let items = format_list_items(inner.as_ref(), 5);
            let total = inner.len();
            if total > 5 {
                format!("[{items} ...] ({total} items)")
            } else {
                format!("[{items}]")
            }
        }
        DataType::LargeList(_) => {
            let list = array.as_any().downcast_ref::<LargeListArray>().unwrap();
            let inner = list.value(row);
            let items = format_list_items(inner.as_ref(), 5);
            let total = inner.len();
            if total > 5 {
                format!("[{items} ...] ({total} items)")
            } else {
                format!("[{items}]")
            }
        }
        _ => {
            // Fallback: use Arrow's Display formatting.
            let formatter =
                arrow::util::display::ArrayFormatter::try_new(array, &Default::default());
            match formatter {
                Ok(fmt) => fmt.value(row).to_string(),
                Err(_) => "<unsupported>".to_string(),
            }
        }
    }
}

pub(in crate::cli) fn format_timestamp_value(
    value: i64,
    fractional_digits: usize,
    timezone_label: Option<&str>,
    nanos_per_unit: i64,
) -> String {
    use time::OffsetDateTime;

    let nanos = i128::from(value) * i128::from(nanos_per_unit);
    let dt = match OffsetDateTime::from_unix_timestamp_nanos(nanos) {
        Ok(dt) => dt,
        Err(_) => return format!("{value} (invalid timestamp)"),
    };

    let mut formatted = format!(
        "{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
        dt.year(),
        dt.month() as u8,
        dt.day(),
        dt.hour(),
        dt.minute(),
        dt.second()
    );

    if fractional_digits > 0 {
        let fractional = match fractional_digits {
            3 => dt.nanosecond() / 1_000_000,
            6 => dt.nanosecond() / 1_000,
            9 => dt.nanosecond(),
            _ => 0,
        };
        formatted.push('.');
        formatted.push_str(&format!("{fractional:0width$}", width = fractional_digits));
    }

    if let Some(label) = timezone_label {
        formatted.push(' ');
        formatted.push_str(label);
    }

    formatted
}

/// Format list items (up to `max`) from an inner array.
pub(in crate::cli) fn format_list_items(array: &dyn arrow::array::Array, max: usize) -> String {
    let n = array.len().min(max);
    let items: Vec<String> = (0..n).map(|i| format_array_value(array, i)).collect();
    items.join(", ")
}

/// Truncate long strings and add ellipsis.
pub(in crate::cli) fn truncate_str(s: &str, max_len: usize) -> String {
    if s.len() <= max_len {
        s.to_string()
    } else {
        format!("{}…", &s[..max_len])
    }
}

/// Format a large number with a human-readable hint (e.g. "397234292 -- 397.23M").
pub(in crate::cli) fn format_number_with_hint(v: i128) -> String {
    let abs = v.unsigned_abs();
    let hint = if abs >= 1_000_000_000_000 {
        format!(" -- {:.2}T", v as f64 / 1_000_000_000_000.0)
    } else if abs >= 1_000_000_000 {
        format!(" -- {:.2}B", v as f64 / 1_000_000_000.0)
    } else if abs >= 1_000_000 {
        format!(" -- {:.2}M", v as f64 / 1_000_000.0)
    } else {
        return v.to_string();
    };
    format!("{v}{hint}")
}

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

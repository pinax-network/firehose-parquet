//! Log parsing, percentiles and per-transaction phase attribution from the
//! loopback S3 request log.
use crate::s3::{is_data_part, Entry};
use serde_json::{json, Map, Value};
use std::collections::BTreeMap;

pub fn strip_ansi(text: &str) -> String {
    let mut plain = String::with_capacity(text.len());
    let mut chars = text.chars();
    while let Some(ch) = chars.next() {
        if ch == '\u{1b}' {
            for next in chars.by_ref() {
                if next.is_ascii_alphabetic() {
                    break;
                }
            }
        } else {
            plain.push(ch);
        }
    }
    plain
}

/// Seconds since the Unix epoch of a `YYYY-MM-DDTHH:MM:SS.ffffffZ` log prefix.
pub fn log_seconds(line: &str) -> Option<f64> {
    let stamp = line.split_whitespace().next()?;
    let (date, time) = stamp.strip_suffix('Z')?.split_once('T')?;
    let mut date = date.split('-').map(|part| part.parse::<i64>());
    let (year, month, day) = (date.next()?.ok()?, date.next()?.ok()?, date.next()?.ok()?);
    let mut clock = time.split(':');
    let hours: f64 = clock.next()?.parse().ok()?;
    let minutes: f64 = clock.next()?.parse().ok()?;
    let seconds: f64 = clock.next()?.parse().ok()?;
    // Days from civil (Howard Hinnant), proleptic Gregorian.
    let year = if month <= 2 { year - 1 } else { year };
    let era = year.div_euclid(400);
    let yoe = year - era * 400;
    let month_index = (month + 9) % 12;
    let doy = (153 * month_index + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146_097 + doe - 719_468;
    Some(days as f64 * 86_400.0 + hours * 3_600.0 + minutes * 60.0 + seconds)
}

/// `key=value` fields of a tracing line; quoted values may contain spaces.
pub fn fields(line: &str) -> BTreeMap<String, String> {
    let mut fields = BTreeMap::new();
    let mut rest = line;
    while let Some(eq) = rest.find('=') {
        let key_start = rest[..eq].rfind(' ').map_or(0, |index| index + 1);
        let key = rest[key_start..eq].to_string();
        let after = &rest[eq + 1..];
        let (value, next) = if let Some(quoted) = after.strip_prefix('"') {
            let end = quoted.find('"').unwrap_or(quoted.len());
            (
                quoted[..end].to_string(),
                &quoted[(end + 1).min(quoted.len())..],
            )
        } else {
            let end = after.find(' ').unwrap_or(after.len());
            (after[..end].to_string(), &after[end..])
        };
        fields.insert(key, value);
        rest = next;
    }
    fields
}

#[derive(Clone, Debug)]
pub struct Flush {
    /// "mapper flush emitted record batches": the callback starts its commit.
    pub emitted: f64,
    /// "committed flush size observation": the callback resumes.
    pub committed: f64,
    pub trigger: String,
    pub commit_ms: f64,
    pub files: u64,
    pub rows: u64,
    pub largest_file_bytes: u64,
    pub peak_publications: u64,
    pub peak_encoders: u64,
}

pub struct ParsedLog {
    pub flushes: Vec<Flush>,
    pub errors: Vec<String>,
    pub started: Option<f64>,
}

pub fn parse_log(text: &str) -> ParsedLog {
    let mut flushes = Vec::new();
    let mut errors = Vec::new();
    let mut started = None;
    let mut open: Option<(f64, u64)> = None;
    for line in text.lines() {
        let Some(time) = log_seconds(line) else {
            continue;
        };
        if line.contains("fireparq starting") {
            started = Some(time);
        } else if line.contains("mapper flush emitted record batches") {
            let rows = fields(line)
                .get("rows")
                .and_then(|value| value.parse().ok())
                .unwrap_or(0);
            open = Some((time, rows));
        } else if line.contains("committed flush size observation") {
            let fields = fields(line);
            let number = |name: &str| {
                fields
                    .get(name)
                    .and_then(|value| value.parse::<f64>().ok())
                    .unwrap_or(0.0)
            };
            let (emitted, rows) = open.take().unwrap_or((time, 0));
            flushes.push(Flush {
                emitted,
                committed: time,
                trigger: fields.get("trigger").cloned().unwrap_or_default(),
                commit_ms: number("commit_ms"),
                files: number("files") as u64,
                rows,
                largest_file_bytes: number("largest_file_bytes") as u64,
                peak_publications: number("peak_publications") as u64,
                peak_encoders: number("peak_encoders") as u64,
            });
        } else if line.contains(" ERROR ") || line.contains(" WARN ") {
            if errors.len() < 20 {
                errors.push(line.to_string());
            }
        }
    }
    ParsedLog {
        flushes,
        errors,
        started,
    }
}

/// Nearest-rank percentiles plus mean and max, in the input's unit.
pub fn summary(values: &[f64]) -> Value {
    if values.is_empty() {
        return Value::Null;
    }
    let mut sorted = values.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let rank = |q: f64| {
        let index = ((q * sorted.len() as f64).ceil() as usize).clamp(1, sorted.len()) - 1;
        round(sorted[index])
    };
    json!({
        "n": sorted.len(),
        "p50": rank(0.50),
        "p95": rank(0.95),
        "p99": rank(0.99),
        "max": round(*sorted.last().unwrap()),
        "mean": round(sorted.iter().sum::<f64>() / sorted.len() as f64),
    })
}

pub fn round(value: f64) -> f64 {
    (value * 1000.0).round() / 1000.0
}

fn class(entry: &Entry) -> &'static str {
    let key = entry.key.as_str();
    let control = key.ends_with("/.fireparq-ingest/pending.json")
        || key.ends_with("/.fireparq-ingest/state.json");
    match (entry.method.as_str(), key) {
        ("HEAD", _) if is_data_part(key) => "unoccupied_head",
        ("GET", _) if key.ends_with(".fireparq-owner-v1.json") => "owner_read",
        ("GET", _) if control => "control_read",
        ("PUT", _) if key.ends_with("/.fireparq-ingest/state.json") => "authority_put",
        ("PUT", _) if key.ends_with("/.fireparq-ingest/pending.json") => {
            match entry.note.as_deref() {
                Some("tombstone") => "clear_put",
                Some("committed") => "committed_put",
                Some(note) if note.ends_with(":0") => "writing_put",
                _ => "receipts_put",
            }
        }
        ("GET", _) if key.ends_with("_fireparq/cursor.parquet") => "mirror_read",
        ("PUT", _) if key.ends_with("_fireparq/cursor.parquet") => "mirror_put",
        ("PUT", _) if is_data_part(key) => "part_put",
        ("GET", _) if is_data_part(key) && entry.pinned => "part_readback",
        ("GET", _) if is_data_part(key) => "part_final_verify",
        _ => "other",
    }
}

/// A control transition: the reads before its PUT on control keys and the
/// verifying read after it, as one serialized span.
fn transition(entries: &[&Entry], put: usize, floor: f64) -> (f64, f64) {
    let is_control = |entry: &Entry| {
        entry.key.ends_with("/.fireparq-ingest/pending.json")
            || entry.key.ends_with("/.fireparq-ingest/state.json")
    };
    let key = &entries[put].key;
    let mut start = entries[put].start;
    let mut reads = 0;
    for entry in entries[..put].iter().rev() {
        if reads == 2 || entry.start < floor {
            break;
        }
        if entry.method == "GET" && is_control(entry) {
            start = entry.start;
            reads += 1;
        }
    }
    let end = entries[put + 1..]
        .iter()
        .find(|entry| entry.method == "GET" && &entry.key == key)
        .map_or(entries[put].end, |entry| entry.end);
    (start, end)
}

/// Critical-path phases of one committed flush, whose durations sum to the
/// callback's wait (`emitted` to `committed`), plus request counts by class.
pub fn phases(flush: &Flush, log: &[Entry]) -> Option<(BTreeMap<&'static str, f64>, Value)> {
    let entries: Vec<&Entry> = log
        .iter()
        .filter(|entry| {
            entry.start >= flush.emitted - 0.002 && entry.end <= flush.committed + 0.002
        })
        .collect();
    let find = |name: &str| entries.iter().position(|entry| class(entry) == name);
    let heads: Vec<&&Entry> = entries
        .iter()
        .filter(|entry| class(entry) == "unoccupied_head")
        .collect();
    let writing = find("writing_put")?;
    let committed = find("committed_put")?;
    let authority = find("authority_put")?;
    let clear = find("clear_put")?;
    let first_head = heads.first().map_or(entries[writing].start, |e| e.start);
    let (writing_start, writing_end) = transition(&entries, writing, first_head);
    let last_publication = entries
        .iter()
        .filter(|entry| class(entry) == "part_readback")
        .map(|entry| entry.end)
        .fold(writing_end, f64::max);
    let (committed_start, committed_end) = transition(&entries, committed, last_publication);
    let (_, authority_end) = transition(&entries, authority, committed_end);
    let mirror_end = entries
        .iter()
        .filter(|entry| class(entry) == "mirror_read" && entry.start >= authority_end)
        .map(|entry| entry.end)
        .fold(authority_end, f64::max);
    let (_, clear_end) = transition(&entries, clear, mirror_end);
    let boundaries = [
        ("prepare", flush.emitted, first_head),
        ("unoccupied_heads", first_head, writing_start),
        ("writing", writing_start, writing_end),
        ("table_work", writing_end, last_publication),
        ("final_verify", last_publication, committed_start),
        ("committed", committed_start, committed_end),
        ("authority", committed_end, authority_end),
        ("mirror", authority_end, mirror_end),
        ("clear", mirror_end, clear_end),
        ("tail", clear_end, flush.committed),
    ];
    let mut phases = BTreeMap::new();
    for (name, from, to) in boundaries {
        phases.insert(name, ((to - from) * 1000.0).max(0.0));
    }
    // Inside table work: when the first receipt write started (after the
    // first encodes), and how long the part requests themselves took.
    let first_receipt = entries
        .iter()
        .filter(|entry| class(entry) == "receipts_put")
        .map(|entry| entry.start)
        .fold(f64::INFINITY, f64::min);
    if first_receipt.is_finite() {
        // Encoding the first parts, then the two control reads of that write.
        phases.insert(
            "table_work.until_first_receipt_put",
            ((first_receipt - writing_end) * 1000.0).max(0.0),
        );
    }
    let mut counts: BTreeMap<&'static str, u64> = BTreeMap::new();
    let mut durations: BTreeMap<&'static str, Vec<f64>> = BTreeMap::new();
    for entry in &entries {
        let name = class(entry);
        *counts.entry(name).or_default() += 1;
        durations
            .entry(name)
            .or_default()
            .push((entry.end - entry.start) * 1000.0);
    }
    let requests = json!({
        "total": entries.len(),
        "by_class": counts,
        "request_ms_by_class": durations
            .into_iter()
            .map(|(name, values)| (name.to_string(), summary(&values)))
            .collect::<Map<_, _>>(),
    });
    Some((phases, requests))
}

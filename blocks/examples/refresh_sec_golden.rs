//! Rewrite the SEC golden expectations (`tests/fixtures/sec-v013/expected/`)
//! from the Rust mapper, after an intended mapper change:
//!
//! ```sh
//! cargo run -p blocks --example refresh_sec_golden            # rewrite in place
//! cargo run -p blocks --example refresh_sec_golden -- --check # fail if any file would change
//! ```
//!
//! Review the diff in the pull request: the committed expectations were
//! produced by the independent reference prototype (`proto_map.py`, see the
//! fixture README), so every changed value must be explained by the change.
//! The fixture blocks themselves are cut offline; this never touches them.
use anyhow::{ensure, Result};
use blocks::sec::schema::TABLE_NAMES;
use firehose_parquet::encode::EncodeBytes;

#[path = "../tests/sec_fixture/mod.rs"]
mod sec_fixture;

/// `(table, file contents)` for every SEC table, from the Rust mapper.
fn render() -> Vec<(&'static str, String)> {
    let batches = sec_fixture::map(false, EncodeBytes::Hex);
    TABLE_NAMES
        .iter()
        .map(|table| (*table, sec_fixture::table_json(table, &batches[*table])))
        .collect()
}

/// The tables whose committed expectation differs from the Rust mapper.
fn stale(rendered: &[(&'static str, String)]) -> Vec<&'static str> {
    rendered
        .iter()
        .filter(|(table, text)| {
            std::fs::read_to_string(sec_fixture::expected_path(table))
                .ok()
                .as_ref()
                != Some(text)
        })
        .map(|(table, _)| *table)
        .collect()
}

fn main() -> Result<()> {
    let check = std::env::args().skip(1).any(|arg| arg == "--check");
    let rendered = render();
    let stale = stale(&rendered);
    if check {
        ensure!(stale.is_empty(), "stale SEC expectations: {stale:?}");
        println!("all {} SEC expectations are current", rendered.len());
        return Ok(());
    }
    std::fs::create_dir_all(sec_fixture::dir().join("expected"))?;
    for (table, text) in &rendered {
        if stale.contains(table) {
            std::fs::write(sec_fixture::expected_path(table), text)?;
        }
    }
    println!(
        "rewrote {} of {} SEC expectations: {stale:?}",
        stale.len(),
        rendered.len()
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The committed expectations are byte-for-byte what this example writes:
    /// running it without a mapper change leaves the tree unchanged.
    #[test]
    fn committed_expectations_are_current() {
        assert_eq!(stale(&render()), Vec::<&str>::new());
    }
}

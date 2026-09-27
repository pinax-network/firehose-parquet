//! Rewrite the schema reference in `docs/schemas/` from the mapper schemas.
use anyhow::Result;

fn main() -> Result<()> {
    for path in blocks::schema_docs::write_all(&blocks::schema_docs::docs_dir())? {
        println!("wrote {}", path.display());
    }
    Ok(())
}

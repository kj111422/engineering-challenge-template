//! Versioned, atomic stress-run manifest writer.
//!
//! Manifests are written even on failure (partial results are useful
//! evidence) and are finalized atomically: write `*.tmp` then rename. If the
//! process dies before finalization, no updated index is left behind and the
//! partial file never appears as a valid success.
//!
//! One manifest is produced per cell under `measurements/stress/`, and the
//! index writer inserts (or replaces) one entry per cell under `cells`.

use std::path::Path;

use serde_json::{Map, Value, json};

pub const STRESS_MANIFEST_SCHEMA_VERSION: u32 = 1;

/// A host field that is unavailable, rendered with a reason instead of 0.
#[must_use]
pub fn unavailable(reason: &str) -> Value {
    json!({ "value": null, "reason": reason })
}

/// Atomically write a manifest document to `path`.
///
/// Serialization errors propagate: a document that cannot be serialized is
/// never silently replaced by a stub.
pub fn write_atomic(path: &Path, document: &Value) -> std::io::Result<()> {
    let parent = path
        .parent()
        .map(Path::to_path_buf)
        .unwrap_or_else(|| Path::new(".").to_path_buf());
    std::fs::create_dir_all(&parent)?;
    let tmp = parent.join(format!(
        ".{}.tmp",
        path.file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("manifest.json")
    ));
    let text = serde_json::to_string_pretty(document).map_err(|error| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("manifest serialization failed: {error}"),
        )
    })?;
    std::fs::write(&tmp, text)?;
    std::fs::rename(&tmp, path)?;
    Ok(())
}

/// Read and parse the stress index. A missing or empty file starts fresh; a
/// malformed or non-object document is an error, never an empty index.
pub fn read_index(index_path: &Path) -> std::io::Result<Map<String, Value>> {
    match std::fs::read_to_string(index_path) {
        Ok(text) if !text.trim().is_empty() => match serde_json::from_str::<Value>(&text) {
            Ok(Value::Object(object)) => Ok(object),
            Ok(_) => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "stress index `{}` is not a JSON object",
                    index_path.display()
                ),
            )),
            Err(error) => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "stress index `{}` is malformed: {error}",
                    index_path.display()
                ),
            )),
        },
        // Missing or empty index: start fresh.
        _ => Ok(Map::new()),
    }
}

/// Fail-closed index check: an error means "do not write any artifact".
pub fn validate_index(index_path: &Path) -> std::io::Result<()> {
    read_index(index_path).map(|_| ())
}

/// Insert (or replace) one cell in the stress index document.
///
/// Fails closed: an unreadable or malformed existing index is an error, never
/// an opportunity to silently reset the document. Callers must treat the
/// error as "cell cannot produce a trustworthy artifact".
pub fn upsert_cell(index_path: &Path, cell_id: &str, entry: Value) -> std::io::Result<()> {
    let parent = index_path.parent().unwrap_or_else(|| Path::new("."));
    std::fs::create_dir_all(parent)?;

    let mut index: Map<String, Value> = read_index(index_path)?;
    index
        .entry("schema_version".to_owned())
        .or_insert_with(|| json!(STRESS_MANIFEST_SCHEMA_VERSION));
    let mut cells = index
        .get("cells")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    cells.insert(cell_id.to_owned(), entry);
    index.insert("cells".to_owned(), Value::Object(cells));

    let tmp = index_path.with_file_name(format!(".index.{}.json.tmp", std::process::id()));
    std::fs::write(&tmp, serde_json::to_string_pretty(&Value::Object(index))?)?;
    std::fs::rename(&tmp, index_path)?;
    Ok(())
}

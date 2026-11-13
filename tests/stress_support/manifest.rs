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
    let text = serde_json::to_string_pretty(document)
        .unwrap_or_else(|_| "{\"run_status\":\"interrupted\"}".to_owned());
    std::fs::write(&tmp, text)?;
    std::fs::rename(&tmp, path)?;
    Ok(())
}

/// Insert (or replace) one cell in the stress index document.
pub fn upsert_cell(index_path: &Path, cell_id: &str, entry: Value) -> std::io::Result<()> {
    let parent = index_path.parent().unwrap_or_else(|| Path::new("."));
    std::fs::create_dir_all(parent)?;

    let mut index: Map<String, Value> = std::fs::read_to_string(index_path)
        .ok()
        .and_then(|text| serde_json::from_str::<Value>(&text).ok())
        .and_then(|value| value.as_object().cloned())
        .unwrap_or_default();
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

    let tmp = index_path.with_file_name(".index.json.tmp");
    std::fs::write(&tmp, serde_json::to_string_pretty(&Value::Object(index))?)?;
    std::fs::rename(&tmp, index_path)?;
    Ok(())
}

//! Versioned, crash-recoverable stress-run manifest publication.
//!
//! Publication protocol (index first, body last):
//!
//! 1. The index entry is written with `publication_state: "pending"` and the
//!    SHA-256 of the body that will be published.
//! 2. The body is written atomically to `<cell>.json.staged`.
//! 3. The index entry advances to `publication_state: "published"`.
//! 4. The staged body is renamed to `<cell>.json`.
//!
//! A body at its published name therefore implies that the index accepted the
//! entry. A crash between steps leaves a recoverable state: [`recover`]
//! finalizes a pending entry whose staged body matches the recorded hash, and
//! demotes an entry whose body is absent or corrupt. The two files are not
//! claimed to commit atomically; the index is the commit point and the hash
//! is the recovery witness.
//!
//! The index is fail-closed. Only `NotFound` and an empty document mean "no
//! index yet". Permission, directory, malformed-JSON and wrong-schema errors
//! propagate: a cell that cannot read the index must not publish an artifact
//! that overwrites the record of every other cell.

use std::{
    fs, io,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

use engineering_challenge::util::sha256_hex;
use serde_json::{Map, Value, json};

pub const STRESS_MANIFEST_SCHEMA_VERSION: u32 = 1;

pub const PUBLICATION_PENDING: &str = "pending";
pub const PUBLICATION_PUBLISHED: &str = "published";
pub const PUBLICATION_FAILED: &str = "failed";

/// Fields a `success` manifest must carry with a non-null value. The list is
/// the implemented subset of the design's manifest contract; a field is
/// required only after the harness can produce it from evidence.
pub const REQUIRED_FIELDS: &[&str] = &[
    "schema_version",
    "run_id",
    "cell_id",
    "execution_mode",
    "evidence_class",
    "run_status",
    "generator_version",
    "prng_algorithm_version",
    "scenario_version",
    "build_profile",
    "target_triple",
    "configuration_sha256",
    "stress_harness_binary_sha256",
    "collector_binary_sha256",
    "source_commit",
    "host",
    "provenance",
    "scenario",
    "collector",
    "counts",
    "mock_transport",
    "event_ledger",
    "coverage",
    "verify",
    "delivery",
    "performance",
    "disk",
    "incidents",
    "collector.desired_topology",
    "collector.actual_topology",
    "counts.generated_events",
    "counts.applied_events",
    "event_ledger.generated_events",
    "event_ledger.expected_applied",
    "event_ledger.applied_events",
    "event_ledger.reconciled",
    "coverage.expected_set_sha256",
    "coverage.acknowledged_set_sha256",
    "coverage.anchored_set_sha256",
    "verify.verify_valid",
    "verify.completeness_valid",
    "verify.coverage_valid",
    "verify.journal_complete",
    "delivery.anchor_completed",
    "delivery.drain_completed",
    "disk.free_bytes_before_start",
    "disk.bytes_written",
    "incidents.discontinuity_incidents",
    "incidents.unquantifiable_gaps",
];

/// Fields that must be present even when their value is `null`, because the
/// absence of the key is indistinguishable from an unfinished document.
pub const REQUIRED_NULLABLE_FIELDS: &[&str] = &["failure_class"];

static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

/// A host field that is unavailable, rendered with a reason instead of 0.
#[must_use]
pub fn unavailable(reason: &str) -> Value {
    json!({ "value": null, "reason": reason })
}

/// Write a document atomically through a unique temporary sibling.
pub fn write_atomic(path: &Path, document: &Value) -> io::Result<()> {
    let bytes = serde_json::to_vec_pretty(document).map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("manifest serialization failed: {error}"),
        )
    })?;
    write_bytes_atomic(path, &bytes)
}

fn write_bytes_atomic(path: &Path, bytes: &[u8]) -> io::Result<()> {
    let parent = parent_of(path);
    fs::create_dir_all(&parent)?;
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("manifest.json");
    let temp = parent.join(format!(
        ".{file_name}.{}.{}.tmp",
        std::process::id(),
        TEMP_COUNTER.fetch_add(1, Ordering::Relaxed)
    ));
    fs::write(&temp, bytes)?;
    fs::rename(&temp, path)
}

fn parent_of(path: &Path) -> PathBuf {
    path.parent()
        .map(Path::to_path_buf)
        .unwrap_or_else(|| Path::new(".").to_path_buf())
}

/// Read and parse the stress index.
///
/// `NotFound` and an empty file mean "no index yet". Every other I/O error
/// and every malformed or non-object document is an error.
pub fn read_index(index_path: &Path) -> io::Result<Map<String, Value>> {
    let text = match fs::read_to_string(index_path) {
        Ok(text) => text,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Map::new()),
        Err(error) => {
            return Err(io::Error::new(
                error.kind(),
                format!(
                    "cannot read stress index `{}`: {error}",
                    index_path.display()
                ),
            ));
        }
    };
    if text.trim().is_empty() {
        return Ok(Map::new());
    }
    match serde_json::from_str::<Value>(&text) {
        Ok(Value::Object(object)) => Ok(object),
        Ok(_) => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "stress index `{}` is not a JSON object",
                index_path.display()
            ),
        )),
        Err(error) => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "stress index `{}` is malformed: {error}",
                index_path.display()
            ),
        )),
    }
}

/// Insert (or replace) one cell entry in the stress index.
///
/// Fails closed on an unreadable, malformed, wrong-schema or structurally
/// invalid index. Callers treat the error as "this cell cannot publish".
pub fn upsert_cell(index_path: &Path, cell_id: &str, entry: Value) -> io::Result<()> {
    let mut index = read_index(index_path)?;
    match index.get("schema_version") {
        None => {
            index.insert(
                "schema_version".to_owned(),
                json!(STRESS_MANIFEST_SCHEMA_VERSION),
            );
        }
        Some(value) if value.as_u64() == Some(u64::from(STRESS_MANIFEST_SCHEMA_VERSION)) => {}
        Some(value) => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "stress index `{}` has unsupported schema_version {value}",
                    index_path.display()
                ),
            ));
        }
    }
    let mut cells = match index.get("cells") {
        None => Map::new(),
        Some(Value::Object(cells)) => cells.clone(),
        Some(_) => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "stress index `{}` has a non-object `cells` field",
                    index_path.display()
                ),
            ));
        }
    };
    cells.insert(cell_id.to_owned(), entry);
    index.insert("cells".to_owned(), Value::Object(cells));
    write_atomic(index_path, &Value::Object(index))
}

/// Failure of a publication attempt, split so the caller can name the failed
/// stage instead of reporting a generic I/O error.
#[derive(Debug)]
pub enum PublishError {
    Serialize { source: serde_json::Error },
    Index { source: io::Error },
    Body { source: io::Error },
}

impl std::fmt::Display for PublishError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Serialize { source } => {
                write!(formatter, "manifest serialization failed: {source}")
            }
            Self::Index { source } => {
                write!(formatter, "stress index publication failed: {source}")
            }
            Self::Body { source } => {
                write!(formatter, "manifest body publication failed: {source}")
            }
        }
    }
}

impl std::error::Error for PublishError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Serialize { source } => Some(source),
            Self::Index { source } => Some(source),
            Self::Body { source } => Some(source),
        }
    }
}

/// Publish one cell body and its index entry under the protocol in the module
/// documentation. `entry` carries the caller's summary fields; the protocol
/// fields (`path`, `body_sha256`, `publication_state`) are added here.
pub fn publish_cell(
    index_path: &Path,
    cell_id: &str,
    mut entry: Map<String, Value>,
    document: &Value,
) -> Result<(), PublishError> {
    let parent = parent_of(index_path);
    let body_path = parent.join(format!("{cell_id}.json"));
    let staged_path = parent.join(format!("{cell_id}.json.staged"));

    let bytes =
        serde_json::to_vec_pretty(document).map_err(|source| PublishError::Serialize { source })?;
    let body_sha256 = sha256_hex(&bytes);

    entry.insert("path".to_owned(), Value::from(format!("{cell_id}.json")));
    entry.insert("body_sha256".to_owned(), Value::from(body_sha256));
    entry.insert(
        "publication_state".to_owned(),
        Value::from(PUBLICATION_PENDING),
    );

    // Commit point 1: the index accepts the entry before any body exists
    // under a published name.
    upsert_cell(index_path, cell_id, Value::Object(entry.clone()))
        .map_err(|source| PublishError::Index { source })?;

    if let Err(source) = write_bytes_atomic(&staged_path, &bytes) {
        demote(index_path, cell_id, &mut entry, "manifest_write_error");
        return Err(PublishError::Body { source });
    }

    entry.insert(
        "publication_state".to_owned(),
        Value::from(PUBLICATION_PUBLISHED),
    );
    if let Err(source) = upsert_cell(index_path, cell_id, Value::Object(entry)) {
        // The staged body and the pending entry are both intact; `recover`
        // finalizes the publication after the index recovers.
        return Err(PublishError::Index { source });
    }

    fs::rename(&staged_path, &body_path).map_err(|source| PublishError::Body { source })
}

fn demote(index_path: &Path, cell_id: &str, entry: &mut Map<String, Value>, class: &str) {
    entry.insert("run_status".to_owned(), Value::from("failed"));
    entry.insert("failure_class".to_owned(), Value::from(class));
    entry.insert(
        "publication_state".to_owned(),
        Value::from(PUBLICATION_FAILED),
    );
    entry.insert("body_sha256".to_owned(), Value::Null);
    // The original failure is already being returned; the demotion is a
    // best-effort attempt to leave the index in a truthful state.
    let _ = upsert_cell(index_path, cell_id, Value::Object(entry.clone()));
}

/// Outcome of reconciling pending entries with their bodies.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct RecoveryReport {
    pub finalized: Vec<String>,
    pub demoted: Vec<String>,
}

impl RecoveryReport {
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.finalized.is_empty() && self.demoted.is_empty()
    }
}

/// Reconcile every non-published index entry with the body on disk.
///
/// A pending entry is finalized when its staged or final body matches the
/// recorded hash, and demoted otherwise. A published entry whose body is
/// missing or corrupt is demoted: the index must not assert a publication the
/// filesystem cannot support.
pub fn recover(index_path: &Path) -> io::Result<RecoveryReport> {
    if !index_path.exists() {
        return Ok(RecoveryReport::default());
    }
    let parent = parent_of(index_path);
    let mut index = read_index(index_path)?;
    let Some(Value::Object(cells)) = index.get_mut("cells") else {
        return Ok(RecoveryReport::default());
    };

    let mut report = RecoveryReport::default();
    for (cell_id, value) in cells.iter_mut() {
        let Some(entry) = value.as_object_mut() else {
            continue;
        };
        let state = entry
            .get("publication_state")
            .and_then(Value::as_str)
            .unwrap_or(PUBLICATION_FAILED);
        if state == PUBLICATION_FAILED {
            continue;
        }
        let Some(expected_hash) = entry.get("body_sha256").and_then(Value::as_str) else {
            demote_entry(entry, "manifest_body_unverifiable");
            report.demoted.push(cell_id.clone());
            continue;
        };
        let body_path = parent.join(format!("{cell_id}.json"));
        match settle_body(&parent, cell_id, &body_path, expected_hash) {
            Ok(true) if state == PUBLICATION_PUBLISHED => {}
            Ok(true) => {
                entry.insert(
                    "publication_state".to_owned(),
                    Value::from(PUBLICATION_PUBLISHED),
                );
                report.finalized.push(cell_id.clone());
            }
            Ok(false) | Err(_) => {
                demote_entry(entry, "manifest_body_unverifiable");
                report.demoted.push(cell_id.clone());
            }
        }
    }

    if !report.is_empty() {
        write_atomic(index_path, &Value::Object(index))?;
    }
    Ok(report)
}

fn demote_entry(entry: &mut Map<String, Value>, class: &str) {
    entry.insert("run_status".to_owned(), Value::from("failed"));
    entry.insert("failure_class".to_owned(), Value::from(class));
    entry.insert(
        "publication_state".to_owned(),
        Value::from(PUBLICATION_FAILED),
    );
}

/// Move a verified body to its published name. Returns whether the published
/// body now matches the recorded hash.
fn settle_body(
    parent: &Path,
    cell_id: &str,
    body_path: &Path,
    expected_hash: &str,
) -> io::Result<bool> {
    if let Ok(bytes) = fs::read(body_path)
        && sha256_hex(&bytes) == expected_hash
    {
        let staged = parent.join(format!("{cell_id}.json.staged"));
        let _ = fs::remove_file(staged);
        return Ok(true);
    }
    let staged = parent.join(format!("{cell_id}.json.staged"));
    if let Ok(bytes) = fs::read(&staged)
        && sha256_hex(&bytes) == expected_hash
    {
        fs::rename(&staged, body_path)?;
        return Ok(true);
    }
    Ok(false)
}

/// Paths that a success manifest does not carry. Returns dotted paths so the
/// failure reason names the exact field.
#[must_use]
pub fn missing_required_fields(document: &Map<String, Value>) -> Vec<String> {
    let mut missing = Vec::new();
    for path in REQUIRED_FIELDS {
        match resolve(document, path) {
            Some(Value::Null) | None => missing.push((*path).to_owned()),
            Some(_) => {}
        }
    }
    for path in REQUIRED_NULLABLE_FIELDS {
        if resolve(document, path).is_none() {
            missing.push((*path).to_owned());
        }
    }
    missing
}

fn resolve<'a>(document: &'a Map<String, Value>, path: &str) -> Option<&'a Value> {
    let mut parts = path.split('.');
    let mut current = document.get(parts.next()?)?;
    for part in parts {
        current = current.as_object()?.get(part)?;
    }
    Some(current)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn body(status: &str) -> Value {
        json!({ "run_status": status, "cell_id": "cell-a" })
    }

    #[test]
    fn missing_index_reads_as_empty() {
        let directory = tempfile::tempdir().expect("tempdir");
        let index = read_index(&directory.path().join("index.json")).expect("missing is empty");
        assert!(index.is_empty());
    }

    #[test]
    fn empty_index_reads_as_empty() {
        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("index.json");
        fs::write(&path, b"  \n\t").expect("write");
        assert!(read_index(&path).expect("empty is empty").is_empty());
    }

    #[test]
    fn directory_index_is_an_error() {
        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("index.json");
        fs::create_dir(&path).expect("mkdir");
        assert!(
            read_index(&path).is_err(),
            "a directory must not read as an empty index"
        );
    }

    #[test]
    fn malformed_index_is_an_error() {
        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("index.json");
        fs::write(&path, b"not json").expect("write");
        assert!(read_index(&path).is_err());
    }

    #[test]
    fn unsupported_schema_is_an_error() {
        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("index.json");
        fs::write(&path, br#"{"schema_version":99,"cells":{}}"#).expect("write");
        let error = upsert_cell(&path, "cell-a", json!({})).expect_err("schema must be rejected");
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn non_object_cells_is_an_error() {
        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("index.json");
        fs::write(&path, br#"{"schema_version":1,"cells":[]}"#).expect("write");
        assert!(upsert_cell(&path, "cell-a", json!({})).is_err());
    }

    #[test]
    fn publish_is_indexed_with_a_body_hash() {
        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("index.json");
        publish_cell(&path, "cell-a", Map::new(), &body("success")).expect("publish");

        let index = read_index(&path).expect("read");
        let entry = index["cells"]["cell-a"].as_object().expect("entry");
        assert_eq!(
            entry["publication_state"].as_str(),
            Some(PUBLICATION_PUBLISHED)
        );
        let published = fs::read(directory.path().join("cell-a.json")).expect("body");
        assert_eq!(
            entry["body_sha256"].as_str(),
            Some(sha256_hex(&published).as_str())
        );
        assert!(!directory.path().join("cell-a.json.staged").exists());
    }

    #[test]
    fn malformed_index_writes_no_body() {
        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("index.json");
        fs::write(&path, b"not json").expect("write");
        let error = publish_cell(&path, "cell-a", Map::new(), &body("success"))
            .expect_err("index must reject");
        assert!(matches!(error, PublishError::Index { .. }));
        assert!(!directory.path().join("cell-a.json").exists());
        assert!(!directory.path().join("cell-a.json.staged").exists());
    }

    #[test]
    fn recovery_finalizes_a_pending_body() {
        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("index.json");
        let document = body("success");
        let bytes = serde_json::to_vec_pretty(&document).expect("serialize");
        let hash = sha256_hex(&bytes);
        upsert_cell(
            &path,
            "cell-a",
            json!({
                "path": "cell-a.json",
                "body_sha256": hash,
                "publication_state": PUBLICATION_PENDING,
            }),
        )
        .expect("index");
        fs::write(directory.path().join("cell-a.json.staged"), &bytes).expect("staged");

        let report = recover(&path).expect("recover");
        assert_eq!(report.finalized, vec!["cell-a".to_owned()]);
        let index = read_index(&path).expect("read");
        assert_eq!(
            index["cells"]["cell-a"]["publication_state"].as_str(),
            Some(PUBLICATION_PUBLISHED)
        );
        assert!(directory.path().join("cell-a.json").exists());
    }

    #[test]
    fn recovery_demotes_a_missing_body() {
        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("index.json");
        upsert_cell(
            &path,
            "cell-a",
            json!({
                "path": "cell-a.json",
                "body_sha256": "00",
                "publication_state": PUBLICATION_PENDING,
            }),
        )
        .expect("index");

        let report = recover(&path).expect("recover");
        assert_eq!(report.demoted, vec!["cell-a".to_owned()]);
        let index = read_index(&path).expect("read");
        assert_eq!(
            index["cells"]["cell-a"]["publication_state"].as_str(),
            Some(PUBLICATION_FAILED)
        );
    }

    #[test]
    fn recovery_demotes_a_hash_mismatch() {
        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("index.json");
        fs::write(directory.path().join("cell-a.json"), b"different").expect("body");
        upsert_cell(
            &path,
            "cell-a",
            json!({
                "path": "cell-a.json",
                "body_sha256": "00",
                "publication_state": PUBLICATION_PUBLISHED,
            }),
        )
        .expect("index");

        let report = recover(&path).expect("recover");
        assert_eq!(report.demoted, vec!["cell-a".to_owned()]);
    }

    #[test]
    fn missing_required_fields_names_every_gap() {
        let missing = missing_required_fields(&Map::new());
        assert_eq!(
            missing.len(),
            REQUIRED_FIELDS.len() + REQUIRED_NULLABLE_FIELDS.len()
        );
        assert!(missing.contains(&"incidents.discontinuity_incidents".to_owned()));
        assert!(missing.contains(&"failure_class".to_owned()));
    }

    #[test]
    fn a_null_field_counts_as_missing() {
        let mut document = Map::new();
        document.insert("run_status".to_owned(), Value::Null);
        assert!(
            missing_required_fields(&document).contains(&"run_status".to_owned()),
            "null is not evidence"
        );
    }
}

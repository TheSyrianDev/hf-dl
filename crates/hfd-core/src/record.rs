//! Log of completed downloads (`downloads.json`), for tools that keep a
//! model list. Enabled by `DownloadOptions::download_log`.

use std::fs;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::{DownloadedFile, RepoRef, parse_quant};
use serde_json::{Value, json};

/// Appends (or replaces, keyed by absolute path) one entry. Errors are
/// returned so callers can warn; they never fail the download.
pub fn record_download(
    log: &Path,
    repo: &RepoRef,
    revision: &str,
    file: &DownloadedFile,
) -> crate::Result<()> {
    append(log, repo, revision, file)
        .map_err(|error| crate::Error::Invalid(format!("download log {}: {error}", log.display())))
}

/// One entry of the download log, as read back by [`list_installed`].
#[derive(Debug, Clone, PartialEq)]
pub struct InstalledModel {
    pub repo: String,
    pub file: String,
    pub quant: Option<String>,
    pub size_bytes: u64,
    pub sha256: Option<String>,
    pub path: String,
    pub revision: String,
    pub downloaded_at: u64,
}

/// Lists every entry in the log. A missing or unreadable log is an empty list.
pub fn list_installed(log: &Path) -> Vec<InstalledModel> {
    let Some(doc) = read_doc(log) else {
        return Vec::new();
    };
    doc["downloads"]
        .as_array()
        .map(|list| list.iter().filter_map(parse_entry).collect())
        .unwrap_or_default()
}

/// Removes the entry whose path matches `path` (the absolute path recorded in
/// the log). When `delete_file` is true the file is also deleted. Returns
/// whether an entry was removed.
pub fn remove_installed(log: &Path, path: &Path, delete_file: bool) -> crate::Result<bool> {
    let key = fs::canonicalize(path)
        .unwrap_or_else(|_| path.to_path_buf())
        .to_string_lossy()
        .to_string();
    let mut doc = read_doc(log).unwrap_or_else(|| json!({ "schema": 1, "downloads": [] }));
    let Some(list) = doc["downloads"].as_array_mut() else {
        return Ok(false);
    };
    let before = list.len();
    list.retain(|entry| entry["path"].as_str() != Some(key.as_str()));
    let removed = list.len() != before;
    if removed {
        write_doc(log, &doc).map_err(|error| {
            crate::Error::Invalid(format!("download log {}: {error}", log.display()))
        })?;
    }
    if delete_file {
        match fs::remove_file(&key) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(crate::Error::Io(error)),
        }
    }
    Ok(removed)
}

fn read_doc(log: &Path) -> Option<Value> {
    fs::read(log)
        .ok()
        .and_then(|bytes| serde_json::from_slice(&bytes).ok())
}

fn parse_entry(entry: &Value) -> Option<InstalledModel> {
    Some(InstalledModel {
        repo: entry["repo"].as_str()?.to_string(),
        file: entry["file"].as_str()?.to_string(),
        quant: entry["quant"].as_str().map(str::to_string),
        size_bytes: entry["size_bytes"].as_u64()?,
        sha256: entry["sha256"].as_str().map(str::to_string),
        path: entry["path"].as_str()?.to_string(),
        revision: entry["revision"].as_str().unwrap_or("main").to_string(),
        downloaded_at: entry["downloaded_at"].as_u64().unwrap_or(0),
    })
}

fn write_doc(log: &Path, doc: &Value) -> std::result::Result<(), Box<dyn std::error::Error>> {
    if let Some(parent) = log.parent() {
        fs::create_dir_all(parent)?;
    }
    let tmp = log.with_extension(format!("tmp.{}", std::process::id()));
    fs::write(&tmp, serde_json::to_vec_pretty(doc)?)?;
    fs::rename(&tmp, log)?;
    Ok(())
}

fn append(
    log: &Path,
    repo: &RepoRef,
    revision: &str,
    file: &DownloadedFile,
) -> std::result::Result<(), Box<dyn std::error::Error>> {
    let path = fs::canonicalize(&file.local_path).unwrap_or_else(|_| file.local_path.clone());
    let path = path.to_string_lossy().to_string();
    let mut doc: Value = fs::read(log)
        .ok()
        .and_then(|bytes| serde_json::from_slice(&bytes).ok())
        .unwrap_or_else(|| json!({ "schema": 1, "downloads": [] }));
    if !doc["downloads"].is_array() {
        doc["downloads"] = json!([]);
    }
    let list = doc["downloads"].as_array_mut().unwrap();
    list.retain(|entry| entry["path"].as_str() != Some(path.as_str()));
    list.push(json!({
        "repo": repo.repo_id,
        "file": file.path,
        "quant": parse_quant(&file.path).map(|quant| quant.name),
        "size_bytes": file.size,
        "sha256": file.sha256,
        "path": path,
        "revision": revision,
        "downloaded_at": SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0),
    }));
    write_doc(log, &doc)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RepoType;

    #[test]
    fn appends_and_replaces_by_path() {
        let dir = tempfile::tempdir().unwrap();
        let log = dir.path().join("downloads.json");
        let model = dir.path().join("m-Q4_K_M.gguf");
        fs::write(&model, b"x").unwrap();
        let file = DownloadedFile {
            path: "m-Q4_K_M.gguf".into(),
            local_path: model,
            size: 1,
            skipped: false,
            resumed_bytes: 0,
            engine: "http",
            sha256: Some("ab".into()),
        };
        let repo = RepoRef::new(RepoType::Model, "o/r");
        record_download(&log, &repo, "main", &file).unwrap();
        record_download(&log, &repo, "main", &file).unwrap();
        let doc: Value = serde_json::from_slice(&fs::read(&log).unwrap()).unwrap();
        let list = doc["downloads"].as_array().unwrap();
        assert_eq!(list.len(), 1);
        assert_eq!(list[0]["quant"], "Q4_K_M");
        assert_eq!(list[0]["sha256"], "ab");
    }

    #[test]
    fn lists_and_removes_installed_entries() {
        let dir = tempfile::tempdir().unwrap();
        let log = dir.path().join("downloads.json");
        let model = dir.path().join("m-Q4_K_M.gguf");
        fs::write(&model, b"x").unwrap();
        let file = DownloadedFile {
            path: "m-Q4_K_M.gguf".into(),
            local_path: model.clone(),
            size: 1,
            skipped: false,
            resumed_bytes: 0,
            engine: "http",
            sha256: None,
        };
        let repo = RepoRef::new(RepoType::Model, "o/r");
        record_download(&log, &repo, "main", &file).unwrap();
        let installed = list_installed(&log);
        assert_eq!(installed.len(), 1);
        assert_eq!(installed[0].repo, "o/r");
        assert_eq!(installed[0].quant.as_deref(), Some("Q4_K_M"));
        assert!(list_installed(&dir.path().join("missing.json")).is_empty());

        assert!(remove_installed(&log, &model, true).unwrap());
        assert!(!model.exists());
        assert!(list_installed(&log).is_empty());
        assert!(!remove_installed(&log, &model, false).unwrap());
    }
}

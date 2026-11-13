//! Optional Pirate Face source. Metadata lookup, file selection and sha256
//! checks live here. Byte transfer sits behind [`TorrentBackend`]; the default
//! [`NoTorrent`] reports `Unavailable`, so nothing is fetched until a backend is
//! supplied. See docs/pirateface-plan.md.

use std::path::{Path, PathBuf};

use serde::Deserialize;
use sha2::{Digest, Sha256};

use crate::error::{Error, Result};

pub const DEFAULT_API: &str = "https://pirateface.co";

/// Where the bytes come from. Chosen explicitly; there is no silent fallback.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Source {
    #[default]
    HuggingFace,
    PirateFace,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Entry {
    pub repo_id: String,
    pub revision: String,
    /// False when the entry was published without an HF commit ("pf_only").
    #[serde(default = "default_true")]
    pub hf: bool,
    pub files: Vec<FileEntry>,
    #[serde(default)]
    pub torrent: Option<String>,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct FileEntry {
    pub path: String,
    pub size: u64,
    pub sha256: String,
}

fn default_true() -> bool {
    true
}

/// Parse a `GET /api/torrents?repo=` body. Returns `Ok(None)` for "not listed".
pub fn parse_entry(json: &str) -> Result<Option<Entry>> {
    let entry: Entry = serde_json::from_str(json)?;
    if entry.files.is_empty() {
        return Err(Error::Invalid("Pirate Face entry lists no files".into()));
    }
    for f in &entry.files {
        if f.sha256.len() != 64 || !f.sha256.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err(Error::Invalid(format!(
                "Pirate Face entry has a bad sha256 for {}",
                f.path
            )));
        }
        if f.path.starts_with('/') || f.path.split('/').any(|p| p == "..") {
            return Err(Error::Invalid(format!("unsafe path in entry: {}", f.path)));
        }
    }
    Ok(Some(entry))
}

/// Keep only the files whose path matches one of `wanted` (exact path or prefix ending in '/').
/// Empty `wanted` keeps everything.
pub fn select_files<'a>(entry: &'a Entry, wanted: &[String]) -> Vec<&'a FileEntry> {
    entry
        .files
        .iter()
        .filter(|f| {
            wanted.is_empty()
                || wanted
                    .iter()
                    .any(|w| f.path == *w || (w.ends_with('/') && f.path.starts_with(w.as_str())))
        })
        .collect()
}

/// Lowercase hex sha256 of a file.
pub fn sha256_file(path: &Path) -> Result<String> {
    let mut file = std::fs::File::open(path)?;
    let mut hasher = Sha256::new();
    std::io::copy(&mut file, &mut hasher)?;
    Ok(format!("{:x}", hasher.finalize()))
}

/// Check a downloaded file against the entry. On mismatch the file is deleted
/// and an error is returned, so a bad copy is never kept.
pub fn verify_file(path: &Path, expected: &FileEntry) -> Result<()> {
    let actual = sha256_file(path)?;
    if actual.eq_ignore_ascii_case(&expected.sha256) {
        Ok(())
    } else {
        let _ = std::fs::remove_file(path);
        Err(Error::Invalid(format!(
            "sha256 mismatch for {}: expected {}, got {actual}; file removed",
            expected.path, expected.sha256
        )))
    }
}

/// Byte transfer for a Pirate Face entry. Implementations write each selected
/// file under `dest` and return the paths. Verification is done by the caller.
pub trait TorrentBackend {
    fn fetch(&self, entry: &Entry, files: &[&FileEntry], dest: &Path) -> Result<Vec<PathBuf>>;
}

/// Default backend: no torrent engine is bundled, so every fetch is unavailable.
pub struct NoTorrent;

impl TorrentBackend for NoTorrent {
    fn fetch(&self, _: &Entry, _: &[&FileEntry], _: &Path) -> Result<Vec<PathBuf>> {
        Err(Error::Invalid(
            "Pirate Face transfer unavailable: no torrent engine configured".into(),
        ))
    }
}

/// Fetch, then verify every selected file. Returns the verified paths.
pub fn fetch_verified(
    backend: &dyn TorrentBackend,
    entry: &Entry,
    wanted: &[String],
    dest: &Path,
) -> Result<Vec<PathBuf>> {
    let files = select_files(entry, wanted);
    if files.is_empty() {
        return Err(Error::Invalid("no files matched the request".into()));
    }
    let paths = backend.fetch(entry, &files, dest)?;
    for file in &files {
        let local = dest.join(&file.path);
        if !paths.contains(&local) {
            return Err(Error::Invalid(format!(
                "backend did not return {}",
                file.path
            )));
        }
        verify_file(&local, file)?;
    }
    Ok(paths)
}

/// Torrent backend that shells out to a user-installed `aria2c`. Optional:
/// when the binary is missing the fetch reports unavailable, so the caller can
/// fall back to Hugging Face. Nothing is bundled and nothing is seeded.
pub struct AriaTorrent {
    pub bin: PathBuf,
}

impl Default for AriaTorrent {
    fn default() -> Self {
        Self {
            bin: PathBuf::from("aria2c"),
        }
    }
}

/// Builds the aria2c argument list. Pure, so it can be unit-tested. Seeding is
/// switched off (no seed time, upload capped) and the torrent is fetched into `dest`.
pub fn aria_args(torrent_url: &str, dest: &Path) -> Vec<String> {
    vec![
        "--dir".into(),
        dest.display().to_string(),
        "--seed-time=0".into(),
        "--max-upload-limit=1K".into(),
        "--bt-enable-lpd=false".into(),
        "--allow-overwrite=true".into(),
        "--console-log-level=warn".into(),
        "--summary-interval=0".into(),
        torrent_url.into(),
    ]
}

impl TorrentBackend for AriaTorrent {
    fn fetch(&self, entry: &Entry, files: &[&FileEntry], dest: &Path) -> Result<Vec<PathBuf>> {
        let torrent = entry
            .torrent
            .as_deref()
            .ok_or_else(|| Error::Invalid("entry has no torrent to fetch".into()))?;
        let status = match std::process::Command::new(&self.bin)
            .args(aria_args(torrent, dest))
            .status()
        {
            Ok(s) => s,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Err(Error::Invalid(
                    "Pirate Face transfer unavailable: aria2c not installed".into(),
                ));
            }
            Err(e) => return Err(e.into()),
        };
        if !status.success() {
            return Err(Error::Invalid(format!("aria2c exited with {status}")));
        }
        Ok(files.iter().map(|f| dest.join(&f.path)).collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const OK: &str = r#"{"repoId":"a/b","revision":"abc","hf":true,
        "files":[{"path":"m.gguf","size":3,"sha256":"ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"},
                 {"path":"sub/x.json","size":0,"sha256":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"}]}"#;

    #[test]
    fn parses_listed_entry() {
        let e = parse_entry(OK).unwrap().unwrap();
        assert!(e.hf);
        assert_eq!(e.files.len(), 2);
    }

    #[test]
    fn rejects_bad_hash_and_unsafe_paths() {
        let bad = OK.replace("ba7816bf", "zz7816bf");
        assert!(parse_entry(&bad).is_err());
        let evil = OK.replace("\"m.gguf\"", "\"../m.gguf\"");
        assert!(parse_entry(&evil).is_err());
        assert!(parse_entry("{not json").is_err());
    }

    #[test]
    fn selects_by_exact_path_or_directory_prefix() {
        let e = parse_entry(OK).unwrap().unwrap();
        assert_eq!(select_files(&e, &[]).len(), 2);
        assert_eq!(select_files(&e, &["m.gguf".into()]).len(), 1);
        assert_eq!(select_files(&e, &["sub/".into()])[0].path, "sub/x.json");
        assert!(select_files(&e, &["m".into()]).is_empty());
    }

    #[test]
    fn default_backend_is_unavailable() {
        let e = parse_entry(OK).unwrap().unwrap();
        let dir = tempfile::tempdir().unwrap();
        assert!(fetch_verified(&NoTorrent, &e, &[], dir.path()).is_err());
    }

    struct Fake(Vec<(String, Vec<u8>)>);
    impl TorrentBackend for Fake {
        fn fetch(&self, _: &Entry, _: &[&FileEntry], dest: &Path) -> Result<Vec<PathBuf>> {
            let mut out = Vec::new();
            for (p, bytes) in &self.0 {
                let local = dest.join(p);
                std::fs::create_dir_all(local.parent().unwrap())?;
                std::fs::write(&local, bytes)?;
                out.push(local);
            }
            Ok(out)
        }
    }

    #[test]
    fn verified_fetch_keeps_good_files_and_removes_bad_ones() {
        let e = parse_entry(OK).unwrap().unwrap();
        let dir = tempfile::tempdir().unwrap();
        // m.gguf content "abc" matches the fixture hash; the json file does not.
        let good = Fake(vec![
            ("m.gguf".into(), b"abc".to_vec()),
            ("sub/x.json".into(), b"oops".to_vec()),
        ]);
        assert!(fetch_verified(&good, &e, &[], dir.path()).is_err());
        assert!(!dir.path().join("sub/x.json").exists());

        let dir2 = tempfile::tempdir().unwrap();
        let ok = Fake(vec![
            ("m.gguf".into(), b"abc".to_vec()),
            ("sub/x.json".into(), b"".to_vec()),
        ]);
        let paths = fetch_verified(&ok, &e, &[], dir2.path()).unwrap();
        assert_eq!(paths.len(), 2);
    }
}

#[cfg(test)]
mod aria_tests {
    use super::*;

    fn entry() -> Entry {
        Entry {
            repo_id: "a/b".into(),
            revision: "main".into(),
            hf: true,
            files: vec![],
            torrent: Some("https://pirateface.co/t/a.torrent".into()),
        }
    }

    #[test]
    fn args_disable_seeding_and_fetch_into_dest() {
        let a = aria_args("https://x/t.torrent", Path::new("/d"));
        assert!(a.contains(&"--seed-time=0".to_string()));
        assert!(a.contains(&"--dir".to_string()) && a.contains(&"/d".to_string()));
        assert_eq!(a.last().unwrap(), "https://x/t.torrent");
    }

    #[test]
    fn missing_binary_reports_unavailable() {
        let b = AriaTorrent {
            bin: "/nonexistent/aria2c-stub".into(),
        };
        let err = b.fetch(&entry(), &[], Path::new("/tmp")).unwrap_err();
        assert!(err.to_string().contains("unavailable"));
    }

    #[test]
    #[cfg(unix)] // the stub is a /bin/sh script
    fn stub_success_returns_expected_paths() {
        let dir = tempfile::tempdir().unwrap();
        let stub = dir.path().join("aria2c");
        std::fs::write(&stub, "#!/bin/sh\nexit 0\n").unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&stub, std::fs::Permissions::from_mode(0o755)).unwrap();
        }
        let f = FileEntry {
            path: "m.gguf".into(),
            size: 3,
            sha256: String::new(),
        };
        let b = AriaTorrent { bin: stub };
        let got = b.fetch(&entry(), &[&f], dir.path()).unwrap();
        assert_eq!(got, vec![dir.path().join("m.gguf")]);
    }

    #[test]
    #[cfg(unix)] // the stub is a /bin/sh script
    fn stub_failure_is_an_error() {
        let dir = tempfile::tempdir().unwrap();
        let stub = dir.path().join("aria2c");
        std::fs::write(&stub, "#!/bin/sh\nexit 3\n").unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&stub, std::fs::Permissions::from_mode(0o755)).unwrap();
        }
        let b = AriaTorrent { bin: stub };
        assert!(b.fetch(&entry(), &[], dir.path()).is_err());
    }
}

/// URL for the `GET /api/torrents?repo=` lookup. The repo id is percent-encoded
/// except for the `/` separator, which the API expects literally.
pub fn lookup_url(api_base: &str, repo_id: &str) -> String {
    let encoded: String = repo_id
        .bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' | b'/' => {
                (b as char).to_string()
            }
            _ => format!("%{b:02X}"),
        })
        .collect();
    format!(
        "{}/api/torrents?repo={encoded}",
        api_base.trim_end_matches('/')
    )
}

/// Interpret a lookup response. 404 means "not listed" (`Ok(None)`); any other
/// non-2xx status is an error, so a server outage is never read as "absent".
pub fn interpret_lookup(status: u16, body: &str) -> Result<Option<Entry>> {
    match status {
        404 => Ok(None),
        200..=299 => parse_entry(body),
        other => Err(Error::Invalid(format!(
            "Pirate Face lookup failed: HTTP {other}"
        ))),
    }
}

#[cfg(test)]
mod lookup_tests {
    use super::*;

    const LISTED: &str = include_str!("../tests/fixtures/pirateface/torrents_listed.json");
    const PF_ONLY: &str = include_str!("../tests/fixtures/pirateface/torrents_pf_only.json");
    const BAD_SHA: &str = include_str!("../tests/fixtures/pirateface/torrents_bad_sha.json");

    #[test]
    fn url_keeps_slash_and_encodes_others() {
        assert_eq!(
            lookup_url("https://pirateface.co/", "acme/tiny v2"),
            "https://pirateface.co/api/torrents?repo=acme/tiny%20v2"
        );
    }

    #[test]
    fn listed_entry_parses() {
        let e = interpret_lookup(200, LISTED).unwrap().unwrap();
        assert!(e.hf);
        assert_eq!(e.files.len(), 1);
    }

    #[test]
    fn pf_only_entry_is_flagged() {
        assert!(!interpret_lookup(200, PF_ONLY).unwrap().unwrap().hf);
    }

    #[test]
    fn not_listed_is_none() {
        assert!(interpret_lookup(404, "").unwrap().is_none());
    }

    #[test]
    fn server_error_is_not_absence() {
        assert!(interpret_lookup(503, "").is_err());
    }

    #[test]
    fn bad_sha_is_rejected() {
        assert!(interpret_lookup(200, BAD_SHA).is_err());
    }
}

/// Look up a repo on Pirate Face over HTTP. `Ok(None)` means not listed.
/// `PF_API` overrides the base URL; a missing variable uses `DEFAULT_API`.
pub async fn lookup(repo_id: &str) -> Result<Option<Entry>> {
    let base = std::env::var("PF_API").unwrap_or_else(|_| DEFAULT_API.to_string());
    lookup_at(&base, repo_id).await
}

/// Same as `lookup`, against an explicit base URL (used by tests).
pub async fn lookup_at(base: &str, repo_id: &str) -> Result<Option<Entry>> {
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()
        .map_err(|e| Error::Invalid(format!("Pirate Face client: {e}")))?;
    let response = client
        .get(lookup_url(base, repo_id))
        .send()
        .await
        .map_err(|e| Error::Invalid(format!("Pirate Face unreachable: {e}")))?;
    let status = response.status().as_u16();
    let body = response
        .text()
        .await
        .map_err(|e| Error::Invalid(format!("Pirate Face body: {e}")))?;
    interpret_lookup(status, &body)
}

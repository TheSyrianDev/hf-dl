use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::os::unix::fs::symlink;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use fs2::FileExt;
use sha2::{Digest, Sha256};

use crate::error::{Error, Result};
use crate::hub::RepoRef;
use crate::uri::validate_repo_path;

#[derive(Debug, Clone)]
pub enum Target {
    Cache(CacheLayout),
    Local(LocalLayout),
}

impl Target {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        cache_dir: &Path,
        local_dir: Option<&Path>,
        repo: &RepoRef,
        commit: &str,
        path: &str,
        etag: &str,
        revision: &str,
    ) -> Result<Self> {
        if let Some(local_dir) = local_dir {
            Ok(Target::Local(LocalLayout::new(
                local_dir, commit, path, etag, revision,
            )?))
        } else {
            if etag.is_empty() {
                return Err(Error::Invalid(
                    "the Hub did not provide an ETag for this file".to_string(),
                ));
            }
            Ok(Target::Cache(CacheLayout::new(
                cache_dir, repo, commit, path, etag, revision,
            )?))
        }
    }

    pub fn final_path(&self) -> &Path {
        match self {
            Target::Cache(layout) => &layout.blob_path,
            Target::Local(layout) => &layout.dest_path,
        }
    }

    pub fn reported_path(&self) -> PathBuf {
        match self {
            Target::Cache(layout) => layout.snapshot_path.clone(),
            Target::Local(layout) => layout.dest_path.clone(),
        }
    }

    pub fn partial_path(&self) -> &Path {
        match self {
            Target::Cache(layout) => &layout.partial_path,
            Target::Local(layout) => &layout.partial_path,
        }
    }

    pub fn state_path(&self) -> &Path {
        match self {
            Target::Cache(layout) => &layout.state_path,
            Target::Local(layout) => &layout.state_path,
        }
    }

    pub fn lock_path(&self) -> &Path {
        match self {
            Target::Cache(layout) => &layout.lock_path,
            Target::Local(layout) => &layout.lock_path,
        }
    }

    pub fn is_complete(&self, size: u64, force: bool) -> bool {
        match self {
            Target::Cache(layout) => layout.is_complete(size, force),
            Target::Local(layout) => layout.is_complete(size, force),
        }
    }

    pub fn publish(&self) -> Result<PathBuf> {
        match self {
            Target::Cache(layout) => layout.publish(),
            Target::Local(layout) => layout.publish(),
        }
    }

    pub fn acquire_lock(&self) -> Result<FileLock> {
        acquire_lock(self.lock_path())
    }
}

#[derive(Debug, Clone)]
pub struct CacheLayout {
    pub blob_path: PathBuf,
    pub snapshot_path: PathBuf,
    pub ref_path: Option<PathBuf>,
    pub partial_path: PathBuf,
    pub state_path: PathBuf,
    pub lock_path: PathBuf,
    commit: String,
    revision: String,
}

impl CacheLayout {
    fn new(
        cache_dir: &Path,
        repo: &RepoRef,
        commit: &str,
        path: &str,
        etag: &str,
        revision: &str,
    ) -> Result<Self> {
        validate_repo_path(path)?;
        let repo_folder = repo_folder_name(repo);
        let repo_dir = cache_dir.join(&repo_folder);
        let blob_dir = repo_dir.join("blobs");
        let blob_path = blob_dir.join(etag);
        let snapshot_path = repo_dir.join("snapshots").join(commit).join(path);
        let ref_path = if is_commit_hash(revision) {
            None
        } else {
            Some(repo_dir.join("refs").join(revision))
        };
        let lock_path = cache_dir
            .join(".locks")
            .join(&repo_folder)
            .join(format!("{etag}.lock"));
        let partial_path = blob_dir.join(format!("{etag}.hfd.incomplete"));
        let state_path = blob_dir.join(format!("{etag}.hfd.state"));

        Ok(Self {
            blob_path,
            snapshot_path,
            ref_path,
            partial_path,
            state_path,
            lock_path,
            commit: commit.to_string(),
            revision: revision.to_string(),
        })
    }

    fn is_complete(&self, size: u64, force: bool) -> bool {
        if force {
            return false;
        }
        match fs::metadata(&self.blob_path) {
            Ok(meta) => meta.is_file() && meta.len() == size,
            Err(_) => false,
        }
    }

    fn publish(&self) -> Result<PathBuf> {
        for dir in [
            self.blob_path.parent(),
            self.snapshot_path.parent(),
            self.ref_path.as_deref().and_then(Path::parent),
            self.lock_path.parent(),
        ]
        .into_iter()
        .flatten()
        {
            fs::create_dir_all(dir)?;
        }

        if self.partial_path.exists() {
            if self.blob_path.exists() {
                let expected = fs::metadata(&self.partial_path)?.len();
                let existing = fs::metadata(&self.blob_path)?.len();
                if existing != expected {
                    fs::remove_file(&self.blob_path)?;
                }
            }
            if self.blob_path.exists() {
                fs::remove_file(&self.partial_path)?;
            } else {
                fs::rename(&self.partial_path, &self.blob_path)?;
            }
        } else if !self.blob_path.exists() {
            return Err(Error::Invalid(format!(
                "download completed without a partial file: {}",
                self.partial_path.display()
            )));
        }

        if let Ok(file) = File::open(&self.blob_path) {
            let _ = file.sync_all();
        }

        let parent = self
            .snapshot_path
            .parent()
            .ok_or_else(|| Error::Invalid("snapshot path has no parent".to_string()))?;
        fs::create_dir_all(parent)?;

        match fs::symlink_metadata(&self.snapshot_path) {
            Ok(meta) if meta.file_type().is_symlink() || meta.is_file() => {
                fs::remove_file(&self.snapshot_path)?;
            }
            Ok(_) => {
                return Err(Error::Invalid(format!(
                    "snapshot destination is a directory: {}",
                    self.snapshot_path.display()
                )));
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }

        let relative =
            pathdiff::diff_paths(&self.blob_path, parent).unwrap_or_else(|| self.blob_path.clone());
        symlink(&relative, &self.snapshot_path)?;

        if let Some(ref_path) = &self.ref_path {
            if let Some(parent) = ref_path.parent() {
                fs::create_dir_all(parent)?;
            }
            let existing = fs::read_to_string(ref_path).unwrap_or_default();
            if existing.trim() != self.commit {
                let tmp = ref_path.with_file_name(format!(
                    "{}.tmp.{}",
                    ref_path.file_name().unwrap_or_default().to_string_lossy(),
                    std::process::id()
                ));
                let mut file = File::create(&tmp)?;
                writeln!(file, "{}", self.commit)?;
                file.sync_all()?;
                fs::rename(&tmp, ref_path)?;
            }
        }

        sync_dir(parent);
        sync_dir(self.blob_path.parent().unwrap_or(Path::new(".")));
        let _ = &self.revision;

        Ok(self.snapshot_path.clone())
    }
}

#[derive(Debug, Clone)]
pub struct LocalLayout {
    pub dest_path: PathBuf,
    pub partial_path: PathBuf,
    pub state_path: PathBuf,
    pub metadata_path: PathBuf,
    pub lock_path: PathBuf,
    commit: String,
    etag: String,
}

impl LocalLayout {
    fn new(
        local_dir: &Path,
        commit: &str,
        path: &str,
        etag: &str,
        _revision: &str,
    ) -> Result<Self> {
        validate_repo_path(path)?;
        let dest_path = local_dir.join(path);
        let metadata_root = local_dir
            .join(".cache")
            .join("huggingface")
            .join("download");
        let metadata_path = metadata_root.join(format!("{path}.metadata"));
        let lock_path = metadata_path.with_extension("lock");
        let short = short_hash(path);
        let suffix = if etag.is_empty() { "unknown" } else { etag };
        let partial_path = metadata_root.join(format!("{short}.{suffix}.hfd.incomplete"));
        let state_path = metadata_root.join(format!("{short}.{suffix}.hfd.state"));

        Ok(Self {
            dest_path,
            partial_path,
            state_path,
            metadata_path,
            lock_path,
            commit: commit.to_string(),
            etag: etag.to_string(),
        })
    }

    fn is_complete(&self, size: u64, force: bool) -> bool {
        if force {
            return false;
        }
        let Ok(meta) = fs::metadata(&self.dest_path) else {
            return false;
        };
        if !meta.is_file() || meta.len() != size {
            return false;
        }
        let Ok(content) = fs::read_to_string(&self.metadata_path) else {
            return false;
        };
        let mut lines = content.lines();
        let commit = lines.next().unwrap_or_default().trim();
        let etag = lines.next().unwrap_or_default().trim();
        commit == self.commit && etag == self.etag
    }

    fn publish(&self) -> Result<PathBuf> {
        if let Some(parent) = self.dest_path.parent() {
            fs::create_dir_all(parent)?;
        }
        if let Some(parent) = self.metadata_path.parent() {
            fs::create_dir_all(parent)?;
        }

        if !self.partial_path.exists() {
            return Err(Error::Invalid(format!(
                "download completed without a partial file: {}",
                self.partial_path.display()
            )));
        }

        if self.dest_path.exists() {
            if self.dest_path.is_dir() {
                return Err(Error::Invalid(format!(
                    "destination is a directory: {}",
                    self.dest_path.display()
                )));
            }
            fs::remove_file(&self.dest_path)?;
        }
        fs::rename(&self.partial_path, &self.dest_path)?;

        if let Ok(file) = File::open(&self.dest_path) {
            let _ = file.sync_all();
        }

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs_f64();
        let metadata = format!("{}\n{}\n{:.3}\n", self.commit, self.etag, timestamp);
        let tmp = self.metadata_path.with_file_name(format!(
            "{}.tmp.{}",
            self.metadata_path
                .file_name()
                .unwrap_or_default()
                .to_string_lossy(),
            std::process::id()
        ));
        let mut file = File::create(&tmp)?;
        file.write_all(metadata.as_bytes())?;
        file.sync_all()?;
        fs::rename(&tmp, &self.metadata_path)?;

        sync_dir(self.dest_path.parent().unwrap_or(Path::new(".")));
        sync_dir(self.metadata_path.parent().unwrap_or(Path::new(".")));

        Ok(self.dest_path.clone())
    }
}

#[derive(Debug)]
pub struct FileLock {
    _file: File,
}

impl FileLock {
    pub fn path(&self) -> Option<&Path> {
        None
    }
}

pub fn acquire_lock(path: &Path) -> Result<FileLock> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(false)
        .open(path)?;
    match file.try_lock_exclusive() {
        Ok(()) => Ok(FileLock { _file: file }),
        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
            Err(Error::Locked(path.to_path_buf()))
        }
        Err(error) if error.raw_os_error() == Some(11) => Err(Error::Locked(path.to_path_buf())),
        Err(error) => Err(error.into()),
    }
}

pub fn repo_folder_name(repo: &RepoRef) -> String {
    format!(
        "{}--{}",
        repo.repo_type.cache_prefix(),
        repo.repo_id.replace('/', "--")
    )
}

pub fn is_commit_hash(value: &str) -> bool {
    value.len() == 40 && value.chars().all(|ch| ch.is_ascii_hexdigit())
}

pub fn is_sha256(value: &str) -> bool {
    value.len() == 64 && value.chars().all(|ch| ch.is_ascii_hexdigit())
}

fn short_hash(value: &str) -> String {
    let digest = Sha256::digest(value.as_bytes());
    hex::encode(&digest[..8])
}

fn sync_dir(path: &Path) {
    if let Ok(file) = File::open(path) {
        let _ = file.sync_all();
    }
}

pub fn read_first_bytes(path: &Path, len: usize) -> Result<Vec<u8>> {
    let mut file = File::open(path)?;
    let mut buf = vec![0u8; len];
    let read = file.read(&mut buf)?;
    buf.truncate(read);
    Ok(buf)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::uri::RepoType;

    #[test]
    fn cache_layout_matches_hf() {
        let dir = tempfile::tempdir().unwrap();
        let repo = RepoRef::new(RepoType::Model, "org/model");
        let layout = CacheLayout::new(
            dir.path(),
            &repo,
            "deadbeefdeadbeefdeadbeefdeadbeefdeadbeef",
            "weights.bin",
            "abc123",
            "main",
        )
        .unwrap();

        assert!(
            layout
                .blob_path
                .ends_with("models--org--model/blobs/abc123")
        );
        assert!(layout.snapshot_path.ends_with(
            "models--org--model/snapshots/deadbeefdeadbeefdeadbeefdeadbeefdeadbeef/weights.bin"
        ));
        assert!(
            layout
                .lock_path
                .ends_with(".locks/models--org--model/abc123.lock")
        );
    }

    #[test]
    fn cache_publish_creates_symlink_and_ref() {
        let dir = tempfile::tempdir().unwrap();
        let repo = RepoRef::new(RepoType::Model, "org/model");
        let layout = CacheLayout::new(
            dir.path(),
            &repo,
            "deadbeefdeadbeefdeadbeefdeadbeefdeadbeef",
            "weights.bin",
            "abc123",
            "main",
        )
        .unwrap();
        fs::create_dir_all(layout.partial_path.parent().unwrap()).unwrap();
        fs::write(&layout.partial_path, b"hello").unwrap();

        let published = layout.publish().unwrap();
        assert_eq!(fs::read(&layout.blob_path).unwrap(), b"hello");
        assert!(published.is_symlink());
        assert_eq!(
            fs::read_link(&published).unwrap(),
            PathBuf::from("../../blobs/abc123")
        );
        assert_eq!(
            fs::read_to_string(layout.ref_path.unwrap()).unwrap().trim(),
            "deadbeefdeadbeefdeadbeefdeadbeefdeadbeef"
        );
    }

    #[test]
    fn local_layout_uses_distinct_partials_for_same_basename() {
        let dir = tempfile::tempdir().unwrap();
        let first =
            LocalLayout::new(dir.path(), "commit", "a/weights.bin", "etag", "main").unwrap();
        let second =
            LocalLayout::new(dir.path(), "commit", "b/weights.bin", "etag", "main").unwrap();
        assert_ne!(first.partial_path, second.partial_path);
        assert_ne!(first.state_path, second.state_path);
        assert_ne!(first.lock_path, second.lock_path);
    }

    #[test]
    fn local_layout_detects_complete_file() {
        let dir = tempfile::tempdir().unwrap();
        let layout = LocalLayout::new(
            dir.path(),
            "deadbeefdeadbeefdeadbeefdeadbeefdeadbeef",
            "sub/weights.bin",
            "abc123",
            "main",
        )
        .unwrap();
        fs::create_dir_all(layout.dest_path.parent().unwrap()).unwrap();
        fs::create_dir_all(layout.metadata_path.parent().unwrap()).unwrap();
        fs::write(&layout.dest_path, b"hello").unwrap();
        fs::write(
            &layout.metadata_path,
            "deadbeefdeadbeefdeadbeefdeadbeefdeadbeef\nabc123\n123.0\n",
        )
        .unwrap();

        assert!(layout.is_complete(5, false));
        assert!(!layout.is_complete(6, false));
        assert!(!layout.is_complete(5, true));
    }
}

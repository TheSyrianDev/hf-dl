use std::fs::{self, File};
use std::io::Write;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};

pub const STATE_VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DownloadState {
    pub version: u32,
    pub repo_id: String,
    pub repo_type: String,
    pub revision: String,
    pub commit: String,
    pub path: String,
    pub size: u64,
    pub etag: String,
    pub sha256: Option<String>,
    pub unit_size: u64,
    pub units: u64,
    /// One bit per unit, least-significant bit first, hex encoded.
    pub bitmap: String,
    pub updated_at: u64,
}

#[derive(Debug, Clone)]
pub struct StateIdentity<'a> {
    pub repo_id: &'a str,
    pub repo_type: &'a str,
    pub commit: &'a str,
    pub path: &'a str,
    pub size: u64,
    pub etag: &'a str,
    pub unit_size: u64,
}

impl DownloadState {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        repo_id: &str,
        repo_type: &str,
        revision: &str,
        commit: &str,
        path: &str,
        size: u64,
        etag: &str,
        sha256: Option<String>,
        unit_size: u64,
    ) -> Self {
        let units = size.div_ceil(unit_size.max(1)).max(1);
        let bitmap_len = (units as usize).div_ceil(8);
        Self {
            version: STATE_VERSION,
            repo_id: repo_id.to_string(),
            repo_type: repo_type.to_string(),
            revision: revision.to_string(),
            commit: commit.to_string(),
            path: path.to_string(),
            size,
            etag: etag.to_string(),
            sha256,
            unit_size: unit_size.max(1),
            units,
            bitmap: hex::encode(vec![0u8; bitmap_len]),
            updated_at: now(),
        }
    }

    pub fn load(path: &Path) -> Option<Self> {
        let data = fs::read(path).ok()?;
        let mut state: Self = serde_json::from_slice(&data).ok()?;
        if state.version != STATE_VERSION {
            return None;
        }
        if state.units == 0 || state.unit_size == 0 {
            return None;
        }
        let bitmap = hex::decode(&state.bitmap).ok()?;
        let expected = (state.units as usize).div_ceil(8);
        if bitmap.len() != expected {
            return None;
        }
        state.bitmap = hex::encode(bitmap);
        Some(state)
    }

    pub fn save(&self, path: &Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let tmp = path.with_file_name(format!(
            "{}.tmp.{}",
            path.file_name().unwrap_or_default().to_string_lossy(),
            std::process::id()
        ));
        let data = serde_json::to_vec(self)?;
        let mut file = File::create(&tmp)?;
        file.write_all(&data)?;
        file.sync_all()?;
        fs::rename(&tmp, path)?;
        Ok(())
    }

    pub fn matches(&self, identity: &StateIdentity<'_>) -> bool {
        self.version == STATE_VERSION
            && self.repo_id == identity.repo_id
            && self.repo_type == identity.repo_type
            && self.commit == identity.commit
            && self.path == identity.path
            && self.size == identity.size
            && self.etag == identity.etag
            && self.unit_size == identity.unit_size
    }

    pub fn is_complete(&self, index: u64) -> bool {
        if index >= self.units {
            return true;
        }
        let byte = (index / 8) as usize;
        let bit = (index % 8) as u8;
        let Some(value) = self.bitmap_bytes().get(byte).copied() else {
            return false;
        };
        value & (1 << bit) != 0
    }

    pub fn mark_complete(&mut self, index: u64) {
        if index >= self.units {
            return;
        }
        let byte = (index / 8) as usize;
        let bit = (index % 8) as u8;
        let mut bytes = self.bitmap_bytes();
        if let Some(value) = bytes.get_mut(byte) {
            *value |= 1 << bit;
            self.bitmap = hex::encode(bytes);
            self.updated_at = now();
        }
    }

    pub fn completed_units(&self) -> u64 {
        self.bitmap_bytes()
            .iter()
            .map(|byte| byte.count_ones() as u64)
            .sum()
    }

    pub fn completed_bytes(&self) -> u64 {
        (0..self.units)
            .filter(|index| self.is_complete(*index))
            .map(|index| self.unit_len(index))
            .sum()
    }

    pub fn unit_len(&self, index: u64) -> u64 {
        if index >= self.units {
            return 0;
        }
        let start = index.saturating_mul(self.unit_size);
        (self.size.saturating_sub(start)).min(self.unit_size)
    }

    pub fn range(&self, index: u64) -> Option<(u64, u64)> {
        if index >= self.units {
            return None;
        }
        let start = index * self.unit_size;
        let end = (start + self.unit_len(index)).min(self.size);
        Some((start, end))
    }

    pub fn is_done(&self) -> bool {
        self.completed_units() >= self.units
    }

    pub fn bitmap_bytes(&self) -> Vec<u8> {
        hex::decode(&self.bitmap).unwrap_or_default()
    }

    pub fn touch(&mut self) {
        self.updated_at = now();
    }
}

fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

pub fn load_valid(path: &Path, identity: &StateIdentity<'_>) -> Option<DownloadState> {
    let state = DownloadState::load(path)?;
    if state.matches(identity) {
        Some(state)
    } else {
        None
    }
}

pub fn remove_state_files(paths: &[&Path]) -> Result<()> {
    for path in paths {
        match fs::remove_file(path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(Error::Io(error)),
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn state() -> DownloadState {
        DownloadState::new(
            "org/model",
            "model",
            "main",
            "0123456789abcdef0123456789abcdef01234567",
            "weights.bin",
            100,
            "etag",
            Some("sha".to_string()),
            16,
        )
    }

    #[test]
    fn marks_units() {
        let mut state = state();
        assert_eq!(state.units, 7);
        assert!(!state.is_complete(3));
        state.mark_complete(3);
        state.mark_complete(6);
        assert!(state.is_complete(3));
        assert!(state.is_complete(6));
        assert_eq!(state.completed_units(), 2);
        assert_eq!(state.completed_bytes(), 16 + 4);
        assert_eq!(state.range(6), Some((96, 100)));
    }

    #[test]
    fn round_trips_state() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("file.state");
        let mut state = state();
        state.mark_complete(0);
        state.save(&path).unwrap();
        let loaded = DownloadState::load(&path).unwrap();
        assert!(loaded.is_complete(0));
        assert_eq!(loaded.size, 100);
    }

    #[test]
    fn validation_rejects_changed_etag() {
        let state = state();
        let identity = StateIdentity {
            repo_id: "org/model",
            repo_type: "model",
            commit: "0123456789abcdef0123456789abcdef01234567",
            path: "weights.bin",
            size: 100,
            etag: "different",
            unit_size: 16,
        };
        assert!(!state.matches(&identity));
    }
}

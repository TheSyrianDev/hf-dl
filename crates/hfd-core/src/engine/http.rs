use std::fs::{self, OpenOptions};
use std::io::{Read, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use futures_util::StreamExt;
use reqwest::header::{ACCEPT_ENCODING, HeaderMap, RANGE};
use reqwest::{Client, StatusCode};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncSeekExt, AsyncWriteExt};
use url::Url;

use crate::cache::{Target, is_sha256};
use crate::error::{Error, Result};
use crate::hub::{HubClient, RepoRef, ResolvedFile};
use crate::progress::{Event, EventSink};
use crate::state::{DownloadState, StateIdentity, load_valid};
use crate::uri::RepoType;

const MIB: u64 = 1024 * 1024;
const SEGMENT_THRESHOLD: u64 = 16 * MIB;
const CHECKPOINT_UNITS: u64 = 4;
const MAX_RETRIES: u32 = 5;
/// Connections a segmented download starts with before ramping up.
const INITIAL_CONNECTIONS: usize = 4;
const RAMP_INTERVAL: Duration = Duration::from_secs(2);
/// Add a connection only while speed keeps rising by at least 10%.
const RAMP_GAIN: f64 = 1.1;
/// Idle connections split an unfinished piece only if each half gets at least this much.
const MIN_STEAL: u64 = 2 * MIB;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum VerifyMode {
    Auto,
    Always,
    Never,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum Engine {
    Auto,
    /// The default until Xet stalls on slow links are fixed and tested live.
    #[default]
    Http,
    Xet,
}

impl Engine {
    pub fn as_str(self) -> &'static str {
        match self {
            Engine::Auto => "auto",
            Engine::Http => "http",
            Engine::Xet => "xet",
        }
    }

    pub fn select(self, xet_hash: Option<&str>) -> Result<Engine> {
        match self {
            Engine::Http => Ok(Engine::Http),
            Engine::Xet if xet_hash.is_some() => Ok(Engine::Xet),
            Engine::Xet => Err(Error::Invalid(
                "the Xet engine requires a file stored on Xet".to_string(),
            )),
            Engine::Auto if xet_hash.is_some() => Ok(Engine::Xet),
            Engine::Auto => Ok(Engine::Http),
        }
    }
}

#[derive(Debug, Clone)]
pub struct DownloadOptions {
    pub cache_dir: PathBuf,
    pub local_dir: Option<PathBuf>,
    /// With `local_dir`, write each file as its basename directly in that folder.
    pub flat: bool,
    pub force: bool,
    pub verify: VerifyMode,
    pub connections: usize,
    pub engine: Engine,
    /// Cap on total download speed in bytes per second, shared by all files.
    pub max_speed: Option<u64>,
    /// Append each completed (non-skipped) download to this JSON file.
    pub download_log: Option<PathBuf>,
}

impl Default for DownloadOptions {
    fn default() -> Self {
        Self {
            cache_dir: PathBuf::from("."),
            local_dir: None,
            flat: false,
            force: false,
            verify: VerifyMode::Auto,
            connections: 8,
            engine: Engine::Http,
            max_speed: None,
            download_log: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Plan {
    pub repo_id: String,
    pub repo_type: RepoType,
    pub path: String,
    pub revision: String,
    pub size: u64,
    pub commit: String,
    pub etag: Option<String>,
    pub sha256: Option<String>,
    pub destination: PathBuf,
    pub final_path: PathBuf,
    pub complete: bool,
    pub segmented: bool,
    pub unit_size: u64,
    pub connections: usize,
    pub accept_ranges: bool,
    pub xet_hash: Option<String>,
    pub engine: Engine,
}

#[derive(Debug, Clone)]
pub struct DownloadedFile {
    pub path: String,
    pub local_path: PathBuf,
    pub size: u64,
    pub skipped: bool,
    pub resumed_bytes: u64,
    pub engine: &'static str,
    /// SHA-256 from the Hub (LFS files only); `None` for small git files.
    pub sha256: Option<String>,
}

#[derive(Clone)]
pub struct Downloader {
    hub: HubClient,
    options: DownloadOptions,
    pub(crate) xet: Arc<tokio::sync::OnceCell<xet::xet_session::XetSession>>,
    limiter: Option<Arc<RateLimiter>>,
}

impl Downloader {
    pub fn new(hub: HubClient, options: DownloadOptions) -> Self {
        Self {
            limiter: options
                .max_speed
                .filter(|&rate| rate > 0)
                .map(|rate| Arc::new(RateLimiter::new(rate))),
            hub,
            options,
            xet: Arc::new(tokio::sync::OnceCell::new()),
        }
    }

    /// Xet can't be rate limited, so a speed cap makes Auto use HTTP.
    fn select_engine(&self, xet_hash: Option<&str>) -> Result<Engine> {
        if self.limiter.is_some() && self.options.engine == Engine::Auto {
            return Ok(Engine::Http);
        }
        let engine = self.options.engine.select(xet_hash)?;
        if engine == Engine::Xet
            && self.options.engine == Engine::Auto
            && xet_recently_failed(&self.xet_failure_path())
        {
            return Ok(Engine::Http);
        }
        Ok(engine)
    }

    /// Marker written when Xet fails on this endpoint, so later downloads
    /// skip straight to HTTP for a day instead of waiting out a stall.
    fn xet_failure_path(&self) -> PathBuf {
        xet_marker_path(self.hub.config())
    }

    pub fn hub(&self) -> &HubClient {
        &self.hub
    }

    pub async fn plan(&self, repo: &RepoRef, path: &str, revision: &str) -> Result<Plan> {
        let resolved = self.hub.resolve(repo, path, revision).await?;
        let etag = resolved.etag.clone().unwrap_or_default();
        let target = Target::new(
            &self.options.cache_dir,
            self.options.local_dir.as_deref(),
            repo,
            &resolved.commit,
            path,
            &etag,
            revision,
            self.options.flat,
        )?;
        let complete = target.is_complete(resolved.size, self.options.force);
        let engine = if complete {
            Engine::Http
        } else {
            self.select_engine(resolved.xet_hash.as_deref())?
        };
        let segmented = engine == Engine::Http
            && resolved.accept_ranges
            && resolved.size >= SEGMENT_THRESHOLD
            && self.options.connections > 1;
        let unit_size = if segmented {
            unit_size_for(resolved.size)
        } else {
            resolved.size.max(1)
        };

        Ok(Plan {
            repo_id: repo.repo_id.clone(),
            repo_type: repo.repo_type,
            path: path.to_string(),
            revision: revision.to_string(),
            size: resolved.size,
            commit: resolved.commit,
            etag: resolved.etag,
            sha256: resolved.sha256,
            destination: target.reported_path(),
            final_path: target.final_path().to_path_buf(),
            complete,
            segmented,
            unit_size,
            connections: self.options.connections.max(1),
            accept_ranges: resolved.accept_ranges,
            xet_hash: resolved.xet_hash,
            engine,
        })
    }

    /// Like [`download_file`](Self::download_file), but stops with
    /// [`Error::Cancelled`] once `cancel` fires. Finished pieces are kept,
    /// so downloading the file again resumes it.
    pub async fn download_file_cancellable(
        &self,
        repo: &RepoRef,
        path: &str,
        revision: &str,
        sink: &EventSink,
        cancel: &crate::CancelToken,
    ) -> Result<DownloadedFile> {
        if cancel.is_cancelled() {
            return Err(Error::Cancelled);
        }
        tokio::select! {
            result = self.download_file(repo, path, revision, sink) => result,
            () = cancel.cancelled() => Err(Error::Cancelled),
        }
    }

    pub async fn download_file(
        &self,
        repo: &RepoRef,
        path: &str,
        revision: &str,
        sink: &EventSink,
    ) -> Result<DownloadedFile> {
        let file = self.download_file_inner(repo, path, revision, sink).await?;
        if !file.skipped
            && let Some(log) = &self.options.download_log
            && let Err(error) = crate::record::record_download(log, repo, revision, &file)
        {
            sink(Event::message(format!("warning: {error}")));
        }
        Ok(file)
    }

    async fn download_file_inner(
        &self,
        repo: &RepoRef,
        path: &str,
        revision: &str,
        sink: &EventSink,
    ) -> Result<DownloadedFile> {
        let sink = &crate::progress::with_rate(sink.clone());
        let resolved = self.hub.resolve(repo, path, revision).await?;
        let etag = resolved.etag.clone().unwrap_or_default();
        let target = Target::new(
            &self.options.cache_dir,
            self.options.local_dir.as_deref(),
            repo,
            &resolved.commit,
            path,
            &etag,
            revision,
            self.options.flat,
        )?;

        if target.is_complete(resolved.size, self.options.force) {
            let local_path = publish_completed(&target)?;
            sink(Event::FileDone {
                path: path.to_string(),
                local_path: local_path.clone(),
                size: resolved.size,
                skipped: true,
            });
            return Ok(DownloadedFile {
                sha256: resolved.sha256.clone(),
                path: path.to_string(),
                local_path,
                size: resolved.size,
                skipped: true,
                resumed_bytes: 0,
                engine: "cache",
            });
        }

        let lock = target.acquire_lock()?;
        let _lock = lock;

        if target.is_complete(resolved.size, self.options.force) {
            let local_path = publish_completed(&target)?;
            sink(Event::FileDone {
                path: path.to_string(),
                local_path: local_path.clone(),
                size: resolved.size,
                skipped: true,
            });
            return Ok(DownloadedFile {
                sha256: resolved.sha256.clone(),
                path: path.to_string(),
                local_path,
                size: resolved.size,
                skipped: true,
                resumed_bytes: 0,
                engine: "cache",
            });
        }

        let engine = self.select_engine(resolved.xet_hash.as_deref())?;

        if let Some(parent) = target.partial_path().parent() {
            fs::create_dir_all(parent)?;
        }
        self.check_space(&target, resolved.size)?;

        let xet_result = if engine == Engine::Xet {
            if self.options.engine == Engine::Auto {
                sink(Event::message(format!(
                    "{path}: using Xet; switching to HTTP if no data arrives for {}s",
                    crate::engine::xet::XET_STALL_TIMEOUT.as_secs()
                )));
            }
            match self
                .download_xet(repo, path, revision, &resolved, &target, sink)
                .await
            {
                // In auto mode, a failing Xet transfer falls back to plain HTTP.
                Err(error @ (Error::Xet(_) | Error::XetStalled(_)))
                    if self.options.engine == Engine::Auto =>
                {
                    sink(Event::message(format!(
                        "{path}: {error}; switching to HTTP (Xet skipped on this host for 24h)"
                    )));
                    let marker = self.xet_failure_path();
                    if let Some(parent) = marker.parent() {
                        let _ = fs::create_dir_all(parent);
                    }
                    let _ = fs::write(&marker, b"");
                    remove_file_if_exists(target.partial_path())?;
                    None
                }
                other => Some(other?),
            }
        } else {
            None
        };

        if let Some((resumed_bytes, engine)) = xet_result {
            self.verify(&target, &resolved, sink).await?;
            let local_path = target.publish()?;
            sink(Event::FileDone {
                path: path.to_string(),
                local_path: local_path.clone(),
                size: resolved.size,
                skipped: false,
            });
            return Ok(DownloadedFile {
                sha256: resolved.sha256.clone(),
                path: path.to_string(),
                local_path,
                size: resolved.size,
                skipped: false,
                resumed_bytes,
                engine,
            });
        }

        let segmented = resolved.accept_ranges
            && resolved.size >= SEGMENT_THRESHOLD
            && self.options.connections > 1;

        let result = if segmented {
            self.download_segmented(repo, path, revision, &resolved, &target, sink)
                .await
        } else {
            self.download_single(repo, path, revision, &resolved, &target, sink)
                .await
        };

        let (resumed_bytes, engine) = match result {
            Ok(outcome) => outcome,
            Err(Error::RangeUnsupported) => {
                remove_file_if_exists(target.partial_path())?;
                remove_file_if_exists(target.state_path())?;
                self.download_single(repo, path, revision, &resolved, &target, sink)
                    .await?
            }
            Err(error) => return Err(error),
        };

        self.verify(&target, &resolved, sink).await?;
        let local_path = target.publish()?;
        remove_file_if_exists(target.state_path())?;
        sink(Event::FileDone {
            path: path.to_string(),
            local_path: local_path.clone(),
            size: resolved.size,
            skipped: false,
        });

        Ok(DownloadedFile {
            sha256: resolved.sha256.clone(),
            path: path.to_string(),
            local_path,
            size: resolved.size,
            skipped: false,
            resumed_bytes,
            engine,
        })
    }

    async fn download_segmented(
        &self,
        repo: &RepoRef,
        path: &str,
        revision: &str,
        resolved: &ResolvedFile,
        target: &Target,
        sink: &EventSink,
    ) -> Result<(u64, &'static str)> {
        let unit_size = unit_size_for(resolved.size);
        let identity = StateIdentity {
            repo_id: &repo.repo_id,
            repo_type: repo.repo_type.as_str(),
            commit: &resolved.commit,
            path,
            size: resolved.size,
            etag: resolved.etag.as_deref().unwrap_or(""),
            unit_size,
        };

        let existing_len = std::fs::metadata(target.partial_path())
            .ok()
            .filter(|meta| meta.is_file())
            .map(|meta| meta.len());
        let state = match load_valid(target.state_path(), &identity) {
            Some(state) if existing_len == Some(resolved.size) => state,
            _ => {
                remove_file_if_exists(target.partial_path())?;
                remove_file_if_exists(target.state_path())?;
                DownloadState::new(
                    &repo.repo_id,
                    repo.repo_type.as_str(),
                    revision,
                    &resolved.commit,
                    path,
                    resolved.size,
                    resolved.etag.as_deref().unwrap_or(""),
                    resolved.sha256.clone(),
                    unit_size,
                )
            }
        };

        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(target.partial_path())?;
        file.set_len(resolved.size)?;
        drop(file);

        let resumed_bytes = state.completed_bytes();
        sink(Event::FileStart {
            path: path.to_string(),
            size: resolved.size,
            destination: target.reported_path(),
            resumed_bytes,
        });

        if state.is_done() {
            state.save(target.state_path())?;
            return Ok((resumed_bytes, "http-segmented"));
        }

        let units = state.units;
        let tracker = Arc::new(Tracker::new(resolved.size, resumed_bytes, units));
        let state = Arc::new(Mutex::new(state));
        let next = Arc::new(AtomicU64::new(0));
        let state_path = target.state_path().to_path_buf();
        let partial_path = target.partial_path().to_path_buf();
        let checkpoint_lock = Arc::new(tokio::sync::Mutex::new(()));
        let url_cache = Arc::new(UrlCache::new(
            self.hub.clone(),
            repo.clone(),
            path.to_string(),
            revision.to_string(),
            resolved.location.clone(),
            resolved.auth_required,
            resolved.expires_at,
        ));

        let progress_task = spawn_progress(sink.clone(), path.to_string(), tracker.clone());
        let checkpoint_task =
            spawn_checkpoint(state.clone(), state_path.clone(), checkpoint_lock.clone());

        let max_workers = self.options.connections.max(1).min(units.max(1) as usize);
        let meta = {
            let guard = state.lock().unwrap();
            (0..units)
                .map(|index| {
                    let (start, end) = guard.range(index).unwrap_or((0, 0));
                    Mutex::new(UnitMeta {
                        start,
                        end,
                        ..UnitMeta::default()
                    })
                })
                .collect()
        };
        let ctx = Arc::new(SegmentCtx {
            client: self.hub.http_client().clone(),
            endpoint: self.hub.config().endpoint.clone(),
            token: self.hub.token().map(str::to_string),
            url_cache,
            meta,
            state: state.clone(),
            tracker: tracker.clone(),
            next,
            units,
            file_size: resolved.size,
            partial_path,
            state_path: state_path.clone(),
            checkpoint_lock,
            path: path.to_string(),
            limiter: self.limiter.clone(),
        });

        // Start with a few connections and add one at a time while each
        // addition still raises the speed, up to `connections`.
        let mut tasks = tokio::task::JoinSet::new();
        let mut spawned = max_workers.min(INITIAL_CONNECTIONS);
        for _ in 0..spawned {
            tasks.spawn(worker(ctx.clone()));
        }
        let mut tick = tokio::time::interval(RAMP_INTERVAL);
        tick.tick().await;
        let mut last_bytes = tracker.downloaded();
        let mut best_rate = 0f64;

        let mut first_error = None;
        loop {
            tokio::select! {
                result = tasks.join_next() => match result {
                    None => break,
                    Some(Ok(Ok(()))) => {}
                    Some(Ok(Err(error))) => {
                        first_error = Some(error);
                        tasks.abort_all();
                        break;
                    }
                    Some(Err(error)) => {
                        first_error =
                            Some(Error::Invalid(format!("download worker failed: {error}")));
                        tasks.abort_all();
                        break;
                    }
                },
                _ = tick.tick(), if spawned < max_workers => {
                    let bytes = tracker.downloaded();
                    let rate = bytes.saturating_sub(last_bytes) as f64;
                    last_bytes = bytes;
                    if rate > best_rate * RAMP_GAIN && !tasks.is_empty() {
                        tasks.spawn(worker(ctx.clone()));
                        spawned += 1;
                    }
                    best_rate = best_rate.max(rate);
                }
            }
        }

        progress_task.abort();
        checkpoint_task.abort();

        {
            let guard = state.lock().unwrap();
            guard.save(&state_path)?;
        }

        if let Some(error) = first_error {
            return Err(error);
        }

        let downloaded = tracker.downloaded();
        sink(Event::Progress {
            path: path.to_string(),
            downloaded,
            total: resolved.size,
            bytes_per_sec: 0.0,
            eta_secs: None,
        });
        Ok((resumed_bytes, "http-segmented"))
    }

    async fn download_single(
        &self,
        repo: &RepoRef,
        path: &str,
        revision: &str,
        resolved: &ResolvedFile,
        target: &Target,
        sink: &EventSink,
    ) -> Result<(u64, &'static str)> {
        remove_file_if_exists(target.partial_path())?;
        remove_file_if_exists(target.state_path())?;

        sink(Event::FileStart {
            path: path.to_string(),
            size: resolved.size,
            destination: target.reported_path(),
            resumed_bytes: 0,
        });

        if resolved.size == 0 {
            fs::File::create(target.partial_path())?.sync_all()?;
            return Ok((0, "http-single"));
        }

        let tracker = Arc::new(Tracker::new(resolved.size, 0, 1));
        let progress_task = spawn_progress(sink.clone(), path.to_string(), tracker.clone());
        let mut file = tokio::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(target.partial_path())
            .await?;

        let url_cache = UrlCache::new(
            self.hub.clone(),
            repo.clone(),
            path.to_string(),
            revision.to_string(),
            resolved.location.clone(),
            resolved.auth_required,
            resolved.expires_at,
        );
        let client = self.hub.http_client().clone();
        let token = self.hub.token().map(str::to_string);
        let endpoint = self.hub.config().endpoint.clone();

        let mut attempt = 0u32;
        let mut auth_attempts = 0u32;
        let result = loop {
            let (url, auth_required) = match url_cache.get().await {
                Ok(value) => value,
                Err(error) => break Err(error),
            };

            match fetch_full(
                &client,
                &endpoint,
                token.as_deref(),
                &url,
                auth_required,
                resolved.size,
                &mut file,
                &tracker,
                path,
                self.limiter.as_deref(),
            )
            .await
            {
                Ok(()) => break Ok(()),
                Err(Error::Auth { .. }) if auth_attempts < 1 => {
                    auth_attempts += 1;
                    if let Err(error) = url_cache.refresh().await {
                        break Err(error);
                    }
                    file.set_len(0).await?;
                    file.seek(SeekFrom::Start(0)).await?;
                    tracker.inflight[0].store(0, Ordering::Relaxed);
                }
                Err(error) if error.is_retryable() && attempt < MAX_RETRIES => {
                    attempt += 1;
                    tokio::time::sleep(backoff(attempt, error.retry_after())).await;
                    file.set_len(0).await?;
                    file.seek(SeekFrom::Start(0)).await?;
                    tracker.inflight[0].store(0, Ordering::Relaxed);
                }
                Err(error) => break Err(error),
            }
        };

        progress_task.abort();
        if let Err(error) = result {
            let _ = file.flush().await;
            return Err(error);
        }
        file.flush().await?;
        sink(Event::Progress {
            path: path.to_string(),
            downloaded: resolved.size,
            total: resolved.size,
            bytes_per_sec: 0.0,
            eta_secs: None,
        });
        Ok((0, "http-single"))
    }

    pub(crate) fn check_space(&self, target: &Target, size: u64) -> Result<()> {
        if size == 0 {
            return Ok(());
        }
        let mut probe = target
            .partial_path()
            .parent()
            .unwrap_or(Path::new("."))
            .to_path_buf();
        while !probe.exists() {
            match probe.parent() {
                Some(parent) if parent != probe => probe = parent.to_path_buf(),
                _ => break,
            }
        }
        let available = fs2::available_space(&probe).unwrap_or(u64::MAX);
        if available < size {
            return Err(Error::NoSpace {
                need: size,
                available,
            });
        }
        Ok(())
    }

    pub(crate) async fn verify(
        &self,
        target: &Target,
        resolved: &ResolvedFile,
        sink: &EventSink,
    ) -> Result<()> {
        if self.options.verify == VerifyMode::Never {
            return Ok(());
        }
        let expected = resolved
            .sha256
            .clone()
            .or_else(|| resolved.etag.clone().filter(|etag| is_sha256(etag)));
        if expected.is_none() && self.options.verify == VerifyMode::Always {
            // Small git-tracked files have no sha256, but their ETag is the git blob sha1.
            if let Some(blob) = resolved.etag.clone().filter(|etag| is_git_sha1(etag)) {
                sink(Event::message(format!(
                    "verifying git blob sha1 for {}",
                    target.reported_path().display()
                )));
                let partial_path = target.partial_path().to_path_buf();
                let size = resolved.size;
                let actual =
                    tokio::task::spawn_blocking(move || git_blob_sha1(&partial_path, size))
                        .await
                        .map_err(|error| {
                            Error::Invalid(format!("checksum task failed: {error}"))
                        })??;
                if !actual.eq_ignore_ascii_case(&blob) {
                    let _ = remove_file_if_exists(target.partial_path());
                    let _ = remove_file_if_exists(target.state_path());
                    return Err(Error::Checksum {
                        path: target.reported_path().display().to_string(),
                        expected: blob,
                        actual,
                    });
                }
                return Ok(());
            }
        }
        let Some(expected) = expected else {
            if self.options.verify == VerifyMode::Always {
                return Err(Error::Invalid(format!(
                    "no SHA256 checksum is available for {}",
                    target.reported_path().display()
                )));
            }
            return Ok(());
        };

        sink(Event::message(format!(
            "verifying sha256 for {}",
            target.reported_path().display()
        )));
        let partial_path = target.partial_path().to_path_buf();
        let actual = tokio::task::spawn_blocking(move || hash_file(&partial_path))
            .await
            .map_err(|error| Error::Invalid(format!("checksum task failed: {error}")))??;
        if !actual.eq_ignore_ascii_case(&expected) {
            let _ = remove_file_if_exists(target.partial_path());
            let _ = remove_file_if_exists(target.state_path());
            return Err(Error::Checksum {
                path: target.reported_path().display().to_string(),
                expected,
                actual,
            });
        }
        Ok(())
    }
}

fn publish_completed(target: &Target) -> Result<PathBuf> {
    match target {
        Target::Cache(_) => target.publish(),
        Target::Local(_) => Ok(target.final_path().to_path_buf()),
    }
}

fn unit_size_for(size: u64) -> u64 {
    if size < MIB {
        1
    } else if size < 1024 * MIB {
        8 * MIB
    } else if size < 20 * 1024 * MIB {
        16 * MIB
    } else if size < 100 * 1024 * MIB {
        32 * MIB
    } else {
        64 * MIB
    }
}

struct Tracker {
    total: u64,
    completed: AtomicU64,
    inflight: Vec<AtomicU64>,
    /// Bytes written by a second connection that split the piece.
    stolen: Vec<AtomicU64>,
}

impl Tracker {
    fn new(total: u64, completed: u64, units: u64) -> Self {
        Self {
            total,
            completed: AtomicU64::new(completed),
            inflight: (0..units).map(|_| AtomicU64::new(0)).collect(),
            stolen: (0..units).map(|_| AtomicU64::new(0)).collect(),
        }
    }

    fn downloaded(&self) -> u64 {
        let inflight: u64 = self
            .inflight
            .iter()
            .chain(&self.stolen)
            .map(|value| value.load(Ordering::Relaxed))
            .sum();
        self.completed
            .load(Ordering::Relaxed)
            .saturating_add(inflight)
            .min(self.total)
    }
}

fn spawn_progress(
    sink: EventSink,
    path: String,
    tracker: Arc<Tracker>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_millis(200)).await;
            sink(Event::Progress {
                path: path.clone(),
                downloaded: tracker.downloaded(),
                total: tracker.total,
                bytes_per_sec: 0.0,
                eta_secs: None,
            });
        }
    })
}

fn spawn_checkpoint(
    state: Arc<Mutex<DownloadState>>,
    state_path: PathBuf,
    lock: Arc<tokio::sync::Mutex<()>>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(2)).await;
            let _guard = lock.lock().await;
            let snapshot = state.lock().unwrap().clone();
            let path = state_path.clone();
            let _ = tokio::task::spawn_blocking(move || snapshot.save(&path)).await;
        }
    })
}

/// Global token-bucket style speed cap shared by every connection.
pub(crate) struct RateLimiter {
    bytes_per_sec: f64,
    next_free: Mutex<Instant>,
}

impl RateLimiter {
    fn new(bytes_per_sec: u64) -> Self {
        Self {
            bytes_per_sec: bytes_per_sec as f64,
            next_free: Mutex::new(Instant::now()),
        }
    }

    async fn take(&self, bytes: usize) {
        let wait = {
            let mut next = self.next_free.lock().unwrap();
            let now = Instant::now();
            let start = (*next).max(now);
            *next = start + Duration::from_secs_f64(bytes as f64 / self.bytes_per_sec);
            start.saturating_duration_since(now)
        };
        if !wait.is_zero() {
            tokio::time::sleep(wait).await;
        }
    }
}

/// Live bookkeeping for one piece of a segmented download.
#[derive(Default)]
struct UnitMeta {
    start: u64,
    /// Where the owning connection stops. Shrinks when the piece is split.
    end: u64,
    owner_active: bool,
    stolen: bool,
    /// Connections still working on this piece; it is complete at zero.
    parts: u32,
}

struct SegmentCtx {
    client: Client,
    endpoint: String,
    token: Option<String>,
    url_cache: Arc<UrlCache>,
    meta: Vec<Mutex<UnitMeta>>,
    state: Arc<Mutex<DownloadState>>,
    tracker: Arc<Tracker>,
    next: Arc<AtomicU64>,
    units: u64,
    file_size: u64,
    partial_path: PathBuf,
    state_path: PathBuf,
    checkpoint_lock: Arc<tokio::sync::Mutex<()>>,
    path: String,
    limiter: Option<Arc<RateLimiter>>,
}

enum Job {
    Own(u64),
    /// Second half of a piece another connection is still downloading.
    Steal {
        index: u64,
        start: u64,
        end: u64,
    },
}

impl SegmentCtx {
    fn next_job(&self) -> Option<Job> {
        loop {
            let index = self.next.fetch_add(1, Ordering::Relaxed);
            if index >= self.units {
                break;
            }
            if self.state.lock().unwrap().is_complete(index) {
                continue;
            }
            let mut meta = self.meta[index as usize].lock().unwrap();
            if meta.end <= meta.start {
                drop(meta);
                self.finish_unit(index);
                continue;
            }
            meta.owner_active = true;
            meta.parts = 1;
            return Some(Job::Own(index));
        }
        self.steal()
    }

    /// Split the unfinished piece with the most bytes left, so one slow
    /// connection can't hold up the end of the download.
    fn steal(&self) -> Option<Job> {
        let mut best: Option<(u64, u64)> = None;
        for (index, meta) in self.meta.iter().enumerate() {
            let meta = meta.lock().unwrap();
            if !meta.owner_active || meta.stolen {
                continue;
            }
            let pos = meta.start + self.tracker.inflight[index].load(Ordering::Relaxed);
            let left = meta.end.saturating_sub(pos);
            if left >= 2 * MIN_STEAL && best.is_none_or(|(_, most)| left > most) {
                best = Some((index as u64, left));
            }
        }
        let (index, _) = best?;
        let mut meta = self.meta[index as usize].lock().unwrap();
        if !meta.owner_active || meta.stolen {
            return None;
        }
        let pos = meta.start + self.tracker.inflight[index as usize].load(Ordering::Relaxed);
        let left = meta.end.saturating_sub(pos);
        if left < 2 * MIN_STEAL {
            return None;
        }
        let mid = pos + left / 2;
        let end = meta.end;
        meta.end = mid;
        meta.stolen = true;
        meta.parts += 1;
        Some(Job::Steal {
            index,
            start: mid,
            end,
        })
    }

    /// One connection finished its share of a piece.
    async fn release(&self, index: u64, owner: bool) {
        let done = {
            let mut meta = self.meta[index as usize].lock().unwrap();
            if owner {
                meta.owner_active = false;
            }
            meta.parts = meta.parts.saturating_sub(1);
            meta.parts == 0
        };
        if done {
            self.finish_unit(index);
            let units_done = self.state.lock().unwrap().completed_units();
            if units_done.is_multiple_of(CHECKPOINT_UNITS) {
                let _guard = self.checkpoint_lock.lock().await;
                let snapshot = self.state.lock().unwrap().clone();
                let path = self.state_path.clone();
                let _ = tokio::task::spawn_blocking(move || snapshot.save(&path)).await;
            }
        }
    }

    fn finish_unit(&self, index: u64) {
        let completed = {
            let mut guard = self.state.lock().unwrap();
            guard.mark_complete(index);
            guard.completed_bytes()
        };
        self.tracker.completed.store(completed, Ordering::Relaxed);
        self.tracker.inflight[index as usize].store(0, Ordering::Relaxed);
        self.tracker.stolen[index as usize].store(0, Ordering::Relaxed);
    }
}

async fn worker(ctx: Arc<SegmentCtx>) -> Result<()> {
    let mut file = tokio::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&ctx.partial_path)
        .await?;

    while let Some(job) = ctx.next_job() {
        let (index, base, end, owner) = match job {
            Job::Own(index) => {
                let meta = ctx.meta[index as usize].lock().unwrap();
                (index, meta.start, meta.end, true)
            }
            Job::Steal { index, start, end } => (index, start, end, false),
        };
        let progress = if owner {
            &ctx.tracker.inflight[index as usize]
        } else {
            &ctx.tracker.stolen[index as usize]
        };
        progress.store(0, Ordering::Relaxed);

        // The owner's stop point can move down while it runs; a stealer's can't.
        let limit = || {
            if owner {
                ctx.meta[index as usize].lock().unwrap().end
            } else {
                end
            }
        };

        let mut attempt = 0u32;
        let mut auth_attempts = 0u32;
        loop {
            // Retries continue from the last byte written instead of
            // restarting the whole piece.
            let from = base + progress.load(Ordering::Relaxed);
            if from >= limit() {
                break;
            }
            let (url, auth_required) = ctx.url_cache.get().await?;
            match fetch_range(
                &ctx,
                &url,
                auth_required,
                from,
                limit(),
                &mut file,
                progress,
                &limit,
            )
            .await
            {
                Ok(()) => break,
                Err(Error::Auth { .. }) if auth_attempts < 1 => {
                    auth_attempts += 1;
                    ctx.url_cache.refresh().await?;
                }
                Err(error) if error.is_retryable() && attempt < MAX_RETRIES => {
                    attempt += 1;
                    tokio::time::sleep(backoff(attempt, error.retry_after())).await;
                }
                Err(error) => return Err(error),
            }
        }

        ctx.release(index, owner).await;
    }

    Ok(())
}

/// Downloads `[start, end)` into the file, stopping early once `limit()`
/// (which can shrink while running) is reached. Adds written bytes to `progress`.
#[allow(clippy::too_many_arguments)]
async fn fetch_range(
    ctx: &SegmentCtx,
    url: &str,
    auth_required: bool,
    start: u64,
    end: u64,
    file: &mut tokio::fs::File,
    progress: &AtomicU64,
    limit: &(dyn Fn() -> u64 + Sync),
) -> Result<()> {
    let path = ctx.path.as_str();
    let mut current = url.to_string();
    let mut auth_required = auth_required;

    for _ in 0..5 {
        let send_auth = auth_required && same_host(&current, &ctx.endpoint);
        let response = {
            let mut request = ctx
                .client
                .get(&current)
                .header(RANGE, format!("bytes={}-{}", start, end - 1))
                .header(ACCEPT_ENCODING, "identity")
                .header("accept", "*/*");
            if send_auth && let Some(token) = ctx.token.as_deref() {
                request = request.bearer_auth(token);
            }
            request.send().await?
        };

        let status = response.status();
        if status.is_redirection() {
            let Some(location) = response
                .headers()
                .get("location")
                .and_then(|value| value.to_str().ok())
            else {
                return Err(Error::Status {
                    status: status.as_u16(),
                    url: current,
                    body: "redirect without a Location header".to_string(),
                });
            };
            current = Url::parse(&current)?.join(location)?.to_string();
            auth_required = same_host(&current, &ctx.endpoint);
            continue;
        }

        match status {
            StatusCode::PARTIAL_CONTENT => {
                if let Some(value) = header_value(response.headers(), "content-range") {
                    let (got_start, got_end, total) = parse_content_range(&value)?;
                    if got_start != start || got_end + 1 > end || total != ctx.file_size {
                        return Err(Error::Invalid(format!(
                            "unexpected Content-Range '{value}' for {path}"
                        )));
                    }
                }
                file.seek(SeekFrom::Start(start)).await?;
                let mut pos = start;
                let mut stream = response.bytes_stream();
                while let Some(chunk) = stream.next().await {
                    let Ok(chunk) = chunk else {
                        return Err(Error::Retryable {
                            status: 0,
                            retry_after: None,
                        });
                    };
                    if pos + chunk.len() as u64 > end {
                        return Err(Error::Invalid(format!(
                            "server sent too many bytes for {path}"
                        )));
                    }
                    let stop = limit();
                    let take = (stop.saturating_sub(pos) as usize).min(chunk.len());
                    if let Some(limiter) = &ctx.limiter {
                        limiter.take(take).await;
                    }
                    file.write_all(&chunk[..take]).await?;
                    pos += take as u64;
                    progress.fetch_add(take as u64, Ordering::Relaxed);
                    if pos >= stop {
                        // Another connection took the rest of this piece.
                        return Ok(());
                    }
                }
                if pos < limit() {
                    return Err(Error::Retryable {
                        status: 0,
                        retry_after: None,
                    });
                }
                return Ok(());
            }
            StatusCode::OK => return Err(Error::RangeUnsupported),
            StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => {
                let body = response.text().await.unwrap_or_default();
                return Err(Error::Auth {
                    repo: current,
                    detail: truncate_body(body),
                });
            }
            StatusCode::TOO_MANY_REQUESTS => {
                let retry_after = header_u64(response.headers(), "retry-after");
                return Err(Error::Retryable {
                    status: 429,
                    retry_after: retry_after.or(Some(5)),
                });
            }
            status if status.is_server_error() || status == StatusCode::REQUEST_TIMEOUT => {
                let retry_after = header_u64(response.headers(), "retry-after");
                return Err(Error::Retryable {
                    status: status.as_u16(),
                    retry_after,
                });
            }
            _ => {
                let body = response.text().await.unwrap_or_default();
                return Err(Error::Status {
                    status: status.as_u16(),
                    url: current,
                    body: truncate_body(body),
                });
            }
        }
    }

    Err(Error::Invalid(format!(
        "too many redirects while downloading {path}"
    )))
}

#[allow(clippy::too_many_arguments)]
async fn fetch_full(
    client: &Client,
    endpoint: &str,
    token: Option<&str>,
    url: &str,
    auth_required: bool,
    file_size: u64,
    file: &mut tokio::fs::File,
    tracker: &Tracker,
    path: &str,
    limiter: Option<&RateLimiter>,
) -> Result<()> {
    let mut current = url.to_string();
    let mut auth_required = auth_required;

    for _ in 0..5 {
        let send_auth = auth_required && same_host(&current, endpoint);
        let response = {
            let mut request = client
                .get(&current)
                .header(ACCEPT_ENCODING, "identity")
                .header("accept", "*/*");
            if send_auth && let Some(token) = token {
                request = request.bearer_auth(token);
            }
            request.send().await?
        };

        let status = response.status();
        if status.is_redirection() {
            let Some(location) = response
                .headers()
                .get("location")
                .and_then(|value| value.to_str().ok())
            else {
                return Err(Error::Status {
                    status: status.as_u16(),
                    url: current,
                    body: "redirect without a Location header".to_string(),
                });
            };
            current = Url::parse(&current)?.join(location)?.to_string();
            auth_required = same_host(&current, endpoint);
            continue;
        }

        match status {
            StatusCode::OK | StatusCode::PARTIAL_CONTENT => {
                if let Some(length) = header_u64(response.headers(), "content-length")
                    && file_size > 0
                    && length != file_size
                {
                    return Err(Error::Invalid(format!(
                        "expected {file_size} bytes for {path}, server sent {length}"
                    )));
                }
                file.seek(SeekFrom::Start(0)).await?;
                file.set_len(0).await?;
                let mut written = 0u64;
                let mut stream = response.bytes_stream();
                while let Some(chunk) = stream.next().await {
                    let chunk = match chunk {
                        Ok(chunk) => chunk,
                        Err(_) => {
                            tracker.inflight[0].store(0, Ordering::Relaxed);
                            return Err(Error::Retryable {
                                status: 0,
                                retry_after: None,
                            });
                        }
                    };
                    if let Some(limiter) = limiter {
                        limiter.take(chunk.len()).await;
                    }
                    file.write_all(&chunk).await?;
                    written += chunk.len() as u64;
                    tracker.inflight[0].store(written, Ordering::Relaxed);
                }
                if file_size > 0 && written != file_size {
                    return Err(Error::Retryable {
                        status: 0,
                        retry_after: None,
                    });
                }
                return Ok(());
            }
            StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => {
                let body = response.text().await.unwrap_or_default();
                return Err(Error::Auth {
                    repo: current,
                    detail: truncate_body(body),
                });
            }
            StatusCode::TOO_MANY_REQUESTS => {
                let retry_after = header_u64(response.headers(), "retry-after");
                return Err(Error::Retryable {
                    status: 429,
                    retry_after: retry_after.or(Some(5)),
                });
            }
            status if status.is_server_error() || status == StatusCode::REQUEST_TIMEOUT => {
                let retry_after = header_u64(response.headers(), "retry-after");
                return Err(Error::Retryable {
                    status: status.as_u16(),
                    retry_after,
                });
            }
            _ => {
                let body = response.text().await.unwrap_or_default();
                return Err(Error::Status {
                    status: status.as_u16(),
                    url: current,
                    body: truncate_body(body),
                });
            }
        }
    }

    Err(Error::Invalid(format!(
        "too many redirects while downloading {path}"
    )))
}

struct UrlCache {
    hub: HubClient,
    repo: RepoRef,
    path: String,
    revision: String,
    inner: tokio::sync::Mutex<CachedUrl>,
}

struct CachedUrl {
    location: String,
    auth_required: bool,
    expires_at: Option<SystemTime>,
    refreshed_at: Instant,
}

impl UrlCache {
    #[allow(clippy::too_many_arguments)]
    fn new(
        hub: HubClient,
        repo: RepoRef,
        path: String,
        revision: String,
        location: String,
        auth_required: bool,
        expires_at: Option<SystemTime>,
    ) -> Self {
        Self {
            hub,
            repo,
            path,
            revision,
            inner: tokio::sync::Mutex::new(CachedUrl {
                location,
                auth_required,
                expires_at,
                refreshed_at: Instant::now(),
            }),
        }
    }

    async fn get(&self) -> Result<(String, bool)> {
        let mut guard = self.inner.lock().await;
        if !is_expired(&guard) {
            return Ok((guard.location.clone(), guard.auth_required));
        }
        let fresh = self
            .hub
            .resolve_location(&self.repo, &self.path, &self.revision)
            .await?;
        guard.location = fresh.location.clone();
        guard.auth_required = fresh.auth_required;
        guard.expires_at = fresh.expires_at;
        guard.refreshed_at = Instant::now();
        Ok((fresh.location, fresh.auth_required))
    }

    async fn refresh(&self) -> Result<()> {
        let mut guard = self.inner.lock().await;
        let fresh = self
            .hub
            .resolve_location(&self.repo, &self.path, &self.revision)
            .await?;
        guard.location = fresh.location;
        guard.auth_required = fresh.auth_required;
        guard.expires_at = fresh.expires_at;
        guard.refreshed_at = Instant::now();
        Ok(())
    }
}

fn is_expired(cached: &CachedUrl) -> bool {
    if cached.location.is_empty() {
        return true;
    }
    if let Some(expires_at) = cached.expires_at {
        let now = SystemTime::now();
        let margin = Duration::from_secs(300);
        if let Some(still_valid) = expires_at.checked_sub(margin) {
            return now >= still_valid;
        }
        return true;
    }
    cached.refreshed_at.elapsed() > Duration::from_secs(30 * 60)
}

fn header_value(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(|value| value.trim().to_string())
}

fn header_u64(headers: &HeaderMap, name: &str) -> Option<u64> {
    header_value(headers, name).and_then(|value| value.parse().ok())
}

fn parse_content_range(value: &str) -> Result<(u64, u64, u64)> {
    let value = value.trim();
    let rest = value
        .strip_prefix("bytes ")
        .ok_or_else(|| Error::Invalid(format!("invalid Content-Range '{value}'")))?;
    let (range, total) = rest
        .split_once('/')
        .ok_or_else(|| Error::Invalid(format!("invalid Content-Range '{value}'")))?;
    let (start, end) = range
        .split_once('-')
        .ok_or_else(|| Error::Invalid(format!("invalid Content-Range '{value}'")))?;
    let start = start
        .parse()
        .map_err(|_| Error::Invalid(format!("invalid Content-Range '{value}'")))?;
    let end = end
        .parse()
        .map_err(|_| Error::Invalid(format!("invalid Content-Range '{value}'")))?;
    let total = total
        .parse()
        .map_err(|_| Error::Invalid(format!("invalid Content-Range '{value}'")))?;
    Ok((start, end, total))
}

fn same_host(left: &str, right: &str) -> bool {
    let left = Url::parse(left).ok().and_then(|url| {
        url.host_str()
            .map(|host| (host.to_ascii_lowercase(), url.port_or_known_default()))
    });
    let right = Url::parse(right).ok().and_then(|url| {
        url.host_str()
            .map(|host| (host.to_ascii_lowercase(), url.port_or_known_default()))
    });
    matches!((left, right), (Some(a), Some(b)) if a == b)
}

fn backoff(attempt: u32, retry_after: Option<u64>) -> Duration {
    let base = retry_after
        .map(|seconds| seconds.saturating_mul(1000))
        .unwrap_or_else(|| 500u64.saturating_mul(1u64 << attempt.min(5)));
    let jitter = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_nanos() as u64
        % 250;
    Duration::from_millis(base.saturating_add(jitter).min(60_000))
}

fn truncate_body(mut body: String) -> String {
    body = body.trim().to_string();
    if body.len() > 2000 {
        body.truncate(2000);
        body.push('…');
    }
    body
}

fn is_git_sha1(value: &str) -> bool {
    value.len() == 40 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

/// Git blob hash: sha1 over "blob <size>\0" followed by the file bytes.
fn git_blob_sha1(path: &Path, size: u64) -> Result<String> {
    use sha1::{Digest, Sha1};
    let mut file = fs::File::open(path)?;
    let mut hasher = Sha1::new();
    hasher.update(format!("blob {size}\0").as_bytes());
    let mut buffer = vec![0u8; 1024 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(hex::encode(hasher.finalize()))
}

fn hash_file(path: &Path) -> Result<String> {
    let mut file = fs::File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buffer = vec![0u8; 1024 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(hex::encode(hasher.finalize()))
}

pub(crate) fn remove_file_if_exists(path: &Path) -> Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

const XET_FAILURE_TTL: Duration = Duration::from_secs(24 * 60 * 60);

fn xet_marker_path(config: &crate::config::Config) -> PathBuf {
    let host = Url::parse(&config.endpoint)
        .ok()
        .and_then(|url| url.host_str().map(str::to_string))
        .unwrap_or_else(|| "default".to_string());
    config.xet_cache_dir.join(format!("hfd-xet-failed-{host}"))
}

/// True when Xet failed on this endpoint in the last 24 hours, so downloads go straight to HTTP.
pub fn xet_failed_recently(config: &crate::config::Config) -> bool {
    xet_recently_failed(&xet_marker_path(config))
}

fn xet_recently_failed(marker: &Path) -> bool {
    fs::metadata(marker)
        .and_then(|meta| meta.modified())
        .ok()
        .and_then(|time| time.elapsed().ok())
        .is_some_and(|age| age < XET_FAILURE_TTL)
}

#[cfg(test)]
mod tests {

    #[test]
    fn git_blob_sha1_matches_git_hash_object() {
        // `printf 'hello\n' | git hash-object --stdin` = ce013625030ba8dba906f756967f9e9ca394464a
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("hello.txt");
        std::fs::write(&path, b"hello\n").unwrap();
        assert_eq!(
            git_blob_sha1(&path, 6).unwrap(),
            "ce013625030ba8dba906f756967f9e9ca394464a"
        );
        assert!(is_git_sha1("ce013625030ba8dba906f756967f9e9ca394464a"));
        assert!(!is_git_sha1("ce0136"));
    }
    use super::*;

    #[test]
    fn xet_failure_marker_expires() {
        let dir = tempfile::tempdir().unwrap();
        let marker = dir.path().join("hfd-xet-failed-test");
        assert!(!xet_recently_failed(&marker));
        fs::write(&marker, b"").unwrap();
        assert!(xet_recently_failed(&marker));
        let old = std::time::SystemTime::now() - XET_FAILURE_TTL - Duration::from_secs(60);
        fs::File::options()
            .write(true)
            .open(&marker)
            .unwrap()
            .set_modified(old)
            .unwrap();
        assert!(!xet_recently_failed(&marker));
    }

    #[test]
    fn parses_content_range() {
        assert_eq!(
            parse_content_range("bytes 0-99/1000").unwrap(),
            (0, 99, 1000)
        );
        assert!(parse_content_range("bytes 0-99/*").is_err());
    }

    #[test]
    fn selects_download_engine() {
        assert_eq!(Engine::Auto.select(None).unwrap(), Engine::Http);
        assert_eq!(Engine::Auto.select(Some("abc")).unwrap(), Engine::Xet);
        assert_eq!(Engine::Http.select(Some("abc")).unwrap(), Engine::Http);
        assert_eq!(Engine::Xet.select(Some("abc")).unwrap(), Engine::Xet);
        assert!(Engine::Xet.select(None).is_err());
    }

    #[test]
    fn chooses_unit_sizes() {
        assert_eq!(unit_size_for(100), 1);
        assert_eq!(unit_size_for(2 * MIB), 8 * MIB);
        assert_eq!(unit_size_for(2 * 1024 * MIB), 16 * MIB);
        assert_eq!(unit_size_for(50 * 1024 * MIB), 32 * MIB);
        assert_eq!(unit_size_for(200 * 1024 * MIB), 64 * MIB);
    }

    #[test]
    fn computes_backoff() {
        assert!(backoff(1, None) >= Duration::from_millis(1000));
        assert!(backoff(1, Some(2)) >= Duration::from_millis(2000));
    }
}

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
    #[default]
    Auto,
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
    pub force: bool,
    pub verify: VerifyMode,
    pub connections: usize,
    pub engine: Engine,
}

impl Default for DownloadOptions {
    fn default() -> Self {
        Self {
            cache_dir: PathBuf::from("."),
            local_dir: None,
            force: false,
            verify: VerifyMode::Auto,
            connections: 8,
            engine: Engine::Auto,
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
}

#[derive(Clone)]
pub struct Downloader {
    hub: HubClient,
    options: DownloadOptions,
    pub(crate) xet: Arc<tokio::sync::OnceCell<xet::xet_session::XetSession>>,
}

impl Downloader {
    pub fn new(hub: HubClient, options: DownloadOptions) -> Self {
        Self {
            hub,
            options,
            xet: Arc::new(tokio::sync::OnceCell::new()),
        }
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
        )?;
        let complete = target.is_complete(resolved.size, self.options.force);
        let engine = if complete {
            Engine::Http
        } else {
            self.options.engine.select(resolved.xet_hash.as_deref())?
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

    pub async fn download_file(
        &self,
        repo: &RepoRef,
        path: &str,
        revision: &str,
        sink: &EventSink,
    ) -> Result<DownloadedFile> {
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
                path: path.to_string(),
                local_path,
                size: resolved.size,
                skipped: true,
                resumed_bytes: 0,
                engine: "cache",
            });
        }

        let engine = self.options.engine.select(resolved.xet_hash.as_deref())?;

        if let Some(parent) = target.partial_path().parent() {
            fs::create_dir_all(parent)?;
        }
        self.check_space(&target, resolved.size)?;

        let xet_result = if engine == Engine::Xet {
            match self
                .download_xet(repo, path, revision, &resolved, &target, sink)
                .await
            {
                // In auto mode, a failing Xet transfer falls back to plain HTTP.
                Err(error @ (Error::Xet(_) | Error::XetStalled(_)))
                    if self.options.engine == Engine::Auto =>
                {
                    sink(Event::Message(format!(
                        "{path}: {error}; falling back to HTTP"
                    )));
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

        let client = self.hub.http_client().clone();
        let token = self.hub.token().map(str::to_string);
        let endpoint = self.hub.config().endpoint.clone();
        let worker_count = self.options.connections.max(1).min(units.max(1) as usize);

        let mut tasks = tokio::task::JoinSet::new();
        for _ in 0..worker_count {
            tasks.spawn(worker(
                client.clone(),
                endpoint.clone(),
                token.clone(),
                url_cache.clone(),
                state.clone(),
                tracker.clone(),
                next.clone(),
                units,
                resolved.size,
                partial_path.clone(),
                state_path.clone(),
                checkpoint_lock.clone(),
                path.to_string(),
            ));
        }

        let mut first_error = None;
        while let Some(result) = tasks.join_next().await {
            match result {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    first_error = Some(error);
                    tasks.abort_all();
                    break;
                }
                Err(error) => {
                    first_error = Some(Error::Invalid(format!("download worker failed: {error}")));
                    tasks.abort_all();
                    break;
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
        let Some(expected) = expected else {
            if self.options.verify == VerifyMode::Always {
                return Err(Error::Invalid(format!(
                    "no SHA256 checksum is available for {}",
                    target.reported_path().display()
                )));
            }
            return Ok(());
        };

        sink(Event::Message(format!(
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
}

impl Tracker {
    fn new(total: u64, completed: u64, units: u64) -> Self {
        Self {
            total,
            completed: AtomicU64::new(completed),
            inflight: (0..units).map(|_| AtomicU64::new(0)).collect(),
        }
    }

    fn downloaded(&self) -> u64 {
        let inflight: u64 = self
            .inflight
            .iter()
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

#[allow(clippy::too_many_arguments)]
async fn worker(
    client: Client,
    endpoint: String,
    token: Option<String>,
    url_cache: Arc<UrlCache>,
    state: Arc<Mutex<DownloadState>>,
    tracker: Arc<Tracker>,
    next: Arc<AtomicU64>,
    units: u64,
    file_size: u64,
    partial_path: PathBuf,
    state_path: PathBuf,
    checkpoint_lock: Arc<tokio::sync::Mutex<()>>,
    path: String,
) -> Result<()> {
    let mut file = match tokio::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&partial_path)
        .await
    {
        Ok(file) => file,
        Err(error) => return Err(error.into()),
    };

    loop {
        let index = next.fetch_add(1, Ordering::Relaxed);
        if index >= units {
            break;
        }
        if state.lock().unwrap().is_complete(index) {
            continue;
        }

        let Some((start, end)) = state.lock().unwrap().range(index) else {
            break;
        };
        let expected = end.saturating_sub(start);
        if expected == 0 {
            state.lock().unwrap().mark_complete(index);
            continue;
        }
        tracker.inflight[index as usize].store(0, Ordering::Relaxed);

        let mut attempt = 0u32;
        let mut auth_attempts = 0u32;
        loop {
            let (url, auth_required) = match url_cache.get().await {
                Ok(value) => value,
                Err(error) => {
                    return Err(error);
                }
            };

            match fetch_range(
                &client,
                &endpoint,
                token.as_deref(),
                &url,
                auth_required,
                start,
                end,
                file_size,
                &mut file,
                &tracker,
                index,
                &path,
            )
            .await
            {
                Ok(()) => break,
                Err(Error::Auth { .. }) if auth_attempts < 1 => {
                    auth_attempts += 1;
                    url_cache.refresh().await?;
                    tracker.inflight[index as usize].store(0, Ordering::Relaxed);
                }
                Err(error) if error.is_retryable() && attempt < MAX_RETRIES => {
                    attempt += 1;
                    tokio::time::sleep(backoff(attempt, error.retry_after())).await;
                    tracker.inflight[index as usize].store(0, Ordering::Relaxed);
                }
                Err(error) => return Err(error),
            }
        }

        let completed = {
            let mut guard = state.lock().unwrap();
            guard.mark_complete(index);
            guard.completed_bytes()
        };
        tracker.completed.store(completed, Ordering::Relaxed);
        tracker.inflight[index as usize].store(0, Ordering::Relaxed);

        let units_done = state.lock().unwrap().completed_units();
        if units_done.is_multiple_of(CHECKPOINT_UNITS) {
            let _guard = checkpoint_lock.lock().await;
            let snapshot = state.lock().unwrap().clone();
            let path = state_path.clone();
            let _ = tokio::task::spawn_blocking(move || snapshot.save(&path)).await;
        }
    }

    Ok::<(), Error>(())
}

#[allow(clippy::too_many_arguments)]
async fn fetch_range(
    client: &Client,
    endpoint: &str,
    token: Option<&str>,
    url: &str,
    auth_required: bool,
    start: u64,
    end: u64,
    file_size: u64,
    file: &mut tokio::fs::File,
    tracker: &Tracker,
    unit: u64,
    path: &str,
) -> Result<()> {
    let mut current = url.to_string();
    let mut auth_required = auth_required;

    for _ in 0..5 {
        let send_auth = auth_required && same_host(&current, endpoint);
        let response = {
            let mut request = client
                .get(&current)
                .header(RANGE, format!("bytes={}-{}", start, end - 1))
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
            StatusCode::PARTIAL_CONTENT => {
                if let Some(value) = header_value(response.headers(), "content-range") {
                    let (got_start, got_end, total) = parse_content_range(&value)?;
                    if got_start != start || got_end + 1 > end || total != file_size {
                        return Err(Error::Invalid(format!(
                            "unexpected Content-Range '{value}' for {path}"
                        )));
                    }
                }
                file.seek(SeekFrom::Start(start)).await?;
                let expected = end.saturating_sub(start);
                let mut written = 0u64;
                let mut stream = response.bytes_stream();
                while let Some(chunk) = stream.next().await {
                    let chunk = match chunk {
                        Ok(chunk) => chunk,
                        Err(_) => {
                            tracker.inflight[unit as usize].store(0, Ordering::Relaxed);
                            return Err(Error::Retryable {
                                status: 0,
                                retry_after: None,
                            });
                        }
                    };
                    if written + chunk.len() as u64 > expected {
                        return Err(Error::Invalid(format!(
                            "server sent too many bytes for {path}"
                        )));
                    }
                    file.write_all(&chunk).await?;
                    written += chunk.len() as u64;
                    tracker.inflight[unit as usize].store(written, Ordering::Relaxed);
                }
                if written != expected {
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

#[cfg(test)]
mod tests {
    use super::*;

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

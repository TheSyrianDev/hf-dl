use std::collections::HashMap;
use std::env;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow, bail};
use fs2::FileExt;
use hfd_core::auth::resolve_token;
use hfd_core::{
    Config, DownloadOptions, Downloader, Engine, Event, EventSink, HubClient, RepoRef, RepoType,
    VerifyMode, parse_target, validate_repo_path,
};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::Notify;

use crate::{
    DownloadArgs, OutputFormat, QueueArgs, QueueCommand, build_globset, collect_plans,
    execute_plans, human_size,
};

const VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct JobRequest {
    target: String,
    #[serde(default)]
    filenames: Vec<String>,
    #[serde(default)]
    repo_type: Option<RepoType>,
    #[serde(default)]
    revision: Option<String>,
    #[serde(default)]
    include: Vec<String>,
    #[serde(default)]
    exclude: Vec<String>,
    #[serde(default)]
    cache_dir: Option<PathBuf>,
    #[serde(default)]
    local_dir: Option<PathBuf>,
    #[serde(default)]
    flat: bool,
    #[serde(default)]
    force: bool,
    #[serde(default = "default_connections")]
    connections: usize,
    #[serde(default = "default_workers")]
    max_workers: usize,
    #[serde(default = "default_verify")]
    verify: VerifyMode,
    #[serde(default)]
    engine: Engine,
    #[serde(default)]
    max_speed: Option<u64>,
}

fn default_connections() -> usize {
    8
}

fn default_workers() -> usize {
    8
}

fn default_verify() -> VerifyMode {
    VerifyMode::Auto
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
enum JobStatus {
    Queued,
    Running,
    Paused,
    Completed,
    Failed,
    Cancelled,
}

/// Status after a pause request, or the reason it is refused.
fn pause_status(current: JobStatus) -> Result<JobStatus, String> {
    match current {
        JobStatus::Queued | JobStatus::Running => Ok(JobStatus::Paused),
        other => Err(format!("is {} and cannot be paused", other.as_str())),
    }
}

/// Status after a resume request, or the reason it is refused.
fn resume_status(current: JobStatus) -> Result<JobStatus, String> {
    match current {
        JobStatus::Paused => Ok(JobStatus::Queued),
        other => Err(format!("is {} and cannot be resumed", other.as_str())),
    }
}

impl JobStatus {
    fn as_str(self) -> &'static str {
        match self {
            JobStatus::Queued => "queued",
            JobStatus::Running => "running",
            JobStatus::Paused => "paused",
            JobStatus::Completed => "completed",
            JobStatus::Failed => "failed",
            JobStatus::Cancelled => "cancelled",
        }
    }

    fn is_terminal(self) -> bool {
        matches!(
            self,
            JobStatus::Completed | JobStatus::Failed | JobStatus::Cancelled
        )
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct JobProgress {
    #[serde(default)]
    total_files: usize,
    #[serde(default)]
    completed_files: usize,
    #[serde(default)]
    failed_files: usize,
    #[serde(default)]
    skipped_files: usize,
    #[serde(default)]
    total_bytes: u64,
    #[serde(default)]
    downloaded_bytes: u64,
    #[serde(default)]
    current: Option<String>,
    #[serde(default)]
    message: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Job {
    id: String,
    status: JobStatus,
    created_at: u64,
    updated_at: u64,
    #[serde(default)]
    started_at: Option<u64>,
    #[serde(default)]
    finished_at: Option<u64>,
    request: JobRequest,
    #[serde(default)]
    progress: JobProgress,
    #[serde(default)]
    error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct QueueState {
    #[serde(default = "default_version")]
    version: u32,
    #[serde(default)]
    jobs: Vec<Job>,
}

fn default_version() -> u32 {
    VERSION
}

impl Default for QueueState {
    fn default() -> Self {
        Self {
            version: VERSION,
            jobs: Vec::new(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "cmd", rename_all = "snake_case")]
#[allow(clippy::large_enum_variant)]
enum Request {
    Submit {
        request: JobRequest,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        token: Option<String>,
    },
    List,
    Status {
        id: String,
    },
    Cancel {
        id: String,
    },
    Pause {
        id: String,
    },
    Resume {
        id: String,
    },
    Clear,
    Ping,
    Shutdown,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct Response {
    ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    job: Option<Job>,
    #[serde(skip_serializing_if = "Option::is_none")]
    jobs: Option<Vec<Job>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    message: Option<String>,
}

impl Response {
    fn ok() -> Self {
        Self {
            ok: true,
            ..Self::default()
        }
    }

    fn error(error: impl Into<String>) -> Self {
        Self {
            ok: false,
            error: Some(error.into()),
            ..Self::default()
        }
    }
}

#[derive(Clone)]
struct DaemonContext {
    state: Arc<Mutex<QueueState>>,
    state_path: PathBuf,
    log_path: PathBuf,
    running: Arc<Mutex<Option<RunningJob>>>,
    tokens: Arc<Mutex<HashMap<String, String>>>,
    shutdown: Arc<Notify>,
}

struct RunningJob {
    id: String,
    abort: tokio::task::AbortHandle,
}

struct Paths {
    state_dir: PathBuf,
    state_path: PathBuf,
    log_path: PathBuf,
    lock_path: PathBuf,
    socket_path: PathBuf,
}

impl Paths {
    fn new() -> Result<Self> {
        let state_dir = state_dir()?;
        let socket_path = socket_path()?;
        Ok(Self {
            state_path: state_dir.join("queue.json"),
            log_path: state_dir.join("daemon.log"),
            lock_path: state_dir.join("daemon.lock"),
            state_dir,
            socket_path,
        })
    }

    fn ensure(&self) -> Result<()> {
        let state_dir_existed = self.state_dir.exists();
        fs::create_dir_all(&self.state_dir)?;
        if !state_dir_existed {
            set_private_dir(&self.state_dir)?;
        }
        if let Some(parent) = self.socket_path.parent() {
            let parent_existed = parent.exists();
            fs::create_dir_all(parent)?;
            if !parent_existed {
                set_private_dir(parent)?;
            }
        }
        Ok(())
    }
}

fn state_dir() -> Result<PathBuf> {
    if let Some(dir) = env::var_os("HFD_STATE_DIR") {
        return Ok(PathBuf::from(dir));
    }
    let base = env::var_os("XDG_STATE_HOME")
        .map(PathBuf::from)
        .or_else(|| {
            env::var_os("HOME").map(|home| PathBuf::from(home).join(".local").join("state"))
        })
        .ok_or_else(|| anyhow!("cannot determine state directory; set HOME or HFD_STATE_DIR"))?;
    Ok(base.join("hf-dl"))
}

fn socket_path() -> Result<PathBuf> {
    if let Some(dir) = env::var_os("HFD_RUNTIME_DIR") {
        return Ok(PathBuf::from(dir).join("daemon.sock"));
    }
    if let Some(dir) = env::var_os("XDG_RUNTIME_DIR") {
        return Ok(PathBuf::from(dir).join("hf-dl").join("daemon.sock"));
    }
    Ok(env::temp_dir()
        .join(format!("hf-dl-{}", current_uid()))
        .join("daemon.sock"))
}

#[cfg(unix)]
fn current_uid() -> u32 {
    use std::os::unix::fs::MetadataExt;
    fs::metadata("/proc/self")
        .map(|metadata| metadata.uid())
        .unwrap_or_else(|_| std::process::id())
}

#[cfg(unix)]
fn set_private_dir(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    let mut permissions = fs::metadata(path)?.permissions();
    permissions.set_mode(0o700);
    fs::set_permissions(path, permissions)?;
    Ok(())
}

fn private_open_options() -> OpenOptions {
    let mut options = OpenOptions::new();
    options.create(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options
}

fn load_state(path: &Path) -> Result<QueueState> {
    match fs::read(path) {
        Ok(bytes) => {
            let mut state: QueueState = serde_json::from_slice(&bytes)
                .with_context(|| format!("invalid queue state at {}", path.display()))?;
            state.version = VERSION;
            Ok(state)
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(QueueState::default()),
        Err(error) => Err(error.into()),
    }
}

fn save_state(path: &Path, state: &QueueState) -> Result<()> {
    let tmp = path.with_extension("json.tmp");
    let bytes = serde_json::to_vec_pretty(state)?;
    {
        let mut options = private_open_options();
        options.truncate(true);
        let mut file = options.open(&tmp)?;
        file.write_all(&bytes)?;
        file.write_all(b"\n")?;
        file.sync_all()?;
    }
    fs::rename(&tmp, path)?;
    Ok(())
}

fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0)
}

fn default_request(args: &DownloadArgs, engine: Engine) -> JobRequest {
    JobRequest {
        target: args.repo_id.clone(),
        filenames: args
            .filenames
            .iter()
            .map(|name| name.trim_end_matches('/').to_string())
            .collect(),
        repo_type: args.repo_type.map(RepoType::from),
        revision: args.revision.clone(),
        include: args.include.clone(),
        exclude: args.exclude.clone(),
        cache_dir: args.cache_dir.clone(),
        local_dir: args.local_dir.clone(),
        flat: args.flat,
        force: args.force_download,
        connections: args.connections,
        max_workers: args.max_workers,
        verify: args.verify.into(),
        engine,
        max_speed: args.max_speed,
    }
}

fn absolute_path(path: PathBuf) -> PathBuf {
    if path.is_absolute() {
        path
    } else {
        env::current_dir()
            .map(|cwd| cwd.join(&path))
            .unwrap_or(path)
    }
}

pub async fn enqueue_download(args: &DownloadArgs) -> Result<()> {
    if args.dry_run {
        bail!("--dry-run cannot be combined with --detach");
    }
    if args.no_daemon {
        bail!("--no-daemon cannot be combined with --detach");
    }
    if args.foreground {
        bail!("--foreground cannot be combined with --detach");
    }
    let engine = args.engine.resolve(args.no_xet)?;

    let config = Config::from_env();
    let uri = parse_target(&args.repo_id, &config.endpoint)?;
    let cli_repo_type = args.repo_type.map(RepoType::from);
    if let (Some(uri_type), Some(cli_type)) = (uri.repo_type, cli_repo_type)
        && uri_type != cli_type
    {
        bail!("repository type mismatch: target is a {uri_type}, but --type is {cli_type}");
    }
    if let (Some(uri_revision), Some(cli_revision)) =
        (uri.revision.as_deref(), args.revision.as_deref())
        && uri_revision != cli_revision
    {
        bail!(
            "revision mismatch: target uses '{uri_revision}', but --revision is '{cli_revision}'"
        );
    }
    if !uri.path.is_empty() && !args.filenames.is_empty() {
        bail!(
            "the target already selects '{}'; pass either a file path in the URI or positional FILENAMES, not both",
            uri.path
        );
    }
    for name in &args.filenames {
        validate_repo_path(name.trim_end_matches('/'))
            .with_context(|| format!("invalid repository path '{name}'"))?;
    }
    build_globset(&args.include)?;
    build_globset(&args.exclude)?;

    ensure_daemon().await?;
    let mut request = default_request(args, engine);
    request.cache_dir = request.cache_dir.map(absolute_path);
    request.local_dir = request.local_dir.map(absolute_path);
    let response = send(Request::Submit {
        request,
        token: args.token.clone(),
    })
    .await?;
    let job = response
        .job
        .ok_or_else(|| anyhow!("daemon returned no job"))?;

    if args.quiet {
        println!("{}", job.id);
    } else if args.json || matches!(args.format, OutputFormat::Json) {
        println!("{}", serde_json::to_string(&job)?);
    } else if matches!(args.format, OutputFormat::Agent) {
        println!("queued\t{}\t{}", job.id, job.request.target);
    } else {
        println!("Queued job {}", job.id);
        println!("Target: {}", job.request.target);
        println!("Follow: hfd queue list");
    }
    Ok(())
}

pub async fn run_queue(args: QueueArgs) -> Result<()> {
    match args.command {
        QueueCommand::List { json } => list_jobs(json).await,
        QueueCommand::Show { id } => show_job(&id).await,
        QueueCommand::Cancel { id } => cancel_job(&id).await,
        QueueCommand::Pause { id } => pause_or_resume_job(&id, true).await,
        QueueCommand::Resume { id } => pause_or_resume_job(&id, false).await,
        QueueCommand::Clear => clear_jobs().await,
    }
}

async fn list_jobs(json: bool) -> Result<()> {
    let alive = daemon_alive().await;
    let mut jobs = if alive {
        send(Request::List).await?.jobs.unwrap_or_default()
    } else {
        load_state(&Paths::new()?.state_path)?.jobs
    };
    jobs.sort_by_key(|job| job.created_at);
    if json {
        println!("{}", serde_json::to_string_pretty(&jobs)?);
        return Ok(());
    }
    if jobs.is_empty() {
        println!("No jobs.");
        return Ok(());
    }

    println!(
        "{:<28} {:<10} {:>9} {:>23}  TARGET",
        "ID", "STATUS", "FILES", "BYTES"
    );
    for job in &jobs {
        let progress = &job.progress;
        let files = format!("{}/{}", progress.completed_files, progress.total_files);
        let bytes = format!(
            "{}/{}",
            human_size(progress.downloaded_bytes),
            human_size(progress.total_bytes)
        );
        println!(
            "{:<28} {:<10} {:>9} {:>23}  {}",
            job.id,
            job.status.as_str(),
            files,
            bytes,
            job.request.target
        );
        if let Some(current) = &progress.current {
            println!("  current: {current}");
        }
        if let Some(error) = &job.error {
            println!("  error: {error}");
        }
    }
    if !alive && jobs.iter().any(|job| job.status == JobStatus::Queued) {
        println!();
        println!("Daemon is not running; run `hfd daemon` to resume queued jobs.");
    }
    Ok(())
}

async fn show_job(id: &str) -> Result<()> {
    let job = if daemon_alive().await {
        send(Request::Status { id: id.to_string() })
            .await?
            .job
            .ok_or_else(|| anyhow!("job not found"))?
    } else {
        load_state(&Paths::new()?.state_path)?
            .jobs
            .into_iter()
            .find(|job| job.id == id)
            .ok_or_else(|| anyhow!("job not found"))?
    };
    println!("{}", serde_json::to_string_pretty(&job)?);
    Ok(())
}

async fn cancel_job(id: &str) -> Result<()> {
    if daemon_alive().await {
        let response = send(Request::Cancel { id: id.to_string() }).await?;
        println!(
            "{}",
            response.message.unwrap_or_else(|| "Cancelled.".to_string())
        );
        return Ok(());
    }

    let paths = Paths::new()?;
    let mut state = load_state(&paths.state_path)?;
    let Some(job) = state.jobs.iter_mut().find(|job| job.id == id) else {
        bail!("job '{id}' not found");
    };
    if job.status.is_terminal() {
        bail!("job '{id}' is already {}", job.status.as_str());
    }
    job.status = JobStatus::Cancelled;
    job.progress.current = None;
    job.updated_at = now();
    job.finished_at = Some(now());
    save_state(&paths.state_path, &state)?;
    println!("Cancelled job {id}.");
    Ok(())
}

async fn pause_or_resume_job(id: &str, pause: bool) -> Result<()> {
    if daemon_alive().await {
        let request = if pause {
            Request::Pause { id: id.to_string() }
        } else {
            Request::Resume { id: id.to_string() }
        };
        let response = send(request).await?;
        if !response.ok {
            bail!(
                "{}",
                response
                    .error
                    .unwrap_or_else(|| "request failed".to_string())
            );
        }
        println!("{}", response.message.unwrap_or_default());
        return Ok(());
    }

    let paths = Paths::new()?;
    let mut state = load_state(&paths.state_path)?;
    let Some(job) = state.jobs.iter_mut().find(|job| job.id == id) else {
        bail!("job '{id}' not found");
    };
    job.status = if pause {
        pause_status(job.status).map_err(|error| anyhow!("job '{id}' {error}"))?
    } else {
        resume_status(job.status).map_err(|error| anyhow!("job '{id}' {error}"))?
    };
    job.progress.current = None;
    job.updated_at = now();
    save_state(&paths.state_path, &state)?;
    println!("{} job {id}.", if pause { "Paused" } else { "Resumed" });
    Ok(())
}

async fn clear_jobs() -> Result<()> {
    if daemon_alive().await {
        let response = send(Request::Clear).await?;
        println!(
            "{}",
            response.message.unwrap_or_else(|| "Cleared.".to_string())
        );
        return Ok(());
    }

    let paths = Paths::new()?;
    let mut state = load_state(&paths.state_path)?;
    let before = state.jobs.len();
    state.jobs.retain(|job| !job.status.is_terminal());
    let removed = before - state.jobs.len();
    save_state(&paths.state_path, &state)?;
    println!("Removed {removed} finished job(s).");
    Ok(())
}

pub async fn daemon_command(stop: bool) -> Result<()> {
    if stop {
        if !daemon_alive().await {
            println!("Daemon is not running.");
            return Ok(());
        }
        let response = send(Request::Shutdown).await?;
        println!(
            "{}",
            response
                .message
                .unwrap_or_else(|| "Stopping daemon.".to_string())
        );
        return Ok(());
    }
    run_daemon().await
}

async fn send(request: Request) -> Result<Response> {
    let path = socket_path()?;
    let mut stream = tokio::time::timeout(Duration::from_secs(3), UnixStream::connect(&path))
        .await
        .map_err(|_| anyhow!("daemon connection timed out at {}", path.display()))??;
    let mut payload = serde_json::to_vec(&request)?;
    payload.push(b'\n');
    stream.write_all(&payload).await?;

    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    tokio::time::timeout(Duration::from_secs(30), reader.read_line(&mut line))
        .await
        .map_err(|_| anyhow!("daemon did not respond"))??;
    if line.trim().is_empty() {
        bail!("daemon closed the connection without a response");
    }
    let response: Response = serde_json::from_str(line.trim())?;
    if !response.ok {
        bail!(
            response
                .error
                .unwrap_or_else(|| "daemon request failed".to_string())
        );
    }
    Ok(response)
}

async fn daemon_alive() -> bool {
    let Ok(path) = socket_path() else {
        return false;
    };
    let Ok(Ok(stream)) =
        tokio::time::timeout(Duration::from_secs(1), UnixStream::connect(&path)).await
    else {
        return false;
    };
    let mut stream = stream;
    if stream.write_all(b"{\"cmd\":\"ping\"}\n").await.is_err() {
        return false;
    }
    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    if tokio::time::timeout(Duration::from_secs(1), reader.read_line(&mut line))
        .await
        .is_err()
    {
        return false;
    }
    serde_json::from_str::<Response>(line.trim()).is_ok_and(|response| response.ok)
}

async fn ensure_daemon() -> Result<()> {
    if daemon_alive().await {
        return Ok(());
    }

    let exe = env::current_exe().context("cannot locate the hfd executable")?;
    Command::new(exe)
        .arg("__daemon")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .context("failed to start the hfd daemon")?;

    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        if daemon_alive().await {
            return Ok(());
        }
    }
    bail!("the hfd daemon did not start; run `hfd daemon` to see the error")
}

async fn run_daemon() -> Result<()> {
    let paths = Paths::new()?;
    paths.ensure()?;

    let lock = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(&paths.lock_path)?;
    if lock.try_lock_exclusive().is_err() {
        println!("Daemon is already running.");
        return Ok(());
    }

    let mut state = load_state(&paths.state_path)?;
    let mut resumed = false;
    for job in &mut state.jobs {
        if job.status == JobStatus::Running {
            job.status = JobStatus::Queued;
            job.progress.current = None;
            job.updated_at = now();
            resumed = true;
        }
    }
    if resumed {
        save_state(&paths.state_path, &state)?;
    }

    match fs::remove_file(&paths.socket_path) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    let listener = UnixListener::bind(&paths.socket_path)
        .with_context(|| format!("cannot bind {}", paths.socket_path.display()))?;
    set_socket_permissions(&paths.socket_path)?;

    let context = DaemonContext {
        state: Arc::new(Mutex::new(state)),
        state_path: paths.state_path.clone(),
        log_path: paths.log_path.clone(),
        running: Arc::new(Mutex::new(None)),
        tokens: Arc::new(Mutex::new(HashMap::new())),
        shutdown: Arc::new(Notify::new()),
    };
    log_line(
        &paths.log_path,
        &format!("daemon started (socket {})", paths.socket_path.display()),
    );

    let worker = tokio::spawn(worker_loop(context.clone()));
    let ctrl_c = tokio::signal::ctrl_c();
    tokio::pin!(ctrl_c);
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;

    loop {
        tokio::select! {
            _ = &mut ctrl_c => break,
            _ = sigterm.recv() => break,
            _ = context.shutdown.notified() => break,
            accepted = listener.accept() => {
                match accepted {
                    Ok((stream, _)) => {
                        let context = context.clone();
                        tokio::spawn(async move {
                            if let Err(error) = handle_connection(stream, context).await {
                                tracing::debug!("daemon request failed: {error:#}");
                            }
                        });
                    }
                    Err(error) => {
                        log_line(&paths.log_path, &format!("accept failed: {error}"));
                    }
                }
            }
        }
    }

    worker.abort();
    let _ = worker.await;
    if let Some(running) = context.running.lock().unwrap().take() {
        running.abort.abort();
    }

    if let Ok(mut state) = context.state.lock() {
        for job in &mut state.jobs {
            if job.status == JobStatus::Running {
                job.status = JobStatus::Queued;
                job.progress.current = None;
                job.updated_at = now();
            }
        }
        let _ = save_state(&context.state_path, &state);
    }
    let _ = fs::remove_file(&paths.socket_path);
    log_line(&paths.log_path, "daemon stopped");
    println!("Daemon stopped.");
    Ok(())
}

#[cfg(unix)]
fn set_socket_permissions(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    let mut permissions = fs::metadata(path)?.permissions();
    permissions.set_mode(0o600);
    fs::set_permissions(path, permissions)?;
    Ok(())
}

async fn handle_connection(stream: UnixStream, context: DaemonContext) -> Result<()> {
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);
    let mut line = String::new();
    tokio::time::timeout(Duration::from_secs(10), reader.read_line(&mut line))
        .await
        .map_err(|_| anyhow!("request timed out"))??;
    if line.trim().is_empty() {
        return Ok(());
    }

    let request: Request = match serde_json::from_str(line.trim()) {
        Ok(request) => request,
        Err(error) => {
            let response = Response::error(format!("invalid request: {error}"));
            write_response(&mut write_half, &response).await?;
            return Ok(());
        }
    };

    let response = match request {
        Request::Submit { request, token } => {
            let mut state = context.state.lock().unwrap();
            let id = next_job_id(&state);
            if let Some(token) = token {
                context.tokens.lock().unwrap().insert(id.clone(), token);
            }
            let timestamp = now();
            let job = Job {
                id,
                status: JobStatus::Queued,
                created_at: timestamp,
                updated_at: timestamp,
                started_at: None,
                finished_at: None,
                request,
                progress: JobProgress::default(),
                error: None,
            };
            state.jobs.push(job.clone());
            save_state(&context.state_path, &state)?;
            log_line(
                &context.log_path,
                &format!("queued job {} for {}", job.id, job.request.target),
            );
            Response {
                ok: true,
                job: Some(job),
                ..Response::default()
            }
        }
        Request::List => {
            let state = context.state.lock().unwrap();
            let mut jobs = state.jobs.clone();
            jobs.sort_by_key(|job| job.created_at);
            Response {
                ok: true,
                jobs: Some(jobs),
                ..Response::default()
            }
        }
        Request::Status { id } => {
            let state = context.state.lock().unwrap();
            match state.jobs.iter().find(|job| job.id == id) {
                Some(job) => Response {
                    ok: true,
                    job: Some(job.clone()),
                    ..Response::default()
                },
                None => Response::error(format!("job '{id}' not found")),
            }
        }
        Request::Cancel { id } => {
            let abort = {
                let running = context.running.lock().unwrap();
                running
                    .as_ref()
                    .filter(|running| running.id == id)
                    .map(|running| running.abort.clone())
            };
            let result = (|| -> Result<Response> {
                let mut state = context.state.lock().unwrap();
                let Some(job) = state.jobs.iter_mut().find(|job| job.id == id) else {
                    return Ok(Response::error(format!("job '{id}' not found")));
                };
                if job.status.is_terminal() {
                    return Ok(Response::error(format!(
                        "job '{id}' is already {}",
                        job.status.as_str()
                    )));
                }
                job.status = JobStatus::Cancelled;
                job.progress.current = None;
                job.updated_at = now();
                job.finished_at = Some(now());
                save_state(&context.state_path, &state)?;
                Ok(Response {
                    ok: true,
                    message: Some(format!("Cancelled job {id}.")),
                    ..Response::default()
                })
            })();
            if result.as_ref().map(|response| response.ok).unwrap_or(false) {
                context.tokens.lock().unwrap().remove(&id);
                if let Some(abort) = abort {
                    abort.abort();
                }
                log_line(&context.log_path, &format!("cancelled job {id}"));
            }
            result?
        }
        Request::Pause { id } => {
            let abort = {
                let running = context.running.lock().unwrap();
                running
                    .as_ref()
                    .filter(|running| running.id == id)
                    .map(|running| running.abort.clone())
            };
            let result = (|| -> Result<Response> {
                let mut state = context.state.lock().unwrap();
                let Some(job) = state.jobs.iter_mut().find(|job| job.id == id) else {
                    return Ok(Response::error(format!("job '{id}' not found")));
                };
                job.status = match pause_status(job.status) {
                    Ok(status) => status,
                    Err(error) => return Ok(Response::error(format!("job '{id}' {error}"))),
                };
                job.progress.current = None;
                job.updated_at = now();
                save_state(&context.state_path, &state)?;
                Ok(Response {
                    ok: true,
                    message: Some(format!("Paused job {id}.")),
                    ..Response::default()
                })
            })();
            if result.as_ref().map(|response| response.ok).unwrap_or(false) {
                if let Some(abort) = abort {
                    abort.abort();
                }
                log_line(&context.log_path, &format!("paused job {id}"));
            }
            result?
        }
        Request::Resume { id } => {
            let result = (|| -> Result<Response> {
                let mut state = context.state.lock().unwrap();
                let Some(job) = state.jobs.iter_mut().find(|job| job.id == id) else {
                    return Ok(Response::error(format!("job '{id}' not found")));
                };
                job.status = match resume_status(job.status) {
                    Ok(status) => status,
                    Err(error) => return Ok(Response::error(format!("job '{id}' {error}"))),
                };
                job.updated_at = now();
                save_state(&context.state_path, &state)?;
                Ok(Response {
                    ok: true,
                    message: Some(format!("Resumed job {id}.")),
                    ..Response::default()
                })
            })();
            if result.as_ref().map(|response| response.ok).unwrap_or(false) {
                log_line(&context.log_path, &format!("resumed job {id}"));
            }
            result?
        }
        Request::Clear => {
            let mut state = context.state.lock().unwrap();
            let before = state.jobs.len();
            let removed_ids: Vec<String> = state
                .jobs
                .iter()
                .filter(|job| job.status.is_terminal())
                .map(|job| job.id.clone())
                .collect();
            state.jobs.retain(|job| !job.status.is_terminal());
            let removed = before - state.jobs.len();
            save_state(&context.state_path, &state)?;
            let mut tokens = context.tokens.lock().unwrap();
            for id in &removed_ids {
                tokens.remove(id);
            }
            Response {
                ok: true,
                message: Some(format!("Removed {removed} finished job(s).")),
                ..Response::default()
            }
        }
        Request::Ping => Response::ok(),
        Request::Shutdown => {
            context.shutdown.notify_one();
            Response {
                ok: true,
                message: Some("Stopping daemon.".to_string()),
                ..Response::default()
            }
        }
    };

    write_response(&mut write_half, &response).await?;
    Ok(())
}

async fn write_response(
    writer: &mut tokio::net::unix::OwnedWriteHalf,
    response: &Response,
) -> Result<()> {
    let mut payload = serde_json::to_vec(response)?;
    payload.push(b'\n');
    writer.write_all(&payload).await?;
    Ok(())
}

fn next_job_id(state: &QueueState) -> String {
    let base = format!(
        "j-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis())
            .unwrap_or(0)
    );
    if !state.jobs.iter().any(|job| job.id == base) {
        return base;
    }
    for suffix in 1..1000 {
        let candidate = format!("{base}-{suffix}");
        if !state.jobs.iter().any(|job| job.id == candidate) {
            return candidate;
        }
    }
    format!("{base}-{}", std::process::id())
}

async fn worker_loop(context: DaemonContext) {
    loop {
        let next = {
            let mut state = context.state.lock().unwrap();
            let position = state
                .jobs
                .iter()
                .position(|job| job.status == JobStatus::Queued);
            position.map(|position| {
                let job = &mut state.jobs[position];
                job.status = JobStatus::Running;
                job.started_at = Some(now());
                job.updated_at = now();
                job.progress.current = None;
                (job.id.clone(), job.request.clone())
            })
        };

        let Some((id, request)) = next else {
            tokio::time::sleep(Duration::from_millis(500)).await;
            continue;
        };

        {
            let state = context.state.lock().unwrap();
            if let Err(error) = save_state(&context.state_path, &state) {
                tracing::debug!("failed to save queue state: {error:#}");
            }
        }
        log_line(&context.log_path, &format!("starting job {id}"));

        let token = context.tokens.lock().unwrap().remove(&id);
        let task = tokio::spawn(run_job(context.clone(), id.clone(), request, token));
        let abort = task.abort_handle();
        *context.running.lock().unwrap() = Some(RunningJob {
            id: id.clone(),
            abort: abort.clone(),
        });
        let interrupted = context
            .state
            .lock()
            .unwrap()
            .jobs
            .iter()
            .find(|job| job.id == id)
            .is_some_and(|job| matches!(job.status, JobStatus::Cancelled | JobStatus::Paused));
        if interrupted {
            abort.abort();
        }
        let result = task.await;
        *context.running.lock().unwrap() = None;

        let mut state = context.state.lock().unwrap();
        let Some(job) = state.jobs.iter_mut().find(|job| job.id == id) else {
            continue;
        };
        if job.status == JobStatus::Running {
            job.finished_at = Some(now());
            job.updated_at = now();
            job.progress.current = None;
            match result {
                Ok(Ok(())) => {
                    job.status = JobStatus::Completed;
                    job.error = None;
                    log_line(&context.log_path, &format!("completed job {id}"));
                }
                Ok(Err(error)) => {
                    job.status = JobStatus::Failed;
                    if job.error.is_none() {
                        job.error = Some(format!("{error:#}"));
                    }
                    log_line(&context.log_path, &format!("failed job {id}: {error:#}"));
                }
                Err(error) => {
                    job.status = JobStatus::Failed;
                    job.error = Some(format!("worker failed: {error}"));
                    log_line(&context.log_path, &format!("failed job {id}: {error}"));
                }
            }
        }
        if let Err(error) = save_state(&context.state_path, &state) {
            tracing::debug!("failed to save queue state: {error:#}");
        }
    }
}

async fn run_job(
    context: DaemonContext,
    id: String,
    request: JobRequest,
    token: Option<String>,
) -> Result<()> {
    let config = Config::from_env().with_cache_dir(request.cache_dir.clone());
    let token = token.or_else(|| resolve_token(None, &config));
    let hub = HubClient::new(config.clone(), token)?;
    let downloader = Downloader::new(
        hub,
        DownloadOptions {
            cache_dir: config.cache_dir.clone(),
            local_dir: request.local_dir.clone(),
            flat: request.flat,
            force: request.force,
            verify: request.verify,
            connections: request.connections,
            engine: request.engine,
            max_speed: request.max_speed,
            download_log: std::env::var_os("HFD_DOWNLOAD_LOG").map(std::path::PathBuf::from),
        },
    );

    let uri = parse_target(&request.target, &config.endpoint)?;
    let repo_type = match (uri.repo_type, request.repo_type) {
        (Some(uri_type), Some(request_type)) if uri_type != request_type => {
            bail!(
                "repository type mismatch: target is a {uri_type}, but the job requests {request_type}"
            )
        }
        (Some(uri_type), _) => uri_type,
        (None, Some(request_type)) => request_type,
        (None, None) => RepoType::Model,
    };
    let repo = RepoRef::new(repo_type, uri.repo_id.clone());
    let revision = match (uri.revision.as_deref(), request.revision.as_deref()) {
        (Some(uri_revision), Some(request_revision)) if uri_revision != request_revision => {
            bail!(
                "revision mismatch: target uses '{uri_revision}', but the job requests '{request_revision}'"
            )
        }
        (Some(uri_revision), _) => uri_revision.to_string(),
        (None, Some(request_revision)) => request_revision.to_string(),
        (None, None) => "main".to_string(),
    };

    if !uri.path.is_empty() && !request.filenames.is_empty() {
        bail!(
            "the target already selects '{}'; pass either a file path in the URI or filenames, not both",
            uri.path
        );
    }
    for name in &request.filenames {
        validate_repo_path(name).with_context(|| format!("invalid repository path '{name}'"))?;
    }

    let include = build_globset(&request.include)?;
    let exclude = build_globset(&request.exclude)?;
    let plans = collect_plans(
        &downloader,
        &repo,
        &revision,
        &uri.path,
        &request.filenames,
        include.as_ref(),
        exclude.as_ref(),
    )
    .await?;
    let plan_count = plans.len();
    let total_bytes = plans.iter().map(|plan| plan.size).sum();

    {
        let mut state = context.state.lock().unwrap();
        if let Some(job) = state.jobs.iter_mut().find(|job| job.id == id) {
            job.progress.total_files = plan_count;
            job.progress.total_bytes = total_bytes;
            job.updated_at = now();
        }
        save_state(&context.state_path, &state)?;
    }

    let sink = progress_sink(context.clone(), id.clone());
    let summary = execute_plans(
        plans,
        &downloader,
        &repo,
        &revision,
        request.max_workers,
        &sink,
    )
    .await;

    {
        let mut state = context.state.lock().unwrap();
        if let Some(job) = state.jobs.iter_mut().find(|job| job.id == id) {
            job.progress.failed_files = summary.failures.len();
            if !summary.failures.is_empty() {
                job.error = Some(
                    summary
                        .failures
                        .iter()
                        .map(|(path, error)| format!("{path}: {error}"))
                        .collect::<Vec<_>>()
                        .join("; "),
                );
            }
            job.updated_at = now();
        }
        save_state(&context.state_path, &state)?;
    }

    if summary.failures.is_empty() {
        Ok(())
    } else {
        bail!("{} of {plan_count} file(s) failed", summary.failures.len());
    }
}

fn progress_sink(context: DaemonContext, id: String) -> EventSink {
    let active = Arc::new(Mutex::new(HashMap::<String, u64>::new()));
    let last_save = Arc::new(Mutex::new(Instant::now()));
    Arc::new(move |event| {
        let mut save = false;
        {
            let mut state = context.state.lock().unwrap();
            let Some(job) = state.jobs.iter_mut().find(|job| job.id == id) else {
                return;
            };
            let progress = &mut job.progress;
            match event {
                Event::FileStart {
                    path,
                    size: _,
                    resumed_bytes,
                    ..
                } => {
                    progress.current = Some(path.clone());
                    active.lock().unwrap().insert(path, resumed_bytes);
                    save = true;
                }
                Event::Progress {
                    path, downloaded, ..
                } => {
                    let mut active = active.lock().unwrap();
                    let previous = active.get(&path).copied().unwrap_or(0);
                    let delta = downloaded.saturating_sub(previous);
                    active.insert(path, downloaded);
                    progress.downloaded_bytes = progress.downloaded_bytes.saturating_add(delta);
                }
                Event::FileDone {
                    path,
                    size,
                    skipped,
                    ..
                } => {
                    progress.completed_files = progress.completed_files.saturating_add(1);
                    progress.current = None;
                    let previous = active.lock().unwrap().remove(&path).unwrap_or(0);
                    progress.downloaded_bytes = progress
                        .downloaded_bytes
                        .saturating_add(size.saturating_sub(previous));
                    if skipped {
                        progress.skipped_files = progress.skipped_files.saturating_add(1);
                    }
                    save = true;
                }
                Event::Message { text: message } => {
                    progress.message = Some(message);
                }
            }
            job.updated_at = now();

            let mut last = last_save.lock().unwrap();
            if save || last.elapsed() >= Duration::from_secs(2) {
                let _ = save_state(&context.state_path, &state);
                *last = Instant::now();
            }
        }
    })
}

fn log_line(path: &Path, message: &str) {
    if let Ok(mut file) = OpenOptions::new().create(true).append(true).open(path) {
        let _ = writeln!(file, "{} {message}", now());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_job(id: &str) -> Job {
        Job {
            id: id.to_string(),
            status: JobStatus::Queued,
            created_at: 0,
            updated_at: 0,
            started_at: None,
            finished_at: None,
            request: JobRequest {
                target: "owner/repo".to_string(),
                filenames: Vec::new(),
                repo_type: None,
                revision: None,
                include: Vec::new(),
                exclude: Vec::new(),
                cache_dir: None,
                local_dir: None,
                flat: false,
                force: false,
                connections: 8,
                max_workers: 8,
                verify: VerifyMode::Auto,
                engine: Engine::Auto,
                max_speed: None,
            },
            progress: JobProgress::default(),
            error: None,
        }
    }

    #[test]
    fn state_round_trips() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("queue.json");
        let mut state = QueueState::default();
        state.jobs.push(sample_job("j-1"));
        save_state(&path, &state).unwrap();
        let loaded = load_state(&path).unwrap();
        assert_eq!(loaded.jobs.len(), 1);
        assert_eq!(loaded.jobs[0].id, "j-1");
        assert_eq!(loaded.version, VERSION);
    }

    #[test]
    fn legacy_jobs_default_to_http_engine() {
        let request: JobRequest = serde_json::from_str(r#"{"target":"owner/repo"}"#).unwrap();
        assert_eq!(request.engine, Engine::Http);
    }

    #[test]
    fn job_ids_are_unique() {
        let mut state = QueueState::default();
        let id = next_job_id(&state);
        state.jobs.push(sample_job(&id));
        assert_ne!(next_job_id(&state), id);
    }
}

#[cfg(test)]
mod flat_field_tests {
    use super::JobRequest;

    #[test]
    fn job_request_without_flat_still_loads() {
        let old = r#"{"target":"owner/repo","local_dir":"/tmp/x"}"#;
        let req: JobRequest = serde_json::from_str(old).expect("old queue JSON loads");
        assert!(!req.flat);
    }

    #[test]
    fn job_request_reads_flat() {
        let req: JobRequest =
            serde_json::from_str(r#"{"target":"owner/repo","flat":true}"#).unwrap();
        assert!(req.flat);
    }
}

#[cfg(test)]
mod pause_tests {
    use super::*;

    #[test]
    fn pause_allowed_from_queued_and_running_only() {
        assert_eq!(pause_status(JobStatus::Queued), Ok(JobStatus::Paused));
        assert_eq!(pause_status(JobStatus::Running), Ok(JobStatus::Paused));
        for status in [
            JobStatus::Paused,
            JobStatus::Completed,
            JobStatus::Failed,
            JobStatus::Cancelled,
        ] {
            assert!(pause_status(status).is_err(), "{status:?} should not pause");
        }
    }

    #[test]
    fn resume_only_from_paused_back_to_queued() {
        assert_eq!(resume_status(JobStatus::Paused), Ok(JobStatus::Queued));
        for status in [
            JobStatus::Queued,
            JobStatus::Running,
            JobStatus::Completed,
            JobStatus::Cancelled,
        ] {
            assert!(
                resume_status(status).is_err(),
                "{status:?} should not resume"
            );
        }
    }

    #[test]
    fn paused_job_is_skipped_by_queue_selection() {
        let jobs = [JobStatus::Paused, JobStatus::Queued];
        let position = jobs.iter().position(|status| *status == JobStatus::Queued);
        assert_eq!(position, Some(1));
    }

    #[test]
    fn paused_status_round_trips_through_state_file() {
        let json = serde_json::to_string(&JobStatus::Paused).unwrap();
        assert_eq!(json, "\"paused\"");
        let back: JobStatus = serde_json::from_str(&json).unwrap();
        assert_eq!(back, JobStatus::Paused);
    }
}

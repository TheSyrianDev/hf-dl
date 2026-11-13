#[cfg(unix)]
mod doctor;
mod lastengine;
mod pirateface_dl;
#[cfg(unix)]
mod queue;
#[cfg(not(unix))]
#[path = "queue_stub.rs"]
mod queue;

use std::collections::HashMap;
use std::io::{self, IsTerminal};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use clap::{Args, Parser, Subcommand, ValueEnum};
use globset::{Glob, GlobSet, GlobSetBuilder};
use hfd_core::auth::{
    mask_token, poll_device_token, read_stored_tokens, remove_tokens, request_device_code,
    resolve_token, save_token,
};
use hfd_core::{
    Config, DownloadOptions, DownloadedFile, Downloader, Engine, Error, Event, EventSink,
    HubClient, Plan, RepoRef, RepoType, VerifyMode, parse_target, validate_repo_path,
};
use indicatif::{MultiProgress, ProgressBar, ProgressDrawTarget, ProgressStyle};
use serde_json::json;

#[derive(Debug, Parser)]
#[command(
    name = "hfd",
    version,
    about = "Hugging Face downloader with resumable, segmented downloads",
    long_about = None
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
#[allow(clippy::large_enum_variant)]
enum Commands {
    /// Download files from a Hugging Face repository.
    Download(DownloadArgs),
    /// Inspect the active Hugging Face credentials.
    Auth(AuthArgs),
    /// Manage the persistent download queue.
    Queue(QueueArgs),
    /// Run the download daemon in the foreground.
    Daemon(DaemonArgs),
    /// Fetch the Last Engine release asset for this machine and verify its SHA-256.
    Lastengine(lastengine::LastEngineArgs),
    /// Check network, Xet/HTTP state and free disk, and recommend settings. Read-only.
    #[cfg(unix)]
    Doctor(doctor::DoctorArgs),
    #[command(name = "__daemon", hide = true)]
    InternalDaemon,
}

#[derive(Debug, Args)]
struct QueueArgs {
    #[command(subcommand)]
    command: QueueCommand,
}

#[derive(Debug, Subcommand)]
enum QueueCommand {
    /// List queued, running, and finished jobs.
    List {
        /// Print the jobs as JSON.
        #[arg(long)]
        json: bool,
    },
    /// Show one job as JSON.
    Show {
        /// Job id.
        id: String,
    },
    /// Cancel a queued, running, or paused job.
    Cancel {
        /// Job id.
        id: String,
    },
    /// Pause a queued or running job. Partial files are kept, so a resume continues them.
    Pause {
        /// Job id.
        id: String,
    },
    /// Resume a paused job by putting it back in the queue.
    Resume {
        /// Job id.
        id: String,
    },
    /// Remove finished jobs from the queue.
    Clear,
}

#[derive(Debug, Args)]
struct DaemonArgs {
    /// Stop the running daemon.
    #[arg(long)]
    stop: bool,
}

#[derive(Debug, Args)]
struct AuthArgs {
    #[command(subcommand)]
    command: AuthCommand,
}

#[derive(Debug, Subcommand)]
enum AuthCommand {
    /// Log in with a token or through the browser.
    Login(LoginArgs),
    /// Remove the active token and stored tokens.
    Logout,
    /// Print the active token.
    Token,
    /// List stored tokens.
    #[command(alias = "ls")]
    List,
    /// Print the user associated with the active token.
    Whoami,
}

#[derive(Debug, Args)]
struct LoginArgs {
    /// Access token. When omitted, the browser device flow is used.
    #[arg(long)]
    token: Option<String>,

    /// Log in again even when a token is already configured.
    #[arg(long)]
    force: bool,
}

#[derive(Debug, Args)]
struct DownloadArgs {
    /// Repository id, hf:// URI, or Hugging Face web URL.
    #[arg(value_name = "REPO_ID")]
    repo_id: String,

    /// Files or directories inside the repository.
    #[arg(value_name = "FILENAMES")]
    filenames: Vec<String>,

    /// Repository type when it is not encoded in the target.
    #[arg(long = "type", alias = "repo-type", value_enum)]
    repo_type: Option<RepoTypeArg>,

    /// Git revision, tag, branch, or commit.
    #[arg(long)]
    revision: Option<String>,

    /// Where bytes come from. `pirateface` is opt-in, needs `aria2c`, and requires
    /// --local-dir. There is no fallback to Hugging Face.
    #[arg(long, value_enum, default_value = "hf")]
    source: SourceArg,

    /// Include files matching this glob (repeatable).
    #[arg(long = "include", value_name = "GLOB")]
    include: Vec<String>,

    /// Exclude files matching this glob (repeatable).
    #[arg(long = "exclude", value_name = "GLOB")]
    exclude: Vec<String>,

    /// Directory used for the Hugging Face cache.
    #[arg(long = "cache-dir", value_name = "DIR")]
    cache_dir: Option<PathBuf>,

    /// Download into this directory instead of the cache.
    #[arg(long = "local-dir", value_name = "DIR")]
    local_dir: Option<PathBuf>,

    /// With --local-dir, put each file directly in that folder by basename,
    /// dropping the repo's subfolders.
    #[arg(long = "flat", requires = "local_dir")]
    flat: bool,

    /// Download again even when a valid local copy exists.
    #[arg(long = "force-download")]
    force_download: bool,

    /// Resolve files and print the plan without downloading.
    #[arg(long = "dry-run")]
    dry_run: bool,

    /// Hugging Face access token.
    #[arg(long)]
    token: Option<String>,

    /// Number of files to download concurrently.
    #[arg(long = "max-workers", default_value_t = 8)]
    max_workers: usize,

    /// Number of connections used for a single large file.
    #[arg(long, default_value_t = 8, value_parser = parse_connections)]
    connections: usize,

    /// Cap total download speed, e.g. 5M, 500K or 2.5MB (bytes per second).
    #[arg(long = "max-speed", value_parser = parse_speed)]
    max_speed: Option<u64>,

    /// Download engine. Plain HTTP is the default until Xet stalls are fixed;
    /// use `auto` or `xet` to try Xet.
    #[arg(long, value_enum, default_value_t = EngineArg::Http)]
    engine: EngineArg,

    /// Disable Xet downloads.
    #[arg(long = "no-xet")]
    no_xet: bool,

    /// Enqueue the download and return immediately.
    #[arg(long)]
    detach: bool,

    /// Keep the process in the foreground.
    #[arg(long)]
    foreground: bool,

    /// Do not use the daemon.
    #[arg(long = "no-daemon")]
    no_daemon: bool,

    /// Output format.
    #[arg(long, value_enum, default_value_t = OutputFormat::Auto)]
    format: OutputFormat,

    /// Emit newline-delimited JSON.
    #[arg(long)]
    json: bool,

    /// Only print final paths.
    #[arg(long, short = 'q')]
    quiet: bool,

    /// Do not truncate long paths in output.
    #[arg(long = "no-truncate")]
    no_truncate: bool,

    /// Checksum verification policy.
    #[arg(long, value_enum, default_value_t = VerifyArg::Auto)]
    verify: VerifyArg,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum RepoTypeArg {
    Model,
    Dataset,
    Space,
}

impl From<RepoTypeArg> for RepoType {
    fn from(value: RepoTypeArg) -> Self {
        match value {
            RepoTypeArg::Model => RepoType::Model,
            RepoTypeArg::Dataset => RepoType::Dataset,
            RepoTypeArg::Space => RepoType::Space,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum SourceArg {
    Hf,
    Pirateface,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum EngineArg {
    Auto,
    Http,
    Xet,
}

impl EngineArg {
    pub(crate) fn resolve(self, no_xet: bool) -> Result<Engine> {
        if no_xet {
            if matches!(self, EngineArg::Xet) {
                bail!("--no-xet cannot be combined with --engine xet");
            }
            return Ok(Engine::Http);
        }
        Ok(match self {
            EngineArg::Auto => Engine::Auto,
            EngineArg::Http => Engine::Http,
            EngineArg::Xet => Engine::Xet,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum VerifyArg {
    Auto,
    Always,
    Never,
}

impl From<VerifyArg> for VerifyMode {
    fn from(value: VerifyArg) -> Self {
        match value {
            VerifyArg::Auto => VerifyMode::Auto,
            VerifyArg::Always => VerifyMode::Always,
            VerifyArg::Never => VerifyMode::Never,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum OutputFormat {
    Auto,
    Human,
    Agent,
    Json,
    Quiet,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    Human,
    Agent,
    Json,
    Quiet,
}

pub async fn run() {
    init_tracing();
    let cli = Cli::parse();
    let result = match cli.command {
        Commands::Download(args) => {
            if args.detach {
                queue::enqueue_download(&args).await
            } else {
                run_download(args).await
            }
        }
        Commands::Auth(args) => run_auth(args).await,
        Commands::Queue(args) => queue::run_queue(args).await,
        Commands::Daemon(args) => queue::daemon_command(args.stop).await,
        Commands::InternalDaemon => queue::daemon_command(false).await,
        Commands::Lastengine(args) => lastengine::run(args).await,
        #[cfg(unix)]
        Commands::Doctor(args) => match doctor::run(args).await {
            Ok(true) => Ok(()),
            Ok(false) => std::process::exit(2),
            Err(error) => Err(error),
        },
    };

    if let Err(error) = result {
        eprintln!("error: {error:#}");
        std::process::exit(1);
    }
}

fn init_tracing() {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        tracing_subscriber::EnvFilter::new(
            "warn,xet_client=error,hf_xet=error,xet_data=error,xet_runtime=error",
        )
    });
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(io::stderr)
        .init();
}

async fn run_auth(args: AuthArgs) -> Result<()> {
    match args.command {
        AuthCommand::Login(args) => run_login(args).await,
        AuthCommand::Logout => run_logout(),
        AuthCommand::Token => run_token(),
        AuthCommand::List => run_list(),
        AuthCommand::Whoami => run_whoami().await,
    }
}

async fn run_login(args: LoginArgs) -> Result<()> {
    let config = Config::from_env();
    if args.token.is_none() && !args.force && resolve_token(None, &config).is_some() {
        println!("Already logged in. Use `hfd auth login --force` to log in again.");
        return Ok(());
    }

    let token = match args.token {
        Some(token) => {
            let token = token.trim().to_string();
            if token.is_empty() {
                bail!("token must not be empty");
            }
            token
        }
        None => {
            let info = request_device_code(&config).await?;
            println!("Open this URL in your browser:");
            println!("  {}", info.complete_uri());
            println!();
            println!("And enter the code: {}", info.user_code);
            println!();
            println!("Waiting for authorization...");
            poll_device_token(&config, &info).await?
        }
    };

    let hub = HubClient::new(config.clone(), Some(token.clone()))?;
    let username = hub.whoami().await?;
    save_token(&config, &token)?;
    println!("Login successful: logged in as {username}");
    Ok(())
}

fn run_logout() -> Result<()> {
    let config = Config::from_env();
    remove_tokens(&config)?;
    println!("Logged out.");
    Ok(())
}

fn run_token() -> Result<()> {
    let config = Config::from_env();
    let token = resolve_token(None, &config)
        .ok_or_else(|| anyhow!("not logged in; run `hfd auth login`"))?;
    println!("{token}");
    Ok(())
}

fn run_list() -> Result<()> {
    let config = Config::from_env();
    let active = resolve_token(None, &config);
    let stored = read_stored_tokens(&config.stored_tokens_path);
    if stored.is_empty() && active.is_none() {
        println!("No access tokens found.");
        return Ok(());
    }

    println!("  name\ttoken");
    for (name, token) in &stored {
        let marker = if active.as_deref() == Some(token.as_str()) {
            "*"
        } else {
            " "
        };
        println!("{marker} {name}\t{}", mask_token(token));
    }
    if let Some(active) = active
        && !stored.iter().any(|(_, token)| token == &active)
    {
        println!("* <active>\t{}", mask_token(&active));
    }
    Ok(())
}

async fn run_whoami() -> Result<()> {
    let config = Config::from_env();
    let token = resolve_token(None, &config)
        .ok_or_else(|| anyhow!("not logged in; set HF_TOKEN or run `hfd auth login`"))?;
    let hub = HubClient::new(config, Some(token))?;
    println!("{}", hub.whoami().await?);
    Ok(())
}

async fn run_download(args: DownloadArgs) -> Result<()> {
    if args.source == SourceArg::Pirateface {
        return pirateface_dl::run(
            &args.repo_id,
            args.revision.as_deref(),
            &args.filenames,
            args.local_dir.as_deref(),
            &Config::from_env().cache_dir,
        )
        .await;
    }
    let mode = output_mode(&args);
    let printer = Arc::new(Printer::new(mode));

    if args.no_xet {
        tracing::debug!("xet disabled by request");
    }
    if args.no_daemon {
        tracing::debug!("daemon disabled by request");
    }
    if args.foreground {
        tracing::debug!("running in the foreground");
    }
    if args.no_truncate {
        tracing::debug!("path truncation disabled");
    }
    let engine = args.engine.resolve(args.no_xet)?;

    let config = Config::from_env().with_cache_dir(args.cache_dir.clone());
    let token = resolve_token(args.token.as_deref(), &config);
    let hub = HubClient::new(config.clone(), token)?;
    let downloader = Downloader::new(
        hub,
        DownloadOptions {
            cache_dir: config.cache_dir.clone(),
            local_dir: args.local_dir.clone(),
            flat: args.flat,
            force: args.force_download,
            verify: args.verify.into(),
            connections: args.connections,
            engine,
            max_speed: args.max_speed,
            download_log: std::env::var_os("HFD_DOWNLOAD_LOG").map(PathBuf::from),
        },
    );

    let uri = parse_target(&args.repo_id, &config.endpoint)?;
    let repo_type = match (uri.repo_type, args.repo_type.map(RepoType::from)) {
        (Some(uri_type), Some(cli_type)) if uri_type != cli_type => {
            bail!("repository type mismatch: target is a {uri_type}, but --type is {cli_type}")
        }
        (Some(uri_type), _) => uri_type,
        (None, Some(cli_type)) => cli_type,
        (None, None) => RepoType::Model,
    };
    let repo = RepoRef::new(repo_type, uri.repo_id.clone());

    let revision = match (uri.revision.as_deref(), args.revision.as_deref()) {
        (Some(uri_revision), Some(cli_revision)) if uri_revision != cli_revision => {
            bail!(
                "revision mismatch: target uses '{uri_revision}', but --revision is '{cli_revision}'"
            )
        }
        (Some(uri_revision), _) => uri_revision.to_string(),
        (None, Some(cli_revision)) => cli_revision.to_string(),
        (None, None) => "main".to_string(),
    };

    if !uri.path.is_empty() && !args.filenames.is_empty() {
        bail!(
            "the target already selects '{}'; pass either a file path in the URI or positional FILENAMES, not both",
            uri.path
        );
    }

    let mut requested = Vec::with_capacity(args.filenames.len());
    for name in &args.filenames {
        let normalized = name.trim_end_matches('/');
        if normalized.is_empty() {
            bail!("filename must not be empty");
        }
        validate_repo_path(normalized)
            .with_context(|| format!("invalid repository path '{name}'"))?;
        requested.push(normalized.to_string());
    }

    let include = build_globset(&args.include)?;
    let exclude = build_globset(&args.exclude)?;
    let plans = collect_plans(
        &downloader,
        &repo,
        &revision,
        &uri.path,
        &requested,
        include.as_ref(),
        exclude.as_ref(),
    )
    .await?;

    if args.dry_run {
        for plan in &plans {
            printer.plan(plan);
        }
        return Ok(());
    }

    let sink = printer.sink();
    let total_plans = plans.len();
    let summary = execute_plans(
        plans,
        &downloader,
        &repo,
        &revision,
        args.max_workers,
        &sink,
    )
    .await;

    for (path, error) in &summary.failures {
        printer.error(path, error);
    }
    printer.summary(
        summary.files,
        summary.skipped,
        summary.failures.len(),
        summary.total_bytes,
        summary.skipped_bytes,
    );
    if !summary.failures.is_empty() {
        bail!("{} of {total_plans} file(s) failed", summary.failures.len());
    }
    Ok(())
}

pub(crate) struct DownloadSummary {
    pub files: usize,
    pub skipped: usize,
    pub total_bytes: u64,
    pub skipped_bytes: u64,
    pub failures: Vec<(String, Error)>,
}

pub(crate) async fn execute_plans(
    plans: Vec<Plan>,
    downloader: &Downloader,
    repo: &RepoRef,
    revision: &str,
    max_workers: usize,
    sink: &EventSink,
) -> DownloadSummary {
    let total_plans = plans.len();
    let max_workers = max_workers.max(1).min(total_plans.max(1));
    let mut queued = plans.into_iter();
    let mut tasks: tokio::task::JoinSet<(String, std::result::Result<DownloadedFile, Error>)> =
        tokio::task::JoinSet::new();
    let mut in_flight = 0usize;
    let mut summary = DownloadSummary {
        files: 0,
        skipped: 0,
        total_bytes: 0,
        skipped_bytes: 0,
        failures: Vec::new(),
    };

    for _ in 0..max_workers {
        if let Some(plan) = queued.next() {
            spawn_download(&mut tasks, plan, downloader, repo, revision, sink);
            in_flight += 1;
        }
    }

    while in_flight > 0 {
        let Some(result) = tasks.join_next().await else {
            break;
        };
        in_flight -= 1;
        match result {
            Ok((_, Ok(file))) => {
                summary.files += 1;
                summary.total_bytes = summary.total_bytes.saturating_add(file.size);
                if file.skipped {
                    summary.skipped += 1;
                    summary.skipped_bytes = summary.skipped_bytes.saturating_add(file.size);
                }
            }
            Ok((path, Err(error))) => summary.failures.push((path, error)),
            Err(error) => summary.failures.push((
                "<worker>".to_string(),
                Error::Invalid(format!("download worker failed: {error}")),
            )),
        }

        if let Some(plan) = queued.next() {
            spawn_download(&mut tasks, plan, downloader, repo, revision, sink);
            in_flight += 1;
        }
    }

    summary
}

fn spawn_download(
    tasks: &mut tokio::task::JoinSet<(String, std::result::Result<DownloadedFile, Error>)>,
    plan: Plan,
    downloader: &Downloader,
    repo: &RepoRef,
    revision: &str,
    sink: &EventSink,
) {
    let downloader = downloader.clone();
    let repo = repo.clone();
    let revision = revision.to_string();
    let sink = sink.clone();
    let path = plan.path.clone();
    tasks.spawn(async move {
        let result = downloader
            .download_file(&repo, &plan.path, &revision, &sink)
            .await;
        (path, result)
    });
}

async fn collect_plans(
    downloader: &Downloader,
    repo: &RepoRef,
    revision: &str,
    uri_path: &str,
    filenames: &[String],
    include: Option<&GlobSet>,
    exclude: Option<&GlobSet>,
) -> Result<Vec<Plan>> {
    if filenames.is_empty() && !uri_path.is_empty() && include.is_none() && exclude.is_none() {
        match downloader.plan(repo, uri_path, revision).await {
            Ok(plan) => return Ok(vec![plan]),
            Err(Error::NotFound(_)) => {}
            Err(error) => return Err(error.into()),
        }
    }

    let tree = downloader.hub().list_tree(repo, revision).await?;
    let mut paths = Vec::new();
    let mut matched = vec![false; filenames.len()];
    let uri_prefix = if uri_path.is_empty() {
        None
    } else {
        Some(format!("{uri_path}/"))
    };

    for entry in tree {
        let path = entry.path;
        if let Some(prefix) = &uri_prefix
            && path != uri_path
            && !path.starts_with(prefix)
        {
            continue;
        }

        if !filenames.is_empty() {
            let mut selected = false;
            for (index, name) in filenames.iter().enumerate() {
                if path == *name || path.starts_with(&format!("{name}/")) {
                    matched[index] = true;
                    selected = true;
                }
            }
            if !selected {
                continue;
            }
        }

        if let Some(include) = include
            && !include.is_match(&path)
        {
            continue;
        }
        if let Some(exclude) = exclude
            && exclude.is_match(&path)
        {
            continue;
        }

        paths.push(path);
    }

    let missing: Vec<&str> = filenames
        .iter()
        .zip(matched.iter())
        .filter(|(_, found)| !**found)
        .map(|(name, _)| name.as_str())
        .collect();
    if !missing.is_empty() {
        bail!(
            "file or directory not found in {}/{}@{}: {}",
            repo.repo_type.as_str(),
            repo.repo_id,
            revision,
            missing.join(", ")
        );
    }

    paths.sort();
    paths.dedup();
    if paths.is_empty() {
        bail!(
            "no files matched in {}/{}@{}",
            repo.repo_type.as_str(),
            repo.repo_id,
            revision
        );
    }

    let mut plans = Vec::with_capacity(paths.len());
    for path in paths {
        plans.push(downloader.plan(repo, &path, revision).await?);
    }
    Ok(plans)
}

fn build_globset(patterns: &[String]) -> Result<Option<GlobSet>> {
    if patterns.is_empty() {
        return Ok(None);
    }
    let mut builder = GlobSetBuilder::new();
    for pattern in patterns {
        builder.add(Glob::new(pattern).with_context(|| format!("invalid glob '{pattern}'"))?);
    }
    Ok(Some(builder.build()?))
}

fn output_mode(args: &DownloadArgs) -> Mode {
    if args.quiet {
        return Mode::Quiet;
    }
    if args.json || matches!(args.format, OutputFormat::Json) {
        return Mode::Json;
    }
    match args.format {
        OutputFormat::Auto => {
            if io::stdout().is_terminal() {
                Mode::Human
            } else {
                Mode::Agent
            }
        }
        OutputFormat::Human => Mode::Human,
        OutputFormat::Agent => Mode::Agent,
        OutputFormat::Json => Mode::Json,
        OutputFormat::Quiet => Mode::Quiet,
    }
}

struct Printer {
    mode: Mode,
    multi: Option<MultiProgress>,
    bars: Mutex<HashMap<String, ProgressBar>>,
}

impl Printer {
    fn new(mode: Mode) -> Self {
        Self {
            mode,
            multi: (mode == Mode::Human)
                .then(|| MultiProgress::with_draw_target(ProgressDrawTarget::stderr())),
            bars: Mutex::new(HashMap::new()),
        }
    }

    fn sink(self: &Arc<Self>) -> EventSink {
        let printer = Arc::clone(self);
        Arc::new(move |event| printer.handle(event))
    }

    fn handle(&self, event: Event) {
        match event {
            Event::FileStart {
                path,
                size,
                destination,
                resumed_bytes,
            } => match self.mode {
                Mode::Human => self.start_bar(&path, size, resumed_bytes),
                Mode::Agent => eprintln!(
                    "start\t{path}\t{size}\tresumed={resumed_bytes}\t{}",
                    destination.display()
                ),
                Mode::Json => self.json(
                    "file_start",
                    json!({
                        "path": path,
                        "size": size,
                        "resumed_bytes": resumed_bytes,
                        "destination": destination.display().to_string(),
                    }),
                ),
                Mode::Quiet => {}
            },
            Event::Progress {
                path,
                downloaded,
                total,
                bytes_per_sec,
                eta_secs,
            } => match self.mode {
                Mode::Human => {
                    let _ = (bytes_per_sec, eta_secs);
                    if let Some(bar) = self.bars.lock().unwrap().get(&path) {
                        bar.set_position(downloaded.min(total));
                    }
                }
                Mode::Json => self.json(
                    "progress",
                    json!({
                        "path": path,
                        "downloaded": downloaded,
                        "total": total,
                        "bytes_per_sec": bytes_per_sec.round() as u64,
                        "eta_secs": eta_secs,
                    }),
                ),
                Mode::Agent | Mode::Quiet => {}
            },
            Event::FileDone {
                path,
                local_path,
                size,
                skipped,
            } => match self.mode {
                Mode::Human => {
                    let bar = self.bars.lock().unwrap().remove(&path);
                    if let Some(bar) = bar {
                        bar.finish_and_clear();
                        if let Some(multi) = &self.multi {
                            multi.remove(&bar);
                        }
                    }
                    self.print_human(&format!(
                        "{path} -> {} ({}){}",
                        local_path.display(),
                        human_size(size),
                        if skipped { ", cached" } else { "" }
                    ));
                }
                Mode::Agent => eprintln!(
                    "done\t{path}\t{}\t{}\t{}",
                    local_path.display(),
                    size,
                    if skipped { "cached" } else { "downloaded" }
                ),
                Mode::Json => self.json(
                    "file_done",
                    json!({
                        "path": path,
                        "local_path": local_path.display().to_string(),
                        "size": size,
                        "skipped": skipped,
                    }),
                ),
                Mode::Quiet => println!("{}", local_path.display()),
            },
            Event::Message { text: message } => match self.mode {
                Mode::Human => self.print_human(&message),
                Mode::Agent => eprintln!("info\t{message}"),
                Mode::Json => self.json("message", json!({ "message": message })),
                Mode::Quiet => {}
            },
        }
    }

    fn start_bar(&self, path: &str, size: u64, resumed: u64) {
        let mut bars = self.bars.lock().unwrap();
        if let Some(old) = bars.remove(path) {
            old.finish_and_clear();
            if let Some(multi) = &self.multi {
                multi.remove(&old);
            }
        }

        let bar = ProgressBar::new(size);
        let style = ProgressStyle::with_template(
            "{spinner:.green} {msg:.bold} [{wide_bar:.cyan/blue}] {bytes}/{total_bytes} {bytes_per_sec} {short_eta}",
        )
        .expect("valid progress template")
        .with_key("short_eta", |state: &indicatif::ProgressState, w: &mut dyn std::fmt::Write| {
            let eta = state.eta();
            let _ = if state.per_sec() < 1024.0 || eta > Duration::from_secs(86_400) {
                write!(w, "ETA --")
            } else {
                write!(w, "ETA {}", indicatif::HumanDuration(eta))
            };
        })
        .progress_chars("=>-");
        bar.set_style(style);
        bar.set_message(if resumed > 0 {
            format!("{path} (resume)")
        } else {
            path.to_string()
        });
        bar.set_position(resumed.min(size));
        bar.enable_steady_tick(Duration::from_millis(100));
        let bar = if let Some(multi) = &self.multi {
            multi.add(bar)
        } else {
            bar
        };
        bars.insert(path.to_string(), bar);
    }

    fn print_human(&self, message: &str) {
        if let Some(multi) = &self.multi {
            multi.suspend(|| eprintln!("{message}"));
        } else {
            eprintln!("{message}");
        }
    }

    fn plan(&self, plan: &Plan) {
        match self.mode {
            Mode::Human => {
                let engine = if plan.complete {
                    "cached".to_string()
                } else if plan.engine == Engine::Xet {
                    "xet".to_string()
                } else if plan.segmented {
                    format!(
                        "http-segmented ({} connections, {} units)",
                        plan.connections,
                        human_size(plan.unit_size)
                    )
                } else {
                    "http-single".to_string()
                };
                println!("Repository:  {}:{}", plan.repo_type, plan.repo_id);
                println!("Revision:    {} ({})", plan.revision, plan.commit);
                println!("File:        {}", plan.path);
                println!("Size:        {}", human_size(plan.size));
                println!("Destination: {}", plan.destination.display());
                println!("Engine:      {engine}");
                println!();
            }
            Mode::Agent => println!(
                "plan\t{}\t{}\t{}\t{}\t{}",
                plan.path,
                plan.size,
                plan.destination.display(),
                plan.commit,
                if plan.complete { "cached" } else { "download" }
            ),
            Mode::Json => self.json(
                "plan",
                json!({
                    "repo_id": plan.repo_id,
                    "repo_type": plan.repo_type.as_str(),
                    "path": plan.path,
                    "revision": plan.revision,
                    "commit": plan.commit,
                    "size": plan.size,
                    "etag": plan.etag,
                    "sha256": plan.sha256,
                    "destination": plan.destination.display().to_string(),
                    "final_path": plan.final_path.display().to_string(),
                    "complete": plan.complete,
                    "segmented": plan.segmented,
                    "unit_size": plan.unit_size,
                    "connections": plan.connections,
                    "accept_ranges": plan.accept_ranges,
                    "xet_hash": plan.xet_hash,
                    "engine": plan.engine.as_str(),
                }),
            ),
            Mode::Quiet => println!("{}", plan.destination.display()),
        }
    }

    fn error(&self, path: &str, error: &Error) {
        match self.mode {
            Mode::Human => {
                if let Some(bar) = self.bars.lock().unwrap().remove(path) {
                    bar.finish_and_clear();
                    if let Some(multi) = &self.multi {
                        multi.remove(&bar);
                    }
                }
                self.print_human(&format!("error: {path}: {error:#}"));
            }
            Mode::Agent => eprintln!("error\t{path}\t{error}"),
            Mode::Json => self.json(
                "error",
                json!({
                    "path": path,
                    "message": format!("{error:#}"),
                }),
            ),
            Mode::Quiet => eprintln!("error: {path}: {error:#}"),
        }
    }

    fn summary(
        &self,
        files: usize,
        skipped: usize,
        failures: usize,
        total_bytes: u64,
        skipped_bytes: u64,
    ) {
        let downloaded_bytes = total_bytes.saturating_sub(skipped_bytes);
        match self.mode {
            Mode::Human => {
                self.print_human(&format!(
                    "{files} file(s) ready: {} downloaded, {skipped} cached ({}), {failures} failed",
                    human_size(downloaded_bytes),
                    human_size(skipped_bytes)
                ));
                if let Some(multi) = &self.multi {
                    let _ = multi.clear();
                }
            }
            Mode::Agent => eprintln!(
                "summary\tfiles={files}\tcached_files={skipped}\tdownloaded_bytes={downloaded_bytes}\tskipped_bytes={skipped_bytes}\tfailures={failures}"
            ),
            Mode::Json => self.json(
                "summary",
                json!({
                    "files": files,
                    "cached_files": skipped,
                    "downloaded_bytes": downloaded_bytes,
                    "skipped_bytes": skipped_bytes,
                    "failures": failures,
                }),
            ),
            Mode::Quiet => {}
        }
    }

    fn json(&self, event: &str, fields: serde_json::Value) {
        let mut object = serde_json::Map::new();
        object.insert("event".to_string(), json!(event));
        if let serde_json::Value::Object(fields) = fields {
            object.extend(fields);
        }
        println!("{}", serde_json::Value::Object(object));
    }
}

fn parse_connections(value: &str) -> std::result::Result<usize, String> {
    let value: usize = value
        .parse()
        .map_err(|_| format!("'{value}' is not a valid connection count"))?;
    if (1..=64).contains(&value) {
        Ok(value)
    } else {
        Err("connection count must be between 1 and 64".to_string())
    }
}

fn human_size(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut value = bytes as f64;
    let mut unit = 0usize;
    while value >= 1024.0 && unit + 1 < UNITS.len() {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{bytes} B")
    } else {
        format!("{value:.2} {}", UNITS[unit])
    }
}

fn parse_speed(value: &str) -> std::result::Result<u64, String> {
    let text = value.trim().to_ascii_lowercase();
    let text = text.trim_end_matches("/s").trim_end_matches('b');
    let (number, scale) = match text.chars().last() {
        Some('k') => (&text[..text.len() - 1], 1024.0),
        Some('m') => (&text[..text.len() - 1], 1024.0 * 1024.0),
        Some('g') => (&text[..text.len() - 1], 1024.0 * 1024.0 * 1024.0),
        _ => (text, 1.0),
    };
    let number: f64 = number
        .trim()
        .parse()
        .map_err(|_| format!("invalid speed '{value}', use e.g. 5M or 500K"))?;
    if number.is_nan() || number <= 0.0 {
        return Err("speed must be above zero".to_string());
    }
    Ok((number * scale) as u64)
}

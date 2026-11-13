//! Download queue for long-lived apps and the CLI daemon.
//!
//! A [`Queue`] holds jobs (a repo plus a list of files), runs them one at a
//! time by default, and persists its state to a JSON file so a restart
//! resumes unfinished work. Progress and state changes are sent on a
//! broadcast channel from [`Queue::subscribe`].
//!
//! Pause and cancel stop the active transfer. Partial files stay on disk, so a
//! later resume continues them. Cancel deletes partials only when asked to
//! (`delete_partial`), and only for jobs with a `local_dir`.

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use tokio::sync::{Notify, broadcast};
use tokio::task::JoinSet;

use crate::engine::{DownloadOptions, Downloader};
use crate::progress::{Event, EventSink};
use crate::{CancelToken, Config, Error, HubClient, RepoRef, RepoType, Result};

/// What to download. `paths` are repo-relative files at `revision`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct JobSpec {
    /// Kind of repository (model, dataset or space).
    pub repo_type: RepoType,
    /// Hub repository id, such as `org/name`.
    pub repo_id: String,
    /// Git revision to fetch: a branch, tag or commit.
    pub revision: String,
    /// Repo-relative file paths to download. Must not be empty; [`Queue::add`] rejects an empty list.
    pub paths: Vec<String>,
    /// Real folder for the files. Required for `delete_partial` on cancel.
    #[serde(default)]
    /// Real folder for the files. Required for `delete_partial` on cancel.
    pub local_dir: Option<PathBuf>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
/// Lifecycle state of a job. `Done`, `Failed` and `Cancelled` are terminal until the job is re-added.
pub enum Status {
    /// Waiting for a worker slot.
    Queued,
    /// Currently downloading.
    Running,
    /// Stopped by [`Queue::pause`]; resumes from its partial files.
    Paused,
    /// Stopped by [`Queue::cancel`]. Partial files stay unless `delete_partial` was set.
    Cancelled,
    /// Every file was fetched and passed verification.
    Done,
    /// Stopped with an error. See [`Job::error`] and [`Job::retryable`].
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
/// A queued download and its current state. Persisted in the queue state file.
pub struct Job {
    /// Stable identifier, `job-N`, assigned by [`Queue::add`].
    pub id: String,
    /// What to download.
    pub spec: JobSpec,
    /// Current lifecycle state.
    pub status: Status,
    /// Unix timestamp in seconds when the job was added.
    pub created_at: u64,
    #[serde(default)]
    /// Reason for the last failure, if any.
    pub error: Option<String>,
    #[serde(default)]
    /// True when the failure looks transient (network, timeout), so a re-add may succeed.
    pub retryable: bool,
}

/// Events from [`Queue::subscribe`]. Tags are stable JSON for GUI consumers.
#[derive(Debug, Clone, Serialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum QueueEvent {
    /// A job was accepted into the queue.
    Queued {
        /// Job id.
        id: String,
    },
    /// A worker began downloading the job.
    Started {
        /// Job id.
        id: String,
    },
    /// Per-file progress. `bytes_per_sec` is a rolling rate; `eta_secs` is `None` when unknown.
    Progress {
        /// Job id.
        id: String,
        /// Repo-relative file being fetched.
        path: String,
        /// Bytes received so far for this file.
        bytes: u64,
        /// Expected size in bytes.
        total: u64,
        /// Rolling transfer rate.
        bytes_per_sec: f64,
        /// Estimated seconds remaining, or `None` when unknown.
        eta_secs: Option<u64>,
    },
    /// A job was paused.
    Paused {
        /// Job id.
        id: String,
    },
    /// A job was cancelled. `deleted_partial` is true when partial files were removed.
    Cancelled {
        /// Job id.
        id: String,
        /// Whether partial files were removed.
        deleted_partial: bool,
    },
    /// A job finished. `sha_ok` is true when every file passed checksum verification.
    Finished {
        /// Job id.
        id: String,
        /// Whether every file passed checksum verification.
        sha_ok: bool,
    },
    /// A job failed. `retryable` mirrors [`Job::retryable`].
    Failed {
        /// Job id.
        id: String,
        /// Human-readable reason.
        reason: String,
        /// Whether a re-add may succeed.
        retryable: bool,
    },
}

#[derive(Default, Serialize, Deserialize)]
struct Persisted {
    version: u32,
    jobs: Vec<Job>,
    next_id: u64,
}

struct Inner {
    jobs: Vec<Job>,
    next_id: u64,
    max_parallel: usize,
    /// Cancel token of each running job, plus whether the stop was a pause.
    running: HashMap<String, (CancelToken, Stop)>,
    path: PathBuf,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Stop {
    Pause,
    Cancel { delete_partial: bool },
}

/// Cheap to clone; all clones share one queue.
#[derive(Clone)]
pub struct Queue {
    inner: Arc<Mutex<Inner>>,
    events: broadcast::Sender<QueueEvent>,
    wake: Arc<Notify>,
    hub: HubClient,
    options: DownloadOptions,
}

fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

impl Queue {
    /// Opens (or creates) the queue at `state_file`. Jobs that were running
    /// when the process stopped go back to `Queued`, so [`Queue::run`] resumes
    /// them.
    pub fn open(
        state_file: impl Into<PathBuf>,
        config: Config,
        token: Option<String>,
        options: DownloadOptions,
    ) -> Result<Self> {
        let path = state_file.into();
        let mut persisted: Persisted = match fs::read(&path) {
            Ok(bytes) => serde_json::from_slice(&bytes)
                .map_err(|e| Error::Invalid(format!("queue state {}: {e}", path.display())))?,
            Err(_) => Persisted::default(),
        };
        for job in &mut persisted.jobs {
            if job.status == Status::Running {
                job.status = Status::Queued;
            }
        }
        let (events, _) = broadcast::channel(256);
        let queue = Self {
            inner: Arc::new(Mutex::new(Inner {
                jobs: persisted.jobs,
                next_id: persisted.next_id.max(1),
                max_parallel: 1,
                running: HashMap::new(),
                path,
            })),
            events,
            wake: Arc::new(Notify::new()),
            hub: HubClient::new(config, token)?,
            options,
        };
        queue.save()?;
        Ok(queue)
    }

    /// Sets how many jobs run at once. Default 1 (sequential).
    pub fn set_max_parallel(&self, n: usize) {
        self.inner.lock().unwrap().max_parallel = n.max(1);
        self.wake.notify_one();
    }

    /// Receive a stream of [`QueueEvent`]s. Only events sent after this call are seen.
    pub fn subscribe(&self) -> broadcast::Receiver<QueueEvent> {
        self.events.subscribe()
    }

    /// Snapshot of every job, in insertion order.
    pub fn list(&self) -> Vec<Job> {
        self.inner.lock().unwrap().jobs.clone()
    }

    /// Snapshot of one job by id, or `None` if it does not exist.
    pub fn get(&self, id: &str) -> Option<Job> {
        self.list().into_iter().find(|j| j.id == id)
    }

    /// Adds a job and returns its id. Idempotent: an identical request (same repo,
    /// revision, files and local_dir) returns the existing job's id and creates
    /// nothing. Use `add_forced` to queue it again.
    pub fn add(&self, spec: JobSpec) -> Result<String> {
        self.add_inner(spec, false)
    }

    /// Like `add`, but a request matching a finished (`Done`) job queues a new run.
    /// Failed or cancelled matches are reset to queued in either case.
    pub fn add_forced(&self, spec: JobSpec) -> Result<String> {
        self.add_inner(spec, true)
    }

    fn add_inner(&self, spec: JobSpec, force: bool) -> Result<String> {
        if spec.paths.is_empty() {
            return Err(Error::Invalid("job has no files".into()));
        }
        let key = dedup_key(&spec);
        {
            let mut inner = self.inner.lock().unwrap();
            let existing = inner
                .jobs
                .iter_mut()
                .find(|j| dedup_key(&j.spec) == key && !(force && j.status == Status::Done));
            if let Some(job) = existing {
                // A failed or cancelled job is reset to queued under the same id;
                // partial files stay, so the run resumes instead of starting over.
                if matches!(job.status, Status::Failed | Status::Cancelled) {
                    job.status = Status::Queued;
                    job.error = None;
                    job.retryable = false;
                    let id = job.id.clone();
                    drop(inner);
                    self.save()?;
                    return Ok(id);
                }
                return Ok(job.id.clone());
            }
        }
        let id = {
            let mut inner = self.inner.lock().unwrap();
            let id = format!("job-{}", inner.next_id);
            inner.next_id += 1;
            inner.jobs.push(Job {
                id: id.clone(),
                spec,
                status: Status::Queued,
                created_at: now(),
                error: None,
                retryable: false,
            });
            id
        };
        if let Err(error) = self.save() {
            // Undo the in-memory insert so a failed add is not run later and a
            // retry does not enqueue the same work twice.
            let mut inner = self.inner.lock().unwrap();
            inner.jobs.retain(|j| j.id != id);
            return Err(error);
        }
        let _ = self.events.send(QueueEvent::Queued { id: id.clone() });
        self.wake.notify_one();
        Ok(id)
    }

    /// Pauses a queued or running job. A running job keeps its partial files.
    pub fn pause(&self, id: &str) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();
        let job = find_mut(&mut inner.jobs, id)?;
        match job.status {
            Status::Queued => job.status = Status::Paused,
            Status::Running => {
                if let Some((token, stop)) = inner.running.get_mut(id) {
                    *stop = Stop::Pause;
                    token.cancel();
                }
            }
            other => return Err(Error::Invalid(format!("cannot pause a {other:?} job"))),
        }
        drop(inner);
        self.save()?;
        let _ = self.events.send(QueueEvent::Paused { id: id.into() });
        Ok(())
    }

    /// Puts a paused job back in the queue.
    pub fn resume(&self, id: &str) -> Result<()> {
        {
            let mut inner = self.inner.lock().unwrap();
            let job = find_mut(&mut inner.jobs, id)?;
            if job.status != Status::Paused {
                return Err(Error::Invalid(format!(
                    "cannot resume a {:?} job",
                    job.status
                )));
            }
            job.status = Status::Queued;
        }
        self.save()?;
        self.wake.notify_one();
        Ok(())
    }

    /// Cancels a job. With `delete_partial` and a `local_dir`, removes that
    /// job's unfinished files. Finished files are never deleted.
    pub fn cancel(&self, id: &str, delete_partial: bool) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();
        let job = find_mut(&mut inner.jobs, id)?;
        match job.status {
            Status::Queued | Status::Paused => {
                job.status = Status::Cancelled;
                let spec = job.spec.clone();
                drop(inner);
                let deleted = delete_partial && remove_partials(&spec);
                self.save()?;
                let _ = self.events.send(QueueEvent::Cancelled {
                    id: id.into(),
                    deleted_partial: deleted,
                });
            }
            Status::Running => {
                if let Some((token, stop)) = inner.running.get_mut(id) {
                    *stop = Stop::Cancel { delete_partial };
                    token.cancel();
                }
            }
            other => return Err(Error::Invalid(format!("cannot cancel a {other:?} job"))),
        }
        Ok(())
    }

    /// Runs queued jobs. Returns only if the future is dropped, which also
    /// aborts any job still running (its partial files stay on disk, and the
    /// job is picked up again after reopening the queue). Spawn it on a tokio
    /// runtime, or select on it to stop with the process.
    pub async fn run(&self) {
        let mut tasks: JoinSet<()> = JoinSet::new();
        loop {
            while tasks.try_join_next().is_some() {}
            let next = {
                let mut inner = self.inner.lock().unwrap();
                if inner.running.len() >= inner.max_parallel {
                    None
                } else {
                    let job = inner.jobs.iter_mut().find(|j| j.status == Status::Queued);
                    job.map(|j| {
                        j.status = Status::Running;
                        j.clone()
                    })
                }
            };
            match next {
                Some(job) => {
                    let _ = self.save();
                    let token = CancelToken::new();
                    self.inner
                        .lock()
                        .unwrap()
                        .running
                        .insert(job.id.clone(), (token.clone(), Stop::Pause));
                    let queue = self.clone();
                    tasks.spawn(async move { queue.execute(job, token).await });
                }
                None if tasks.is_empty() => self.wake.notified().await,
                None => {
                    tokio::select! {
                        _ = self.wake.notified() => {}
                        _ = tasks.join_next() => {}
                    }
                }
            }
        }
    }

    async fn execute(&self, job: Job, token: CancelToken) {
        let _ = self.events.send(QueueEvent::Started { id: job.id.clone() });
        let outcome = self.download_all(&job, &token).await;
        let stop = self
            .inner
            .lock()
            .unwrap()
            .running
            .remove(&job.id)
            .map(|(_, stop)| stop)
            .unwrap_or(Stop::Pause);

        match outcome {
            Ok(sha_ok) => {
                self.set_status(&job.id, Status::Done, None, false);
                let _ = self.events.send(QueueEvent::Finished {
                    id: job.id.clone(),
                    sha_ok,
                });
            }
            Err(Error::Cancelled) => match stop {
                Stop::Pause => {
                    self.set_status(&job.id, Status::Paused, None, false);
                }
                Stop::Cancel { delete_partial } => {
                    self.set_status(&job.id, Status::Cancelled, None, false);
                    let deleted = delete_partial && remove_partials(&job.spec);
                    let _ = self.events.send(QueueEvent::Cancelled {
                        id: job.id.clone(),
                        deleted_partial: deleted,
                    });
                }
            },
            Err(error) => {
                let retryable = is_retryable(&error);
                let reason = error.to_string();
                self.set_status(&job.id, Status::Failed, Some(reason.clone()), retryable);
                let _ = self.events.send(QueueEvent::Failed {
                    id: job.id.clone(),
                    reason,
                    retryable,
                });
            }
        }
        let _ = self.save();
        self.wake.notify_one();
    }

    async fn download_all(&self, job: &Job, token: &CancelToken) -> Result<bool> {
        let repo = RepoRef::new(job.spec.repo_type, job.spec.repo_id.clone());
        let mut options = self.options.clone();
        options.local_dir = job.spec.local_dir.clone();
        let downloader = Downloader::new(self.hub.clone(), options);
        let events = self.events.clone();
        let id = job.id.clone();
        let sink: EventSink = Arc::new(move |event: Event| {
            if let Event::Progress {
                path,
                downloaded,
                total,
                bytes_per_sec,
                eta_secs,
            } = event
            {
                let _ = events.send(QueueEvent::Progress {
                    id: id.clone(),
                    path,
                    bytes: downloaded,
                    total,
                    bytes_per_sec,
                    eta_secs,
                });
            }
        });
        for path in &job.spec.paths {
            downloader
                .download_file_cancellable(&repo, path, &job.spec.revision, &sink, token)
                .await?;
        }
        Ok(true)
    }

    fn set_status(&self, id: &str, status: Status, error: Option<String>, retryable: bool) {
        let mut inner = self.inner.lock().unwrap();
        if let Some(job) = inner.jobs.iter_mut().find(|j| j.id == id) {
            job.status = status;
            job.error = error;
            job.retryable = retryable;
        }
    }

    fn save(&self) -> Result<()> {
        let inner = self.inner.lock().unwrap();
        let persisted = Persisted {
            version: 1,
            jobs: inner.jobs.clone(),
            next_id: inner.next_id,
        };
        let path = inner.path.clone();
        drop(inner);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let tmp = path.with_extension(format!("tmp.{}", std::process::id()));
        let bytes =
            serde_json::to_vec_pretty(&persisted).map_err(|e| Error::Invalid(e.to_string()))?;
        {
            // Flush the data before the rename, so a crash cannot leave the
            // renamed file empty or truncated.
            use std::io::Write;
            let mut file = fs::File::create(&tmp)?;
            file.write_all(&bytes)?;
            file.sync_all()?;
        }
        fs::rename(&tmp, &path)?;
        Ok(())
    }
}

fn find_mut<'a>(jobs: &'a mut [Job], id: &str) -> Result<&'a mut Job> {
    jobs.iter_mut()
        .find(|j| j.id == id)
        .ok_or_else(|| Error::Invalid(format!("no job {id}")))
}

fn is_retryable(error: &Error) -> bool {
    !matches!(error, Error::Invalid(_) | Error::Checksum { .. })
}

/// Removes `.hfd.incomplete` and `.hfd.state` files from the job's
/// `<local_dir>/.cache/huggingface/download/`. Returns true if any were removed.
fn remove_partials(spec: &JobSpec) -> bool {
    let Some(local_dir) = &spec.local_dir else {
        return false;
    };
    let dir: PathBuf = Path::new(local_dir).join(".cache/huggingface/download");
    let Ok(entries) = fs::read_dir(&dir) else {
        return false;
    };
    let mut removed = false;
    for entry in entries.flatten() {
        let name = entry.file_name().to_string_lossy().to_string();
        if name.ends_with(".hfd.incomplete") || name.ends_with(".hfd.state") {
            removed |= fs::remove_file(entry.path()).is_ok();
        }
    }
    removed
}

/// Identity of a request for deduplication. Paths are sorted so order does not matter.
fn dedup_key(spec: &JobSpec) -> (String, String, String, Vec<String>, Option<PathBuf>) {
    let mut paths = spec.paths.clone();
    paths.sort();
    (
        format!("{:?}", spec.repo_type),
        spec.repo_id.clone(),
        spec.revision.clone(),
        paths,
        spec.local_dir.clone(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec() -> JobSpec {
        JobSpec {
            repo_type: RepoType::Model,
            repo_id: "o/r".into(),
            revision: "main".into(),
            paths: vec!["m.gguf".into()],
            local_dir: None,
        }
    }

    fn queue(dir: &Path) -> Queue {
        Queue::open(
            dir.join("queue.json"),
            Config::from_env(),
            None,
            DownloadOptions::default(),
        )
        .unwrap()
    }

    #[test]
    fn add_pause_resume_cancel_and_persist() {
        let dir = tempfile::tempdir().unwrap();
        let q = queue(dir.path());
        let id = q.add(spec()).unwrap();
        q.pause(&id).unwrap();
        assert_eq!(q.get(&id).unwrap().status, Status::Paused);
        q.resume(&id).unwrap();
        assert_eq!(q.get(&id).unwrap().status, Status::Queued);
        q.cancel(&id, false).unwrap();
        assert_eq!(q.get(&id).unwrap().status, Status::Cancelled);
        assert!(q.resume(&id).is_err());
    }

    #[test]
    fn restart_turns_running_into_queued_and_keeps_ids() {
        let dir = tempfile::tempdir().unwrap();
        {
            let q = queue(dir.path());
            let id = q.add(spec()).unwrap();
            q.inner.lock().unwrap().jobs[0].status = Status::Running;
            q.save().unwrap();
            assert_eq!(id, "job-1");
        }
        let q = queue(dir.path());
        let job = q.get("job-1").unwrap();
        assert_eq!(job.status, Status::Queued);
        let other = JobSpec {
            paths: vec!["other.gguf".into()],
            ..spec()
        };
        assert_eq!(q.add(other).unwrap(), "job-2");
    }

    #[test]
    fn duplicate_add_returns_existing_id() {
        let dir = tempfile::tempdir().unwrap();
        let q = queue(dir.path());
        let first = q.add(spec()).unwrap();
        let reordered = JobSpec {
            paths: vec!["m.gguf".into()],
            ..spec()
        };
        assert_eq!(q.add(reordered).unwrap(), first);
        assert_eq!(q.list().len(), 1);
    }

    #[test]
    fn done_job_is_returned_unless_forced() {
        let dir = tempfile::tempdir().unwrap();
        let q = queue(dir.path());
        let id = q.add(spec()).unwrap();
        q.inner.lock().unwrap().jobs[0].status = Status::Done;
        assert_eq!(q.add(spec()).unwrap(), id);
        assert_eq!(q.get(&id).unwrap().status, Status::Done);
        let forced = q.add_forced(spec()).unwrap();
        assert_ne!(forced, id);
        assert_eq!(q.get(&forced).unwrap().status, Status::Queued);
    }

    #[test]
    fn failed_or_cancelled_job_is_requeued_under_same_id() {
        let dir = tempfile::tempdir().unwrap();
        let q = queue(dir.path());
        let id = q.add(spec()).unwrap();
        {
            let mut inner = q.inner.lock().unwrap();
            inner.jobs[0].status = Status::Failed;
            inner.jobs[0].error = Some("boom".into());
        }
        assert_eq!(q.add(spec()).unwrap(), id);
        let job = q.get(&id).unwrap();
        assert_eq!(job.status, Status::Queued);
        assert!(job.error.is_none());
        assert_eq!(q.list().len(), 1);
    }

    #[test]
    fn different_local_dir_is_a_different_job() {
        let dir = tempfile::tempdir().unwrap();
        let q = queue(dir.path());
        let a = q.add(spec()).unwrap();
        let b = q
            .add(JobSpec {
                local_dir: Some(dir.path().join("x")),
                ..spec()
            })
            .unwrap();
        assert_ne!(a, b);
    }

    #[test]
    fn failed_add_does_not_leave_a_phantom_job() {
        let dir = tempfile::tempdir().unwrap();
        let q = queue(dir.path());
        // Replace the state directory with a file so the next save() fails.
        let state_dir = dir.path();
        fs::remove_file(state_dir.join("queue.json")).ok();
        let blocked = dir.path().join("blocked");
        fs::write(&blocked, b"x").unwrap();
        q.inner.lock().unwrap().path = blocked.join("queue.json");
        assert!(q.add(spec()).is_err());
        assert!(
            q.list().is_empty(),
            "failed add must not leave a queued job"
        );
    }

    #[test]
    fn cancel_delete_partial_removes_only_partials() {
        let dir = tempfile::tempdir().unwrap();
        let local = dir.path().join("out");
        let download = local.join(".cache/huggingface/download");
        fs::create_dir_all(&download).unwrap();
        fs::write(download.join("a.hfd.incomplete"), b"partial").unwrap();
        fs::write(download.join("a.hfd.state"), b"{}").unwrap();
        fs::write(local.join("finished.gguf"), b"done").unwrap();
        let q = queue(dir.path());
        let id = q
            .add(JobSpec {
                local_dir: Some(local.clone()),
                ..spec()
            })
            .unwrap();
        q.cancel(&id, true).unwrap();
        assert!(!download.join("a.hfd.incomplete").exists());
        assert!(!download.join("a.hfd.state").exists());
        assert!(local.join("finished.gguf").exists());
    }

    #[test]
    fn cancel_without_flag_keeps_partials() {
        let dir = tempfile::tempdir().unwrap();
        let local = dir.path().join("out");
        let download = local.join(".cache/huggingface/download");
        fs::create_dir_all(&download).unwrap();
        fs::write(download.join("a.hfd.incomplete"), b"partial").unwrap();
        let q = queue(dir.path());
        let id = q
            .add(JobSpec {
                local_dir: Some(local),
                ..spec()
            })
            .unwrap();
        q.cancel(&id, false).unwrap();
        assert!(download.join("a.hfd.incomplete").exists());
    }
}

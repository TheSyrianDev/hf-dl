//! Kills a queue mid-download, reopens it from disk, and checks the job
//! finishes with the right bytes. Needs python3; the mock hub is in
//! tests/support/flaky_hub.py. Unix only: on Windows the mock hub finishes
//! before the kill, so the test cannot observe a running job.

#![cfg(unix)]

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use hfd_core::engine::Engine;
use hfd_core::queue::{JobSpec, Queue, QueueEvent, Status};
use hfd_core::{Config, DownloadOptions, RepoType};

const SIZE: usize = 2 * 1024 * 1024;

fn hub(dir: &Path) -> (Child, u16) {
    let script = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/support/flaky_hub.py");
    let mut child = Command::new("python3")
        .arg(&script)
        .args(["--dir", dir.to_str().unwrap(), "--throttle", "300000"])
        .stdout(Stdio::piped())
        .spawn()
        .expect("python3 is needed for this test");
    let mut line = String::new();
    use std::io::BufRead;
    std::io::BufReader::new(child.stdout.as_mut().unwrap())
        .read_line(&mut line)
        .unwrap();
    let port = line
        .trim()
        .strip_prefix("ready ")
        .expect("hub ready line")
        .parse()
        .unwrap();
    (child, port)
}

/// Kills the mock hub when the test ends, even on panic.
struct Hub(Child);

impl Drop for Hub {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn partial_bytes(local: &Path) -> u64 {
    let dir = local.join(".cache/huggingface/download");
    std::fs::read_dir(&dir)
        .into_iter()
        .flatten()
        .flatten()
        .filter(|e| e.file_name().to_string_lossy().ends_with(".hfd.incomplete"))
        .map(|e| e.metadata().map(|m| m.len()).unwrap_or(0))
        .sum()
}

fn http_only() -> DownloadOptions {
    DownloadOptions {
        engine: Engine::Http,
        ..DownloadOptions::default()
    }
}

fn config(port: u16, cache: PathBuf) -> Config {
    let mut cfg = Config::from_env();
    cfg.endpoint = format!("http://127.0.0.1:{port}");
    cfg.cache_dir = cache.clone();
    cfg.xet_cache_dir = cache.join("xet");
    cfg
}

#[tokio::test(flavor = "multi_thread")]
async fn killed_job_resumes_after_reopen() {
    let tmp = tempfile::tempdir().unwrap();
    let hub_dir = tmp.path().join("hub");
    std::fs::create_dir_all(&hub_dir).unwrap();
    let data: Vec<u8> = (0..SIZE).map(|i| (i % 251) as u8).collect();
    std::fs::write(hub_dir.join("model.gguf"), &data).unwrap();

    let (hub_child, port) = hub(&hub_dir);
    let _hub = Hub(hub_child);
    let cfg = config(port, tmp.path().join("cache"));
    let state = tmp.path().join("queue.json");
    let local = tmp.path().join("out");
    let spec = JobSpec {
        repo_type: RepoType::Model,
        repo_id: "test/robust".into(),
        revision: "main".into(),
        paths: vec!["model.gguf".into()],
        local_dir: Some(local.clone()),
    };

    // First run: start the job, wait for some bytes, then "kill" the process
    // by aborting the runner task and dropping the queue.
    let q1 = Queue::open(&state, cfg.clone(), None, http_only()).unwrap();
    let id = q1.add(spec).unwrap();
    let runner = {
        let q = q1.clone();
        tokio::spawn(async move { q.run().await })
    };
    let deadline = Instant::now() + Duration::from_secs(60);
    while partial_bytes(&local) < 256 * 1024 {
        if Instant::now() >= deadline {
            let out = Command::new("find").arg(tmp.path()).output().unwrap();
            panic!(
                "download never started; job={:?}; tree:\n{}",
                q1.get(&id),
                String::from_utf8_lossy(&out.stdout)
            );
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    runner.abort();
    drop(q1);
    let killed_at = partial_bytes(&local);
    assert!(killed_at < SIZE as u64, "job finished before the kill");

    // Second run: reopen from disk. The job must come back as Queued and finish.
    let q2 = Queue::open(&state, cfg, None, http_only()).unwrap();
    assert_eq!(q2.get(&id).unwrap().status, Status::Queued);
    let mut events = q2.subscribe();
    let runner = {
        let q = q2.clone();
        tokio::spawn(async move { q.run().await })
    };
    let finished = tokio::time::timeout(Duration::from_secs(120), async {
        loop {
            match events.recv().await {
                Ok(QueueEvent::Finished { .. }) => return true,
                Ok(QueueEvent::Failed { reason, .. }) => panic!("job failed: {reason}"),
                Ok(_) => {}
                Err(_) => return false,
            }
        }
    })
    .await
    .expect("job did not finish in time");
    runner.abort();

    assert!(finished);
    assert_eq!(q2.get(&id).unwrap().status, Status::Done);
    assert_eq!(std::fs::read(local.join("model.gguf")).unwrap(), data);
}

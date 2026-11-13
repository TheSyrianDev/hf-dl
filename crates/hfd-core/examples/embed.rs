//! Minimal embedding example: list a repo's GGUF files, pick a quant,
//! download it with live progress, and cancel after a timeout.
//!
//! cargo run -p hfd-core --example embed -- <repo> <quant> [seconds]
//! e.g. cargo run -p hfd-core --example embed -- Qwen/Qwen2.5-0.5B-Instruct-GGUF Q4_K_M 10

use std::sync::Arc;
use std::time::Duration;

use hfd_core::{
    CancelToken, Config, DownloadOptions, Downloader, Error, Event, EventSink, HubClient, RepoRef,
    RepoType, auth::resolve_token, parse_quant,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let repo_id = args.next().ok_or("usage: embed <repo> <quant> [seconds]")?;
    let want = args.next().ok_or("usage: embed <repo> <quant> [seconds]")?;
    let seconds: u64 = args.next().map_or(Ok(0), |s| s.parse())?;

    // 1. Client. The token comes from HF_TOKEN or the saved login; never log it.
    let config = Config::from_env();
    let token = resolve_token(None, &config);
    let hub = HubClient::new(config.clone(), token)?;
    let repo = RepoRef::new(RepoType::Model, repo_id);

    // 2. List files and show the quants on offer.
    let files = hub.list_tree(&repo, "main").await?;
    let mut ggufs: Vec<_> = files
        .iter()
        .filter(|entry| entry.kind == "file" && entry.path.ends_with(".gguf"))
        .filter_map(|entry| Some((entry, parse_quant(&entry.path)?)))
        .collect();
    ggufs.sort_by_key(|(entry, _)| entry.size.unwrap_or(0));
    for (entry, quant) in &ggufs {
        let size = entry.size.unwrap_or(0) as f64 / 1e9;
        println!("{:>8}  {size:6.2} GB  {}", quant.name, entry.path);
    }
    let (entry, _) = ggufs
        .iter()
        .find(|(_, quant)| quant.name.eq_ignore_ascii_case(&want))
        .ok_or("no file with that quant")?;

    // 3. Download into the standard HF cache (set local_dir for a plain folder).
    let options = DownloadOptions {
        cache_dir: config.cache_dir.clone(),
        ..DownloadOptions::default()
    };
    let downloader = Downloader::new(hub, options);
    let sink: EventSink = Arc::new(|event| match event {
        Event::Progress {
            downloaded,
            total,
            bytes_per_sec,
            eta_secs,
            ..
        } => eprintln!(
            "{:5.1}%  {:6.2} MB/s  ETA {}",
            downloaded as f64 * 100.0 / total.max(1) as f64,
            bytes_per_sec / 1e6,
            eta_secs.map_or("--".into(), |s| format!("{s}s")),
        ),
        Event::FileDone { local_path, .. } => println!("done: {}", local_path.display()),
        Event::Message { text: message } => eprintln!("{message}"),
        Event::FileStart { resumed_bytes, .. } if resumed_bytes > 0 => {
            eprintln!("resuming at {resumed_bytes} bytes")
        }
        _ => {}
    });

    // 4. Cancel (pause) after `seconds`; running it again resumes.
    let cancel = CancelToken::new();
    if seconds > 0 {
        let cancel = cancel.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(seconds)).await;
            cancel.cancel();
        });
    }
    match downloader
        .download_file_cancellable(&repo, &entry.path, "main", &sink, &cancel)
        .await
    {
        Ok(file) => println!("saved to {}", file.local_path.display()),
        Err(Error::Cancelled) => println!("paused; run again to resume"),
        Err(error) => return Err(error.into()),
    }
    Ok(())
}

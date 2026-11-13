//! `hfd doctor`: checks what a download would run into and recommends settings.
//! Read-only. Checks network reachability, Xet vs HTTP state, and free disk at the target.

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use anyhow::Result;
use hfd_core::engine::xet_failed_recently;
use hfd_core::{Config, HubClient};
use serde_json::{Value, json};

#[derive(Debug, clap::Args)]
pub struct DoctorArgs {
    /// Directory the download would write to. Defaults to the cache directory.
    #[arg(long)]
    pub target: Option<PathBuf>,
    /// Size in bytes of the largest file you plan to download, to compare with free disk.
    #[arg(long)]
    pub need: Option<u64>,
    /// Print the report as JSON (for Last AI and other callers).
    #[arg(long)]
    pub json: bool,
}

pub async fn run(args: DoctorArgs) -> Result<bool> {
    let config = Config::from_env();
    let target = args
        .target
        .clone()
        .unwrap_or_else(|| config.cache_dir.clone());
    let report = build_report(&config, &target, args.need).await;
    if args.json {
        println!("{}", serde_json::to_string_pretty(&report)?);
    } else {
        print_text(&report);
    }
    Ok(report["ok"].as_bool().unwrap_or(false))
}

async fn build_report(config: &Config, target: &Path, need: Option<u64>) -> Value {
    let network = check_network(config).await;
    let xet_marker = xet_failed_recently(config);
    let disk = check_disk(target, need);

    let token = hfd_core::auth::resolve_token(None, config).is_some();
    let mut problems = Vec::new();
    if !network["reachable"].as_bool().unwrap_or(false) {
        problems.push("network: cannot reach the Hugging Face endpoint");
    }
    if !disk["enough"].as_bool().unwrap_or(true) {
        problems.push("disk: not enough free space at the target");
    }

    json!({
        "ok": problems.is_empty(),
        "problems": problems,
        "endpoint": config.endpoint,
        "network": network,
        "auth": { "token_present": token },
        "xet": {
            "failed_recently": xet_marker,
            "note": "Xet is opt-in; an earlier stall switches a host to HTTP for 24 hours",
        },
        "disk": disk,
        "pirateface": { "aria2c_available": aria2c_available(), "optional": true },
        "recommended": {
            "engine": "http",
            "connections": 8,
            "verify": "auto",
            "max_speed": null,
        },
    })
}

/// Pirate Face is opt-in and needs an external `aria2c`; its absence is not a problem.
fn aria2c_available() -> bool {
    std::process::Command::new("aria2c")
        .arg("--version")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

async fn check_network(config: &Config) -> Value {
    let token = hfd_core::auth::resolve_token(None, config);
    let hub = match HubClient::new(config.clone(), token) {
        Ok(hub) => hub,
        Err(error) => {
            return json!({ "reachable": false, "error": error.to_string() });
        }
    };
    let url = format!(
        "{}/api/models?limit=1",
        config.endpoint.trim_end_matches('/')
    );
    let started = Instant::now();
    let result = hub
        .http_client()
        .get(&url)
        .timeout(Duration::from_secs(15))
        .send()
        .await;
    match result {
        Ok(response) => json!({
            "reachable": response.status().is_success(),
            "status": response.status().as_u16(),
            "latency_ms": started.elapsed().as_millis() as u64,
        }),
        Err(error) => json!({ "reachable": false, "error": error.to_string() }),
    }
}

fn check_disk(target: &Path, need: Option<u64>) -> Value {
    let mut probe = target.to_path_buf();
    while !probe.exists() {
        match probe.parent() {
            Some(parent) if parent != probe => probe = parent.to_path_buf(),
            _ => break,
        }
    }
    match fs2::available_space(&probe) {
        Ok(free) => json!({
            "path": target.display().to_string(),
            "free_bytes": free,
            "need_bytes": need,
            "enough": need.is_none_or(|need| free >= need),
        }),
        Err(error) => json!({
            "path": target.display().to_string(),
            "error": error.to_string(),
            "enough": need.is_none(),
        }),
    }
}

fn print_text(report: &Value) {
    println!("endpoint: {}", report["endpoint"].as_str().unwrap_or("?"));
    let aria = report["pirateface"]["aria2c_available"]
        .as_bool()
        .unwrap_or(false);
    println!(
        "pirateface: aria2c {} (optional)",
        if aria { "found" } else { "not installed" }
    );
    let net = &report["network"];
    if net["reachable"].as_bool().unwrap_or(false) {
        println!("network:  ok ({} ms)", net["latency_ms"]);
    } else {
        println!(
            "network:  FAILED {}",
            net.get("error")
                .or(net.get("status"))
                .unwrap_or(&Value::Null)
        );
    }
    println!(
        "token:    {}",
        if report["auth"]["token_present"].as_bool().unwrap_or(false) {
            "present"
        } else {
            "none (public repos only)"
        }
    );
    println!(
        "xet:      {}",
        if report["xet"]["failed_recently"].as_bool().unwrap_or(false) {
            "failed in the last 24h, using HTTP"
        } else {
            "no recent failure"
        }
    );
    let disk = &report["disk"];
    println!(
        "disk:     {} bytes free at {}",
        disk["free_bytes"],
        disk["path"].as_str().unwrap_or("?")
    );
    println!(
        "recommended: --engine {} --connections {} --verify auto",
        report["recommended"]["engine"].as_str().unwrap_or("http"),
        report["recommended"]["connections"]
    );
    for problem in report["problems"].as_array().into_iter().flatten() {
        println!("problem:  {}", problem.as_str().unwrap_or(""));
    }
    println!(
        "{}",
        if report["ok"].as_bool().unwrap_or(false) {
            "ok"
        } else {
            "not ready"
        }
    );
}

//! `hfd lastengine`: fetch the Last Engine release asset for this machine and
//! check it against the release's SHA256SUMS.txt before keeping it.

use std::io::Write;
use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use clap::Args;
use sha2::{Digest, Sha256};

const REPO: &str = "TheSyrianDev/LastEngine";

#[derive(Debug, Args)]
pub struct LastEngineArgs {
    /// Folder to save the asset in (created if missing).
    #[arg(long, default_value = ".")]
    pub dest: PathBuf,
    /// Release tag to fetch, or `latest`.
    #[arg(long, default_value = "latest")]
    pub tag: String,
    /// Build variant: `auto` (pick from hw-probe), or `cuda`. Only `cuda` is published today.
    #[arg(long, default_value = "auto")]
    pub variant: String,
    /// Path to the hw-probe script used by `--variant auto`.
    #[arg(long, default_value = "~/AI/Rig/bin/hw-probe")]
    pub hw_probe: String,
    /// Print what would be fetched and stop.
    #[arg(long)]
    pub dry_run: bool,
}

/// Asset name for this OS and CPU, e.g. `lastengine-linux-x86_64-cuda.tar.gz`.
pub fn asset_name(os: &str, arch: &str, variant: &str) -> Result<String> {
    let os_part = match os {
        "linux" => "linux",
        "windows" => "windows",
        other => bail!("no Last Engine build for {other} yet"),
    };
    if arch != "x86_64" {
        bail!("no Last Engine build for {arch} yet");
    }
    let ext = if os == "windows" { "zip" } else { "tar.gz" };
    Ok(format!("lastengine-{os_part}-{arch}-{variant}.{ext}"))
}

/// Finds `name`'s checksum in a `sha256sum`-style file.
pub fn expected_sha256(sums: &str, name: &str) -> Option<String> {
    sums.lines().find_map(|line| {
        let mut parts = line.split_whitespace();
        let hash = parts.next()?;
        let file = parts.next()?.trim_start_matches('*');
        (file == name && hash.len() == 64).then(|| hash.to_ascii_lowercase())
    })
}

/// Picks the build variant from hw-probe's JSON. NVIDIA GPUs with at least
/// 4 GB VRAM get `cuda`. Anything else has no published build yet, so it errors
/// instead of guessing.
pub fn variant_from_probe(json: &str) -> Result<String> {
    let v: serde_json::Value = serde_json::from_str(json).context("hw-probe output is not JSON")?;
    let gpu = &v["gpu"];
    let vendor = gpu["vendor"].as_str().unwrap_or_default();
    let vram = gpu["vram_mib"].as_u64().unwrap_or(0);
    if vendor == "nvidia" && vram >= 4096 {
        return Ok("cuda".into());
    }
    bail!(
        "no Last Engine build for this machine yet (GPU vendor {:?}, {} MiB VRAM; needs NVIDIA with >= 4096 MiB)",
        vendor,
        vram
    )
}

fn resolve_variant(variant: &str, hw_probe: &str) -> Result<String> {
    if variant != "auto" {
        return Ok(variant.to_string());
    }
    let path = hw_probe.replacen('~', &std::env::var("HOME").unwrap_or_default(), 1);
    let out = std::process::Command::new(&path)
        .output()
        .with_context(|| format!("running hw-probe at {path}"))?;
    if !out.status.success() {
        bail!("hw-probe failed: {}", String::from_utf8_lossy(&out.stderr));
    }
    variant_from_probe(&String::from_utf8_lossy(&out.stdout))
}

fn release_base(tag: &str) -> String {
    if tag == "latest" {
        format!("https://github.com/{REPO}/releases/latest/download")
    } else {
        format!("https://github.com/{REPO}/releases/download/{tag}")
    }
}

pub async fn run(args: LastEngineArgs) -> Result<()> {
    let variant = resolve_variant(&args.variant, &args.hw_probe)?;
    let name = asset_name(std::env::consts::OS, std::env::consts::ARCH, &variant)?;
    let base = release_base(&args.tag);
    if args.dry_run {
        println!("would fetch {base}/{name} and verify against {base}/SHA256SUMS.txt");
        return Ok(());
    }
    // GitHub answers /releases/latest/download/* with a 302 to the asset host.
    // The Hub client does not follow redirects, so it would read an empty body.
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::limited(10))
        .build()?;

    let sums_url = format!("{base}/SHA256SUMS.txt");
    let sums = client
        .get(&sums_url)
        .send()
        .await
        .and_then(|r| r.error_for_status())
        .with_context(|| format!("fetching {sums_url}"))?
        .text()
        .await?;
    let expected = expected_sha256(&sums, &name)
        .with_context(|| format!("{name} is not listed in SHA256SUMS.txt"))?;

    std::fs::create_dir_all(&args.dest)?;
    let target = args.dest.join(&name);
    let asset_url = format!("{base}/{name}");
    let mut response = client
        .get(&asset_url)
        .send()
        .await
        .and_then(|r| r.error_for_status())
        .with_context(|| format!("fetching {asset_url}"))?;

    let partial = target.with_extension("partial");
    let mut file = std::fs::File::create(&partial)?;
    let mut hasher = Sha256::new();
    while let Some(chunk) = response.chunk().await? {
        hasher.update(&chunk);
        file.write_all(&chunk)?;
    }
    file.sync_all()?;
    drop(file);

    let actual = format!("{:x}", hasher.finalize());
    if actual != expected {
        let _ = std::fs::remove_file(&partial);
        bail!("SHA-256 mismatch for {name}: expected {expected}, got {actual}; file discarded");
    }
    std::fs::rename(&partial, &target)?;
    println!("{}", target.display());
    println!("sha256 {actual} (verified against SHA256SUMS.txt)");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn picks_asset_per_platform() {
        assert_eq!(
            asset_name("linux", "x86_64", "cuda").unwrap(),
            "lastengine-linux-x86_64-cuda.tar.gz"
        );
        assert_eq!(
            asset_name("windows", "x86_64", "cuda").unwrap(),
            "lastengine-windows-x86_64-cuda.zip"
        );
        assert!(asset_name("macos", "aarch64", "cuda").is_err());
        assert!(asset_name("linux", "aarch64", "cuda").is_err());
    }

    #[test]
    fn variant_follows_probe_gpu() {
        let gpu = r#"{"gpu":{"vendor":"nvidia","vram_mib":4096}}"#;
        assert_eq!(variant_from_probe(gpu).unwrap(), "cuda");
        let small = r#"{"gpu":{"vendor":"nvidia","vram_mib":2048}}"#;
        assert!(variant_from_probe(small).is_err());
        let cpu = r#"{"gpu":null}"#;
        assert!(variant_from_probe(cpu).is_err());
        assert!(variant_from_probe("nope").is_err());
    }

    #[test]
    fn reads_checksum_for_named_asset() {
        let sums = "d28268f953dc9f62fb4f51b3332ad15ae5a09ce66ab28c5dff11486926f0cd5f  lastengine-linux-x86_64-cuda.tar.gz\nefda67bc7068ab5908c6c749bfa0aebf893eeb2ce4ce122cd74fdc2968d3416f  *lastengine-windows-x86_64-cuda.zip\n";
        assert_eq!(
            expected_sha256(sums, "lastengine-linux-x86_64-cuda.tar.gz").unwrap(),
            "d28268f953dc9f62fb4f51b3332ad15ae5a09ce66ab28c5dff11486926f0cd5f"
        );
        assert_eq!(
            expected_sha256(sums, "lastengine-windows-x86_64-cuda.zip").unwrap(),
            "efda67bc7068ab5908c6c749bfa0aebf893eeb2ce4ce122cd74fdc2968d3416f"
        );
        assert!(expected_sha256(sums, "missing.tar.gz").is_none());
    }
}

//! `hfd download --source pirateface`: opt-in transfer through Pirate Face.
//! Lookup over HTTP, torrent transfer through `aria2c`, then sha256 verification.
//! Never falls back to Hugging Face.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use hfd_core::pirateface::{self, AriaTorrent};

pub async fn run(
    repo_id: &str,
    revision: Option<&str>,
    filenames: &[String],
    local_dir: Option<&Path>,
    cache_root: &Path,
) -> Result<()> {
    let base = std::env::var("PF_API").unwrap_or_else(|_| pirateface::DEFAULT_API.to_string());
    let paths = run_with_cache(
        &base,
        PathBuf::from("aria2c"),
        repo_id,
        revision,
        filenames,
        local_dir,
        Some(cache_root),
    )
    .await?;
    for path in paths {
        println!("{}", path.display());
    }
    Ok(())
}

/// Runner with the API base and aria2c binary injected, so tests can point it at a
/// local mock server and a stub binary.
#[cfg(all(test, unix))]
pub async fn run_with(
    base: &str,
    aria2c: PathBuf,
    repo_id: &str,
    revision: Option<&str>,
    filenames: &[String],
    local_dir: Option<&Path>,
) -> Result<Vec<PathBuf>> {
    run_with_cache(base, aria2c, repo_id, revision, filenames, local_dir, None).await
}

/// As `run_with`, plus an HF cache root used when no `local_dir` is given.
pub async fn run_with_cache(
    base: &str,
    aria2c: PathBuf,
    repo_id: &str,
    revision: Option<&str>,
    filenames: &[String],
    local_dir: Option<&Path>,
    cache_root: Option<&Path>,
) -> Result<Vec<PathBuf>> {
    if local_dir.is_none() && cache_root.is_none() {
        bail!("--source pirateface needs --local-dir or an HF cache directory");
    }
    let entry = match pirateface::lookup_at(base, repo_id)
        .await
        .context("Pirate Face lookup")?
    {
        Some(entry) => entry,
        None => bail!("{repo_id} is not listed on Pirate Face"),
    };
    if !entry.hf {
        bail!(
            "{repo_id} is a Pirate Face-only entry (no Hugging Face commit); this is not supported yet"
        );
    }
    if let Some(wanted) = revision
        && wanted != entry.revision
    {
        bail!(
            "revision mismatch: Pirate Face has {}, you asked for {wanted}",
            entry.revision
        );
    }
    let backend = AriaTorrent { bin: aria2c };
    // Without --local-dir, stage in the cache root, then place each verified file
    // into the HF cache layout (blobs/, snapshots/, refs/).
    let staging = match (local_dir, cache_root) {
        (Some(dir), _) => dir.to_path_buf(),
        (None, Some(root)) => root.join(".pf-staging").join(repo_id.replace('/', "--")),
        (None, None) => unreachable!("checked above"),
    };
    let fetched = pirateface::fetch_verified(&backend, &entry, filenames, &staging)
        .context("Pirate Face transfer")?;
    let Some(root) = local_dir.is_none().then_some(cache_root).flatten() else {
        return Ok(fetched);
    };
    let repo = hfd_core::hub::RepoRef::new(hfd_core::uri::RepoType::Model, repo_id);
    let wanted_revision = revision.unwrap_or(&entry.revision);
    let mut placed = Vec::new();
    for file in entry
        .files
        .iter()
        .filter(|f| fetched.iter().any(|p| p.ends_with(&f.path)))
    {
        let src = staging.join(&file.path);
        let snap = hfd_core::cache::place_verified(
            root,
            &repo,
            &entry.revision,
            &file.path,
            &file.sha256,
            wanted_revision,
            &src,
        )
        .context("placing file in HF cache")?;
        placed.push(snap);
    }
    let _ = std::fs::remove_dir_all(&staging);
    Ok(placed)
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::os::unix::fs::PermissionsExt;

    const LISTED: &str = r#"{"repoId":"acme/tiny","revision":"rev1","hf":true,"torrent":"/t/x.torrent",
        "files":[{"path":"model.gguf","size":3,"sha256":"ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"}]}"#;
    const PF_ONLY: &str = r#"{"repoId":"acme/tiny","revision":"rev1","hf":false,"torrent":"/t/x.torrent",
        "files":[{"path":"model.gguf","size":3,"sha256":"ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"}]}"#;

    /// Serves one canned response per request on 127.0.0.1, then exits.
    /// Returns the base URL. Nothing leaves loopback.
    fn mock_api(status: u16, body: &'static str) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        std::thread::spawn(move || {
            if let Ok((mut conn, _)) = listener.accept() {
                let mut buf = [0u8; 4096];
                let _ = conn.read(&mut buf);
                let reason = if status == 200 { "OK" } else { "ERR" };
                let resp = format!(
                    "HTTP/1.1 {status} {reason}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = conn.write_all(resp.as_bytes());
            }
        });
        base
    }

    /// A stub `aria2c` that writes the expected file contents into --dir. It never
    /// touches the network. `content` is written to `model.gguf`.
    fn stub_aria2c(dir: &Path, content: &str) -> PathBuf {
        let bin = dir.join("aria2c");
        let script = format!(
            "#!/bin/sh\ndest=\"\"\nfor a in \"$@\"; do case \"$a\" in --dir=*) dest=\"${{a#--dir=}}\";; esac; if [ \"$prev\" = \"--dir\" ]; then dest=\"$a\"; fi; prev=\"$a\"; done\nmkdir -p \"$dest\"\nprintf '{content}' > \"$dest/model.gguf\"\n"
        );
        std::fs::write(&bin, script).unwrap();
        std::fs::set_permissions(&bin, std::fs::Permissions::from_mode(0o755)).unwrap();
        bin
    }

    fn rt() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
    }

    #[test]
    fn transfers_and_verifies_listed_entry() {
        let dir = tempfile::tempdir().unwrap();
        let bin = stub_aria2c(dir.path(), "abc");
        let dest = dir.path().join("out");
        let base = mock_api(200, LISTED);
        let paths = rt()
            .block_on(run_with(
                &base,
                bin,
                "acme/tiny",
                Some("rev1"),
                &[],
                Some(&dest),
            ))
            .unwrap();
        assert_eq!(paths, vec![dest.join("model.gguf")]);
        assert_eq!(std::fs::read(dest.join("model.gguf")).unwrap(), b"abc");
    }

    #[test]
    fn corrupt_download_is_rejected_and_removed() {
        let dir = tempfile::tempdir().unwrap();
        let bin = stub_aria2c(dir.path(), "wrong");
        let dest = dir.path().join("out");
        let base = mock_api(200, LISTED);
        let err = rt()
            .block_on(run_with(&base, bin, "acme/tiny", None, &[], Some(&dest)))
            .unwrap_err();
        assert!(format!("{err:#}").contains("sha256 mismatch"));
        assert!(!dest.join("model.gguf").exists());
    }

    #[test]
    fn unlisted_repo_is_refused_without_transfer() {
        let dir = tempfile::tempdir().unwrap();
        let bin = stub_aria2c(dir.path(), "abc");
        let dest = dir.path().join("out");
        let base = mock_api(404, "");
        let err = rt()
            .block_on(run_with(&base, bin, "acme/tiny", None, &[], Some(&dest)))
            .unwrap_err();
        assert!(format!("{err:#}").contains("not listed"));
        assert!(!dest.exists());
    }

    #[test]
    fn pf_only_entry_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let bin = stub_aria2c(dir.path(), "abc");
        let dest = dir.path().join("out");
        let base = mock_api(200, PF_ONLY);
        let err = rt()
            .block_on(run_with(&base, bin, "acme/tiny", None, &[], Some(&dest)))
            .unwrap_err();
        assert!(format!("{err:#}").contains("Pirate Face-only"));
        assert!(!dest.exists());
    }

    #[test]
    fn revision_mismatch_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let bin = stub_aria2c(dir.path(), "abc");
        let dest = dir.path().join("out");
        let base = mock_api(200, LISTED);
        let err = rt()
            .block_on(run_with(
                &base,
                bin,
                "acme/tiny",
                Some("other"),
                &[],
                Some(&dest),
            ))
            .unwrap_err();
        assert!(format!("{err:#}").contains("revision mismatch"));
    }

    #[test]
    fn missing_local_dir_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let bin = stub_aria2c(dir.path(), "abc");
        let base = mock_api(200, LISTED);
        let err = rt()
            .block_on(run_with(&base, bin, "acme/tiny", None, &[], None))
            .unwrap_err();
        assert!(format!("{err:#}").contains("--local-dir"));
    }

    #[test]
    fn missing_aria2c_reports_unavailable() {
        let dir = tempfile::tempdir().unwrap();
        let dest = dir.path().join("out");
        let base = mock_api(200, LISTED);
        let err = rt()
            .block_on(run_with(
                &base,
                dir.path().join("no-such-aria2c"),
                "acme/tiny",
                None,
                &[],
                Some(&dest),
            ))
            .unwrap_err();
        assert!(format!("{err:#}").contains("unavailable"));
    }

    #[test]
    fn without_local_dir_places_file_in_hf_cache_layout() {
        let dir = tempfile::tempdir().unwrap();
        let bin = stub_aria2c(dir.path(), "abc");
        let cache = dir.path().join("hub");
        let base = mock_api(200, LISTED);
        let paths = rt()
            .block_on(run_with_cache(
                &base,
                bin,
                "acme/tiny",
                None,
                &[],
                None,
                Some(&cache),
            ))
            .unwrap();
        let repo = cache.join("models--acme--tiny");
        assert_eq!(paths, vec![repo.join("snapshots/rev1/model.gguf")],);
        assert_eq!(std::fs::read(&paths[0]).unwrap(), b"abc");
        assert_eq!(
            std::fs::read_to_string(repo.join("refs/rev1")).unwrap(),
            "rev1"
        );
        assert!(
            repo.join("blobs/ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad")
                .exists()
        );
        assert!(!cache.join(".pf-staging").join("acme--tiny").exists());
    }
}

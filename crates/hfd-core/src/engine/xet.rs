use std::fs;
use std::time::Duration;

use url::Url;
use xet::xet_session::{
    HeaderMap, HeaderValue, XetFileDownload, XetFileInfo, XetSession, XetSessionBuilder, header,
};

use crate::cache::Target;
use crate::engine::http::{Downloader, remove_file_if_exists};
use crate::error::{Error, Result};
use crate::hub::{RepoRef, ResolvedFile};
use crate::progress::{Event, EventSink};

impl Downloader {
    pub(crate) async fn download_xet(
        &self,
        repo: &RepoRef,
        path: &str,
        revision: &str,
        resolved: &ResolvedFile,
        target: &Target,
        sink: &EventSink,
    ) -> Result<(u64, &'static str)> {
        let xet_hash = resolved
            .xet_hash
            .as_deref()
            .ok_or_else(|| Error::Invalid("the file has no Xet hash".to_string()))?;

        let session = self.xet_session().await?;
        let mut headers = HeaderMap::new();
        if let Some(token) = self.hub().token() {
            let value = HeaderValue::from_str(&format!("Bearer {token}")).map_err(|error| {
                Error::Invalid(format!("invalid Xet authorization header: {error}"))
            })?;
            headers.insert(header::AUTHORIZATION, value);
        }

        let group = session
            .new_file_download_group()
            .map_err(xet_error)?
            .with_token_refresh_url(
                xet_token_url(&self.hub().config().endpoint, repo, revision)?,
                headers,
            )
            .build()
            .await
            .map_err(xet_error)?;

        remove_file_if_exists(target.state_path())?;
        sink(Event::FileStart {
            path: path.to_string(),
            size: resolved.size,
            destination: target.reported_path(),
            resumed_bytes: 0,
        });

        let file_info = XetFileInfo {
            hash: xet_hash.to_string(),
            file_size: Some(resolved.size),
            sha256: resolved.sha256.clone(),
        };
        let mut guard = XetGroupGuard::new(group.clone());
        let handle = group
            .download_file_to_path(file_info, target.partial_path().to_path_buf())
            .await
            .map_err(xet_error)?;
        let progress_task = AbortTask::new(spawn_progress(
            sink.clone(),
            path.to_string(),
            resolved.size,
            handle.clone(),
        ));

        // Xet retries failed chunks internally and can spin forever without
        // moving forward, so give up if completed bytes stop increasing.
        tokio::select! {
            result = group.finish() => { result.map_err(xet_error)?; }
            stalled = watch_stall(handle.clone(), XET_STALL_TIMEOUT) => {
                return Err(Error::XetStalled(stalled.as_secs()));
            }
        }
        guard.disarm();
        drop(progress_task);

        let actual = fs::metadata(target.partial_path())
            .map(|metadata| metadata.len())
            .unwrap_or(0);
        if actual != resolved.size {
            return Err(Error::Xet(format!(
                "expected {} bytes, wrote {actual}",
                resolved.size
            )));
        }

        sink(Event::Progress {
            path: path.to_string(),
            downloaded: resolved.size,
            total: resolved.size,
        });
        Ok((0, "xet"))
    }

    async fn xet_session(&self) -> Result<&XetSession> {
        self.xet
            .get_or_try_init(|| async {
                XetSessionBuilder::new()
                    .with_cache_dir(self.hub().config().xet_cache_dir.clone())
                    .build()
                    .map_err(xet_error)
            })
            .await
    }
}

fn xet_token_url(endpoint: &str, repo: &RepoRef, revision: &str) -> Result<String> {
    let mut url = Url::parse(&format!(
        "{}/api/{}/{}/xet-read-token",
        endpoint.trim_end_matches('/'),
        repo.repo_type.api_segment(),
        repo.repo_id
    ))?;
    url.path_segments_mut()
        .map_err(|_| Error::Invalid("cannot build the Xet token URL".to_string()))?
        .push(revision);
    Ok(url.to_string())
}

const XET_STALL_TIMEOUT: Duration = Duration::from_secs(120);

/// Resolves once completed bytes have not grown for `timeout`.
async fn watch_stall(handle: XetFileDownload, timeout: Duration) -> Duration {
    let mut last = 0u64;
    let mut last_change = tokio::time::Instant::now();
    loop {
        tokio::time::sleep(Duration::from_secs(1)).await;
        let done = handle.progress().map(|r| r.bytes_completed).unwrap_or(0);
        if done > last {
            last = done;
            last_change = tokio::time::Instant::now();
        } else if last_change.elapsed() >= timeout {
            return timeout;
        }
    }
}

fn spawn_progress(
    sink: EventSink,
    path: String,
    total: u64,
    handle: XetFileDownload,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_millis(200)).await;
            if let Some(report) = handle.progress() {
                sink(Event::Progress {
                    path: path.clone(),
                    downloaded: report.bytes_completed.min(total),
                    total,
                });
            }
        }
    })
}

struct XetGroupGuard {
    group: Option<xet::xet_session::XetFileDownloadGroup>,
}

impl XetGroupGuard {
    fn new(group: xet::xet_session::XetFileDownloadGroup) -> Self {
        Self { group: Some(group) }
    }

    fn disarm(&mut self) {
        self.group.take();
    }
}

impl Drop for XetGroupGuard {
    fn drop(&mut self) {
        if let Some(group) = self.group.take() {
            let _ = group.abort();
        }
    }
}

struct AbortTask(tokio::task::JoinHandle<()>);

impl AbortTask {
    fn new(handle: tokio::task::JoinHandle<()>) -> Self {
        Self(handle)
    }
}

impl Drop for AbortTask {
    fn drop(&mut self) {
        self.0.abort();
    }
}

fn xet_error(error: impl std::fmt::Display) -> Error {
    Error::Xet(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::uri::RepoType;

    #[test]
    fn builds_xet_token_url() {
        let repo = RepoRef::new(RepoType::Model, "owner/repo");
        assert_eq!(
            xet_token_url("https://huggingface.co/", &repo, "refs/pr/1").unwrap(),
            "https://huggingface.co/api/models/owner/repo/xet-read-token/refs%2Fpr%2F1"
        );
    }
}

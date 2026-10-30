use std::time::{Duration, SystemTime, UNIX_EPOCH};

use percent_encoding::{AsciiSet, CONTROLS, utf8_percent_encode};
use reqwest::header::{HeaderMap, HeaderValue};
use reqwest::{Client, Response, StatusCode};
use serde::Deserialize;
use url::Url;

use crate::config::Config;
use crate::error::{Error, Result};
use crate::uri::{HfUri, RepoType};

const PATH_ENCODE_SET: &AsciiSet = &CONTROLS
    .add(b' ')
    .add(b'"')
    .add(b'#')
    .add(b'%')
    .add(b'?')
    .add(b'`')
    .add(b'{')
    .add(b'}')
    .add(b'<')
    .add(b'>');

const REVISION_ENCODE_SET: &AsciiSet = &PATH_ENCODE_SET.add(b'/').add(b'\\').add(b'&').add(b'+');

#[derive(Debug, Clone)]
pub struct RepoRef {
    pub repo_type: RepoType,
    pub repo_id: String,
}

impl RepoRef {
    pub fn new(repo_type: RepoType, repo_id: impl Into<String>) -> Self {
        Self {
            repo_type,
            repo_id: repo_id.into(),
        }
    }

    pub fn label(&self) -> String {
        format!("{}:{}", self.repo_type.as_str(), self.repo_id)
    }
}

#[derive(Debug, Clone)]
pub struct ResolvedFile {
    pub commit: String,
    pub size: u64,
    pub etag: Option<String>,
    pub sha256: Option<String>,
    pub xet_hash: Option<String>,
    pub location: String,
    pub auth_required: bool,
    pub expires_at: Option<SystemTime>,
    pub accept_ranges: bool,
}

#[derive(Debug, Clone)]
pub struct LocationInfo {
    pub location: String,
    pub auth_required: bool,
    pub expires_at: Option<SystemTime>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TreeEntry {
    #[serde(rename = "type")]
    pub kind: String,
    pub path: String,
    #[serde(default)]
    pub size: Option<u64>,
    #[serde(default)]
    pub oid: Option<String>,
    #[serde(default)]
    pub lfs: Option<LfsInfo>,
    #[serde(default, rename = "xetHash")]
    pub xet_hash: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct LfsInfo {
    #[serde(default)]
    pub oid: Option<String>,
    #[serde(default)]
    pub size: Option<u64>,
}

#[derive(Clone)]
pub struct HubClient {
    config: Config,
    token: Option<String>,
    resolve_client: Client,
    api_client: Client,
}

impl HubClient {
    pub fn new(config: Config, token: Option<String>) -> Result<Self> {
        let resolve_client = Client::builder()
            .user_agent(config.user_agent.clone())
            .connect_timeout(Duration::from_secs(30))
            .read_timeout(Duration::from_secs(60))
            .redirect(reqwest::redirect::Policy::none())
            // HTTP/2 would multiplex every segment onto one TCP connection,
            // defeating parallel downloads.
            .http1_only()
            .build()?;

        let api_client = Client::builder()
            .user_agent(config.user_agent.clone())
            .connect_timeout(Duration::from_secs(30))
            .read_timeout(Duration::from_secs(60))
            .redirect(reqwest::redirect::Policy::limited(5))
            .build()?;

        Ok(Self {
            config,
            token,
            resolve_client,
            api_client,
        })
    }

    pub fn config(&self) -> &Config {
        &self.config
    }

    pub fn token(&self) -> Option<&str> {
        self.token.as_deref()
    }

    pub fn has_token(&self) -> bool {
        self.token.is_some()
    }

    pub fn http_client(&self) -> &Client {
        &self.resolve_client
    }

    pub fn repo_ref(uri: &HfUri) -> RepoRef {
        RepoRef::new(
            uri.repo_type.unwrap_or(RepoType::Model),
            uri.repo_id.clone(),
        )
    }

    pub async fn resolve(
        &self,
        repo: &RepoRef,
        path: &str,
        revision: &str,
    ) -> Result<ResolvedFile> {
        let head = self.head_info(repo, path, revision).await?;
        let mut size = header_u64(&head.headers, "x-linked-size");

        if size.is_none() && !head.redirected && head.auth_required {
            size = header_u64(&head.headers, "content-length");
        }
        if size.is_none() {
            size = self.probe_size(&head.location, head.auth_required).await?;
        }
        let size = size.ok_or_else(|| {
            Error::Invalid(format!(
                "the Hub did not report a size for {}/{path}",
                repo.repo_id
            ))
        })?;

        let etag = header_value(&head.headers, "x-linked-etag")
            .or_else(|| header_value(&head.headers, "etag"))
            .map(|value| clean_etag(&value));

        let sha256 = etag
            .as_deref()
            .filter(|value| is_hex_len(value, 64))
            .map(str::to_string);

        let commit = match header_value(&head.headers, "x-repo-commit") {
            Some(commit) => commit,
            None if is_hex_len(revision, 40) => revision.to_string(),
            None => self.resolve_commit(repo, revision).await?,
        };

        let accept_ranges = header_value(&head.headers, "accept-ranges")
            .map(|value| value.to_ascii_lowercase().contains("bytes"))
            .unwrap_or(true);

        Ok(ResolvedFile {
            commit,
            size,
            etag,
            sha256,
            xet_hash: header_value(&head.headers, "x-xet-hash"),
            location: head.location,
            auth_required: head.auth_required,
            expires_at: head.expires_at,
            accept_ranges,
        })
    }

    pub async fn resolve_location(
        &self,
        repo: &RepoRef,
        path: &str,
        revision: &str,
    ) -> Result<LocationInfo> {
        let head = self.head_info(repo, path, revision).await?;
        Ok(LocationInfo {
            location: head.location,
            auth_required: head.auth_required,
            expires_at: head.expires_at,
        })
    }

    async fn head_info(&self, repo: &RepoRef, path: &str, revision: &str) -> Result<HeadInfo> {
        if path.is_empty() {
            return Err(Error::Invalid(
                "cannot resolve an empty file path".to_string(),
            ));
        }
        let url = self.resolve_url(repo, path, revision);
        let response = self
            .resolve_client
            .head(&url)
            .headers(self.auth_headers())
            .timeout(Duration::from_secs(90))
            .send()
            .await?;

        let status = response.status();
        let headers = response.headers().clone();
        log_rate_limit(&headers);

        if status.is_redirection() {
            let location = headers
                .get("location")
                .and_then(|value| value.to_str().ok())
                .ok_or_else(|| Error::Status {
                    status: status.as_u16(),
                    url: url.clone(),
                    body: "redirect without a Location header".to_string(),
                })?;
            let location = Url::parse(&url)?.join(location)?.to_string();
            return Ok(HeadInfo {
                auth_required: same_host(&location, &self.config.endpoint),
                expires_at: url_expiry(&location),
                location,
                headers,
                redirected: true,
            });
        }

        if status == StatusCode::METHOD_NOT_ALLOWED || status == StatusCode::NOT_IMPLEMENTED {
            return self.head_info_via_range(repo, path, revision).await;
        }

        if status.is_success() {
            return Ok(HeadInfo {
                location: url,
                auth_required: true,
                expires_at: None,
                headers,
                redirected: false,
            });
        }

        Err(self.response_error(status, &url, response).await)
    }

    async fn head_info_via_range(
        &self,
        repo: &RepoRef,
        path: &str,
        revision: &str,
    ) -> Result<HeadInfo> {
        let url = self.resolve_url(repo, path, revision);
        let response = self
            .resolve_client
            .get(&url)
            .headers(self.auth_headers())
            .header("range", "bytes=0-0")
            .header("accept-encoding", "identity")
            .timeout(Duration::from_secs(90))
            .send()
            .await?;

        let status = response.status();
        let headers = response.headers().clone();
        log_rate_limit(&headers);

        if status.is_redirection() {
            let location = headers
                .get("location")
                .and_then(|value| value.to_str().ok())
                .ok_or_else(|| Error::Status {
                    status: status.as_u16(),
                    url: url.clone(),
                    body: "redirect without a Location header".to_string(),
                })?;
            let location = Url::parse(&url)?.join(location)?.to_string();
            return Ok(HeadInfo {
                auth_required: same_host(&location, &self.config.endpoint),
                expires_at: url_expiry(&location),
                location,
                headers,
                redirected: true,
            });
        }

        if status == StatusCode::PARTIAL_CONTENT || status.is_success() {
            return Ok(HeadInfo {
                location: url,
                auth_required: true,
                expires_at: None,
                headers,
                redirected: false,
            });
        }

        Err(self.response_error(status, &url, response).await)
    }

    async fn probe_size(&self, location: &str, auth_required: bool) -> Result<Option<u64>> {
        let mut request = self
            .resolve_client
            .get(location)
            .header("range", "bytes=0-0")
            .header("accept-encoding", "identity")
            .timeout(Duration::from_secs(90));
        if auth_required && let Some(token) = &self.token {
            request = request.bearer_auth(token);
        }
        let response = request.send().await?;
        let status = response.status();
        let headers = response.headers();

        if status == StatusCode::PARTIAL_CONTENT
            && let Some(value) = header_value(headers, "content-range")
            && let Some(total) = value.rsplit('/').next().and_then(|v| v.parse().ok())
        {
            return Ok(Some(total));
        }
        if status.is_success() {
            return Ok(header_u64(headers, "content-length"));
        }
        Err(self.response_error(status, location, response).await)
    }

    pub async fn list_tree(&self, repo: &RepoRef, revision: &str) -> Result<Vec<TreeEntry>> {
        let mut next = Some(self.tree_url(repo, revision));
        let mut entries = Vec::new();
        let mut pages = 0usize;

        while let Some(url) = next.take() {
            pages += 1;
            if pages > 100 {
                return Err(Error::Invalid(
                    "repository tree pagination exceeded 100 pages".to_string(),
                ));
            }

            let response = self
                .api_client
                .get(&url)
                .headers(self.auth_headers())
                .timeout(Duration::from_secs(90))
                .send()
                .await?;
            let status = response.status();
            log_rate_limit(response.headers());

            if !status.is_success() {
                return Err(self.response_error(status, &url, response).await);
            }

            let link = response
                .headers()
                .get("link")
                .and_then(|value| value.to_str().ok())
                .map(str::to_string);
            let page: Vec<TreeEntry> = response.json().await?;
            entries.extend(page);
            next = link.and_then(|link| next_link(&link, &url));
        }

        entries.retain(|entry| entry.kind == "file");
        Ok(entries)
    }

    pub async fn resolve_commit(&self, repo: &RepoRef, revision: &str) -> Result<String> {
        if is_hex_len(revision, 40) {
            return Ok(revision.to_string());
        }

        let url = format!(
            "{}/api/{}/{}/revision/{}",
            self.config.endpoint,
            repo.repo_type.api_segment(),
            repo.repo_id,
            encode_revision(revision)
        );
        let response = self
            .api_client
            .get(&url)
            .headers(self.auth_headers())
            .timeout(Duration::from_secs(60))
            .send()
            .await?;
        let status = response.status();
        log_rate_limit(response.headers());
        if !status.is_success() {
            return Err(self.response_error(status, &url, response).await);
        }

        #[derive(Deserialize)]
        struct RevisionInfo {
            sha: Option<String>,
        }

        let info: RevisionInfo = response.json().await?;
        info.sha
            .filter(|sha| !sha.is_empty())
            .ok_or_else(|| Error::Invalid(format!("revision '{revision}' has no commit hash")))
    }

    pub async fn whoami(&self) -> Result<String> {
        let url = format!("{}/api/whoami-v2", self.config.endpoint);
        let response = self
            .api_client
            .get(&url)
            .headers(self.auth_headers())
            .timeout(Duration::from_secs(30))
            .send()
            .await?;
        let status = response.status();
        if !status.is_success() {
            return Err(self.response_error(status, &url, response).await);
        }
        #[derive(Deserialize)]
        struct WhoAmI {
            name: String,
        }
        let info: WhoAmI = response.json().await?;
        Ok(info.name)
    }

    pub fn resolve_url(&self, repo: &RepoRef, path: &str, revision: &str) -> String {
        format!(
            "{}/resolve/{}/{}",
            self.repo_url(repo),
            encode_revision(revision),
            encode_path(path)
        )
    }

    pub fn tree_url(&self, repo: &RepoRef, revision: &str) -> String {
        format!(
            "{}/api/{}/{}/tree/{}?recursive=true",
            self.config.endpoint,
            repo.repo_type.api_segment(),
            repo.repo_id,
            encode_revision(revision)
        )
    }

    fn repo_url(&self, repo: &RepoRef) -> String {
        let prefix = repo.repo_type.url_prefix();
        if prefix.is_empty() {
            format!("{}/{}", self.config.endpoint, repo.repo_id)
        } else {
            format!("{}/{}/{}", self.config.endpoint, prefix, repo.repo_id)
        }
    }

    fn auth_headers(&self) -> HeaderMap {
        let mut headers = HeaderMap::new();
        if let Some(token) = &self.token
            && let Ok(value) = HeaderValue::from_str(&format!("Bearer {token}"))
        {
            headers.insert(reqwest::header::AUTHORIZATION, value);
        }
        headers
    }

    async fn response_error(&self, status: StatusCode, url: &str, response: Response) -> Error {
        let retry_after = header_u64(response.headers(), "retry-after");
        let request_id = header_value(response.headers(), "x-request-id");
        let body = response
            .text()
            .await
            .unwrap_or_else(|_| "<unreadable response body>".to_string());
        let mut body = body.trim().to_string();
        if body.len() > 2000 {
            body.truncate(2000);
            body.push('…');
        }
        if let Some(request_id) = request_id {
            body = format!("{body} (request id: {request_id})");
        }

        match status.as_u16() {
            401 | 403 => Error::Auth {
                repo: url.to_string(),
                detail: body,
            },
            404 => Error::NotFound(format!("{url}: {body}")),
            429 => Error::RateLimited {
                retry_after: retry_after.unwrap_or(5),
            },
            _ => Error::Status {
                status: status.as_u16(),
                url: url.to_string(),
                body,
            },
        }
    }
}

struct HeadInfo {
    location: String,
    auth_required: bool,
    expires_at: Option<SystemTime>,
    headers: HeaderMap,
    redirected: bool,
}

fn encode_path(path: &str) -> String {
    path.split('/')
        .map(|segment| utf8_percent_encode(segment, PATH_ENCODE_SET).to_string())
        .collect::<Vec<_>>()
        .join("/")
}

fn encode_revision(revision: &str) -> String {
    if revision.starts_with("refs/pr/") || revision.starts_with("refs/convert/") {
        return encode_path(revision);
    }
    utf8_percent_encode(revision, REVISION_ENCODE_SET).to_string()
}

fn header_value(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn header_u64(headers: &HeaderMap, name: &str) -> Option<u64> {
    header_value(headers, name).and_then(|value| value.parse().ok())
}

fn clean_etag(value: &str) -> String {
    let value = value.trim();
    let value = value.strip_prefix("W/").unwrap_or(value);
    value.trim_matches('"').to_string()
}

fn is_hex_len(value: &str, len: usize) -> bool {
    value.len() == len && value.chars().all(|ch| ch.is_ascii_hexdigit())
}

fn same_host(url: &str, endpoint: &str) -> bool {
    let url_host = Url::parse(url)
        .ok()
        .and_then(|url| url.host_str().map(|host| host.to_ascii_lowercase()));
    let endpoint_host = Url::parse(endpoint)
        .ok()
        .and_then(|url| url.host_str().map(|host| host.to_ascii_lowercase()));
    matches!((url_host, endpoint_host), (Some(a), Some(b)) if a == b)
}

fn url_expiry(url: &str) -> Option<SystemTime> {
    let url = Url::parse(url).ok()?;
    for (key, value) in url.query_pairs() {
        if (key.eq_ignore_ascii_case("expires") || key.eq_ignore_ascii_case("exp"))
            && let Ok(seconds) = value.parse::<u64>()
        {
            return UNIX_EPOCH.checked_add(Duration::from_secs(seconds));
        }
    }
    None
}

fn log_rate_limit(headers: &HeaderMap) {
    if let Some(value) = headers
        .get("ratelimit")
        .and_then(|value| value.to_str().ok())
    {
        tracing::debug!(ratelimit = value, "hub rate limit");
    }
}

fn next_link(link: &str, base: &str) -> Option<String> {
    for part in link.split(',') {
        if !part.contains("rel=\"next\"") {
            continue;
        }
        let start = part.find('<')? + 1;
        let end = part[start..].find('>')? + start;
        let target = &part[start..end];
        return Url::parse(base)
            .ok()?
            .join(target)
            .ok()
            .map(|url| url.to_string());
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cleans_etags() {
        assert_eq!(clean_etag("\"abc\""), "abc");
        assert_eq!(clean_etag("W/\"abc\""), "abc");
    }

    #[test]
    fn parses_next_link() {
        let link = "<https://huggingface.co/api/models/x/y/tree/main?cursor=abc>; rel=\"next\"";
        assert_eq!(
            next_link(link, "https://huggingface.co").as_deref(),
            Some("https://huggingface.co/api/models/x/y/tree/main?cursor=abc")
        );
    }

    #[test]
    fn encodes_revisions() {
        assert_eq!(encode_revision("refs/pr/1"), "refs/pr/1");
        assert_eq!(encode_revision("feature/foo"), "feature%2Ffoo");
    }
}

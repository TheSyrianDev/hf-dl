use std::fmt;
use std::str::FromStr;

use percent_encoding::percent_decode_str;
use serde::{Deserialize, Serialize};
use url::Url;

use crate::error::{Error, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RepoType {
    Model,
    Dataset,
    Space,
}

impl RepoType {
    pub fn api_segment(self) -> &'static str {
        match self {
            RepoType::Model => "models",
            RepoType::Dataset => "datasets",
            RepoType::Space => "spaces",
        }
    }

    pub fn url_prefix(self) -> &'static str {
        match self {
            RepoType::Model => "",
            RepoType::Dataset => "datasets",
            RepoType::Space => "spaces",
        }
    }

    pub fn cache_prefix(self) -> &'static str {
        self.api_segment()
    }

    pub fn as_str(self) -> &'static str {
        match self {
            RepoType::Model => "model",
            RepoType::Dataset => "dataset",
            RepoType::Space => "space",
        }
    }
}

impl fmt::Display for RepoType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for RepoType {
    type Err = Error;

    fn from_str(value: &str) -> Result<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "model" | "models" => Ok(RepoType::Model),
            "dataset" | "datasets" => Ok(RepoType::Dataset),
            "space" | "spaces" => Ok(RepoType::Space),
            other => Err(Error::Invalid(format!(
                "invalid repository type '{other}' (expected model, dataset, or space)"
            ))),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HfUri {
    pub repo_type: Option<RepoType>,
    pub repo_id: String,
    pub revision: Option<String>,
    pub path: String,
}

impl HfUri {
    pub fn repo_path(&self) -> String {
        format!(
            "{}/{}",
            self.repo_type.unwrap_or(RepoType::Model).url_prefix(),
            self.repo_id
        )
        .trim_start_matches('/')
        .to_string()
    }
}

pub fn parse_target(input: &str, endpoint: &str) -> Result<HfUri> {
    let input = input.trim();
    if input.is_empty() {
        return Err(Error::Invalid("repository is empty".to_string()));
    }

    if let Some(rest) = input.strip_prefix("hf://") {
        parse_hf_uri(rest)
    } else if input.starts_with("http://") || input.starts_with("https://") {
        parse_web_url(input, endpoint)
    } else {
        parse_bare_repo(input)
    }
}

fn parse_bare_repo(input: &str) -> Result<HfUri> {
    if input.contains('@') || input.contains('?') || input.contains('#') {
        return Err(Error::Invalid(format!(
            "invalid repository '{input}'; use an hf:// URI for revisions and paths"
        )));
    }
    let segments: Vec<&str> = input.split('/').collect();
    if segments.is_empty() || segments.len() > 2 {
        return Err(Error::Invalid(format!(
            "invalid repository '{input}'; expected 'owner/name' (use an hf:// URI to select a file)"
        )));
    }
    for segment in &segments {
        validate_repo_segment(segment)?;
    }
    Ok(HfUri {
        repo_type: None,
        repo_id: segments.join("/"),
        revision: None,
        path: String::new(),
    })
}

fn parse_hf_uri(rest: &str) -> Result<HfUri> {
    if rest.is_empty() {
        return Err(Error::Invalid("hf:// URI is empty".to_string()));
    }

    let (before_revision, revision_and_path) = match rest.split_once('@') {
        Some((left, right)) => (left, Some(right)),
        None => (rest, None),
    };

    let mut segments: Vec<&str> = before_revision.split('/').collect();
    if segments.iter().any(|segment| segment.is_empty()) {
        return Err(Error::Invalid(format!(
            "invalid hf:// URI '{rest}': empty path segment"
        )));
    }

    let mut repo_type = None;
    if let Some(first) = segments.first()
        && let Some(kind) = type_prefix(first)
    {
        repo_type = Some(kind);
        segments.remove(0);
    }

    if segments.len() < 2 {
        return Err(Error::Invalid(format!(
            "invalid hf:// URI '{rest}': expected namespace/name"
        )));
    }

    let repo_id = format!("{}/{}", segments[0], segments[1]);
    validate_repo_segment(segments[0])?;
    validate_repo_segment(segments[1])?;

    let mut path = segments[2..].join("/");
    let mut revision = None;

    if let Some(rev_and_path) = revision_and_path {
        if rev_and_path.is_empty() {
            return Err(Error::Invalid(format!(
                "invalid hf:// URI '{rest}': empty revision"
            )));
        }
        let (rev_raw, extra_path) = split_revision(rev_and_path);
        if rev_raw.is_empty() {
            return Err(Error::Invalid(format!(
                "invalid hf:// URI '{rest}': empty revision"
            )));
        }
        revision = Some(decode_segment(rev_raw, "revision")?);
        if !extra_path.is_empty() {
            if !path.is_empty() {
                return Err(Error::Invalid(format!(
                    "invalid hf:// URI '{rest}': path cannot appear before '@' and after it"
                )));
            }
            path = extra_path.to_string();
        }
    }

    let path = normalize_path(&path)?;
    Ok(HfUri {
        repo_type,
        repo_id,
        revision,
        path,
    })
}

fn split_revision(value: &str) -> (&str, &str) {
    let special = value.starts_with("refs/pr/") || value.starts_with("refs/convert/");
    let segments_to_keep = if special { 3 } else { 1 };

    let mut seen = 0;
    for (index, ch) in value.char_indices() {
        if ch == '/' {
            seen += 1;
            if seen == segments_to_keep {
                return (&value[..index], &value[index + 1..]);
            }
        }
    }
    (value, "")
}

fn parse_web_url(input: &str, endpoint: &str) -> Result<HfUri> {
    let url = Url::parse(input)?;
    if url.scheme() != "http" && url.scheme() != "https" {
        return Err(Error::Invalid(format!(
            "unsupported URL scheme '{}'",
            url.scheme()
        )));
    }

    let host = url.host_str().unwrap_or_default();
    let endpoint_host = Url::parse(endpoint)
        .ok()
        .and_then(|url| url.host_str().map(str::to_string))
        .unwrap_or_else(|| "huggingface.co".to_string());
    let known_host = matches!(
        host,
        "huggingface.co" | "www.huggingface.co" | "hf.co" | "www.hf.co"
    ) || host.eq_ignore_ascii_case(&endpoint_host);

    if !known_host {
        return Err(Error::Invalid(format!(
            "unsupported Hugging Face host '{host}'"
        )));
    }

    let segments: Vec<String> = url
        .path_segments()
        .map(|segments| {
            segments
                .map(|segment| percent_decode_str(segment).decode_utf8_lossy().to_string())
                .filter(|segment| !segment.is_empty())
                .collect()
        })
        .unwrap_or_default();

    if segments.is_empty() {
        return Err(Error::Invalid(
            "URL does not point at a repository".to_string(),
        ));
    }

    let mut index = 0;
    let mut repo_type = None;
    if let Some(kind) = type_prefix(&segments[0]) {
        repo_type = Some(kind);
        index += 1;
    }

    if segments.len() < index + 2 {
        return Err(Error::Invalid(format!(
            "URL '{input}' does not contain a namespace/name repository"
        )));
    }

    let owner = &segments[index];
    let repo = &segments[index + 1];
    validate_repo_segment(owner)?;
    validate_repo_segment(repo)?;
    let repo_id = format!("{owner}/{repo}");
    index += 2;

    let mut revision = None;
    let mut path = String::new();

    if index < segments.len() {
        let action = segments[index].as_str();
        if matches!(action, "blob" | "tree" | "resolve" | "raw" | "blame") {
            index += 1;
            if index >= segments.len() {
                return Err(Error::Invalid(format!(
                    "URL '{input}' is missing a revision after '{action}'"
                )));
            }
            revision = Some(segments[index].clone());
            index += 1;
            if index < segments.len() {
                path = segments[index..].join("/");
            }
        } else if segments.len() == index {
            // Repository root.
        } else {
            return Err(Error::Invalid(format!(
                "unsupported Hugging Face URL route '{}'",
                segments[index]
            )));
        }
    }

    let path = normalize_path(&path)?;
    Ok(HfUri {
        repo_type,
        repo_id,
        revision,
        path,
    })
}

fn type_prefix(value: &str) -> Option<RepoType> {
    match value.to_ascii_lowercase().as_str() {
        "model" | "models" => Some(RepoType::Model),
        "dataset" | "datasets" => Some(RepoType::Dataset),
        "space" | "spaces" => Some(RepoType::Space),
        _ => None,
    }
}

fn decode_segment(value: &str, what: &str) -> Result<String> {
    let decoded = percent_decode_str(value)
        .decode_utf8()
        .map_err(|_| Error::Invalid(format!("{what} is not valid UTF-8")))?
        .to_string();
    if decoded.is_empty() || decoded.contains('\0') || decoded.contains('\\') {
        return Err(Error::Invalid(format!("invalid {what}")));
    }
    Ok(decoded)
}

fn normalize_path(path: &str) -> Result<String> {
    let path = path.trim_start_matches('/');
    if path.is_empty() {
        return Ok(String::new());
    }

    let mut normalized = Vec::new();
    for segment in path.split('/') {
        if segment.is_empty() {
            continue;
        }
        let segment = percent_decode_str(segment)
            .decode_utf8()
            .map_err(|_| Error::Invalid("path is not valid UTF-8".to_string()))?
            .to_string();
        if segment.is_empty() || segment == "." || segment == ".." || segment.contains('\0') {
            return Err(Error::Invalid(format!(
                "unsafe path segment '{segment}' in repository path"
            )));
        }
        // ':' makes a Windows drive or stream prefix ("C:x", "a:b"), which
        // escapes the cache folder when joined.
        if segment.contains(':') {
            return Err(Error::Invalid(format!(
                "':' is not allowed in repository path segments: '{segment}'"
            )));
        }
        if segment.contains('\\') || segment.contains('/') {
            return Err(Error::Invalid(format!(
                "path separators are not allowed in repository path segments: '{segment}'"
            )));
        }
        normalized.push(segment);
    }

    Ok(normalized.join("/"))
}

fn validate_repo_segment(segment: &str) -> Result<()> {
    if segment.is_empty()
        || segment == "."
        || segment == ".."
        || segment.contains('\0')
        || segment.contains('\\')
        || segment.contains('/')
    {
        return Err(Error::Invalid(format!(
            "invalid repository path segment '{segment}'"
        )));
    }
    Ok(())
}

pub fn validate_repo_path(path: &str) -> Result<()> {
    let normalized = normalize_path(path)?;
    if normalized != path {
        return Err(Error::Invalid(format!(
            "repository path '{path}' is not normalized"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_windows_prefixes_and_separators() {
        for bad in [
            "C:evil.bin",
            "a:b",
            "..\\x",
            "dir\\file",
            "dir/..",
            "./x",
            "a/./b",
        ] {
            assert!(validate_repo_path(bad).is_err(), "should reject {bad}");
        }
        assert!(validate_repo_path("sub/model.gguf").is_ok());
    }

    #[test]
    fn long_nested_path_is_accepted() {
        let deep = (0..40)
            .map(|i| format!("d{i:02}"))
            .collect::<Vec<_>>()
            .join("/");
        let path = format!("{deep}/{}.gguf", "x".repeat(200));
        assert!(validate_repo_path(&path).is_ok());
    }

    const EP: &str = "https://huggingface.co";

    #[test]
    fn parses_file_uri() {
        let uri = parse_target(
            "hf://IsValorum/Sage-A1.5-APEX-I-MiniPlus-V2.1-GGUF/Sage-A1.5.APEX-I-MiniPlus-V2.1.gguf",
            EP,
        )
        .unwrap();
        assert_eq!(uri.repo_id, "IsValorum/Sage-A1.5-APEX-I-MiniPlus-V2.1-GGUF");
        assert_eq!(uri.path, "Sage-A1.5.APEX-I-MiniPlus-V2.1.gguf");
        assert_eq!(uri.revision, None);
    }

    #[test]
    fn parses_dataset_uri_with_revision() {
        let uri = parse_target(
            "hf://datasets/HuggingFaceH4/ultrachat_200k@v1/data/train.jsonl",
            EP,
        )
        .unwrap();
        assert_eq!(uri.repo_type, Some(RepoType::Dataset));
        assert_eq!(uri.revision.as_deref(), Some("v1"));
        assert_eq!(uri.path, "data/train.jsonl");
    }

    #[test]
    fn parses_pull_request_ref() {
        let uri = parse_target("hf://org/model@refs/pr/12/file.bin", EP).unwrap();
        assert_eq!(uri.revision.as_deref(), Some("refs/pr/12"));
        assert_eq!(uri.path, "file.bin");
    }

    #[test]
    fn parses_encoded_slash_in_revision() {
        let uri = parse_target("hf://org/model@feature%2Ffoo/file.bin", EP).unwrap();
        assert_eq!(uri.revision.as_deref(), Some("feature/foo"));
        assert_eq!(uri.path, "file.bin");
    }

    #[test]
    fn parses_web_file_url() {
        let uri = parse_target(
            "https://huggingface.co/datasets/org/data/blob/main/train.csv",
            EP,
        )
        .unwrap();
        assert_eq!(uri.repo_type, Some(RepoType::Dataset));
        assert_eq!(uri.revision.as_deref(), Some("main"));
        assert_eq!(uri.path, "train.csv");
    }

    #[test]
    fn rejects_traversal() {
        assert!(parse_target("hf://org/model/../etc/passwd", EP).is_err());
        assert!(parse_target("hf://org/model/%2e%2e/secret", EP).is_err());
        assert!(parse_target("hf://org/model/%2e%2e%2fsecret", EP).is_err());
        assert!(parse_target("hf://org/model/a%2Fb", EP).is_err());
    }
}

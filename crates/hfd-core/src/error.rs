use std::path::PathBuf;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("download cancelled")]
    Cancelled,
    #[error("network error: {0}")]
    Http(#[from] reqwest::Error),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("invalid JSON: {0}")]
    Json(#[from] serde_json::Error),

    #[error("invalid URL: {0}")]
    Url(#[from] url::ParseError),

    #[error("{0}")]
    Invalid(String),

    #[error("Xet error: {0}")]
    Xet(String),

    #[error("Xet download made no progress for {0}s")]
    XetStalled(u64),

    #[error("authentication failed for {repo}: {detail}")]
    Auth { repo: String, detail: String },

    #[error("not found: {0}")]
    NotFound(String),

    #[error("HTTP {status} from {url}: {body}")]
    Status {
        status: u16,
        url: String,
        body: String,
    },

    #[error("rate limited by the Hub; retry after {retry_after}s")]
    RateLimited { retry_after: u64 },

    #[error("another process holds the download lock at {0}")]
    Locked(PathBuf),

    #[error("checksum mismatch for {path}: expected {expected}, got {actual}")]
    Checksum {
        path: String,
        expected: String,
        actual: String,
    },

    #[error("the signed download URL expired")]
    UrlExpired,

    #[error("the server ignored the Range request")]
    RangeUnsupported,

    #[error("transient HTTP {status}")]
    Retryable {
        status: u16,
        retry_after: Option<u64>,
    },

    #[error("not enough free disk space: need {need} bytes, have {available} bytes")]
    NoSpace { need: u64, available: u64 },

    #[error("operation interrupted")]
    Interrupted,
}

impl Error {
    pub fn is_retryable(&self) -> bool {
        match self {
            Error::Http(e) => e.is_timeout() || e.is_connect() || e.is_request(),
            Error::Retryable { .. } | Error::RateLimited { .. } => true,
            _ => false,
        }
    }

    pub fn retry_after(&self) -> Option<u64> {
        match self {
            Error::Retryable { retry_after, .. } => *retry_after,
            Error::RateLimited { retry_after } => Some(*retry_after),
            _ => None,
        }
    }
}

pub type Result<T> = std::result::Result<T, Error>;

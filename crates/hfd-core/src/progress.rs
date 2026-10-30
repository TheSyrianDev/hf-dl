use std::path::PathBuf;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub enum Event {
    FileStart {
        path: String,
        size: u64,
        destination: PathBuf,
        resumed_bytes: u64,
    },
    Progress {
        path: String,
        downloaded: u64,
        total: u64,
    },
    FileDone {
        path: String,
        local_path: PathBuf,
        size: u64,
        skipped: bool,
    },
    Message(String),
}

pub type EventSink = Arc<dyn Fn(Event) + Send + Sync>;

pub fn noop_sink() -> EventSink {
    Arc::new(|_| {})
}

use std::path::PathBuf;
use std::sync::Arc;

/// Download events. With serde this is a stable, tagged JSON shape, e.g.
/// `{"type":"progress","path":"m.gguf","downloaded":1,"total":2,"bytes_per_sec":3.0,"eta_secs":4}`.
/// Field names and `type` tags are kept stable for GUI consumers.
#[derive(Debug, Clone, serde::Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
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
        /// Average speed over the last few seconds, in bytes per second.
        bytes_per_sec: f64,
        /// Estimated seconds left; `None` until the speed is known.
        eta_secs: Option<u64>,
    },
    FileDone {
        path: String,
        local_path: PathBuf,
        size: u64,
        skipped: bool,
    },
    Message {
        text: String,
    },
}

impl Event {
    pub fn message(text: impl Into<String>) -> Self {
        Event::Message { text: text.into() }
    }
}

pub type EventSink = Arc<dyn Fn(Event) + Send + Sync>;

pub fn noop_sink() -> EventSink {
    Arc::new(|_| {})
}

/// Wraps a sink so `Progress` events carry speed and ETA, averaged over a
/// sliding window of about 5 seconds.
pub(crate) fn with_rate(inner: EventSink) -> EventSink {
    use std::collections::VecDeque;
    use std::sync::Mutex;
    use std::time::{Duration, Instant};

    const WINDOW: Duration = Duration::from_secs(5);
    let samples: Mutex<VecDeque<(Instant, u64)>> = Mutex::new(VecDeque::new());
    Arc::new(move |event| {
        let Event::Progress {
            path,
            downloaded,
            total,
            ..
        } = event
        else {
            return inner(event);
        };
        let now = Instant::now();
        let (bytes_per_sec, eta_secs) = {
            let mut samples = samples.lock().unwrap();
            if samples.back().is_some_and(|&(_, bytes)| downloaded < bytes) {
                samples.clear();
            }
            samples.push_back((now, downloaded));
            while samples.len() > 2 && now - samples[1].0 >= WINDOW {
                samples.pop_front();
            }
            let (first_time, first_bytes) = samples[0];
            let elapsed = (now - first_time).as_secs_f64();
            let rate = if elapsed >= 0.5 {
                (downloaded - first_bytes) as f64 / elapsed
            } else {
                0.0
            };
            let eta = (rate >= 1.0)
                .then(|| (total.saturating_sub(downloaded) as f64 / rate).ceil() as u64);
            (rate, eta)
        };
        inner(Event::Progress {
            path,
            downloaded,
            total,
            bytes_per_sec,
            eta_secs,
        })
    })
}

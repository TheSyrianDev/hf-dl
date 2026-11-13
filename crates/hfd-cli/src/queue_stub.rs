//! The background daemon uses Unix sockets, so it is not available on this
//! platform yet. Plain `hfd download` works everywhere.

use anyhow::{Result, bail};

use crate::{DownloadArgs, QueueArgs};

const MSG: &str = "the background daemon and queue are only available on Linux and macOS for now; run `hfd download` without --detach";

pub async fn enqueue_download(_args: &DownloadArgs) -> Result<()> {
    bail!(MSG)
}

pub async fn run_queue(_args: QueueArgs) -> Result<()> {
    bail!(MSG)
}

pub async fn daemon_command(_stop: bool) -> Result<()> {
    bail!(MSG)
}

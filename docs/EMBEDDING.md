# Embedding hfd-core

`hfd-core` is the download library behind `hfd`. It is licensed under Apache-2.0 so applications can bundle it; the `hfd` CLI stays under PolyForm Noncommercial.

A complete, runnable example is in [`crates/hfd-core/examples/embed.rs`](../crates/hfd-core/examples/embed.rs):

```sh
cargo run -p hfd-core --example embed -- Qwen/Qwen2.5-0.5B-Instruct-GGUF Q4_K_M
```

## Dependency

```toml
hfd-core = { git = "https://github.com/TheSyrianDev/hf-dl" }
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
```

All download calls are async and need a multi-threaded Tokio runtime.

## Flow

1. **Client.** `Config::from_env()` reads the standard Hugging Face variables (`HF_HOME`, `HF_HUB_CACHE`, `HF_ENDPOINT`, …). `auth::resolve_token(None, &config)` finds a token in `HF_TOKEN`, `HUGGING_FACE_HUB_TOKEN`, or the saved login. Never log the token; use `auth::mask_token` to display it. Then `HubClient::new(config, token)`.
2. **List files.** `hub.list_tree(&RepoRef::new(RepoType::Model, "org/name"), "main")` returns every file with `path` and `size`.
3. **Pick a quant.** `parse_quant(path)` returns `Quant { name, bits_per_weight }` (for example `Q4_K_M`, `IQ3_S`, `BF16`), or `None`. `parse_shard(path)` returns `(part, total)` for split files like `-00002-of-00005.gguf`. Download every shard of a split model.
4. **Download.** `Downloader::new(hub, DownloadOptions { .. })`, then `download_file_cancellable(&repo, path, "main", &sink, &cancel)`. This returns `DownloadedFile { local_path, size, skipped, resumed_bytes, .. }`.

## Options (`DownloadOptions`)

| Field | Default | Meaning |
|---|---|---|
| `cache_dir` | HF hub cache | Where the standard `models--org--name` layout goes. It is shared with the `hf` CLI. |
| `local_dir` | `None` | Set this to write plain files into a folder instead, such as `~/AI/models/<name>/`. |
| `engine` | `Http` | `Http`, `Xet` or `Auto`. HTTP is the default until Xet stalls on slow links are fixed. |
| `connections` | 8 | The maximum number of parallel connections per file. It starts at 4 and adds more while speed improves. |
| `max_speed` | `None` | A speed cap in bytes per second, shared by all files. |
| `verify` | `Auto` | SHA-256 checking: `Auto`, `Always` or `Never`. |
| `force` | `false` | Download again even if the file is already complete. |
| `download_log` | `None` | Path of a `downloads.json` to append each completed download to (repo, file, quant, size_bytes, sha256, path, revision, downloaded_at). The CLI fills it from `HFD_DOWNLOAD_LOG`. |

### Saving into a real folder (`local_dir`)

Set `local_dir: Some(PathBuf::from("/home/me/AI/Models/qwen3-4b"))` to get plain files in that folder (repo subpaths are kept, e.g. `split_files/vae/ae.safetensors`). Partial-download state lives in `<local_dir>/.cache/huggingface/download/`, so pause and resume work the same as in the cache. `DownloadedFile.local_path` is the final file path. Leave it `None` to use the shared HF cache.

## Progress (`EventSink`)

`EventSink` is an `Arc<dyn Fn(Event) + Send + Sync>`. It is called from download tasks, so keep it fast; to update a UI, send to a channel. The events are:

- `FileStart { path, size, destination, resumed_bytes }`
- `Progress { path, downloaded, total, bytes_per_sec, eta_secs }`. This arrives about every 200 ms. Speed is averaged over about 5 seconds. `eta_secs` is `None` until the speed is known.
- `FileDone { path, local_path, size, skipped }`
- `Message { text }`: human-readable notices, such as an engine switch.

`Event` implements `serde::Serialize` with a `type` tag, so a GUI can forward it as JSON without its own mapping:

```json
{"type":"progress","path":"m-Q4_K_M.gguf","downloaded":1048576,"total":2497281120,"bytes_per_sec":312000.0,"eta_secs":8000}
```

The tags are `file_start`, `progress`, `file_done` and `message`. These names and fields are stable.

## Pause, cancel and resume

Create a `CancelToken` and call `token.cancel()` from anywhere. The download returns `Err(Error::Cancelled)`. Finished pieces stay on disk next to a `.hfd.state` file, so calling `download_file_cancellable` again for the same file resumes it. Pause is cancel followed by a later download.

Resume works in whole pieces of 8–64 MB, depending on file size. A piece that was only partly written when you cancelled is downloaded again.

## Download queue (`queue`)

`Queue` runs download jobs, one at a time by default. It saves its state to a JSON file, so a restart picks up unfinished jobs. Pause keeps partial files. Cancel removes partial files only when you pass `delete_partial = true` and the job has a `local_dir`. Finished files are never deleted.

```rust
use hfd_core::queue::{JobSpec, Queue, QueueEvent, Status};
use hfd_core::{Config, DownloadOptions, RepoType};

let queue = Queue::open(
    "/path/to/queue.json",
    Config::from_env(),
    None,                      // or Some(token)
    DownloadOptions::default(),
)?;
queue.set_max_parallel(1);     // default; raise for more jobs at once

let mut events = queue.subscribe();
tokio::spawn({
    let queue = queue.clone();
    async move { queue.run().await }   // runs until the process stops
});

let id = queue.add(JobSpec {
    repo_type: RepoType::Model,
    repo_id: "bartowski/SmolLM2-135M-Instruct-GGUF".into(),
    revision: "main".into(),
    paths: vec!["SmolLM2-135M-Instruct-Q4_K_M.gguf".into()],
    local_dir: Some("/models/smollm2".into()),
})?;

while let Ok(event) = events.recv().await {
    match event {
        QueueEvent::Progress { bytes, total, bytes_per_sec, eta_secs, .. } => { /* update UI */ }
        QueueEvent::Finished { id, sha_ok } => println!("{id} done, sha ok: {sha_ok}"),
        QueueEvent::Failed { id, reason, retryable } => eprintln!("{id}: {reason} (retry: {retryable})"),
        _ => {}
    }
}

queue.pause(&id)?;                       // keeps partial files
queue.resume(&id)?;
queue.cancel(&id, true)?;                // removes this job's partial files
let jobs = queue.list();
assert!(matches!(queue.get(&id).map(|j| j.status), Some(Status::Cancelled)));
```

`run()` owns the jobs it starts. Dropping its future (for example with `JoinHandle::abort`) stops any running download at once. Its partial files stay on disk, and the job comes back as `Queued` when the queue is reopened. Dropping the `Queue` alone does not stop jobs, since `run()` still holds them.

Events are JSON-tagged with `type`: `queued`, `started`, `progress` (`id`, `path`, `bytes`, `total`, `bytes_per_sec`, `eta_secs`), `paused`, `cancelled` (`deleted_partial`), `finished` (`sha_ok`), and `failed` (`reason`, `retryable`). `resume` only works on `Paused` jobs. A `Failed` job is final: add a new job to retry it. A job that was `Running` when the process stopped comes back as `Queued`, and its partial files let the download continue. `finished.sha_ok` is `true` for every job that completes, because each file is checked before it counts as done.

## Errors

`hfd_core::Error` variants a UI should handle:

- `Cancelled`: you cancelled it.
- `Auth { repo, detail }`: private or gated repo; the token's account must accept the repo's terms on the website.
- `NotFound(_)`: wrong repo or file name.
- `NoSpace { need, available }`: not enough disk space (checked before downloading).
- `RateLimited { retry_after }`: the Hub asked to slow down.
- `Checksum { .. }`: the file didn't match its SHA-256 and was not kept.
- Network-level: `Http`, `Retryable`, `Status`, `UrlExpired`, `Interrupted`. Retrying later resumes.
- `Xet` / `XetStalled`: only with the `Xet` or `Auto` engine; `Auto` switches to HTTP by itself.
- Others (`Io`, `Json`, `Url`, `Invalid`, `Locked`, `RangeUnsupported`) are rare; show the message.

## Models page API (`catalog`)

Everything a model picker needs, without the CLI:

- **Search:** `hub.search_models(&SearchQuery { text, gguf_only: true, sort: SearchSort::Downloads, limit: 20 })` returns `Vec<RepoSummary { id, downloads, likes, last_modified, gated, pipeline_tag, tags }>`. Sorts: `Downloads`, `Likes`, `Trending`, `Updated`. `gated.is_gated()` tells you a token and accepted terms are needed.
- **Quants of a repo:** `hub.repo_quants(&repo, "main")` returns `RepoQuants { options, mmproj }`. Each `QuantOption { quant, bits_per_weight, name, files, size }` is one choice: a single file or a whole shard set merged (`files` lists every shard in order, `size` is the total). `mmproj` holds vision projector files a multimodal model needs alongside its weights. Options are sorted smallest first. `group_quants(&entries)` does the same on a tree you already have.
- **Fits:** `MemoryBudget { free_ram_bytes, free_vram_bytes }.fit(option.size)` returns `Fit::Fits` (size ≤ free RAM), `Fit::Tight` (up to 10% over free RAM) or `Fit::TooBig`. VRAM is not counted. `fit_for(size, budget)` is the bare rule. Detecting free RAM is up to the app.
- **Download a choice:** call `download_file_cancellable` for each path in `option.files` (and the chosen `mmproj` file, if any).

Runnable example: `cargo run -p hfd-core --example catalog -- SmolVLM-256M 8` (search text, free RAM in GiB).

```rust
let hits = hub.search_models(&SearchQuery { text: "qwen3 4b".into(), gguf_only: true, ..Default::default() }).await?;
let repo = RepoRef::new(RepoType::Model, &hits[0].id);
let quants = hub.repo_quants(&repo, "main").await?;
let budget = MemoryBudget { free_ram_bytes: 16 << 30, free_vram_bytes: 0 };
let pick = quants.options.iter().rev().find(|o| budget.fit(o.size) == Fit::Fits).unwrap();
for path in &pick.files {
    downloader.download_file_cancellable(&repo, path, "main", &sink, &cancel).await?;
}
```

## Installed models (`record`)

The download log (`downloads.json`, set with `download_log`) is also the list of installed models:

- `record::list_installed(&log_path) -> Vec<InstalledModel>` returns every entry (repo, file, quant, size_bytes, sha256, path, revision, downloaded_at). A missing or unreadable log gives an empty list.
- `record::remove_installed(&log_path, &model_path, delete_file)` removes the entry for that absolute path and, when `delete_file` is true, deletes the file too. Returns `Ok(true)` if an entry was removed. The log is rewritten atomically.

Use `remove_installed` for "delete model" so the registry and the disk stay in step.

## Platforms

CI builds and tests on Linux, macOS and Windows. Real downloads have only been tested on Linux. On Windows, the cache uses hard links or copies when symlinks aren't allowed. The CLI's background daemon is Unix-only and is not part of the library.

## Onboarding recommendation: `recommend`

`hfd_core::recommend::recommend(probe_json, registry_json)` takes the output of
`hw-probe` and a models JSON file (for example one from your local model registry) and returns the chat, image and
video picks for this machine's tier, with a fit verdict where the registry gives
a size.

- Tier: matches `tier.tier` from the probe against the registry tier names; falls
  back to the hardware class (GPU with at least 4096 MiB VRAM, or CPU by RAM).
- Fit: `yes` if size <= free RAM, `tight` if up to 10% over, `no` beyond that.
  VRAM is not counted. A pick with no size in the registry has `fits: null`.
- Errors on malformed JSON or an empty tier list. It never downloads anything.

## Queue: duplicate requests

`Queue::add(spec)` is idempotent. A request with the same repo type, repo id,
revision, set of files (order ignored) and `local_dir` as an existing job returns
that job's id and creates nothing.

- If the existing job is `Done`, `add` still returns its id; check its status.
- `Queue::add_forced(spec)` queues a new run when the match is `Done`.
- A match that is `Failed` or `Cancelled` is reset to `Queued` under the same id
  (both `add` and `add_forced`). Partial files are kept, so the run resumes.
- Queued, running and paused matches return the existing id unchanged.
- A different `local_dir` or file set is a separate job.

## Pirate Face source (opt-in)

`hfd download <repo> --source pirateface --local-dir <dir> [files...]`

- Looks the repo up at `PF_API` (default `https://pirateface.co`), then transfers
  through `aria2c`. Seeding is disabled. Every file is checked against the sha256
  in the entry.
- Needs `aria2c` on PATH. Without it the command fails with "transfer unavailable";
  `hfd doctor` reports whether it is installed.
- `--source hf` (the default) never contacts Pirate Face.
- Refused, with no fallback to Hugging Face: repos not listed, Pirate Face-only
  entries (no HF commit), and a `--revision` that differs from the entry.
- Not yet supported: the HF cache layout (`--local-dir` required).

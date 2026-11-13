# Changelog

## 0.4.0-rc.1 (unreleased)

### Added
- `hfd download --source pirateface`: opt-in transfer through Pirate Face via `aria2c` (seeding off), every file verified by sha256. Needs `--local-dir`. Never falls back to Hugging Face. Default `--source hf` unchanged.
- `hfd doctor` reports whether `aria2c` is installed (informational).
- `hfd-core` `recommend()`: picks chat/image/video models for the probe's hardware tier and reports whether each fits free RAM.
- Queue: idempotent `add` (same repo, revision, files and local_dir return the existing id), `add_forced` for finished jobs, and failed or cancelled jobs requeue under the same id.

### Fixed
- Queue state is fsynced before rename; a failed add no longer leaves a phantom job.

## 0.3.0

### Added
- `hfd doctor`: checks network, token, Xet state and free disk, then recommends settings. `--json` for scripts. Exits 2 on a problem.
- `hfd lastengine`: downloads the Last Engine release asset and checks it against `SHA256SUMS.txt`. Discards the file on mismatch. Defaults to hw-probe-based variant selection.
- `hfd-core` `queue` module: `Queue` with add, pause, resume, cancel, list and subscribe. Persists to a JSON file so a restart resumes. Sequential by default. Cancel removes partials only with `delete_partial`.
- `hfd lastengine --variant auto` (default) picks the build from hw-probe. NVIDIA with at least 4 GB VRAM gets `cuda`; other hardware errors.
- Installed-model helpers in the core library: `record::list_installed` and `record::remove_installed` (see `docs/EMBEDDING.md`).

### Fixed
- `--verify always` on small git-stored files: checks the git blob SHA-1 from the ETag instead of failing with "no SHA256 checksum".
- Redirects from renamed repos are followed.
- `--flat` with `--local-dir` writes each file by basename.

## Unreleased

### Added
- `hfd queue pause <id>` and `hfd queue resume <id>`. A paused queued job is skipped by the worker. A paused running job is stopped, and its partial files stay on disk, so resume continues them. Paused jobs can still be cancelled.
- Status `paused` in `hfd queue list --json` and `hfd queue show <id> --json` for polling.

## 0.2.0

### Changed
- **HTTP is the default engine.** Xet is used only with `--engine auto` or `--engine xet`.
- **Xet stall fallback.** If Xet gets no data for 10 s, the download switches to HTTP. The host is then skipped for Xet for 24 h, so later downloads go straight to HTTP.
- **Fit rule matches the registry.** A model fits if its size is at most free RAM. It is "tight" if up to 10% over, and too big beyond that. VRAM is not counted.

### Added
- **sha256 in the download log.** LFS files record the Hub's sha256.
- **Models API in `hfd-core`.** `search_models`, `repo_quants` (shards merged, mmproj split out), `group_quants`, `MemoryBudget`/`fit_for`. Documented in `docs/EMBEDDING.md`.
- **Download log** (`HFD_DOWNLOAD_LOG` or `DownloadOptions.download_log`). Records each completed download in a JSON registry.
- **`--flat`** (requires `--local-dir`). Writes each file by basename, with no repo subfolders.
- **Queue `flat` field**, optional and default false, so older queue files still load.
- **Listing retries.** Hub listing requests retry up to 3 times on transport errors, with backoff.

### Fixed
- **Hub rename redirects are followed** on HEAD and size probes (HTTP 302 used to fail).
- Quant table now includes `UD-Q2_K_XL` and related unsloth dynamic types.
- Segment requests no longer use a total timeout that killed slow pieces.

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="assets/banner-dark.svg">
  <img src="assets/banner-light.svg" alt="hf-dl" width="100%">
</picture>

# hf-dl

A Hugging Face downloader with resumable, segmented downloads, written in Rust. It installs two binaries, `hfd` and `hf-dl`, which are the same program.

> [!NOTE]
> **Beta.** It works and has tests, but it has had little use outside the author's machine. Report problems on the issue tracker.

## Install

```sh
cargo install --path crates/hfd-cli
```

Needs Rust 1.88 or newer.

## Usage

```sh
hfd download <repo-id | hf:// URI | web URL> [paths...]
hfd download Qwen/Qwen3-0.6B --include "*.safetensors" --local-dir ./model
hfd download <repo> --dry-run          # resolve files and print the plan
```

Useful flags: `--repo-type`, `--revision`, `--include` / `--exclude` (globs, repeatable), `--cache-dir`, `--local-dir`, `--force`, `--verify`, `--json`, `--quiet`.

Downloads can run through a background daemon with a persistent queue:

```sh
hfd download <repo> --detach           # enqueue and return immediately
hfd queue list | show <id> | cancel <id> | clear
hfd daemon                             # run the daemon in the foreground
```

## Authentication

```sh
hfd auth login          # token, or the browser device flow when no token is given
hfd auth whoami
hfd auth list
hfd auth token          # prints the active token to stdout, like `hf auth token`
hfd auth logout
```

The token is read from `--token`, then `HF_TOKEN` / `HUGGING_FACE_HUB_TOKEN`, then stored tokens.

## License

Copyright (c) 2026 Omar Baroud.

Licensed under the [PolyForm Noncommercial License 1.0.0](LICENSE). Noncommercial use, modification and redistribution are permitted. Commercial use needs a separate written agreement; see [COMMERCIAL-LICENSING.md](COMMERCIAL-LICENSING.md).

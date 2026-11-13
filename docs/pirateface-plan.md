# Pirate Face backend for hfd (plan)

Status: research only. No code yet. Nothing here has touched the network except read-only
requests to pirateface.co and PyPI (metadata and the wheel, inspected offline).

## What Pirate Face is (from the 0.2.1 wheel and site)
- Torrent mirror of Hugging Face repos. Seeders publish a .torrent per repo. Files are checked
  against HF's own sha256 fingerprint, taken from the Pirate Face entry.
- Public API (default base `https://pirateface.co`, override `PF_API`):
  - `GET /api/torrents?repo=<id>` returns the entry (`repoId`, `revision`, `hf`, `files[{path,size,sha256,url}]`,
    `magnet`, `torrent` path) or 404 when not listed.
  - `GET <torrent path>` returns the .torrent bytes, used to read per-file offsets (`torrent.files_in`).
  - `GET /api/trackers` (needs a Bearer key) for the account's seeding tracker.
  - `GET /api/licenses`, `GET /api/cli` (minimum client version). Only used by the seeder side.
- Client flow in the wheel: look up entry → if `hf` is false, use "pf_only" mode (commit + files from the entry) →
  fetch torrent → download pieces → verify each file's sha256 → place in the HF cache layout.
  On `torrent.Unavailable` it falls back to HF.
- Env knobs in the wheel: `PF_CACHE` (~/.cache/pirateface), `PF_METADATA_TIMEOUT=30`, `PF_STALL_TIMEOUT=60`,
  `PF_ONLY_STALL_TIMEOUT=600`, `PF_LISTEN`.

## Decisions for hfd
1. **Optional backend, opt-in.** `--source pirateface` on `download`, or `HFD_SOURCE=pirateface`.
   Default stays Hugging Face. Setting `--source hf` (default) never contacts Pirate Face.
2. **Fallback only when asked.** If Pirate Face is selected and it has no usable entry or is unreachable,
   hfd fails with a clear message. It does NOT silently switch to HF or the reverse, so the user
   always knows where bytes came from. (Optional `--source pirateface,hf` for an ordered fallback later.)
3. **Verify against HF's fingerprint.** Every file must match the sha256 recorded in the entry. For
   LFS files this equals the sha256 HF publishes. For small git files, check the git blob sha1 (as in `--verify always`).
   Refuse to keep a file that fails verification and remove its partial.
4. **Torrent engine.** Do not bundle a torrent client in the first version. Options, decided later:
   (a) shell out to a user-installed client (e.g. aria2c with `--bt-*` options), or (b) a Rust crate.
   Either way the backend sits behind a trait so the queue and CLI don't change.
5. **Licence respect.** Pirate Face lists licences and some repos need terms accepted first
   (`lookup_magnet` returns None in that case). hfd surfaces that and does not bypass it.
6. **No seeding.** hfd only downloads. It does not upload pieces, does not expose a seeding tracker,
   and does not run the submit/rescue endpoints.
7. **Queue integration.** `JobSpec` gains `source: Hf | PirateFace` (default Hf, `#[serde(default)]`
   so old queue files still load, like the `flat` field).

## Test plan (no real network or torrents on the owner's machine)
- Unit: parse `/api/torrents` JSON (fixtures for hf:true, hf:false, 404, malformed); pick the right files;
  sha256 mismatch deletes the partial; `--source hf` never calls the PF URL (mock records requests).
- Integration: local mock HTTP server serving a fixture entry and a fixture .torrent built from small
  files. Torrent download itself is faked behind the backend trait in tests.
- Container only, offline: any test that needs a real torrent client runs inside a container with networking
  off and a loopback tracker. Nothing is installed on the owner's host.
- Explicitly out of scope on the host: real `pirateface.co` downloads, real swarm traffic, installing a torrent client.

## Open questions
- Which torrent engine (4a vs 4b) and its licence compatibility with hfd's Apache-2.0 core.
- Whether `pf_only` (no HF entry) should be allowed at all or limited to `--source pirateface --allow-hf-missing`.

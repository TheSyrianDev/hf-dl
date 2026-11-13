#!/usr/bin/env bash
# Builds the Linux release tarball: hfd binary, install script, licences, checksums.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
VERSION="$(sed -n 's/^version = "\(.*\)"/\1/p' "$ROOT/Cargo.toml" | head -1)"
OUT="$ROOT/dist/hfd-$VERSION-linux-x86_64"
cargo build --release --locked -p hfd-cli --manifest-path "$ROOT/Cargo.toml"
rm -rf "$OUT" && mkdir -p "$OUT"
install -m 755 "$ROOT/target/release/hfd" "$OUT/hfd"
install -m 755 "$ROOT/packaging/tarball/install.sh" "$OUT/install.sh"
install -m 644 "$ROOT/packaging/tarball/README.txt" "$OUT/README.txt"
install -m 644 "$ROOT/LICENSE" "$OUT/LICENSE"
install -m 644 "$ROOT/crates/hfd-core/LICENSE" "$OUT/LICENSE-hfd-core"
(cd "$OUT" && sha256sum hfd install.sh README.txt LICENSE LICENSE-hfd-core > SHA256SUMS.txt)
tar -C "$ROOT/dist" -czf "$OUT.tar.gz" "$(basename "$OUT")"
sha256sum "$OUT.tar.gz"

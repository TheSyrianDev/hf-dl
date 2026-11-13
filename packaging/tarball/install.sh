#!/usr/bin/env sh
# Installs hfd (and the hf-dl alias) from this release folder.
# Usage: ./install.sh [PREFIX]   (default PREFIX: /usr/local; use ~/.local for no root)
set -eu
PREFIX="${1:-/usr/local}"
HERE="$(cd "$(dirname "$0")" && pwd)"
BIN="$PREFIX/bin"
mkdir -p "$BIN"
install -m 755 "$HERE/hfd" "$BIN/hfd"
ln -sf hfd "$BIN/hf-dl"
"$BIN/hfd" --version
echo "Installed to $BIN (hfd, hf-dl). Make sure $BIN is on PATH."

#!/usr/bin/env bash
# Clean-container install test.
# Usage: tests/clean-install/run.sh <image> [release-tag]
# Starts a fresh container from <image>, runs install-inside.sh, and reports
# pass/fail. Models and binaries are never run from the host.
set -euo pipefail

IMAGE="${1:?usage: run.sh <image> [tag]}"
TAG="${2:-v0.2.0}"
HERE="$(cd "$(dirname "$0")" && pwd)"
RUNTIME="${CONTAINER_RUNTIME:-docker}"

command -v "$RUNTIME" >/dev/null || { echo "no $RUNTIME found" >&2; exit 2; }

"$RUNTIME" run --rm \
  -e HFD_TAG="$TAG" \
  -v "$HERE/install-inside.sh:/test/install-inside.sh:ro" \
  "$IMAGE" bash /test/install-inside.sh

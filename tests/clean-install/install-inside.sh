#!/usr/bin/env bash
# Runs inside a fresh container. Installs the hfd release binary from GitHub,
# verifies its checksum, and downloads a tiny GGUF model.
set -uo pipefail

TAG="${HFD_TAG:-v0.2.0}"
REPO="TheSyrianDev/hf-dl"
ASSET="hfd-${TAG#v}-x86_64-linux"
BASE="https://github.com/$REPO/releases/download/$TAG"
MODEL_REPO="bartowski/SmolLM2-135M-Instruct-GGUF"
MODEL_FILE="SmolLM2-135M-Instruct-Q4_K_M.gguf"
WORK=/tmp/clean-install
mkdir -p "$WORK"
FAILS=0
step() { echo; echo "== $*"; }
fail() { echo "FAIL: $*"; FAILS=$((FAILS + 1)); }

step "os"
. /etc/os-release && echo "$PRETTY_NAME"; ldd --version 2>&1 | head -1

step "base packages"
if command -v pacman >/dev/null; then
  pacman -Syu --noconfirm --needed curl ca-certificates >/tmp/pkg.log 2>&1 || { fail "pacman"; tail -20 /tmp/pkg.log; }
elif command -v apt-get >/dev/null; then
  (apt-get update && apt-get install -y curl ca-certificates) >/tmp/pkg.log 2>&1 || { fail "apt"; tail -20 /tmp/pkg.log; }
else
  fail "no supported package manager"
fi

step "download release binary and checksum"
curl -fsSL -o "$WORK/hfd" "$BASE/$ASSET" || fail "binary download"
curl -fsSL -o "$WORK/hfd.sha256" "$BASE/$ASSET.sha256" || fail "checksum download"
if [ -s "$WORK/hfd" ] && [ -s "$WORK/hfd.sha256" ]; then
  (cd "$WORK" && awk '{print $1"  hfd"}' hfd.sha256 | sha256sum -c -) || fail "sha256 mismatch"
fi
chmod +x "$WORK/hfd"

step "binary runs"
"$WORK/hfd" --version || fail "hfd --version"
ldd "$WORK/hfd" 2>&1 | grep -q 'not found' && { fail "missing shared libraries"; ldd "$WORK/hfd" | grep 'not found'; }

step "download tiny model"
"$WORK/hfd" download "$MODEL_REPO" "$MODEL_FILE" --local-dir "$WORK/model" --flat --verify always \
  || fail "model download"
SIZE=$(stat -c %s "$WORK/model/$MODEL_FILE" 2>/dev/null || echo 0)
echo "model bytes: $SIZE"
[ "$SIZE" -gt 100000000 ] || fail "model file too small or missing"

echo
if [ "$FAILS" -eq 0 ]; then echo "RESULT: PASS"; else echo "RESULT: FAIL ($FAILS)"; fi
exit "$FAILS"

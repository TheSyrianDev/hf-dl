#!/usr/bin/env bash
# Real full-disk test: mounts a 50 MB tmpfs (needs sudo), downloads a file
# larger than the mount, and checks for a clear error and no finished file.
# Usage: sudo-capable user runs tests/robustness/tmpfs_full_disk.sh [hfd-binary]
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
HFD="${1:-$HERE/../../target/release/hfd}"
MNT="$(mktemp -d /tmp/hfd-tmpfs.XXXX)"
cleanup() { sudo umount "$MNT" 2>/dev/null; rmdir "$MNT" 2>/dev/null; }
trap cleanup EXIT
sudo mount -t tmpfs -o size=50M tmpfs "$MNT" || { echo "FAIL mount"; exit 1; }
sudo chown "$(id -u):$(id -g)" "$MNT"
HFD_BIN="$(readlink -f "$HFD")" python3 - "$HERE" "$MNT" <<'PY'
import sys, subprocess, os, time
here, mnt = sys.argv[1], sys.argv[2]
sys.path.insert(0, here)
import run_robustness as r
import tempfile
root = r.new_root(tempfile.mkdtemp(prefix="hfd-hub-"), "tmpfs")  # hub data on the real disk
root["local"] = os.path.join(mnt, "local")  # download target on the 50 MB tmpfs
os.makedirs(root["local"])
r.SIZE = 80 * 1024 * 1024
want = r.make_data(root)
hub, port = r.start_hub(root)
try:
    res = subprocess.run(r.hfd_cmd(os.environ["HFD_BIN"], port, root["local"]),
                         env=r.hfd_env(root, port), capture_output=True, text=True, timeout=600)
    path = os.path.join(root["local"], r.FILE)
    out = res.stdout + res.stderr
    ok = res.returncode != 0 and "panick" not in out and not os.path.exists(path)
    print(("PASS" if ok else "FAIL"), "tmpfs full disk: rc=%d final_exists=%s" % (res.returncode, os.path.exists(path)))
    print(out[-300:])
    sys.exit(0 if ok else 1)
finally:
    hub.terminate(); hub.wait()
    import shutil; shutil.rmtree(os.path.dirname(root["base"]), ignore_errors=True)
PY

#!/usr/bin/env python3
"""Download robustness tests for hfd against a local flaky Hub.

Usage: tests/robustness/run_robustness.py [--hfd PATH] [--keep]
Needs only the stdlib. Each case gets its own scratch directory under a
temp root and never touches the real Hugging Face cache.
Exit code is the number of failed cases.
"""
import argparse
import glob
import hashlib
import os
import resource
import shutil
import signal
import subprocess
import sys
import tempfile
import time

HERE = os.path.dirname(os.path.abspath(__file__))
SIZE = 24 * 1024 * 1024
REPO = "test/robust"
FILE = "model.gguf"
results = []


def sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


def start_hub(root, **faults):
    cmd = [sys.executable, os.path.join(HERE, "flaky_hub.py"), "--dir", root["hub"]]
    for key, value in faults.items():
        if value is None:
            continue
        cmd += [f"--{key.replace('_', '-')}", str(value)]
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, text=True)
    line = proc.stdout.readline().strip()
    if not line.startswith("ready "):
        raise RuntimeError(f"hub failed to start: {line!r}")
    return proc, int(line.split()[1])


def hfd_cmd(hfd, port, local_dir, extra=()):
    return [hfd, "download", REPO, FILE, "--local-dir", local_dir,
            "--engine", "http", "--max-workers", "1", "--connections", "1",
            "--verify", "always", "--format", "human", *extra]


def hfd_env(root, port):
    env = dict(os.environ)
    env["HF_ENDPOINT"] = f"http://127.0.0.1:{port}"
    env["HF_HOME"] = os.path.join(root["base"], "hf-home")
    env["HF_HUB_CACHE"] = os.path.join(root["base"], "hub-cache")
    env["HFD_STATE_DIR"] = os.path.join(root["base"], "state")
    env["HOME"] = root["base"]
    return env


def new_root(tmp, name):
    base = os.path.join(tmp, name)
    hub = os.path.join(base, "hub")
    os.makedirs(hub)
    return {"base": base, "hub": hub, "local": os.path.join(base, "local")}


def make_data(root):
    path = os.path.join(root["hub"], FILE)
    with open(path, "wb") as f:
        f.write(os.urandom(SIZE))
    return sha256(path)


def partials(local):
    """Partial download files; hfd keeps them under hidden .cache dirs."""
    found = []
    for dirpath, _, names in os.walk(local):
        for n in names:
            if n.endswith(".incomplete"):
                found.append(os.path.join(dirpath, n))
    return found


def wait_for_partial(local, minimum, timeout=60):
    end = time.time() + timeout
    while time.time() < end:
        for p in partials(local):
            if os.path.getsize(p) >= minimum:
                return p
        time.sleep(0.2)
    raise RuntimeError("partial file never reached size")


def check(name, ok, detail=""):
    results.append((name, ok, detail))
    print(f"[{'PASS' if ok else 'FAIL'}] {name}" + (f" -- {detail}" if detail and not ok else ""))


def case_kill_resume(hfd, tmp):
    name = "kill mid-download, then resume"
    root = new_root(tmp, "kill")
    want = make_data(root)
    hub, port = start_hub(root, throttle=2 * 1024 * 1024)
    try:
        env = hfd_env(root, port)
        proc = subprocess.Popen(hfd_cmd(hfd, port, root["local"]), env=env,
                                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        wait_for_partial(root["local"], 4 * 1024 * 1024)
        proc.send_signal(signal.SIGKILL)
        proc.wait()
        if os.path.exists(os.path.join(root["local"], FILE)):
            check(name, False, "final file exists after SIGKILL")
            return
        before = sum(os.path.getsize(p) for p in partials(root["local"]))
        hub.terminate(); hub.wait()
        hub, port = start_hub(root)
        env = hfd_env(root, port)
        res = subprocess.run(hfd_cmd(hfd, port, root["local"]), env=env,
                             capture_output=True, text=True, timeout=300)
        got = sha256(os.path.join(root["local"], FILE)) if os.path.exists(os.path.join(root["local"], FILE)) else None
        ok = res.returncode == 0 and got == want and before > 0
        check(name, ok, f"rc={res.returncode} sha_match={got == want} partial_before={before} stderr={res.stderr[-300:]}")
    finally:
        hub.terminate(); hub.wait()


def case_network_drop(hfd, tmp):
    name = "network drop mid-file is retried to a correct file"
    root = new_root(tmp, "drop")
    want = make_data(root)
    hub, port = start_hub(root, drop_after=5 * 1024 * 1024, drop_count=2)
    try:
        res = subprocess.run(hfd_cmd(hfd, port, root["local"]), env=hfd_env(root, port),
                             capture_output=True, text=True, timeout=300)
        path = os.path.join(root["local"], FILE)
        got = sha256(path) if os.path.exists(path) else None
        check(name, res.returncode == 0 and got == want,
              f"rc={res.returncode} sha_match={got == want} stderr={res.stderr[-400:]}")
    finally:
        hub.terminate(); hub.wait()


def case_disk_full(hfd, tmp):
    name = "disk full gives a clear error and no finished file"
    root = new_root(tmp, "full")
    want = make_data(root)
    hub, port = start_hub(root)
    try:
        def limit():
            # A file-size limit makes write() fail with EFBIG, like a full disk.
            resource.setrlimit(resource.RLIMIT_FSIZE, (5 * 1024 * 1024, 5 * 1024 * 1024))
            signal.signal(signal.SIGXFSZ, signal.SIG_IGN)
        res = subprocess.run(hfd_cmd(hfd, port, root["local"]), env=hfd_env(root, port),
                             capture_output=True, text=True, timeout=300, preexec_fn=limit)
        path = os.path.join(root["local"], FILE)
        output = res.stdout + res.stderr
        clear = "panick" not in output and res.returncode not in (0,) and res.returncode >= 0
        no_final = not os.path.exists(path)
        check(name, clear and no_final,
              f"rc={res.returncode} final_exists={not no_final} tail={output[-400:]}")
        # Recovery: with space back, the same command must finish correctly.
        res2 = subprocess.run(hfd_cmd(hfd, port, root["local"]), env=hfd_env(root, port),
                              capture_output=True, text=True, timeout=300)
        got = sha256(path) if os.path.exists(path) else None
        check(name + " (recovery)", res2.returncode == 0 and got == want,
              f"rc={res2.returncode} sha_match={got == want}")
    finally:
        hub.terminate(); hub.wait()


def case_ctrl_c(hfd, tmp):
    name = "Ctrl-C leaves a clean, resumable state"
    root = new_root(tmp, "sigint")
    want = make_data(root)
    hub, port = start_hub(root, throttle=2 * 1024 * 1024)
    try:
        env = hfd_env(root, port)
        proc = subprocess.Popen(hfd_cmd(hfd, port, root["local"]), env=env,
                                stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True)
        wait_for_partial(root["local"], 4 * 1024 * 1024)
        proc.send_signal(signal.SIGINT)
        try:
            _, err = proc.communicate(timeout=30)
        except subprocess.TimeoutExpired:
            proc.kill(); proc.communicate()
            check(name, False, "hfd did not exit within 30 s of SIGINT")
            return
        path = os.path.join(root["local"], FILE)
        no_final = not os.path.exists(path)
        no_panic = "panicked" not in (err or "")
        check(name + " (exit)", no_final and no_panic and proc.returncode != 0,
              f"rc={proc.returncode} final_exists={not no_final} stderr={(err or '')[-300:]}")
        hub.terminate(); hub.wait()
        hub, port = start_hub(root)
        res = subprocess.run(hfd_cmd(hfd, port, root["local"]), env=hfd_env(root, port),
                             capture_output=True, text=True, timeout=300)
        got = sha256(path) if os.path.exists(path) else None
        check(name + " (resume)", res.returncode == 0 and got == want,
              f"rc={res.returncode} sha_match={got == want} stderr={res.stderr[-300:]}")
    finally:
        hub.terminate(); hub.wait()


def case_two_processes(hfd, tmp):
    name = "two hfd processes on the same file: second fails clearly"
    root = new_root(tmp, "two")
    want = make_data(root)
    hub, port = start_hub(root, throttle=2 * 1024 * 1024)
    try:
        env = hfd_env(root, port)
        first = subprocess.Popen(hfd_cmd(hfd, port, root["local"]), env=env,
                                 stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        wait_for_partial(root["local"], 2 * 1024 * 1024)
        second = subprocess.run(hfd_cmd(hfd, port, root["local"]), env=env,
                                capture_output=True, text=True, timeout=120)
        out = (second.stdout + second.stderr).lower()
        clear = second.returncode != 0 and ("lock" in out or "in use" in out or "another" in out)
        check(name + " (second)", clear, f"rc={second.returncode} out={out[-300:]}")
        first.wait(timeout=300)
        path = os.path.join(root["local"], FILE)
        got = sha256(path) if os.path.exists(path) else None
        check(name + " (first finishes)", first.returncode == 0 and got == want,
              f"rc={first.returncode} sha_match={got == want}")
    finally:
        hub.terminate(); hub.wait()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--hfd", default=os.path.join(HERE, "..", "..", "target", "release", "hfd"))
    ap.add_argument("--keep", action="store_true")
    ap.add_argument("--only", default="")
    args = ap.parse_args()
    hfd = os.path.abspath(args.hfd)
    tmp = tempfile.mkdtemp(prefix="hfd-robust-")
    cases = [case_kill_resume, case_network_drop, case_disk_full, case_ctrl_c, case_two_processes]
    try:
        for case in cases:
            if args.only and args.only not in case.__name__:
                continue
            try:
                case(hfd, tmp)
            except Exception as exc:  # report and keep going
                check(case.__name__, False, f"exception: {exc!r}")
    finally:
        if not args.keep:
            shutil.rmtree(tmp, ignore_errors=True)
        else:
            print("kept", tmp)
    failed = sum(1 for _, ok, _ in results if not ok)
    print(f"\n{len(results) - failed}/{len(results)} passed")
    return failed


if __name__ == "__main__":
    sys.exit(main())

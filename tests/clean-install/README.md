# Clean-container install test

Runs the hfd release binary in a fresh container, verifies its sha256, and downloads a 105 MB model (bartowski/SmolLM2-135M-Instruct-GGUF Q4_K_M).

    tests/clean-install/run.sh ubuntu:24.04
    tests/clean-install/run.sh archlinux:latest
    tests/clean-install/run.sh <image> v0.3.0   # another release tag

Env: `CONTAINER_RUNTIME=podman` to use podman instead of docker.

Results at v0.2.0: ubuntu:24.04 PASS (glibc 2.39), archlinux:latest PASS (glibc 2.44).
Finding: the Arch base image is stale. `pacman -Sy` alone breaks on the new openssl; the script uses `-Syu`.

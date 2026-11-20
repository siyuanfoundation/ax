# 2. Official AX Release Images (Pre-Configured)

The extension comes pre-configured with the official multi-arch (`linux/amd64` & `linux/arm64`), `@sha256` digest-pinned **AX `v0.4.0-rc.1`** images on GitHub Container Registry (`ghcr.io/google/ax`)—**no local Go toolchain, `Dockerfile`, or `localhost` container registry is needed**:

- **`ax-server`**: `ghcr.io/google/ax/ax-server@sha256:95737151ccc54fdb8d99a8a8674f11777d7b08b7108a7b80820a1c502bb71223`
- **`ax-task-runner`**: `ghcr.io/google/ax/ax-task-runner@sha256:3a61e70d70cede25cec04c34b2902e2febac08bd78741207101d5c102f6e7a2c` (`python:3.12-slim` + `git` + `curl` + `ax-task-runner`)
- **`ax-runner-volume`**: `ghcr.io/google/ax/ax-runner-volume@sha256:ac849f8357ac5b412d2d35d9076ef94b00557399d95c52c62353954e2101be07`

Click **Switch Release or Custom Images** only if you want to pin another `google/ax` GitHub release tag or point to a private container registry mirror.

# 3. Bootstrap Agent Substrate & AX Control Plane

AX runs sandboxed `Task` actors on **Agent Substrate** (`v1.0.0`) using gVisor isolation and `rustfs` workspace snapshots, pulling official `@sha256`-pinned `ghcr.io/google/ax` images directly.

- **Local Full Stack (`ax local start`)**: Creates the `ax-local` Kubernetes 1.36+ Kind cluster, bootstraps Agent Substrate (`ate-system` + `ate-workers` with 1, 4, or 10 gVisor workers), and deploys `ax-postgres` + `ax-server` (`ax-system`).
- **Existing Substrate Cluster (`ax install`)**: Deploys `ax-postgres` + `ax-server` onto your active Kubernetes context.

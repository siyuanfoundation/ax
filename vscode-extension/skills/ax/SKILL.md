---
name: ax
description: >
  Orchestrate isolated agent sandboxes, Workspaces, Gateways, and Models using the `ax` CLI (`ax.io/v1alpha1`) and VS Code AX tools.
  Use whenever the user asks to set up a local or remote AX sandbox cluster, author or validate AX YAML manifests, run or debug tasks via `ax ssh`, checkpoint with `suspend`/`resume`, or promote tasks across Kubernetes clusters.
---

# Using AX (`ax` CLI & Agent Sandboxes)

AX (`~/.ax/bin/ax`, automatically added to VS Code's `PATH` by the AX extension) runs isolated gVisor/microVM agent sandboxes on [Agent Substrate](https://github.com/agent-substrate/substrate) using four `ax.io/v1alpha1` primitives: **`Task`**, **`Workspace`**, **`Gateway`**, and **`Model`**.

For full annotated YAML templates of all four kinds, read [`references/manifests.md`](references/manifests.md).

---

## Critical Rules (Avoid Common Mistakes)

1. **Check environment status first**:
   - Run `ax local status -o json` and `ax ctx` (or invoke the `ax_status` VS Code tool) before applying manifests so you know which Kubernetes context is active and whether `ax-server` is reachable.
2. **Tasks start `Suspended` after `ax apply`**:
   - `ax apply -f task.yaml` creates the `Task` in `STATE: Suspended` without consuming a worker or running `spec.commands`.
   - Always run **`ax resume task <name>`** to start execution and wait for `Ready=True`.
   - Run **`ax suspend task <name>`** when idle or waiting for user review to snapshot `/workspace` and release the worker.
3. **Set `spec.debug: true` whenever you need `ax ssh`**:
   - Guest services (`ax ssh <task> -- <cmd>`) are disabled unless the `Task` has `spec.debug: true` and is in `STATE: Working`.
4. **Never use tag-only container images or `resources.requests`**:
   - Agent Substrate requires `spec.profile.image` to be `@sha256:...` digest-pinned (or **omit `spec.profile` entirely** to use `ax-server`'s default digest-pinned runner image).
   - Set only `spec.profile.resources.limits` (`cpu` and `memory`); `resources.requests` is rejected.
5. **No external network without a `Gateway`**:
   - A `Task` without `spec.gateway` has **zero external internet egress** — including during `Workspace.spec.setup` (`git` clones and setup `commands`). Bind a `Gateway` allowing `github.com` / model API hosts whenever network access is required.
6. **Ordering & Immutability**:
   - `metadata.name` and `metadata.atespace` must be lowercase RFC 1123 labels (`[a-z0-9-]`, max 63 chars).
   - `Workspace` specs are **immutable** once created (create `my-ws-v2` to change setup).
   - In multi-document YAML (`---`), list `Gateway` and `Workspace` **before** the `Task` that binds them.

---

## Core Workflows

### 1. Local Sandbox Setup (`ax local`) or Cluster Selection (`ax ctx`)

```bash
# Inspect container engine (Docker / Podman / OrbStack / Colima), kernel >= 6.8, RAM >= 4 GiB, and cluster health:
ax local status -o json

# Start or resume local Kind + Agent Substrate + AX stack:
ax local start \
  --server-image "$AX_SERVER_IMAGE" \
  --default-task-image "$AX_DEFAULT_TASK_IMAGE"

# List kubeconfig contexts & background tunnels, or switch to a remote GKE cluster:
ax ctx
ax ctx <kube-context>
```

- On macOS, if `ax local status -o json` reports `host.error` (no container runtime or VM RAM/kernel too low), guide the user to start OrbStack, Docker Desktop ($\ge 6\text{ GiB}$ RAM), or Colima (`colima start --cpu 4 --memory 8 --disk 60 --vm-type vz --mount-type virtiofs`).

### 2. Running & Inspecting a Sandboxed Task

```bash
# 1. Apply manifest(s)
ax apply -f task.yaml

# 2. Start the sandbox (waits for Workspace setup & Ready=True)
ax resume task my-task

# 3. Inspect status & events
ax get tasks
ax describe task my-task

# 4. Run non-interactive commands inside /workspace (requires spec.debug: true)
ax ssh my-task -- sh -c 'ls -la /workspace && git status'

# 5. Checkpoint /workspace and free worker capacity
ax suspend task my-task
```

### 3. Offloading Risky or Parallel Work from VS Code

When asked to test risky changes, run untrusted commands, or try multiple solutions in parallel:
1. Create one shared `Workspace` (or inline `Task`) and apply 1..N `Task`s (`spec.debug: true`, `commands: ["sleep", "infinity"]` for interactive driving, or an agent command).
2. Resume the task(s) (`ax resume task <name>`).
3. Drive builds/tests inside the isolated gVisor sandbox via `ax ssh <name> -- <cmd>` and retrieve the resulting patch (`ax ssh <name> -- git diff`).
4. Suspend (`ax suspend task <name>`) or clean up (`ax delete task <name>`).

### 4. Promoting a Local Task to a Remote Cluster (`ax promote`)

```bash
# 1. Dry-run preflight check first (never mutates target cluster)
ax promote task my-task --to <target-kube-context> --preflight-only

# 2. Promote task (+ bound Workspace/Gateway) and suspend the source copy so it never runs twice
ax promote task my-task --to <target-kube-context> --suspend-source
```

- Note: Kubernetes Secrets referenced by a `Gateway`'s `credentialUri` (`ate-secret://k8s.io/...`) are never copied across clusters; remind the user to create any named Secrets on the target cluster.

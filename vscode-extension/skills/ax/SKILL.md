---
name: ax
description: >-
  ALWAYS load and read this skill BEFORE running any `ax` CLI commands or writing AX manifests.
  Orchestrates isolated gVisor/microVM agent sandboxes, Workspaces, Gateways, and Models using the `ax` CLI (`ax.io/v1alpha1`) and VS Code AX tools.
  Use whenever the user mentions AX, `ax`, `ax-local`, Agent Substrate, or asks to set up a sandbox cluster, author or validate AX YAML manifests, fan out parallel sandboxed tasks (such as per-PR or per-package code reviews), monitor task completion, debug via `ax ssh`, checkpoint with `suspend`/`resume`, or promote tasks across clusters.
---

# Using AX (`ax` CLI & Agent Sandboxes)

AX (`~/.ax/bin/ax`, automatically added to VS Code's `PATH` by the AX extension) runs isolated gVisor/microVM agent sandboxes on [Agent Substrate](https://github.com/agent-substrate/substrate) using four `ax.io/v1alpha1` primitives: **`Task`**, **`Workspace`**, **`Gateway`**, and **`Model`**.

For full annotated YAML templates (including multi-task parallel fan-out), read [`references/manifests.md`](references/manifests.md).

---

## Critical Rules (Avoid Common Mistakes)

1. **Check environment status first**:
   - Run `ax local status -o json` and `ax ctx` (or invoke the `ax_status` VS Code tool) before applying manifests so you know which Kubernetes context is active and whether `ax-server` is reachable.
2. **Tasks start `Suspended` after `ax apply` — and one-shot commands auto-complete**:
   - `ax apply -f manifest.yaml` creates every `Task` in `STATE: Suspended` without consuming a worker or running `spec.commands`.
   - Run **`ax resume task <name>`** to start execution (`ax resume` blocks until sandbox & workspace report `Ready=True`, then returns while `spec.commands` continues running asynchronously).
   - **Interactive tasks** (`commands: ["sleep", "infinity"]` + `debug: true`): stay `Working` so you can run `ax ssh <name> -- <cmd>`, and require `ax suspend task <name>` when done.
   - **Run-to-completion tasks** (`commands: ["python3", "/workspace/run.py"]`): write final Markdown/text output to **`/workspace/.ax/output`** (`$AX_OUTPUT_FILE`). When the command exits `0`, AX automatically transitions the `Task` to terminal **`STATE: Completed`**, stores `/workspace/.ax/output` in `status.outcome.output` (read via `ax describe task <name>` or `ax get task <name>`), and **automatically releases the worker**! Do **not** call `ax suspend` or `ax ssh` on `Completed` tasks.
3. **`ax get` and `ax describe` do NOT support `-o json` or `-o yaml`!**:
   - **ONLY `ax local status -o json` accepts `-o json`**.
   - **NEVER run `ax get tasks -o json` or `ax get task <name> -o yaml`** — `ax get` treats `-o` as a positional `<name>` argument and fails with `Error: getting task "-o": ... task "-o" not found`!
   - Exact `ax` CLI output formats:
     - `ax get tasks` *(no `-o` flag)* -> plain-text whitespace table (`NAME  ATESPACE  STATE  ACTOR  AGE`). Parse lines after header `NAME` with `cols = line.split()` (`cols[0]` = task name, `cols[2]` = `STATE`: `Suspended`, `Working`, `Completed`, or `Failed`).
     - `ax get task <name>` *(no `-o` flag)* -> prints that single task's full live **YAML** directly to stdout (including `status.outcome.output`).
     - `ax describe task <name>` *(no `-o` flag)* -> prints human-readable fields (`State:`, `Outcome:`, and `Output:` indented by 2 spaces).
     - *(In VS Code LM-tool hosts, you can also call `ax_list_resources` with `{"kind": "tasks"}` to get JSON directly.)*
4. **Never use tag-only container images or `resources.requests`**:
   - Agent Substrate requires `spec.profile.image` to be `@sha256:...` digest-pinned (or **omit `image`** to use `ax-server`'s default digest-pinned `ax-task-runner` image, which includes `python3`, `git`, `curl`, and `bash`).
   - Set only `spec.profile.resources.limits` (`cpu` and `memory`); `resources.requests` is rejected.
5. **No external network without a `Gateway` — including `Workspace.spec.setup.gateway`**:
   - Sandboxes have **zero external internet egress** without a `Gateway` — including during `Workspace` pre-warming (`spec.setup.git` clones and `spec.setup.commands`)!
   - Whenever `Workspace.spec.setup` accesses the network (such as `git` cloning from GitHub), you **must** bind **`spec.setup.gateway: { name: <gw> }`** on the `Workspace` AND bind `spec.gateway: { name: <gw> }` on each `Task` (listing `Gateway` before `Workspace` before `Task`).
6. **Zero-Trust Egress Credential Injection & SSL CA Bundle**:
   - Never put real API keys in manifests. Automatically discover available labeled credential Secrets and their data key names via:
     `kubectl -n ax-system get secrets -l ax.io/credential=true -o go-template='{{range .items}}{{.metadata.name}}: {{range $k, $_ := .data}}{{$k}} {{end}}{{"\n"}}{{end}}'`
     (inspect only key names—never decode secret values).
   - Inject credentials in `Gateway.spec.egress[].https.effects.replaceHeaders` (e.g. `header: x-goog-api-key`, `credentialUri: "ate-secret://k8s.io/default/ax-system/<secret-name>/<key-name>"`), and pass `GEMINI_API_KEY: "placeholder"` in `Task.spec.env`.
   - Inside the sandbox, `atenet-egress` intercepts HTTPS using a cluster CA mounted at `$SSL_CERT_FILE` (`/run/ate/egress-trust-bundle/trust-bundle.pem`). In Python `urllib.request`, always pass `context=ssl.create_default_context(cafile=os.environ.get("SSL_CERT_FILE"))`.
   - When calling `gemini-3-flash-preview` (`v1beta/models/gemini-3-flash-preview:generateContent`), pass `"thinkingConfig": {"thinkingLevel": "LOW"}` + `"maxOutputTokens": 3072` and filter out thought parts (`"".join(p.get("text", "") for p in parts if not p.get("thought"))`).
7. **Warm Snapshot Profile Matching (Crucial for Fast N-Task Fan-Out!)**:
   - `Task.spec.profile` uses whole-profile precedence (never field-merged with `Workspace.spec.profile`).
   - Set `spec.profile` (`resources.limits`) on the shared **`Workspace`**, and **omit `Task.spec.profile`** on all fan-out `Task`s! Every `Task` then inherits the `Workspace` profile and boots in ~1s directly from the pre-warmed snapshot without re-cloning git!
8. **Naming & Immutability**:
   - `metadata.name` and `metadata.atespace` must be lowercase RFC 1123 labels (`[a-z0-9-]`, max 63 chars).
   - `Workspace` and `Task` specs are **immutable** once created.

---

## Core Workflows

### 1. Parallel Multi-Task Fan-Out (e.g. Reviewing N Pull Requests or Packages in Separate Sandboxes)

When the user gives a high-level goal like *"Review the 10 most recent PRs in `<repo>` using AX sandboxes"*, plan the entire AX workflow autonomously—**do NOT ask the user how to configure `Gateway`/`Workspace`/`Task` and do NOT run all N items inside a single `Task`**. Act as the local coordinator and fan out **N separate AX `Task`s** sharing **1 `Gateway`** and **1 pre-warmed `Workspace`** (read [`references/manifests.md`](references/manifests.md) first for the complete ready-to-adapt YAML template):

1. **Inspect cluster & fetch the target list locally**:
   - Run `ax local status -o json`, `ax get tasks` (no `-o` flag!), and check labeled credential Secrets in `ax-system` (Rule 6).
   - Fetch the N items locally (e.g. `https://api.github.com/repos/<owner>/<repo>/pulls?state=all&sort=created&direction=desc&per_page=10`) to get PR numbers, titles, authors, states, and URLs.
2. **Write & apply 1 `Gateway` + 1 shared `Workspace` + N `Task`s in `./<name>.ax.yaml`** (saving `*.ax.yaml` in the workspace gives live VS Code schema validation):
   - `Gateway` allows `["github.com", "api.github.com", "*.githubusercontent.com"]` (fetch PR diffs via `https://patch-diff.githubusercontent.com/raw/<owner>/<repo>/pull/<num>.diff` to avoid GitHub REST API rate limits!) and `["generativelanguage.googleapis.com"]` with `replaceHeaders` (`x-goog-api-key` -> `ate-secret://k8s.io/default/ax-system/<secret-name>/<key-name>` discovered in Step 1).
   - `Workspace` sets `spec.profile.resources.limits` (`cpu: "1"`, `memory: "1Gi"`), `spec.setup.gateway`, `spec.setup.git` (`depth: 1`), and `spec.setup.files` containing `/workspace/AGENTS.md` + `/workspace/review_pr.py` (following [`references/manifests.md`](references/manifests.md) so each task writes its Markdown review to `/workspace/.ax/output`).
   - Each of the N `Task`s (`etcd-pr-<num>`) binds the shared `Workspace` and `Gateway`, **omits `spec.profile`**, passes `PR_NUMBER` / `PR_TITLE` + `GEMINI_API_KEY: "placeholder"`, and runs `commands: ["python3", "/workspace/review_pr.py"]`.
3. **Resume all N `Task`s concurrently**:
   ```bash
   for t in etcd-pr-1 etcd-pr-2 ...; do
     ax resume task "$t" &
   done
   wait
   ```
4. **Poll `ax get tasks` (plain table, NO `-o json`!) until all N tasks reach `Completed`**, then collect outputs and synthesize the report:
   ```python
   import subprocess, time, yaml

   target_tasks = ["etcd-pr-1", "etcd-pr-2"]  # your N task names
   while True:
       # IMPORTANT: `ax get tasks` does NOT support `-o json`; parse its whitespace table directly
       table = subprocess.check_output(["ax", "get", "tasks"], text=True)
       states = {
           cols[0]: cols[2]
           for line in table.splitlines()[1:]
           if len(cols := line.split()) >= 3 and cols[0] in target_tasks
       }
       done = sum(1 for s in states.values() if s in ("Completed", "Failed"))
       print(f"Progress: {done}/{len(target_tasks)} finished", flush=True)
       if len(states) == len(target_tasks) and done == len(target_tasks):
           break
       time.sleep(3)

   # Read each completed task's output from `ax get task <name>` (returns live YAML) or `ax describe task <name>`
   outputs = {}
   for t in target_tasks:
       doc = yaml.safe_load(subprocess.check_output(["ax", "get", "task", t], text=True))
       outputs[t] = (doc.get("status", {}).get("outcome", {}) or {}).get("output", "")
   ```

### 2. Interactive Sandbox Debugging & Promotion

```bash
# Inspect live tasks & status (plain table / YAML — never pass `-o json` to `ax get`)
ax get tasks
ax get task <name>
ax describe task <name>

# Run commands inside a Working task with spec.debug: true
ax ssh <name> -- sh -c 'ls -la /workspace && git status'

# Promote a task (+ bound Workspace/Gateway) to a remote cluster
ax promote task <name> --to <target-kube-context> --preflight-only
ax promote task <name> --to <target-kube-context> --suspend-source
```

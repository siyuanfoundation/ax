# AX — Agent Sandbox Orchestrator for VS Code

The official VS Code extension for **AX** (`ax.io/v1alpha1`). Installing this extension equips both you and your coding agent (GitHub Copilot, Claude Code, Gemini / Antigravity, Cursor, or Codex) with guided zero-to-sandbox setup and lifecycle management for isolated gVisor/microVM agent sandboxes on Agent Substrate—**with zero local Go or Docker image builds required**:

1. **Zero-Friction Released `ax` CLI & Agent Skill Distribution**:
   - Bundles the official SHA-256 verified `ax` release binary (`v0.4.0-rc.1`), installs it to `~/.ax/bin/ax` on startup, and prepends `~/.ax/bin` to VS Code's integrated terminal and agent subshell `PATH`.
   - Registers `./skills/ax/SKILL.md` natively via VS Code `contributes.chatSkills`, syncs `skills/ax/` (`SKILL.md` + `references/manifests.md`) to `~/.gemini/config/skills/ax` (Antigravity), `~/.gemini/skills/ax` (Gemini CLI), `~/.claude/skills/ax` (Claude Code), and `~/.agents/skills/ax`, and contributes 4 Language Model Tools (`ax_status`, `ax_list_resources`, `ax_manage_task`, `ax_ssh_exec`).
2. **Guided Setup Checklist & Official GHCR Release Images (`ax.guidedSetup`)**:
   - Pre-configured with the official multi-arch `@sha256`-pinned `v0.4.0-rc.1` release images on `ghcr.io/google/ax` (`ax-server`, `ax-task-runner`, and `ax-runner-volume`), with one-click switching to any `google/ax` GitHub release tag.
   - Inspects all environment layers (CLI & skills, container host preflight, release images, Kind + Agent Substrate v1.0.0 cluster with 1/4/10 gVisor workers, AX control-plane deployment, and `ax.io/credential=true` Kubernetes Secrets) and streams `ax local start` or `ax install` inside VS Code's Integrated Terminal.
3. **YAML Schema, Live Diagnostics & Templates**:
   - Validates `apiVersion: ax.io/v1alpha1` (`Task`, `Workspace`, `Gateway`, `Model`) including `Workspace.spec.profile` and `Workspace.spec.setup.gateway`, skips embedded block scalars cleanly, and warns when `spec.setup.git` omits `spec.setup.gateway`.
   - Includes one-click manifest templates for interactive sandboxes and automated GitHub PR review pipelines with zero-trust Gateway credential injection.
4. **AX Sandboxes & Cluster Explorer**:
   - Live-refreshing Sidebar TreeView + Status Bar for Tasks (`Suspended`, `Working`, `Completed`, `Failed`), Workspaces, Gateways, Models, Kubernetes contexts, and background tunnels, with one-click `Resume`, `Suspend`, `View Output`, `Promote`, `Describe`, and `ax ssh` terminals.

## Development & Packaging (No Local Go Build Needed)

```bash
cd vscode-extension
npm install

# Run unit tests (installer, CLI parsers, schema, manifest linter, release image defaults)
npm test

# Run full VS Code Extension Host E2E tests (downloads official v0.4.0-rc.1 ax binary automatically)
npm run test:e2e

# Package ax-vscode.vsix with official v0.4.0-rc.1 CLI & GHCR image defaults
npm run package
```

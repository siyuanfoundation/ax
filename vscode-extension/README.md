# AX — Agent Sandbox Orchestrator for VS Code

The official VS Code extension for **AX** (`ax.io/v1alpha1`). Installing this extension equips both you and your coding agent (GitHub Copilot, Claude Code, Gemini / Antigravity, Cursor, or Codex) with everything needed to run isolated gVisor/microVM agent sandboxes locally or on Kubernetes:

1. **Zero-Friction `ax` CLI Distribution**: Bundles the static `ax` binary, installs it to `~/.ax/bin/ax` on startup, and prepends `~/.ax/bin` to VS Code's integrated terminal and agent subshell `PATH`.
2. **Automatic Coding-Agent Skill Delivery**:
   - Registers `./skills/ax/SKILL.md` natively via VS Code `contributes.chatSkills`.
   - Syncs `skills/ax/` (`SKILL.md` + `references/manifests.md`) to `~/.agents/skills/ax`, `~/.claude/skills/ax`, and `~/.gemini/skills/ax`.
   - Contributes 4 VS Code Language Model Tools (`ax_status`, `ax_list_resources`, `ax_manage_task`, `ax_ssh_exec`) for direct agent tool-calling.
3. **YAML Schema & Live Manifest Diagnostics**: Validates `apiVersion: ax.io/v1alpha1` (`Task`, `Workspace`, `Gateway`, `Model`) for RFC 1123 lowercase naming, `@sha256:...` digest-pinned images, and `resources.limits` rules.
4. **AX Sandboxes & Cluster Explorer**: Sidebar TreeView + Status Bar showing local Kind + Agent Substrate + AX health (`ax local status -o json`), Kubernetes contexts (`ax ctx`), background tunnels, and one-click `Resume`, `Suspend`, `Promote`, `Describe`, and integrated `ax ssh` terminals.

## Development & Verification

```bash
cd vscode-extension
npm install

# Run unit tests (installer, CLI parsers, schema, manifest linter)
npm test

# Run full VS Code Extension Host E2E tests (uses live cluster if ../.tmp/kubeconfig-ax-e2e or $AX_E2E_KUBECONFIG is present)
npm run test:e2e

# Build `ax` binary and package `ax-vscode.vsix`
npm run package
```

To build a platform-specific `.vsix` (e.g. `linux-x64`, `linux-arm64`, `darwin-x64`, `darwin-arm64`):

```bash
VSCODE_TARGET=darwin-arm64 bash ./scripts/build-ax-bin.sh
npx @vscode/vsce package --target darwin-arm64 --no-git-tag-version -o ax-vscode-darwin-arm64.vsix
```

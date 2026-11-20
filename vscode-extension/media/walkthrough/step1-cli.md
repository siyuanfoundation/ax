# 1. Install `ax` CLI & Agent Skills

On startup, the **AX** extension automatically installs the bundled static `ax` CLI binary into `~/.ax/bin/ax`, prepends `~/.ax/bin` to VS Code's integrated terminal `PATH`, and syncs the `ax` coding-agent skill (`SKILL.md` + `references/manifests.md`) to:

- `~/.gemini/config/skills/ax` (**Antigravity** global skill discovery)
- `~/.gemini/skills/ax` (Gemini CLI)
- `~/.claude/skills/ax` (Claude Code)
- `~/.agents/skills/ax` (universal `.agents` hosts)

> **Tip**: Start a **new conversation (`+`)** after installing the extension so your agent indexes the newly synced `ax` skill.

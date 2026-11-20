# 4. Add Egress Credentials & Run Your First Sandboxed Task

Sandboxes have **zero external internet access** unless bound to a **`Gateway`**, and never hold raw API keys:

1. Click **Configure Egress Credential Secret** to store your `GEMINI_API_KEY` (or `ANTHROPIC_API_KEY`) in `ax-system` labeled `ax.io/credential=true`.
2. Click **New Task & Workspace Manifest** to scaffold a `Gateway` + pre-warmed `Workspace` + `Task` (including a ready-to-run **GitHub PR Review** template).
3. Right-click the YAML file -> **AX: Apply Manifest File**, then click **Resume Task** (`▶`) in the AX Sidebar and **View Task Output** once completed!

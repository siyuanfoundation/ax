// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

import { ChildProcess, spawn } from 'child_process';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import * as vscode from 'vscode';
import {
  AxCli,
  AxCliConfig,
  AxResourceKind,
  validateAxManifestText,
} from './axCli';
import { InstallResult, installAxCliAndSkills, resolveInstallDir } from './installer';
import {
  AxToolHandlers,
  createAxToolHandlers,
  registerAxLanguageModelTools,
} from './lmTools';
import {
  AxImageBundle,
  DEFAULT_AX_RELEASE_VERSION,
  DEFAULT_RELEASE_IMAGES,
  EnvironmentReadiness,
  applyCredentialSecret,
  fetchReleaseImagesFromGitHub,
  inspectEnvironmentReadiness,
  resolveEffectiveImages,
  resolveKubectlBinary,
} from './setupHelper';
import { AxExplorerProvider, AxTreeItem } from './treeProvider';

export interface ResourceRefArg {
  resourceKind: AxResourceKind;
  resourceName: string;
  atespace?: string;
}

export interface ConfigureSecretArg {
  secretName: string;
  secretKey: string;
  secretValue?: string;
  fromEnvVar?: string;
}

export interface LocalStartArg {
  serverImage?: string;
  defaultTaskImage?: string;
  runnerVolumeImage?: string;
  workers?: number;
  clusterName?: string;
}

export interface AxExtensionApi {
  getInstallResult(): InstallResult | null;
  getReadiness(): Promise<EnvironmentReadiness>;
  cli: AxCli;
  explorer: AxExplorerProvider;
  tools: AxToolHandlers;
  registeredLmToolCount: number;
  refreshAll(): Promise<void>;
}

class AxInspectContentProvider implements vscode.TextDocumentContentProvider {
  private readonly contents = new Map<string, string>();
  private readonly _onDidChange = new vscode.EventEmitter<vscode.Uri>();
  public readonly onDidChange = this._onDidChange.event;

  public set(uri: vscode.Uri, text: string): void {
    this.contents.set(uri.toString(), text);
    this._onDidChange.fire(uri);
  }

  public provideTextDocumentContent(uri: vscode.Uri): string {
    return this.contents.get(uri.toString()) ?? '';
  }
}

function buildStarterSandboxTemplate(): string {
  return [
    'apiVersion: ax.io/v1alpha1',
    'kind: Workspace',
    'metadata:',
    '  name: demo-workspace',
    'spec:',
    '  setup:',
    '    files:',
    '      - path: "AGENTS.md"',
    '        content: |',
    '          - Keep changes minimal and run tests.',
    '---',
    'apiVersion: ax.io/v1alpha1',
    'kind: Task',
    'metadata:',
    '  name: demo-task',
    'spec:',
    '  workspace:',
    '    name: demo-workspace',
    '  goal:',
    '    text: "Inspect /workspace and run checks."',
    '  commands: ["sleep", "infinity"]',
    '  debug: true',
    '',
  ].join('\n');
}

export function buildGitHubPrReviewTemplate(
  repoSlug: string = 'etcd-io/etcd',
  prCount: number = 10,
  secretName: string = 'gemini-api-creds'
): string {
  const safePrefix = repoSlug
    .toLowerCase()
    .replace(/[^a-z0-9-]+/g, '-')
    .replace(/^-+|-+$/g, '')
    .slice(0, 32) || 'repo';

  return [
    `# AX Sandboxed PR Review Pipeline for https://github.com/${repoSlug}`,
    `# 1. Gateway allows HTTPS only to GitHub & Gemini API and injects GEMINI_API_KEY outside the sandbox.`,
    `# 2. Workspace pre-warms a shallow clone of ${repoSlug} + review script into a reusable snapshot.`,
    `# 3. Task reviews the ${prCount} most recent PRs in an isolated gVisor sandbox and writes /workspace/.ax/output.`,
    'apiVersion: ax.io/v1alpha1',
    'kind: Gateway',
    'metadata:',
    `  name: ${safePrefix}-review-gw`,
    '  atespace: default',
    'spec:',
    '  egress:',
    '    - https:',
    '        hostnames: ["github.com", "api.github.com", "*.githubusercontent.com"]',
    '    - https:',
    '        hostnames: ["generativelanguage.googleapis.com"]',
    '        effects:',
    '          replaceHeaders:',
    '            - header: x-goog-api-key',
    `              credentialUri: "ate-secret://k8s.io/default/ax-system/${secretName}/api-key"`,
    '---',
    'apiVersion: ax.io/v1alpha1',
    'kind: Workspace',
    'metadata:',
    `  name: ${safePrefix}-review-ws`,
    '  atespace: default',
    'spec:',
    '  profile:',
    '    resources:',
    '      limits:',
    '        cpu: "2"',
    '        memory: "2Gi"',
    '  setup:',
    '    gateway:',
    `      name: ${safePrefix}-review-gw`,
    '    git:',
    '      - name: origin',
    `        repo: "https://github.com/${repoSlug}.git"`,
    '        ref: "main"',
    '        dir: "repo"',
    '        depth: 1',
    '    files:',
    '      - path: "AGENTS.md"',
    '        content: |',
    `          # ${repoSlug} Automated Code Review Guidelines`,
    '          - Focus on correctness, concurrency/distributed-systems safety, resource bounds, API compatibility, and test coverage.',
    '          - Cite concrete files, functions, and actionable recommendations.',
    '      - path: "review_recent_prs.py"',
    '        content: |',
    '          import concurrent.futures, json, os, ssl, sys, urllib.request',
    '',
    `          REPO = os.environ.get("TARGET_REPO", "${repoSlug}")`,
    `          PR_COUNT = int(os.environ.get("PR_COUNT", "${prCount}"))`,
    '          MODEL = os.environ.get("GEMINI_MODEL", "gemini-3-flash-preview")',
    '          OUT_FILE = os.environ.get("AX_OUTPUT_FILE", "/workspace/.ax/output")',
    '          REVIEWS_DIR = "/workspace/reviews"',
    '          os.makedirs(REVIEWS_DIR, exist_ok=True)',
    '          os.makedirs(os.path.dirname(OUT_FILE), exist_ok=True)',
    '',
    '          ca_file = os.environ.get("SSL_CERT_FILE") or "/run/ate/egress-trust-bundle/trust-bundle.pem"',
    '          ssl_ctx = ssl.create_default_context(cafile=ca_file if os.path.exists(ca_file) else None)',
    '',
    '          def http_get_json(url, accept="application/vnd.github+json"):',
    '              req = urllib.request.Request(url, headers={"User-Agent": "ax-pr-reviewer/1.0", "Accept": accept})',
    '              with urllib.request.urlopen(req, context=ssl_ctx, timeout=45) as r:',
    '                  return json.loads(r.read().decode("utf-8", errors="replace"))',
    '',
    '          def fetch_pr_diff(num):',
    '              for url, accept in [',
    '                  (f"https://patch-diff.githubusercontent.com/raw/{REPO}/pull/{num}.diff", "text/plain"),',
    '                  (f"https://api.github.com/repos/{REPO}/pulls/{num}", "application/vnd.github.v3.diff"),',
    '              ]:',
    '                  try:',
    '                      req = urllib.request.Request(url, headers={"User-Agent": "ax-pr-reviewer/1.0", "Accept": accept})',
    '                      with urllib.request.urlopen(req, context=ssl_ctx, timeout=45) as r:',
    '                          return r.read().decode("utf-8", errors="replace")',
    '                  except Exception:',
    '                      continue',
    '              return "(diff unavailable)"',
    '',
    '          def call_gemini(prompt):',
    '              url = f"https://generativelanguage.googleapis.com/v1beta/models/{MODEL}:generateContent"',
    '              body = json.dumps({',
    '                  "contents": [{"parts": [{"text": prompt}]}],',
    '                  "generationConfig": {',
    '                      "temperature": 0.2,',
    '                      "maxOutputTokens": 3072,',
    '                      "thinkingConfig": {"thinkingLevel": "LOW"},',
    '                  },',
    '              }).encode("utf-8")',
    '              req = urllib.request.Request(',
    '                  url,',
    '                  data=body,',
    '                  method="POST",',
    '                  headers={',
    '                      "Content-Type": "application/json",',
    '                      "x-goog-api-key": os.environ.get("GEMINI_API_KEY", "placeholder"),',
    '                  },',
    '              )',
    '              with urllib.request.urlopen(req, context=ssl_ctx, timeout=90) as r:',
    '                  resp = json.loads(r.read().decode("utf-8", errors="replace"))',
    '              parts = resp.get("candidates", [{}])[0].get("content", {}).get("parts", [])',
    '              return "".join(p.get("text", "") for p in parts if not p.get("thought")).strip()',
    '',
    '          guidelines = open("/workspace/AGENTS.md", encoding="utf-8").read() if os.path.exists("/workspace/AGENTS.md") else ""',
    '          pulls_url = f"https://api.github.com/repos/{REPO}/pulls?state=all&sort=created&direction=desc&per_page={PR_COUNT}"',
    '          print(f"==> Fetching {PR_COUNT} most recent PRs from {REPO}...", flush=True)',
    '          prs = http_get_json(pulls_url)',
    '',
    '          def review_one(item):',
    '              idx, pr = item',
    '              num = pr["number"]',
    '              title = pr["title"]',
    '              author = pr.get("user", {}).get("login", "unknown")',
    '              state = "MERGED" if pr.get("merged_at") else pr.get("state", "OPEN").upper()',
    '              html_url = pr["html_url"]',
    '              desc = (pr.get("body") or "(no description)")[:2000]',
    '              print(f"==> [{idx}/{len(prs)}] Reviewing PR #{num}: {title}...", flush=True)',
    '              diff_text = fetch_pr_diff(num)[:18000]',
    '              prompt = (',
    '                  f"{guidelines}\\n\\n"',
    '                  f"Review Pull Request #{num} in {REPO}:\\n"',
    '                  f"Title: {title}\\nAuthor: {author}\\nState: {state}\\nURL: {html_url}\\n\\n"',
    '                  f"PR Description:\\n{desc}\\n\\nUnified Diff (truncated if large):\\n```diff\\n{diff_text}\\n```\\n\\n"',
    '                  "Respond in concise Markdown with:\\n"',
    '                  "1. **Verdict**: one of `LGTM`, `LGTM with minor nits`, or `Needs attention` on the first line as `Verdict: <choice>`\\n"',
    '                  "2. **Summary**: 1-2 sentences on what the PR changes.\\n"',
    '                  "3. **Key Findings & Risks**: 2-3 concrete technical bullets referencing files/symbols.\\n"',
    '              )',
    '              review_md = call_gemini(prompt)',
    '              verdict = "Reviewed"',
    '              for line in review_md.splitlines():',
    '                  clean = line.replace("*", "").strip()',
    '                  if clean.lower().startswith("verdict:"):',
    '                      verdict = clean.split(":", 1)[1].strip()[:40]',
    '                      break',
    '              pr_file = os.path.join(REVIEWS_DIR, f"PR-{num}.md")',
    '              with open(pr_file, "w", encoding="utf-8") as fh:',
    '                  fh.write(f"# PR #{num}: {title}\\n\\n- **URL**: {html_url}\\n- **Author**: `{author}`\\n- **State**: `{state}`\\n\\n{review_md}\\n")',
    '              safe_title = title.replace("|", "\\\\|")',
    '              row = f"| [#{num}]({html_url}) | `{state}` | `{author}` | {safe_title} | **{verdict}** |"',
    '              sec = f"## {idx}. [PR #{num}: {title}]({html_url}) (`{state}` by @{author})\\n\\n{review_md}\\n"',
    '              print(f"==> [{idx}/{len(prs)}] Finished PR #{num} ({verdict})", flush=True)',
    '              return idx, row, sec',
    '',
    '          with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:',
    '              results = sorted(pool.map(review_one, enumerate(prs, 1)), key=lambda x: x[0])',
    '',
    '          summary_sections = [',
    '              f"# Sandboxed AX PR Review Report: `{REPO}` (Recent {len(prs)} PRs)\\n",',
    '              "| PR | State | Author | Title | Verdict |",',
    '              "|---|---|---|---|---|",',
    '          ] + [r[1] for r in results]',
    '          detailed_sections = [r[2] for r in results]',
    '',
    '          report = "\\n".join(summary_sections) + "\\n\\n---\\n\\n" + "\\n---\\n\\n".join(detailed_sections)',
    '          with open(os.path.join(REVIEWS_DIR, "SUMMARY.md"), "w", encoding="utf-8") as fh:',
    '              fh.write(report)',
    '          with open(OUT_FILE, "w", encoding="utf-8") as fh:',
    '              fh.write(report)',
    '          print(f"==> Completed all {len(prs)} PR reviews! Report saved to {OUT_FILE} and {REVIEWS_DIR}/SUMMARY.md", flush=True)',
    '---',
    'apiVersion: ax.io/v1alpha1',
    'kind: Task',
    'metadata:',
    `  name: ${safePrefix}-review-prs`,
    '  atespace: default',
    'spec:',
    '  # Omit spec.profile so this Task starts warm from Workspace.spec.profile snapshot',
    '  workspace:',
    `    name: ${safePrefix}-review-ws`,
    '  gateway:',
    `    name: ${safePrefix}-review-gw`,
    '  goal:',
    `    text: "Review the ${prCount} most recent PRs in https://github.com/${repoSlug} and write structured findings to /workspace/.ax/output."`,
    '  env:',
    '    - name: GEMINI_API_KEY',
    `      value: "placeholder" # Replaced outside the sandbox by ${safePrefix}-review-gw`,
    '    - name: TARGET_REPO',
    `      value: "${repoSlug}"`,
    '    - name: PR_COUNT',
    `      value: "${prCount}"`,
    '  commands: ["python3", "/workspace/review_recent_prs.py"]',
    '  debug: true',
    '',
  ].join('\n');
}

export async function activate(context: vscode.ExtensionContext): Promise<AxExtensionApi> {
  const outputChannel = vscode.window.createOutputChannel('AX');
  const statusBarItem = vscode.window.createStatusBarItem(vscode.StatusBarAlignment.Left, 50);
  const diagnostics = vscode.languages.createDiagnosticCollection('ax-manifest');
  const inspectProvider = new AxInspectContentProvider();

  context.subscriptions.push(
    outputChannel,
    statusBarItem,
    diagnostics,
    vscode.workspace.registerTextDocumentContentProvider('ax-inspect', inspectProvider)
  );

  const log = (msg: string): void => {
    const ts = new Date().toTimeString().slice(0, 8);
    outputChannel.appendLine(`[${ts}] ${msg}`);
  };

  const getSection = () => vscode.workspace.getConfiguration('ax');

  let lastInstallResult: InstallResult | null = null;

  const runInstall = (showNotification: boolean): InstallResult => {
    const cfg = getSection();
    const res = installAxCliAndSkills({
      extensionPath: context.extensionPath,
      installDir: cfg.get<string>('installDir', ''),
      skillHomeDir: cfg.get<string>('skillHomeDir', ''),
      log,
    });
    lastInstallResult = res;
    context.environmentVariableCollection.replace('AX_CLI_BIN', res.axBinaryPath);
    context.environmentVariableCollection.prepend('PATH', `${res.installDir}${path.delimiter}`);
    if (showNotification) {
      vscode.window.showInformationMessage(
        `Installed 'ax' CLI (${res.axBinaryPath}) and synced AX skill to ${res.syncedSkillDirs.length} agent directory(s). Start a new agent chat session (+) so Antigravity / Claude / Gemini loads the 'ax' skill.`
      );
    }
    return res;
  };

  if (getSection().get<boolean>('autoInstallCliAndSkills', true)) {
    try {
      runInstall(false);
    } catch (err) {
      log(`Initial CLI/skill install failed: ${(err as Error).message}`);
    }
  }

  const readCliConfig = (): AxCliConfig => {
    const cfg = getSection();
    const installDir =
      lastInstallResult?.installDir ?? resolveInstallDir(cfg.get<string>('installDir', ''));
    const axBinaryPath = lastInstallResult?.axBinaryPath ?? path.join(installDir, 'ax');
    const images = resolveEffectiveImages({
      releaseVersion: cfg.get<string>('releaseVersion', DEFAULT_AX_RELEASE_VERSION),
      serverImage: process.env.AX_SERVER_IMAGE || cfg.get<string>('serverImage', ''),
      defaultTaskImage:
        process.env.AX_DEFAULT_TASK_IMAGE || cfg.get<string>('defaultTaskImage', ''),
      runnerVolumeImage:
        process.env.AX_RUNNER_VOLUME_IMAGE || cfg.get<string>('runnerVolumeImage', ''),
    });
    return {
      axBinaryPath,
      installDir,
      atespace: cfg.get<string>('atespace', 'default') || 'default',
      namespace: cfg.get<string>('namespace', 'ax-system') || 'ax-system',
      context: cfg.get<string>('context', '') || process.env.AX_KUBE_CONTEXT || '',
      kubeconfig: cfg.get<string>('kubeconfig', '') || process.env.KUBECONFIG || '',
      server: cfg.get<string>('server', '') || process.env.AX_SERVER || '',
      serverImage: images.serverImage,
      defaultTaskImage: images.defaultTaskImage,
      runnerVolumeImage: images.runnerVolumeImage,
    };
  };

  const cli = new AxCli(readCliConfig);

  const computeReadiness = async (): Promise<EnvironmentReadiness> => {
    const cfg = getSection();
    const cliCfg = readCliConfig();
    const skillHomeDir =
      (cfg.get<string>('skillHomeDir', '') || process.env.AX_SKILL_HOME_DIR || '').trim() ||
      os.homedir();
    return inspectEnvironmentReadiness({
      cli,
      installResult: lastInstallResult,
      skillHomeDir,
      namespace: cliCfg.namespace || 'ax-system',
      kubeconfig: cliCfg.kubeconfig,
      configuredContext: cliCfg.context,
      releaseVersion: cfg.get<string>('releaseVersion', DEFAULT_AX_RELEASE_VERSION),
      serverImage: cliCfg.serverImage,
      defaultTaskImage: cliCfg.defaultTaskImage,
      runnerVolumeImage: cliCfg.runnerVolumeImage,
    });
  };

  const explorer = new AxExplorerProvider(
    cli,
    () => readCliConfig().atespace || 'default',
    computeReadiness
  );

  const treeView = vscode.window.createTreeView('axExplorer', {
    treeDataProvider: explorer,
    showCollapseAll: true,
  });
  context.subscriptions.push(treeView);

  const updateStatusBar = async (): Promise<EnvironmentReadiness | null> => {
    try {
      const readiness = await computeReadiness();
      const cfg = readCliConfig();

      if (!readiness.allCoreReady) {
        const missingCount = readiness.steps.filter((s) => s.state === 'missing').length;
        statusBarItem.text = `$(tools) AX: Setup Needed (${missingCount})`;
        statusBarItem.backgroundColor = new vscode.ThemeColor(
          'statusBarItem.warningBackground'
        );
        statusBarItem.command = 'ax.guidedSetup';
        const md = new vscode.MarkdownString('', true);
        md.isTrusted = true;
        md.appendMarkdown(`### $(tools) AX Guided Setup Needed\n\n`);
        for (const st of readiness.steps) {
          const badge = st.state === 'ready' ? '$(pass)' : st.state === 'optional' ? '$(key)' : '$(warning)';
          md.appendMarkdown(`- ${badge} **${st.title}**: ${st.summary}\n`);
        }
        md.appendMarkdown(`\n[$(sparkle) Launch Guided Setup Wizard](command:ax.guidedSetup)`);
        statusBarItem.tooltip = md;
        statusBarItem.show();
        return readiness;
      }

      statusBarItem.backgroundColor = undefined;
      statusBarItem.text = `$(rocket) AX: ${readiness.activeContext}`;
      statusBarItem.command = 'ax.switchContext';
      const md = new vscode.MarkdownString('', true);
      md.isTrusted = true;
      md.appendMarkdown(`### $(rocket) AX Orchestrator Ready\n\n`);
      md.appendMarkdown(`- **CLI**: \`${cfg.axBinaryPath}\`\n`);
      md.appendMarkdown(`- **Active Context**: \`${readiness.activeContext}\`\n`);
      md.appendMarkdown(`- **Atespace**: \`${cfg.atespace}\`\n`);
      if (readiness.credentialSecrets.length > 0) {
        md.appendMarkdown(
          `- **Credential Secrets**: \`${readiness.credentialSecrets.join(', ')}\`\n`
        );
      }
      md.appendMarkdown(
        `\n[$(new-file) New Manifest](command:ax.newTaskManifest) · [$(key) Add Secret](command:ax.configureCredentialSecret) · [$(server-environment) Switch Context](command:ax.switchContext) · [$(refresh) Refresh](command:ax.refresh)`
      );
      statusBarItem.tooltip = md;
      statusBarItem.show();
      return readiness;
    } catch (err) {
      statusBarItem.text = '$(warning) AX: Setup Needed';
      statusBarItem.tooltip = `Click to launch Guided Setup (${(err as Error).message})`;
      statusBarItem.command = 'ax.guidedSetup';
      statusBarItem.show();
      return null;
    }
  };

  let refreshInFlight = false;
  const refreshAll = async (): Promise<void> => {
    if (refreshInFlight) {
      return;
    }
    refreshInFlight = true;
    try {
      explorer.refresh();
      await updateStatusBar();
    } finally {
      refreshInFlight = false;
    }
  };

  // Live background auto-refresh when the AX Explorer view is visible so tasks
  // spawned from VS Code's integrated terminal (e.g. by Gemini CLI / Claude Code)
  // appear and update states in real time.
  const autoRefreshTimer = setInterval(() => {
    const intervalSec = getSection().get<number>('autoRefreshIntervalSeconds', 4);
    if (intervalSec > 0 && treeView.visible && !refreshInFlight) {
      void refreshAll();
    }
  }, 4000);
  context.subscriptions.push({ dispose: () => clearInterval(autoRefreshTimer) });

  // Live YAML diagnostics for `apiVersion: ax.io/v1alpha1`
  const lintDocument = (doc: vscode.TextDocument): void => {
    const isYaml =
      doc.languageId === 'yaml' ||
      doc.fileName.endsWith('.yaml') ||
      doc.fileName.endsWith('.yml');
    if (!isYaml) {
      return;
    }
    const items = validateAxManifestText(doc.getText());
    if (items.length === 0) {
      diagnostics.delete(doc.uri);
      return;
    }
    const vscDiags = items.map((d) => {
      const lineIdx = Math.min(Math.max(0, d.line), Math.max(0, doc.lineCount - 1));
      const range = doc.lineAt(lineIdx).range;
      const sev =
        d.severity === 'error'
          ? vscode.DiagnosticSeverity.Error
          : vscode.DiagnosticSeverity.Warning;
      const diag = new vscode.Diagnostic(range, d.message, sev);
      diag.source = 'ax';
      return diag;
    });
    diagnostics.set(doc.uri, vscDiags);
  };

  for (const doc of vscode.workspace.textDocuments) {
    lintDocument(doc);
  }
  context.subscriptions.push(
    vscode.workspace.onDidOpenTextDocument(lintDocument),
    vscode.workspace.onDidChangeTextDocument((e) => lintDocument(e.document)),
    vscode.workspace.onDidCloseTextDocument((doc) => diagnostics.delete(doc.uri)),
    vscode.workspace.onDidChangeConfiguration((e) => {
      if (e.affectsConfiguration('ax')) {
        void refreshAll();
      }
    })
  );

  const tools = createAxToolHandlers(
    cli,
    () => {
      void refreshAll();
    },
    computeReadiness
  );
  const registeredLmToolCount = registerAxLanguageModelTools(context, tools, log);

  const openInspectDoc = async (
    titlePath: string,
    content: string,
    languageId: string,
    openMarkdownPreview: boolean = false
  ): Promise<vscode.TextDocument> => {
    const uri = vscode.Uri.parse(`ax-inspect:/${titlePath}`);
    inspectProvider.set(uri, content);
    const doc = await vscode.workspace.openTextDocument(uri);
    await vscode.languages.setTextDocumentLanguage(doc, languageId);
    await vscode.window.showTextDocument(doc, { preview: false });
    if (openMarkdownPreview && languageId === 'markdown') {
      try {
        await vscode.commands.executeCommand('markdown.showPreviewToSide', uri);
      } catch {
        // Markdown extension preview optional
      }
    }
    return doc;
  };

  /**
   * Streams a long-running setup/install command inside VS Code's Integrated
   * Terminal using a Pseudoterminal so output stays visible on screen, never
   * closes abruptly on exit, and resolves with `{ exitCode, output }`.
   */
  const runStreamingTerminalCommand = async (options: {
    title: string;
    command: string;
    args: string[];
    cwd?: string;
    env?: NodeJS.ProcessEnv;
    banner?: string;
  }): Promise<{ exitCode: number; output: string }> => {
    return new Promise((resolve) => {
      const writeEmitter = new vscode.EventEmitter<string>();
      let child: ChildProcess | null = null;
      let combined = '';
      let settled = false;

      const emit = (text: string) => {
        writeEmitter.fire(text.replace(/\r?\n/g, '\r\n'));
      };

      const finish = (exitCode: number) => {
        if (settled) {
          return;
        }
        settled = true;
        if (exitCode === 0) {
          emit(`\r\n\x1b[32m✓ ${options.title} completed successfully.\x1b[0m\r\n`);
        } else {
          emit(`\r\n\x1b[31m✗ ${options.title} failed (exit code ${exitCode}).\x1b[0m\r\n`);
        }
        resolve({ exitCode, output: combined });
      };

      const pty: vscode.Pseudoterminal = {
        onDidWrite: writeEmitter.event,
        open: () => {
          if (options.banner) {
            emit(`\x1b[36m${options.banner}\x1b[0m\r\n`);
          }
          emit(`\x1b[90m$ ${options.command} ${options.args.join(' ')}\x1b[0m\r\n\r\n`);
          child = spawn(options.command, options.args, {
            cwd: options.cwd,
            env: options.env ?? cli.buildEnv(),
            stdio: 'pipe',
          });
          child.stdout?.on('data', (chunk: Buffer) => {
            const s = chunk.toString('utf8');
            combined += s;
            log(s.trimEnd());
            emit(s);
          });
          child.stderr?.on('data', (chunk: Buffer) => {
            const s = chunk.toString('utf8');
            combined += s;
            log(s.trimEnd());
            emit(s);
          });
          child.on('error', (err) => {
            combined += `${err.message}\n`;
            log(`Command error: ${err.message}`);
            emit(`\x1b[31mError: ${err.message}\x1b[0m\r\n`);
            finish(1);
          });
          child.on('close', (code) => {
            finish(code ?? 1);
          });
        },
        close: () => {
          if (child && !settled) {
            child.kill('SIGTERM');
            finish(130);
          }
        },
        handleInput: (data: string) => {
          // Ctrl+C cancels running command
          if (data === '\x03' && child && !settled) {
            emit('^C\r\n');
            child.kill('SIGINT');
          }
        },
      };

      const term = vscode.window.createTerminal({ name: options.title, pty });
      term.show(false);
    });
  };

  const saveImageSettings = async (images: AxImageBundle): Promise<void> => {
    const cfg = getSection();
    if (images.releaseVersion) {
      await cfg.update(
        'releaseVersion',
        images.releaseVersion,
        vscode.ConfigurationTarget.Global
      );
    }
    await cfg.update('serverImage', images.serverImage, vscode.ConfigurationTarget.Global);
    await cfg.update(
      'defaultTaskImage',
      images.defaultTaskImage,
      vscode.ConfigurationTarget.Global
    );
    await cfg.update(
      'runnerVolumeImage',
      images.runnerVolumeImage,
      vscode.ConfigurationTarget.Global
    );
    await refreshAll();
  };

  const resolveTaskTarget = async (
    arg?: AxTreeItem | ResourceRefArg | string,
    promptTitle: string = 'Select AX Task',
    allowedStates?: string[]
  ): Promise<{ name: string; atespace?: string } | null> => {
    if (typeof arg === 'string' && arg.trim()) {
      return { name: arg.trim() };
    }
    if (arg && typeof arg === 'object' && 'resourceName' in arg && arg.resourceName) {
      return { name: arg.resourceName, atespace: arg.atespace };
    }
    const allTasks = await cli.listTasks();
    const tasks = allowedStates
      ? allTasks.filter((t) => allowedStates.includes(t.state.toLowerCase()))
      : allTasks;
    if (tasks.length === 0) {
      const stateHint = allowedStates ? ` in state [${allowedStates.join(', ')}]` : '';
      vscode.window.showInformationMessage(
        `No AX tasks${stateHint} found in the active atespace.`
      );
      return null;
    }
    const picked = await vscode.window.showQuickPick(
      tasks.map((t) => ({
        label: t.name,
        description: `${t.state} (${t.atespace})`,
        atespace: t.atespace,
      })),
      { title: promptTitle }
    );
    return picked ? { name: picked.label, atespace: picked.atespace } : null;
  };

  const resolveResourceTarget = async (
    arg?: AxTreeItem | ResourceRefArg
  ): Promise<{ kind: AxResourceKind; name: string; atespace?: string } | null> => {
    if (arg && arg.resourceKind && arg.resourceName) {
      return {
        kind: arg.resourceKind,
        name: arg.resourceName,
        atespace: arg.atespace,
      };
    }
    const kindPick = await vscode.window.showQuickPick(
      ['task', 'workspace', 'gateway', 'model'] as AxResourceKind[],
      { title: 'Select AX Resource Kind' }
    );
    if (!kindPick) {
      return null;
    }
    const nameInput = await vscode.window.showInputBox({
      title: `Enter ${kindPick} name`,
      placeHolder: `my-${kindPick}`,
    });
    if (!nameInput?.trim()) {
      return null;
    }
    return { kind: kindPick as AxResourceKind, name: nameInput.trim() };
  };

  const runInteractiveTerminal = (title: string, args: string[]): vscode.Terminal => {
    const cfg = readCliConfig();
    const term = vscode.window.createTerminal({
      name: title,
      shellPath: cfg.axBinaryPath,
      shellArgs: args,
      env: cli.buildEnv(),
    });
    term.show();
    return term;
  };

  // Register extension commands
  context.subscriptions.push(
    vscode.commands.registerCommand('ax.refresh', async () => {
      await refreshAll();
    }),

    vscode.commands.registerCommand('ax.installCliAndSkills', async () => {
      const res = runInstall(true);
      await refreshAll();
      return res;
    }),

    vscode.commands.registerCommand('ax.showLogs', () => {
      outputChannel.show(true);
    }),

    vscode.commands.registerCommand('ax.localStatus', async () => {
      const report = await cli.localStatus();
      await openInspectDoc(
        'local-status.json',
        JSON.stringify(report, null, 2),
        'json'
      );
      return report;
    }),

    vscode.commands.registerCommand(
      'ax.prepareImages',
      async (
        modeArg?: 'default-release' | { releaseTag: string } | AxImageBundle
      ): Promise<AxImageBundle | null> => {
        if (modeArg === 'default-release') {
          const bundle: AxImageBundle = { ...DEFAULT_RELEASE_IMAGES };
          await saveImageSettings(bundle);
          return bundle;
        }
        if (modeArg && typeof modeArg === 'object' && 'releaseTag' in modeArg) {
          const fetched = await fetchReleaseImagesFromGitHub(modeArg.releaseTag);
          await saveImageSettings(fetched);
          return fetched;
        }
        if (modeArg && typeof modeArg === 'object' && 'serverImage' in modeArg) {
          await saveImageSettings(modeArg);
          return modeArg;
        }

        const currentRel = getSection().get<string>('releaseVersion', DEFAULT_AX_RELEASE_VERSION);
        const picked = await vscode.window.showQuickPick(
          [
            {
              id: 'default',
              label: `$(pass-filled) (Recommended) Use Official ${DEFAULT_AX_RELEASE_VERSION} Release Images (ghcr.io/google/ax)`,
              description: 'Zero local build · @sha256 digest-pinned',
              detail:
                'Uses published multi-arch ghcr.io/google/ax/ax-server, ax-task-runner, and ax-runner-volume images.',
            },
            {
              id: 'github-release',
              label: '$(tag) Fetch Images from Another google/ax GitHub Release Tag...',
              description: `Current: ${currentRel}`,
              detail:
                'Downloads ax-server.yaml from https://github.com/google/ax/releases/download/<tag>/ and pins its @sha256 digests.',
            },
            {
              id: 'custom',
              label: '$(cloud) Enter Custom Container Registry Image References...',
              description: 'e.g. private mirror / Artifact Registry / GHCR',
              detail: 'Override --server-image, --default-task-image, and --runner-volume-image manually.',
            },
          ],
          {
            title: 'AX Release & Container Image Configuration',
            placeHolder: 'Official ghcr.io/google/ax images are pre-configured by default',
          }
        );

        if (!picked) {
          return null;
        }

        if (picked.id === 'default') {
          const bundle: AxImageBundle = { ...DEFAULT_RELEASE_IMAGES };
          await saveImageSettings(bundle);
          vscode.window.showInformationMessage(
            `Configured official AX ${DEFAULT_AX_RELEASE_VERSION} GHCR release images.`
          );
          return bundle;
        }

        if (picked.id === 'github-release') {
          const tagInput = await vscode.window.showInputBox({
            title: 'Enter google/ax GitHub Release Tag',
            value: currentRel || DEFAULT_AX_RELEASE_VERSION,
            placeHolder: 'v0.4.0-rc.1',
            validateInput: (v) => (v.trim() ? null : 'Enter a release tag such as v0.4.0-rc.1'),
          });
          if (!tagInput?.trim()) {
            return null;
          }
          const fetched = await vscode.window.withProgress(
            {
              location: vscode.ProgressLocation.Notification,
              title: `Fetching @sha256 image digests for google/ax ${tagInput.trim()}...`,
            },
            () => fetchReleaseImagesFromGitHub(tagInput.trim())
          );
          await saveImageSettings(fetched);
          vscode.window.showInformationMessage(
            `Configured AX ${fetched.releaseVersion} images from GitHub Releases.`
          );
          return fetched;
        }

        const currentCfg = readCliConfig();
        const srvInput = await vscode.window.showInputBox({
          title: 'AX Server Container Image (--server-image)',
          value: currentCfg.serverImage || DEFAULT_RELEASE_IMAGES.serverImage,
          placeHolder: 'ghcr.io/google/ax/ax-server@sha256:...',
          validateInput: (v) => (v.trim() ? null : 'ax-server image cannot be empty.'),
        });
        if (!srvInput?.trim()) {
          return null;
        }
        const taskInput = await vscode.window.showInputBox({
          title: 'AX Default Task Runner Image (--default-task-image)',
          value: currentCfg.defaultTaskImage || DEFAULT_RELEASE_IMAGES.defaultTaskImage,
          placeHolder: 'ghcr.io/google/ax/ax-task-runner@sha256:...',
        });
        if (taskInput === undefined) {
          return null;
        }
        const volInput = await vscode.window.showInputBox({
          title: 'AX Runner Volume Image (--runner-volume-image, optional)',
          value: currentCfg.runnerVolumeImage ?? DEFAULT_RELEASE_IMAGES.runnerVolumeImage,
          placeHolder: 'ghcr.io/google/ax/ax-runner-volume@sha256:...',
        });
        if (volInput === undefined) {
          return null;
        }
        const customImages: AxImageBundle = {
          releaseVersion: 'custom',
          serverImage: srvInput.trim(),
          defaultTaskImage: taskInput.trim(),
          runnerVolumeImage: volInput.trim(),
        };
        await saveImageSettings(customImages);
        return customImages;
      }
    ),

    vscode.commands.registerCommand(
      'ax.localStart',
      async (arg?: LocalStartArg): Promise<boolean> => {
        // Ensure CLI is installed first
        if (!lastInstallResult || !fs.existsSync(lastInstallResult.axBinaryPath)) {
          runInstall(false);
        }

        const cliCfg = readCliConfig();
        const effective = resolveEffectiveImages({
          serverImage: arg?.serverImage ?? cliCfg.serverImage,
          defaultTaskImage: arg?.defaultTaskImage ?? cliCfg.defaultTaskImage,
          runnerVolumeImage: arg?.runnerVolumeImage ?? cliCfg.runnerVolumeImage,
        });
        const serverImg = effective.serverImage;
        const taskImg = effective.defaultTaskImage;
        const runnerVolImg = effective.runnerVolumeImage;

        const clusterName = arg?.clusterName?.trim() || 'ax-local';
        let workers = arg?.workers;
        if (workers === undefined) {
          const defaultWorkers = Math.max(1, getSection().get<number>('localWorkers', 1) || 1);
          const workerPick = await vscode.window.showQuickPick(
            [
              {
                workers: 1,
                label: '$(vm) 1 gVisor Worker — Minimal RAM footprint',
                description: defaultWorkers === 1 ? 'current default' : '6 GiB+ VMs',
                detail: 'Runs 1 active sandbox at a time; great for single-task interactive work.',
              },
              {
                workers: 4,
                label: '$(server-process) 4 gVisor Workers — Parallel Multi-Task Execution',
                description: defaultWorkers === 4 ? 'current default' : '8–16 GiB+ hosts',
                detail: 'Runs up to 4 isolated sandboxes concurrently.',
              },
              {
                workers: 10,
                label: '$(rocket) 10 gVisor Workers — High-Concurrency Agent Fan-Out',
                description: defaultWorkers === 10 ? 'current default' : '16 GiB+ hosts',
                detail:
                  'Runs 10 sandboxes simultaneously (ideal for reviewing 10 PRs in parallel from one warm Workspace).',
              },
            ],
            {
              title: 'AX Local Start: Select gVisor Sandbox WorkerPool Size (--workers)',
              placeHolder: 'Choose how many concurrent sandbox workers Agent Substrate should run',
            }
          );
          if (!workerPick) {
            return false;
          }
          workers = workerPick.workers;
          await getSection().update('localWorkers', workers, vscode.ConfigurationTarget.Global);
        }

        const args: string[] = [];
        if (cliCfg.namespace?.trim()) {
          args.push('-n', cliCfg.namespace.trim());
        }
        args.push('local', 'start', '--name', clusterName, '--workers', String(workers));
        args.push('--server-image', serverImg);
        if (taskImg) {
          args.push('--default-task-image', taskImg);
        }
        if (runnerVolImg) {
          args.push('--runner-volume-image', runnerVolImg);
        }

        const res = await runStreamingTerminalCommand({
          title: 'AX: Local Stack Setup',
          command: cliCfg.axBinaryPath,
          args,
          banner: `Bootstrapping local Kind cluster "${clusterName}" + Agent Substrate v1.0.0 + AX (${DEFAULT_AX_RELEASE_VERSION} GHCR images)...`,
        });
        await refreshAll();
        if (res.exitCode !== 0) {
          vscode.window.showErrorMessage(
            `AX local stack bootstrap failed (exit ${res.exitCode}). Check the "AX: Local Stack Setup" terminal.`
          );
          return false;
        }

        const next = await vscode.window.showInformationMessage(
          `Local Agent Substrate + AX stack "${clusterName}" is Ready!`,
          'Configure Credential Secret',
          'New Task Manifest'
        );
        if (next === 'Configure Credential Secret') {
          void vscode.commands.executeCommand('ax.configureCredentialSecret');
        } else if (next === 'New Task Manifest') {
          void vscode.commands.executeCommand('ax.newTaskManifest');
        }
        return true;
      }
    ),

    vscode.commands.registerCommand('ax.installOnCluster', async (): Promise<boolean> => {
      const cliCfg = readCliConfig();
      const effective = resolveEffectiveImages({
        serverImage: cliCfg.serverImage,
        defaultTaskImage: cliCfg.defaultTaskImage,
        runnerVolumeImage: cliCfg.runnerVolumeImage,
      });

      const args: string[] = [];
      if (cliCfg.namespace?.trim()) {
        args.push('-n', cliCfg.namespace.trim());
      }
      if (cliCfg.context?.trim()) {
        args.push('--context', cliCfg.context.trim());
      }
      args.push('install', '--server-image', effective.serverImage);
      if (effective.defaultTaskImage) {
        args.push('--default-task-image', effective.defaultTaskImage);
      }
      if (effective.runnerVolumeImage) {
        args.push('--runner-volume-image', effective.runnerVolumeImage);
      }

      const res = await runStreamingTerminalCommand({
        title: 'AX: Cluster Install',
        command: cliCfg.axBinaryPath,
        args,
        banner: 'Deploying ax-postgres + ax-server onto active Agent Substrate cluster...',
      });
      await refreshAll();
      return res.exitCode === 0;
    }),

    vscode.commands.registerCommand(
      'ax.configureCredentialSecret',
      async (arg?: ConfigureSecretArg): Promise<boolean> => {
        const readiness = await computeReadiness();
        const cliCfg = readCliConfig();
        const kubeContext =
          cliCfg.context?.trim() ||
          (readiness.localStatus?.state === 'Running'
            ? readiness.localStatus.kubeContext
            : readiness.activeContext);

        if (!kubeContext || kubeContext === 'no-context') {
          vscode.window.showWarningMessage(
            'Start or select a Kubernetes cluster first before creating an egress credential Secret.'
          );
          return false;
        }

        let secretName = arg?.secretName?.trim() || '';
        let secretKey = arg?.secretKey?.trim() || '';
        let secretValue = arg?.secretValue ?? '';

        if (arg?.fromEnvVar && !secretValue) {
          secretValue = process.env[arg.fromEnvVar] || '';
        }

        if (!secretName || !secretKey || !secretValue) {
          const hasEnvGemini = Boolean(process.env.GEMINI_API_KEY);
          const hasEnvAnthropic = Boolean(process.env.ANTHROPIC_API_KEY);
          const hasEnvGithub = Boolean(process.env.GITHUB_TOKEN);

          const presets: Array<
            vscode.QuickPickItem & {
              secretName: string;
              secretKey: string;
              envVar?: string;
            }
          > = [
            {
              label: '$(sparkle) Gemini API Key (gemini-api-creds)',
              description: hasEnvGemini
                ? 'Detected $GEMINI_API_KEY in environment'
                : 'For generativelanguage.googleapis.com',
              detail:
                'Creates Secret ax-system/gemini-api-creds (key: api-key, label: ax.io/credential=true)',
              secretName: 'gemini-api-creds',
              secretKey: 'api-key',
              envVar: 'GEMINI_API_KEY',
            },
            {
              label: '$(hubot) Anthropic API Key (anthropic-creds)',
              description: hasEnvAnthropic
                ? 'Detected $ANTHROPIC_API_KEY in environment'
                : 'For api.anthropic.com',
              detail:
                'Creates Secret ax-system/anthropic-creds (key: api-key, label: ax.io/credential=true)',
              secretName: 'anthropic-creds',
              secretKey: 'api-key',
              envVar: 'ANTHROPIC_API_KEY',
            },
            {
              label: '$(github) GitHub Token (github-creds)',
              description: hasEnvGithub
                ? 'Detected $GITHUB_TOKEN in environment'
                : 'For api.github.com / private repos',
              detail:
                'Creates Secret ax-system/github-creds (key: token, label: ax.io/credential=true)',
              secretName: 'github-creds',
              secretKey: 'token',
              envVar: 'GITHUB_TOKEN',
            },
            {
              label: '$(key) Custom Egress Credential Secret...',
              description: 'Specify custom Secret name and key',
              secretName: '',
              secretKey: 'api-key',
            },
          ];

          const picked = await vscode.window.showQuickPick(presets, {
            title: `Configure Egress Credential Secret in "${cliCfg.namespace}" (context: ${kubeContext})`,
            placeHolder: 'Select API credential to store for Agent Substrate Gateway injection',
          });
          if (!picked) {
            return false;
          }

          secretName =
            picked.secretName ||
            (
              await vscode.window.showInputBox({
                title: 'Kubernetes Secret Name',
                placeHolder: 'my-api-creds',
              })
            )?.trim() ||
            '';
          if (!secretName) {
            return false;
          }

          secretKey =
            picked.secretKey ||
            (
              await vscode.window.showInputBox({
                title: 'Secret Data Key',
                value: 'api-key',
              })
            )?.trim() ||
            '';
          if (!secretKey) {
            return false;
          }

          const envCandidate = picked.envVar ? process.env[picked.envVar] : '';
          if (envCandidate) {
            const sourcePick = await vscode.window.showQuickPick(
              [
                {
                  id: 'env',
                  label: `$(check) Use $${picked.envVar} from current environment (masked, ${envCandidate.length} chars)`,
                },
                {
                  id: 'manual',
                  label: '$(edit) Paste a different key manually...',
                },
              ],
              { title: `Secret Value Source for ${secretName}/${secretKey}` }
            );
            if (!sourcePick) {
              return false;
            }
            if (sourcePick.id === 'env') {
              secretValue = envCandidate;
            }
          }

          if (!secretValue) {
            const manual = await vscode.window.showInputBox({
              title: `Enter secret value for ${secretName} (${secretKey})`,
              password: true,
              prompt: 'Stored in Kubernetes Secret with label ax.io/credential=true; never written to YAML manifests.',
              validateInput: (v) => (v.trim() ? null : 'Secret value cannot be empty'),
            });
            if (!manual?.trim()) {
              return false;
            }
            secretValue = manual.trim();
          }
        }

        const kubectlBin = resolveKubectlBinary(cliCfg.installDir);
        await vscode.window.withProgress(
          {
            location: vscode.ProgressLocation.Notification,
            title: `Saving credential Secret ${cliCfg.namespace}/${secretName}...`,
          },
          () =>
            applyCredentialSecret({
              kubectlBin,
              kubeContext,
              namespace: cliCfg.namespace || 'ax-system',
              secretName,
              secretKey,
              secretValue,
              kubeconfig: cliCfg.kubeconfig,
            })
        );

        log(
          `Applied labeled credential Secret ${cliCfg.namespace}/${secretName} (key=${secretKey}, context=${kubeContext}).`
        );
        await refreshAll();
        vscode.window.showInformationMessage(
          `Credential Secret "${secretName}" (${secretKey}) is ready for Gateway URI ate-secret://k8s.io/default/${cliCfg.namespace}/${secretName}/${secretKey}`
        );
        return true;
      }
    ),

    vscode.commands.registerCommand(
      'ax.guidedSetup',
      async (autoRunAll?: boolean): Promise<EnvironmentReadiness> => {
        let readiness = await computeReadiness();

        if (autoRunAll === true) {
          if (readiness.steps.find((s) => s.id === 'cli')?.state === 'missing') {
            runInstall(false);
            readiness = await computeReadiness();
          }
          if (
            readiness.steps.find((s) => s.id === 'substrate')?.state === 'missing' ||
            readiness.steps.find((s) => s.id === 'ax')?.state === 'missing'
          ) {
            await vscode.commands.executeCommand('ax.localStart', {});
            readiness = await computeReadiness();
          }
          await refreshAll();
          return readiness;
        }

        const items: Array<vscode.QuickPickItem & { id: string; cmd?: string }> = [];
        if (!readiness.allCoreReady) {
          items.push({
            id: 'auto-all',
            label: '$(rocket) (Recommended) Install All Missing AX & Substrate Components Automatically',
            description: 'Guided end-to-end bootstrap (pulls ghcr.io/google/ax release images)',
            detail:
              'Installs ax CLI & skills -> bootstraps Kind + Agent Substrate v1.0.0 + AX Control Plane with zero local build.',
          });
        }

        for (const step of readiness.steps) {
          const icon =
            step.state === 'ready'
              ? '$(pass-filled)'
              : step.state === 'optional'
                ? '$(key)'
                : '$(warning)';
          items.push({
            id: step.id,
            cmd: step.actionCommand,
            label: `${icon} ${step.title}`,
            description: step.summary,
            detail: step.detail.replace(/\n/g, ' · '),
          });
        }

        items.push({
          id: 'new-manifest',
          cmd: 'ax.newTaskManifest',
          label: '$(new-file) Scaffold New Task / GitHub PR Review Manifest...',
          description: 'Gateway + Warm Workspace + Sandboxed Task',
        });

        const picked = await vscode.window.showQuickPick(items, {
          title: readiness.allCoreReady
            ? 'AX Guided Setup — All Core Components Ready!'
            : 'AX Guided Setup — Select Missing Component to Install',
          placeHolder: 'Follow the numbered steps to install missing parts of AX & Agent Substrate',
        });

        if (!picked) {
          return readiness;
        }
        if (picked.id === 'auto-all') {
          return await vscode.commands.executeCommand<EnvironmentReadiness>('ax.guidedSetup', true);
        }
        if (picked.cmd && picked.cmd !== 'ax.guidedSetup') {
          await vscode.commands.executeCommand(picked.cmd);
        }
        return await computeReadiness();
      }
    ),

    vscode.commands.registerCommand('ax.localStop', async () => {
      await vscode.window.withProgress(
        {
          location: vscode.ProgressLocation.Notification,
          title: 'Stopping local AX cluster...',
        },
        async () => {
          const res = await cli.execRaw(['local', 'stop'], { timeoutMs: 60_000 });
          if (res.exitCode !== 0) {
            throw new Error(res.stderr || res.stdout);
          }
        }
      );
      await refreshAll();
    }),

    vscode.commands.registerCommand('ax.localDelete', async (skipConfirm?: boolean) => {
      if (!skipConfirm) {
        const confirm = await vscode.window.showWarningMessage(
          'Delete the local AX cluster (ax-local) and all local sandboxes?',
          { modal: true },
          'Delete'
        );
        if (confirm !== 'Delete') {
          return;
        }
      }
      await vscode.window.withProgress(
        {
          location: vscode.ProgressLocation.Notification,
          title: 'Deleting local AX cluster...',
        },
        async () => {
          const res = await cli.execRaw(['local', 'delete'], { timeoutMs: 90_000 });
          if (res.exitCode !== 0) {
            throw new Error(res.stderr || res.stdout);
          }
        }
      );
      await refreshAll();
    }),

    vscode.commands.registerCommand(
      'ax.switchContext',
      async (arg?: AxTreeItem | string) => {
        let targetCtx: string | undefined;
        if (typeof arg === 'string' && arg.trim()) {
          targetCtx = arg.trim();
        } else if (arg && typeof arg === 'object' && arg.contextName) {
          targetCtx = arg.contextName;
        } else {
          const contexts = await cli.listContexts();
          const picked = await vscode.window.showQuickPick(
            contexts.map((c) => ({
              label: c.name,
              description: c.current ? `active · ${c.tunnel}` : c.cluster,
            })),
            { title: 'Switch Active Kubernetes Context (ax ctx)' }
          );
          targetCtx = picked?.label;
        }
        if (!targetCtx) {
          return null;
        }
        const out = await cli.switchContext(targetCtx);
        log(out);
        await refreshAll();
        return out;
      }
    ),

    vscode.commands.registerCommand(
      'ax.applyFile',
      async (uriOrPath?: vscode.Uri | string) => {
        let filePath: string | undefined;
        if (typeof uriOrPath === 'string') {
          filePath = uriOrPath;
        } else if (uriOrPath instanceof vscode.Uri) {
          filePath = uriOrPath.fsPath;
        } else if (vscode.window.activeTextEditor?.document.uri.scheme === 'file') {
          await vscode.window.activeTextEditor.document.save();
          filePath = vscode.window.activeTextEditor.document.uri.fsPath;
        } else {
          const selected = await vscode.window.showOpenDialog({
            canSelectMany: false,
            filters: { 'YAML Manifests': ['yaml', 'yml'] },
            title: 'Select AX Manifest to Apply',
          });
          filePath = selected?.[0]?.fsPath;
        }
        if (!filePath) {
          return null;
        }
        const out = await vscode.window.withProgress(
          {
            location: vscode.ProgressLocation.Notification,
            title: `Applying ${path.basename(filePath)} to AX (pre-warming Workspace snapshot if new)...`,
          },
          () => cli.applyFile(filePath)
        );
        log(`Applied ${filePath}:\n${out}`);
        await refreshAll();
        vscode.window.showInformationMessage(`Applied ${path.basename(filePath)}: ${out.replace(/\s+/g, ' ')}`);
        return out;
      }
    ),

    vscode.commands.registerCommand(
      'ax.newTaskManifest',
      async (
        templateArg?:
          | 'starter'
          | { template: 'pr-review'; repo?: string; prCount?: number; secretName?: string; saveToPath?: string }
      ) => {
        let content: string;
        let saveToPath: string | undefined;

        if (templateArg === 'starter') {
          content = buildStarterSandboxTemplate();
        } else if (templateArg && typeof templateArg === 'object' && templateArg.template === 'pr-review') {
          content = buildGitHubPrReviewTemplate(
            templateArg.repo || 'etcd-io/etcd',
            templateArg.prCount || 10,
            templateArg.secretName || 'gemini-api-creds'
          );
          saveToPath = templateArg.saveToPath;
        } else {
          const picked = await vscode.window.showQuickPick(
            [
              {
                id: 'starter',
                label: '$(terminal) Minimal Interactive Sandbox (Workspace + Task)',
                description: 'ax.io/v1alpha1 starter template',
                detail: 'Creates an isolated gVisor sandbox with debug SSH enabled.',
              },
              {
                id: 'pr-review',
                label: '$(git-pull-request) GitHub PR Review Pipeline (Gateway + Warm Workspace + Task)',
                description: 'Zero-trust Gemini API injection + shallow repo snapshot',
                detail:
                  'Pre-warms a GitHub repository snapshot and reviews recent PRs inside a sandboxed AX Task.',
              },
            ],
            {
              title: 'Select AX Manifest Template',
              placeHolder: 'Choose a manifest template to scaffold',
            }
          );
          if (!picked) {
            return null;
          }
          if (picked.id === 'starter') {
            content = buildStarterSandboxTemplate();
          } else {
            const repo =
              (
                await vscode.window.showInputBox({
                  title: 'GitHub Repository (owner/repo)',
                  value: 'etcd-io/etcd',
                  placeHolder: 'etcd-io/etcd',
                })
              )?.trim() || 'etcd-io/etcd';
            const countRaw = await vscode.window.showInputBox({
              title: 'Number of Recent Pull Requests to Review',
              value: '10',
              validateInput: (v) =>
                /^\d+$/.test(v.trim()) && Number(v) >= 1 && Number(v) <= 50
                  ? null
                  : 'Enter a number between 1 and 50',
            });
            if (!countRaw) {
              return null;
            }
            content = buildGitHubPrReviewTemplate(repo, Number(countRaw.trim()), 'gemini-api-creds');
          }
        }

        if (saveToPath) {
          fs.mkdirSync(path.dirname(saveToPath), { recursive: true });
          fs.writeFileSync(saveToPath, content, 'utf8');
          const doc = await vscode.workspace.openTextDocument(vscode.Uri.file(saveToPath));
          await vscode.window.showTextDocument(doc);
          return doc;
        }

        const doc = await vscode.workspace.openTextDocument({
          language: 'yaml',
          content,
        });
        await vscode.window.showTextDocument(doc);
        return doc;
      }
    ),

    vscode.commands.registerCommand(
      'ax.resumeTask',
      async (arg?: AxTreeItem | ResourceRefArg | string) => {
        const target = await resolveTaskTarget(arg, 'Select Suspended AX Task to Resume', [
          'suspended',
        ]);
        if (!target) {
          return null;
        }
        const out = await vscode.window.withProgress(
          {
            location: vscode.ProgressLocation.Notification,
            title: `Resuming AX task "${target.name}"...`,
          },
          () => cli.resumeTask(target.name, target.atespace)
        );
        log(out);
        await refreshAll();
        return out;
      }
    ),

    vscode.commands.registerCommand(
      'ax.suspendTask',
      async (arg?: AxTreeItem | ResourceRefArg | string) => {
        const target = await resolveTaskTarget(arg, 'Select Working AX Task to Suspend', [
          'working',
        ]);
        if (!target) {
          return null;
        }
        const out = await vscode.window.withProgress(
          {
            location: vscode.ProgressLocation.Notification,
            title: `Checkpointing & suspending AX task "${target.name}"...`,
          },
          () => cli.suspendTask(target.name, target.atespace)
        );
        log(out);
        await refreshAll();
        return out;
      }
    ),

    vscode.commands.registerCommand(
      'ax.sshTerminal',
      async (arg?: AxTreeItem | ResourceRefArg | string) => {
        const target = await resolveTaskTarget(
          arg,
          'Open SSH Shell into Working AX Task Sandbox',
          ['working']
        );
        if (!target) {
          return null;
        }
        return runInteractiveTerminal(
          `AX SSH: ${target.name}`,
          cli.buildSshTerminalArgs(target.name, target.atespace)
        );
      }
    ),

    vscode.commands.registerCommand(
      'ax.viewTaskOutput',
      async (arg?: AxTreeItem | ResourceRefArg | string) => {
        const target = await resolveTaskTarget(arg, 'Select AX Task to View Output');
        if (!target) {
          return null;
        }
        let text = await cli.getTaskOutput(target.name, target.atespace);
        // If task is still Working or output is truncated, try reading full /workspace/.ax/output via ax ssh
        if (!text.startsWith('#')) {
          const sshRes = await cli
            .execInTask(
              target.name,
              'cat /workspace/reviews/SUMMARY.md 2>/dev/null || cat /workspace/.ax/output 2>/dev/null',
              target.atespace
            )
            .catch(() => null);
          if (sshRes && sshRes.exitCode === 0 && sshRes.stdout.trim()) {
            text = sshRes.stdout;
          }
        }
        await openInspectDoc(`output-${target.name}.md`, text, 'markdown', true);
        return text;
      }
    ),

    vscode.commands.registerCommand(
      'ax.describeResource',
      async (arg?: AxTreeItem | ResourceRefArg) => {
        const target = await resolveResourceTarget(arg);
        if (!target) {
          return null;
        }
        const text = await cli.describeResource(
          target.kind,
          target.name,
          target.atespace
        );
        await openInspectDoc(
          `describe-${target.kind}-${target.name}.yaml`,
          text,
          'yaml'
        );
        return text;
      }
    ),

    vscode.commands.registerCommand(
      'ax.getResourceYaml',
      async (arg?: AxTreeItem | ResourceRefArg) => {
        const target = await resolveResourceTarget(arg);
        if (!target) {
          return null;
        }
        const text = await cli.getResourceYaml(
          target.kind,
          target.name,
          target.atespace
        );
        await openInspectDoc(`${target.kind}-${target.name}.yaml`, text, 'yaml');
        return text;
      }
    ),

    vscode.commands.registerCommand(
      'ax.promoteTask',
      async (arg?: AxTreeItem | ResourceRefArg | string, targetContextArg?: string) => {
        const target = await resolveTaskTarget(arg, 'Select AX Task to Promote');
        if (!target) {
          return null;
        }
        let destCtx = targetContextArg;
        if (!destCtx) {
          const contexts = await cli.listContexts();
          const picked = await vscode.window.showQuickPick(
            contexts.filter((c) => !c.current).map((c) => ({ label: c.name, description: c.cluster })),
            { title: `Promote task "${target.name}" to cluster context` }
          );
          destCtx = picked?.label;
        }
        if (!destCtx) {
          return null;
        }
        const preflight = await cli.promoteTask(target.name, destCtx, {
          atespace: target.atespace,
          preflightOnly: true,
        });
        const choice = await vscode.window.showInformationMessage(
          `Preflight to "${destCtx}":\n${preflight}`,
          { modal: true },
          'Promote & Suspend Local',
          'Promote (Keep Local)'
        );
        if (!choice) {
          return null;
        }
        const out = await cli.promoteTask(target.name, destCtx, {
          atespace: target.atespace,
          suspendSource: choice === 'Promote & Suspend Local',
        });
        log(out);
        await refreshAll();
        return out;
      }
    ),

    vscode.commands.registerCommand(
      'ax.deleteResource',
      async (arg?: AxTreeItem | ResourceRefArg, skipConfirm?: boolean) => {
        const target = await resolveResourceTarget(arg);
        if (!target) {
          return null;
        }
        if (!skipConfirm) {
          const answer = await vscode.window.showWarningMessage(
            `Delete AX ${target.kind} "${target.name}"?`,
            { modal: true },
            'Delete'
          );
          if (answer !== 'Delete') {
            return null;
          }
        }
        const out = await cli.deleteResource(target.kind, target.name, target.atespace);
        log(out);
        await refreshAll();
        return out;
      }
    ),

    vscode.commands.registerCommand(
      'ax.stopTunnel',
      async (arg?: AxTreeItem | string) => {
        const ctxName =
          typeof arg === 'string'
            ? arg
            : arg && typeof arg === 'object'
              ? arg.contextName
              : undefined;
        const out = await cli.stopTunnel(ctxName);
        log(out);
        await refreshAll();
        return out;
      }
    ),

    vscode.commands.registerCommand('ax.openStudio', () => {
      return runInteractiveTerminal('AX Studio', ['studio']);
    })
  );

  // Check readiness asynchronously on activation and surface non-blocking setup prompt if missing
  void updateStatusBar().then((readiness) => {
    if (
      readiness &&
      !readiness.allCoreReady &&
      process.env.AX_SUPPRESS_STARTUP_PROMPT !== '1' &&
      getSection().get<boolean>('promptGuidedSetupOnStartup', true)
    ) {
      const missingNames = readiness.steps
        .filter((s) => s.state === 'missing')
        .map((s) => s.title.replace(/^\d+\.\s*/, ''))
        .join(', ');
      void vscode.window
        .showInformationMessage(
          `AX extension installed 'ax' CLI, and detected missing stack components (${missingNames}).`,
          'Launch Guided Setup',
          'Open Walkthrough'
        )
        .then((choice) => {
          if (choice === 'Launch Guided Setup') {
            void vscode.commands.executeCommand('ax.guidedSetup');
          } else if (choice === 'Open Walkthrough') {
            void vscode.commands.executeCommand(
              'workbench.action.openWalkthrough',
              'google.ax-vscode#axGettingStarted',
              false
            );
          }
        });
    }
  });

  log('AX VS Code extension activated.');

  return {
    getInstallResult: () => lastInstallResult,
    getReadiness: computeReadiness,
    cli,
    explorer,
    tools,
    registeredLmToolCount,
    refreshAll,
  };
}

export function deactivate(): void {
  // No persistent daemons started by extension host; background tunnels live in ~/.ax/tunnels.
}

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
import { AxExplorerProvider, AxTreeItem } from './treeProvider';

export interface ResourceRefArg {
  resourceKind: AxResourceKind;
  resourceName: string;
  atespace?: string;
}

export interface AxExtensionApi {
  getInstallResult(): InstallResult | null;
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
    context.environmentVariableCollection.replace(
      'AX_CLI_BIN',
      res.axBinaryPath
    );
    context.environmentVariableCollection.prepend(
      'PATH',
      `${res.installDir}${path.delimiter}`
    );
    if (showNotification) {
      vscode.window.showInformationMessage(
        `Installed 'ax' CLI (${res.axBinaryPath}) and synced AX skill to ${res.syncedSkillDirs.length} agent directory(s).`
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
    return {
      axBinaryPath,
      installDir,
      atespace: cfg.get<string>('atespace', 'default') || 'default',
      namespace: cfg.get<string>('namespace', 'ax-system') || 'ax-system',
      context: cfg.get<string>('context', '') || process.env.AX_KUBE_CONTEXT || '',
      kubeconfig: cfg.get<string>('kubeconfig', '') || process.env.KUBECONFIG || '',
      server: cfg.get<string>('server', '') || process.env.AX_SERVER || '',
    };
  };

  const cli = new AxCli(readCliConfig);
  const explorer = new AxExplorerProvider(cli, () => readCliConfig().atespace || 'default');

  const treeView = vscode.window.createTreeView('axExplorer', {
    treeDataProvider: explorer,
    showCollapseAll: true,
  });
  context.subscriptions.push(treeView);

  const updateStatusBar = async (): Promise<void> => {
    try {
      const cfg = readCliConfig();
      const contexts = await cli.listContexts().catch(() => []);
      const activeCtx =
        cfg.context || contexts.find((c) => c.current)?.name || 'no-context';
      const activeEntry = contexts.find((c) => c.name === activeCtx);

      statusBarItem.text = `$(rocket) AX: ${activeCtx}`;
      statusBarItem.command = 'ax.switchContext';
      const md = new vscode.MarkdownString('', true);
      md.isTrusted = true;
      md.appendMarkdown(`### $(rocket) AX Orchestrator\n\n`);
      md.appendMarkdown(`- **CLI**: \`${cfg.axBinaryPath}\`\n`);
      md.appendMarkdown(`- **Active Context**: \`${activeCtx}\`\n`);
      md.appendMarkdown(`- **Atespace**: \`${cfg.atespace}\`\n`);
      if (activeEntry && activeEntry.tunnel !== '-') {
        md.appendMarkdown(`- **Tunnel**: \`${activeEntry.tunnel}\`\n`);
      }
      md.appendMarkdown(
        `\n[$(new-file) New Manifest](command:ax.newTaskManifest) · [$(server-environment) Switch Context](command:ax.switchContext) · [$(refresh) Refresh](command:ax.refresh) · [$(output) Logs](command:ax.showLogs)`
      );
      statusBarItem.tooltip = md;
      statusBarItem.show();
    } catch (err) {
      statusBarItem.text = '$(warning) AX: Setup Needed';
      statusBarItem.tooltip = `Click to install AX CLI & skills (${(err as Error).message})`;
      statusBarItem.command = 'ax.installCliAndSkills';
      statusBarItem.show();
    }
  };

  const refreshAll = async (): Promise<void> => {
    explorer.refresh();
    await updateStatusBar();
  };

  // Editor YAML diagnostics for `apiVersion: ax.io/v1alpha1`
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

  // Register VS Code Language Model Tools (`ax_status`, `ax_list_resources`, `ax_manage_task`, `ax_ssh_exec`)
  const tools = createAxToolHandlers(cli, () => {
    void refreshAll();
  });
  const registeredLmToolCount = registerAxLanguageModelTools(context, tools, log);

  const openInspectDoc = async (
    titlePath: string,
    content: string,
    languageId: string
  ): Promise<vscode.TextDocument> => {
    const uri = vscode.Uri.parse(`ax-inspect:/${titlePath}`);
    inspectProvider.set(uri, content);
    const doc = await vscode.workspace.openTextDocument(uri);
    await vscode.languages.setTextDocumentLanguage(doc, languageId);
    await vscode.window.showTextDocument(doc, { preview: true });
    return doc;
  };

  const resolveTaskTarget = async (
    arg?: AxTreeItem | ResourceRefArg | string,
    promptTitle: string = 'Select AX Task'
  ): Promise<{ name: string; atespace?: string } | null> => {
    if (typeof arg === 'string' && arg.trim()) {
      return { name: arg.trim() };
    }
    if (arg && typeof arg === 'object' && 'resourceName' in arg && arg.resourceName) {
      return { name: arg.resourceName, atespace: arg.atespace };
    }
    const tasks = await cli.listTasks();
    if (tasks.length === 0) {
      vscode.window.showInformationMessage('No AX tasks found in the active atespace.');
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

  const runInTerminal = (title: string, args: string[]): vscode.Terminal => {
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

    vscode.commands.registerCommand('ax.installCliAndSkills', () => {
      return runInstall(true);
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

    vscode.commands.registerCommand('ax.localStart', async () => {
      const serverImg =
        process.env.AX_SERVER_IMAGE ||
        (await vscode.window.showInputBox({
          title: 'AX Local Start: ax-server Image (optional if release default exists)',
          placeHolder: 'ghcr.io/google/ax/ax-server:latest (leave empty to use CLI default)',
        })) ||
        '';
      const taskImg =
        process.env.AX_DEFAULT_TASK_IMAGE ||
        (await vscode.window.showInputBox({
          title: 'AX Local Start: Default Task Image (optional)',
          placeHolder: 'ghcr.io/google/ax/ax-task-runner:latest (leave empty to use CLI default)',
        })) ||
        '';
      const args = ['local', 'start'];
      if (serverImg.trim()) {
        args.push('--server-image', serverImg.trim());
      }
      if (taskImg.trim()) {
        args.push('--default-task-image', taskImg.trim());
      }
      runInTerminal('AX: Local Start', args);
    }),

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

    vscode.commands.registerCommand('ax.localDelete', async () => {
      const confirm = await vscode.window.showWarningMessage(
        'Delete the local AX cluster (ax-local) and all local sandboxes?',
        { modal: true },
        'Delete'
      );
      if (confirm !== 'Delete') {
        return;
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
            title: `Applying ${path.basename(filePath)} to AX...`,
          },
          () => cli.applyFile(filePath)
        );
        log(`Applied ${filePath}:\n${out}`);
        await refreshAll();
        return out;
      }
    ),

    vscode.commands.registerCommand('ax.newTaskManifest', async () => {
      const template = [
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
      const doc = await vscode.workspace.openTextDocument({
        language: 'yaml',
        content: template,
      });
      await vscode.window.showTextDocument(doc);
      return doc;
    }),

    vscode.commands.registerCommand(
      'ax.resumeTask',
      async (arg?: AxTreeItem | ResourceRefArg | string) => {
        const target = await resolveTaskTarget(arg, 'Select AX Task to Resume');
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
        const target = await resolveTaskTarget(arg, 'Select AX Task to Suspend');
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
        const target = await resolveTaskTarget(arg, 'Open SSH Shell into Working AX Task');
        if (!target) {
          return null;
        }
        return runInTerminal(
          `AX SSH: ${target.name}`,
          cli.buildSshTerminalArgs(target.name, target.atespace)
        );
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
      return runInTerminal('AX Studio', ['studio']);
    })
  );

  void updateStatusBar();
  log('AX VS Code extension activated.');

  return {
    getInstallResult: () => lastInstallResult,
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

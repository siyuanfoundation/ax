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

import * as vscode from 'vscode';
import {
  AxCli,
  AxResourceKind,
  ContextEntry,
  GatewaySummary,
  ModelSummary,
  TaskSummary,
  TunnelEntry,
  WorkspaceSummary,
} from './axCli';
import { EnvironmentReadiness, ReadinessStep } from './setupHelper';

export type AxTreeGroup =
  | 'setup'
  | 'contexts'
  | 'tunnels'
  | 'tasks'
  | 'workspaces'
  | 'gateways'
  | 'models';

export class AxTreeItem extends vscode.TreeItem {
  public readonly group?: AxTreeGroup;
  public readonly resourceKind?: AxResourceKind;
  public readonly resourceName?: string;
  public readonly atespace?: string;
  public readonly contextName?: string;

  constructor(options: {
    label: string;
    collapsibleState?: vscode.TreeItemCollapsibleState;
    description?: string;
    tooltip?: string | vscode.MarkdownString;
    contextValue?: string;
    icon?: vscode.ThemeIcon;
    command?: vscode.Command;
    group?: AxTreeGroup;
    resourceKind?: AxResourceKind;
    resourceName?: string;
    atespace?: string;
    contextName?: string;
  }) {
    super(options.label, options.collapsibleState ?? vscode.TreeItemCollapsibleState.None);
    this.description = options.description;
    this.tooltip = options.tooltip;
    this.contextValue = options.contextValue;
    this.iconPath = options.icon;
    this.command = options.command;
    this.group = options.group;
    this.resourceKind = options.resourceKind;
    this.resourceName = options.resourceName;
    this.atespace = options.atespace;
    this.contextName = options.contextName;
  }
}

export class AxExplorerProvider implements vscode.TreeDataProvider<AxTreeItem> {
  private readonly _onDidChangeTreeData = new vscode.EventEmitter<AxTreeItem | undefined | void>();
  public readonly onDidChangeTreeData = this._onDidChangeTreeData.event;
  private lastReadiness: EnvironmentReadiness | null = null;

  constructor(
    private readonly cli: AxCli,
    private readonly getDefaultAtespace: () => string,
    private readonly getReadiness: () => Promise<EnvironmentReadiness>
  ) {}

  public refresh(): void {
    this.lastReadiness = null;
    this._onDidChangeTreeData.fire();
  }

  public getCachedReadiness(): EnvironmentReadiness | null {
    return this.lastReadiness;
  }

  public getTreeItem(element: AxTreeItem): vscode.TreeItem {
    return element;
  }

  public async getChildren(element?: AxTreeItem): Promise<AxTreeItem[]> {
    if (!element) {
      const atespace = this.getDefaultAtespace();
      return [
        new AxTreeItem({
          label: 'Setup & Cluster Readiness',
          collapsibleState: vscode.TreeItemCollapsibleState.Expanded,
          icon: new vscode.ThemeIcon('checklist'),
          group: 'setup',
        }),
        new AxTreeItem({
          label: `Tasks (${atespace})`,
          collapsibleState: vscode.TreeItemCollapsibleState.Expanded,
          icon: new vscode.ThemeIcon('rocket'),
          group: 'tasks',
        }),
        new AxTreeItem({
          label: `Workspaces (${atespace})`,
          collapsibleState: vscode.TreeItemCollapsibleState.Expanded,
          icon: new vscode.ThemeIcon('folder-library'),
          group: 'workspaces',
        }),
        new AxTreeItem({
          label: `Gateways (${atespace})`,
          collapsibleState: vscode.TreeItemCollapsibleState.Collapsed,
          icon: new vscode.ThemeIcon('shield'),
          group: 'gateways',
        }),
        new AxTreeItem({
          label: `Models (${atespace})`,
          collapsibleState: vscode.TreeItemCollapsibleState.Collapsed,
          icon: new vscode.ThemeIcon('hubot'),
          group: 'models',
        }),
        new AxTreeItem({
          label: 'Kubernetes Contexts',
          collapsibleState: vscode.TreeItemCollapsibleState.Collapsed,
          icon: new vscode.ThemeIcon('server-environment'),
          group: 'contexts',
        }),
        new AxTreeItem({
          label: 'Background Tunnels',
          collapsibleState: vscode.TreeItemCollapsibleState.Collapsed,
          icon: new vscode.ThemeIcon('plug'),
          group: 'tunnels',
        }),
      ];
    }

    switch (element.group) {
      case 'setup':
        return this.getSetupChildren();
      case 'contexts':
        return this.getContextChildren();
      case 'tunnels':
        return this.getTunnelChildren();
      case 'tasks':
        return this.getTaskChildren();
      case 'workspaces':
        return this.getWorkspaceChildren();
      case 'gateways':
        return this.getGatewayChildren();
      case 'models':
        return this.getModelChildren();
      default:
        return [];
    }
  }

  private infoItem(
    label: string,
    description?: string,
    iconName: string = 'info',
    command?: vscode.Command
  ): AxTreeItem {
    return new AxTreeItem({
      label,
      description,
      icon: new vscode.ThemeIcon(iconName),
      command,
    });
  }

  private stepIcon(step: ReadinessStep): vscode.ThemeIcon {
    switch (step.state) {
      case 'ready':
        return new vscode.ThemeIcon('pass-filled', new vscode.ThemeColor('testing.iconPassed'));
      case 'warning':
        return new vscode.ThemeIcon('warning', new vscode.ThemeColor('editorWarning.foreground'));
      case 'optional':
        return new vscode.ThemeIcon('key');
      case 'missing':
      default:
        return new vscode.ThemeIcon('circle-large-outline', new vscode.ThemeColor('editorWarning.foreground'));
    }
  }

  private async getSetupChildren(): Promise<AxTreeItem[]> {
    try {
      const readiness = await this.getReadiness();
      this.lastReadiness = readiness;

      const items: AxTreeItem[] = [];
      if (!readiness.allCoreReady) {
        items.push(
          new AxTreeItem({
            label: 'Launch Guided Setup Wizard...',
            description: 'Install missing AX & Substrate components',
            icon: new vscode.ThemeIcon('sparkle', new vscode.ThemeColor('textLink.foreground')),
            contextValue: 'axGuidedSetupCTA',
            command: {
              command: 'ax.guidedSetup',
              title: 'Run Guided Setup',
            },
          })
        );
      }

      for (const step of readiness.steps) {
        const md = new vscode.MarkdownString('', true);
        md.isTrusted = true;
        md.appendMarkdown(`### ${step.title}\n\n`);
        md.appendMarkdown(`**Status**: ${step.summary}\n\n`);
        md.appendMarkdown(`${step.detail}\n`);
        if (step.actionCommand && step.actionLabel) {
          md.appendMarkdown(`\n[$(play) ${step.actionLabel}](command:${step.actionCommand})`);
        }

        let contextValue = `axStep_${step.id}_${step.state}`;
        if (step.id === 'substrate') {
          contextValue =
            readiness.localStatus?.state === 'Running' ? 'axLocalRunning' : 'axLocalStopped';
        }

        items.push(
          new AxTreeItem({
            label: step.title,
            description: step.summary,
            tooltip: md,
            contextValue,
            icon: this.stepIcon(step),
            command: step.actionCommand
              ? {
                  command: step.actionCommand,
                  title: step.actionLabel || step.title,
                }
              : undefined,
          })
        );
      }
      return items;
    } catch (err) {
      return [
        this.infoItem(
          'Unable to inspect setup readiness',
          (err as Error).message,
          'warning',
          { command: 'ax.guidedSetup', title: 'Run Guided Setup' }
        ),
      ];
    }
  }

  private async isClusterProbablyReady(): Promise<boolean> {
    const r = this.lastReadiness ?? (await this.getReadiness().catch(() => null));
    if (!r) {
      return false;
    }
    this.lastReadiness = r;
    if (r.allCoreReady) {
      return true;
    }
    if (!r.activeContext || r.activeContext === 'no-context') {
      return false;
    }
    if (r.activeContext === 'ax-local' && r.localStatus?.state !== 'Running') {
      return false;
    }
    return true;
  }

  private unreadyClusterItem(): AxTreeItem {
    return this.infoItem(
      'AX cluster not set up yet',
      'Click to run Guided Setup',
      'tools',
      { command: 'ax.guidedSetup', title: 'Run Guided Setup' }
    );
  }

  private async getContextChildren(): Promise<AxTreeItem[]> {
    try {
      const contexts: ContextEntry[] = await this.cli.listContexts();
      if (contexts.length === 0) {
        return [this.infoItem('No kubeconfig contexts found')];
      }
      return contexts.map(
        (c) =>
          new AxTreeItem({
            label: c.name,
            description: c.current
              ? `active${c.tunnel !== '-' ? ` · ${c.tunnel}` : ''}`
              : c.tunnel !== '-'
                ? c.tunnel
                : c.cluster,
            tooltip: `Cluster: ${c.cluster}\nTunnel: ${c.tunnel}`,
            contextValue: 'axContext',
            contextName: c.name,
            icon: new vscode.ThemeIcon(
              c.current ? 'check' : 'circle-large-outline',
              c.current ? new vscode.ThemeColor('testing.iconPassed') : undefined
            ),
          })
      );
    } catch (err) {
      return [this.infoItem('Unable to list contexts', (err as Error).message, 'warning')];
    }
  }

  private async getTunnelChildren(): Promise<AxTreeItem[]> {
    try {
      const tunnels: TunnelEntry[] = await this.cli.listTunnels();
      if (tunnels.length === 0) {
        return [this.infoItem('No active background tunnels')];
      }
      return tunnels.map(
        (t) =>
          new AxTreeItem({
            label: t.context,
            description: `127.0.0.1:${t.localPort} -> ${t.remoteService} (${t.status})`,
            tooltip: `PID: ${t.pid}\nCreated: ${t.created}`,
            contextValue: 'axTunnel',
            contextName: t.context,
            icon: new vscode.ThemeIcon(t.status === 'Active' ? 'plug' : 'warning'),
          })
      );
    } catch (err) {
      return [this.infoItem('Unable to list tunnels', (err as Error).message, 'warning')];
    }
  }

  private async getTaskChildren(): Promise<AxTreeItem[]> {
    if (!(await this.isClusterProbablyReady())) {
      return [this.unreadyClusterItem()];
    }
    try {
      const tasks: TaskSummary[] = await this.cli.listTasks();
      if (tasks.length === 0) {
        return [
          this.infoItem('No tasks in atespace', 'Click + to create manifest', 'add', {
            command: 'ax.newTaskManifest',
            title: 'New Task & Workspace Manifest',
          }),
        ];
      }
      return tasks.map((t) => {
        const lower = t.state.toLowerCase();
        const isWorking = lower === 'working';
        const isCompleted = lower === 'completed';
        const isSuspended = lower === 'suspended';
        const isFailed = lower.includes('fail') || lower.includes('error');

        let contextValue = 'axTaskOther';
        let icon = new vscode.ThemeIcon('loading~spin');
        if (isWorking) {
          contextValue = 'axTaskWorking';
          icon = new vscode.ThemeIcon(
            'play-circle',
            new vscode.ThemeColor('testing.iconPassed')
          );
        } else if (isCompleted) {
          contextValue = 'axTaskCompleted';
          icon = new vscode.ThemeIcon(
            'pass-filled',
            new vscode.ThemeColor('testing.iconPassed')
          );
        } else if (isSuspended) {
          contextValue = 'axTaskSuspended';
          icon = new vscode.ThemeIcon('debug-pause');
        } else if (isFailed) {
          contextValue = 'axTaskFailed';
          icon = new vscode.ThemeIcon('error', new vscode.ThemeColor('testing.iconFailed'));
        }

        const item = new AxTreeItem({
          label: t.name,
          description: `${t.state} · ${t.age}`,
          tooltip: `Task: ${t.atespace}/${t.name}\nState: ${t.state}\nActor: ${t.actor}\nAge: ${t.age}`,
          contextValue,
          icon,
          resourceKind: 'task',
          resourceName: t.name,
          atespace: t.atespace,
        });
        item.command = {
          command: isCompleted ? 'ax.viewTaskOutput' : 'ax.describeResource',
          title: isCompleted ? 'View Task Output' : 'Describe Task',
          arguments: [item],
        };
        return item;
      });
    } catch (err) {
      return [
        this.infoItem('Cluster unreachable', (err as Error).message, 'warning', {
          command: 'ax.guidedSetup',
          title: 'Run Guided Setup',
        }),
      ];
    }
  }

  private async getWorkspaceChildren(): Promise<AxTreeItem[]> {
    if (!(await this.isClusterProbablyReady())) {
      return [this.unreadyClusterItem()];
    }
    try {
      const workspaces: WorkspaceSummary[] = await this.cli.listWorkspaces();
      if (workspaces.length === 0) {
        return [this.infoItem('No workspaces in atespace')];
      }
      return workspaces.map((ws) => {
        const ready = ws.phase.toLowerCase() === 'ready';
        const item = new AxTreeItem({
          label: ws.name,
          description: `${ws.phase} · ${ws.gitRepos} git · ${ws.mcpServers} mcp`,
          tooltip: `Workspace: ${ws.atespace}/${ws.name}\nPhase: ${ws.phase}`,
          contextValue: 'axWorkspace',
          icon: new vscode.ThemeIcon(
            'folder-library',
            ready ? new vscode.ThemeColor('testing.iconPassed') : undefined
          ),
          resourceKind: 'workspace',
          resourceName: ws.name,
          atespace: ws.atespace,
        });
        item.command = {
          command: 'ax.describeResource',
          title: 'Describe Workspace',
          arguments: [item],
        };
        return item;
      });
    } catch (err) {
      return [this.infoItem('Cluster unreachable', (err as Error).message, 'warning')];
    }
  }

  private async getGatewayChildren(): Promise<AxTreeItem[]> {
    if (!(await this.isClusterProbablyReady())) {
      return [this.unreadyClusterItem()];
    }
    try {
      const gateways: GatewaySummary[] = await this.cli.listGateways();
      if (gateways.length === 0) {
        return [this.infoItem('No gateways in atespace')];
      }
      return gateways.map((gw) => {
        const item = new AxTreeItem({
          label: gw.name,
          description: `${gw.egressRules} egress rule(s)`,
          tooltip: `Gateway: ${gw.atespace}/${gw.name}`,
          contextValue: 'axGateway',
          icon: new vscode.ThemeIcon('shield'),
          resourceKind: 'gateway',
          resourceName: gw.name,
          atespace: gw.atespace,
        });
        item.command = {
          command: 'ax.describeResource',
          title: 'Describe Gateway',
          arguments: [item],
        };
        return item;
      });
    } catch (err) {
      return [this.infoItem('Cluster unreachable', (err as Error).message, 'warning')];
    }
  }

  private async getModelChildren(): Promise<AxTreeItem[]> {
    if (!(await this.isClusterProbablyReady())) {
      return [this.unreadyClusterItem()];
    }
    try {
      const models: ModelSummary[] = await this.cli.listModels();
      if (models.length === 0) {
        return [this.infoItem('No models in atespace')];
      }
      return models.map((m) => {
        const item = new AxTreeItem({
          label: m.name,
          description: `${m.provider} / ${m.model}`,
          tooltip: `Model: ${m.atespace}/${m.name} (${m.provider}: ${m.model})`,
          contextValue: 'axModel',
          icon: new vscode.ThemeIcon('hubot'),
          resourceKind: 'model',
          resourceName: m.name,
          atespace: m.atespace,
        });
        item.command = {
          command: 'ax.describeResource',
          title: 'Describe Model',
          arguments: [item],
        };
        return item;
      });
    } catch (err) {
      return [this.infoItem('Cluster unreachable', (err as Error).message, 'warning')];
    }
  }
}

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
  LocalStatusReport,
  ModelSummary,
  TaskSummary,
  TunnelEntry,
  WorkspaceSummary,
} from './axCli';

export type AxTreeGroup =
  | 'local'
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

  constructor(
    private readonly cli: AxCli,
    private readonly getDefaultAtespace: () => string
  ) {}

  public refresh(): void {
    this._onDidChangeTreeData.fire();
  }

  public getTreeItem(element: AxTreeItem): vscode.TreeItem {
    return element;
  }

  public async getChildren(element?: AxTreeItem): Promise<AxTreeItem[]> {
    if (!element) {
      const atespace = this.getDefaultAtespace();
      return [
        new AxTreeItem({
          label: 'Local Sandbox Stack',
          collapsibleState: vscode.TreeItemCollapsibleState.Expanded,
          icon: new vscode.ThemeIcon('vm'),
          group: 'local',
        }),
        new AxTreeItem({
          label: 'Kubernetes Contexts',
          collapsibleState: vscode.TreeItemCollapsibleState.Collapsed,
          icon: new vscode.ThemeIcon('server-environment'),
          group: 'contexts',
        }),
        new AxTreeItem({
          label: `Tasks (${atespace})`,
          collapsibleState: vscode.TreeItemCollapsibleState.Expanded,
          icon: new vscode.ThemeIcon('rocket'),
          group: 'tasks',
        }),
        new AxTreeItem({
          label: `Workspaces (${atespace})`,
          collapsibleState: vscode.TreeItemCollapsibleState.Collapsed,
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
          label: 'Background Tunnels',
          collapsibleState: vscode.TreeItemCollapsibleState.Collapsed,
          icon: new vscode.ThemeIcon('plug'),
          group: 'tunnels',
        }),
      ];
    }

    switch (element.group) {
      case 'local':
        return this.getLocalChildren();
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

  private infoItem(label: string, description?: string, iconName: string = 'info'): AxTreeItem {
    return new AxTreeItem({
      label,
      description,
      icon: new vscode.ThemeIcon(iconName),
    });
  }

  private async getLocalChildren(): Promise<AxTreeItem[]> {
    try {
      const st: LocalStatusReport = await this.cli.localStatus();
      const isRunning = st.state === 'Running';
      const memGiB = st.host.memTotalBytes
        ? `${(st.host.memTotalBytes / (1024 ** 3)).toFixed(1)} GiB`
        : 'N/A';

      const md = new vscode.MarkdownString();
      md.appendMarkdown(`### Local AX Stack (\`${st.name}\`)\n\n`);
      md.appendMarkdown(`- **State**: \`${st.state}\`\n`);
      md.appendMarkdown(`- **Substrate**: \`${st.substrateStatus}\`\n`);
      md.appendMarkdown(`- **AX Server**: \`${st.axStatus}\`\n`);
      md.appendMarkdown(
        `- **Host Runtime**: \`${st.host.runtime || 'none'}\` (kernel \`${st.host.kernelVersion || 'N/A'}\`, RAM \`${memGiB}\`)\n`
      );
      if (st.host.error) {
        md.appendMarkdown(`- **Host Preflight Error**: ${st.host.error}\n`);
      }

      const items: AxTreeItem[] = [
        new AxTreeItem({
          label: `Cluster: ${st.name}`,
          description: st.state,
          tooltip: md,
          contextValue: isRunning ? 'axLocalRunning' : 'axLocalStopped',
          icon: new vscode.ThemeIcon(
            isRunning ? 'pass-filled' : 'circle-outline',
            isRunning ? new vscode.ThemeColor('testing.iconPassed') : undefined
          ),
          command: {
            command: 'ax.localStatus',
            title: 'Show Local Status',
          },
        }),
      ];

      if (st.host.error) {
        items.push(this.infoItem('Host Check', st.host.error, 'warning'));
      } else if (st.host.runtime) {
        items.push(
          this.infoItem(
            `Runtime: ${st.host.runtime}`,
            `Linux ${st.host.kernelVersion} · ${memGiB}`,
            'server-process'
          )
        );
      }
      return items;
    } catch (err) {
      return [this.infoItem('Unable to query local status', (err as Error).message, 'warning')];
    }
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
    try {
      const tasks: TaskSummary[] = await this.cli.listTasks();
      if (tasks.length === 0) {
        return [this.infoItem('No tasks in atespace')];
      }
      return tasks.map((t) => {
        const lower = t.state.toLowerCase();
        const isWorking = lower === 'working';
        const isSuspended = lower === 'suspended';
        const isFailed = lower.includes('fail') || lower.includes('error');

        let contextValue = 'axTaskOther';
        let icon = new vscode.ThemeIcon('Loading~spin');
        if (isWorking) {
          contextValue = 'axTaskWorking';
          icon = new vscode.ThemeIcon(
            'play-circle',
            new vscode.ThemeColor('testing.iconPassed')
          );
        } else if (isSuspended) {
          contextValue = 'axTaskSuspended';
          icon = new vscode.ThemeIcon('debug-pause');
        } else if (isFailed) {
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
          command: 'ax.describeResource',
          title: 'Describe Task',
          arguments: [item],
        };
        return item;
      });
    } catch (err) {
      return [this.infoItem('Cluster unreachable', (err as Error).message, 'warning')];
    }
  }

  private async getWorkspaceChildren(): Promise<AxTreeItem[]> {
    try {
      const workspaces: WorkspaceSummary[] = await this.cli.listWorkspaces();
      if (workspaces.length === 0) {
        return [this.infoItem('No workspaces in atespace')];
      }
      return workspaces.map((ws) => {
        const item = new AxTreeItem({
          label: ws.name,
          description: `${ws.phase} · ${ws.gitRepos} git · ${ws.mcpServers} mcp`,
          tooltip: `Workspace: ${ws.atespace}/${ws.name}\nPhase: ${ws.phase}`,
          contextValue: 'axWorkspace',
          icon: new vscode.ThemeIcon('folder-library'),
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

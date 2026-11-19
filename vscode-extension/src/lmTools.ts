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
import { AxCli, AxResourceKind } from './axCli';

export interface AxStatusToolInput {
  localClusterName?: string;
}

export interface AxListResourcesToolInput {
  kind: 'tasks' | 'workspaces' | 'gateways' | 'models';
  atespace?: string;
}

export interface AxManageTaskToolInput {
  action: 'apply' | 'resume' | 'suspend' | 'describe' | 'delete';
  manifestYaml?: string;
  kind?: AxResourceKind;
  name?: string;
  atespace?: string;
}

export interface AxSshExecToolInput {
  taskName: string;
  command: string;
  atespace?: string;
}

export interface AxToolHandlers {
  status(input: AxStatusToolInput): Promise<string>;
  listResources(input: AxListResourcesToolInput): Promise<string>;
  manageTask(input: AxManageTaskToolInput): Promise<string>;
  sshExec(input: AxSshExecToolInput): Promise<string>;
}

export function createAxToolHandlers(cli: AxCli, onChanged?: () => void): AxToolHandlers {
  return {
    async status(input: AxStatusToolInput): Promise<string> {
      const [ver, local, contexts] = await Promise.all([
        cli.version().catch((e) => `unavailable (${(e as Error).message})`),
        cli.localStatus(input.localClusterName).catch((e) => ({
          error: (e as Error).message,
        })),
        cli.listContexts().catch(() => []),
      ]);
      const activeContext = contexts.find((c) => c.current)?.name ?? null;
      return JSON.stringify(
        {
          axVersion: ver,
          activeContext,
          contexts,
          localStack: local,
        },
        null,
        2
      );
    },

    async listResources(input: AxListResourcesToolInput): Promise<string> {
      switch (input.kind) {
        case 'tasks':
          return JSON.stringify(await cli.listTasks(input.atespace), null, 2);
        case 'workspaces':
          return JSON.stringify(await cli.listWorkspaces(input.atespace), null, 2);
        case 'gateways':
          return JSON.stringify(await cli.listGateways(input.atespace), null, 2);
        case 'models':
          return JSON.stringify(await cli.listModels(input.atespace), null, 2);
        default:
          throw new Error(`Unsupported resource collection "${String(input.kind)}"`);
      }
    },

    async manageTask(input: AxManageTaskToolInput): Promise<string> {
      let out: string;
      switch (input.action) {
        case 'apply': {
          if (!input.manifestYaml?.trim()) {
            throw new Error('`manifestYaml` is required when `action` is `apply`.');
          }
          out = await cli.applyManifestYaml(input.manifestYaml, input.atespace);
          onChanged?.();
          return `${out}\n\nNote: Newly created Tasks start in STATE: Suspended. Call resume to start execution.`;
        }
        case 'resume': {
          if (!input.name?.trim()) {
            throw new Error('`name` is required when `action` is `resume`.');
          }
          out = await cli.resumeTask(input.name.trim(), input.atespace);
          onChanged?.();
          return out;
        }
        case 'suspend': {
          if (!input.name?.trim()) {
            throw new Error('`name` is required when `action` is `suspend`.');
          }
          out = await cli.suspendTask(input.name.trim(), input.atespace);
          onChanged?.();
          return out;
        }
        case 'describe': {
          if (!input.name?.trim()) {
            throw new Error('`name` is required when `action` is `describe`.');
          }
          const kind: AxResourceKind = input.kind ?? 'task';
          return await cli.describeResource(kind, input.name.trim(), input.atespace);
        }
        case 'delete': {
          if (!input.name?.trim()) {
            throw new Error('`name` is required when `action` is `delete`.');
          }
          const kind: AxResourceKind = input.kind ?? 'task';
          out = await cli.deleteResource(kind, input.name.trim(), input.atespace);
          onChanged?.();
          return out;
        }
        default:
          throw new Error(`Unsupported action "${String(input.action)}"`);
      }
    },

    async sshExec(input: AxSshExecToolInput): Promise<string> {
      if (!input.taskName?.trim() || !input.command?.trim()) {
        throw new Error('Both `taskName` and `command` are required.');
      }
      const res = await cli.execInTask(
        input.taskName.trim(),
        input.command,
        input.atespace
      );
      return JSON.stringify(
        {
          exitCode: res.exitCode,
          stdout: res.stdout,
          stderr: res.stderr,
        },
        null,
        2
      );
    },
  };
}

export function registerAxLanguageModelTools(
  context: vscode.ExtensionContext,
  handlers: AxToolHandlers,
  log: (msg: string) => void
): number {
  const lmApi = (vscode as unknown as { lm?: typeof vscode.lm }).lm;
  if (!lmApi || typeof lmApi.registerTool !== 'function') {
    log('vscode.lm.registerTool is not available in this editor host; relying on SKILL.md + CLI.');
    return 0;
  }

  const wrapTool = <T>(fn: (input: T) => Promise<string>): vscode.LanguageModelTool<T> => ({
    async invoke(
      options: vscode.LanguageModelToolInvocationOptions<T>,
      _token: vscode.CancellationToken
    ): Promise<vscode.LanguageModelToolResult> {
      const text = await fn(options.input);
      return new vscode.LanguageModelToolResult([new vscode.LanguageModelTextPart(text)]);
    },
  });

  const registrations = [
    lmApi.registerTool<AxStatusToolInput>('ax_status', wrapTool((i) => handlers.status(i))),
    lmApi.registerTool<AxListResourcesToolInput>(
      'ax_list_resources',
      wrapTool((i) => handlers.listResources(i))
    ),
    lmApi.registerTool<AxManageTaskToolInput>(
      'ax_manage_task',
      wrapTool((i) => handlers.manageTask(i))
    ),
    lmApi.registerTool<AxSshExecToolInput>('ax_ssh_exec', wrapTool((i) => handlers.sshExec(i))),
  ];

  context.subscriptions.push(...registrations);
  log(`Registered ${registrations.length} AX Language Model Tools with VS Code.`);
  return registrations.length;
}

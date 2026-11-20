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

import { spawn } from 'child_process';
import * as path from 'path';

export interface LocalHostReport {
  runtime: string;
  kernelVersion: string;
  memTotalBytes: number;
  rootless: boolean;
  error: string;
  warning: string;
}

export interface LocalStatusReport {
  name: string;
  backend: string;
  state: string;
  kubeContext: string;
  nodes: string[];
  substrateStatus: string;
  axStatus: string;
  host: LocalHostReport;
}

export interface ContextEntry {
  current: boolean;
  name: string;
  cluster: string;
  tunnel: string;
}

export interface TunnelEntry {
  context: string;
  localPort: number;
  remoteService: string;
  pid: number;
  status: string;
  created: string;
}

export interface TaskSummary {
  name: string;
  atespace: string;
  state: string;
  actor: string;
  age: string;
}

export interface WorkspaceSummary {
  name: string;
  atespace: string;
  phase: string;
  gitRepos: number;
  mcpServers: number;
}

export interface GatewaySummary {
  name: string;
  atespace: string;
  egressRules: number;
}

export interface ModelSummary {
  name: string;
  atespace: string;
  provider: string;
  model: string;
}

export type AxResourceKind = 'task' | 'workspace' | 'gateway' | 'model';

export interface AxCliConfig {
  axBinaryPath: string;
  installDir?: string;
  atespace?: string;
  namespace?: string;
  context?: string;
  kubeconfig?: string;
  server?: string;
  serverImage?: string;
  defaultTaskImage?: string;
  runnerVolumeImage?: string;
  timeoutMs?: number;
}

export interface ExecOutput {
  stdout: string;
  stderr: string;
  exitCode: number;
}

export interface ManifestDiagnostic {
  line: number;
  severity: 'error' | 'warning';
  message: string;
}

const RFC1123_LABEL_RE = /^[a-z0-9]([-a-z0-9]*[a-z0-9])?$/;
const SUPPORTED_KINDS = new Set(['Task', 'Workspace', 'Gateway', 'Model']);
const GLOBAL_VALUE_FLAGS = new Set(['-a', '--atespace', '-n', '--namespace', '--context', '--server']);

export function summarizeAxSubcommand(args: string[]): string {
  const tokens: string[] = [];
  for (let i = 0; i < args.length; i++) {
    const tok = args[i];
    if (GLOBAL_VALUE_FLAGS.has(tok)) {
      i++; // skip value argument
      continue;
    }
    if (tok.startsWith('-')) {
      continue;
    }
    tokens.push(tok);
    if (tokens.length >= 2) {
      break;
    }
  }
  return tokens.join(' ') || args[0] || '';
}

export function splitTableRow(line: string): string[] {
  return line
    .trim()
    .split(/\s{2,}/)
    .map((c) => c.trim())
    .filter((c) => c.length > 0);
}

export function parseTasksTable(output: string): TaskSummary[] {
  const lines = output.split(/\r?\n/).filter((l) => l.trim().length > 0);
  const results: TaskSummary[] = [];
  for (let i = 0; i < lines.length; i++) {
    const cols = splitTableRow(lines[i]);
    if (i === 0 && cols[0] === 'NAME') {
      continue;
    }
    if (cols.length >= 5) {
      results.push({
        name: cols[0],
        atespace: cols[1],
        state: cols[2],
        actor: cols[3],
        age: cols[4],
      });
    }
  }
  return results;
}

export function parseWorkspacesTable(output: string): WorkspaceSummary[] {
  const lines = output.split(/\r?\n/).filter((l) => l.trim().length > 0);
  const results: WorkspaceSummary[] = [];
  for (let i = 0; i < lines.length; i++) {
    const cols = splitTableRow(lines[i]);
    if (i === 0 && cols[0] === 'NAME') {
      continue;
    }
    if (cols.length >= 5) {
      results.push({
        name: cols[0],
        atespace: cols[1],
        phase: cols[2],
        gitRepos: Number.parseInt(cols[3], 10) || 0,
        mcpServers: Number.parseInt(cols[4], 10) || 0,
      });
    }
  }
  return results;
}

export function parseGatewaysTable(output: string): GatewaySummary[] {
  const lines = output.split(/\r?\n/).filter((l) => l.trim().length > 0);
  const results: GatewaySummary[] = [];
  for (let i = 0; i < lines.length; i++) {
    const cols = splitTableRow(lines[i]);
    if (i === 0 && cols[0] === 'NAME') {
      continue;
    }
    if (cols.length >= 3) {
      results.push({
        name: cols[0],
        atespace: cols[1],
        egressRules: Number.parseInt(cols[2], 10) || 0,
      });
    }
  }
  return results;
}

export function parseModelsTable(output: string): ModelSummary[] {
  const lines = output.split(/\r?\n/).filter((l) => l.trim().length > 0);
  const results: ModelSummary[] = [];
  for (let i = 0; i < lines.length; i++) {
    const cols = splitTableRow(lines[i]);
    if (i === 0 && cols[0] === 'NAME') {
      continue;
    }
    if (cols.length >= 4) {
      results.push({
        name: cols[0],
        atespace: cols[1],
        provider: cols[2],
        model: cols[3],
      });
    }
  }
  return results;
}

export function parseContextsTable(output: string): ContextEntry[] {
  const lines = output.split(/\r?\n/).filter((l) => l.trim().length > 0);
  const results: ContextEntry[] = [];
  for (let i = 0; i < lines.length; i++) {
    const raw = lines[i];
    if (i === 0 && raw.includes('CURRENT') && raw.includes('NAME')) {
      continue;
    }
    const trimmed = raw.trim();
    const isCurrent = trimmed.startsWith('*');
    const rest = isCurrent ? trimmed.slice(1).trim() : trimmed;
    const tokens = rest.split(/\s+/).filter(Boolean);
    if (tokens.length >= 3) {
      results.push({
        current: isCurrent,
        name: tokens[0],
        cluster: tokens[1],
        tunnel: tokens.slice(2).join(' '),
      });
    }
  }
  return results;
}

export function parseTunnelsTable(output: string): TunnelEntry[] {
  if (output.includes('No active tunnels found')) {
    return [];
  }
  const lines = output.split(/\r?\n/).filter((l) => l.trim().length > 0);
  const results: TunnelEntry[] = [];
  for (let i = 0; i < lines.length; i++) {
    const raw = lines[i].trim();
    if (i === 0 && raw.startsWith('CONTEXT')) {
      continue;
    }
    const tokens = raw.split(/\s+/).filter(Boolean);
    if (tokens.length >= 6) {
      results.push({
        context: tokens[0],
        localPort: Number.parseInt(tokens[1], 10) || 0,
        remoteService: tokens[2],
        pid: Number.parseInt(tokens[3], 10) || 0,
        status: tokens[4],
        created: tokens.slice(5).join(' '),
      });
    }
  }
  return results;
}

/**
 * Extracts `Output:` block from `ax describe task <name>` output when present.
 */
export function extractDescribeOutputSection(describeText: string): string | null {
  const lines = describeText.split(/\r?\n/);
  const outIdx = lines.findIndex((l) => l === 'Output:');
  if (outIdx < 0) {
    return null;
  }
  const collected: string[] = [];
  for (let i = outIdx + 1; i < lines.length; i++) {
    const line = lines[i];
    if (line === '' && lines[i + 1] === 'Conditions:') {
      break;
    }
    if (/^[A-Z][A-Za-z ]+:$/.test(line) && !line.startsWith('  ')) {
      break;
    }
    collected.push(line.startsWith('  ') ? line.slice(2) : line);
  }
  return collected.join('\n').trim() || null;
}

/**
 * Lightweight built-in linter for multi-document `ax.io/v1alpha1` YAML files so users
 * and agents receive immediate editor feedback even without external YAML extensions.
 * Tracks multiline YAML block scalars (`|` / `>`) so inline scripts/files inside
 * `Workspace.spec.setup.files[].content: |` never trigger false-positive diagnostics.
 */
export function validateAxManifestText(text: string): ManifestDiagnostic[] {
  if (!text.includes('ax.io/v1alpha1')) {
    return [];
  }

  const diagnostics: ManifestDiagnostic[] = [];
  const lines = text.split(/\r?\n/);

  let docStartLine = 0;
  let inAxDoc = false;
  let docKind: string | null = null;
  let inMetadata = false;
  let metadataIndent = -1;
  let blockScalarParentIndent = -1;

  let inSetup = false;
  let setupIndent = -1;
  let setupGitLine = -1;
  let hasSetupGateway = false;

  const flushDoc = (endLine: number) => {
    if (inAxDoc && !docKind) {
      diagnostics.push({
        line: docStartLine,
        severity: 'error',
        message: 'AX manifest is missing required `kind` (Task, Workspace, Gateway, or Model).',
      });
    }
    if (inAxDoc && docKind === 'Workspace' && setupGitLine >= 0 && !hasSetupGateway) {
      diagnostics.push({
        line: setupGitLine,
        severity: 'warning',
        message:
          'Workspace `spec.setup.git` clones over HTTPS during pre-warming; set `spec.setup.gateway` so the setup actor has egress to the Git host.',
      });
    }
    inAxDoc = false;
    docKind = null;
    inMetadata = false;
    metadataIndent = -1;
    blockScalarParentIndent = -1;
    inSetup = false;
    setupIndent = -1;
    setupGitLine = -1;
    hasSetupGateway = false;
    docStartLine = endLine;
  };

  for (let idx = 0; idx < lines.length; idx++) {
    const raw = lines[idx];
    if (!raw.trim()) {
      continue;
    }
    const indent = raw.search(/\S/);

    // Skip lines inside a YAML literal/folded block scalar (`|` or `>`)
    if (blockScalarParentIndent >= 0) {
      if (indent > blockScalarParentIndent) {
        continue;
      }
      blockScalarParentIndent = -1;
    }

    const codeLine = raw.replace(/#.*$/, '');
    const trimmed = codeLine.trim();
    if (!trimmed) {
      continue;
    }
    if (trimmed === '---') {
      flushDoc(idx + 1);
      continue;
    }

    // Detect start of a YAML block scalar (`key: |`, `key: |-`, `key: >`, `- |`, etc.)
    if (/:\s*[|>][-+]?\d*\s*$/.test(trimmed) || /^-\s+[|>][-+]?\d*\s*$/.test(trimmed)) {
      blockScalarParentIndent = indent;
      continue;
    }

    const apiMatch = trimmed.match(/^apiVersion:\s*["']?([^"'\s]+)["']?$/);
    if (apiMatch && indent === 0) {
      if (apiMatch[1] === 'ax.io/v1alpha1') {
        inAxDoc = true;
        docStartLine = idx;
      }
      continue;
    }

    const kindMatch = trimmed.match(/^kind:\s*["']?([^"'\s]+)["']?$/);
    if (kindMatch && indent === 0) {
      docKind = kindMatch[1];
      if (inAxDoc && !SUPPORTED_KINDS.has(docKind)) {
        diagnostics.push({
          line: idx,
          severity: 'error',
          message: `Unsupported AX kind "${docKind}" (expected Task, Workspace, Gateway, or Model).`,
        });
      }
      continue;
    }

    if (!inAxDoc) {
      continue;
    }

    if (trimmed === 'metadata:' && indent === 0) {
      inMetadata = true;
      metadataIndent = indent;
      continue;
    }
    if (inMetadata && indent <= metadataIndent) {
      inMetadata = false;
    }

    if (inMetadata) {
      const metaField = trimmed.match(/^(name|atespace):\s*["']?([^"'\s]+)["']?$/);
      if (metaField) {
        const fieldName = metaField[1];
        const val = metaField[2];
        if (val.length > 63 || !RFC1123_LABEL_RE.test(val)) {
          diagnostics.push({
            line: idx,
            severity: 'error',
            message: `metadata.${fieldName} "${val}" must be a lowercase RFC 1123 label (<= 63 lowercase alphanumeric chars or '-', starting and ending with alphanumeric).`,
          });
        }
      }
    }

    if (docKind === 'Workspace') {
      if (trimmed === 'setup:' && indent > 0) {
        inSetup = true;
        setupIndent = indent;
        continue;
      }
      if (inSetup && indent <= setupIndent) {
        inSetup = false;
      }
      if (inSetup) {
        if (trimmed === 'git:' && setupGitLine < 0) {
          setupGitLine = idx;
        }
        if (trimmed === 'gateway:' || trimmed.startsWith('gateway:')) {
          hasSetupGateway = true;
        }
      }
    }

    const imageMatch = trimmed.match(/^image:\s*["']?([^"'\s]+)["']?$/);
    if (imageMatch) {
      const img = imageMatch[1];
      if (!img.includes('@sha256:')) {
        diagnostics.push({
          line: idx,
          severity: 'error',
          message: `Agent Substrate rejects tag-only image "${img}". Pin spec.profile.image by digest (@sha256:...) or omit spec.profile to use ax-server's default image.`,
        });
      }
    }

    if (/^requests:\s*$/.test(trimmed)) {
      diagnostics.push({
        line: idx,
        severity: 'error',
        message:
          'Agent Substrate sizes sandboxes by `resources.limits` only; `resources.requests` is rejected by `ax apply`.',
      });
    }
  }

  flushDoc(lines.length);
  return diagnostics;
}

export class AxCli {
  private readonly getConfig: () => AxCliConfig;

  constructor(configOrProvider: AxCliConfig | (() => AxCliConfig)) {
    this.getConfig =
      typeof configOrProvider === 'function' ? configOrProvider : () => configOrProvider;
  }

  public buildEnv(): NodeJS.ProcessEnv {
    const cfg = this.getConfig();
    const env: NodeJS.ProcessEnv = { ...process.env };
    if (cfg.installDir) {
      env.PATH = `${cfg.installDir}${path.delimiter}${env.PATH || ''}`;
    }
    if (cfg.kubeconfig && cfg.kubeconfig.trim()) {
      env.KUBECONFIG = cfg.kubeconfig.trim();
    }
    if (cfg.serverImage?.trim()) {
      env.AX_SERVER_IMAGE = cfg.serverImage.trim();
    }
    if (cfg.defaultTaskImage?.trim()) {
      env.AX_DEFAULT_TASK_IMAGE = cfg.defaultTaskImage.trim();
    }
    if (cfg.runnerVolumeImage?.trim()) {
      env.AX_RUNNER_VOLUME_IMAGE = cfg.runnerVolumeImage.trim();
    }
    return env;
  }

  private buildGlobalFlags(overrides?: { atespace?: string; context?: string }): string[] {
    const cfg = this.getConfig();
    const flags: string[] = [];
    const atespace = (overrides?.atespace ?? cfg.atespace ?? '').trim();
    if (atespace) {
      flags.push('-a', atespace);
    }
    const ns = (cfg.namespace ?? '').trim();
    if (ns) {
      flags.push('-n', ns);
    }
    const ctx = (overrides?.context ?? cfg.context ?? '').trim();
    if (ctx) {
      flags.push('--context', ctx);
    }
    const srv = (cfg.server ?? '').trim();
    if (srv) {
      flags.push('--server', srv);
    }
    return flags;
  }

  public async execRaw(
    args: string[],
    options?: { stdin?: string; timeoutMs?: number }
  ): Promise<ExecOutput> {
    const cfg = this.getConfig();
    const timeoutMs = options?.timeoutMs ?? cfg.timeoutMs ?? 60_000;

    return new Promise<ExecOutput>((resolve, reject) => {
      const child = spawn(cfg.axBinaryPath, args, {
        env: this.buildEnv(),
        stdio: 'pipe',
      });

      let stdout = '';
      let stderr = '';
      let settled = false;

      const timer = setTimeout(() => {
        if (!settled) {
          settled = true;
          child.kill('SIGTERM');
          reject(
            new Error(
              `ax ${summarizeAxSubcommand(args)} timed out after ${Math.round(timeoutMs / 1000)}s`
            )
          );
        }
      }, timeoutMs);

      child.stdout.on('data', (chunk: Buffer) => {
        stdout += chunk.toString('utf8');
      });
      child.stderr.on('data', (chunk: Buffer) => {
        stderr += chunk.toString('utf8');
      });
      child.on('error', (err) => {
        if (!settled) {
          settled = true;
          clearTimeout(timer);
          reject(err);
        }
      });
      child.on('close', (code) => {
        if (!settled) {
          settled = true;
          clearTimeout(timer);
          resolve({
            stdout,
            stderr,
            exitCode: code ?? 1,
          });
        }
      });

      if (options?.stdin !== undefined) {
        child.stdin.write(options.stdin, 'utf8');
      }
      child.stdin.end();
    });
  }

  private async execChecked(
    args: string[],
    options?: { stdin?: string; timeoutMs?: number }
  ): Promise<string> {
    const res = await this.execRaw(args, options);
    if (res.exitCode !== 0) {
      const detail = (res.stderr || res.stdout || `exit code ${res.exitCode}`).trim();
      throw new Error(`ax ${summarizeAxSubcommand(args)} failed: ${detail}`);
    }
    return res.stdout;
  }

  public async version(): Promise<string> {
    const out = await this.execChecked(['version'], { timeoutMs: 10_000 });
    return out.trim();
  }

  public async localStatus(clusterName?: string): Promise<LocalStatusReport> {
    const cfg = this.getConfig();
    const args: string[] = [];
    if (cfg.namespace?.trim()) {
      args.push('-n', cfg.namespace.trim());
    }
    args.push('local', 'status', '-o', 'json');
    if (clusterName?.trim()) {
      args.push('--name', clusterName.trim());
    }
    const out = await this.execChecked(args, { timeoutMs: 25_000 });
    return JSON.parse(out) as LocalStatusReport;
  }

  public async listContexts(): Promise<ContextEntry[]> {
    const out = await this.execChecked(['ctx'], { timeoutMs: 15_000 });
    return parseContextsTable(out);
  }

  public async switchContext(contextName: string): Promise<string> {
    const out = await this.execChecked(['ctx', contextName], { timeoutMs: 15_000 });
    return out.trim();
  }

  public async listTunnels(): Promise<TunnelEntry[]> {
    const out = await this.execChecked(['tunnel', 'list'], { timeoutMs: 15_000 });
    return parseTunnelsTable(out);
  }

  public async stopTunnel(contextName?: string): Promise<string> {
    const args = ['tunnel', 'stop'];
    if (contextName?.trim()) {
      args.push(contextName.trim());
    }
    const out = await this.execChecked(args, { timeoutMs: 15_000 });
    return out.trim();
  }

  public async listTasks(atespace?: string): Promise<TaskSummary[]> {
    const args = [...this.buildGlobalFlags({ atespace }), 'get', 'tasks'];
    const out = await this.execChecked(args, { timeoutMs: 25_000 });
    return parseTasksTable(out);
  }

  public async listWorkspaces(atespace?: string): Promise<WorkspaceSummary[]> {
    const args = [...this.buildGlobalFlags({ atespace }), 'get', 'workspaces'];
    const out = await this.execChecked(args, { timeoutMs: 25_000 });
    return parseWorkspacesTable(out);
  }

  public async listGateways(atespace?: string): Promise<GatewaySummary[]> {
    const args = [...this.buildGlobalFlags({ atespace }), 'get', 'gateways'];
    const out = await this.execChecked(args, { timeoutMs: 25_000 });
    return parseGatewaysTable(out);
  }

  public async listModels(atespace?: string): Promise<ModelSummary[]> {
    const args = [...this.buildGlobalFlags({ atespace }), 'get', 'models'];
    const out = await this.execChecked(args, { timeoutMs: 25_000 });
    return parseModelsTable(out);
  }

  public async applyFile(filePath: string, atespace?: string): Promise<string> {
    // Only pass -a if explicitly overridden so manifest-level metadata.atespace is respected.
    // Allow 5 minutes because Workspace pre-warming clones git repos & uploads snapshots synchronously.
    const cfg = this.getConfig();
    const args: string[] = [];
    if (atespace?.trim()) {
      args.push('-a', atespace.trim());
    }
    if (cfg.namespace?.trim()) {
      args.push('-n', cfg.namespace.trim());
    }
    if (cfg.context?.trim()) {
      args.push('--context', cfg.context.trim());
    }
    if (cfg.server?.trim()) {
      args.push('--server', cfg.server.trim());
    }
    args.push('apply', '-f', filePath);
    const out = await this.execChecked(args, { timeoutMs: 300_000 });
    return out.trim();
  }

  public async applyManifestYaml(manifestYaml: string, atespace?: string): Promise<string> {
    const cfg = this.getConfig();
    const args: string[] = [];
    if (atespace?.trim()) {
      args.push('-a', atespace.trim());
    }
    if (cfg.namespace?.trim()) {
      args.push('-n', cfg.namespace.trim());
    }
    if (cfg.context?.trim()) {
      args.push('--context', cfg.context.trim());
    }
    if (cfg.server?.trim()) {
      args.push('--server', cfg.server.trim());
    }
    args.push('apply', '-f', '-');
    const out = await this.execChecked(args, { stdin: manifestYaml, timeoutMs: 300_000 });
    return out.trim();
  }

  public async resumeTask(name: string, atespace?: string): Promise<string> {
    const args = [...this.buildGlobalFlags({ atespace }), 'resume', 'task', name];
    const out = await this.execChecked(args, { timeoutMs: 240_000 });
    return out.trim();
  }

  public async suspendTask(name: string, atespace?: string): Promise<string> {
    const args = [...this.buildGlobalFlags({ atespace }), 'suspend', 'task', name];
    const out = await this.execChecked(args, { timeoutMs: 180_000 });
    return out.trim();
  }

  public async describeResource(
    kind: AxResourceKind,
    name: string,
    atespace?: string
  ): Promise<string> {
    const args = [...this.buildGlobalFlags({ atespace }), 'describe', kind, name];
    return await this.execChecked(args, { timeoutMs: 25_000 });
  }

  public async getResourceYaml(
    kind: AxResourceKind,
    name: string,
    atespace?: string
  ): Promise<string> {
    const args = [...this.buildGlobalFlags({ atespace }), 'get', kind, name];
    return await this.execChecked(args, { timeoutMs: 25_000 });
  }

  public async getTaskOutput(name: string, atespace?: string): Promise<string> {
    const desc = await this.describeResource('task', name, atespace);
    const extracted = extractDescribeOutputSection(desc);
    return extracted ?? desc;
  }

  public async deleteResource(
    kind: AxResourceKind,
    name: string,
    atespace?: string
  ): Promise<string> {
    const args = [...this.buildGlobalFlags({ atespace }), 'delete', kind, name];
    const out = await this.execChecked(args, { timeoutMs: 30_000 });
    return out.trim();
  }

  public async execInTask(
    taskName: string,
    shellCommand: string,
    atespace?: string
  ): Promise<ExecOutput> {
    const args = [
      ...this.buildGlobalFlags({ atespace }),
      'ssh',
      taskName,
      '--',
      'sh',
      '-c',
      shellCommand,
    ];
    return await this.execRaw(args, { timeoutMs: 120_000 });
  }

  public async promoteTask(
    taskName: string,
    targetContext: string,
    options?: {
      atespace?: string;
      preflightOnly?: boolean;
      suspendSource?: boolean;
      replace?: boolean;
    }
  ): Promise<string> {
    const args = [
      ...this.buildGlobalFlags({ atespace: options?.atespace }),
      'promote',
      'task',
      taskName,
      '--to',
      targetContext,
    ];
    if (options?.preflightOnly) {
      args.push('--preflight-only');
    }
    if (options?.suspendSource) {
      args.push('--suspend-source');
    }
    if (options?.replace) {
      args.push('--replace');
    }
    const out = await this.execChecked(args, { timeoutMs: 180_000 });
    return out.trim();
  }

  public buildSshTerminalArgs(taskName: string, atespace?: string): string[] {
    return [...this.buildGlobalFlags({ atespace }), 'ssh', taskName];
  }
}

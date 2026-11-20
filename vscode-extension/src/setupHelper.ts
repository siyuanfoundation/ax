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
import * as fs from 'fs';
import * as https from 'https';
import * as os from 'os';
import * as path from 'path';
import { AxCli, LocalStatusReport } from './axCli';
import { AGENT_SKILL_ROOTS, InstallResult } from './installer';

export type StepState = 'ready' | 'missing' | 'warning' | 'optional';

export interface ReadinessStep {
  id: 'cli' | 'host' | 'images' | 'substrate' | 'ax' | 'credentials';
  title: string;
  state: StepState;
  summary: string;
  detail: string;
  actionCommand?: string;
  actionLabel?: string;
}

export interface EnvironmentReadiness {
  allCoreReady: boolean;
  localStatus: LocalStatusReport | null;
  activeContext: string;
  releaseVersion: string;
  serverImage: string;
  defaultTaskImage: string;
  runnerVolumeImage: string;
  credentialSecrets: string[];
  steps: ReadinessStep[];
}

export interface AxImageBundle {
  releaseVersion?: string;
  serverImage: string;
  defaultTaskImage: string;
  runnerVolumeImage: string;
}

/**
 * Official multi-arch (`linux/amd64`, `linux/arm64`) `@sha256`-pinned container
 * images published in https://github.com/google/ax/releases/tag/v0.4.0-rc.1.
 */
export const DEFAULT_AX_GITHUB_REPO = 'google/ax';
export const DEFAULT_AX_RELEASE_VERSION = 'v0.4.0-rc.1';
export const DEFAULT_AX_SERVER_IMAGE =
  'ghcr.io/google/ax/ax-server@sha256:95737151ccc54fdb8d99a8a8674f11777d7b08b7108a7b80820a1c502bb71223';
export const DEFAULT_AX_TASK_IMAGE =
  'ghcr.io/google/ax/ax-task-runner@sha256:3a61e70d70cede25cec04c34b2902e2febac08bd78741207101d5c102f6e7a2c';
export const DEFAULT_AX_RUNNER_VOLUME_IMAGE =
  'ghcr.io/google/ax/ax-runner-volume@sha256:ac849f8357ac5b412d2d35d9076ef94b00557399d95c52c62353954e2101be07';

export const DEFAULT_RELEASE_IMAGES: Readonly<AxImageBundle> = Object.freeze({
  releaseVersion: DEFAULT_AX_RELEASE_VERSION,
  serverImage: DEFAULT_AX_SERVER_IMAGE,
  defaultTaskImage: DEFAULT_AX_TASK_IMAGE,
  runnerVolumeImage: DEFAULT_AX_RUNNER_VOLUME_IMAGE,
});

/**
 * Returns effective AX container image references, falling back to the official
 * `v0.4.0-rc.1` GHCR release images whenever user settings / env vars are unset.
 */
export function resolveEffectiveImages(input?: {
  releaseVersion?: string;
  serverImage?: string;
  defaultTaskImage?: string;
  runnerVolumeImage?: string;
}): AxImageBundle {
  const srv = input?.serverImage?.trim() || DEFAULT_AX_SERVER_IMAGE;
  const task = input?.defaultTaskImage?.trim() || DEFAULT_AX_TASK_IMAGE;
  const vol =
    input?.runnerVolumeImage !== undefined && input.runnerVolumeImage.trim() !== ''
      ? input.runnerVolumeImage.trim()
      : srv === DEFAULT_AX_SERVER_IMAGE
        ? DEFAULT_AX_RUNNER_VOLUME_IMAGE
        : '';
  return {
    releaseVersion: input?.releaseVersion?.trim() || DEFAULT_AX_RELEASE_VERSION,
    serverImage: srv,
    defaultTaskImage: task,
    runnerVolumeImage: vol,
  };
}

/**
 * Parses `ax-server`, `AX_DEFAULT_TASK_IMAGE`, and `AX_RUNNER_VOLUME_IMAGE`
 * digest-pinned references from an official `ax-server.yaml` release asset.
 */
export function parseReleaseServerYamlImages(
  yamlText: string,
  releaseVersion?: string
): AxImageBundle {
  const serverMatch = yamlText.match(/^\s*image:\s*["']?([^\s"']*ax-server[^\s"']*)["']?\s*$/m);
  const taskMatch = yamlText.match(
    /-\s*name:\s*AX_DEFAULT_TASK_IMAGE\s*\r?\n\s*value:\s*["']?([^\s"']+)["']?/m
  );
  const volMatch = yamlText.match(
    /-\s*name:\s*AX_RUNNER_VOLUME_IMAGE\s*\r?\n\s*value:\s*["']?([^\s"']*)["']?/m
  );

  const serverImage = serverMatch?.[1]?.trim() || '';
  const defaultTaskImage = taskMatch?.[1]?.trim() || '';
  const runnerVolumeImage = volMatch?.[1]?.trim() || '';

  if (!serverImage) {
    throw new Error('Could not find ax-server image reference in release ax-server.yaml.');
  }

  return {
    releaseVersion,
    serverImage,
    defaultTaskImage,
    runnerVolumeImage,
  };
}

function httpsGetTextFollowRedirects(url: string, redirectsLeft: number = 5): Promise<string> {
  return new Promise((resolve, reject) => {
    const req = https.get(
      url,
      {
        headers: { 'User-Agent': 'ax-vscode-extension/0.1' },
        timeout: 15_000,
      },
      (res) => {
        const status = res.statusCode ?? 0;
        const location = res.headers.location;
        if (status >= 300 && status < 400 && location) {
          res.resume();
          if (redirectsLeft <= 0) {
            reject(new Error(`Too many redirects fetching ${url}`));
            return;
          }
          const nextUrl = new URL(location, url).toString();
          httpsGetTextFollowRedirects(nextUrl, redirectsLeft - 1).then(resolve, reject);
          return;
        }
        if (status !== 200) {
          res.resume();
          reject(new Error(`HTTP ${status} fetching ${url}`));
          return;
        }
        let body = '';
        res.setEncoding('utf8');
        res.on('data', (chunk: string) => {
          body += chunk;
        });
        res.on('end', () => resolve(body));
      }
    );
    req.on('error', reject);
    req.on('timeout', () => {
      req.destroy();
      reject(new Error(`Timed out fetching ${url}`));
    });
  });
}

/**
 * Fetches `@sha256`-pinned image references directly from a published GitHub
 * release's `ax-server.yaml` asset (`https://github.com/google/ax/releases/download/<tag>/ax-server.yaml`).
 */
export async function fetchReleaseImagesFromGitHub(
  versionTag: string,
  githubRepo: string = DEFAULT_AX_GITHUB_REPO
): Promise<AxImageBundle> {
  const tag = versionTag.trim();
  if (!tag) {
    throw new Error('Release tag cannot be empty (e.g. v0.4.0-rc.1).');
  }
  const url = `https://github.com/${githubRepo}/releases/download/${encodeURIComponent(tag)}/ax-server.yaml`;
  const yamlText = await httpsGetTextFollowRedirects(url);
  return parseReleaseServerYamlImages(yamlText, tag);
}

export function resolveKubectlBinary(installDir?: string): string {
  if (installDir) {
    const bundled = path.join(installDir, 'kubectl');
    if (fs.existsSync(bundled)) {
      return bundled;
    }
  }
  const homeAxKubectl = path.join(os.homedir(), '.ax', 'bin', 'kubectl');
  if (fs.existsSync(homeAxKubectl)) {
    return homeAxKubectl;
  }
  return 'kubectl';
}

/**
 * Lists Kubernetes Secret names labeled `ax.io/credential=true` in `namespace`
 * on `kubeContext` without hanging on unreachable clusters.
 */
export async function listCredentialSecrets(options: {
  kubectlBin: string;
  kubeContext: string;
  namespace: string;
  kubeconfig?: string;
}): Promise<string[]> {
  if (!options.kubeContext || options.kubeContext === 'no-context') {
    return [];
  }
  return new Promise((resolve) => {
    const env: NodeJS.ProcessEnv = { ...process.env };
    if (options.kubeconfig?.trim()) {
      env.KUBECONFIG = options.kubeconfig.trim();
    }
    const args = [
      '--context',
      options.kubeContext,
      '--request-timeout=4s',
      '-n',
      options.namespace || 'ax-system',
      'get',
      'secrets',
      '-l',
      'ax.io/credential=true',
      '-o',
      'jsonpath={.items[*].metadata.name}',
    ];
    const child = spawn(options.kubectlBin, args, { env, stdio: 'pipe' });
    let stdout = '';
    const timer = setTimeout(() => {
      child.kill('SIGTERM');
      resolve([]);
    }, 4500);
    child.stdout.on('data', (d: Buffer) => {
      stdout += d.toString('utf8');
    });
    child.on('error', () => {
      clearTimeout(timer);
      resolve([]);
    });
    child.on('close', (code) => {
      clearTimeout(timer);
      if (code !== 0) {
        resolve([]);
        return;
      }
      const names = stdout
        .trim()
        .split(/\s+/)
        .map((s) => s.trim())
        .filter(Boolean);
      resolve(names);
    });
  });
}

/**
 * Creates or updates an Opaque Secret in `namespace` labeled `ax.io/credential=true`
 * so Agent Substrate's `k8s-credential-provider` permits egress header injection.
 */
export async function applyCredentialSecret(options: {
  kubectlBin: string;
  kubeContext: string;
  namespace: string;
  secretName: string;
  secretKey: string;
  secretValue: string;
  kubeconfig?: string;
}): Promise<void> {
  const ns = options.namespace || 'ax-system';
  const manifest = JSON.stringify({
    apiVersion: 'v1',
    kind: 'Secret',
    type: 'Opaque',
    metadata: {
      name: options.secretName,
      namespace: ns,
      labels: {
        'ax.io/credential': 'true',
      },
    },
    stringData: {
      [options.secretKey]: options.secretValue,
    },
  });

  await new Promise<void>((resolve, reject) => {
    const env: NodeJS.ProcessEnv = { ...process.env };
    if (options.kubeconfig?.trim()) {
      env.KUBECONFIG = options.kubeconfig.trim();
    }
    const args: string[] = [];
    if (options.kubeContext) {
      args.push('--context', options.kubeContext);
    }
    args.push('apply', '-f', '-');

    const child = spawn(options.kubectlBin, args, { env, stdio: 'pipe' });
    let stderr = '';
    child.stderr.on('data', (d: Buffer) => {
      stderr += d.toString('utf8');
    });
    child.on('error', reject);
    child.on('close', (code) => {
      if (code === 0) {
        resolve();
      } else {
        reject(new Error(stderr.trim() || `kubectl apply exited with code ${code}`));
      }
    });
    child.stdin.write(manifest, 'utf8');
    child.stdin.end();
  });
}

function summarizeImageSource(serverImg: string, releaseVersion: string): string {
  if (serverImg === DEFAULT_AX_SERVER_IMAGE) {
    return `ghcr.io/google/ax (${DEFAULT_AX_RELEASE_VERSION})`;
  }
  const shortRef = serverImg.split('@')[0];
  if (shortRef.startsWith('ghcr.io/google/ax/') && releaseVersion) {
    return `ghcr.io/google/ax (${releaseVersion})`;
  }
  return shortRef;
}

/**
 * Evaluates full AX + Agent Substrate readiness across all 6 setup stages so
 * the VS Code TreeView, Status Bar, and Guided Setup Wizard can guide the user
 * to install any missing component without requiring any local builds.
 */
export async function inspectEnvironmentReadiness(options: {
  cli: AxCli;
  installResult: InstallResult | null;
  skillHomeDir: string;
  namespace: string;
  kubeconfig?: string;
  configuredContext?: string;
  releaseVersion?: string;
  serverImage?: string;
  defaultTaskImage?: string;
  runnerVolumeImage?: string;
}): Promise<EnvironmentReadiness> {
  const steps: ReadinessStep[] = [];
  const effectiveImages = resolveEffectiveImages({
    releaseVersion: options.releaseVersion,
    serverImage: options.serverImage,
    defaultTaskImage: options.defaultTaskImage,
    runnerVolumeImage: options.runnerVolumeImage,
  });

  // 1. CLI & Skills check
  const axPath = options.installResult?.axBinaryPath || '';
  const cliExists = Boolean(axPath && fs.existsSync(axPath));
  let versionStr = '';
  if (cliExists) {
    try {
      versionStr = await options.cli.version();
    } catch {
      versionStr = '';
    }
  }
  const skillsSynced = AGENT_SKILL_ROOTS.every((root) =>
    fs.existsSync(path.join(options.skillHomeDir, root, 'skills', 'ax', 'SKILL.md'))
  );

  if (cliExists && versionStr && skillsSynced) {
    steps.push({
      id: 'cli',
      title: '1. AX CLI & Agent Skills',
      state: 'ready',
      summary: `Installed (${versionStr.replace(/^ax version\s*/i, '')})`,
      detail: `Binary: ${axPath}\nSynced skills: ~/.agents, ~/.claude, ~/.gemini, ~/.gemini/config (Antigravity)`,
      actionCommand: 'ax.installCliAndSkills',
      actionLabel: 'Reinstall CLI & Skills',
    });
  } else {
    steps.push({
      id: 'cli',
      title: '1. AX CLI & Agent Skills',
      state: 'missing',
      summary: 'Missing — click to install bundled ax CLI & skills',
      detail: 'Installs ~/.ax/bin/ax and syncs SKILL.md to .agents, .claude, and .gemini.',
      actionCommand: 'ax.installCliAndSkills',
      actionLabel: 'Install CLI & Skills',
    });
  }

  // 2. Local status + container host preflight + contexts
  const [localSt, contexts] = await Promise.all([
    cliExists ? options.cli.localStatus().catch(() => null) : Promise.resolve(null),
    cliExists ? options.cli.listContexts().catch(() => []) : Promise.resolve([]),
  ]);

  const activeContext =
    options.configuredContext?.trim() ||
    contexts.find((c) => c.current)?.name ||
    (localSt?.state === 'Running' ? localSt.kubeContext : 'no-context');

  if (!localSt) {
    steps.push({
      id: 'host',
      title: '2. Container Host Preflight',
      state: 'missing',
      summary: 'Install ax CLI first to check Docker/Podman host',
      detail: 'Requires rootful Docker or Podman with Linux kernel >= 6.8 and >= 4 GiB RAM.',
      actionCommand: 'ax.guidedSetup',
      actionLabel: 'Run Guided Setup',
    });
  } else if (localSt.host.error) {
    steps.push({
      id: 'host',
      title: '2. Container Host Preflight',
      state: 'missing',
      summary: localSt.host.error,
      detail: `Runtime: ${localSt.host.runtime || 'none'}\nFix container engine before starting local Kind cluster.`,
      actionCommand: 'ax.localStatus',
      actionLabel: 'Inspect Host Status',
    });
  } else {
    const memGiB = localSt.host.memTotalBytes
      ? `${(localSt.host.memTotalBytes / 1024 ** 3).toFixed(1)} GiB`
      : 'N/A';
    steps.push({
      id: 'host',
      title: '2. Container Host Preflight',
      state: localSt.host.warning ? 'warning' : 'ready',
      summary: `${localSt.host.runtime} · Linux ${localSt.host.kernelVersion} · ${memGiB}`,
      detail: localSt.host.warning || 'Container runtime passed rootful, kernel >= 6.8, and memory checks.',
      actionCommand: 'ax.localStatus',
      actionLabel: 'View Host Report',
    });
  }

  // 3. AX Release Container Images (pre-configured to official GHCR release — zero local build!)
  const relVer = effectiveImages.releaseVersion || DEFAULT_AX_RELEASE_VERSION;
  steps.push({
    id: 'images',
    title: '3. AX Release Images',
    state: 'ready',
    summary: summarizeImageSource(effectiveImages.serverImage, relVer),
    detail: `ax-server: ${effectiveImages.serverImage}\ndefault-task: ${effectiveImages.defaultTaskImage}${
      effectiveImages.runnerVolumeImage ? `\nrunner-volume: ${effectiveImages.runnerVolumeImage}` : ''
    }`,
    actionCommand: 'ax.prepareImages',
    actionLabel: 'Switch Release / Custom Images',
  });

  // 4. Kubernetes Cluster & Agent Substrate check
  const localRunning = localSt?.state === 'Running';
  const substrateReady = Boolean(
    localRunning &&
      localSt?.substrateStatus &&
      localSt.substrateStatus !== 'NotInstalled' &&
      !localSt.substrateStatus.includes('(0/') &&
      !localSt.substrateStatus.includes('[degraded')
  );

  if (substrateReady) {
    steps.push({
      id: 'substrate',
      title: '4. Cluster & Agent Substrate',
      state: 'ready',
      summary: `${localSt!.name} · ${localSt!.substrateStatus}`,
      detail: `Kind cluster "${localSt!.name}" is Running with Agent Substrate control plane & gVisor workers.`,
      actionCommand: 'ax.localStatus',
      actionLabel: 'Inspect Cluster Status',
    });
  } else if (localSt?.state === 'Stopped') {
    steps.push({
      id: 'substrate',
      title: '4. Cluster & Agent Substrate',
      state: 'missing',
      summary: `Stopped (${localSt.name}) — click to resume cluster`,
      detail: 'Local Kind cluster exists but is stopped; run ax local start to resume.',
      actionCommand: 'ax.localStart',
      actionLabel: 'Resume Local Cluster',
    });
  } else {
    steps.push({
      id: 'substrate',
      title: '4. Cluster & Agent Substrate',
      state: 'missing',
      summary: 'Not installed — click to bootstrap Kind + Agent Substrate',
      detail: 'Provisions Kubernetes 1.36+ Kind cluster (ax-local) and installs Agent Substrate v1.0.0 + gVisor WorkerPool.',
      actionCommand: 'ax.localStart',
      actionLabel: 'Bootstrap Local Stack',
    });
  }

  // 5. AX Control Plane Deployment check
  const axReady = Boolean(
    localRunning &&
      localSt?.axStatus &&
      localSt.axStatus !== 'NotInstalled' &&
      !localSt.axStatus.includes('(0/')
  );

  if (axReady) {
    steps.push({
      id: 'ax',
      title: '5. AX Control Plane Deployment',
      state: 'ready',
      summary: localSt!.axStatus,
      detail: `ax-postgres and ax-server are Ready on context "${localSt!.kubeContext}".`,
      actionCommand: 'ax.installOnCluster',
      actionLabel: 'Upgrade / Reinstall AX',
    });
  } else if (substrateReady) {
    steps.push({
      id: 'ax',
      title: '5. AX Control Plane Deployment',
      state: 'missing',
      summary: 'NotInstalled on Substrate cluster — click to deploy AX',
      detail: 'Agent Substrate is healthy, but ax-server is not yet deployed in ax-system.',
      actionCommand: 'ax.installOnCluster',
      actionLabel: 'Deploy AX (ax install)',
    });
  } else {
    steps.push({
      id: 'ax',
      title: '5. AX Control Plane Deployment',
      state: 'missing',
      summary: 'Waiting for Substrate cluster — click to bootstrap stack',
      detail: `Pulls ${summarizeImageSource(effectiveImages.serverImage, relVer)} directly from GHCR and rolls out ax-postgres + ax-server.`,
      actionCommand: 'ax.localStart',
      actionLabel: 'Bootstrap Local Stack',
    });
  }

  // 6. Egress Credential Secrets (for LLM / GitHub Gateway header injection)
  const kubectlBin = resolveKubectlBinary(options.installResult?.installDir);
  const targetKubeCtx = localRunning ? localSt!.kubeContext : '';
  const credentialSecrets = targetKubeCtx
    ? await listCredentialSecrets({
        kubectlBin,
        kubeContext: targetKubeCtx,
        namespace: options.namespace,
        kubeconfig: options.kubeconfig,
      })
    : [];

  if (credentialSecrets.length > 0) {
    steps.push({
      id: 'credentials',
      title: '6. Egress Credential Secrets',
      state: 'ready',
      summary: `${credentialSecrets.join(', ')} (${options.namespace})`,
      detail:
        'Labeled ax.io/credential=true for zero-trust Gateway header injection outside the sandbox.',
      actionCommand: 'ax.configureCredentialSecret',
      actionLabel: 'Add / Update Secret',
    });
  } else {
    steps.push({
      id: 'credentials',
      title: '6. Egress Credential Secrets',
      state: 'optional',
      summary: axReady
        ? 'None configured — click to add LLM/GitHub API key Secret'
        : 'Configure after cluster is running (for Gateway API keys)',
      detail:
        'Creates a Secret in ax-system with label ax.io/credential=true for Gateway effects.replaceHeaders.',
      actionCommand: 'ax.configureCredentialSecret',
      actionLabel: 'Configure Credential Secret',
    });
  }

  const allCoreReady = steps
    .filter((s) => s.id !== 'credentials')
    .every((s) => s.state === 'ready' || s.state === 'warning');

  return {
    allCoreReady,
    localStatus: localSt,
    activeContext,
    releaseVersion: relVer,
    serverImage: effectiveImages.serverImage,
    defaultTaskImage: effectiveImages.defaultTaskImage,
    runnerVolumeImage: effectiveImages.runnerVolumeImage,
    credentialSecrets,
    steps,
  };
}

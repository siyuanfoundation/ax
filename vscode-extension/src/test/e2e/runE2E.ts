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

import { spawnSync } from 'child_process';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { runTests } from '@vscode/test-electron';
import { resolveBundledAxBinary } from '../../installer';

function hasCommand(cmd: string): boolean {
  const res = spawnSync('sh', ['-c', `command -v ${cmd}`], { stdio: 'ignore' });
  return res.status === 0;
}

async function main(): Promise<void> {
  // Automatically wrap with xvfb-run on headless Linux so `npm run test:e2e` needs no manual flags.
  if (
    process.platform === 'linux' &&
    !process.env.DISPLAY &&
    process.env.AX_XVFB_WRAPPED !== '1' &&
    hasCommand('xvfb-run')
  ) {
    console.log('==> Headless Linux detected; re-launching E2E suite under xvfb-run...');
    const res = spawnSync('xvfb-run', ['-a', process.execPath, __filename], {
      stdio: 'inherit',
      env: { ...process.env, AX_XVFB_WRAPPED: '1' },
    });
    process.exit(res.status ?? 1);
  }

  const extensionDevelopmentPath = path.resolve(__dirname, '..', '..', '..');
  const extensionTestsPath = path.resolve(__dirname, './suite/index');
  const repoRoot = path.resolve(extensionDevelopmentPath, '..');

  const tmpRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'ax-vscode-e2e-'));
  const fakeHome = path.join(tmpRoot, 'skill-home');
  const installDir = path.join(fakeHome, '.ax', 'bin');
  const workspaceDir = path.join(tmpRoot, 'workspace');
  const userDataDir = path.join(tmpRoot, 'user-data');
  const extensionsDir = path.join(tmpRoot, 'extensions');

  fs.mkdirSync(fakeHome, { recursive: true });
  fs.mkdirSync(workspaceDir, { recursive: true });
  fs.mkdirSync(userDataDir, { recursive: true });
  fs.mkdirSync(extensionsDir, { recursive: true });

  const extensionTestsEnv: Record<string, string | undefined> = {
    ...process.env,
    AX_INSTALL_DIR: installDir,
    AX_SKILL_HOME_DIR: fakeHome,
    VSCODE_IPC_HOOK_CLI: undefined,
    ELECTRON_RUN_AS_NODE: undefined,
  };

  // Detect live AX cluster kubeconfig (`AX_E2E_KUBECONFIG` or `../.tmp/kubeconfig-ax-e2e`).
  const candidateKubeconfig =
    process.env.AX_E2E_KUBECONFIG || path.join(repoRoot, '.tmp', 'kubeconfig-ax-e2e');
  const bundledAx = resolveBundledAxBinary(extensionDevelopmentPath);

  if (fs.existsSync(candidateKubeconfig) && bundledAx && fs.existsSync(bundledAx)) {
    const isolatedKubeconfig = path.join(tmpRoot, 'kubeconfig');
    fs.copyFileSync(candidateKubeconfig, isolatedKubeconfig);
    fs.chmodSync(isolatedKubeconfig, 0o600);

    const probe = spawnSync(bundledAx, ['get', 'tasks'], {
      env: { ...process.env, KUBECONFIG: isolatedKubeconfig },
      encoding: 'utf8',
      timeout: 20_000,
    });
    if (probe.status === 0) {
      console.log(`==> Live AX cluster reachable via ${candidateKubeconfig}; enabling full cluster E2E.`);
      extensionTestsEnv.KUBECONFIG = isolatedKubeconfig;
      extensionTestsEnv.AX_E2E_CLUSTER = '1';
    } else {
      console.warn(
        `==> Kubeconfig ${candidateKubeconfig} found, but cluster probe exited ${probe.status}: ${probe.stderr}`
      );
    }
  }

  // Reuse locally installed VS Code Electron binary on Linux if available to avoid network download.
  const localLinuxCode = '/usr/share/code/code';
  const vscodeExecutablePath =
    process.platform === 'linux' && fs.existsSync(localLinuxCode) ? localLinuxCode : undefined;

  try {
    await runTests({
      vscodeExecutablePath,
      extensionDevelopmentPath,
      extensionTestsPath,
      extensionTestsEnv,
      launchArgs: [
        workspaceDir,
        `--user-data-dir=${userDataDir}`,
        `--extensions-dir=${extensionsDir}`,
        '--disable-extensions',
        '--no-sandbox',
        '--disable-gpu',
        '--disable-updates',
        '--skip-welcome',
        '--skip-release-notes',
      ],
    });
  } finally {
    fs.rmSync(tmpRoot, { recursive: true, force: true });
  }
}

main().catch((err) => {
  console.error('E2E suite failed:', err);
  process.exit(1);
});

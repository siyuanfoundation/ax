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

import * as assert from 'assert';
import * as fs from 'fs';
import * as path from 'path';
import * as vscode from 'vscode';
import { AxExtensionApi } from '../../../extension';
import { AGENT_SKILL_ROOTS } from '../../../installer';

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

export async function run(): Promise<void> {
  console.log('==> [E2E] Inside VS Code Extension Host; activating google.ax-vscode...');

  const ext = vscode.extensions.getExtension<AxExtensionApi>('google.ax-vscode');
  assert.ok(ext, 'Extension google.ax-vscode not found in VS Code Extension Host');

  const api = await ext.activate();
  assert.ok(ext.isActive, 'Extension should be active');

  // 1. Verify bundled CLI & Agent Skills installation
  const installRes = api.getInstallResult();
  assert.ok(installRes, 'Expected installAxCliAndSkills to run on activation');
  assert.strictEqual(installRes.installedFromBundle, true, 'Expected ax binary installed from bundle');
  assert.ok(fs.existsSync(installRes.axBinaryPath), `Missing ax binary at ${installRes.axBinaryPath}`);

  const versionStr = await api.cli.version();
  console.log(`==> [E2E] Verified bundled ax CLI: ${versionStr}`);
  assert.ok(versionStr.startsWith('ax version '), `Unexpected version output: ${versionStr}`);

  const skillHome = process.env.AX_SKILL_HOME_DIR;
  assert.ok(skillHome, 'AX_SKILL_HOME_DIR must be set by E2E harness');
  for (const agentRoot of AGENT_SKILL_ROOTS) {
    const skillPath = path.join(skillHome, agentRoot, 'skills', 'ax', 'SKILL.md');
    const refPath = path.join(skillHome, agentRoot, 'skills', 'ax', 'references', 'manifests.md');
    assert.ok(fs.existsSync(skillPath), `Missing synced skill at ${skillPath}`);
    assert.ok(fs.existsSync(refPath), `Missing synced skill reference at ${refPath}`);
  }
  console.log('==> [E2E] Verified SKILL.md + references synced to .agents, .claude, and .gemini');

  // 2. Verify Language Model Tools (`vscode.lm.registerTool`) & commands registration
  assert.strictEqual(
    api.registeredLmToolCount,
    4,
    'Expected all 4 AX Language Model Tools to register with vscode.lm'
  );
  const allCommands = await vscode.commands.getCommands(true);
  for (const cmd of [
    'ax.guidedSetup',
    'ax.prepareImages',
    'ax.installOnCluster',
    'ax.configureCredentialSecret',
    'ax.viewTaskOutput',
    'ax.refresh',
    'ax.installCliAndSkills',
    'ax.applyFile',
    'ax.newTaskManifest',
    'ax.resumeTask',
    'ax.suspendTask',
    'ax.sshTerminal',
    'ax.describeResource',
    'ax.getResourceYaml',
    'ax.promoteTask',
    'ax.deleteResource',
  ]) {
    assert.ok(allCommands.includes(cmd), `Command ${cmd} not registered in VS Code`);
  }
  console.log('==> [E2E] Verified all VS Code commands and 4 Language Model Tools registered');

  // 3. Verify live YAML editor diagnostics and manifest scaffolding command
  const scaffoldDoc = await vscode.commands.executeCommand<vscode.TextDocument>(
    'ax.newTaskManifest',
    'starter'
  );
  assert.ok(scaffoldDoc, 'ax.newTaskManifest should open a starter YAML document');
  assert.ok(scaffoldDoc.getText().includes('apiVersion: ax.io/v1alpha1'));
  await sleep(150);
  assert.strictEqual(
    vscode.languages.getDiagnostics(scaffoldDoc.uri).length,
    0,
    'Starter manifest should have zero AX lint errors'
  );

  const prReviewDoc = await vscode.commands.executeCommand<vscode.TextDocument>(
    'ax.newTaskManifest',
    { template: 'pr-review', repo: 'etcd-io/etcd', prCount: 10 }
  );
  assert.ok(prReviewDoc, 'ax.newTaskManifest pr-review should open PR review pipeline YAML');
  assert.ok(prReviewDoc.getText().includes('etcd-io/etcd'));
  await sleep(150);
  assert.strictEqual(
    vscode.languages.getDiagnostics(prReviewDoc.uri).length,
    0,
    'PR review manifest template should have zero AX lint errors/warnings'
  );

  const badDoc = await vscode.workspace.openTextDocument({
    language: 'yaml',
    content: [
      'apiVersion: ax.io/v1alpha1',
      'kind: Task',
      'metadata:',
      '  name: Bad_Name',
      'spec:',
      '  profile:',
      '    image: "nginx:latest"',
    ].join('\n'),
  });
  await vscode.window.showTextDocument(badDoc);
  await sleep(200);
  const badDiags = vscode.languages.getDiagnostics(badDoc.uri);
  assert.ok(
    badDiags.some((d) => d.source === 'ax' && d.message.includes('@sha256:')),
    `Expected AX digest diagnostic on unpinned image, got: ${JSON.stringify(badDiags)}`
  );
  console.log('==> [E2E] Verified live YAML diagnostics in VS Code editor');

  // 4. Live AX Cluster Sandbox Lifecycle E2E (when KUBECONFIG points at a running AX cluster)
  if (process.env.AX_E2E_CLUSTER === '1') {
    const suffix = `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 6)}`;
    const wsName = `vscode-ws-${suffix}`;
    const taskName = `vscode-task-${suffix}`;

    const statusJson = JSON.parse(await api.tools.status({}));
    console.log(
      `==> [E2E] ax_status tool reported context=${statusJson.activeContext}, runtime=${statusJson.localStack?.host?.runtime}`
    );
    assert.ok(statusJson.activeContext, 'Expected active Kubernetes context in ax_status');

    const workspaceFolder = vscode.workspace.workspaceFolders?.[0]?.uri.fsPath;
    assert.ok(workspaceFolder, 'Expected open workspace folder in VS Code');
    const manifestFile = path.join(workspaceFolder, `${taskName}.ax.yaml`);

    const manifestContent = [
      'apiVersion: ax.io/v1alpha1',
      'kind: Workspace',
      'metadata:',
      `  name: ${wsName}`,
      'spec:',
      '  setup:',
      '    files:',
      '      - path: "AGENTS.md"',
      '        content: |',
      `          workspace-marker=${wsName}`,
      '---',
      'apiVersion: ax.io/v1alpha1',
      'kind: Task',
      'metadata:',
      `  name: ${taskName}`,
      'spec:',
      '  workspace:',
      `    name: ${wsName}`,
      '  commands: ["sleep", "infinity"]',
      '  env:',
      '    - name: FROM_VSCODE_EXT',
      '      value: "verified-e2e"',
      '  debug: true',
      '',
    ].join('\n');
    fs.writeFileSync(manifestFile, manifestContent, 'utf8');

    try {
      // 4a. Apply manifest via VS Code command `ax.applyFile`
      const applyOut = await vscode.commands.executeCommand<string>(
        'ax.applyFile',
        vscode.Uri.file(manifestFile)
      );
      console.log(`==> [E2E] ax.applyFile output:\n${applyOut}`);
      assert.ok(applyOut?.includes(`workspace.ax.io/${wsName} created`));
      assert.ok(applyOut?.includes(`task.ax.io/${taskName} created`));

      // 4b. Verify TreeView shows task in Suspended state (`axTaskSuspended`)
      const rootNodes = await api.explorer.getChildren();
      const tasksGroup = rootNodes.find((n) => n.group === 'tasks');
      assert.ok(tasksGroup, 'Expected Tasks group in AX Explorer TreeView');
      const taskNodesBefore = await api.explorer.getChildren(tasksGroup);
      const suspendedNode = taskNodesBefore.find((n) => n.resourceName === taskName);
      assert.ok(suspendedNode, `Expected ${taskName} in AX Explorer TreeView`);
      assert.strictEqual(suspendedNode.contextValue, 'axTaskSuspended');

      // 4c. Resume task via VS Code command `ax.resumeTask`
      console.log(`==> [E2E] Resuming task ${taskName} on live Substrate + AX cluster...`);
      const resumeOut = await vscode.commands.executeCommand<string>('ax.resumeTask', taskName);
      assert.ok(resumeOut?.includes('resumed'), `Unexpected resume output: ${resumeOut}`);

      const taskNodesWorking = await api.explorer.getChildren(tasksGroup);
      const workingNode = taskNodesWorking.find((n) => n.resourceName === taskName);
      assert.ok(workingNode, `Expected ${taskName} in TreeView after resume`);
      assert.strictEqual(workingNode.contextValue, 'axTaskWorking');

      // 4d. Execute commands inside the gVisor sandbox via `ax_ssh_exec` Language Model Tool
      const execRaw = await api.tools.sshExec({
        taskName,
        command:
          'cat /workspace/AGENTS.md && echo "env=$FROM_VSCODE_EXT" && echo "durable-across-checkpoint" > /workspace/checkpoint.txt',
      });
      const execRes = JSON.parse(execRaw) as {
        exitCode: number;
        stdout: string;
        stderr: string;
      };
      console.log(`==> [E2E] ax_ssh_exec stdout: ${execRes.stdout.trim()}`);
      assert.strictEqual(execRes.exitCode, 0, `ax_ssh_exec failed: ${execRes.stderr}`);
      assert.ok(execRes.stdout.includes(`workspace-marker=${wsName}`));
      assert.ok(execRes.stdout.includes('env=verified-e2e'));

      // 4e. Inspect via `ax.describeResource` virtual document
      const descText = await vscode.commands.executeCommand<string>('ax.describeResource', {
        resourceKind: 'task',
        resourceName: taskName,
      });
      assert.ok(descText?.includes(`Name:         ${taskName}`));
      assert.ok(descText?.includes('State:        Working'));

      // 4f. Checkpoint (`ax.suspendTask`) -> Resume (`ax.resumeTask`) -> verify durable `/workspace`
      console.log(`==> [E2E] Testing suspend -> resume snapshot persistence for ${taskName}...`);
      const suspendOut = await vscode.commands.executeCommand<string>('ax.suspendTask', taskName);
      assert.ok(suspendOut?.includes('suspended'));

      await vscode.commands.executeCommand<string>('ax.resumeTask', taskName);
      const verifySnap = JSON.parse(
        await api.tools.sshExec({
          taskName,
          command: 'cat /workspace/checkpoint.txt',
        })
      ) as { exitCode: number; stdout: string; stderr: string };
      assert.strictEqual(verifySnap.exitCode, 0, verifySnap.stderr);
      assert.strictEqual(verifySnap.stdout.trim(), 'durable-across-checkpoint');
      console.log('==> [E2E] Verified durable /workspace snapshot across suspend -> resume!');
    } finally {
      // 4g. Clean up Task and Workspace via VS Code command `ax.deleteResource`
      try {
        await vscode.commands.executeCommand(
          'ax.deleteResource',
          { resourceKind: 'task', resourceName: taskName },
          true
        );
      } catch (e) {
        console.warn(`Cleanup task warning: ${(e as Error).message}`);
      }
      try {
        await vscode.commands.executeCommand(
          'ax.deleteResource',
          { resourceKind: 'workspace', resourceName: wsName },
          true
        );
      } catch (e) {
        console.warn(`Cleanup workspace warning: ${(e as Error).message}`);
      }

      const remainingTasks = JSON.parse(
        await api.tools.listResources({ kind: 'tasks' })
      ) as Array<{ name: string }>;
      assert.ok(
        !remainingTasks.some((t) => t.name === taskName),
        `Task ${taskName} should have been deleted`
      );
      console.log(`==> [E2E] Cleaned up ${taskName} and ${wsName} cleanly.`);
    }
  } else {
    console.log('==> [E2E] No live AX kubeconfig detected; skipped live cluster lifecycle step.');
  }

  console.log('==> [E2E] All VS Code Extension Host E2E checks PASSED!');
}

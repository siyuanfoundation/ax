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
import * as os from 'os';
import * as path from 'path';
import {
  parseContextsTable,
  parseGatewaysTable,
  parseModelsTable,
  parseTasksTable,
  parseTunnelsTable,
  parseWorkspacesTable,
  validateAxManifestText,
} from '../../axCli';
import { AGENT_SKILL_ROOTS, installAxCliAndSkills } from '../../installer';

interface TestCase {
  name: string;
  fn: () => void | Promise<void>;
}

const tests: TestCase[] = [];
function test(name: string, fn: () => void | Promise<void>): void {
  tests.push({ name, fn });
}

const EXT_ROOT = path.resolve(__dirname, '..', '..', '..');

test('package.json contributes chatSkills, languageModelTools, yamlValidation, and commands', () => {
  const pkg = JSON.parse(fs.readFileSync(path.join(EXT_ROOT, 'package.json'), 'utf8'));
  assert.strictEqual(pkg.contributes.chatSkills.length, 1);
  assert.strictEqual(pkg.contributes.chatSkills[0].path, './skills/ax/SKILL.md');

  const skillMd = fs.readFileSync(path.join(EXT_ROOT, 'skills', 'ax', 'SKILL.md'), 'utf8');
  assert.ok(
    /^---\nname:\s*ax\n/m.test(skillMd),
    'SKILL.md frontmatter `name` must match folder name `ax` for VS Code chatSkills'
  );
  assert.ok(
    fs.existsSync(path.join(EXT_ROOT, 'skills', 'ax', 'references', 'manifests.md')),
    'Expected references/manifests.md in bundled skill'
  );

  const schema = JSON.parse(
    fs.readFileSync(path.join(EXT_ROOT, 'schemas', 'ax-v1alpha1.schema.json'), 'utf8')
  );
  assert.ok(schema.definitions.Task && schema.definitions.Workspace);
  assert.ok(schema.definitions.Gateway && schema.definitions.Model);

  const toolNames = pkg.contributes.languageModelTools.map((t: { name: string }) => t.name);
  assert.deepStrictEqual(toolNames, [
    'ax_status',
    'ax_list_resources',
    'ax_manage_task',
    'ax_ssh_exec',
  ]);
});

test('installAxCliAndSkills copies bundled binary and syncs skills to all agent roots', () => {
  const tmpRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'ax-vscode-unit-'));
  try {
    const fakeExt = path.join(tmpRoot, 'ext');
    fs.mkdirSync(path.join(fakeExt, 'bin'), { recursive: true });
    fs.mkdirSync(path.join(fakeExt, 'skills', 'ax', 'references'), { recursive: true });

    const fakeBin = path.join(fakeExt, 'bin', `ax-${process.platform}-${process.arch}`);
    fs.writeFileSync(fakeBin, '#!/usr/bin/env sh\necho "ax version 0.1.0-test"\n', { mode: 0o755 });
    fs.copyFileSync(
      path.join(EXT_ROOT, 'skills', 'ax', 'SKILL.md'),
      path.join(fakeExt, 'skills', 'ax', 'SKILL.md')
    );
    fs.copyFileSync(
      path.join(EXT_ROOT, 'skills', 'ax', 'references', 'manifests.md'),
      path.join(fakeExt, 'skills', 'ax', 'references', 'manifests.md')
    );

    const fakeHome = path.join(tmpRoot, 'home');
    const installDir = path.join(fakeHome, '.ax', 'bin');

    const res = installAxCliAndSkills({
      extensionPath: fakeExt,
      installDir,
      skillHomeDir: fakeHome,
    });

    assert.strictEqual(res.installedFromBundle, true);
    assert.strictEqual(res.axBinaryPath, path.join(installDir, 'ax'));
    assert.ok(fs.existsSync(res.axBinaryPath));
    const stat = fs.statSync(res.axBinaryPath);
    assert.notStrictEqual(stat.mode & 0o111, 0, 'Installed ax binary must be executable');

    assert.strictEqual(res.syncedSkillDirs.length, AGENT_SKILL_ROOTS.length);
    for (const agentRoot of AGENT_SKILL_ROOTS) {
      const skillFile = path.join(fakeHome, agentRoot, 'skills', 'ax', 'SKILL.md');
      const refFile = path.join(fakeHome, agentRoot, 'skills', 'ax', 'references', 'manifests.md');
      assert.ok(fs.existsSync(skillFile), `Missing synced skill at ${skillFile}`);
      assert.ok(fs.existsSync(refFile), `Missing synced reference at ${refFile}`);
    }
  } finally {
    fs.rmSync(tmpRoot, { recursive: true, force: true });
  }
});

test('axCli table parsers decode tasks, workspaces, gateways, models, contexts, and tunnels', () => {
  const tasks = parseTasksTable(
    [
      'NAME          ATESPACE   STATE       ACTOR         AGE',
      'simple-task   default    Working     simple-task   2m',
      'idle-task     team-a     Suspended   <none>        15s',
    ].join('\n')
  );
  assert.strictEqual(tasks.length, 2);
  assert.deepStrictEqual(tasks[0], {
    name: 'simple-task',
    atespace: 'default',
    state: 'Working',
    actor: 'simple-task',
    age: '2m',
  });
  assert.strictEqual(tasks[1].state, 'Suspended');

  const workspaces = parseWorkspacesTable(
    [
      'NAME         ATESPACE   PHASE   GIT-REPOS   MCP-SERVERS',
      'golang-ws    default    Ready   1           2',
    ].join('\n')
  );
  assert.strictEqual(workspaces.length, 1);
  assert.strictEqual(workspaces[0].gitRepos, 1);
  assert.strictEqual(workspaces[0].mcpServers, 2);

  const gateways = parseGatewaysTable(
    [
      'NAME            ATESPACE   EGRESS-RULES',
      'review-bot-gw   default    3',
    ].join('\n')
  );
  assert.strictEqual(gateways.length, 1);
  assert.strictEqual(gateways[0].egressRules, 3);

  const models = parseModelsTable(
    [
      'NAME            ATESPACE   PROVIDER   MODEL',
      'default-model   default    google     gemini-3.8-flash',
    ].join('\n')
  );
  assert.strictEqual(models.length, 1);
  assert.strictEqual(models[0].provider, 'google');

  const contexts = parseContextsTable(
    [
      'CURRENT  NAME          CLUSTER       AX TUNNEL',
      '*        ax-local      kind-ax-local 127.0.0.1:44007 -> ax-system/ax-server (Healthy)',
      '         gke-staging   gke-staging   -',
    ].join('\n')
  );
  assert.strictEqual(contexts.length, 2);
  assert.strictEqual(contexts[0].current, true);
  assert.strictEqual(contexts[0].name, 'ax-local');
  assert.ok(contexts[0].tunnel.includes('Healthy'));
  assert.strictEqual(contexts[1].current, false);
  assert.strictEqual(contexts[1].tunnel, '-');

  assert.deepStrictEqual(parseTunnelsTable('No active tunnels found in ~/.ax/tunnels.\n'), []);
  const tunnels = parseTunnelsTable(
    [
      'CONTEXT    LOCAL PORT  REMOTE SERVICE       PID     STATUS  CREATED',
      'ax-local   44007       ax-system/ax-server  12345   Active  2026-11-19 02:30:00',
    ].join('\n')
  );
  assert.strictEqual(tunnels.length, 1);
  assert.strictEqual(tunnels[0].localPort, 44007);
  assert.strictEqual(tunnels[0].status, 'Active');
});

test('validateAxManifestText flags invalid names, tag-only images, and resources.requests', () => {
  const validYaml = [
    'apiVersion: ax.io/v1alpha1',
    'kind: Workspace',
    'metadata:',
    '  name: demo-ws',
    '---',
    'apiVersion: ax.io/v1alpha1',
    'kind: Task',
    'metadata:',
    '  name: demo-task',
    'spec:',
    '  profile:',
    '    image: "ghcr.io/org/agent@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"',
    '    resources:',
    '      limits:',
    '        cpu: "1"',
    '        memory: "512Mi"',
  ].join('\n');
  assert.deepStrictEqual(validateAxManifestText(validYaml), []);

  const invalidYaml = [
    'apiVersion: ax.io/v1alpha1',
    'kind: Task',
    'metadata:',
    '  name: Invalid_UpperCase',
    'spec:',
    '  profile:',
    '    image: "ghcr.io/org/agent:v1"',
    '    resources:',
    '      requests:',
    '        cpu: "1"',
  ].join('\n');
  const diags = validateAxManifestText(invalidYaml);
  assert.strictEqual(diags.length, 3);
  assert.ok(diags.some((d) => d.message.includes('RFC 1123')));
  assert.ok(diags.some((d) => d.message.includes('@sha256:')));
  assert.ok(diags.some((d) => d.message.includes('resources.requests')));
});

async function main(): Promise<void> {
  let passed = 0;
  for (const t of tests) {
    try {
      await t.fn();
      console.log(`  PASS  ${t.name}`);
      passed++;
    } catch (err) {
      console.error(`  FAIL  ${t.name}`);
      console.error(err);
      process.exit(1);
    }
  }
  console.log(`\n${passed}/${tests.length} unit tests passed.`);
}

void main();

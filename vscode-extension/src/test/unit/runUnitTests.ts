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
  extractDescribeOutputSection,
  parseContextsTable,
  parseGatewaysTable,
  parseModelsTable,
  parseTasksTable,
  parseTunnelsTable,
  parseWorkspacesTable,
  summarizeAxSubcommand,
  validateAxManifestText,
} from '../../axCli';
import { AGENT_SKILL_ROOTS, installAxCliAndSkills } from '../../installer';
import {
  DEFAULT_AX_RELEASE_VERSION,
  DEFAULT_AX_RUNNER_VOLUME_IMAGE,
  DEFAULT_AX_SERVER_IMAGE,
  DEFAULT_AX_TASK_IMAGE,
  parseReleaseServerYamlImages,
  resolveEffectiveImages,
} from '../../setupHelper';

interface TestCase {
  name: string;
  fn: () => void | Promise<void>;
}

const tests: TestCase[] = [];
function test(name: string, fn: () => void | Promise<void>): void {
  tests.push({ name, fn });
}

const EXT_ROOT = path.resolve(__dirname, '..', '..', '..');

test('package.json contributes walkthroughs, chatSkills, languageModelTools, yamlValidation, and guided commands', () => {
  const pkg = JSON.parse(fs.readFileSync(path.join(EXT_ROOT, 'package.json'), 'utf8'));
  assert.strictEqual(pkg.contributes.walkthroughs.length, 1);
  assert.strictEqual(pkg.contributes.walkthroughs[0].id, 'axGettingStarted');
  for (const step of pkg.contributes.walkthroughs[0].steps) {
    const mdPath = path.join(EXT_ROOT, step.media.markdown);
    assert.ok(fs.existsSync(mdPath), `Missing walkthrough markdown: ${mdPath}`);
  }

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
  assert.ok(
    schema.definitions.Workspace.properties.spec.properties.profile,
    'Workspace schema must support spec.profile'
  );
  assert.ok(
    schema.definitions.Workspace.properties.spec.properties.setup.properties.gateway,
    'Workspace schema must support spec.setup.gateway'
  );

  const toolNames = pkg.contributes.languageModelTools.map((t: { name: string }) => t.name);
  assert.deepStrictEqual(toolNames, [
    'ax_status',
    'ax_list_resources',
    'ax_manage_task',
    'ax_ssh_exec',
  ]);

  const cfgProps = pkg.contributes.configuration.properties;
  assert.strictEqual(cfgProps['ax.sourceRepoPath'], undefined, 'Extension must not depend on local sourceRepoPath');
  assert.strictEqual(cfgProps['ax.releaseVersion'].default, DEFAULT_AX_RELEASE_VERSION);
  assert.strictEqual(cfgProps['ax.serverImage'].default, DEFAULT_AX_SERVER_IMAGE);
  assert.strictEqual(cfgProps['ax.defaultTaskImage'].default, DEFAULT_AX_TASK_IMAGE);
  assert.strictEqual(cfgProps['ax.runnerVolumeImage'].default, DEFAULT_AX_RUNNER_VOLUME_IMAGE);

  const cmdNames = pkg.contributes.commands.map((c: { command: string }) => c.command);
  for (const requiredCmd of [
    'ax.guidedSetup',
    'ax.prepareImages',
    'ax.installOnCluster',
    'ax.configureCredentialSecret',
    'ax.viewTaskOutput',
  ]) {
    assert.ok(cmdNames.includes(requiredCmd), `Missing command ${requiredCmd}`);
  }
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

    // Verify misnamed `.gemini/config/skills` JSON file is migrated to `skills.json` so Antigravity can scan `.gemini/config/skills/ax`
    const fakeHome2 = path.join(tmpRoot, 'home-misnamed');
    fs.mkdirSync(path.join(fakeHome2, '.gemini', 'config'), { recursive: true });
    fs.writeFileSync(
      path.join(fakeHome2, '.gemini', 'config', 'skills'),
      '{"entries": []}\n',
      'utf8'
    );
    installAxCliAndSkills({
      extensionPath: fakeExt,
      installDir: path.join(fakeHome2, '.ax', 'bin'),
      skillHomeDir: fakeHome2,
    });
    assert.ok(
      fs.existsSync(path.join(fakeHome2, '.gemini', 'config', 'skills.json')),
      'Expected misnamed .gemini/config/skills JSON file to migrate to skills.json'
    );
    assert.ok(
      fs.existsSync(path.join(fakeHome2, '.gemini', 'config', 'skills', 'ax', 'SKILL.md')),
      'Expected Antigravity skill synced to .gemini/config/skills/ax/SKILL.md'
    );
  } finally {
    fs.rmSync(tmpRoot, { recursive: true, force: true });
  }
});

test('axCli helpers summarize subcommands, extract task Output, and decode tables', () => {
  assert.strictEqual(
    summarizeAxSubcommand(['-a', 'default', '-n', 'ax-system', 'get', 'tasks']),
    'get tasks'
  );
  assert.strictEqual(
    summarizeAxSubcommand(['-n', 'ax-system', 'apply', '-f', '/tmp/task.yaml']),
    'apply /tmp/task.yaml'
  );

  const sampleDescribe = [
    'Name:         etcd-review-prs',
    'State:        Completed',
    'Outcome:      TASK_RESULT_SUCCEEDED (exitCode=0)',
    'Output:',
    '  # Review Report',
    '  - PR #1: LGTM',
    '',
    'Conditions:',
    '  TYPE   STATUS',
  ].join('\n');
  assert.strictEqual(
    extractDescribeOutputSection(sampleDescribe),
    '# Review Report\n- PR #1: LGTM'
  );

  const tasks = parseTasksTable(
    [
      'NAME          ATESPACE   STATE       ACTOR         AGE',
      'simple-task   default    Working     simple-task   2m',
      'done-task     default    Completed   done-task     45s',
      'idle-task     team-a     Suspended   <none>        15s',
    ].join('\n')
  );
  assert.strictEqual(tasks.length, 3);
  assert.strictEqual(tasks[1].state, 'Completed');

  const workspaces = parseWorkspacesTable(
    [
      'NAME         ATESPACE   PHASE   GIT-REPOS   MCP-SERVERS',
      'golang-ws    default    Ready   1           2',
    ].join('\n')
  );
  assert.strictEqual(workspaces.length, 1);
  assert.strictEqual(workspaces[0].gitRepos, 1);

  const gateways = parseGatewaysTable(
    [
      'NAME            ATESPACE   EGRESS-RULES',
      'review-bot-gw   default    3',
    ].join('\n')
  );
  assert.strictEqual(gateways.length, 1);

  const models = parseModelsTable(
    [
      'NAME            ATESPACE   PROVIDER   MODEL',
      'default-model   default    google     gemini-3.8-flash',
    ].join('\n')
  );
  assert.strictEqual(models.length, 1);

  const contexts = parseContextsTable(
    [
      'CURRENT  NAME          CLUSTER       AX TUNNEL',
      '*        ax-local      kind-ax-local 127.0.0.1:44007 -> ax-system/ax-server (Healthy)',
      '         gke-staging   gke-staging   -',
    ].join('\n')
  );
  assert.strictEqual(contexts.length, 2);
  assert.strictEqual(contexts[0].current, true);

  assert.deepStrictEqual(parseTunnelsTable('No active tunnels found in ~/.ax/tunnels.\n'), []);
  const tunnels = parseTunnelsTable(
    [
      'CONTEXT    LOCAL PORT  REMOTE SERVICE       PID     STATUS  CREATED',
      'ax-local   44007       ax-system/ax-server  12345   Active  2026-11-19 02:30:00',
    ].join('\n')
  );
  assert.strictEqual(tunnels.length, 1);
});

test('validateAxManifestText skips YAML block scalars, warns on setup.git without gateway, and flags invalid fields', () => {
  const validWithBlockScalar = [
    'apiVersion: ax.io/v1alpha1',
    'kind: Workspace',
    'metadata:',
    '  name: demo-ws',
    'spec:',
    '  setup:',
    '    gateway:',
    '      name: demo-gw',
    '    git:',
    '      - repo: "https://github.com/etcd-io/etcd.git"',
    '    files:',
    '      - path: "script.py"',
    '        content: |',
    '          # Embedded content with image: foo:latest and requests: should not trigger lint',
    '          image: not-an-ax-field:latest',
    '          requests:',
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
  assert.deepStrictEqual(validateAxManifestText(validWithBlockScalar), []);

  const missingSetupGatewayYaml = [
    'apiVersion: ax.io/v1alpha1',
    'kind: Workspace',
    'metadata:',
    '  name: no-gw-ws',
    'spec:',
    '  setup:',
    '    git:',
    '      - repo: "https://github.com/etcd-io/etcd.git"',
  ].join('\n');
  const gwWarn = validateAxManifestText(missingSetupGatewayYaml);
  assert.strictEqual(gwWarn.length, 1);
  assert.strictEqual(gwWarn[0].severity, 'warning');
  assert.ok(gwWarn[0].message.includes('spec.setup.gateway'));

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
});

test('setupHelper resolves official v0.4.0-rc.1 GHCR release images and parses release ax-server.yaml', () => {
  const defaults = resolveEffectiveImages({});
  assert.strictEqual(defaults.releaseVersion, 'v0.4.0-rc.1');
  assert.strictEqual(defaults.serverImage, DEFAULT_AX_SERVER_IMAGE);
  assert.strictEqual(defaults.defaultTaskImage, DEFAULT_AX_TASK_IMAGE);
  assert.strictEqual(defaults.runnerVolumeImage, DEFAULT_AX_RUNNER_VOLUME_IMAGE);
  assert.ok(defaults.serverImage.startsWith('ghcr.io/google/ax/ax-server@sha256:'));
  assert.ok(defaults.defaultTaskImage.startsWith('ghcr.io/google/ax/ax-task-runner@sha256:'));
  assert.ok(defaults.runnerVolumeImage.startsWith('ghcr.io/google/ax/ax-runner-volume@sha256:'));

  const sampleReleaseYaml = [
    'apiVersion: apps/v1',
    'kind: Deployment',
    'spec:',
    '  template:',
    '    spec:',
    '      containers:',
    '        - name: ax-server',
    `          image: ${DEFAULT_AX_SERVER_IMAGE}`,
    '          env:',
    '            - name: AX_DEFAULT_TASK_IMAGE',
    `              value: "${DEFAULT_AX_TASK_IMAGE}"`,
    '            - name: AX_RUNNER_VOLUME_IMAGE',
    `              value: "${DEFAULT_AX_RUNNER_VOLUME_IMAGE}"`,
  ].join('\n');
  const parsed = parseReleaseServerYamlImages(sampleReleaseYaml, 'v0.4.0-rc.1');
  assert.deepStrictEqual(parsed, {
    releaseVersion: 'v0.4.0-rc.1',
    serverImage: DEFAULT_AX_SERVER_IMAGE,
    defaultTaskImage: DEFAULT_AX_TASK_IMAGE,
    runnerVolumeImage: DEFAULT_AX_RUNNER_VOLUME_IMAGE,
  });
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

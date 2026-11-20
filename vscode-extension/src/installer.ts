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

import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';

export interface InstallOptions {
  extensionPath: string;
  installDir?: string;
  skillHomeDir?: string;
  platform?: NodeJS.Platform;
  arch?: string;
  log?: (msg: string) => void;
}

export interface InstallResult {
  axBinaryPath: string;
  installDir: string;
  installedFromBundle: boolean;
  syncedSkillDirs: string[];
}

export const AGENT_SKILL_ROOTS = [
  '.agents',
  '.claude',
  '.gemini',
  '.gemini/config',
] as const;

export function resolveInstallDir(explicitDir?: string, homeDir: string = os.homedir()): string {
  const trimmed = (explicitDir || process.env.AX_INSTALL_DIR || '').trim();
  if (trimmed) {
    return path.resolve(trimmed);
  }
  const axHome = (process.env.AX_HOME || '').trim();
  if (axHome) {
    return path.resolve(axHome, 'bin');
  }
  return path.join(homeDir, '.ax', 'bin');
}

export function resolveBundledAxBinary(
  extensionPath: string,
  platform: NodeJS.Platform = process.platform,
  arch: string = process.arch
): string | null {
  const candidates = [
    path.join(extensionPath, 'bin', `ax-${platform}-${arch}`),
    path.join(extensionPath, 'bin', 'ax'),
  ];
  for (const candidate of candidates) {
    if (fs.existsSync(candidate) && fs.statSync(candidate).isFile()) {
      return candidate;
    }
  }
  return null;
}

function copyDirectoryRecursive(srcDir: string, dstDir: string): void {
  fs.mkdirSync(dstDir, { recursive: true });
  for (const entry of fs.readdirSync(srcDir, { withFileTypes: true })) {
    const srcPath = path.join(srcDir, entry.name);
    const dstPath = path.join(dstDir, entry.name);
    if (entry.isDirectory()) {
      copyDirectoryRecursive(srcPath, dstPath);
    } else if (entry.isFile()) {
      const tmpPath = `${dstPath}.tmp.${process.pid}`;
      fs.copyFileSync(srcPath, tmpPath);
      fs.renameSync(tmpPath, dstPath);
    }
  }
}

function findOnPath(binName: string): string | null {
  const pathEnv = process.env.PATH || '';
  for (const dir of pathEnv.split(path.delimiter)) {
    if (!dir) {
      continue;
    }
    const candidate = path.join(dir, binName);
    try {
      if (fs.existsSync(candidate) && fs.statSync(candidate).isFile()) {
        fs.accessSync(candidate, fs.constants.X_OK);
        return candidate;
      }
    } catch {
      // continue
    }
  }
  return null;
}

/**
 * Installs the bundled `ax` binary into `<installDir>/ax` (`~/.ax/bin/ax` by default)
 * and syncs the bundled `skills/ax/` directory (`SKILL.md` + `references/`) into
 * `~/.agents/skills/ax`, `~/.claude/skills/ax`, and `~/.gemini/skills/ax`.
 */
export function installAxCliAndSkills(options: InstallOptions): InstallResult {
  const homeDir = (options.skillHomeDir || process.env.AX_SKILL_HOME_DIR || '').trim() || os.homedir();
  const installDir = resolveInstallDir(options.installDir, homeDir);
  const log = options.log ?? (() => {});

  fs.mkdirSync(installDir, { recursive: true });
  const targetAxPath = path.join(installDir, 'ax');

  let axBinaryPath = targetAxPath;
  let installedFromBundle = false;

  const bundledBin = resolveBundledAxBinary(options.extensionPath, options.platform, options.arch);
  if (bundledBin) {
    if (path.resolve(bundledBin) !== path.resolve(targetAxPath)) {
      const tmpDest = `${targetAxPath}.tmp.${process.pid}`;
      fs.copyFileSync(bundledBin, tmpDest);
      fs.chmodSync(tmpDest, 0o755);
      fs.renameSync(tmpDest, targetAxPath);
    } else {
      fs.chmodSync(targetAxPath, 0o755);
    }
    installedFromBundle = true;
    log(`Installed bundled ax binary (${bundledBin}) -> ${targetAxPath}`);
  } else if (fs.existsSync(targetAxPath)) {
    log(`Using existing ax binary at ${targetAxPath}`);
  } else {
    const pathBin = findOnPath('ax');
    if (pathBin) {
      axBinaryPath = pathBin;
      log(`Using ax binary from PATH: ${pathBin}`);
    } else {
      log(`Warning: bundled ax binary not found under ${options.extensionPath}/bin and not on PATH`);
    }
  }

  const syncedSkillDirs: string[] = [];
  const bundledSkillDir = path.join(options.extensionPath, 'skills', 'ax');
  const bundledSkillMd = path.join(bundledSkillDir, 'SKILL.md');

  if (fs.existsSync(bundledSkillMd)) {
    for (const rootName of AGENT_SKILL_ROOTS) {
      const rootDir = path.join(homeDir, rootName);
      try {
        const skillsParent = path.join(rootDir, 'skills');
        if (fs.existsSync(skillsParent) && fs.lstatSync(skillsParent).isFile()) {
          const raw = fs.readFileSync(skillsParent, 'utf8').trim();
          const jsonDest = `${skillsParent}.json`;
          if (raw.startsWith('{') && !fs.existsSync(jsonDest)) {
            fs.renameSync(skillsParent, jsonDest);
            log(`Migrated misplaced JSON config file ${skillsParent} -> ${jsonDest}`);
          }
        }
        const dstSkillDir = path.join(skillsParent, 'ax');
        copyDirectoryRecursive(bundledSkillDir, dstSkillDir);
        syncedSkillDirs.push(dstSkillDir);
      } catch (err) {
        log(`Skipped syncing skill to ${rootDir}: ${(err as Error).message}`);
      }
    }
  }

  return {
    axBinaryPath,
    installDir,
    installedFromBundle,
    syncedSkillDirs,
  };
}

#!/usr/bin/env bash
# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Downloads and SHA-256 verifies the official released `ax` CLI binary from
# GitHub Releases into ./bin/ for bundling inside the VS Code extension (.vsix)
# without requiring Go or a local AX build.
#
# Accepts an optional VS Code target (linux-x64, linux-arm64, darwin-x64,
# darwin-arm64) as $1 or $VSCODE_TARGET.
# Set AX_RELEASE_VERSION (default: v0.4.0-rc.1) to pin a specific release tag,
# or AX_BUILD_FROM_SOURCE=1 only when explicitly testing unreleased local Go code.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
EXT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
AX_GITHUB_REPO="${AX_GITHUB_REPO:-google/ax}"
AX_RELEASE_VERSION="${AX_RELEASE_VERSION:-v0.4.0-rc.1}"
AX_BUILD_FROM_SOURCE="${AX_BUILD_FROM_SOURCE:-0}"

TARGET="${1:-${VSCODE_TARGET:-}}"

if [[ -n "${TARGET}" ]]; then
  case "${TARGET}" in
    linux-x64)    GOOS=linux;  GOARCH=amd64; NODE_ARCH=x64 ;;
    linux-arm64)  GOOS=linux;  GOARCH=arm64; NODE_ARCH=arm64 ;;
    darwin-x64)   GOOS=darwin; GOARCH=amd64; NODE_ARCH=x64 ;;
    darwin-arm64) GOOS=darwin; GOARCH=arm64; NODE_ARCH=arm64 ;;
    *)
      echo "error: unsupported VS Code target '${TARGET}' (expected linux-x64, linux-arm64, darwin-x64, darwin-arm64)" >&2
      exit 1
      ;;
  esac
else
  case "$(uname -s)" in
    Linux)  GOOS=linux ;;
    Darwin) GOOS=darwin ;;
    *)
      echo "error: unsupported host OS '$(uname -s)'" >&2
      exit 1
      ;;
  esac
  case "$(uname -m)" in
    x86_64|amd64)  GOARCH=amd64; NODE_ARCH=x64 ;;
    arm64|aarch64) GOARCH=arm64; NODE_ARCH=arm64 ;;
    *)
      echo "error: unsupported host arch '$(uname -m)'" >&2
      exit 1
      ;;
  esac
fi

mkdir -p "${EXT_DIR}/bin"
rm -f "${EXT_DIR}/bin/ax" "${EXT_DIR}/bin/ax-"* "${EXT_DIR}/bin/.ax-source-repo"
OUT_TARGET="${EXT_DIR}/bin/ax-${GOOS}-${NODE_ARCH}"

if [[ "${AX_BUILD_FROM_SOURCE}" == "1" ]]; then
  REPO_ROOT="$(cd "${EXT_DIR}/.." && pwd)"
  echo "==> [AX_BUILD_FROM_SOURCE=1] Building ax CLI (${GOOS}/${GOARCH}) from ${REPO_ROOT}..."
  (
    cd "${REPO_ROOT}"
    GOWORK=off GOTOOLCHAIN="${GOTOOLCHAIN:-auto}" CGO_ENABLED=0 GOOS="${GOOS}" GOARCH="${GOARCH}" \
      go build -trimpath -ldflags="-s -w" -o "${OUT_TARGET}" ./cmd/ax
  )
  chmod 0755 "${OUT_TARGET}"
  echo "==> Bundled source-built ax binary ready: ${OUT_TARGET}"
  exit 0
fi

ARCHIVE="ax_${GOOS}_${GOARCH}.tar.gz"
BASE_URL="https://github.com/${AX_GITHUB_REPO}/releases/download/${AX_RELEASE_VERSION}"
WORK_DIR="$(mktemp -d)"
trap 'rm -rf "${WORK_DIR}"' EXIT

echo "==> Downloading official ax CLI ${AX_RELEASE_VERSION} (${ARCHIVE}) from ${BASE_URL}..."
curl -fsSL "${BASE_URL}/${ARCHIVE}" -o "${WORK_DIR}/${ARCHIVE}"
curl -fsSL "${BASE_URL}/checksums.txt" -o "${WORK_DIR}/checksums.txt"

EXPECTED_SHA="$(awk -v f="${ARCHIVE}" '$2 == f { print $1 }' "${WORK_DIR}/checksums.txt")"
if [[ -z "${EXPECTED_SHA}" ]]; then
  echo "error: ${ARCHIVE} not found in ${BASE_URL}/checksums.txt" >&2
  exit 1
fi

if command -v sha256sum >/dev/null 2>&1; then
  ACTUAL_SHA="$(sha256sum "${WORK_DIR}/${ARCHIVE}" | awk '{ print $1 }')"
else
  ACTUAL_SHA="$(shasum -a 256 "${WORK_DIR}/${ARCHIVE}" | awk '{ print $1 }')"
fi

if [[ "${ACTUAL_SHA}" != "${EXPECTED_SHA}" ]]; then
  echo "error: SHA-256 mismatch for ${ARCHIVE}: expected ${EXPECTED_SHA}, got ${ACTUAL_SHA}" >&2
  exit 1
fi

tar -xzf "${WORK_DIR}/${ARCHIVE}" -C "${WORK_DIR}" ax
install -m 0755 "${WORK_DIR}/ax" "${OUT_TARGET}"
echo "==> Verified SHA-256 (${ACTUAL_SHA:0:12}...) and bundled ${AX_RELEASE_VERSION} ax binary: ${OUT_TARGET}"

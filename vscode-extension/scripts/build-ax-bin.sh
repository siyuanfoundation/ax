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

# Builds the `ax` CLI from ../cmd/ax into ./bin/ for bundling inside the VS Code
# extension (.vsix). Accepts an optional VS Code target (e.g. linux-x64,
# linux-arm64, darwin-x64, darwin-arm64) as $1 or $VSCODE_TARGET.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
EXT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
REPO_ROOT="$(cd "${EXT_DIR}/.." && pwd)"

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
  GOOS="$(cd "${REPO_ROOT}" && go env GOOS)"
  GOARCH="$(cd "${REPO_ROOT}" && go env GOARCH)"
  case "${GOARCH}" in
    amd64) NODE_ARCH=x64 ;;
    arm64) NODE_ARCH=arm64 ;;
    *)     NODE_ARCH="${GOARCH}" ;;
  esac
fi

mkdir -p "${EXT_DIR}/bin"
rm -f "${EXT_DIR}/bin/ax" "${EXT_DIR}/bin/ax-"*
OUT_TARGET="${EXT_DIR}/bin/ax-${GOOS}-${NODE_ARCH}"

echo "==> Building ax CLI (${GOOS}/${GOARCH}) into ${OUT_TARGET}..."
(
  cd "${REPO_ROOT}"
  GOWORK=off GOTOOLCHAIN="${GOTOOLCHAIN:-auto}" CGO_ENABLED=0 GOOS="${GOOS}" GOARCH="${GOARCH}" \
    go build -trimpath -ldflags="-s -w" -o "${OUT_TARGET}" ./cmd/ax
)
chmod 0755 "${OUT_TARGET}"
echo "==> Bundled ax binary ready: ${OUT_TARGET}"

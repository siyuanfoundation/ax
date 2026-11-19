# AX `ax.io/v1alpha1` Manifest Reference

All resources share `apiVersion: ax.io/v1alpha1` and RFC 1123 lowercase `metadata.name` / `metadata.atespace` (default `"default"`). Put `Gateway` and `Workspace` documents **before** `Task` documents in multi-document YAML files.

## Minimal Interactive Sandbox `Task`

Uses `ax-server`'s default `@sha256`-pinned task image and enables `ax ssh`:

```yaml
apiVersion: ax.io/v1alpha1
kind: Task
metadata:
  name: dev-sandbox
  atespace: default
spec:
  debug: true
  commands: ["sleep", "infinity"]
  env:
    - name: ENVIRONMENT
      value: "dev"
```

## Full `Gateway` + `Workspace` + `Task` Example

```yaml
apiVersion: ax.io/v1alpha1
kind: Gateway
metadata:
  name: github-and-llm-gw
  atespace: default
spec:
  egress:
    - https:
        hostnames: ["github.com", "api.github.com", "*.githubusercontent.com"]
    - https:
        hostnames: ["api.anthropic.com"]
        effects:
          replaceHeaders:
            - header: x-api-key
              credentialUri: "ate-secret://k8s.io/default/default/anthropic-creds/api-key"
---
apiVersion: ax.io/v1alpha1
kind: Workspace
metadata:
  name: repo-workspace
  atespace: default
spec:
  setup:
    git:
      - name: origin
        repo: "https://github.com/my-org/my-repo.git"
        ref: "main"
        dir: "."
        depth: 1
    files:
      - path: "AGENTS.md"
        content: |
          - Run unit tests before finishing.
    commands:
      - "go test ./..."
  mcp:
    servers:
      - name: local-git-mcp
        command: "git-mcp"
        args: ["--repo", "/workspace"]
---
apiVersion: ax.io/v1alpha1
kind: Task
metadata:
  name: fix-issue-42
  atespace: default
spec:
  profile:
    # Optional: omit image to use server default; if set, MUST be @sha256:... digest-pinned
    # image: "ghcr.io/my-org/agent@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
    resources:
      limits:
        cpu: "2"
        memory: "4Gi"
  workspace:
    name: repo-workspace
    path: "/workspace"
  gateway:
    name: github-and-llm-gw
  goal:
    text: "Fix failing unit tests in internal/auth and verify go test passes."
  env:
    - name: ANTHROPIC_API_KEY
      value: "placeholder"
  commands: ["sleep", "infinity"]
  debug: true
```

## `Model` Example

```yaml
apiVersion: ax.io/v1alpha1
kind: Model
metadata:
  name: default-model
  atespace: default
spec:
  provider: google
  model: gemini-3.8-flash
  secretKey:
    name: gemini-api-secret
    key: GEMINI_API_KEY
```

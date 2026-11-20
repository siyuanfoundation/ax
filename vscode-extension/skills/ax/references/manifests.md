# AX `ax.io/v1alpha1` Manifest Reference

All resources share `apiVersion: ax.io/v1alpha1` and RFC 1123 lowercase `metadata.name` / `metadata.atespace` (default `"default"`). Put `Gateway` and `Workspace` documents **before** `Task` documents in multi-document YAML files.

## Parallel Multi-Task Fan-Out Example (1 `Gateway` + 1 Warm `Workspace` + N `Task`s)

```yaml
apiVersion: ax.io/v1alpha1
kind: Gateway
metadata:
  name: pr-review-gw
  atespace: default
spec:
  egress:
    - https:
        hostnames: ["github.com", "api.github.com", "*.githubusercontent.com"]
    - https:
        hostnames: ["generativelanguage.googleapis.com"]
        effects:
          replaceHeaders:
            - header: x-goog-api-key
              credentialUri: "ate-secret://k8s.io/default/ax-system/gemini-api-creds/api-key"
---
apiVersion: ax.io/v1alpha1
kind: Workspace
metadata:
  name: repo-warm-ws
  atespace: default
spec:
  profile:
    resources:
      limits:
        cpu: "1"
        memory: "1Gi"
  setup:
    gateway:
      name: pr-review-gw
    git:
      - name: origin
        repo: "https://github.com/etcd-io/etcd.git"
        ref: "main"
        dir: "repo"
        depth: 1
    files:
      - path: "AGENTS.md"
        content: |
          - Focus on correctness, concurrency/distributed safety, resource bounds, and tests.
      - path: "review_pr.py"
        content: |
          import json, os, ssl, urllib.request
          pr_num = os.environ["PR_NUMBER"]
          repo = os.environ.get("TARGET_REPO", "etcd-io/etcd")
          title = os.environ.get("PR_TITLE", "")
          out_file = os.environ.get("AX_OUTPUT_FILE", "/workspace/.ax/output")
          os.makedirs(os.path.dirname(out_file), exist_ok=True)
          ctx = ssl.create_default_context(cafile=os.environ.get("SSL_CERT_FILE"))
          diff_req = urllib.request.Request(
              f"https://patch-diff.githubusercontent.com/raw/{repo}/pull/{pr_num}.diff",
              headers={"User-Agent": "ax-reviewer/1.0"},
          )
          diff_txt = urllib.request.urlopen(diff_req, context=ctx, timeout=45).read().decode("utf-8", "replace")[:18000]
          body = json.dumps({
              "contents": [{"parts": [{"text": f"Review PR #{pr_num} ({title}) in {repo}:\n```diff\n{diff_txt}\n```\nStart line 1 with `Verdict: LGTM` or `Verdict: LGTM with minor nits` or `Verdict: Needs attention`, followed by Summary and Key Findings."}]}],
              "generationConfig": {"temperature": 0.2, "maxOutputTokens": 3072, "thinkingConfig": {"thinkingLevel": "LOW"}},
          }).encode()
          req = urllib.request.Request(
              "https://generativelanguage.googleapis.com/v1beta/models/gemini-3-flash-preview:generateContent",
              data=body,
              method="POST",
              headers={"Content-Type": "application/json", "x-goog-api-key": os.environ.get("GEMINI_API_KEY", "placeholder")},
          )
          resp = json.loads(urllib.request.urlopen(req, context=ctx, timeout=90).read().decode())
          parts = resp.get("candidates", [{}])[0].get("content", {}).get("parts", [])
          review = "".join(p.get("text", "") for p in parts if not p.get("thought")).strip()
          with open(out_file, "w", encoding="utf-8") as f:
              f.write(f"## PR #{pr_num}: {title}\n\n{review}\n")
---
# Repeat one Task per PR — omit spec.profile so every Task boots warm from repo-warm-ws!
apiVersion: ax.io/v1alpha1
kind: Task
metadata:
  name: etcd-pr-22875
  atespace: default
spec:
  workspace:
    name: repo-warm-ws
  gateway:
    name: pr-review-gw
  env:
    - name: GEMINI_API_KEY
      value: "placeholder"
    - name: TARGET_REPO
      value: "etcd-io/etcd"
    - name: PR_NUMBER
      value: "22875"
    - name: PR_TITLE
      value: "client: limit size of TLS and YAML config files"
  commands: ["python3", "/workspace/review_pr.py"]
  debug: true
```

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

# ShellCheck GitHub Workflow Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Harden the existing ShellCheck GitHub workflow to lint all shell scripts in the repo (not just `install.sh`), fix existing warnings, and enforce strictest settings.

**Architecture:** Replace the hardcoded single-file shellcheck invocation with a dynamic `find`-based approach that catches all `.sh` files. Fix existing shellcheck warnings in `tests/docker/entrypoint.sh`. Both scripts use `#!/bin/sh` (POSIX sh), so `--shell=sh` stays appropriate as a default, but we switch to auto-detection so future scripts with different shebangs work correctly.

**Tech Stack:** GitHub Actions, ShellCheck (pre-installed on `ubuntu-latest`)

---

### Task 1: Fix shellcheck warnings in `tests/docker/entrypoint.sh`

**Files:**
- Modify: `tests/docker/entrypoint.sh:7`

**Step 1: Run shellcheck locally to confirm current warnings**

Run: `shellcheck --shell=sh --severity=style --enable=all --external-sources --format=gcc ./tests/docker/entrypoint.sh`
Expected output:
```
./tests/docker/entrypoint.sh:7:9: note: Prefer double quoting even when variables don't contain special characters. [SC2248]
./tests/docker/entrypoint.sh:7:9: note: Prefer putting braces around variable references even when not strictly required. [SC2250]
```

**Step 2: Fix the warnings**

Change line 7 in `tests/docker/entrypoint.sh` from:
```sh
while [ $elapsed -lt 60 ]; do
```
to:
```sh
while [ "${elapsed}" -lt 60 ]; do
```

**Step 3: Run shellcheck again to verify clean**

Run: `shellcheck --shell=sh --severity=style --enable=all --external-sources --format=gcc ./tests/docker/entrypoint.sh`
Expected: No output (clean)

**Step 4: Commit**

```bash
git add tests/docker/entrypoint.sh
git commit -m "fix: resolve shellcheck warnings in entrypoint.sh"
```

---

### Task 2: Update the ShellCheck workflow to lint all shell scripts

**Files:**
- Modify: `.github/workflows/shellcheck.yml`

**Step 1: Replace the workflow file with the improved version**

Replace the entire content of `.github/workflows/shellcheck.yml` with:

```yaml
name: ShellCheck

on:
  push:
    branches: [main]
    paths: ["**.sh"]
  pull_request:
    branches: [main]
    paths: ["**.sh"]

concurrency:
  group: ${{ github.workflow }}-${{ github.ref }}
  cancel-in-progress: true

jobs:
  shellcheck:
    name: ShellCheck
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Collect shell scripts
        id: collect
        run: |
          files=$(find . -name '*.sh' -type f | sort)
          if [ -z "${files}" ]; then
            echo "No .sh files found"
            echo "skip=true" >> "$GITHUB_OUTPUT"
          else
            echo "Found shell scripts:"
            echo "${files}"
            echo "skip=false" >> "$GITHUB_OUTPUT"
          fi

      - name: Run ShellCheck
        if: steps.collect.outputs.skip != 'true'
        run: |
          shellcheck --version
          find . -name '*.sh' -type f -print0 \
            | xargs -0 shellcheck \
              --severity=style \
              --enable=all \
              --external-sources \
              --check-sourced \
              --format=gcc
```

Key changes from the existing workflow:
- **Finds all `.sh` files dynamically** instead of hardcoding `./install.sh`
- **Removed `--shell=sh`** — lets shellcheck auto-detect from the shebang, so bash scripts (if any are added later) get proper checks too
- **Added `--check-sourced`** — also checks files that are sourced by other scripts
- **Added a skip step** — gracefully handles the (unlikely) case where the paths filter triggers but no `.sh` files exist
- **Uses `-print0` / `xargs -0`** — handles filenames with spaces safely

**Step 2: Validate the workflow YAML syntax**

Run: `python3 -c "import yaml; yaml.safe_load(open('.github/workflows/shellcheck.yml'))" && echo "YAML valid"`
Expected: `YAML valid`

**Step 3: Dry-run the shellcheck command locally to verify it passes**

Run: `find . -name '*.sh' -type f -print0 | xargs -0 shellcheck --severity=style --enable=all --external-sources --check-sourced --format=gcc`
Expected: No output (clean — since we fixed entrypoint.sh in Task 1)

**Step 4: Commit**

```bash
git add .github/workflows/shellcheck.yml
git commit -m "ci: shellcheck all .sh files with strictest settings"
```

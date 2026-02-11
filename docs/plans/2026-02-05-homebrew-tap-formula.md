# Homebrew Tap Formula for git-fs (v2 — versioned releases)

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** On each promote-to-latest, create a permanent versioned GitHub release AND update the Homebrew tap with both an updated `git-fs.rb` (latest) and a new versioned `git-fs@{version}.rb` formula.

**Architecture:** The promote-to-latest workflow gets two new capabilities: (1) it creates a permanent versioned release tag (e.g., `v0.1.1-alpha.1`) in addition to the ephemeral `latest` tag, and (2) it pushes a commit to `mesa-dot-dev/homebrew-tap` that updates `Formula/git-fs.rb` and creates `Formula/git-fs@{version}.rb`. Both formulas point to the permanent versioned release URL.

**Tech Stack:** Homebrew Ruby formula, GitHub Actions, bash, `gh` CLI

---

### Task 1: Add outputs and versioned release to the promote job

**Files:**
- Modify: `.github/workflows/promote-to-latest.yml` (the `promote` job only)

**Step 1: Add `outputs` to the `promote` job**

The `update-homebrew` job needs the version and tag from `promote`. Add an `outputs` block to the `promote` job so downstream jobs can access them:

```yaml
  promote:
    name: Promote canary to latest
    runs-on: ubuntu-latest
    outputs:
      version: ${{ steps.canary.outputs.version }}
      base_version: ${{ steps.canary.outputs.base_version }}
      tag: ${{ steps.canary.outputs.tag }}
      target: ${{ steps.canary.outputs.target }}
    steps:
      ...
```

**Step 2: Extract the base version (strip `+sha`) in the "Find latest canary release" step**

After the existing `VERSION` extraction (line ~42), add:

```bash
          BASE_VERSION=$(echo "${VERSION}" | sed 's/+.*//')
          echo "base_version=${BASE_VERSION}" >> "$GITHUB_OUTPUT"
```

**Step 3: Add a new step to create the permanent versioned release**

After the "Create latest release" step (line ~86), add a new step:

```yaml
      - name: Create versioned release
        env:
          GH_TOKEN: ${{ github.token }}
          VERSION: ${{ steps.canary.outputs.version }}
          BASE_VERSION: ${{ steps.canary.outputs.base_version }}
          TARGET: ${{ steps.canary.outputs.target }}
        run: |
          TAG="v${BASE_VERSION}"
          if gh release view "${TAG}" &>/dev/null; then
            echo "Release ${TAG} already exists, skipping."
            exit 0
          fi
          gh release create "${TAG}" \
            --title "git-fs ${BASE_VERSION}" \
            --notes "Stable release of git-fs ${BASE_VERSION}." \
            --target "${TARGET}" \
            assets/*
```

**Step 4: Commit**

```bash
git add .github/workflows/promote-to-latest.yml
git commit -m "feat: add versioned release to promote workflow"
```

---

### Task 2: Rewrite the update-homebrew job

**Files:**
- Modify: `.github/workflows/promote-to-latest.yml` (the `update-homebrew` job only)

Replace the entire `update-homebrew` job with the version below. Key changes:
- Reads `base_version` from promote outputs
- Downloads tarball from the versioned release tag (not `latest`)
- Updates `Formula/git-fs.rb` with new `url`, `sha256`, and `version`
- Creates a new `Formula/git-fs@{version}.rb` with the correct Homebrew class name
- Commits both files in a single push

```yaml
  update-homebrew:
    name: Update Homebrew formula
    needs: [promote]
    runs-on: ubuntu-latest
    steps:
      - name: Download macOS universal tarball
        env:
          GH_TOKEN: ${{ github.token }}
        run: |
          TAG="v${{ needs.promote.outputs.base_version }}"
          curl -fSL -o git-fs-macos-universal.tar.gz \
            "https://github.com/${{ github.repository }}/releases/download/${TAG}/git-fs-macos-universal.tar.gz"

      - name: Compute SHA256
        id: sha
        run: |
          SHA=$(sha256sum git-fs-macos-universal.tar.gz | cut -d' ' -f1)
          echo "sha256=${SHA}" >> "$GITHUB_OUTPUT"
          echo "SHA256: ${SHA}"

      - name: Update tap formulae
        env:
          TAP_TOKEN: ${{ secrets.HOMEBREW_TAP_TOKEN }}
          BASE_VERSION: ${{ needs.promote.outputs.base_version }}
          SHA256: ${{ steps.sha.outputs.sha256 }}
        run: |
          git clone "https://x-access-token:${TAP_TOKEN}@github.com/mesa-dot-dev/homebrew-tap.git" tap
          cd tap

          TAG="v${BASE_VERSION}"
          URL="https://github.com/mesa-dot-dev/git-fs/releases/download/${TAG}/git-fs-macos-universal.tar.gz"

          # Compute Homebrew class name for versioned formula
          # git-fs@0.1.1-alpha.1 → GitFsAT011Alpha1
          CLASS_NAME=$(ruby -e "
            name = 'git-fs@${BASE_VERSION}'
            class_name = name.capitalize
            class_name.gsub!(/[-_.\s]([a-zA-Z0-9])/) { \$1.upcase }
            class_name.tr!('+', 'x')
            class_name.sub!(/(.)@(\d)/, '\1AT\2')
            puts class_name
          ")
          FORMULA_FILE="Formula/git-fs@${BASE_VERSION}.rb"

          # Update Formula/git-fs.rb (latest)
          cat > Formula/git-fs.rb << FORMULA
          class GitFs < Formula
            desc "Mount Mesa, GitHub and GitLab repositories as local filesystems via FUSE"
            homepage "https://github.com/mesa-dot-dev/git-fs"
            version "${BASE_VERSION}"
            url "${URL}"
            sha256 "${SHA256}"
            license "MIT"

            depends_on :macos
            depends_on cask: "macfuse"

            def install
              bin.install "git-fs"
            end

            test do
              assert_match "git-fs", shell_output("#{bin}/git-fs --version", 2)
            end
          end
          FORMULA

          # Create versioned formula (e.g., Formula/git-fs@0.1.1-alpha.1.rb)
          cat > "${FORMULA_FILE}" << FORMULA
          class ${CLASS_NAME} < Formula
            desc "Mount Mesa, GitHub and GitLab repositories as local filesystems via FUSE"
            homepage "https://github.com/mesa-dot-dev/git-fs"
            version "${BASE_VERSION}"
            url "${URL}"
            sha256 "${SHA256}"
            license "MIT"

            depends_on :macos
            depends_on cask: "macfuse"

            def install
              bin.install "git-fs"
            end

            test do
              assert_match "git-fs", shell_output("#{bin}/git-fs --version", 2)
            end
          end
          FORMULA

          git config user.name "github-actions[bot]"
          git config user.email "github-actions[bot]@users.noreply.github.com"
          git add Formula/
          git diff --cached --quiet && echo "No changes to commit" && exit 0
          git commit -m "git-fs ${BASE_VERSION}"
          git push
```

**Important detail about heredoc indentation:** The `cat > file << FORMULA` heredocs above must produce Ruby files with **no leading indentation** (Homebrew requires the class definition at column 0, methods indented 2 spaces). The heredocs in the YAML `run:` block must be written so the output has correct Ruby indentation — i.e., the content lines inside the heredoc should NOT be indented relative to the YAML block. Use `<<-FORMULA` with tab-stripping or write the content flush-left.

**Step 1: Replace the update-homebrew job in the workflow file**

Delete lines 102-135 (the current `update-homebrew` job) and replace with the YAML above.

**Step 2: Verify YAML indentation is correct**

The `update-homebrew` job must be at the same indent level as `promote` (2 spaces under `jobs:`).

**Step 3: Commit**

```bash
git add .github/workflows/promote-to-latest.yml
git commit -m "feat: versioned Homebrew formulae on promote-to-latest"
```

---

### Task 3: Update the initial formula in the tap repo

**Files:**
- Modify: `Formula/git-fs.rb` (in `mesa-dot-dev/homebrew-tap` repo)

The formula currently has no `version` field and points to the `latest` download URL. Update it to match the structure that CI will maintain, so the first CI run doesn't produce a confusing diff.

**Step 1: Update the formula**

Push directly to main in the tap repo:

```bash
cd /tmp
rm -rf homebrew-tap-update
gh repo clone mesa-dot-dev/homebrew-tap homebrew-tap-update
cd homebrew-tap-update
```

Overwrite `Formula/git-fs.rb` with:

```ruby
class GitFs < Formula
  desc "Mount Mesa, GitHub and GitLab repositories as local filesystems via FUSE"
  homepage "https://github.com/mesa-dot-dev/git-fs"
  version "0.0.0"
  url "https://github.com/mesa-dot-dev/git-fs/releases/download/v0.0.0/git-fs-macos-universal.tar.gz"
  sha256 "PLACEHOLDER"
  license "MIT"

  depends_on :macos
  depends_on cask: "macfuse"

  def install
    bin.install "git-fs"
  end

  test do
    assert_match "git-fs", shell_output("#{bin}/git-fs --version", 2)
  end
end
```

**Step 2: Commit and push**

```bash
git add Formula/git-fs.rb
git commit -m "Add version field to formula template"
git push
```

---

## Verification

After all tasks are complete:

1. Push the workflow changes to `main` in `mesa-dot-dev/git-fs`
2. Run the `Promote to Latest` workflow manually from GitHub Actions
3. Verify a new permanent release `v0.1.1-alpha.1` exists alongside `latest`
4. Verify `mesa-dot-dev/homebrew-tap` has both:
   - `Formula/git-fs.rb` — updated with real SHA256 and versioned URL
   - `Formula/git-fs@0.1.1-alpha.1.rb` — new file with same content and correct class name
5. Test install: `brew tap mesa-dot-dev/homebrew-tap && brew install git-fs`
6. Test versioned install: `brew install mesa-dot-dev/homebrew-tap/git-fs@0.1.1-alpha.1`

## User install flow

```bash
# Latest version
brew tap mesa-dot-dev/homebrew-tap
brew install git-fs

# Specific version
brew install mesa-dot-dev/homebrew-tap/git-fs@0.1.1-alpha.1
```

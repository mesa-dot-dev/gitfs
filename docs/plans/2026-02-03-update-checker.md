# Update Checker Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Check at startup whether the user is running the latest released version of git-fs, and warn them if not.

**Architecture:** Use the `self_update` crate to fetch the latest GitHub release from `mesa-dot-dev/git-fs`. Since releases use `canary-{short_sha}` tags (not semver), we embed the git commit SHA at build time and compare it against the latest release tag. If they differ, log an `error!` but continue execution normally.

**Tech Stack:** `self_update` (GitHub backend), `vergen-gitcl` (build-time git SHA embedding)

---

### Task 1: Add dependencies to Cargo.toml

**Files:**
- Modify: `Cargo.toml`

**Step 1: Add `self_update` and `vergen-gitcl` to Cargo.toml**

Add to `[dependencies]`:
```toml
self_update = { version = "0.42", default-features = false, features = ["rustls"] }
```

Add a new section:
```toml
[build-dependencies]
vergen-gitcl = { version = "1", features = ["build"] }
```

The `self_update` crate is used to query GitHub releases. We disable default features and enable `rustls` to avoid linking OpenSSL. `vergen-gitcl` embeds the git short SHA at compile time so the binary knows what commit it was built from.

**Step 2: Create `build.rs` to embed git SHA**

Create: `build.rs`

```rust
use vergen_gitcl::{BuildBuilder, Emitter, GitclBuilder};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let build = BuildBuilder::default().build_timestamp(false).build()?;
    let gitcl = GitclBuilder::default().sha(true).build()?;

    Emitter::default()
        .add_instructions(&build)?
        .add_instructions(&gitcl)?
        .emit()?;

    Ok(())
}
```

This makes `VERGEN_GIT_SHA` available as an environment variable at compile time.

**Step 3: Verify it compiles**

Run: `cargo check`
Expected: Compiles without errors

**Step 4: Commit**

```bash
git add Cargo.toml Cargo.lock build.rs
git commit -m "feat: add self_update and vergen-gitcl dependencies"
```

---

### Task 2: Create `src/updates.rs` with update check logic

**Files:**
- Create: `src/updates.rs`
- Modify: `src/main.rs` (add `mod updates;`)

**Step 1: Create `src/updates.rs`**

```rust
//! Checks whether the running binary is the latest released version.

use tracing::{error, info};

/// The git SHA baked in at compile time by `vergen-gitcl`.
const BUILD_SHA: &str = env!("VERGEN_GIT_SHA");

/// Check GitHub for the latest release and warn if this binary is outdated.
///
/// This function never fails the application — it logs errors and returns.
pub fn check_for_updates() {
    let short_sha = &BUILD_SHA[..7.min(BUILD_SHA.len())];

    let releases = match self_update::backends::github::ReleaseList::configure()
        .repo_owner("mesa-dot-dev")
        .repo_name("git-fs")
        .build()
    {
        Ok(list) => match list.fetch() {
            Ok(releases) => releases,
            Err(e) => {
                info!("Could not check for updates: {e}");
                return;
            }
        },
        Err(e) => {
            info!("Could not configure update check: {e}");
            return;
        }
    };

    let Some(latest) = releases.first() else {
        info!("No releases found on GitHub.");
        return;
    };

    // Release tags are "canary-{short_sha}". Extract the SHA suffix.
    let latest_sha = latest
        .version
        .strip_prefix("canary-")
        .unwrap_or(&latest.version);

    if short_sha == latest_sha {
        info!("You are running the latest version ({short_sha}).");
    } else {
        error!(
            "You are running git-fs built from commit {short_sha}, \
             but the latest release is from commit {latest_sha}. \
             Please update: https://github.com/mesa-dot-dev/git-fs/releases"
        );
    }
}
```

**Step 2: Register the module in `src/main.rs`**

Add `mod updates;` after the existing `mod fs;` line (line 10 of `src/main.rs`):

```rust
mod app_config;
mod daemon;
mod fs;
mod updates;
```

**Step 3: Verify it compiles**

Run: `cargo check`
Expected: Compiles without errors (there will be a "function never used" warning, which is fine — we call it in the next task)

**Step 4: Commit**

```bash
git add src/updates.rs src/main.rs
git commit -m "feat: add update checker module"
```

---

### Task 3: Call `check_for_updates()` from main

**Files:**
- Modify: `src/main.rs`

**Step 1: Add the update check call in `main()`**

In `src/main.rs`, add the call right after tracing is initialized and before argument parsing (after line 51, before `let args = Args::parse();`):

```rust
fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_span_events(FmtSpan::ENTER | FmtSpan::CLOSE)
        .init();

    updates::check_for_updates();

    let args = Args::parse();
    // ... rest of main
```

**Step 2: Verify it compiles**

Run: `cargo check`
Expected: Compiles without errors or warnings

**Step 3: Run the binary to verify update check works**

Run: `cargo run -- --help`
Expected: You should see either the "latest version" info log or the "please update" error log (depending on whether your local commit matches the latest release), followed by the normal help output.

**Step 4: Commit**

```bash
git add src/main.rs
git commit -m "feat: check for updates on startup"
```

---

### Task 4: Verify clippy and formatting pass

**Files:** (no changes expected, just verification)

**Step 1: Run clippy**

Run: `cargo clippy -- -D warnings`
Expected: No errors or warnings. If there are issues (e.g., the strict lint config may flag `expect_used` or `unwrap_used`), fix them by replacing with match/if-let as needed.

**Step 2: Run rustfmt**

Run: `cargo fmt --check`
Expected: No formatting issues.

**Step 3: Commit any fixes if needed**

```bash
git add -A
git commit -m "fix: address clippy and formatting issues"
```

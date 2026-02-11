# Mount Directory Interlock Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Ensure the mount directory is created (including all parents) before mounting, and return an error if the directory already exists and is non-empty.

**Architecture:** Add a `prepare_mount_point` function in `daemon.rs` that checks whether the mount directory exists and is non-empty (error), creates it with `create_dir_all` if it doesn't exist (logging via `info!`), or proceeds silently if it exists and is empty. This runs in `daemon::run` before spawning the FUSE session. The existing validation in `app_config.rs` that checks for a parent directory is no longer needed since `create_dir_all` handles the full path.

**Tech Stack:** Rust std (`std::fs`, `tokio::fs`), `tracing` (`info!`)

---

### Task 1: Add `prepare_mount_point` function to `daemon.rs`

**Files:**
- Modify: `src/daemon.rs`

**Step 1: Write the `prepare_mount_point` function**

Add the following function after the `managed_fuse` module (before `wait_for_exit`), around line 141:

```rust
/// Prepares the mount point directory.
///
/// - If the directory exists and is non-empty, returns an error.
/// - If the directory does not exist, creates it (including parents) and logs an info message.
/// - If the directory exists and is empty, does nothing.
async fn prepare_mount_point(mount_point: &std::path::Path) -> Result<(), std::io::Error> {
    match tokio::fs::read_dir(mount_point).await {
        Ok(mut entries) => {
            if entries.next_entry().await?.is_some() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::AlreadyExists,
                    format!(
                        "Mount point '{}' already exists and is not empty.",
                        mount_point.display()
                    ),
                ));
            }
            Ok(())
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            tokio::fs::create_dir_all(mount_point).await?;
            info!(path = %mount_point.display(), "Created mount point directory.");
            Ok(())
        }
        Err(e) => Err(e),
    }
}
```

The logic is:
- `read_dir` succeeds → directory exists. Check if it has any entry; if so, error out.
- `read_dir` fails with `NotFound` → directory doesn't exist. Create it and log.
- `read_dir` fails with another error → propagate it (e.g., permission denied).

**Step 2: Call `prepare_mount_point` in `daemon::run`**

In the `run` function (`src/daemon.rs:162`), add the call **before** `ManagedFuse::new`. The function currently looks like:

```rust
pub async fn run(
    config: app_config::Config,
    handle: tokio::runtime::Handle,
) -> Result<(), std::io::Error> {
    // Spawn the cache if it doesn't exist.
    tokio::fs::create_dir_all(&config.cache.path).await?;

    debug!(config = ?config, "Starting git-fs daemon...");

    let fuse = managed_fuse::ManagedFuse::new(&config);
```

Change it to:

```rust
pub async fn run(
    config: app_config::Config,
    handle: tokio::runtime::Handle,
) -> Result<(), std::io::Error> {
    // Spawn the cache if it doesn't exist.
    tokio::fs::create_dir_all(&config.cache.path).await?;

    prepare_mount_point(&config.mount_point).await?;

    debug!(config = ?config, "Starting git-fs daemon...");

    let fuse = managed_fuse::ManagedFuse::new(&config);
```

**Step 3: Add `info` to the tracing imports**

The file currently imports `use tracing::{debug, error};` at line 5. Change to:

```rust
use tracing::{debug, error, info};
```

**Step 4: Verify it compiles**

Run: `cargo check`
Expected: No errors.

**Step 5: Commit**

```bash
git add src/daemon.rs
git commit -m "feat: add mount point interlock - create dir or error if non-empty"
```

---

### Task 2: Remove stale mount_point parent validation from `app_config.rs`

**Files:**
- Modify: `src/app_config.rs`

**Step 1: Remove the mount_point parent check**

In `Config::validate()` (`src/app_config.rs:225`), remove the mount_point parent directory validation block (lines 235-240):

```rust
        // REMOVE THIS BLOCK:
        if self.mount_point.parent().is_none() {
            errors.push(format!(
                "Mount point path '{}' has no parent directory.",
                self.mount_point.display()
            ));
        }
```

This check is no longer needed because `prepare_mount_point` in `daemon.rs` now calls `create_dir_all` which handles the full path including all parents. The only path that has no parent is `/`, and that's not a valid mount point for other reasons (the non-empty check will catch it).

**Step 2: Verify it compiles**

Run: `cargo check`
Expected: No errors.

**Step 3: Commit**

```bash
git add src/app_config.rs
git commit -m "refactor: remove stale mount_point parent validation"
```

---

### Task 3: Manual smoke test

**Step 1: Test with a non-existent mount point**

```bash
# Pick a temp path that doesn't exist
export TEST_MNT=$(mktemp -d)/git-fs-test-mnt
rmdir "$(dirname "$TEST_MNT")"  # remove so the full path is gone
cargo run -- --config-path /dev/null run  # uses default mount point
# Or set GIT_FS_MOUNT_POINT=$TEST_MNT
```

Expected: The directory is created and an `info` log line appears saying "Created mount point directory."

**Step 2: Test with a non-empty mount point**

```bash
mkdir -p /tmp/git-fs-nonempty-test
touch /tmp/git-fs-nonempty-test/somefile
GIT_FS_MOUNT_POINT=/tmp/git-fs-nonempty-test cargo run -- run
```

Expected: Error message about mount point not being empty. Process exits with an error.

**Step 3: Test with an existing empty mount point**

```bash
mkdir -p /tmp/git-fs-empty-test
GIT_FS_MOUNT_POINT=/tmp/git-fs-empty-test cargo run -- run
```

Expected: No error about the directory, proceeds to mount normally.

**Step 4: Clean up**

```bash
rm -rf /tmp/git-fs-nonempty-test /tmp/git-fs-empty-test
```

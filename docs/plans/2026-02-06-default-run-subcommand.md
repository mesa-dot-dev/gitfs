# Default `run` Subcommand Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Make `run` the default subcommand so `git-fs` works without explicitly typing `run`.

**Architecture:** Use clap's `Args::command` as `Option<Command>` and default to `Run { daemonize: false }` when no subcommand is provided. This is the idiomatic clap approach — no external crates or hacks needed.

**Tech Stack:** Rust, clap 4.x (derive)

---

### Task 1: Make `run` the default subcommand

**Files:**
- Modify: `src/main.rs:30-31` (change `command` field to `Option<Command>`)
- Modify: `src/main.rs:56` (unwrap_or default to `Run`)

**Step 1: Change the `Args` struct to make `command` optional**

In `src/main.rs`, change the `Args` struct:

```rust
struct Args {
    #[arg(
        short,
        long,
        value_parser,
        help = "Optional path to a mesa config TOML."
    )]
    config_path: Option<PathBuf>,

    #[command(subcommand)]
    command: Option<Command>,
}
```

**Step 2: Default to `Run` when no subcommand is given**

In the `main()` function, change:

```rust
    match args.command {
```

to:

```rust
    match args.command.unwrap_or(Command::Run { daemonize: false }) {
```

**Step 3: Build and verify it compiles**

Run: `cargo check`
Expected: Compiles with no errors.

**Step 4: Manual smoke test**

Run: `cargo build && ./target/debug/git-fs --help`
Expected: Help text shows `run` and `reload` as subcommands, but `run` is no longer required.

Run: `cargo build && ./target/debug/git-fs` (without `run`)
Expected: Behaves the same as `git-fs run` (attempts to start the daemon).

**Step 5: Commit**

```bash
git add src/main.rs
git commit -m "MES-707: make run the default subcommand"
```

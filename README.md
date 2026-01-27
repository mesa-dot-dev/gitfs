# git-fs

Mount GitHub repositories as local filesystems via FUSE, powered by the [Mesa](https://mesa.dev) API. No cloning required.

## Workspace

| Crate | Description |
|-------|-------------|
| [`git-fs`](crates/git-fs/) | FUSE binary that mounts a repo as a read-only filesystem |
| [`mesa-dev`](crates/mesa-dev/) | Rust SDK for the mesa.dev API |

## Quick start

```bash
# Build
cargo build --release

# Mount a repository (reads MESA_API_KEY from env if --mesa-api-key is omitted)
export MESA_API_KEY="your-api-key"
./target/release/git-fs rust-lang/rust /mnt/rust

# Or pass the key directly
./target/release/git-fs rust-lang/rust /mnt/rust --mesa-api-key your-api-key

# Pin to a specific branch, tag, or commit
./target/release/git-fs rust-lang/rust /mnt/rust --ref main
```

Requires [macFUSE](https://osxfuse.github.io/) on macOS or `libfuse` on Linux.

See the individual crate READMEs for more details.

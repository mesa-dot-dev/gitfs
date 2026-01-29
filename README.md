# git-fs

FUSE filesystem that mounts a GitHub repository as a read-only local directory, without cloning.

## Usage

```bash
git-fs <org/repo> <mountpoint> --mesa-api-key <KEY> [--ref <REF>]
```

The API key can also be provided via the `MESA_API_KEY` environment variable:

```bash
export MESA_API_KEY="your-api-key"
git-fs rust-lang/rust /mnt/rust
```

Pin to a specific branch, tag, or commit SHA with `--ref`:

```bash
git-fs rust-lang/rust /mnt/rust --ref v1.80.0
```

## Requirements

- **macOS**: [macFUSE](https://osxfuse.github.io/)
- **Linux**: `libfuse` (e.g. `apt install libfuse-dev` or `dnf install fuse-devel`)

## Architecture

FUSE operations are handled by `MesaFS`, which delegates to an inode cache (`SsFs`) backed by the Mesa API via `mesa-dev`. Directory contents and file data are fetched lazily on first access, with request deduplication and speculative prefetching of subdirectories.

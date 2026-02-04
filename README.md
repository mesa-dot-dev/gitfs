# gitfs

Mount Mesa repositories as **read-only** local directories without cloning.

`gitfs` is a FUSE-based virtual filesystem ideal for agents, CI pipelines, and
large monorepos where a full clone is impractical. Supports macOS and Linux.

> **Alpha Software** — GitFS is early-stage. If you run into issues, please
> [open an issue](https://github.com/mesa-dot-dev/gitfs/issues).

## Quick Start

Install from [GitHub
Releases](https://github.com/mesa-dot-dev/gitfs/releases/latest), then:

```bash
gitfs run
```

`gitfs` generates a default config and creates a mount directory automatically.
Browse any public GitHub repo:

```bash
ls /run/user/$(id -u)/gitfs/mnt/github/daytonaio/daytona   # Linux
ls ~/Library/Application\ Support/gitfs/mnt/github/daytonaio/daytona  # macOS
```

Hit `Ctrl+C` to stop — `gitfs` cleans up after itself.

## Documentation

For full documentation — installation for all platforms, configuration, Mesa
org setup, daemon mode, and more — visit:

**[docs.mesa.dev/content/guides/gitfs](https://docs.mesa.dev/content/guides/gitfs)**

## License

MIT

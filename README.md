# mesafs

Mount Mesa repositories as **read-only** local directories without cloning.

`mesafs` is a FUSE-based virtual filesystem ideal for agents, CI pipelines, and
large monorepos where a full clone is impractical. Supports macOS and Linux.

> **Alpha Software** - mesafs is early-stage. If you run into issues, please
> [open an issue](https://github.com/mesa-dot-dev/mesafs/issues).

## Quick Start

Install from [GitHub
Releases](https://github.com/mesa-dot-dev/mesafs/releases/latest), then:

```bash
mesafs run
```

`mesafs` generates a default config and creates a mount directory automatically.
Browse any public GitHub repo:

```bash
ls /run/user/$(id -u)/mesafs/mnt/github/daytonaio/daytona   # Linux
ls ~/Library/Application\ Support/mesafs/mnt/github/daytonaio/daytona  # macOS
```

Hit `Ctrl+C` to stop.

## Documentation

For full documentation visit: **[docs.mesa.dev](https://docs.mesa.dev/content/guides/mesafs)**

#!/bin/sh
set -eu

# --- Constants ---
BASE_URL="https://github.com/mesa-dot-dev/git-fs/releases/latest/download"
DEFAULT_INSTALL_DIR="/usr/local/bin"
AUTO_YES=false
TMPDIR=""

# --- Colors (only when stdout is a terminal) ---
if [ -t 1 ]; then
    RED='\033[0;31m'
    GREEN='\033[0;32m'
    CYAN='\033[0;36m'
    YELLOW='\033[0;33m'
    BOLD='\033[1m'
    RESET='\033[0m'
else
    RED='' GREEN='' CYAN='' YELLOW='' BOLD='' RESET=''
fi

# --- Output helpers ---
info()    { printf "${CYAN}-->${RESET} %s\n" "$1"; }
success() { printf "${GREEN}-->${RESET} %s\n" "$1"; }
warn()    { printf "${YELLOW}-->${RESET} %s\n" "$1" >&2; }
error()   { printf "${RED}-->${RESET} %s\n" "$1" >&2; exit 1; }

# --- Cleanup ---
cleanup() {
    if [ -n "$TMPDIR" ] && [ -d "$TMPDIR" ]; then
        rm -rf "$TMPDIR"
    fi
}
trap cleanup EXIT

# --- Sudo wrapper ---
sudo_cmd() {
    if [ "$(id -u)" -eq 0 ]; then
        "$@"
    else
        sudo "$@"
    fi
}

# --- Argument parsing ---
parse_args() {
    while [ $# -gt 0 ]; do
        case "$1" in
            -y|--yes) AUTO_YES=true ;;
            -h|--help)
                printf "Usage: install.sh [OPTIONS]\n\n"
                printf "Install or update git-fs.\n\n"
                printf "Options:\n"
                printf "  -y, --yes    Non-interactive mode (use defaults, requires root)\n"
                printf "  -h, --help   Show this help message\n"
                exit 0
                ;;
            *) error "Unknown option: $1 (use -h for help)" ;;
        esac
        shift
    done
}

# --- Dependency checks ---
check_deps() {
    if ! command -v curl >/dev/null 2>&1; then
        error "curl is required but not found. Install it with your package manager."
    fi
}

# --- OS detection ---
detect_os() {
    case "$(uname -s)" in
        Darwin)
            printf "\n"
            info "${BOLD}git-fs installer${RESET}"
            printf "\n"
            warn "macOS is not yet supported by this install script."
            info "Download manually from:"
            info "  ${BASE_URL}/git-fs-macos-universal.tar.gz"
            printf "\n"
            exit 0
            ;;
        Linux) OS=linux ;;
        *) error "Unsupported operating system: $(uname -s)" ;;
    esac
}

# --- Architecture detection ---
detect_arch() {
    case "$(uname -m)" in
        x86_64)  ARCH_DEB=amd64;  ARCH_RPM=x86_64;  ARCH_TAR=amd64 ;;
        aarch64) ARCH_DEB=arm64;  ARCH_RPM=aarch64;  ARCH_TAR=arm64 ;;
        arm64)   ARCH_DEB=arm64;  ARCH_RPM=aarch64;  ARCH_TAR=arm64 ;;
        *) error "Unsupported architecture: $(uname -m)" ;;
    esac
}

# --- Distro detection ---
detect_distro() {
    PKG_TYPE=""
    DISTRO=""

    if [ ! -f /etc/os-release ]; then
        PKG_TYPE=tarball
        return
    fi

    . /etc/os-release

    case "$ID" in
        debian)
            case "$VERSION_ID" in
                12|13) DISTRO="debian-${VERSION_ID}"; PKG_TYPE=deb ;;
                *) PKG_TYPE=tarball ;;
            esac
            ;;
        ubuntu)
            case "$VERSION_ID" in
                20.04|22.04|24.04) DISTRO="ubuntu-${VERSION_ID}"; PKG_TYPE=deb ;;
                *) PKG_TYPE=tarball ;;
            esac
            ;;
        rocky)
            MAJOR=$(echo "$VERSION_ID" | cut -d. -f1)
            case "$MAJOR" in
                8|9) DISTRO="rocky-${MAJOR}"; PKG_TYPE=rpm ;;
                *) PKG_TYPE=tarball ;;
            esac
            ;;
        almalinux)
            MAJOR=$(echo "$VERSION_ID" | cut -d. -f1)
            case "$MAJOR" in
                8|9) DISTRO="alma-${MAJOR}"; PKG_TYPE=rpm ;;
                *) PKG_TYPE=tarball ;;
            esac
            ;;
        fedora)
            case "$VERSION_ID" in
                40|41|42|43) DISTRO="fedora-${VERSION_ID}"; PKG_TYPE=rpm ;;
                *) PKG_TYPE=tarball ;;
            esac
            ;;
        *)
            PKG_TYPE=tarball
            ;;
    esac
}

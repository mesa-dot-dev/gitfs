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
info()    { printf "%b-->%b %b\n" "$CYAN" "$RESET" "$1"; }
success() { printf "%b-->%b %b\n" "$GREEN" "$RESET" "$1"; }
warn()    { printf "%b-->%b %b\n" "$YELLOW" "$RESET" "$1" >&2; }
error()   { printf "%b-->%b %b\n" "$RED" "$RESET" "$1" >&2; exit 1; }

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

# --- Tarball fallback prompting ---
prompt_tarball_install() {
    INSTALL_DIR="$DEFAULT_INSTALL_DIR"

    if [ "$AUTO_YES" = true ]; then
        info "No native package for this distro. Installing generic Linux binary to ${INSTALL_DIR}."
        return
    fi

    printf "\n"
    if [ -n "${ID:-}" ]; then
        warn "No native package available for ${ID} ${VERSION_ID:-}."
    else
        warn "Could not detect your Linux distribution."
    fi
    printf "    Would you like to install the generic Linux binary instead? [Y/n] "
    read -r answer </dev/tty || true

    case "$answer" in
        [nN]*) info "Installation cancelled."; exit 0 ;;
    esac

    printf "    Install location [${DEFAULT_INSTALL_DIR}]: "
    read -r custom_dir </dev/tty || true

    if [ -n "$custom_dir" ]; then
        INSTALL_DIR="$custom_dir"
    fi

    if [ ! -d "$INSTALL_DIR" ]; then
        printf "    ${INSTALL_DIR} does not exist. Create it? [Y/n] "
        read -r create_answer </dev/tty || true
        case "$create_answer" in
            [nN]*) error "Installation cancelled." ;;
        esac
        sudo_cmd mkdir -p "$INSTALL_DIR"
    fi
}

# --- Build download URL ---
build_url() {
    case "$PKG_TYPE" in
        deb)
            FILENAME="git-fs_${DISTRO}_${ARCH_DEB}.deb"
            ;;
        rpm)
            FILENAME="git-fs_${DISTRO}.${ARCH_RPM}.rpm"
            ;;
        tarball)
            FILENAME="git-fs-linux-${ARCH_TAR}.tar.gz"
            ;;
    esac
    URL="${BASE_URL}/${FILENAME}"
}

# --- Download ---
download() {
    TMPDIR=$(mktemp -d)
    info "Downloading ${FILENAME}..."
    curl -fSL -o "${TMPDIR}/${FILENAME}" "$URL" || error "Download failed. Check your internet connection and try again."
}

# --- Install ---
install_package() {
    case "$PKG_TYPE" in
        deb)
            info "Installing deb package..."
            sudo_cmd apt install -y "${TMPDIR}/${FILENAME}"
            ;;
        rpm)
            info "Installing rpm package..."
            if command -v dnf >/dev/null 2>&1; then
                sudo_cmd dnf install -y "${TMPDIR}/${FILENAME}"
            elif command -v yum >/dev/null 2>&1; then
                sudo_cmd yum install -y "${TMPDIR}/${FILENAME}"
            else
                error "Neither dnf nor yum found. Cannot install rpm package."
            fi
            ;;
        tarball)
            info "Installing binary to ${INSTALL_DIR}..."
            tar -xzf "${TMPDIR}/${FILENAME}" -C "$TMPDIR"
            sudo_cmd install -m 755 "${TMPDIR}/git-fs" "${INSTALL_DIR}/git-fs"
            warn "Note: git-fs requires FUSE3. Install it with your package manager if not already present."
            ;;
    esac
}

# --- Verify ---
verify_install() {
    if command -v git-fs >/dev/null 2>&1; then
        VERSION=$(git-fs --version 2>/dev/null || echo "unknown")
        success "git-fs installed successfully! (${VERSION})"
    else
        success "git-fs installed successfully!"
    fi
}

# --- Main ---
main() {
    parse_args "$@"
    check_deps

    printf "\n"
    info "${BOLD}git-fs installer${RESET}"
    printf "\n"

    detect_os
    detect_arch
    detect_distro

    if [ "$PKG_TYPE" = "tarball" ]; then
        prompt_tarball_install
    fi

    # In non-interactive mode, require root
    if [ "$AUTO_YES" = true ] && [ "$(id -u)" -ne 0 ]; then
        error "Non-interactive mode requires root. Re-run with: sudo $0 -y"
    fi

    build_url
    download
    install_package
    verify_install
    printf "\n"
}

main "$@"

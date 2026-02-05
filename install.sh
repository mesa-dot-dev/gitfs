#!/bin/sh
# shellcheck disable=SC1091  # /etc/os-release is not available at lint time
set -eu

BASE_URL="https://github.com/mesa-dot-dev/git-fs/releases/latest/download"
DEFAULT_INSTALL_DIR="/usr/local/bin"
AUTO_YES=false
TMPDIR=""

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

info()    { printf "%b-->%b %b\n" "${CYAN}" "${RESET}" "$1"; }
success() { printf "%b-->%b %b\n" "${GREEN}" "${RESET}" "$1"; }
warn()    { printf "%b-->%b %b\n" "${YELLOW}" "${RESET}" "$1" >&2; }
error()   { printf "%b-->%b %b\n" "${RED}" "${RESET}" "$1" >&2; exit 1; }

cleanup() {
    if [ -n "${TMPDIR}" ] && [ -d "${TMPDIR}" ]; then
        rm -rf "${TMPDIR}"
    fi
}
trap cleanup EXIT

sudo_cmd() {
    _uid=$(id -u)
    if [ "${_uid}" -eq 0 ]; then
        "$@"
    else
        sudo "$@"
    fi
}

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

check_deps() {
    if ! command -v curl >/dev/null 2>&1; then
        error "curl is required but not found. Install it with your package manager."
    fi
}

detect_os() {
    _os=$(uname -s)
    case "${_os}" in
        Darwin)
            warn "macOS is not yet supported by this install script."
            info "Download manually from:"
            info "  ${BASE_URL}/git-fs-macos-universal.tar.gz"
            printf "\n"
            exit 0
            ;;
        Linux) ;;
        *) error "Unsupported operating system: ${_os}" ;;
    esac
}

detect_arch() {
    _machine=$(uname -m)
    case "${_machine}" in
        x86_64)  echo "amd64 x86_64 amd64" ;;
        aarch64) echo "arm64 aarch64 arm64" ;;
        arm64)   echo "arm64 aarch64 arm64" ;;
        *) error "Unsupported architecture: ${_machine}" ;;
    esac
}

detect_distro() {
    if [ ! -f /etc/os-release ]; then
        echo "tarball"
        return
    fi

    # shellcheck disable=SC2154  # ID and VERSION_ID are defined by os-release
    _distro_id=$(. /etc/os-release && echo "${ID}")
    _version_id=$(. /etc/os-release && echo "${VERSION_ID:-}")

    case "${_distro_id}" in
        debian)
            case "${_version_id}" in
                12|13) echo "deb debian-${_version_id} ${_distro_id} ${_version_id}" ;;
                *) echo "tarball _ ${_distro_id} ${_version_id}" ;;
            esac
            ;;
        ubuntu)
            case "${_version_id}" in
                20.04|22.04|24.04) echo "deb ubuntu-${_version_id} ${_distro_id} ${_version_id}" ;;
                *) echo "tarball _ ${_distro_id} ${_version_id}" ;;
            esac
            ;;
        rocky)
            _major=$(echo "${_version_id}" | cut -d. -f1)
            case "${_major}" in
                8|9) echo "rpm rocky-${_major} ${_distro_id} ${_version_id}" ;;
                *) echo "tarball _ ${_distro_id} ${_version_id}" ;;
            esac
            ;;
        almalinux)
            _major=$(echo "${_version_id}" | cut -d. -f1)
            case "${_major}" in
                8|9) echo "rpm alma-${_major} ${_distro_id} ${_version_id}" ;;
                *) echo "tarball _ ${_distro_id} ${_version_id}" ;;
            esac
            ;;
        fedora)
            case "${_version_id}" in
                40|41|42|43) echo "rpm fedora-${_version_id} ${_distro_id} ${_version_id}" ;;
                *) echo "tarball _ ${_distro_id} ${_version_id}" ;;
            esac
            ;;
        *)
            echo "tarball _ ${_distro_id} ${_version_id}"
            ;;
    esac
}

prompt_tarball_install() {
    _auto_yes="$1" _distro_id="$2" _distro_ver="$3"
    _install_dir="${DEFAULT_INSTALL_DIR}"

    if [ "${_auto_yes}" = true ]; then
        info "No native package for this distro. Installing generic Linux binary to ${_install_dir}." >&2
        echo "${_install_dir}"
        return
    fi

    printf "\n" >&2
    if [ "${_distro_id}" != "_" ] && [ -n "${_distro_id}" ]; then
        warn "No native package available for ${_distro_id} ${_distro_ver}."
    else
        warn "Could not detect your Linux distribution."
    fi
    printf "    Would you like to install the generic Linux binary instead? [Y/n] " >&2
    read -r answer </dev/tty || true
    case "${answer}" in
        [nN]*) info "Installation cancelled." >&2; exit 0 ;;
        *) ;;
    esac

    printf "    Install location [%s]: " "${DEFAULT_INSTALL_DIR}" >&2
    read -r custom_dir </dev/tty || true
    if [ -n "${custom_dir}" ]; then
        _install_dir="${custom_dir}"
    fi

    if [ ! -d "${_install_dir}" ]; then
        printf "    %s does not exist. Create it? [Y/n] " "${_install_dir}" >&2
        read -r create_answer </dev/tty || true
        case "${create_answer}" in
            [nN]*) error "Installation cancelled." ;;
            *) ;;
        esac
        sudo_cmd mkdir -p "${_install_dir}"
    fi

    echo "${_install_dir}"
}

build_filename() {
    _pkg_type="$1" _distro="$2" _arch_deb="$3" _arch_rpm="$4" _arch_tar="$5"
    case "${_pkg_type}" in
        deb)     echo "git-fs_${_distro}_${_arch_deb}.deb" ;;
        rpm)     echo "git-fs_${_distro}.${_arch_rpm}.rpm" ;;
        tarball) echo "git-fs-linux-${_arch_tar}.tar.gz" ;;
        *)       error "Unknown package type: ${_pkg_type}" ;;
    esac
}

download() {
    _filename="$1" _url="$2"
    TMPDIR=$(mktemp -d)
    info "Downloading ${_filename}..."
    curl -fSL -o "${TMPDIR}/${_filename}" "${_url}" \
        || error "Download failed. Check your internet connection and try again."
}

install_package() {
    _pkg_type="$1" _filepath="$2" _install_dir="${3:-}"
    case "${_pkg_type}" in
        deb)
            info "Installing deb package..."
            sudo_cmd apt install -y "${_filepath}"
            ;;
        rpm)
            info "Installing rpm package..."
            if command -v dnf >/dev/null 2>&1; then
                sudo_cmd dnf install -y "${_filepath}"
            elif command -v yum >/dev/null 2>&1; then
                sudo_cmd yum install -y "${_filepath}"
            else
                error "Neither dnf nor yum found. Cannot install rpm package."
            fi
            ;;
        tarball)
            info "Installing binary to ${_install_dir}..."
            tar -xzf "${_filepath}" -C "$(dirname "${_filepath}")"
            sudo_cmd install -m 755 "$(dirname "${_filepath}")/git-fs" "${_install_dir}/git-fs"
            warn "Note: git-fs requires FUSE3. Install it with your package manager if not already present."
            ;;
        *)
            error "Unknown package type: ${_pkg_type}"
            ;;
    esac
}

verify_install() {
    if command -v git-fs >/dev/null 2>&1; then
        _version=$(git-fs --version 2>/dev/null || echo "unknown")
        success "git-fs installed successfully! (${_version})"
    else
        success "git-fs installed successfully!"
    fi
}

main() {
    parse_args "$@"
    check_deps

    printf "\n"
    info "${BOLD}git-fs installer${RESET}"
    printf "\n"

    detect_os

    arch=$(detect_arch)
    arch_deb=$(echo "${arch}" | cut -d' ' -f1)
    arch_rpm=$(echo "${arch}" | cut -d' ' -f2)
    arch_tar=$(echo "${arch}" | cut -d' ' -f3)

    distro_info=$(detect_distro)
    pkg_type=$(echo "${distro_info}" | cut -d' ' -f1)
    distro=$(echo "${distro_info}" | cut -d' ' -f2)
    distro_id=$(echo "${distro_info}" | cut -d' ' -f3)
    distro_ver=$(echo "${distro_info}" | cut -d' ' -f4)

    install_dir="${DEFAULT_INSTALL_DIR}"
    if [ "${pkg_type}" = "tarball" ]; then
        install_dir=$(prompt_tarball_install "${AUTO_YES}" "${distro_id:-_}" "${distro_ver:-}")
    fi

    _uid=$(id -u)
    if [ "${AUTO_YES}" = true ] && [ "${_uid}" -ne 0 ]; then
        error "Non-interactive mode requires root. Re-run with: sudo $0 -y"
    fi

    filename=$(build_filename "${pkg_type}" "${distro}" "${arch_deb}" "${arch_rpm}" "${arch_tar}")
    url="${BASE_URL}/${filename}"

    download "${filename}" "${url}"
    install_package "${pkg_type}" "${TMPDIR}/${filename}" "${install_dir}"
    verify_install
    printf "\n"
}

main "$@"

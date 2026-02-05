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

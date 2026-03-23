#!/usr/bin/env bash
# ─── Common Functions ───────────────────────────────────────────────────────
# Shared utilities for all FluxMQ build/test/run scripts.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_DIR="$REPO_ROOT/build"
SDK_DIR="$REPO_ROOT/sdk"

# ── Colors ──────────────────────────────────────────────────────────────────

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
BOLD='\033[1m'
NC='\033[0m' # No Color

info()    { echo -e "${BLUE}[info]${NC}  $*"; }
success() { echo -e "${GREEN}[ok]${NC}    $*"; }
warn()    { echo -e "${YELLOW}[warn]${NC}  $*"; }
error()   { echo -e "${RED}[error]${NC} $*" >&2; }

# ── CPU detection ───────────────────────────────────────────────────────────

detect_cpus() {
    if command -v nproc &>/dev/null; then
        nproc
    elif command -v sysctl &>/dev/null; then
        sysctl -n hw.ncpu
    else
        echo 2
    fi
}

JOBS="$(detect_cpus)"

# ── CMake wrapper ───────────────────────────────────────────────────────────

cmake_configure_and_build() {
    local build_type="${1:-Release}"
    local target="${2:-}"

    info "Configuring CMake (${build_type})..."
    cmake -S "$REPO_ROOT" -B "$BUILD_DIR" \
        -DCMAKE_BUILD_TYPE="$build_type" \
        -DCMAKE_EXPORT_COMPILE_COMMANDS=ON

    if [ -n "$target" ]; then
        info "Building target: ${target} (${JOBS} jobs)..."
        cmake --build "$BUILD_DIR" --target "$target" -j "$JOBS"
    else
        info "Building all targets (${JOBS} jobs)..."
        cmake --build "$BUILD_DIR" -j "$JOBS"
    fi
}

# ── Timer ───────────────────────────────────────────────────────────────────

start_timer() { date +%s; }

format_duration() {
    local secs=$1
    if [ "$secs" -ge 60 ]; then
        printf "%dm%ds" $((secs / 60)) $((secs % 60))
    else
        printf "%ds" "$secs"
    fi
}

get_elapsed() {
    local start=$1
    local now
    now=$(date +%s)
    echo $((now - start))
}

# ── File helpers ────────────────────────────────────────────────────────────

safe_remove() {
    local path="$1"
    if [ -e "$path" ]; then
        rm -rf "$path"
        info "Removed $path"
    fi
}

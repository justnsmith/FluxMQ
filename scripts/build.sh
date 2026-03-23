#!/usr/bin/env bash
# ─── FluxMQ Build Script ───────────────────────────────────────────────────
#
# Usage:
#   ./scripts/build.sh [target] [options]
#
# Targets:
#   broker       Build the FluxMQ broker binary (default)
#   tests        Build all C++ test executables
#   cli          Build the Go CLI tool
#   bench        Build the Go benchmark tool
#   sdk          Build all Go tools (cli + bench)
#   all          Build everything (broker + tests + Go tools)
#
# Options:
#   --clean      Clean build directory before building
#   --debug      Build in Debug mode (default: Release)
#   --verbose    Verbose CMake output
#   --help       Show this help message

source "$(dirname "${BASH_SOURCE[0]}")/common.sh"

# ── Defaults ────────────────────────────────────────────────────────────────

TARGET="broker"
BUILD_TYPE="Release"
CLEAN=false
VERBOSE=false

# ── Parse arguments ─────────────────────────────────────────────────────────

show_help() {
    sed -n '/^# Usage:/,/^$/p' "${BASH_SOURCE[0]}" | sed 's/^# //'
    exit 0
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        broker|tests|cli|bench|sdk|all) TARGET="$1" ;;
        --clean)   CLEAN=true ;;
        --debug)   BUILD_TYPE="Debug" ;;
        --verbose) VERBOSE=true ;;
        --help|-h) show_help ;;
        *) error "Unknown argument: $1"; exit 1 ;;
    esac
    shift
done

if $VERBOSE; then
    export VERBOSE=1
fi

# ── Clean ───────────────────────────────────────────────────────────────────

if $CLEAN; then
    info "Cleaning build directory..."
    safe_remove "$BUILD_DIR"
fi

# ── Build ───────────────────────────────────────────────────────────────────

START=$(start_timer)

build_broker() {
    cmake_configure_and_build "$BUILD_TYPE" fluxmq
    success "Broker binary → $BUILD_DIR/fluxmq"
}

build_tests() {
    cmake_configure_and_build "$BUILD_TYPE"
    success "Test binaries → $BUILD_DIR/tests/"
}

build_cli() {
    info "Building Go CLI..."
    cd "$SDK_DIR" && go build -o "$BUILD_DIR/fluxmq-cli" ./cmd/fluxmq
    success "CLI → $BUILD_DIR/fluxmq-cli"
}

build_bench() {
    info "Building Go benchmark tool..."
    cd "$SDK_DIR" && go build -o "$BUILD_DIR/fluxmq-bench" ./cmd/fluxmq-bench
    success "Bench → $BUILD_DIR/fluxmq-bench"
}

case "$TARGET" in
    broker) build_broker ;;
    tests)  build_tests ;;
    cli)    build_broker && build_cli ;;
    bench)  build_bench ;;
    sdk)    build_broker && build_cli && build_bench ;;
    all)    build_tests && build_cli && build_bench ;;
esac

ELAPSED=$(get_elapsed "$START")
success "Build completed in $(format_duration "$ELAPSED")"

#!/usr/bin/env bash
# ─── FluxMQ Test Script ────────────────────────────────────────────────────
#
# Usage:
#   ./scripts/test.sh [suite] [options]
#
# Suites:
#   all          Run all C++ and Go tests (default)
#   cpp          Run all C++ tests
#   go           Run Go integration tests
#   log          Run storage engine tests only
#   server       Run TCP server tests only
#   broker       Run broker protocol tests only
#   chaos        Run chaos/replication tests only
#
# Options:
#   --build      Build before running tests
#   --verbose    Verbose test output
#   --filter     Filter test by name pattern (Go only)
#   --valgrind   Run C++ tests under valgrind
#   --help       Show this help message

source "$(dirname "${BASH_SOURCE[0]}")/common.sh"

# ── Defaults ────────────────────────────────────────────────────────────────

SUITE="all"
DO_BUILD=false
VERBOSE_FLAG=""
FILTER=""
USE_VALGRIND=false

# ── Parse arguments ─────────────────────────────────────────────────────────

while [[ $# -gt 0 ]]; do
    case "$1" in
        all|cpp|go|log|server|broker|chaos) SUITE="$1" ;;
        --build)     DO_BUILD=true ;;
        --verbose)   VERBOSE_FLAG="-v" ;;
        --filter)    shift; FILTER="$1" ;;
        --valgrind)  USE_VALGRIND=true ;;
        --help|-h)   sed -n '/^# Usage:/,/^$/p' "${BASH_SOURCE[0]}" | sed 's/^# //'; exit 0 ;;
        *) error "Unknown argument: $1"; exit 1 ;;
    esac
    shift
done

# ── Build if requested ──────────────────────────────────────────────────────

if $DO_BUILD; then
    cmake_configure_and_build Release
fi

# ── Helpers ─────────────────────────────────────────────────────────────────

PASS=0
FAIL=0

run_cpp_test() {
    local name="$1"
    local binary="$BUILD_DIR/tests/fluxmq_test_${name}"

    if [ ! -x "$binary" ]; then
        # Fall back to old Makefile-style paths
        binary="$BUILD_DIR/test_${name}"
    fi

    if [ ! -x "$binary" ]; then
        warn "Binary not found: $binary (run with --build)"
        FAIL=$((FAIL + 1))
        return
    fi

    info "Running ${name} tests..."
    local cmd="$binary"
    if $USE_VALGRIND; then
        cmd="valgrind --leak-check=full --error-exitcode=1 $binary"
    fi

    if $cmd; then
        success "${name} tests passed"
        PASS=$((PASS + 1))
    else
        error "${name} tests FAILED"
        FAIL=$((FAIL + 1))
    fi
}

run_go_tests() {
    info "Running Go integration tests..."

    # Ensure broker binary exists
    local broker="$BUILD_DIR/fluxmq"
    if [ ! -x "$broker" ]; then
        # Try CMake output path
        broker="$BUILD_DIR/src/fluxmq"
    fi
    if [ ! -x "$broker" ]; then
        warn "Broker binary not found (run with --build)"
        FAIL=$((FAIL + 1))
        return
    fi

    local go_args=("./tests/" "-timeout" "120s")
    [ -n "$VERBOSE_FLAG" ] && go_args+=("-v")
    [ -n "$FILTER" ] && go_args+=("-run" "$FILTER")

    if (cd "$SDK_DIR" && go test "${go_args[@]}"); then
        success "Go integration tests passed"
        PASS=$((PASS + 1))
    else
        error "Go integration tests FAILED"
        FAIL=$((FAIL + 1))
    fi
}

# ── Run tests ───────────────────────────────────────────────────────────────

START=$(start_timer)

case "$SUITE" in
    all)
        run_cpp_test log
        run_cpp_test server
        run_cpp_test broker
        run_cpp_test chaos
        run_go_tests
        ;;
    cpp)
        run_cpp_test log
        run_cpp_test server
        run_cpp_test broker
        run_cpp_test chaos
        ;;
    go)
        run_go_tests
        ;;
    log|server|broker|chaos)
        run_cpp_test "$SUITE"
        ;;
esac

ELAPSED=$(get_elapsed "$START")

# ── Summary ─────────────────────────────────────────────────────────────────

echo ""
echo -e "${BOLD}Test Summary${NC}"
echo -e "  ${GREEN}Passed:${NC} $PASS"
echo -e "  ${RED}Failed:${NC} $FAIL"
echo -e "  Duration: $(format_duration "$ELAPSED")"

[ "$FAIL" -gt 0 ] && exit 1
exit 0

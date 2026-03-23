#!/usr/bin/env bash
# ─── FluxMQ CI Pipeline ────────────────────────────────────────────────────
#
# Runs the full CI pipeline locally, matching what GitHub Actions does.
#
# Usage:
#   ./scripts/ci.sh [options]
#
# Options:
#   --fast           Build + test only (skip lint/format/docker)
#   --skip-clean     Don't clean before building
#   --skip-tests     Skip test stage
#   --skip-check     Skip lint + format stage
#   --skip-docker    Skip Docker build + smoke test
#   --fix-format     Auto-fix formatting issues
#   --debug          Build in Debug mode
#   --help           Show this help message

source "$(dirname "${BASH_SOURCE[0]}")/common.sh"

# ── Defaults ────────────────────────────────────────────────────────────────

DO_CLEAN=true
DO_TESTS=true
DO_CHECK=true
DO_DOCKER=true
FIX_FORMAT=false
BUILD_TYPE="Release"

# ── Parse arguments ─────────────────────────────────────────────────────────

while [[ $# -gt 0 ]]; do
    case "$1" in
        --fast)        DO_CHECK=false; DO_DOCKER=false ;;
        --skip-clean)  DO_CLEAN=false ;;
        --skip-tests)  DO_TESTS=false ;;
        --skip-check)  DO_CHECK=false ;;
        --skip-docker) DO_DOCKER=false ;;
        --fix-format)  FIX_FORMAT=true ;;
        --debug)       BUILD_TYPE="Debug" ;;
        --help|-h)     sed -n '/^# Usage:/,/^$/p' "${BASH_SOURCE[0]}" | sed 's/^# //'; exit 0 ;;
        *) error "Unknown argument: $1"; exit 1 ;;
    esac
    shift
done

STAGES_PASS=0
STAGES_FAIL=0
STAGES_SKIP=0

run_stage() {
    local name="$1"
    shift
    echo ""
    echo -e "${BOLD}━━━ Stage: ${name} ━━━${NC}"
    if "$@"; then
        success "$name passed"
        STAGES_PASS=$((STAGES_PASS + 1))
    else
        error "$name FAILED"
        STAGES_FAIL=$((STAGES_FAIL + 1))
    fi
}

skip_stage() {
    local name="$1"
    echo ""
    echo -e "${YELLOW}━━━ Stage: ${name} (skipped) ━━━${NC}"
    STAGES_SKIP=$((STAGES_SKIP + 1))
}

# ── Pipeline ────────────────────────────────────────────────────────────────

PIPELINE_START=$(start_timer)

# 1. Clean
if $DO_CLEAN; then
    run_stage "Clean" "$REPO_ROOT/scripts/clean.sh" --build
else
    skip_stage "Clean"
fi

# 2. Build
run_stage "Build" "$REPO_ROOT/scripts/build.sh" all

# 3. Tests (C++ + Go)
if $DO_TESTS; then
    run_stage "C++ Tests" "$REPO_ROOT/scripts/test.sh" cpp
    run_stage "Go Tests" "$REPO_ROOT/scripts/test.sh" go
else
    skip_stage "Tests"
fi

# 4. Static analysis + formatting
if $DO_CHECK; then
    check_args=()
    $FIX_FORMAT && check_args+=(--fix-format)
    run_stage "Lint & Format" "$REPO_ROOT/scripts/check.sh" all "${check_args[@]}"
else
    skip_stage "Lint & Format"
fi

# 5. Docker
if $DO_DOCKER; then
    run_stage "Docker Build" docker build -t fluxmq-broker "$REPO_ROOT"
else
    skip_stage "Docker"
fi

# ── Summary ─────────────────────────────────────────────────────────────────

ELAPSED=$(get_elapsed "$PIPELINE_START")

echo ""
echo -e "${BOLD}═══════════════════════════════════════${NC}"
echo -e "${BOLD}  CI Pipeline Summary${NC}"
echo -e "${BOLD}═══════════════════════════════════════${NC}"
echo -e "  ${GREEN}Passed:${NC}  $STAGES_PASS"
echo -e "  ${RED}Failed:${NC}  $STAGES_FAIL"
echo -e "  ${YELLOW}Skipped:${NC} $STAGES_SKIP"
echo -e "  Duration: $(format_duration "$ELAPSED")"
echo -e "${BOLD}═══════════════════════════════════════${NC}"

[ "$STAGES_FAIL" -gt 0 ] && exit 1
exit 0

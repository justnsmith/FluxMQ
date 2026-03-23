#!/usr/bin/env bash
# ─── FluxMQ Static Analysis & Formatting ───────────────────────────────────
#
# Usage:
#   ./scripts/check.sh [target] [options]
#
# Targets:
#   all          Run all checks (default)
#   cpp          C++ checks only (cppcheck + format)
#   go           Go checks only (vet + fmt)
#   format       Formatting check only (C++ and Go)
#
# Options:
#   --fix-format   Auto-fix formatting issues
#   --verbose      Verbose output
#   --help         Show this help message

source "$(dirname "${BASH_SOURCE[0]}")/common.sh"

# ── Defaults ────────────────────────────────────────────────────────────────

TARGET="all"
FIX_FORMAT=false
VERBOSE=false
PASS=0
FAIL=0
SKIP=0

# ── Parse arguments ─────────────────────────────────────────────────────────

while [[ $# -gt 0 ]]; do
    case "$1" in
        all|cpp|go|format) TARGET="$1" ;;
        --fix-format) FIX_FORMAT=true ;;
        --verbose)    VERBOSE=true ;;
        --help|-h)    sed -n '/^# Usage:/,/^$/p' "${BASH_SOURCE[0]}" | sed 's/^# //'; exit 0 ;;
        *) error "Unknown argument: $1"; exit 1 ;;
    esac
    shift
done

# ── C++ checks ──────────────────────────────────────────────────────────────

run_cppcheck() {
    if ! command -v cppcheck &>/dev/null; then
        warn "cppcheck not found — skipping"
        SKIP=$((SKIP + 1))
        return
    fi

    info "Running cppcheck..."
    if cppcheck \
        --enable=all \
        --std=c++20 \
        --inconclusive \
        --suppress=missingIncludeSystem \
        -I "$REPO_ROOT/include" \
        "$REPO_ROOT/src" "$REPO_ROOT/include" "$REPO_ROOT/tests"; then
        success "cppcheck passed"
        PASS=$((PASS + 1))
    else
        error "cppcheck failed"
        FAIL=$((FAIL + 1))
    fi
}

run_cpp_format() {
    # Prefer pinned version (18); fall back to whatever is on PATH.
    local CF=""
    if command -v clang-format-18 &>/dev/null; then
        CF=clang-format-18
    elif command -v clang-format &>/dev/null; then
        CF=clang-format
    else
        warn "clang-format not found — skipping"
        SKIP=$((SKIP + 1))
        return
    fi

    local files=()
    while IFS= read -r -d '' f; do
        files+=("$f")
    done < <(find "$REPO_ROOT/src" "$REPO_ROOT/include" "$REPO_ROOT/tests" \
        -type f \( -name "*.cpp" -o -name "*.h" -o -name "*.hpp" \) -print0)

    if [ ${#files[@]} -eq 0 ]; then
        warn "No C++ files found"
        return
    fi

    if $FIX_FORMAT; then
        info "Formatting C++ files..."
        "$CF" -i "${files[@]}"
        success "C++ files formatted"
        PASS=$((PASS + 1))
    else
        info "Checking C++ formatting..."
        local needs_format=()
        for f in "${files[@]}"; do
            if ! "$CF" --dry-run --Werror "$f" &>/dev/null; then
                needs_format+=("$f")
            fi
        done
        if [ ${#needs_format[@]} -gt 0 ]; then
            error "Files need formatting:"
            printf '  %s\n' "${needs_format[@]}"
            echo "  Run: ./scripts/check.sh format --fix-format"
            FAIL=$((FAIL + 1))
        else
            success "C++ formatting OK"
            PASS=$((PASS + 1))
        fi
    fi
}

# ── Go checks ──────────────────────────────────────────────────────────────

run_go_vet() {
    info "Running go vet..."
    if (cd "$SDK_DIR" && go vet ./...); then
        success "go vet passed"
        PASS=$((PASS + 1))
    else
        error "go vet failed"
        FAIL=$((FAIL + 1))
    fi
}

run_go_format() {
    info "Checking Go formatting..."
    local unformatted
    unformatted=$(cd "$SDK_DIR" && gofmt -l . 2>/dev/null || true)

    if [ -z "$unformatted" ]; then
        success "Go formatting OK"
        PASS=$((PASS + 1))
    elif $FIX_FORMAT; then
        (cd "$SDK_DIR" && gofmt -w .)
        success "Go files formatted"
        PASS=$((PASS + 1))
    else
        error "Go files need formatting:"
        echo "$unformatted" | sed 's/^/  /'
        FAIL=$((FAIL + 1))
    fi
}

# ── Run checks ──────────────────────────────────────────────────────────────

START=$(start_timer)

case "$TARGET" in
    all)
        run_cppcheck
        run_cpp_format
        run_go_vet
        run_go_format
        ;;
    cpp)
        run_cppcheck
        run_cpp_format
        ;;
    go)
        run_go_vet
        run_go_format
        ;;
    format)
        run_cpp_format
        run_go_format
        ;;
esac

ELAPSED=$(get_elapsed "$START")

# ── Summary ─────────────────────────────────────────────────────────────────

echo ""
echo -e "${BOLD}Check Summary${NC}"
echo -e "  ${GREEN}Passed:${NC}  $PASS"
echo -e "  ${RED}Failed:${NC}  $FAIL"
echo -e "  ${YELLOW}Skipped:${NC} $SKIP"
echo -e "  Duration: $(format_duration "$ELAPSED")"

[ "$FAIL" -gt 0 ] && exit 1
exit 0

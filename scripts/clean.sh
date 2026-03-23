#!/usr/bin/env bash
# ─── FluxMQ Clean Script ───────────────────────────────────────────────────
#
# Usage:
#   ./scripts/clean.sh [options]
#
# Options:
#   --all        Clean everything (build + data + Go cache)
#   --build      Clean build directory only (default)
#   --data       Clean temporary data directories
#   --dry-run    Show what would be removed without removing
#   --help       Show this help message

source "$(dirname "${BASH_SOURCE[0]}")/common.sh"

# ── Defaults ────────────────────────────────────────────────────────────────

CLEAN_BUILD=true
CLEAN_DATA=false
DRY_RUN=false

# ── Parse arguments ─────────────────────────────────────────────────────────

while [[ $# -gt 0 ]]; do
    case "$1" in
        --all)     CLEAN_BUILD=true; CLEAN_DATA=true ;;
        --build)   CLEAN_BUILD=true ;;
        --data)    CLEAN_DATA=true; CLEAN_BUILD=false ;;
        --dry-run) DRY_RUN=true ;;
        --help|-h) sed -n '/^# Usage:/,/^$/p' "${BASH_SOURCE[0]}" | sed 's/^# //'; exit 0 ;;
        *) error "Unknown argument: $1"; exit 1 ;;
    esac
    shift
done

# ── Clean ───────────────────────────────────────────────────────────────────

remove() {
    local path="$1"
    if [ -e "$path" ]; then
        if $DRY_RUN; then
            info "[dry-run] Would remove: $path"
        else
            safe_remove "$path"
        fi
    fi
}

if $CLEAN_BUILD; then
    info "Cleaning build artifacts..."
    remove "$BUILD_DIR"

    # Go build cache for SDK
    if $DRY_RUN; then
        info "[dry-run] Would clean Go build cache"
    else
        (cd "$SDK_DIR" && go clean -cache 2>/dev/null) || true
        success "Go build cache cleaned"
    fi
fi

if $CLEAN_DATA; then
    info "Cleaning temporary data..."
    for d in /tmp/fluxmq_data /tmp/fluxmq-cluster-* /tmp/fluxmq-b*; do
        remove "$d"
    done
fi

success "Clean complete"

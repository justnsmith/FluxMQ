#!/usr/bin/env bash
# Run clang-format on all C++ source and header files.
#
# Requires clang-format-18 (the version pinned in CI).
# macOS: brew install llvm@18  (then: export PATH="/opt/homebrew/opt/llvm@18/bin:$PATH")
# Linux: sudo apt-get install clang-format-18
#
# Usage:
#   ./scripts/format.sh          -- format files in-place
#   ./scripts/format.sh --check  -- check only (non-zero exit if any file needs formatting)

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CHECK_ONLY=false

for arg in "$@"; do
  case "$arg" in
    --check) CHECK_ONLY=true ;;
    *) echo "Unknown argument: $arg" >&2; exit 1 ;;
  esac
done

# Prefer the pinned version (18); fall back to whatever clang-format is on PATH.
if command -v clang-format-18 &>/dev/null; then
  CF=clang-format-18
elif command -v clang-format &>/dev/null; then
  CF=clang-format
else
  echo "Error: clang-format not found." >&2
  echo "  macOS: brew install llvm@18  (then add to PATH)" >&2
  echo "  Linux: sudo apt-get install clang-format-18" >&2
  exit 1
fi

FILES=()
while IFS= read -r -d '' f; do
  FILES+=("$f")
done < <(find "$REPO_ROOT/src" "$REPO_ROOT/include" "$REPO_ROOT/tests" \
  -type f \( -name "*.cpp" -o -name "*.h" -o -name "*.hpp" \) -print0)

if [ ${#FILES[@]} -eq 0 ]; then
  echo "No C++ files found."
  exit 0
fi

if $CHECK_ONLY; then
  echo "Checking formatting..."
  NEEDS_FORMAT=()
  for f in "${FILES[@]}"; do
    if ! "$CF" --dry-run --Werror "$f" &>/dev/null; then
      NEEDS_FORMAT+=("$f")
    fi
  done
  if [ ${#NEEDS_FORMAT[@]} -gt 0 ]; then
    echo "The following files need formatting:"
    printf '  %s\n' "${NEEDS_FORMAT[@]}"
    echo ""
    echo "Run 'make format' to fix (requires clang-format-18)."
    exit 1
  fi
  echo "All files are properly formatted."
else
  echo "Formatting files..."
  "$CF" -i "${FILES[@]}"
  echo "Done."
fi

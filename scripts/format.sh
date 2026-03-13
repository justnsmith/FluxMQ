#!/usr/bin/env bash
# Run clang-format on all C++ source and header files.
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

if ! command -v clang-format &>/dev/null; then
  echo "Error: clang-format not found. Install with: brew install clang-format" >&2
  exit 1
fi

FILES=()
while IFS= read -r -d '' f; do
  FILES+=("$f")
done < <(find "$REPO_ROOT/src" "$REPO_ROOT/include" \
  -type f \( -name "*.cpp" -o -name "*.h" -o -name "*.hpp" \) -print0)

if [ ${#FILES[@]} -eq 0 ]; then
  echo "No C++ files found."
  exit 0
fi

if $CHECK_ONLY; then
  echo "Checking formatting..."
  NEEDS_FORMAT=()
  for f in "${FILES[@]}"; do
    if ! clang-format --dry-run --Werror "$f" &>/dev/null; then
      NEEDS_FORMAT+=("$f")
    fi
  done
  if [ ${#NEEDS_FORMAT[@]} -gt 0 ]; then
    echo "The following files need formatting:"
    printf '  %s\n' "${NEEDS_FORMAT[@]}"
    exit 1
  fi
  echo "All files are properly formatted."
else
  echo "Formatting files..."
  clang-format -i "${FILES[@]}"
  echo "Done."
fi

#!/usr/bin/env bash
# Run cppcheck static analysis on all C++ source and header files.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SRC_DIRS=("$REPO_ROOT/src" "$REPO_ROOT/include" "$REPO_ROOT/tests")

if ! command -v cppcheck &>/dev/null; then
  echo "Error: cppcheck not found. Install with: brew install cppcheck" >&2
  exit 1
fi

echo "Running cppcheck..."
cppcheck \
  --enable=all \
  --std=c++20 \
  --inconclusive \
  --suppress=missingIncludeSystem \
  -I "$REPO_ROOT/include" \
  "${SRC_DIRS[@]}"

echo "cppcheck passed."

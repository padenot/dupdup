#!/bin/bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
ZIG_BIN="${ZIG_BIN:-$ROOT/.tools/zig/zig}"
export ZIG_GLOBAL_CACHE_DIR="${ZIG_GLOBAL_CACHE_DIR:-$ROOT/.tools/zig-cache/global}"
export ZIG_LOCAL_CACHE_DIR="${ZIG_LOCAL_CACHE_DIR:-$ROOT/.tools/zig-cache/local}"

if [[ ! -x "$ZIG_BIN" ]]; then
  LEGACY="$ROOT/zig-macos-aarch64-0.11.0/zig"
  if [[ -x "$LEGACY" ]]; then
    ZIG_BIN="$LEGACY"
  elif command -v zig >/dev/null 2>&1; then
    ZIG_BIN="$(command -v zig)"
  else
    echo "zig not found. Run 'just setup-cross' or set ZIG_BIN." >&2
    exit 1
  fi
fi

ARGS=()
skip_next=false
for arg in "$@"; do
  if $skip_next; then
    skip_next=false
    continue
  fi
  case "$arg" in
    --target=*|-target=*)
      continue
      ;;
    --target|-target)
      skip_next=true
      continue
      ;;
    *)
      ARGS+=("$arg")
      ;;
  esac
done

exec "$ZIG_BIN" c++ -target x86_64-linux-musl "${ARGS[@]}"

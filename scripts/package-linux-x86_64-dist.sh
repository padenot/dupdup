#!/bin/bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

VERSION="$(sed -n 's/^version = "\(.*\)"/\1/p' Cargo.toml | head -n1)"
if [[ -z "$VERSION" ]]; then
  echo "failed to read version from Cargo.toml" >&2
  exit 1
fi

TARGET_TRIPLE="x86_64-unknown-linux-musl"
BINARY_PATH="target/$TARGET_TRIPLE/release/dupdup"
PACKAGE_NAME="dupdup-${VERSION}-linux-x86_64"
DIST_ROOT="dist"
PACKAGE_DIR="$DIST_ROOT/$PACKAGE_NAME"
ARCHIVE_PATH="$DIST_ROOT/$PACKAGE_NAME.tar.xz"

if [[ ! -x "$BINARY_PATH" ]]; then
  echo "missing binary: $BINARY_PATH" >&2
  echo "build it first with 'just cross-linux'" >&2
  exit 1
fi

rm -rf "$PACKAGE_DIR"
mkdir -p "$PACKAGE_DIR"

cp "$BINARY_PATH" "$PACKAGE_DIR/dupdup"
cp -R docs "$PACKAGE_DIR/"
cp -R schemas "$PACKAGE_DIR/"

tar -cJf "$ARCHIVE_PATH" -C "$DIST_ROOT" "$PACKAGE_NAME"

echo "$ARCHIVE_PATH"

default:
    @just --list

# Build native release
build:
    cargo build --release

# Build Linux x86_64 (musl static) from macOS host
cross-linux:
    CC_x86_64_unknown_linux_musl={{justfile_directory()}}/zigcc.sh \
    CARGO_TARGET_X86_64_UNKNOWN_LINUX_MUSL_LINKER=rust-lld \
    ZIG_GLOBAL_CACHE_DIR={{justfile_directory()}}/.tools/zig-cache/global \
    ZIG_LOCAL_CACHE_DIR={{justfile_directory()}}/.tools/zig-cache/local \
    cargo build --release --target x86_64-unknown-linux-musl

# Add Linux musl target to local rustup (repo-local)
setup-cross:
    @set -euo pipefail; \
    rustup target add x86_64-unknown-linux-musl; \
    if command -v zig >/dev/null 2>&1; then \
        echo "zig found in PATH"; \
        exit 0; \
    fi; \
    ZIG_DIR="{{justfile_directory()}}/.tools/zig"; \
    if [ -x "$ZIG_DIR/zig" ]; then \
        echo "zig already installed in $ZIG_DIR"; \
        exit 0; \
    fi; \
    OS="$(uname -s)"; \
    ARCH="$(uname -m)"; \
    if [ "$OS" != "Darwin" ]; then \
        echo "setup-cross only supports macOS hosts"; \
        exit 1; \
    fi; \
    case "$ARCH" in \
        arm64) ZARCH="aarch64" ;; \
        x86_64) ZARCH="x86_64" ;; \
        *) echo "unsupported arch: $ARCH" ; exit 1 ;; \
    esac; \
    ZVER="${ZIG_VERSION:-0.12.0}"; \
    URL="https://ziglang.org/download/${ZVER}/zig-macos-${ZARCH}-${ZVER}.tar.xz"; \
    TMP="$(mktemp -d)"; \
    echo "downloading zig from $URL"; \
    curl -L "$URL" -o "$TMP/zig.tar.xz"; \
    mkdir -p "$ZIG_DIR"; \
    tar -xJf "$TMP/zig.tar.xz" -C "$TMP"; \
    rm -rf "$ZIG_DIR"; \
    mkdir -p "$ZIG_DIR"; \
    cp -R "$TMP/zig-macos-${ZARCH}-${ZVER}/." "$ZIG_DIR/"; \
    rm -rf "$TMP"; \
    echo "zig installed to $ZIG_DIR/zig"

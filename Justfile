default:
    @just --list

# Build native release
build:
    cargo build --release

# Build Linux x86_64 (musl static) from macOS host
cross-linux: setup-cross setup-cross-chromaprint
    CC_x86_64_unknown_linux_musl={{justfile_directory()}}/zigcc.sh \
    CARGO_TARGET_X86_64_UNKNOWN_LINUX_MUSL_LINKER=rust-lld \
    ZIG_GLOBAL_CACHE_DIR={{justfile_directory()}}/.tools/zig-cache/global \
    ZIG_LOCAL_CACHE_DIR={{justfile_directory()}}/.tools/zig-cache/local \
    cargo build --release --target x86_64-unknown-linux-musl

# Add Linux musl target to local rustup (repo-local)
setup-cross:
    @set -euo pipefail; \
    cleanup() { \
        status="$?"; \
        rm -rf "${TMP:-}"; \
        if [ "${status}" -ne 0 ]; then \
            rm -rf "${ZIG_DIR:-}"; \
        fi; \
        exit "${status}"; \
    }; \
    rustup target add x86_64-unknown-linux-musl; \
    if command -v zig >/dev/null 2>&1; then \
        echo "zig found in PATH"; \
        exit 0; \
    fi; \
    ZIG_DIR="{{justfile_directory()}}/.tools/zig"; \
    if [ -x "$ZIG_DIR/zig" ] && "$ZIG_DIR/zig" env >/dev/null 2>&1; then \
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
    trap cleanup EXIT; \
    echo "downloading zig from $URL"; \
    curl -L "$URL" -o "$TMP/zig.tar.xz"; \
    rm -rf "$ZIG_DIR"; \
    mkdir -p "$ZIG_DIR"; \
    tar -xJf "$TMP/zig.tar.xz" --strip-components=1 -C "$ZIG_DIR"; \
    echo "zig installed to $ZIG_DIR/zig"

# Build Chromaprint for Linux musl so the cross build has a target-native static library.
setup-cross-chromaprint: setup-cross
    @set -euo pipefail; \
    CHROMAPRINT_VERSION="${CHROMAPRINT_VERSION:-1.6.0}"; \
    TARGET_TRIPLE="x86_64-unknown-linux-musl"; \
    PREFIX_DIR="{{justfile_directory()}}/.tools/chromaprint/$TARGET_TRIPLE"; \
    LIB_PATH="$PREFIX_DIR/lib/libchromaprint.a"; \
    if [ -f "$LIB_PATH" ]; then \
        echo "chromaprint already built in $PREFIX_DIR"; \
        exit 0; \
    fi; \
    if ! command -v cmake >/dev/null 2>&1; then \
        echo "cmake not found in PATH" >&2; \
        exit 1; \
    fi; \
    TMP="$(mktemp -d)"; \
    cleanup() { \
        status="$?"; \
        rm -rf "${TMP:-}"; \
        if [ "${status}" -ne 0 ]; then \
            rm -rf "${PREFIX_DIR:-}"; \
        fi; \
        exit "${status}"; \
    }; \
    trap cleanup EXIT; \
    URL="https://github.com/acoustid/chromaprint/archive/refs/tags/v${CHROMAPRINT_VERSION}.tar.gz"; \
    SRC_DIR="$TMP/chromaprint-$CHROMAPRINT_VERSION"; \
    BUILD_DIR="$TMP/chromaprint-build"; \
    echo "downloading chromaprint from $URL"; \
    curl -L "$URL" -o "$TMP/chromaprint.tar.gz"; \
    tar -xzf "$TMP/chromaprint.tar.gz" -C "$TMP"; \
    rm -rf "$PREFIX_DIR"; \
    mkdir -p "$PREFIX_DIR"; \
    ZIG_GLOBAL_CACHE_DIR="{{justfile_directory()}}/.tools/zig-cache/global" \
    ZIG_LOCAL_CACHE_DIR="{{justfile_directory()}}/.tools/zig-cache/local" \
    cmake -S "$SRC_DIR" -B "$BUILD_DIR" \
        -DCMAKE_BUILD_TYPE=Release \
        -DCMAKE_SYSTEM_NAME=Linux \
        -DCMAKE_SYSTEM_PROCESSOR=x86_64 \
        -DCMAKE_INSTALL_PREFIX="$PREFIX_DIR" \
        -DBUILD_SHARED_LIBS=OFF \
        -DBUILD_TOOLS=OFF \
        -DBUILD_TESTS=OFF \
        -DFFT_LIB=kissfft \
        -DCMAKE_C_COMPILER={{justfile_directory()}}/zigcc.sh \
        -DCMAKE_CXX_COMPILER={{justfile_directory()}}/zigcxx.sh; \
    cmake --build "$BUILD_DIR" --target install --config Release; \
    echo "chromaprint installed to $PREFIX_DIR"

# Cross-building dupdup (macOS → x86_64 Linux)

We use Zig as the cross linker for a simple, portable setup.

## One-time setup (host: macOS)
1. Run the setup (installs Zig if missing and adds the musl target):
   ```sh
   just setup-cross
   ```
   You can also set `ZIG_BIN=/path/to/zig` to use a custom install.

## Build command
```sh
just cross-linux
```

Output: `target/x86_64-unknown-linux-musl/release/dupdup` (static musl binary).

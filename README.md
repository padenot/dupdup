# dupdup

Rust CLI to find duplicate files by content. It performs a fast partial pass
(default 4 KiB) to narrow candidates, then full hashes, and writes a report for
the web UI.

## Quick start
```sh
cargo build --release
./target/release/dupdup /path/to/scan
```

## Output formats
- JSONL (default): one record per line: `{"hash":"...","paths":["..."],"size":N}`

## UI
The built-in server serves the UI and streams results. By default it auto-starts
and opens a browser unless `--no-open-ui` is set. To run it explicitly:
```sh
./target/release/dupdup /path/to/scan --serve --port 3030
```
The server exposes `/report` and auto-opens the UI unless `--no-open-ui` is set.

## Useful flags
- `--preset auto|ssd|hdd`: presets for SSD vs HDD/NAS (auto by default).
- `--cache PATH` + `--resume`: reuse hashes between runs.
- `--ordered`: deterministic single-thread traversal.
- `--tui`: show the terminal UI during scans.
Note: cache is enabled automatically when using the HDD preset (including auto-detected HDD).

## Build (cross)
See `CROSSBUILD.md` for macOS -> Linux musl builds.

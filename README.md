# dupdup

Rust CLI to find exact duplicate files, audio-equivalent tracks, and conservative
tree diffs.

## Build
```sh
cargo build --release
```

## Scan
```sh
./target/release/dupdup /path/to/scan --mode ui
./target/release/dupdup /path/to/scan --mode headless --no-tui
RUST_LOG=dupdup=trace ./target/release/dupdup /path/to/scan --mode diagnostic
```

Modes:
- `ui`: serve the HTTP UI and open a browser
- `serve`: serve the HTTP UI only
- `headless`: no HTTP UI
- `diagnostic`: no UI/TUI, tracing logs only

## Diff
```sh
./target/release/dupdup diff /path/to/A /path/to/B --output tree-diff.jsonl
```

`dupdup diff` compares by path and type, then size, then partial hash, then
full hash before claiming equal content.

## Output
- duplicate scan: JSONL `group` records
- audio-equivalent scan: JSONL `audio-group` records with a recommended keep
- tree diff: JSONL `meta`, `path-diff`, `relocation`, `summary`

Tree diff references:
- contract: `docs/tree-diff.md`
- examples: `docs/tree-diff-examples.md`
- playbook: `docs/tree-diff-playbook.md`
- `jq`: `docs/tree-diff-jq-cheatsheet.md`
- schema: `schemas/tree-diff-v1.schema.json`

## Useful Flags
- `--preset auto|ssd|hdd`
- `--partial-bytes N`
- `--cache PATH --resume`
- `--mode ui|serve|headless|diagnostic`
- `--tui` / `--no-tui`
- `--ordered`

## Cross Build
See `CROSSBUILD.md`.

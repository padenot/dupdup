# dupdup

`dupdup` finds duplicate files, audio-equivalent tracks, and conservative
directory-tree differences.

The main scan command is built for large folders: it narrows candidates by size,
uses a small prefix hash to avoid unnecessary I/O, and only reports exact
duplicates after a full BLAKE3 hash match. It can also look for audio files that
sound equivalent across different codecs or qualities, then recommend the best
copy to keep.

## Build

```sh
cargo build --release
```

The binary will be at `./target/release/dupdup`.

## Find Duplicates

```sh
./target/release/dupdup /path/to/scan --mode ui
```

By default, scan results are written as JSON Lines to `results.jsonl`. The report
is append-friendly while the scan runs, so the terminal UI and web UI can show
progressive results.

The exact-duplicate pipeline is:

1. Walk the tree and group files by byte size.
2. Read `--partial-bytes` from same-size files to remove obvious non-matches.
3. Full-hash the remaining candidates.
4. Emit duplicate groups only when full hashes match.

This means prefix hashes are only a performance filter. They are not used as
proof that two files are equal.

## Run Modes

```sh
./target/release/dupdup /path/to/scan --mode ui
./target/release/dupdup /path/to/scan --mode serve
./target/release/dupdup /path/to/scan --mode headless --no-tui
RUST_LOG=dupdup=trace ./target/release/dupdup /path/to/scan --mode diagnostic
```

- `ui`: start the HTTP UI, print its URL, and open it in a browser.
- `serve`: start the HTTP UI without opening a browser.
- `headless`: write the report without the HTTP UI.
- `diagnostic`: disable the HTTP UI and terminal UI, and favor tracing logs.

When stdout is a terminal, `dupdup` also starts a TUI unless disabled with
`--no-tui`. The TUI shows scan phases, current worker paths, recent errors, and
the largest duplicate candidates. Once full duplicates are known, it can move
selected copies to the trash with `d`, keep one copy and trash the rest with `k`,
or permanently delete with uppercase `D`/`K`.

The web UI reads the live report from `/report`, filters duplicate groups by
path regex, sorts by reclaimable space/count/size/hash, can reveal files in the
file manager, and can export a removal script. In `ui` and `serve` mode, the
server binds to `0.0.0.0:<port>` and prints the best URL it can find.

## Audio-Equivalent Groups

During a normal duplicate scan, `dupdup` also probes recognized audio files. It
skips obvious stem/sample-pack paths, groups plausible candidates by duration
and channel count, then fingerprints them with Chromaprint plus PCM window
hashes.

Audio matches are emitted as `audio-group` records. Each group includes codec,
duration, sample rate, bit depth, bitrate, lossless status, and a recommended
`keep` path. The recommendation prefers higher-quality copies, for example
lossless files, higher bit depth, higher sample rate, and higher bitrate.

## Diff Directory Trees

```sh
./target/release/dupdup diff /path/to/A /path/to/B --output tree-diff.jsonl
```

`dupdup diff` compares two directory trees and writes a structured JSONL report.
It is intentionally conservative:

- paths and entry types are compared first;
- files at the same path are compared by size, then partial hash, then full hash;
- same-content claims require a full-hash match;
- moved or renamed files are reported as `relocation` records only after a
  full-hash match;
- read failures are reported as `comparison-error`, not guessed around.

Use this when you need to understand whether two trees are the same, whether
same-path files changed, and which unmatched files are probably relocations.

Tree diff references:

- contract: [`docs/tree-diff.md`](docs/tree-diff.md)
- examples: [`docs/tree-diff-examples.md`](docs/tree-diff-examples.md)
- playbook: [`docs/tree-diff-playbook.md`](docs/tree-diff-playbook.md)
- `jq`: [`docs/tree-diff-jq-cheatsheet.md`](docs/tree-diff-jq-cheatsheet.md)
- schema: [`schemas/tree-diff-v1.schema.json`](schemas/tree-diff-v1.schema.json)

## Output Records

Duplicate scans write JSONL records:

- `file`: one fully hashed file and its content hash;
- `group`: an exact duplicate group with one full hash and multiple paths;
- `audio-group`: audio-equivalent files with a recommended copy to keep;
- `summary`: totals, duration, reclaimable bytes, and error-log path.

Tree diffs write JSONL records:

- `meta`: report format, roots, and hash settings;
- `path-diff`: same-path, only-in-one-side, type, metadata, or comparison result;
- `relocation`: same-content files found at different paths;
- `summary`: counts for every diff bucket and the error-log path.

## Useful Flags

- `--output PATH`: write the JSONL report somewhere else.
- `--error PATH`: write read/hash errors somewhere else.
- `--mode ui|serve|headless|diagnostic`: choose runtime UI behavior.
- `--tui` / `--no-tui`: force-enable or disable the terminal UI.
- `--port N`: HTTP UI port, default `3030`.
- `--preset auto|ssd|hdd`: tune threading and block size for the storage device.
- `--ordered`: deterministic path order and single-threaded I/O, useful on HDDs.
- `--partial-bytes N`: bytes read for candidate prefix hashes, default `4096`.
- `--block-size N`: read size for hashing, default `1M`.
- `--threads N`: worker count, with `0` meaning automatic.
- `--cache PATH --resume`: reuse cached hashes from an explicit SQLite cache.
- `--dump-disk-info`: print disk detection details and exit.

Human-readable byte values such as `256K`, `1M`, and `4M` are accepted where a
byte count is expected.

## Cross Build

See [`CROSSBUILD.md`](CROSSBUILD.md).

# dupdup improvement plan (Jan 31, 2026)

## Backend (dupdup)
- Swap MD5 for BLAKE3; add rayon for controlled parallel hashing with device-aware presets (`--fast-ssd`, `--hdd-safe`, `--io-parallel`, `--block-size`).
- Size-bucket prefilter + optional 4 KB prefix pass (default on HDD); single-pass hash on SSD.
- Add cache (SQLite preferred) storing size/mtime/partial/full hash; `--resume` and optional verification sweep.
- Improve progress: indicatif multi-bars (discovery, partial, full), throughput, ETA, duplicate bytes found; `--status-json`.
- Output formats: JSONL stream (optionally zstd), classic JSON; error summary top-N; allow writing to stdout.
- Built-in static server: `dupdup analyze PATH --open-ui [--no-ui] [--port]` serves results + live status endpoint; flag to disable auto-launch.

## Frontend
- Rebuild UI (static bundle) served by CLI or opened from file.
- Use ECharts for charts: reclaimable space by directory/extension, timeline of dup discovery, progress over time.
- Selection UX: checkboxes per file/group, enforce “keep >=1”; bulk by regex, directory prefix, extension, size/age buckets; presets (prefer newest/shortest path/preferred root).
- Tools: fuzzy search, tree view by directory, tags (keep/delete/unsure), pattern select (same basename/extension).
- Exports: rm script, trash script, CSV; live stats panel.

## Performance profiles
- MacBook SSD: `--fast-ssd`, higher threads, 1 MiB blocks, mmap for <512 MiB, single-pass full hash.
- NAS HDD: `--hdd-safe`, 128 KiB blocks, low `--io-parallel` (2–4), directory-ordered walk, prefix prefilter, cache on by default, no mmap.

## Milestones
1) Backend refactor: deps upgrade, blake3+rayon, progress bars, JSONL output.
2) Cache and resume; device presets; compressed output.
3) HTTP server + auto UI launch flag.
4) New frontend with selection tools + ECharts visualizations.
5) Bench/QA on SSD vs NAS; doc best-practice presets.

## Worklog
- [ ] 2026-01-31 Created plan file.
- [x] 2026-01-31 Baseline run: `dupdup` release binary on `/Users/padenot/Music` -> 90,406 files, report `/tmp/dupdup_music.json` (1.0 MB). Time: real 48.04s, user 27.41s, sys 7.06s. Duplicate bytes: first pass 9.2 GB, final 7.0 GB across 3,038 duplicate files.
- [x] 2026-01-31 Backend refactor: switch to Clap v4, blake3 hashing, rayon parallel full hashing, partial-pass toggle, human-size CLI parsing, progress bars, JSON/JSONL output, auto-UI launch flag. Rebuilt release binary.
- [x] 2026-01-31 Cache/resume + presets: added SQLite cache (`--cache`, `--resume`), device presets (`--fast-ssd`, `--hdd-safe`), serve flag/port, default cache path `.dupdup-cache.sqlite`.
- [x] 2026-01-31 HTTP UI + ECharts: new `index.html` with checkbox selection, regex filter, prefer rule, rm export, ECharts charts (group sizes, top directories), fetches `/report`; tiny HTTP server via `--serve` with auto-open retained.
- [x] 2026-02-01 Removed legacy Python scripts and moved the Rust crate to the repo root.

# Tree Diff Contract

Command:

```sh
dupdup diff A B --output tree-diff.jsonl
```

`dupdup diff` emits one JSON object per line. Consumers should treat the report
as a stream with a small envelope around the actual diff records.

Envelope:
- JSONL
- first record: `meta`
- last record: `summary`
- `format`: `dupdup-tree-diff/v1`
- `schema_id`: `https://github.com/padenot/dupdup/blob/main/schemas/tree-diff-v1.schema.json`

Records:
- `meta`: report format, roots, `partial_bytes`, and `block_size`
- `path-diff`: a path-level comparison result
- `relocation`: same file content found under different relative paths
- `summary`: counters, duration, and error-log path

File comparison order:
1. entry type
2. file size
3. partial hash
4. full hash

Guarantees:
- equal file content is never claimed without a full-hash match
- partial hashes are only candidate filters
- relocations are only emitted after a full-hash match
- `comparison-error` means the required comparison did not complete
- directories are compared by path and kind; their descendants carry the useful diff
- symlinks are compared by link target

`path-diff.status`:
- `only-in-a`: entry exists only under root A
- `only-in-b`: entry exists only under root B
- `same-path-same-content`: same relative path and same content
- `same-path-different-content`: same relative path but different content
- `same-path-different-metadata`: same content, but metadata such as mtime differs
- `type-mismatch`: same relative path, different entry kind
- `comparison-error`: reading, hashing, or comparing failed

`comparison_basis`:
- `size`: file sizes were enough to prove different content
- `partial-hash`: prefix hashes were enough to prove different content
- `full-hash`: full hashes were used
- `link-target`: symlink target comparison
- `entry-type`: file/directory/symlink/other mismatch
- `read-error`: comparison could not finish

`relocation`:
- `status` is `same-content-different-path`
- `entry_type` is `file`
- `comparison_basis` is `full-hash`
- paths listed in `a_paths` and `b_paths` are omitted from later `only-in-a` and
  `only-in-b` file records

Interpretation:
- `same-path-same-content`, `same-path-different-metadata`, and `relocation` are
  content-safe buckets.
- `same-path-different-content`, `type-mismatch`, and `comparison-error` need
  manual review.
- `only-in-a` and `only-in-b` are inventory buckets unless matched by a
  relocation record.

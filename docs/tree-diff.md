# Tree Diff Contract

Command:

```sh
dupdup diff A B --output tree-diff.jsonl
```

Format:
- JSONL
- first record: `meta`
- last record: `summary`
- `format`: `dupdup-tree-diff/v1`
- `schema_id`: `https://github.com/padenot/dupdup/blob/main/schemas/tree-diff-v1.schema.json`

Records:
- `meta`
- `path-diff`
- `relocation`
- `summary`

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

`path-diff.status`:
- `only-in-a`
- `only-in-b`
- `same-path-same-content`
- `same-path-different-content`
- `same-path-different-metadata`
- `type-mismatch`
- `comparison-error`

`comparison_basis`:
- `size`
- `partial-hash`
- `full-hash`
- `link-target`
- `entry-type`
- `read-error`

`relocation`:
- `status` is `same-content-different-path`
- `entry_type` is `file`
- `comparison_basis` is `full-hash`

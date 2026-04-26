# Tree Diff Playbook

Run:

```sh
dupdup diff A B --output tree-diff.jsonl --partial-bytes 4096 --block-size 1M
```

Validate before acting:
1. first record is `meta`
2. `format == "dupdup-tree-diff/v1"`
3. last record is `summary`
4. schema-check if needed
5. inspect `summary.stats.comparison_errors`

Safe buckets:
- `same-path-same-content`
- `same-path-different-metadata`
- `relocation`
- inventory from `only-in-a`
- inventory from `only-in-b`

Unsafe buckets:
- `same-path-different-content`
- `type-mismatch`
- `comparison-error`

Default rule:
- do not auto-resolve unsafe buckets
- treat `comparison-error` as unresolved

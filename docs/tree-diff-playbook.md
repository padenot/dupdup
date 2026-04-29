# Tree Diff Playbook

Run:

```sh
dupdup diff A B --output tree-diff.jsonl --partial-bytes 4096 --block-size 1M
```

Use `tree-diff.jsonl` as evidence, not as an automatic delete list. The command
is conservative about equality, but it still reports inventory and unresolved
errors that need human intent.

Validate before acting:
1. first record is `meta`
2. `format == "dupdup-tree-diff/v1"`
3. last record is `summary`
4. schema-check if needed
5. inspect `summary.stats.comparison_errors`

Safe buckets:
- `same-path-same-content`: same relative path and full-hash-confirmed content
- `same-path-different-metadata`: same content, metadata differs
- `relocation`: full-hash-confirmed same content at different paths
- inventory from `only-in-a`
- inventory from `only-in-b`

Unsafe buckets:
- `same-path-different-content`: same path, different bytes or symlink target
- `type-mismatch`: same path, different entry kind
- `comparison-error`: the comparison did not complete

Default rule:
- do not auto-resolve unsafe buckets
- treat `comparison-error` as unresolved

Typical review order:
1. Check `summary.stats.comparison_errors`; fix permissions or unreadable files
   and rerun if it is non-zero.
2. Review `type-mismatch`, because those paths often need a deliberate decision.
3. Review `same-path-different-content`; these are true content conflicts.
4. Use `relocation` records to understand moves or renames.
5. Treat remaining `only-in-a` and `only-in-b` records as unmatched inventory.

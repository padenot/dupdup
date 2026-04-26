# Tree Diff `jq`

Assume `tree-diff.jsonl`.

Envelope:

```sh
jq 'select(.type=="meta")' tree-diff.jsonl
jq 'select(.type=="summary") | .stats' tree-diff.jsonl
```

Path diffs:

```sh
jq -c 'select(.type=="path-diff") | {path,status,comparison_basis}' tree-diff.jsonl
jq -c 'select(.type=="path-diff" and .status=="same-path-different-content")' tree-diff.jsonl
jq -c 'select(.type=="path-diff" and .status=="comparison-error") | {path,note}' tree-diff.jsonl
```

Relocations:

```sh
jq -c 'select(.type=="relocation") | {a_paths,b_paths,size,content_hash}' tree-diff.jsonl
```

Sanity:

```sh
jq -e 'select(.type=="summary") | .stats.comparison_errors == 0' tree-diff.jsonl >/dev/null
```

Schema:

```sh
jq . schemas/tree-diff-v1.schema.json
```

# Tree Diff Examples

Minimal envelope:

```json
{"type":"meta","format":"dupdup-tree-diff/v1","schema_id":"https://github.com/padenot/dupdup/blob/main/schemas/tree-diff-v1.schema.json","root_a":"/data/A","root_b":"/data/B","partial_bytes":4096,"block_size":1048576}
{"type":"summary","stats":{"only_in_a":0,"only_in_b":0,"same_path_same_content":0,"same_path_different_content":0,"same_path_different_metadata":0,"type_mismatch":0,"relocation_groups":0,"relocated_paths_a":0,"relocated_paths_b":0,"comparison_errors":0,"duration_seconds":0.02,"error_log":"diff-error.log"}}
```

`only-in-a`:

```json
{"type":"path-diff","path":"docs/notes.txt","status":"only-in-a","entry_type":"file","type_a":"file","type_b":null,"size_a":1234,"size_b":null,"mtime_a":1714130000,"mtime_b":null,"link_target_a":null,"link_target_b":null,"comparison_basis":null,"partial_hash_a":null,"partial_hash_b":null,"hash_a":null,"hash_b":null,"metadata_differences":[],"note":null}
```

`same-path-different-content` by full hash:

```json
{"type":"path-diff","path":"same-prefix.bin","status":"same-path-different-content","entry_type":"file","type_a":null,"type_b":null,"size_a":17,"size_b":17,"mtime_a":1714130200,"mtime_b":1714130205,"link_target_a":null,"link_target_b":null,"comparison_basis":"full-hash","partial_hash_a":"cccc3333","partial_hash_b":"cccc3333","hash_a":"fullaaaa","hash_b":"fullbbbb","metadata_differences":[],"note":null}
```

`same-path-different-metadata`:

```json
{"type":"path-diff","path":"photos/img001.jpg","status":"same-path-different-metadata","entry_type":"file","type_a":null,"type_b":null,"size_a":400000,"size_b":400000,"mtime_a":1714130300,"mtime_b":1714139999,"link_target_a":null,"link_target_b":null,"comparison_basis":"full-hash","partial_hash_a":"dddd4444","partial_hash_b":"dddd4444","hash_a":"samehash1","hash_b":"samehash1","metadata_differences":["mtime"],"note":null}
```

`type-mismatch`:

```json
{"type":"path-diff","path":"cache","status":"type-mismatch","entry_type":null,"type_a":"directory","type_b":"file","size_a":null,"size_b":4096,"mtime_a":1714130400,"mtime_b":1714130405,"link_target_a":null,"link_target_b":null,"comparison_basis":"entry-type","partial_hash_a":null,"partial_hash_b":null,"hash_a":null,"hash_b":null,"metadata_differences":[],"note":null}
```

`relocation`:

```json
{"type":"relocation","status":"same-content-different-path","entry_type":"file","content_hash":"eeff001122","size":3210,"a_paths":["old/name.txt"],"b_paths":["new/name.txt"],"comparison_basis":"full-hash"}
```

`comparison-error`:

```json
{"type":"path-diff","path":"broken/file.dat","status":"comparison-error","entry_type":"file","type_a":null,"type_b":null,"size_a":4096,"size_b":4096,"mtime_a":1714130500,"mtime_b":1714130510,"link_target_a":null,"link_target_b":null,"comparison_basis":"read-error","partial_hash_a":"ffff9999","partial_hash_b":"ffff9999","hash_a":null,"hash_b":null,"metadata_differences":[],"note":"full hash failed for /data/A/broken/file.dat: Permission denied"}
```

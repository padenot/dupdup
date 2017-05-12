# dupdup

This program hashes all the files under the specified directory, and finds the
duplicates, eventually writing a report in JSON format for further analysis, for
example using `dupdup.html`.

## JSON format

```
{
  hash1 : ["duplicated file 1a",
           "duplicated file 1b",
           ...],
  hash2 : ["duplicated file 2a",
          "duplicated file 2b",
          ...]
}
```

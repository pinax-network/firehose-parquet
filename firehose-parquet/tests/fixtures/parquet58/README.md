# Parquet 58 compatibility fixtures

These synthetic files were generated before changing dependencies, using
Arrow/Parquet **58.0.0** and repository base
`f3e99f327d889cc466d5e1ce29cc708d9f73ddd3`. They contain no live chain data or
credentials. Do not regenerate them with the current dependency version: their
purpose is to preserve a prior-version reader compatibility check.

- `types.parquet` contains the batch returned by `compatibility_batch()` in
  `tests/parquet_compatibility.rs`, written using the 58.0.0 `ArrowWriter` with
  default properties. It covers signed and unsigned integers, float, boolean,
  text, binary, UTC millisecond timestamps, dates, nullable lists and dictionary
  values, including empty values, nulls and integer boundaries. Its footer
  records `parquet-rs version 58.0.0`.
- `cursor.parquet` contains `compatibility_cursor()` from that test, written by
  the production `save_cursor_parquet` function on the same dependency graph.
  It covers resume position, byte block ID, range limits, option flags and
  namespaced file metadata.

Generation used a temporary example in the old checkout that included the test
module (so it used the identical batch/cursor constructors), then performed:

```rust
let batch = compatibility_batch();
let file = std::fs::File::create(output.join("types.parquet")).unwrap();
let mut writer = parquet::arrow::ArrowWriter::try_new(file, batch.schema(), None).unwrap();
writer.write(&batch).unwrap();
writer.close().unwrap();
firehose_parquet::cursor::save_cursor_parquet(
    &output.join("cursor.parquet"), &compatibility_cursor(),
).unwrap();
```

The two compatibility tests passed against 58.0.0 before upgrading. SHA-256:

```text
96c2173e106ad8c9e553651a75cf31c76823f6f279af8493aff5eaa3ae61b989  types.parquet
a424e9cf92ceeee17c00473f754536951dcef5742a2b8a6883aca55ad7b36906  cursor.parquet
```

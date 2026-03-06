# Partitions Build Defaults

This note records the intended defaults and inference rules for `fireparq partitions build`.

## Decision

`fireparq partitions build` should be easy to resume and automate without repeating values that can be inferred safely, while still keeping the build range bounded and deterministic.

## Start Block

`--start-block` is optional.

Resolution order:

1. explicit `--start-block`
2. sibling `cursor.parquet` under the resolved chain output root
3. Firehose endpoint `first_streamable_block_num`

This lets operators resume partition index generation from an existing artifact directory without manually looking up the last written block.

## Output Root

`--output` is optional when `--s3-bucket` (or `S3_BUCKET`) is set.

- `--output ./prefix --s3-bucket my-bucket` resolves to `s3://my-bucket/prefix`
- `--s3-bucket my-bucket` resolves to `s3://my-bucket`
- an explicit `s3://...` `--output` remains authoritative

This keeps S3-oriented automation concise while preserving explicit override behavior.

## Stop Block

`--stop-block` remains required.

Unlike the main ingestion pipeline, `partitions build` produces a canonical bounded artifact (`partitions.parquet`) and optionally `partitions.lookup.json`. Requiring a finite upper bound keeps those artifacts deterministic, reviewable, and safe to rerun in scheduled jobs.

In short:

- ingestion may stream indefinitely
- `partitions build` must describe a closed coverage interval

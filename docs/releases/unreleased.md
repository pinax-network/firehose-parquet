# firehose-parquet: unreleased

Changes merged since [v1.1.0](v1.1.0.md). Fold this file into
`docs/releases/vX.Y.Z.md` when the next release is cut, then reset it to this
template.

Add one entry per change under the matching heading, with its issue and PR
(`#N` refs are fine). Say what changed for operators or data consumers, what
they must do (for example rebuild into a new output root), and link the record
in `docs/audit/` or elsewhere when there is one. Remove headings that stay
empty when the release is cut.

## Breaking changes

## New features

## Fixes

- **v1.1.0 writers could not read their control records** (#698, #699).
  - **Symptom:** every v1.1.0 `build` on a dataset written by a v1.0.2–v1.0.7
    Docker image stopped at startup with
    `Error: control record checksum mismatch`.
  - **Cause:** the checksum of `.fireparq-ingest/state.json` and
    `pending.json` was checked by re-serializing the parsed payload. Its JSON
    key order depends on whether the build has serde_json's `preserve_order`
    feature. The v1.0.2–v1.0.7 images had it (through the maintenance crate's
    DataFusion) and wrote keys in field order. v1.1.0 and every release
    tarball don't, and sort them.
  - **Fix:** the checksum is now checked over the payload exactly as stored.
    Any version reads the records of any other, and the written format is
    unchanged.
  - **Image build:** the image builds `fireparq` with `--locked -p blocks`,
    the same features as the release tarballs.
  - **What to do:** upgrade from v1.0.x to v1.1.1, not v1.1.0. Nothing needs
    rebuilding. See the [record](../audit/698-control-record-key-order.md).

## Performance

## Internal

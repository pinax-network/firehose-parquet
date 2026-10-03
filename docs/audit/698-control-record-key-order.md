# #698: control record checksums and the build's JSON key order

Issue: [#698](https://github.com/pinax-network/firehose-parquet/issues/698).
Released in [v1.1.1](../releases/v1.1.1.md).

## Symptom

At 20:26 UTC on 2026-10-03,
[k8s-parquet#48](https://github.com/pinax-network/k8s-parquet/pull/48) rolled
riv-dev1's writers to v1.1.0. Every writer crash-looped at startup with:

```
Error: control record checksum mismatch
```

This hit eth, base, bsc and the `n*` backfills. At 20:37 UTC,
[k8s-parquet#49](https://github.com/pinax-network/k8s-parquet/pull/49)
reverted them to v1.0.4, and they resumed from their state and committed
again. The writers were down for about 11 minutes. v1.1.0 failed while
reading the records, so it rewrote none of them.

## Cause

`.fireparq-ingest/state.json` and `pending.json` each hold an envelope:

```json
{"format_version":1,"incarnation":"…","revision":2394,"deleted":false,"payload":{…},"sha256":"…"}
```

`sha256` is the digest of the compact JSON tuple
`[format_version, incarnation, revision, deleted, payload]`. A writer
serializes its payload struct into a `serde_json::Value`, then hashes and
stores that `Value`.

Up to v1.1.0, `decode_slot` parsed the stored payload into a `Value`, then
serialized it again and hashed the result. That only gives back the stored
digest if the reader orders object keys the way the writer did. The key order
of a `Value` map is a compile-time property of serde_json. With
`preserve_order`, it keeps insertion order: a struct's field order when
written, and the file's order when parsed. Without it, keys are sorted.

`preserve_order` differed between builds:

| Build | `preserve_order` | Records written | Reads |
|---|---|---|---|
| v1.0.0, v1.0.1 (image and tarball) | off | sorted | sorted only |
| v1.0.2–v1.0.7 release tarballs (`-p blocks -p firehose-parquet`) | off | sorted | sorted only |
| v1.0.2–v1.0.7 images (`cargo build --release --bin fireparq`) | **on** | field order | both |
| v1.1.0 (image and tarball) | off | sorted | sorted only |

Without `-p`, `cargo build --bin fireparq` resolves features for every
workspace member. The `fireparq-maintenance` crate (v1.0.2 to v1.0.7) linked
DataFusion, which enables serde_json's `preserve_order`, so the images had it
on. Nothing in the tests runs that build. v1.1.0 removed DataFusion from the
workspace ([#696](https://github.com/pinax-network/firehose-parquet/pull/696)),
which turned it off. The production records were written by v1.0.4 images, and
their payload keys are in field order: the descriptor has `format_version`
before `chain`, for example. v1.1.0 re-serialized them sorted, and every one failed its checksum.

The rollback to v1.0.4 was safe. A `preserve_order` build re-serializes a
payload in the stored order, so it verifies records of either order.

## Fix

`decode_slot` deserializes a `StoredEnvelope`, whose payload is a borrowed
`serde_json::value::RawValue` (the `raw_value` feature): the payload's exact
stored text. `stored_payload_digest` hashes the same tuple with that text in
place of a re-serialization. A `RawValue` serializes verbatim, so these are the
bytes the writer hashed. The payload is parsed into a `Value` only after the
checksum passes, and the tombstone check and returned payload use it.

- **Any build verifies a record of any build.** Neither the build's
  `preserve_order` nor the writing build's key order matters.
- **The written format and digest are unchanged.** `encode` still hashes the
  `Value` it stores. A v1.1.1 writer (sorted) is read by every earlier build
  that reads sorted records, so a rollback to v1.0.4 or any earlier version
  still works.
- **Integrity is unchanged.** A changed payload, header field or digest still
  fails. The check covers every byte of the payload, including its key order.
  A record whose stored text differs from its writer's compact serialization
  (whitespace, for example) fails as before, since writers only write compact
  JSON.

Every other digest of JSON in the crate goes through `canonical_json`
(`ingest/state.rs`), which sorts object keys itself, so none depends on
`preserve_order`. This covers `Digest::hash` (transaction identities, schema
digests, the cursor mirror) and the accepted-window digest in
`ingest/frontier.rs`.

The Dockerfile now builds with `cargo build --release --locked -p blocks --bin
fireparq`. This resolves exactly the release tarball's feature set
(`cargo tree -e features,normal,build` lists the same packages and features),
and `--locked` builds the image from the lockfile CI tests.

## Tests

| Test | Checks |
|---|---|
| `durable_state::key_order_tests::a_record_verifies_whatever_key_order_its_writer_used` | A hand-written record with its payload in field order, and one sorted, each with the digest of its own text, both decode to the same `Value` |
| `durable_state::key_order_tests::a_changed_payload_fails_its_checksum` | One changed payload value fails with `checksum mismatch` |
| `durable_state::key_order_tests::records_this_build_writes_verify` | `encode` then `decode_slot` round trips, with keys out of order |

The first test fails if `decode_slot` goes back to re-serializing a parsed
payload, as it did in v1.1.0. A build without `preserve_order` then rejects the
field-order record, and that check was run.

## Validation

The six production records, `https://parquet.riv-dev1.pinax.io/<net>/.fireparq-ingest/{state,pending}.json`
for eth, base and bsc, were downloaded on 2026-10-03 after the revert. All six
verified with this `decode_slot`, in a build without `preserve_order`
(`cargo tree --workspace -e features -i serde_json` lists none). These were
the v1.0.4 records v1.1.0 rejected. The three state records have payloads, and
the three pending records are tombstones. That temporary test isn't committed,
since it depends on live data.

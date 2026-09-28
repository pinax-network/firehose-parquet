# #678: Ceph RGW 19.2 `If-Match` ETag form

Issue: [#678](https://github.com/pinax-network/firehose-parquet/issues/678).
Released in [v1.0.1](../releases/v1.0.1.md).

## Symptom

v1.0.0 was deployed against Ceph RGW **19.2.3** (Squid). `build` failed at
startup:

```
Error: acquiring all dataset ownership scopes
Caused by: conditional-write capability could not be proven; ownership was not acquired
```

## Measured provider behavior

SigV4 requests with curl against the real RGW, 2026-09-28:

| Request | RGW 19.2.3 | RFC 9110 (AWS S3, MinIO) |
|---|---|---|
| PUT `If-None-Match: *`, new key | 200 | 200 |
| PUT `If-None-Match: *`, existing key | 412 | 412 |
| PUT `If-Match: "wrong"` | 412 | 412 |
| PUT `If-Match: "<correct etag>"`, quoted | **412** | 200 |
| PUT `If-Match: <correct etag>`, unquoted | **200** | 200 |
| PUT `If-Match: *` | 200 | 200 |
| GET `If-Match: "<correct etag>"`, quoted | **412** | 200 |

RGW returns ETags quoted, as S3 does. It compares an `If-Match` value
literally with its stored ETag without the quotes, so every correct
compare-and-swap in the quoted form (the form `object_store` sends, since it
passes the returned ETag through) is refused. Create-if-absent is correct.

## Impact

- The ownership canary's step "correct CAS succeeds" failed, so no S3 owner
  could be acquired, and neither `build` nor `recovery` could start.
- Past the canary, every compare-and-swap of fireparq's own state would have
  been refused too.
- Delta log commits aren't affected: delta-rs commits with
  `If-None-Match: *` only. They are unchanged.

## Fix

`ETagForm` in `firehose-parquet/src/dataset_lock_s3.rs` is `AsReturned` or
`Unquoted`. `ETagForm::if_match` is the one helper that renders a returned
ETag for an `If-Match` header. `Unquoted` removes exactly one pair of
surrounding quotes and passes an already-unquoted ETag through. It refuses
an empty result, a weak `W/"…"` ETag or stray quotes (`None`), and every
caller treats `None` as a missing version: it fails closed before sending.
`precondition` and `update` build the `UpdateVersion` / `PutMode::Update` in
the form, with the version ID unchanged. Readbacks are still compared with
the version as returned, never with its rendering.

The canary chooses the form when an owner is acquired and stores it in
`S3Ownership`. Every conditional request through that owner uses it:

| Call site | Request |
|---|---|
| `S3Ownership::acquire` | CAS of a Released owner record to Owned |
| `S3Ownership::release` | CAS of the owner record to Released |
| `S3Ownership::operator_release` | CAS to Released, after its own canary |
| `qualify_conditions` / `canary_steps` | wrong-version CAS, pinned GET, correct CAS, stale CAS on the probe |
| `S3StateStore::create` (over a tombstone), `replace`, `remove` (`durable_state_s3.rs`) | CAS of the `.fireparq-ingest/` authority and pending records |
| `ProtectedMirror::reconcile`, S3 binding (`ingest/mirror.rs`) | CAS of the S3 cursor mirror |
| `S3PartStore::read_spooled` (`writer/protected.rs`) | GET pinned to an uploaded part's acknowledged version (`GetOptions.if_match`) |

No other code builds `UpdateVersion.e_tag`, `GetOptions.if_match` or
`PutMode::Update` for a request. The native part upload sends
`If-None-Match: *` only.

### Qualification flow

1. Run the canary on a fresh `.fireparq-owner-probes-v1/<uuid>.json` with
   `AsReturned`:
   1. Create-if-absent succeeds, and the exact bytes and version read back.
   2. A duplicate Create is refused, and the probe is unchanged.
   3. A wrong-version CAS in the form is refused, and the probe is unchanged.
   4. A GET pinned to the correct version in the form serves exactly it.
   5. The correct CAS in the form succeeds, changes the version, and reads
      back exactly.
   6. The previous, now stale version in the form is refused, and the probe
      is unchanged.
   7. The probe is deleted, and its absence is confirmed.
2. If it passes, adopt `AsReturned` (logged at debug).
3. If steps 1-3 passed and step 4 or 5 answered a precondition failure with
   the probe unchanged, and the ETag was quoted, run the whole canary again
   on a new probe key with `Unquoted`. Adopt `Unquoted` only if every step
   passes, and log once at info:
   `s3 conditional writes: If-Match ETags sent unquoted (provider compares them literally)`.
4. Anything else fails closed with `ConditionalWritesUnproven`, as before. That
   includes an Unquoted run that fails, a quoted-form failure at any other
   step, and a store whose ETags are already unquoted (both forms render
   alike).

Step 4 is new. A native `build` verifies each uploaded part with a GET pinned
to its acknowledged ETag, so the canary proves that request too. It only
requires the correct version to be served. The part read still compares the
served version with the acknowledged one, so a provider that ignores `If-Match`
on GET stays safe. A successful `AsReturned` acquisition now makes at most 16
`ObjectStore` calls, one more than before. An `Unquoted` one makes at most 27.

### Safety properties kept

- One form per owner for the whole run, never mixed and never chosen per
  request. The form is a property of the store for the process lifetime and
  isn't persisted, so each start qualifies it again.
- Wrong and stale versions must be refused in the chosen form. The canary
  proves both, in both forms.
- A missing or unusable ETag fails closed.
- Probe keys are random and never reused. Each run deletes its own probe and
  confirms its absence, and a failed cleanup fails closed.

## Tests

| Test | Provider model | Checks |
|---|---|---|
| `dataset_lock_s3::tests::etag_forms_render_if_match_values` | - | Rendering in both forms; unusable ETags; version ID kept; missing ETag unchanged |
| `dataset_lock_s3::tests::one_canary_run_keeps_etags_as_returned_on_an_rfc_store` | in-memory, RFC | `AsReturned`, one probe |
| `dataset_lock_s3::tests::each_canary_form_proves_its_refusals` | in-memory RFC, RGW 19.2, refuse-all | Per-form canary outcomes; probes deleted |
| `dataset_lock_s3::tests::rgw_19_ownership_uses_unquoted_etags_for_every_transition` | in-memory RGW 19.2 | Acquire, release, reacquire and operator release in `Unquoted` |
| `dataset_lock_s3::tests::a_store_refusing_both_forms_fails_closed` | in-memory refuse-all | `ConditionalWritesUnproven`, no owner record, no probe |
| `dataset_lock_s3::tests::wire::rfc_provider_keeps_quoted_etags_in_one_canary_run` | real `AmazonS3` over loopback HTTP, RFC | Exact quoted `If-Match` sequence; one run |
| `dataset_lock_s3::tests::wire::rgw_19_provider_gets_unquoted_etags_on_every_conditional_request` | same, RGW 19.2 | Two runs per acquisition; unquoted wrong and stale refused; unquoted owner and control-state CAS |
| `dataset_lock_s3::tests::wire::providers_matching_no_form_or_broken_versions_fail_closed` | same, refuse-all; RGW 19.2 or RFC accepting stale or any value | Fail closed; a quoted-form failure other than the correct version never reruns |
| `durable_state_s3::tests::rgw_19_control_slots_cas_in_the_owners_etag_form` | in-memory RGW 19.2 | Create, replace, tombstone and re-create; stale refused |
| `ingest::mirror::tests::rgw_19_mirror_updates_through_the_owners_etag_form` | in-memory RGW 19.2 | Mirror Create, then CAS; an older authority is still refused |
| `blocks/tests/delta_tables.rs` `s3_build_writes_delta_tables_with_conditional_log_commits` | loopback HTTPS S3, RFC (default) | Real binary: one canary run per start; every `If-Match` quoted and applied |
| `blocks/tests/delta_tables.rs` `s3_build_on_rgw_19_sends_unquoted_if_match_etags` | loopback HTTPS S3, RGW 19.2 | Real binary, two starts: logs the choice once per start; wrong and stale refused unquoted; owner, control-state and pinned part GET all unquoted and applied; exact Delta logs |
| `blocks/tests/delta_tables.rs` `s3_build_fails_closed_when_no_if_match_form_matches` | loopback HTTPS S3, refuse-all | Real binary fails with the existing error; nothing left in the bucket |
| `blocks/tests/delta_recovery.rs` `committed_without_a_delta_commit_on_rgw_19` | loopback HTTPS S3, RGW 19.2 | Crash at `CommittedPersisted`, `recovery release`, restart: rolled forward exactly once; every conditional request unquoted and applied |

The in-memory model is `firehose-parquet/src/dataset_lock_s3/rgw19_store.rs`.
The loopback HTTPS server (`blocks/examples/bench_live_flush/s3.rs`) takes
`Server::set_if_match(IfMatch::{Rfc, Rgw19, RefuseAll})`. `Rfc` is the
default, and the benchmark keeps it.

## Opt-in real-provider check

`dataset_lock_s3::tests::provider::qualify_real_provider_conditional_writes`
runs only the canary against a real bucket and prints the chosen form. It uses
the same single-attempt client as `build`, and a `PrefixStore` keeps the
probes at `<prefix>/.fireparq-owner-probes-v1/<uuid>.json`. It never touches
an owner record, control state or data. When qualification fails, it runs
each form once more and prints the outcome. It is skipped without
`FIREPARQ_S3_QUALIFY_ENDPOINT`, and always when `CI` is set:

```sh
FIREPARQ_S3_QUALIFY_ENDPOINT=https://rgw.example.com \
FIREPARQ_S3_QUALIFY_BUCKET=disposable-bucket \
FIREPARQ_S3_QUALIFY_PREFIX=fireparq-qualify \
AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=... AWS_REGION=us-east-1 \
cargo test -p firehose-parquet --lib qualify_real_provider -- --nocapture
```

The prefix is required and must not be empty. `AWS_SESSION_TOKEN` is
optional, and `AWS_REGION` defaults to `us-east-1`. On RGW 19.2.x it should
print `If-Match ETag form Unquoted`. On AWS S3 or MinIO it should print
`AsReturned`.

## Validation

This change is validated offline only, with in-memory and loopback providers.
It wasn't run against riv-dev1 or any real bucket. The owner validates it on a
disposable bucket with the opt-in check above.

## Limits

- The detection models the measured RGW 19.2.3 behavior. A later RGW release
  that follows RFC 9110 passes the first run, and `AsReturned` is chosen again
  without a configuration change.
- A provider whose ETags aren't a plain quoted or unquoted token (for example
  weak `W/"…"` ETags) can't use `Unquoted` and fails closed. S3 providers
  don't return weak ETags.

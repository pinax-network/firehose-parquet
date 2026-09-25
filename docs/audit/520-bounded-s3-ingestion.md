# #520: bounded native S3 ingestion uploads

Issue: [#520](https://github.com/pinax-network/firehose-parquet/issues/520).
Work starts from merged #527/main `270af16`. No production S3 request is part of qualification.

## Accepted scope

Authenticated native protected CLI ingestion will spool one encoded Parquet part
to private disk storage and make one conditional streaming PUT. The existing
transaction receipts, owner retention and recovery order remain authoritative.
Anonymous maintenance and the public generic `ParquetTableWriter::new_s3` API keep
their existing credential and one-encoded-part-buffer behavior. This change does
not stream every repository upload and does not introduce multipart or retries.

The native single-PUT cap is 5,000,000,000 encoded bytes. Its connect deadline is
10 seconds, PUT and complete verification deadlines are 15 minutes each, and
presigned URLs expire after 20 minutes (or earlier credential expiry). The native
ObjectStore client also has the long data deadline; existing control operations
retain their shorter outer deadlines. The native footer limit is 32 MiB, including
recovery of older files; exceeding it is a new fail-closed compatibility limit.

## Stage 1: same-client capability and transport

`NativeS3Upload` retains one `Arc<AmazonS3>` for ownership/control operations,
URL signing and readback. `S3Ownership::acquire_native` derives its own store
from that capability, preventing arbitrary store/signer pairing.
`DatasetOwnership::acquire_for_ingestion` selects only the explicit output bucket;
ordinary acquisition and external cursor buckets keep their existing policy.
The CLI has not enabled the capability at this implementation checkpoint.

The upload request uses exact Content-Length, Parquet Content-Type, optional
Cache-Control and `If-None-Match: *`. The SDK's signed URL is used unchanged.
Redirects and HTTP/application retries are disabled. Errors omit the URL, raw
provider response, credential values and reqwest source chains. Success requires
one usable ETag/version pair and a bounded response; readback verification is
required before the controller can resolve the mutation.

Four hermetic native HTTP tests pass: exact body/hash/header/path, one attempt
for lost/late/409/412/500/redirect outcomes, malformed or absent versions and error
redaction, pre-request spool/header validation, and refusal of absent credentials.
These tests use fixture credentials and loopback only. They do not yet qualify
the complete controller, large-file memory or real-provider compatibility.

## Source contracts

[S3 query signing](https://docs.aws.amazon.com/AmazonS3/latest/developerguide/sigv4-query-string-auth.html)
requires host and any x-amz headers in the signature; other headers are optional
signature inputs. Current part attributes use only standard HTTP headers;
transaction metadata stays in the Parquet footer. No unsigned x-amz metadata is
added. The SDK signs with its own resolved endpoint, region and provider.

[PutObject](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html)
defines the conditional-write and response-version contract. Existing owner
canaries and compliant-backend assumptions remain necessary; no client can
prove that a server which consistently lies honors conditional writes.

[AWS upload limits](https://docs.aws.amazon.com/AmazonS3/latest/userguide/upload-objects.html)
document the single-PUT ceiling. The decimal 5,000,000,000-byte bound is
conservative and never falls back to multipart. [Presigned validity](https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-presigned-url.html)
may end earlier when credentials expire; this remains a safe operation failure.

Spool/controller qualification and measured memory/scratch evidence follow in
separate implementation commits before this work is ready for publication.

## Stage 2: private spool and transaction publication

`TransactionParts::encode` selects a file-backed `EncodedPart` only for the native
capability. The controller retains its existing full preflight and Writing ->
receipt -> publish -> verify -> Committed -> authority -> mirror ordering. Generic
and local parts still use the previous encoding path. Native encoding writes one
private, automatically removed spool with an incremental hash and checked size;
4096-row slices share mapper arrays. The encoder flushes row groups at its separate
32 MiB estimated-memory trigger. This is not a hard RSS limit: the current slice,
large values, codec/dictionaries, footer and allocator overhead remain additional.

Readback uses the same native client, explicit long data timeout, exact acknowledged
ETag/version conditions and complete byte/size/hash/schema/row/footer verification.
Its response streams to a second private file. Peak scratch may therefore include
two complete encoded parts. No full encoded object is collected into a Vec in this
native path. Footer parsing first checks the 32 MiB serialized-footer limit.

Large file verification runs on one owned thread. Timeout/cancellation signals it
between 64 KiB hash reads and joins before returning, including a bounded footer
parse already in progress. A deadline starts cancellation; a blocked disk call or
in-progress parser still must drain. There are no detached verification workers.

The native transport now rejects non-HTTPS configured endpoints at construction,
before persistent ownership. This is a deliberate compatibility restriction for
native ingestion; the ordinary maintenance builders keep their existing policy.
The shared transport validates singleton version headers before object_store can
collapse them. GET/HEAD/PUT object responses require a usable identity; bucket
ListObjectsV2 responses correctly do not. A valid ETag with version `null` is
supported, but `null` without a usable ETag is not. Duplicate headers, list/wildcard
ETags, control characters and unusable versions fail closed.

HTTP/2 response headers have a 32 KiB transport limit. Reqwest 0.12 does not expose
a separate HTTP/1 parser-cap override: the pinned hyper implementation has a finite
417,792-byte parser buffer limit, followed by our 32 KiB semantic header check.
Thus the semantic limit is not falsely presented as an HTTP/1 preallocation limit.
Upload response bodies are separately capped at 8 KiB.

Checkpoint validation: `cargo test -p firehose-parquet --locked -j4` passed **715
tests, 9 ignored** (709 library + 3 generator + 3 compatibility). The real native
adapter talks only to a stateful loopback provider. Tests establish persisted exact
receipts before each data PUT, query signing, conditional no-overwrite with a known
object and concurrent winner, pinned complete readback, normal authority/cursor
advance, and read-only bucket listing. Eleven publication/readback faults preserve
Writing, the unchanged authority/cursor, and Owned after exactly one data PUT:
lost acknowledgement, duplicate ETag/version, wildcard/list ETag, null-only or
control-character version, changed version, corrupt/truncated body and oversized
headers. Worker cancellation is independently checked to finish the worker before
its task returns. Native cache-control is validated before any Writing record.

This checkpoint still does not enable the CLI switch or claim large-file RSS,
scratch or slow-transfer qualification. Those are the next qualification stage.


Publication readback sends the ETag/version acknowledged by PUT as its GET
conditions. Recovery has a journal receipt but no persisted provider-version pair:
its one GET requires a usable observed version and exact receipt size/hash/schema,
row count and footer identity. It does not claim a pre-known recovery ETag predicate.
Joining the local verification worker proves only local worker drainage; it never
proves provider request quiescence. Transport and file-I/O cancellation continue to
retain uncertain remote ownership under the existing recovery contract.

## Stage 3: active CLI and bounded local qualification

The extracted `ResolvedEndpoint::acquire_ownership` now selects native acquisition
for `build` output. Dry-run skips ownership as before. A real setup test confirms
an HTTP endpoint is rejected before the loopback listener receives any request,
and the same dry-run remains read-only. Native cancellation after remote acceptance
also retains Owned/Writing with one PUT and an unchanged authority/mirror.

The retained Parquet 58 type fixture passes file-spool schema/value equality.
Offline retained Ethereum qualification re-encoded **26 files, 14 tables and
12,298 rows** from `/tmp/fireparq-469-live-20260925/mainnet`; all field schemas and
all typed values were equal, including multiple dictionaries and nullable/nested
values. No new provider calls were made. This is offline qualification of already
captured data, not new live S3 provider qualification.

The reproducible [measurement driver](520-s3-spool-benchmark.py) and
[complete results](520-s3-spool-results.json) use synthetic random binary rows,
a separately running disk-backed loopback provider, and a preserved core test
executable (`de29f7804a74706239cb5bd6cef86773526c240a7810745e35be8b4041d4163c`).
It was built from stage-2 production code plus the qualification tests and merged
main `ab0888e`. The complete run held `/tmp/fireparq-cargo-session.lock`.
Each client child has its own `/usr/bin/time -l` measurement; these are per-process
macOS peak RSS bytes, not cumulative child resource usage. Provider memory and
fixture preparation are excluded from transfer measurements.

| Encoded file bytes | Transfer + full verification peak RSS | PUT / GET |
| ---: | ---: | ---: |
| 16,909,610 | 34,095,104 (32.52 MiB) | 1 / 1 |
| 135,168,186 | 34,095,104 (32.52 MiB) | 1 / 1 |
| 540,615,607 | 34,111,488 (32.53 MiB) | 1 / 1 |

The provider independently verifies the real SDK's SigV4 HMAC and checks the
conditional header; readback uses the exact acknowledged ETag/version. A separate
16,909,610-byte case delays both PUT acknowledgement and GET headers by **31 seconds
each**. It completed in 63.344 seconds with one PUT/GET and 32.31 MiB peak RSS,
proving the native read client is no longer cut off by the old 30-second default.
These loopback numbers establish bounded client behavior, not internet throughput.

The encoding measurements include full mapper-batch construction and retention:
16 MiB of payload used 97.61 MiB RSS in the buffered encoder versus 87.72 MiB in
the spool encoder; 128 MiB used 533.69 MiB versus 445.94 MiB. There is no claim of
constant total ingestion RSS. At 128 MiB, row-group boundaries changed the encoded
size from 135,188,177 to 135,168,186 bytes. Spooling also adds disk/hash work (the
single diagnostic encoding samples took 4.962 versus 9.501 seconds); these are
not repeated throughput estimates. Input memory, variable-width values, encoder
state, footer metadata and allocator retention remain separate costs.

For the largest transfer, the upload file and verified readback file each contain
540,615,607 bytes, so the native client requires up to **1,081,231,214 bytes of
scratch** while both coexist. Those file lengths are validated by the production
receipt verifier; the reported two-file peak is structural accounting, not a
filesystem-free-space sampler. The independent provider additionally stores one
complete object on its own disk. Private client scratch is gone at child exit.
The test cap is 512 MiB of synthetic payload; the 5 GB limit is exercised through
checked-size and over-limit validation, not by transferring a 5 GB fixture.

## Final integrated validation

At source head `4d214b40014baf166b53bf7fead74beb07d23555`, based on actual main
`81f5b79` (including #525 and #528), the full workspace passed **1,082 tests,
14 ignored**. The separate capture-example auth regression passed (one test,
one subprocess helper ignored). Formatting, the `fireparq` build, top-level/build
help, and Bash/Zsh/Fish completion generation all passed under the shared lock.
The preserved final CLI SHA-256 is
`a41f8e72e37fb2989898b373aa114c97132f04e9bd41e8ab909dfbeb0731e703`.
The two ignored qualification helpers were run explicitly for the benchmark and
retained EVM evidence above; remaining ignored tests retain their existing scopes.
No production S3 requests were performed. This record is implementation/test
evidence; GitHub PR merge and issue state must be verified separately.

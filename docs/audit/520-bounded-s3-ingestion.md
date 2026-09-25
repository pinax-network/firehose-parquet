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

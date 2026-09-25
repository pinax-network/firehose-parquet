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

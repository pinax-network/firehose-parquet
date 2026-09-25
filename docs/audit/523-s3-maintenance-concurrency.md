# #523: bounded S3 maintenance concurrency

Issue: [#523](https://github.com/pinax-network/firehose-parquet/issues/523).
The implementation is split into independently reviewable deletion and read
stages. Deletion landed locally as `e83ebe2`; the following stage adds ordered,
byte-bounded merge reads. Issue closure requires review and combined qualification
of both stages.

## Diagnosis and selected contract

The existing truncate, merge, merge recovery and rollup implementations issued
one DELETE at a time. Their ordering is meaningful: merge commits every output
before removing sources and removes its journal last; rollup publishes a complete
group before old-copy cleanup, then source cleanup. Protected root discovery,
recovery and mutation refusal remain ahead of those operations.

`object_store` is pinned to 0.12.5. Its generic `delete_stream` buffers ten
individual requests, but `AmazonS3` overrides that method with batches of 1,000
keys and up to twenty buffered requests. More importantly, its
`aws/client.rs::bulk_delete_request` starts with a successful result for every
requested key and updates only keys reported as errors. It ignores `Deleted`
entries, so a partial or unrelated success list becomes success for the entire
requested set. A wholly empty `DeleteResult` is rejected by the current XML
parser. An unknown error key also reaches an unchecked lookup in the dependency.

The [AWS DeleteObjects contract](https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjects.html)
distinguishes default verbose per-key results from explicitly requested quiet
mode. A returned Rust success set from this pinned adapter does not establish a
complete verbose acknowledgement set. Hermetic tests send partial and unrelated
`Deleted` entries through the actual signed `AmazonS3` client and demonstrate the
fabricated success results. Production deliberately does not call the bulk API.
No dependency upgrade, custom signer or wire-response interception is introduced.

The shared `s3::delete` helper therefore uses at most **ten individual DELETE
futures** at once. It accepts an exact preselected key set, rejects duplicates,
empty keys and ownership/transaction/merge control paths before any request,
and does not list objects or release ownership. Legacy truncate's explicitly
selected ordinary artifacts retain their existing semantics; they are not
silently removed from its plan.

Each key gets one client invocation with a 60-second deadline. Production callers
use the existing mutation client with transport retries disabled. Already-absent
objects count as complete, as they did in merge recovery. The first observed
request or observer failure stops new dispatch. Already-started client requests
are drained; queued futures that have never started check the stop latch before
issuing a request. No later success hides a failed, timed-out or lost response.

A settled or timed-out client future does **not** prove that an accepted provider
request has ended. Any error or cancellation leaves the persistent remote owner
held. Borrowed-owner native recovery additionally latches mutation uncertainty,
so its caller cannot ordinarily release that guard. Explicit operator recovery
still requires both stopped writers and provider-level request-quiescence proof.
There is no timeout-based takeover or new mutation retry.

## Call-site barriers

- **Truncate:** list/filter/confirmation still complete first. Only the confirmed
  keys are dispatched, and a partial result returns an error instead of a
  successful deletion count.
- **Merge:** every output and the Committed journal precede source deletion.
  The existing failure observer remains inside cleanup. Data futures finish or
  return an uncertain error before journal removal can run.
- **Merge recovery:** Writing cleanup deletes only planned partial outputs and
  run-owned temporary files. Committed cleanup first requires all final outputs,
  then deletes retained sources. Both paths keep journal removal in a distinct
  final phase. Local recovery remains sequential with its existing sync barriers.
- **Rollup:** all output publication precedes old-copy cleanup; all old-copy
  cleanup precedes source cleanup. An error prevents later groups from starting.
  Protected destructive/in-place rollup remains refused. No whole-object prefetch
  is added to its pinned range-reader path.

## Ordered merge reads

Merge preflight and full-object reads now use windows of at most four listed
objects and 64 MiB of summed listed compressed bytes. A single object above that
budget runs alone. Checked arithmetic rejects overflow. The entire window is
validated before any GET, including its request/byte bounds and usable immutable
snapshot identities. Completed buffers remain owned until every object in the
window has been consumed in source order; no next window starts early.

Footer preflight retains the 64 KiB initial suffix read, with a checked larger
suffix only when Parquet metadata requests it. Reserving the listed full-object
size for footer windows is deliberately conservative. All footer windows finish
before journal/output creation. Body windows follow the existing Writing journal
and leave its recovery path intact if a later window fails. Source order, first
schema/metadata selection, protected metadata stripping, row counts and output
publication remain unchanged. There are no detached/background read tasks and no
new CPU/read overlap or whole-object rollup prefetch.

Every GET carries the original listed ETag and/or version. Returned key, total
object size, exact range, ETag/version and bounded streamed body length must
match. Wildcard, null-only or missing version identities fail before requests;
backends unable to provide a usable identity can no longer use an unpinned merge
fallback. A missing/changed source, ignored conditions, inconsistent metadata or
short/oversized returned body is fatal. Safe read request/body failures and timeouts
retain the existing five-attempt application retry policy (100 ms exponential
backoff, 60-second attempt deadline), with identical snapshot conditions on every
attempt. The pinned SDK can reject a malformed/ignored HTTP range before
returning metadata, then expose only an opaque request error shared with transport
failures. That class also uses bounded identical-snapshot read retries; it never
turns into an unpinned request. Returned metadata/body mismatches fail immediately.
Mutation transport/application retries remain disabled.

The 64 MiB value bounds retained compressed **byte lengths** within an ordinary
window, not total process RSS. One oversized source is still a lower bound, and
Parquet decoded batches, dictionaries, footer structures, writer/codec state,
transport chunks and allocator overhead are additional memory. The synchronous
maintenance API still has its existing current-thread-runtime restriction.

## Validation

The tests cover zero/one/ten/1,001 keys, exact membership, no more than ten active
requests, complete preflight, immediate and delayed failures, observer failure,
deadline expiry, accepted DELETE with lost acknowledgement and cancellation.
A semaphore deliberately holds nine requests after the first returned error and
proves the caller has not returned or released ownership before they drain.

Real signed `AmazonS3` requests to a local HTTP fixture verify individual DELETEs,
maximum concurrency, absence of bulk POST requests, and exactly one request for
a key after both HTTP 503 and server acceptance followed by connection loss.
No external bucket is accessed.

Actual command/recovery regressions verify a complete merged output plus retained
Committed journal/owner on cleanup failure; both Writing and Committed recovery
preserve the other phase's files and journal; rollup cleanup failure preserves
sources and later groups; and truncate failure preserves unselected keys.
Existing crash, protected-maintenance and schema/value tests remain in the suite.

Read tests additionally exercise deliberately out-of-order completion, window
count/byte reservations, oversized inputs alone, invalid later identities before
any GET, immutable retries, ignored conditions, changed same-size objects,
metadata/range/body mismatches and timeout exhaustion. Actual merge regressions
prove that a corrupt later footer leaves all original bytes and no journal or
output; a later body-window failure after partial publication retains Writing,
then recovery produces exactly the original 80 rows. A real signed AmazonS3
loopback fixture checks Range, If-Match and versionId on both original requests
and safe retries, and rejects ignored ranges or changed response versions.
The wire fixture observes one successful request, one immediate changed-version
rejection, two pinned failed requests for an SDK-rejected ignored range, and two
identically pinned requests for each recoverable 503, lost-header response and
truncated response body. No mutation is part of these read tests. The read-stage focused merge suite
passed 39 tests; its separately invoked latency benchmark also passed.


The full workspace suite on main `137ab32` plus this deletion change passed
**1,039 tests**, with 10 explicit ignores (including the separately run
latency benchmark). `cargo fmt --all -- --check` and `git diff --check` passed.
The native request tests and actual command/recovery regressions run in that
suite. No production request was made.

## Deletion measurements and limits

The ignored test `s3::delete::tests::benchmark::delayed_store_deletion_benchmark`
compares concurrency one and ten using the same helper and exact key sets in a
delayed `InMemory` store. Every request sleeps for a synthetic 1 ms; actual elapsed
time includes Tokio timer resolution. It alternates order across three samples
for 100, 1,000 and 10,000 keys. Seeding and exact-result verification are outside
the measured interval. Every run checks all objects absent, exactly N invocations,
no cancellation, zero active requests on return and the expected maximum active
count. The whole Cargo invocation holds `/tmp/fireparq-cargo-session.lock`, shared
with repository builds and other benchmarks.

Measured on macOS 26.5.1 / arm64 with Rust 1.93.1, the test profile, base
`137ab32` plus this deletion change. Three-sample medians from the 18 serialized
runs are:

| Exact keys | Sequential | Ten active | Ratio |
|---:|---:|---:|---:|
| 100 | 0.2584 s | 0.0283 s | 9.13× |
| 1,000 | 2.5746 s | 0.2682 s | 9.60× |
| 10,000 | 26.1417 s | 2.6829 s | 9.74× |

All samples completed exactly N DELETE invocations with no active requests left,
no cancellation, and the expected maximum of one or ten. The committed
[raw samples and source fingerprints](523-delete-benchmark.json) identify the
measured helper, fixture and benchmark. The complete locked command took 96.17 s
inside the test harness, excluding compilation.

This isolates scheduling latency. It does not reduce HTTP request count, measure
a real provider, include CLI listing/Parquet work, or claim a production speedup.
The retained input plan, duplicate-validation set and at most ten active request
futures are additional memory; this is a concurrency bound, not an RSS limit.

Reproduce from the repository root, with no external credentials required:

```sh
FIREPARQ_DELETE_BENCH_OUTPUT=/tmp/fireparq-delete-benchmark.json \
  cargo test -p firehose-parquet --lib \
  s3::delete::tests::benchmark::delayed_store_deletion_benchmark \
  --locked -- --ignored --exact
```

Run it without concurrent CPU-heavy work; in this audit the whole command is
wrapped by the shared file lock. The machine-readable evidence accompanies this
record as `523-delete-benchmark.json`.

## Read-window measurement method

The separately ignored `merge::read::tests::benchmark::bounded_read_window_benchmark`
compares one-object windows with the production four-object/64 MiB windows on
three source layouts: 128 small objects, heterogeneous objects that hit the byte
budget before the request bound, and one oversized object that must run alone.
A delayed in-memory store sleeps 3 ms per GET, with the first object deliberately
four times slower. Three samples alternate execution order. Timings sum only the
read-window calls; source creation and complete byte-for-byte verification are
excluded. Every run verifies exact bytes, request count and maximum active GETs.
This isolates bounded scheduling and buffer handling, not provider throughput,
CPU/encoding overlap or complete CLI runtime. The whole invocation uses the same
shared benchmark lock. Measured on the same macOS/Rust host and test profile,
base `35b2134` plus the read stage, the three-sample medians were:

| Source layout | Sequential | Bounded windows | Ratio | Maximum window bytes / active GETs |
|---|---:|---:|---:|---:|
| 128 × 64 KiB | 0.6861 s | 0.1822 s | 3.77× | 256 KiB / 4 |
| 12 heterogeneous objects, 216 MiB total | 0.0806 s | 0.0407 s | 1.98× | 52 MiB / 3 |
| 65 MiB + 4 MiB + 4 MiB | 0.0271 s | 0.0231 s | 1.17× | 65 MiB alone; later two GETs |

The larger windows retain more compressed input than a single-object reader:
256 versus 64 KiB in the small case, and up to 52 versus 24 MiB in the mixed
case. The oversized object's memory lower bound is unchanged. There is no
request-count or egress reduction. The improvement is intentionally modest when
one object dominates the window. All 18 runs returned exactly the listed bytes
in order and issued exactly one GET per object. [Raw samples and source
fingerprints](523-read-benchmark.json) accompany this record. The complete test
harness invocation took 3.38 seconds, excluding compilation.

Reproduce with `FIREPARQ_READ_BENCH_OUTPUT=/tmp/fireparq-read-benchmark.json`
and the ignored test name above, using the same whole-command serialization as
for deletion. This is synthetic latency/copy evidence; production S3 speed and
RSS were not measured.

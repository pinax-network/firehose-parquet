# #517: measured gRPC receive transport

Issue: <https://github.com/pinax-network/firehose-parquet/issues/517>.
Isolated branch `codex/grpc-transport-517` began at main `f555898100fbfb486e97f40162c71b223112a94f`.
Original `.claude` worktrees were not changed. No public Firehose/RPC calls or
production S3 writes are needed for this transport-only change.

## Diagnosis and decision

The checked-in tonic 0.14.5 / hyper 1.8.1 stack left receive windows at fixed
library defaults (2 MiB stream, 5 MiB connection), accepted only gzip, and repeated
a 128 MiB receive ceiling across five current RPC paths. Window limits can stall
a sender while HTTP/2 flow-control updates travel back across a high-latency link.
The issue's historical count of three client construction sites predates finality
and finalized-range traversal.

The default is now **fixed 16 MiB stream and connection windows**, with adaptive
flow control available as an opt-in. This chooses the issue's fixed-window
alternative based on the measurements below; it does not assume adaptive control
always wins. Hyper's adaptive mode starts at the HTTP/2 65,535-byte window and
ramps toward a maximum 16 MiB, which costs additional exchanges on a new connection.
Setting `--grpc-window-bytes 0 --grpc-adaptive-window=false` restores the previous
library-default windows. No receive-side window changes alter cursor handling,
stream ordering, callback completion, keepalive, cancellation, or concurrency.

`Config.grpc` and shared CLI arguments expose:

- `--grpc-window-bytes` / `GRPC_WINDOW_BYTES`: default 16,777,216; zero selects
  library defaults; explicit windows range from 1 through 2,147,483,647.
- `--grpc-adaptive-window[=true|false]` / `GRPC_ADAPTIVE_WINDOW`: default false;
  true overrides initial fixed windows according to tonic/hyper's API.
- `--grpc-max-message-bytes` / `GRPC_MAX_MESSAGE_BYTES`: positive UInt32,
  default 134,217,728, preserving the existing 128 MiB receive ceiling. This
  limits both the wire message body and decompressed protobuf body, not the
  whole process memory footprint or total HTTP/2 buffered bytes.

Both `build` and `partitions build` use the same options. Shared Info, Fetch,
and Stream constructors apply the same ceiling and accept zstd plus gzip;
uncompressed replies remain valid. The server selects its response compression;
requests remain uncompressed. Parquet compression is independent. Tonic's zstd
feature adds `zstd` 0.13.3 and `zstd-safe` 7.3.0 alongside Parquet's 0.14.0/8.0.0;
they use the existing `zstd-sys` 2.1.0 dependency.

Tonic returns OutOfRange for an oversized wire message, but ResourceExhausted
for exceeding the decompression ceiling. The latter now fails without retrying
that same payload: classification matches tonic's complete diagnostic and a
canonical numeric byte limit, not arbitrary server text about resources. Existing
quota-fatal and transient rate-limit handling is preserved. Info/Fetch and both
finalized traversal/proof paths also propagate the configured receive failure.

Primary implementation references (also inspected in the locked local crates):

- [tonic Endpoint](https://docs.rs/tonic/0.14.5/src/tonic/transport/channel/endpoint.rs.html)
  and [connection builder](https://docs.rs/tonic/0.14.5/src/tonic/transport/channel/service/connection.rs.html).
- [hyper HTTP/2 builder](https://docs.rs/hyper/1.8.1/src/hyper/client/conn/http2.rs.html),
  [default windows](https://docs.rs/hyper/1.8.1/src/hyper/proto/h2/client.rs.html),
  and [adaptive estimator](https://docs.rs/hyper/1.8.1/src/hyper/proto/h2/ping.rs.html).
- [tonic compression](https://docs.rs/tonic/0.14.5/tonic/codec/enum.CompressionEncoding.html)
  and [bounded decoding](https://docs.rs/tonic/0.14.5/src/tonic/codec/decode.rs.html).

## Bounded release benchmark

`grpc::transport_tests::benchmark_receive_windows` uses the actual public
`FirehoseClient::stream_blocks` callback with a local tonic server. Each sample
receives 64 consecutive 1 MiB payloads. The timer includes a cold connection,
HTTP/2 startup, transport, protobuf decode and callback identity/length checks.
It excludes mapping, Parquet, storage, TLS, provider limits and reconnects.
Compression is disabled, so the repeated-byte payload does not confer a wire-size
advantage. Each response is built/cloned by the same server path in all modes.

A local TCP relay schedules each read chunk for a fixed 0 or 25 ms later in each
direction. Reading and delayed writing run independently: there is no sleep per
completed chunk and no configured bandwidth cap or packet loss. Its bounded
queue asserts available capacity before every send, so queue throttling invalidates
the benchmark rather than masquerading as network flow control. The 25 ms case
adds approximately 50 ms round-trip propagation. This simulation is not a real WAN.

Apple M1 Max, macOS 26.5.1, Rust 1.93.1, release profile; three cold samples per
mode/latency with mode order rotated between runs. All timing runs use the shared
whole-command Cargo lock, avoiding concurrent builds. Medians:

| Added RTT | Prior fixed defaults | Adaptive | Fixed 16 MiB (selected) |
|---|---:|---:|---:|
| 0 ms | 0.128930 s / 496.4 MiB/s | 0.165606 s / 386.5 MiB/s | 0.035561 s / 1799.7 MiB/s |
| 50 ms | 2.075797 s / 30.8 MiB/s | 0.884563 s / 72.4 MiB/s | 0.355127 s / 180.2 MiB/s |

The selected fixed windows improve this fixture's median throughput by 3.63x and
5.85x respectively. Adaptive improves the delayed case but regresses the
zero-delay cold case. These are local receive/decode measurements, not a claim
about full ingestion speed, compression speed, production latency, or memory
reduction. Larger windows permit more in-flight buffering; operators can reduce
them when memory matters more than network utilization. Exact samples and
configuration are in [517-benchmark.json](517-benchmark.json).

Reproduce (whole-process wrapper also coordinates shared-target protobuf builds):

```sh
CARGO_TARGET_DIR=/Users/denis/.codex/worktrees/fireparq-arrow-security-target \
CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 \
python3 /tmp/fireparq-cargo-locked.py cargo test --release \
  -p firehose-parquet --lib grpc::transport_tests::benchmark_receive_windows \
  --locked -j4 -- --ignored --exact --nocapture
```

## Regression coverage

- Real local plain, gzip, and zstd responses through Info, Fetch, ingestion,
  finalized-anchor proof (both requests), and finalized-range traversal; original
  request headers advertise both codecs, request compression is absent, actual
  server reply encoding and payload identities/bytes are checked.
- Raising/lowering the configured limit permits/rejects the same response;
  oversized wire and compressed replies fail before callback with one RPC per
  path, including decompression-limit classification separate from quota/rate limits.
- CLI/env defaults, explicit bool overrides, zero-window restoration, UInt32
  receive bounds and HTTP/2 signed-31-bit window bounds; both build commands use
  the same parsed configuration. Library zero/invalid values are rejected too.
- Existing authentication, cancellation, finality and probe suites remain required.

The fixture explicitly narrows its own copy of the accepted-encoding header after
checking the real client header: tonic 0.14.5's server selects the first compiled
codec rather than honoring its enabled-only set. This allows exercising a real
gzip response and a real zstd response separately without altering production
client code or claiming this upstream server behavior is fixed.

Initial release protocol/benchmark run: 4 passed (3 regression tests plus the
explicitly enabled benchmark). Final current-main workspace/build/format results
will be recorded after integration.

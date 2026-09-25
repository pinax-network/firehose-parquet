# Issue #530: shared authenticated RPC clients

## Diagnosis and scope

This isolated branch began at PR #602 head
`bf37f47e7345a25d5478f9d15f45b6a44941a8b3`. The original audit described three
inconsistent authentication insertions, four reconnect metric blocks, two stall
checks, and three client builders. Current source had already resolved much of
that finding:

| Original concern | State at the starting commit | Work in this change |
| --- | --- | --- |
| Authentication parsing/insertion | `AuthMetadata` validates and trims credentials once, then has one insertion implementation; six request sites must still call it manually | Attach the same metadata through a tonic interceptor owned by every typed client |
| Reconnect metrics | `record_reconnect_metric()` owns disconnect state and both counters | Preserve the helper and all call sites |
| Stall checks/backoff | `ReconnectState` owns progress reset, failed-attempt cap, stall deadline and delay | Preserve the state machine and call order |
| Client construction | #517 has three private typed constructors for five public RPC paths; compression and receive-size options remain repeated | One small macro defines the three typed constructors with identical auth and transport settings |

The six request sites are Info, Fetch, ingestion Blocks, both finality-proof
Blocks requests, and finalized metadata traversal. Future requests made through
these clients receive the configured credentials automatically. Authentication
no longer depends on remembering a separate call at each request site.

## Behavior contract

Credential selection remains in `auth.rs`, before construction. Headers are still
validated when `FirehoseClient::new` runs, before any connection. Empty credentials
are omitted, surrounding whitespace is trimmed, and invalid values return an
error without logging their contents. The interceptor clones already validated
metadata and preserves unrelated headers and request extensions.

There was no token-refresh flow in this client: API keys/JWTs are an immutable
configuration snapshot. This change introduces no refresh, environment reread,
new provider fallback or retry on authentication failure. Existing Info retries,
stream fatal/quota classification, probe classification, and error ordering
remain in place.

The typed constructors still advertise zstd/gzip responses, send uncompressed
requests, and enforce the configured receive limit. Endpoint/TLS/keepalive/window
settings, cached Fetch/finality channels, reconnect cursor handling, message
progress semantics, cancellation, finality proof and callback ordering are
unchanged. The macro is private and generates only the three short constructors;
there is no new public abstraction or dependency.

## Validation

All protocol fixtures bind loopback only; no live Firehose calls or production
storage operations are needed.

New `grpc/auth_tests.rs` coverage:

- Info, repeated Fetch on the cached channel, ingestion, both finality-proof
  requests, and finalized traversal carry the exact configured headers. Cases:
  neither credential, key only, token only, both, and whitespace-only values.
- A real Info RPC fails once with Unavailable and succeeds on retry. Ingestion
  fails once at response headers, then receives block 100 and fails inside the
  stream, then resumes from `cursor-100` and receives block 101. All five RPCs
  retain both credentials, Blocks bounds/finality flags remain identical, and
  exactly two reconnects/two reconnect errors/zero fatal errors are recorded.
- The interceptor preserves unrelated ASCII/binary metadata, the deadline header
  and extensions, while replacing the configured auth header as before.

Existing required checks:

- `grpc::transport_tests`: all five public paths retain plain/gzip/zstd support
  and wire/decompressed size limits, including fatal oversized-message handling.
- `grpc::tests`: provider scoping, malformed/blank credentials, Info retries,
  fatal/quota/probe classifications, cached channel reuse, backoff/progress/stall
  limits, clean EOF and cancellation during connect/RPC/message/backoff waits.
- `grpc::finality` and `grpc::finalized_range`: exact anchor proof, bounded request
  validation, source identity, deadline and cancellation behavior.
- The full workspace suite covers ingestion authority, durable cursor behavior,
  schema mapping and real CLI startup/shutdown.

Portable commands:

```sh
cargo test -p firehose-parquet --lib grpc:: --locked -j4
cargo test --workspace --locked -j4
cargo test -p blocks --example refresh_evm_golden --locked -j4
cargo build --workspace --locked -j4
cargo fmt --all -- --check
```

Local agents serialize complete Cargo commands because worktrees share a target
cache. Validation at implementation commit `3a4a6dd`:

- Focused gRPC suite: **50 passed, 0 failed, 1 ignored** (the opt-in benchmark).
- Full workspace: **1,010 passed, 0 failed, 9 ignored**.
- CI-selected capture example: **1 passed, 1 ignored** (subprocess entrypoint).
- Workspace build and formatting check passed.

Independent review found no production blocker. Main
`b364681de51bcf6d98031f860bfd12e231dd80b4` was integrated after its #602 merge;
its tree is identical to the #602 head already tested as this branch's base, so
this ancestry merge changed no code. Subsequent changes only record evidence and
add the unreleased note. No performance change or live-provider qualification is
claimed.

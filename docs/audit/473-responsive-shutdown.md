# Responsive ingestion shutdown (#473)

## Preserved source and recovery

The earlier agent left six modified files in
`.claude/worktrees/agent-a0a0c2bd7884f025d`, branch
`audit/473-shutdown-responsive`, based at
`751fa462fe96ccb6375bae1e5518c34927bc2077`. No branch-only implementation commit
existed. The original worktree and its process were preserved.

Its exact tracked worktree diff was copied into a separate patch before
recovery (20,833 bytes), with SHA-256:

```text
d50d2d94dc8983bf74b7dd485f40f29b2a3a80f80184f46b3143ad47bfc9762c
```

The copied files were `Cargo.lock`, `README.md`, `blocks/src/bin/main.rs`,
`docs/releases/unreleased.md`, `firehose-parquet/Cargo.toml`, and
`firehose-parquet/src/grpc.rs`. The patch was applied in a new worktree on
current main. The startup conflict was resolved by retaining #467's required,
fallible endpoint Info response inside the cancellation boundary; unavailable
Info never restores a fallback output identity.

The recovered implementation supplied the cancellation token, typed shutdown
error, interruptible stream awaits and a second-signal force exit. This
follow-up adds current-main cursor-retry integration, explicit dependency
features, deterministic established-stream tests and real process signal tests.

## Diagnosis and behavior

The stream checked a shutdown flag only after a block handler ran. On a quiet
chain, cancellation could wait for the idle timeout, connection timeout or
reconnect backoff. A string containing `__shutdown__` was also treated as a
successful shutdown, even if it represented a storage or other real error.

`stream_blocks` now receives a `CancellationToken`. Its connect attempt,
initial Blocks RPC, each message wait (with or without an idle timeout), and
all reconnect sleeps select cancellation first. A cancelled operation returns
`ShutdownRequested`; callers inspect its error type through context wrappers.
Ordinary error strings cannot masquerade as a graceful exit. A synchronous
block handler that has already started is allowed to finish.

The ingestion startup healthcheck and required Info request share the same
cancellation boundary. A shutdown there returns successfully before any cursor
or output is touched. Unix SIGINT and SIGTERM listeners are both installed
before scheduling their task, avoiding a lazy SIGINT registration window.

The first signal cancels endpoint waits and sets the existing cursor retry
shutdown flag. Cursor persistence retains #469's contract: an in-flight write
is awaited, and a signal after a failed attempt interrupts retry backoff with
a durability error, not a graceful-success marker. No cursor API or persistence
format changes are needed. A second signal keeps the recovered force-exit
behavior (code 130), which can interrupt a write; the README states this limit.

## Verification

- Existing recovered tests cover an already-cancelled token, long sleeps,
  reconnection to a closed port, a silent TCP server, and typed-error detection
  through context.
- A local tonic server drives the actual production stream into five observed
  states: pending Blocks response, idle after a delivered block with and
  without the idle timer, backoff after an RPC error, and backoff after a stream
  error. Tests wait for actual handler/reconnect activity before cancelling,
  require the typed result within two seconds, and assert no additional block
  handling or reconnect beyond the established scenario.
- The binary receives real SIGTERM and SIGINT while a local endpoint holds its
  Info RPC open. Each exits zero within two seconds after the signal, creates
  no output, and leaves an existing sentinel cursor byte-for-byte unchanged.
- An isolated test subprocess uses the production signal handler. The first
  signal sets both cancellation states, then a second signal terminates the
  deliberately unfinished operation with code 130.
- Existing durable-cursor retry and startup failure regressions remain active.
  Stream-exit tests additionally reject a storage error containing the old
  `__shutdown__` text.

Validation on main `2793421` (including Arrow/Parquet 60) passed all 732
workspace tests, with zero failures and three ignored tests; all doc tests
passed. The binary build, formatting check, and Bash, Zsh, and Fish completion
generation passed. This includes the cursor durability, final completion
checkpoint, startup Info failure, and startup cancellation regressions.

Validation used the shared audit target, disabled dev/test debug information,
and a whole-process Cargo lock. Independent review found no implementation
blockers; its wording corrections now explicitly acknowledge interruption of
in-flight writes on a second signal and avoid claiming atomic part-file
publication. The original worktree's diff hash and HEAD were rechecked and
remain unchanged. Protocol and signal tests are local and deterministic; no
production signal or broad live stream is needed to reproduce these waits.

# Network Registry Integration Recommendation

This note records the recommendation for issue #218 while the first `--network` release ships.

## Recommendation

Ship the initial `--network` support with a small static mapping table plus `FIREHOSE_ENDPOINT_*` overrides, and defer live registry integration to a follow-up.

## Why

- The CLI should stay predictable at startup and work offline.
- Firehose ingestion should not gain a hard runtime dependency on a remote registry service.
- Static aliases cover the initial operator workflows for `eth`, `solana`, `tron`, and `tronevm`.
- Per-network env overrides already let operators pin private or provider-specific endpoints without waiting for a registry update.

## Preferred future design

When registry-backed discovery is added, prefer a vendored snapshot or generated source file committed into the repo over runtime fetching.

That future design should:

- ingest registry data in a repeatable update step
- filter to the provider/endpoints we want to expose by default
- emit a generated Rust source/module consumed by the CLI
- keep tests pinned to the generated snapshot version
- preserve explicit env overrides above generated defaults

## What should not happen

- No runtime fetch during normal CLI startup
- No silent endpoint drift caused by external registry changes
- No broad provider selection logic in the first `--network` release

## v0.5.0 decision

For `v0.5.0`, the built-in alias table augments future registry work rather than trying to replace it immediately.

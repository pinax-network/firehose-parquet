# Network Registry Integration Recommendation

This note records the recommendation for issue #218 while the first `--network` release ships.

## Recommendation

Use a one-off generator to turn a checked-in or locally downloaded registry snapshot into `firehose-parquet/src/networks_generated.rs`, keep `FIREHOSE_ENDPOINT_*` overrides, and avoid live registry fetches during builds or CLI startup.

## Why

- The CLI should stay predictable at startup and work offline.
- Firehose ingestion should not gain a hard runtime dependency on a remote registry service.
- A generated Rust module keeps builds reproducible while making it easy to refresh aliases from the registry.
- Per-network env overrides already let operators pin private or provider-specific endpoints without waiting for a registry update.

## Preferred future design

## Current workflow

- Download `TheGraphNetworksRegistry.json` locally.
- Run `cargo run -p firehose-parquet --bin generate-networks -- <path-to-registry-json>`.
- Review the generated changes in `firehose-parquet/src/networks_generated.rs`.
- Keep explicit `FIREHOSE_ENDPOINT_*` overrides above generated defaults.

The generator should:

- ingest registry data in a repeatable update step
- filter to the `pinax.network` Firehose endpoints we want to expose by default
- emit a generated Rust source/module consumed by the CLI
- keep tests pinned to the generated snapshot version

## What should not happen

- No runtime fetch during normal CLI startup
- No silent endpoint drift caused by external registry changes
- No broad provider selection logic in the first `--network` release

## Endpoint metadata

For alias generation, the registry is enough.

Querying each endpoint for `EndpointInfo/Info` can be useful as a separate enrichment or validation step, but it should not be required for generating aliases because it adds network dependencies, slows refreshes, and can fail for temporary endpoint availability reasons.

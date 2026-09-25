# Network Registry Integration

This note covers how the built-in `--network` aliases are generated from The Graph networks registry, which provider each alias uses, and how the registry is kept from going stale. It started as the recommendation for issue #218; #535 added the provider fallback and the endpoint check.

## Recommendation

Use a one-off generator to turn a locally downloaded registry snapshot into `firehose-parquet/src/networks_generated.rs`, keep `FIREHOSE_ENDPOINT_*` overrides, and avoid live registry fetches during builds or CLI startup.

## Why

- The CLI should stay predictable at startup and work offline.
- Firehose ingestion should not gain a hard runtime dependency on a remote registry service.
- A generated Rust module keeps builds reproducible while making it easy to refresh aliases from the registry.
- Per-network env overrides already let operators pin private or provider-specific endpoints without waiting for a registry update.

## Current workflow

1. Download the registry snapshot:

   ```bash
   curl -sSLo TheGraphNetworksRegistry.json \
     https://networks-registry.thegraph.com/TheGraphNetworksRegistry.json
   ```

2. Regenerate the aliases:

   ```bash
   cargo run -p firehose-parquet --bin generate-networks -- TheGraphNetworksRegistry.json
   ```

3. Check that every generated endpoint answers:

   ```bash
   scripts/check_network_endpoints.sh
   ```

4. Review the diff in `firehose-parquet/src/networks_generated.rs`. The file header records the registry version and `updatedAt` it came from. Added aliases are new features; removed or re-pointed aliases are user-facing changes and belong in `docs/releases/unreleased.md`.

Explicit `FIREHOSE_ENDPOINT_*` overrides still take precedence over the generated defaults.

## Provider policy

The generator (`scripts/generate_networks.rs`) picks one endpoint per registry network:

1. The registry's `pinax.network` Firehose endpoint, when it lists one (`DEFAULT_PROVIDER`).
2. Otherwise, for networks in `FALLBACK_PROVIDERS` only, the endpoint of the provider named there.
3. Otherwise, the network gets no alias.

Networks in `EXCLUDED_NETWORKS` are skipped even though the registry lists an endpoint for them, because that endpoint is known to be broken. Each entry records the reason and the date it was checked.

The fallback list is explicit on purpose. New registry networks served only by another provider are not added automatically, and an alias never switches provider without a reviewed change to the generator.

Current fallbacks, all verified by streaming blocks with a `SUBSTREAMS_API_TOKEN` on 2026-09-24 (#535):

| Alias | Endpoint | Reason |
|---|---|---|
| `near-mainnet` | `mainnet.near.streamingfast.io:443` | Pinax no longer serves NEAR |
| `near-testnet` | `testnet.near.streamingfast.io:443` | Pinax no longer serves NEAR |
| `tron` | `mainnet.tron.streamingfast.io:443` | Pinax no longer serves Tron |
| `tron-evm` | `mainnet-evm.tron.streamingfast.io:443` | Pinax no longer serves Tron |

StreamingFast endpoints need a credential that StreamingFast accepts, for example a The Graph Market API token in `SUBSTREAMS_API_TOKEN`.

Current exclusions:

| Network | Reason |
|---|---|
| `robinhood-sepolia` | `robsepolia.firehose.pinax.network` has no DNS record (2026-09-24) |

## Staleness check

`scripts/check_network_endpoints.sh` sends an unauthenticated Firehose `EndpointInfo/Info` gRPC call to every generated endpoint with `curl`. That covers DNS, TLS (certificate and hostname), HTTP/2, and a gRPC answer. `grpc-status: 16` (Unauthenticated) counts as served, so the check needs no credentials. It exits non-zero and lists the failing aliases when any endpoint does not answer.

The `Network endpoints` workflow (`.github/workflows/network-endpoints.yml`) runs the script weekly, on demand, and on pull requests that touch the generated registry or the script. It is separate from `ci.yml`, so `cargo test` never depends on the network.

When the check fails: refresh the registry snapshot and regenerate. If the registry still lists the broken endpoint, verify another provider it lists and add a `FALLBACK_PROVIDERS` entry, or add an `EXCLUDED_NETWORKS` entry. A removed alias is a breaking change for its users.

## What should not happen

- No runtime fetch during normal CLI startup
- No silent endpoint drift caused by external registry changes
- No automatic provider selection beyond the explicit fallback list

## Endpoint metadata

For alias generation, the registry is enough.

Querying each endpoint for `EndpointInfo/Info` is a separate validation step (the staleness check above), not part of generation. Generation stays offline because network calls slow refreshes and can fail for temporary endpoint availability reasons.

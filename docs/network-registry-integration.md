# Network Registry Integration

This note covers how the built-in `--network` aliases are generated from The Graph networks registry and a short internal list of Pinax-served networks the registry does not list yet, which provider each alias uses, and how the aliases are kept from going stale. It started as the recommendation for issue #218; #535 added the provider fallback and the endpoint check.

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

   To regenerate without moving to a newer registry (for example after a change to the generator's lists), download the snapshot named in the generated file's header instead. The registry serves every release at a versioned URL, the version's dots replaced by underscores, and its `version` and `updatedAt` must match the header:

   ```bash
   curl -sSLo TheGraphNetworksRegistry.json \
     https://networks-registry.thegraph.com/TheGraphNetworksRegistry_v0_8_4.json
   ```

2. Regenerate the aliases:

   ```bash
   cargo run -p firehose-parquet --bin generate-networks -- TheGraphNetworksRegistry.json
   ```

   The generator prints a `warning:` line for every `PINAX_NETWORKS` entry that now conflicts with the registry: the registry lists the same name, or gives another name the same endpoint host (see [Internal Pinax networks](#internal-pinax-networks)). It also warns for every fallback network the registry no longer lists with its fallback provider.

3. Check that every generated endpoint answers:

   ```bash
   scripts/check_network_endpoints.sh
   ```

4. Review the diff in `firehose-parquet/src/networks_generated.rs`. The file header records the registry version and `updatedAt` it came from, and that the internal list is included; each internal alias is marked with a `// PINAX_NETWORKS:` comment and its reason. Added aliases are new features; removed or re-pointed aliases are user-facing changes and belong in `docs/releases/unreleased.md`.

Explicit `FIREHOSE_ENDPOINT_*` overrides still take precedence over the generated defaults.

## Provider policy

The generator (`scripts/generate_networks.rs`) picks one endpoint per registry network:

1. The registry's `pinax.network` Firehose endpoint, when it lists one (`DEFAULT_PROVIDER`).
2. Otherwise, for networks in `FALLBACK_PROVIDERS` only, the endpoint of the provider named there.
3. Otherwise, the network gets no alias.

Networks in `EXCLUDED_NETWORKS` are skipped even though the registry lists an endpoint for them, because that endpoint is known to be broken. Each entry records the reason and the date it was checked.

The fallback list is explicit on purpose. New registry networks served only by another provider are not added automatically, and an alias never switches provider without a reviewed change to the generator.

Current fallbacks, all verified by streaming blocks with a StreamingFast-compatible token on 2026-09-24 (#535):

| Alias | Endpoint | Reason |
|---|---|---|
| `near-mainnet` | `mainnet.near.streamingfast.io:443` | Pinax no longer serves NEAR |
| `near-testnet` | `testnet.near.streamingfast.io:443` | Pinax no longer serves NEAR |
| `tron` | `mainnet.tron.streamingfast.io:443` | Pinax no longer serves Tron |
| `tron-evm` | `mainnet-evm.tron.streamingfast.io:443` | Pinax no longer serves Tron |

StreamingFast endpoints need a credential that StreamingFast accepts, for example a The Graph Market API token in `STREAMINGFAST_API_TOKEN`. Since #562, legacy `SUBSTREAMS_*` variables are Pinax-only defaults; custom endpoints require explicit credential selectors. See [Authentication](authentication.md).

Current exclusions:

| Network | Reason |
|---|---|
| `robinhood-sepolia` | `robsepolia.firehose.pinax.network` has no DNS record (2026-09-24) |

## Internal Pinax networks

Pinax serves some Firehose networks that The Graph networks registry does not list yet; the registry only covers networks of The Graph network. `PINAX_NETWORKS` in `scripts/generate_networks.rs` is a hand-maintained list of them, as `(alias, endpoint, reason)`, and the generator appends them to the registry's aliases. They are built-in aliases like any other: `--network` resolves them, `FIREHOSE_ENDPOINT_*` overrides them, their hosts are built-in Pinax hosts that receive the ambient `PINAX_API_KEY` / `PINAX_API_TOKEN` (and the legacy `SUBSTREAMS_*` fallbacks; see [Authentication](authentication.md)), and the staleness check covers them.

Policy:

- **Pinax-served only.** An entry is a `https://<host>.pinax.network:443` endpoint that Pinax serves. A unit test of the generator checks the form, a normalized alias, a nonempty reason and that the alias is not also a fallback or exclusion.
- **Reviewed additions.** An entry is added in a reviewed change, after its endpoint answers `scripts/check_network_endpoints.sh` and a stream with a Pinax key returns blocks. The reason says what the network is and why the registry does not list it.
- **Removed once the registry lists it.** The generator compares each entry with the snapshot and warns on a conflict:
  - **The registry lists the same name with an endpoint the provider policy accepts** (its `pinax.network` endpoint, or the `FALLBACK_PROVIDERS` provider). The registry entry wins, and the warning names the registry's endpoint and says to drop the entry from `PINAX_NETWORKS`. Drop it in the same change. If the registry's endpoint differs from the internal one, the alias is re-pointed: say so in `docs/releases/unreleased.md`.
  - **The registry lists the same name without such an endpoint** (for example only another provider's). The internal entry is kept, so the alias keeps working, and the warning says to review it: its reason no longer holds. Update the reason, and keep the entry until the registry lists the network's `pinax.network` endpoint.
  - **A registry network under another name has the same endpoint host.** Both aliases are kept, and the warning says to drop the internal entry or confirm that both aliases are intended. Dropping it removes the internal alias, a breaking change for its users.

Current internal networks:

| Alias | Endpoint | Reason |
|---|---|---|
| `hypercore` | `hypercore.firehose.pinax.network:443` | HyperLiquid L1 (HyperCore); not in The Graph networks registry |

A network whose known data starts later than its endpoint's first streamable block also has an entry in `NETWORK_DATA_ORIGINS` (`firehose-parquet/src/networks.rs`, hand-written, not generated), which sets the default start of a new stream and refuses an earlier `--start-block` ([start and stop blocks](cursor-and-resume.md#start-and-stop-blocks)). HyperCore's origin is block 846903317 (2026-01-01).

## Staleness check

`scripts/check_network_endpoints.sh` sends an unauthenticated Firehose `EndpointInfo/Info` gRPC call to every generated endpoint with `curl`. That covers DNS, TLS (certificate and hostname), HTTP/2, and a gRPC answer. `grpc-status: 16` (Unauthenticated) counts as served, so the check needs no credentials. It exits non-zero and lists the failing aliases when any endpoint does not answer.

The `Network endpoints` workflow (`.github/workflows/network-endpoints.yml`) runs the script weekly, on demand, and on pull requests that touch the generated registry or the script. It is separate from `ci.yml`, so `cargo test` never depends on the network.

When the check fails: refresh the registry snapshot and regenerate. If the registry still lists the broken endpoint, verify another provider it lists and add a `FALLBACK_PROVIDERS` entry, or add an `EXCLUDED_NETWORKS` entry. A failing internal network is fixed or removed in `PINAX_NETWORKS`. A removed alias is a breaking change for its users.

## What should not happen

- No runtime fetch during normal CLI startup
- No silent endpoint drift caused by external registry changes
- No automatic provider selection beyond the explicit fallback list
- No alias outside the registry except the reviewed `PINAX_NETWORKS` entries

## Endpoint metadata

For alias generation, the registry snapshot and `PINAX_NETWORKS` are enough.

Querying each endpoint for `EndpointInfo/Info` is a separate validation step (the staleness check above), not part of generation. Generation stays offline because network calls slow refreshes and can fail for temporary endpoint availability reasons.

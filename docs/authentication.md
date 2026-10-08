# Authentication

Credentials are selected from the **resolved endpoint host**, including any
`--endpoint`, `ENDPOINT`, or `FIREHOSE_ENDPOINT_*` override:

| Destination | API key environment variables, in priority order | Bearer token environment variables, in priority order |
|---|---|---|
| Built-in Pinax host over HTTPS on port 443 | `PINAX_API_KEY`, then `SUBSTREAMS_API_KEY` | `PINAX_API_TOKEN`, then `SUBSTREAMS_API_TOKEN` |
| Built-in StreamingFast host over HTTPS on port 443 | `STREAMINGFAST_API_KEY` | `STREAMINGFAST_API_TOKEN` |
| Other host, port, or plaintext connection | No automatic credentials | No automatic credentials |

A built-in host is the endpoint host of a built-in `--network` alias: the
registry's aliases and the reviewed Pinax networks the registry does not list
yet ([internal Pinax networks](network-registry-integration.md#internal-pinax-networks)),
such as `hypercore` (`hypercore.firehose.pinax.network`). HyperCore therefore
receives `PINAX_API_KEY` (or `SUBSTREAMS_API_KEY`) without a selector, also
through `--endpoint https://hypercore.firehose.pinax.network:443`.

```bash
export PINAX_API_KEY=your-pinax-api-key
# For near-mainnet, near-testnet, tron, or tron-evm:
export STREAMINGFAST_API_TOKEN=your-streamingfast-compatible-token
```

`SUBSTREAMS_API_KEY` and `SUBSTREAMS_API_TOKEN` are legacy **Pinax-only**
fallbacks. If you previously used `SUBSTREAMS_API_TOKEN` with StreamingFast,
move that token to `STREAMINGFAST_API_TOKEN` or explicitly select it with
`--api-token-envvar SUBSTREAMS_API_TOKEN` for that endpoint.

For a custom endpoint, explicitly select the credential names with
`--api-key-envvar` / `--api-token-envvar` (or `API_KEY_ENVVAR` /
`API_TOKEN_ENVVAR`). An explicit selector authorizes that credential for the
chosen destination and overrides automatic selection for that header. If the
selected variable is unset or blank, that header is omitted; it does not fall
back to another variable. The other header still follows its own selection
rules. Only explicitly select a credential for a destination you intend it to reach.

An explicit selector is **not provider-scoped**. If `API_KEY_ENVVAR` or
`API_TOKEN_ENVVAR` is set globally, for example to the legacy
`SUBSTREAMS_API_KEY` in a shared `.env` or container environment, that
credential is sent to every endpoint the process connects to, including
StreamingFast and custom hosts. Startup logs a `WARN` line naming the variable
(never its value) and the host whenever an explicitly selected credential is
sent to a non-Pinax host, with a stronger message for Pinax and legacy
`SUBSTREAMS_*` names. A `STREAMINGFAST_*` variable sent to a built-in
StreamingFast host is its normal destination and is not warned about. When
migrating, unset global selectors and rely on the provider-scoped variables
above, or pass `--api-key-envvar` / `--api-token-envvar` only on the command
for the intended endpoint.

Startup logs identify the destination host, provider, and names of credential
variables selected for transmission (`none` when absent), never their values.
Surrounding whitespace is trimmed, so a key mounted from a secret file with a
trailing newline works. A selected credential that still contains characters a
gRPC header cannot carry (control characters or line breaks inside the value)
fails at startup with an error.

## Advanced authentication

Most deployments should use the provider-scoped variables in [Authentication](#authentication).
For custom endpoints or different secret names, explicitly authorize a credential
for the destination with:

- `--api-key-envvar <API_KEY_ENVVAR>`
- `--api-token-envvar <API_TOKEN_ENVVAR>`

```bash
fireparq build \
  --network mainnet \
  --api-key-envvar INTERNAL_FIREHOSE_API_KEY \
  --start-block 20000000 \
  --stop-block 20001000 \
  --output ./output
```

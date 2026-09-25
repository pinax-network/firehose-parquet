# NEAR live qualification remains blocked (#506 / PR #559)

## Preserved work and integration

On 2026-09-25, qualification started from PR #559's published head
`8056a07d358bf1ddbe9a091d862377fd26cae478` in a new isolated branch,
`codex/near-live-qualification`. Main `774cc65c1025ba7b4ca0d2f1546d1e309a7b9742`
was merged locally as `de2484668920e709ff60e86cfca5601b7618c320` without conflicts.
The original `audit/506-near-joins-logs-actions` branch and its Claude worktree
remain unchanged at `8056a07` and clean. Nothing was pushed to the original PR.

This integration includes current provider-scoped authentication and bounded
EndpointInfo startup handling. No application implementation was changed in
this qualification attempt.

## Credential selection and bounded request

The repository `.env` has a legacy `SUBSTREAMS_API_TOKEN`. Its issuer was checked
internally and recognized as StreamingFast. Only credential presence and the
provider classification were exposed; no token value or private claims were
printed or saved as evidence.

The calling shell also has legacy credentials. Those were deliberately excluded
from the request subprocess. The repository's confirmed StreamingFast token was
passed only as the subprocess's `STREAMINGFAST_API_TOKEN`, with one Bearer header
expanded from that variable. No Pinax credential or API-key header was sent.

The new authentication rules treat ambient `SUBSTREAMS_*` values as Pinax-only
fallbacks. Any future qualification run must use the provider-specific variable
or explicitly select the known StreamingFast token for this endpoint; the
original PR's ambient-legacy invocation should not be reused unchanged.

At approximately 2026-09-25 15:35 UTC, exactly one gRPC request was made:

- Endpoint: `mainnet.near.streamingfast.io:443`, using TLS.
- RPC: `sf.firehose.v2.Stream/Blocks`.
- Start and stop block: `150000000`, requesting one finalized block.
- RPC deadline: 20 seconds; process timeout: 25 seconds.
- Response payload: zero bytes. No raw block or cursor was received.
- Client exit status: 66.

## Sanitized blocker

```text
Code: Unknown
Message: Quota exceeded: billable egress bytes quota exceeded (quota '5368709120', current '15762311347')
```

This is the same quota rejection recorded in PR #559. It demonstrates that the
provider still refuses this account's read; it does not establish a fresh data
or schema validation result. No second request, alternate credential attempt,
or larger block range was tried. The empty temporary response file was removed.

## Qualification verdict and next step

**Live qualification is incomplete.** Issue #506 explicitly requires a live
NEAR block comparison, and the provider returned no block. PR #559 should remain
unmerged and issue #506 open on this evidence.

The StreamingFast account administrator must restore sufficient egress quota
or supply an intended, authorized StreamingFast credential with available quota.
Once that access changes, resume at block `150000000`, extending only within
the reviewed five-block window `150000000..150000004` if additional coverage is
needed. Compare raw protobuf data with Parquet for:

- Every receipt log line verbatim and its `log_index` order.
- Receipt actions, kinds, arguments, deposits, gas and `action_index` order.
- Transaction and receipt position keys across chunks/shards.
- Converted receipt IDs, generated receipt IDs, same-block origin joins and
  explicitly unknown cross-block origins.
- Round-trip consistency and current-main workspace tests after a live pass.

No Cargo build or test run was started in this attempt because the prerequisite
live read failed. The earlier PR's reported tests are historical evidence, not
a current-main qualification result. Future shared-target Cargo commands must
use the whole-process lock wrapper to avoid concurrent rustdoc artifact races.

# firehose-parquet: unreleased

Changes merged since [v1.7.0](v1.7.0.md). Fold this file into
`docs/releases/vX.Y.Z.md` when the next release is cut, then reset it to this
template.

Add one entry per change under the matching heading, with its issue and PR
(`#N` refs are fine). Say what changed for operators or data consumers, what
they must do (for example rebuild into a new output root), and link the record
in `docs/audit/` or elsewhere when there is one. Remove headings that stay
empty when the release is cut.

## Breaking changes

- **Tron is no longer supported: the Tron decoder, `--block-type tron`, the
  `tron_base58` encoding and the Tron-style EVM profile are removed
  (#747).** Pinax does not serve or support Tron, and v1.7.0 removed its
  network aliases (#735). This corrects v1.7.0's note that Tron blocks still
  decode from a Tron Firehose given with `--endpoint`: they no longer do.
  - `--block-type tron` is refused as an unsupported block type, and
    `--block-type auto` refuses a Tron Firehose's `sf.tron.type.v1.Block`.
  - An endpoint whose chain name is `tron-evm` is now plain EVM: every byte
    field is `0x` hex, with no Tron Base58Check addresses. Write it into a new
    output root.
  - An existing Tron dataset, native `tron` or EVM written from a `tron-evm`
    endpoint (`tron_base58`), is refused by name: `build` does not resume it,
    and `backfill`, `recovery recover` and `recovery rebind` refuse it, each
    when it loads the dataset's state and before it writes a table or that
    state ("this dataset was written from Tron, which fireparq no longer
    supports"). On S3 the refusal leaves the bucket's owner record released,
    as any failure with no uncertain request does, so it needs no `recovery
    release` afterwards. `recovery status` and `recovery release` still work
    on it, `fireparq maintenance` still compacts, VACUUMs and checkpoints its
    tables (it reads only their Delta logs), and `fireparq inspect` still
    reads its files. Every Delta reader (DuckDB `delta_scan`, Polars
    `scan_delta`) still reads its tables, whose schema is in
    [v1.7.0's docs](https://github.com/pinax-network/firehose-parquet/blob/v1.7.0/docs/schemas/tron.md).
    To keep writing one, stay on v1.7.0.

## New features

## Fixes

- **A malformed `./.env` no longer breaks `--help`, `--version` or
  `completions` (#637).** The env file is loaded before the arguments are
  parsed, so before this fix a malformed `./.env`, or a missing
  `--env-file` / `FIREPARQ_ENV_FILE` path, failed `fireparq --help`, `-h`,
  `--version`, `-V`, `fireparq help`, a subcommand's `--help` and
  `fireparq completions <shell>` with `invalid env file …`. They now run as
  usual: help and the version print a stderr warning naming the file,
  `completions` stays silent. Every other command still fails on such a file
  before it reads or writes anything, as before.
- **A SEC writer no longer stops on two disconnects in a quiet day (#711).**
  `--reconnect-stall-timeout-secs` runs from the first failed reconnect until a
  message arrives, so its generic 900 s ended a healthy SEC writer with
  `reconnect stalled` when the stream dropped twice, 15 minutes apart, between
  two daily bursts. A SEC build now defaults it to 129600 (36 hours, as
  `--metrics-stale-after-secs`), when neither the flag nor
  `RECONNECT_STALL_TIMEOUT_SECS` sets it; the `--reconnect-stall-timeout-secs 0`
  workaround is no longer needed. The cap of 30 failed attempts without a
  message still stops a dead endpoint after 10 to 31 minutes, and still counts
  each disconnect of a quiet day: a gateway in front of firesec must not close
  idle streams more often than about once an hour
  ([SEC notes](../chains/sec.md)).
- **The idle flush also runs while `build` reconnects (#711).** With
  `--flush-idle-secs` (60 s for SEC), the rows of a burst were held until a new
  stream when the endpoint went down right after it; they are now committed
  once the stream has been quiet that long, reconnecting or not. A failed flush
  ends the run, as during a stream.
- **The idle-reconnect log prints `idle_for=120s` again (#711),** not
  `idle_for=Some(120s)`, on every chain.

## Performance

## Internal

- **SEC code cleanup (#711).** The SEC modules no longer allow dead code: the
  unused helpers are removed or test-only. Comments no longer point to a local
  `/tmp` specification or oracle. The 16 `#[ignore]`d tests that compared the
  mapper with the prototype on local sample days are removed, since
  `blocks/tests/sec_golden.rs` compares every column of every table with the
  prototype's rows on the golden fixture. The eight that checked the tests'
  mirrors against real filings now read those filings from the fixture and run
  in CI.

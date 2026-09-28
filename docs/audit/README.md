# Audit implementation records

These records document the diagnosis, implementation decisions, validation and
limits of each fix from the September 2026 code audit
([#463](https://github.com/pinax-network/firehose-parquet/issues/463)). The linked
GitHub issues and PRs provide the lifecycle state; a local implementation or a
passing test alone is not closure. The user-facing summary of all of these
changes is the [v1.0.0 release notes](../releases/v1.0.0.md).

## Final summary (v1.0.0)

Every issue the audit tracker lists, plus the issues found and fixed during the
audit (#567, #568, #572, #578). Codes and severities come from the tracker.
All of them are closed.

| Issue | Code | Severity | PR(s) | Outcome |
|---|---|---|---|---|
| [#464](https://github.com/pinax-network/firehose-parquet/issues/464) | A1 | critical | [#532](https://github.com/pinax-network/firehose-parquet/pull/532), [#600](https://github.com/pinax-network/firehose-parquet/pull/600) | A failed write discards the uncommitted window, advances neither authority nor cursor and exits non-zero; recovery removes published parts before replay. |
| [#465](https://github.com/pinax-network/firehose-parquet/issues/465) | A2 | critical | [#538](https://github.com/pinax-network/firehose-parquet/pull/538) | An unreadable cursor fails the run instead of silently restarting from scratch; local cursor saves are atomic. |
| [#466](https://github.com/pinax-network/firehose-parquet/issues/466) | A3 | high | [#544](https://github.com/pinax-network/firehose-parquet/pull/544) | Blocks below `--start-block` are skipped; a bounded run succeeds only after committing `stop_block - 1`; a live stream that closes cleanly reconnects. |
| [#467](https://github.com/pinax-network/firehose-parquet/issues/467) | A4 | high | [#570](https://github.com/pinax-network/firehose-parquet/pull/570) | Startup stops unless EndpointInfo succeeds, so a transient failure can no longer change the output root or cursor path. |
| [#468](https://github.com/pinax-network/firehose-parquet/issues/468) | A5 | high | [#591](https://github.com/pinax-network/firehose-parquet/pull/591), [#600](https://github.com/pinax-network/firehose-parquet/pull/600), [#619](https://github.com/pinax-network/firehose-parquet/pull/619) | All-table ingestion transactions: deterministic `part-v1-*` names, output authority under `.fireparq-ingest/`, dataset ownership and `recovery` subcommands. |
| [#469](https://github.com/pinax-network/firehose-parquet/issues/469) | A6 | medium | [#571](https://github.com/pinax-network/firehose-parquet/pull/571) | A cursor save that cannot be persisted stops `build` instead of logging a warning. |
| [#470](https://github.com/pinax-network/firehose-parquet/issues/470) | A7 | medium | [#573](https://github.com/pinax-network/firehose-parquet/pull/573) | An explicit `s3://bucket/key` cursor uses its own bucket; relative cursors inherit the output bucket. |
| [#471](https://github.com/pinax-network/firehose-parquet/issues/471) | A8 | medium | [#553](https://github.com/pinax-network/firehose-parquet/pull/553) | Zero-valued sizes are rejected or mean disabled, bounded runs never send `stop_block_num = 0`, and credentials are trimmed and validated once. |
| [#472](https://github.com/pinax-network/firehose-parquet/issues/472) | A9 | medium | [#556](https://github.com/pinax-network/firehose-parquet/pull/556) | Fatal gRPC statuses end the run; back-off and the stall timer reset only on stream messages; 30 failed attempts in a row end the run. |
| [#473](https://github.com/pinax-network/firehose-parquet/issues/473) | A10 | medium | [#579](https://github.com/pinax-network/firehose-parquet/pull/579) | SIGINT/SIGTERM interrupt idle waits, back-off and connection attempts; a second signal exits with 130. |
| [#474](https://github.com/pinax-network/firehose-parquet/issues/474) | A11 | medium | [#597](https://github.com/pinax-network/firehose-parquet/pull/597) | `--final-blocks-only=false` enables append-only non-final output; NEW/UNDO semantics and query limits are documented. |
| [#475](https://github.com/pinax-network/firehose-parquet/issues/475) | A12 | medium | [#587](https://github.com/pinax-network/firehose-parquet/pull/587) | Single `_total` suffix, no unbounded `partition` label, misleading gauges removed, `/ready` follows stream state. |
| [#476](https://github.com/pinax-network/firehose-parquet/issues/476) | A13 | low | [#584](https://github.com/pinax-network/firehose-parquet/pull/584) | Invalid timestamps and streamed blocks without metadata are errors instead of routing to 1970 or block 0. |
| [#477](https://github.com/pinax-network/firehose-parquet/issues/477) | A14 | medium | [#581](https://github.com/pinax-network/firehose-parquet/pull/581) | The unreachable partition-split writer path is removed; every mapper flush targets one partition; the `OutputWriter` buffering is superseded by #643 (L5b), which removes it, while its partition contract stays. |
| [#478](https://github.com/pinax-network/firehose-parquet/issues/478) | B1 | critical | [#536](https://github.com/pinax-network/firehose-parquet/pull/536), [#624](https://github.com/pinax-network/firehose-parquet/pull/624) | Rollup re-runs no longer delete or duplicate rows; in-place runs need `--delete-source`; reserved root files are skipped; rollups are journaled. `rollup` was later removed by #652. |
| [#479](https://github.com/pinax-network/firehose-parquet/issues/479) | B2 | high | [#542](https://github.com/pinax-network/firehose-parquet/pull/542), [#624](https://github.com/pinax-network/firehose-parquet/pull/624) | `merge` and `rollup` refuse partitions whose files differ in schema or value metadata; superseded by #652 and #643 (L5a), which remove `rollup` and `merge`. |
| [#480](https://github.com/pinax-network/firehose-parquet/issues/480) | B3 | high | [#561](https://github.com/pinax-network/firehose-parquet/pull/561) | Partition merges are journaled in `_fireparq_merge.json`, outputs are atomic, and overlapping merges are refused; superseded by #643 (L5a), which removes `merge`. |
| [#481](https://github.com/pinax-network/firehose-parquet/issues/481) | B4 | high | [#549](https://github.com/pinax-network/firehose-parquet/pull/549) | `truncate` ANDs filters across keys, supports path filters and needs `--yes`; destructive commands no longer fall back to `S3_BUCKET`; superseded by #643 (L5a), which removes `truncate` and `merge`. |
| [#482](https://github.com/pinax-network/firehose-parquet/issues/482) | C1 | high | [#531](https://github.com/pinax-network/firehose-parquet/pull/531) | `block_range` partition rows are ordered and filtered by numeric value; superseded by #653, which removes the partition index. |
| [#483](https://github.com/pinax-network/firehose-parquet/issues/483) | C2 | high | [#545](https://github.com/pinax-network/firehose-parquet/pull/545) | `partitions build` refuses implicit index rewrites and resumes only from the recorded frontier; superseded by #653, which removes `partitions build`. |
| [#484](https://github.com/pinax-network/firehose-parquet/issues/484) | C3 | medium | [#537](https://github.com/pinax-network/firehose-parquet/pull/537), [#618](https://github.com/pinax-network/firehose-parquet/pull/618) | `validate` detects timestamp reversals in Timestamp columns, at millisecond precision. |
| [#485](https://github.com/pinax-network/firehose-parquet/issues/485) | C4 | medium | [#583](https://github.com/pinax-network/firehose-parquet/pull/583), [#618](https://github.com/pinax-network/firehose-parquet/pull/618) | Probe timeouts no longer look like missing blocks; live `partitions build` retries transient failures; superseded by #653, which removes the probes. |
| [#486](https://github.com/pinax-network/firehose-parquet/issues/486) | C5 | medium | [#592](https://github.com/pinax-network/firehose-parquet/pull/592), [#618](https://github.com/pinax-network/firehose-parquet/pull/618) | `partitions.parquet` v2 records verified finalized coverage and span proofs; consumers refuse incomplete matches; `partitions validate` checks v2; superseded by #653, which removes the index. |
| [#487](https://github.com/pinax-network/firehose-parquet/issues/487) | D1 | high | [#533](https://github.com/pinax-network/firehose-parquet/pull/533) | `merkle_v2` roots (domain separation, committed row count) detect duplicated trailing rows; superseded by #643 (L5a), which removes `verify` for the launch; #666 brings it back. |
| [#488](https://github.com/pinax-network/firehose-parquet/issues/488) | D2 | high | [#546](https://github.com/pinax-network/firehose-parquet/pull/546) | One registry per network in its dataset root (`_fireparq/merkle_roots.parquet` since #647); chain and table are inferred from the data; superseded by #643 (L5a), which removes `verify` and its registry; #666 brings them back. |
| [#489](https://github.com/pinax-network/firehose-parquet/issues/489) | D3 | medium | [#555](https://github.com/pinax-network/firehose-parquet/pull/555), [#621](https://github.com/pinax-network/firehose-parquet/pull/621) | Failing runs never write the registry; writes are atomic or conditional; `verify` runs beside `build` and skips open partitions; superseded by #643 (L5a), which removes `verify`; #666 brings it back. |
| [#490](https://github.com/pinax-network/firehose-parquet/issues/490) | D4 | medium | [#540](https://github.com/pinax-network/firehose-parquet/pull/540), [#621](https://github.com/pinax-network/firehose-parquet/pull/621) | Every Arrow type, including timestamps and structs, has an explicit leaf encoding instead of display strings; superseded by #643 (L5a), which removes `verify`; #666 keeps the encoding. |
| [#491](https://github.com/pinax-network/firehose-parquet/issues/491) | E1 | high | [#539](https://github.com/pinax-network/firehose-parquet/pull/539) | Canonical `timestamp` is `Timestamp(Millisecond, UTC)`, a real Parquet timestamp. |
| [#492](https://github.com/pinax-network/firehose-parquet/issues/492) | E2 | critical | [#534](https://github.com/pinax-network/firehose-parquet/pull/534) | Tron transaction time columns renamed to `tx_timestamp_ms` / `expiration_ms`; every schema is tested for unique names. |
| [#493](https://github.com/pinax-network/firehose-parquet/issues/493) | E3 | low | [#543](https://github.com/pinax-network/firehose-parquet/pull/543) | The day-of-month partition directory is `day=DD`; superseded by #652, which writes `date=YYYY-MM-DD` only. |
| [#494](https://github.com/pinax-network/firehose-parquet/issues/494) | F1 | high | [#547](https://github.com/pinax-network/firehose-parquet/pull/547) | EVM includes failed transactions by default with only their persistent state changes; `--exclude-failed-transactions` added. |
| [#495](https://github.com/pinax-network/firehose-parquet/issues/495) | F2 | medium | [#554](https://github.com/pinax-network/firehose-parquet/pull/554) | EVM change tables gain `tx_index`, `call_index`, `state_reverted` and `persisted`. |
| [#496](https://github.com/pinax-network/firehose-parquet/issues/496) | F3 | medium | [#557](https://github.com/pinax-network/firehose-parquet/pull/557) | Missing EVM block, transaction, call and log fields are written. |
| [#497](https://github.com/pinax-network/firehose-parquet/issues/497) | F4 | medium | [#558](https://github.com/pinax-network/firehose-parquet/pull/558) | New EVM `withdrawals`, `access_lists` and `set_code_authorizations` tables. |
| [#498](https://github.com/pinax-network/firehose-parquet/issues/498) | F5 | low | [#575](https://github.com/pinax-network/firehose-parquet/pull/575) | EVM `log_index` versus RPC block index and tables absent from current Firehose blocks are documented. |
| [#499](https://github.com/pinax-network/firehose-parquet/issues/499) | F6 | medium | [#588](https://github.com/pinax-network/firehose-parquet/pull/588), [#616](https://github.com/pinax-network/firehose-parquet/pull/616) | Offline golden-block regression over retained ETH mainnet data, including EIP-7702. |
| [#500](https://github.com/pinax-network/firehose-parquet/issues/500) | G1 | medium | [#582](https://github.com/pinax-network/firehose-parquet/pull/582) | Solana `reward_index` is scoped to the block and independent of flush windows. |
| [#501](https://github.com/pinax-network/firehose-parquet/issues/501) | G2 | medium | [#586](https://github.com/pinax-network/firehose-parquet/pull/586) | Solana vote filtering applies only to simple vote transactions; administrative activity is kept. |
| [#502](https://github.com/pinax-network/firehose-parquet/issues/502) | G3 | low | [#585](https://github.com/pinax-network/firehose-parquet/pull/585) | Solana `instructions` gain `parent_instruction_index` and `inner_instruction_index`. |
| [#503](https://github.com/pinax-network/firehose-parquet/issues/503) | G4 | high | [#593](https://github.com/pinax-network/firehose-parquet/pull/593) | Solana payloads are Binary and account-index arrays are `List<UInt8>`. |
| [#504](https://github.com/pinax-network/firehose-parquet/issues/504) | G5 | high | [#560](https://github.com/pinax-network/firehose-parquet/pull/560) | Beacon withdrawals, execution requests, BLS changes, committee bits and slashing indices. |
| [#505](https://github.com/pinax-network/firehose-parquet/issues/505) | G6 | medium | [#594](https://github.com/pinax-network/firehose-parquet/pull/594) | Beacon decimal `base_fee_per_gas`, Binary blobs, dictionary `spec` and null presence. |
| [#506](https://github.com/pinax-network/firehose-parquet/issues/506) | G7 | high | [#559](https://github.com/pinax-network/firehose-parquet/pull/559) | NEAR join and position columns plus `receipt_actions` and `execution_logs` tables. |
| [#507](https://github.com/pinax-network/firehose-parquet/issues/507) | G8 | medium | [#626](https://github.com/pinax-network/firehose-parquet/pull/626) | NEAR `success_receipt_id` and rebuilt, attributed `state_changes`; the producer gap is #625. |
| [#508](https://github.com/pinax-network/firehose-parquet/issues/508) | G9 | high | [#590](https://github.com/pinax-network/firehose-parquet/pull/590) | Antelope `db_ops` gain `tx_hash`, `tx_index` and `db_op_index`. |
| [#509](https://github.com/pinax-network/firehose-parquet/issues/509) | G10 | medium | [#595](https://github.com/pinax-network/firehose-parquet/pull/595) | Tron `contracts` and `internal_call_values` tables, receipt fees and decoded contract parameters. |
| [#510](https://github.com/pinax-network/firehose-parquet/issues/510) | G11 | medium | [#601](https://github.com/pinax-network/firehose-parquet/pull/601) | Cosmos keeps event attribute order, marks unknown results as null and adds transaction metadata. |
| [#511](https://github.com/pinax-network/firehose-parquet/issues/511) | G12 | medium | [#589](https://github.com/pinax-network/firehose-parquet/pull/589) | Bitcoin `value_sats`, input `tx_index` and faithful nulls. |
| [#512](https://github.com/pinax-network/firehose-parquet/issues/512) | H1 | high | [#548](https://github.com/pinax-network/firehose-parquet/pull/548) | Canonical `block_id` / `parent_id` are encoded once per block. |
| [#513](https://github.com/pinax-network/firehose-parquet/issues/513) | H2 | medium | [#596](https://github.com/pinax-network/firehose-parquet/pull/596) | EVM big-integer decimals are written directly into Arrow. |
| [#514](https://github.com/pinax-network/firehose-parquet/issues/514) | H3 | medium | [#563](https://github.com/pinax-network/firehose-parquet/pull/563) | Hex and base58 encode into reused buffers; builders keep capacity. |
| [#515](https://github.com/pinax-network/firehose-parquet/issues/515) | H4 | medium | [#605](https://github.com/pinax-network/firehose-parquet/pull/605) | `--flush-bytes` is an adaptive compressed-file target; `--flush-memory-bytes` bounds mapper buffers; 32 MiB default everywhere. |
| [#516](https://github.com/pinax-network/firehose-parquet/issues/516) | H5 | medium | [#629](https://github.com/pinax-network/firehose-parquet/pull/629) | Stage A: one flush's parts are encoded and published concurrently within `--flush-encode-concurrency`, `--flush-publish-concurrency` and `--flush-inflight-bytes`; stage B is #630. |
| [#517](https://github.com/pinax-network/firehose-parquet/issues/517) | H6 | medium | [#602](https://github.com/pinax-network/firehose-parquet/pull/602) | 16 MiB receive windows, zstd responses and receive-transport flags. |
| [#518](https://github.com/pinax-network/firehose-parquet/issues/518) | H7 | low | [#606](https://github.com/pinax-network/firehose-parquet/pull/606) | Protobuf byte fields share the owned Firehose payload during mapping. |
| [#519](https://github.com/pinax-network/firehose-parquet/issues/519) | H8 | medium | [#608](https://github.com/pinax-network/firehose-parquet/pull/608) | Bounded Bloom filters, 65,536-row groups, sort metadata and `zstd:<level>`. |
| [#520](https://github.com/pinax-network/firehose-parquet/issues/520) | H9 | medium | [#613](https://github.com/pinax-network/firehose-parquet/pull/613) | Native S3 ingestion spools each part to disk and streams one conditional PUT with explicit timeouts. |
| [#521](https://github.com/pinax-network/firehose-parquet/issues/521) | H10 | medium | [#564](https://github.com/pinax-network/firehose-parquet/pull/564) | `verify` builds roots while streaming; protocol-only runs skip hashing; superseded by #643 (L5a), which removes `verify`. |
| [#522](https://github.com/pinax-network/firehose-parquet/issues/522) | H11 | medium | [#604](https://github.com/pinax-network/firehose-parquet/pull/604) | `rollup` streams bounded batches and reads S3 sources through pinned ranges. `rollup` was later removed by #652. |
| [#523](https://github.com/pinax-network/firehose-parquet/issues/523) | H12 | medium | [#609](https://github.com/pinax-network/firehose-parquet/pull/609) | S3 maintenance runs up to ten deletes at once and reads bounded merge windows; superseded by #643 (L5a), which removes `merge` and `truncate`. |
| [#524](https://github.com/pinax-network/firehose-parquet/issues/524) | H13 | low | [#599](https://github.com/pinax-network/firehose-parquet/pull/599) | `validate` reads only canonical columns and compares cached partition endpoints. |
| [#525](https://github.com/pinax-network/firehose-parquet/issues/525) | R1 | medium | [#611](https://github.com/pinax-network/firehose-parquet/pull/611) | Ingestion setup, runtime state and flush windows live in focused modules. |
| [#526](https://github.com/pinax-network/firehose-parquet/issues/526) | R2 | low | [#614](https://github.com/pinax-network/firehose-parquet/pull/614) | `ChainKind` / `ChainProfile` replaces `block_type` string matching. |
| [#527](https://github.com/pinax-network/firehose-parquet/issues/527) | R3 | low | [#610](https://github.com/pinax-network/firehose-parquet/pull/610) | Shared `AwsArgs` and explicitly named S3 client policies. |
| [#528](https://github.com/pinax-network/firehose-parquet/issues/528) | R4 | low | [#612](https://github.com/pinax-network/firehose-parquet/pull/612) | `cli.rs` split into focused modules. |
| [#529](https://github.com/pinax-network/firehose-parquet/issues/529) | R5 | low | [#620](https://github.com/pinax-network/firehose-parquet/pull/620) | One local/S3 engine per maintenance sequence; the `merge`, `truncate` and `verify` engines are superseded by #643 (L5a), which removes those commands. |
| [#530](https://github.com/pinax-network/firehose-parquet/issues/530) | R6 | low | [#603](https://github.com/pinax-network/firehose-parquet/pull/603) | Shared authenticated gRPC client construction. |
| [#535](https://github.com/pinax-network/firehose-parquet/issues/535) | N1 | high | [#541](https://github.com/pinax-network/firehose-parquet/pull/541) | Network registry regenerated: 13 aliases removed, 4 moved to StreamingFast, 5 added; weekly endpoint check. |
| [#550](https://github.com/pinax-network/firehose-parquet/issues/550) | N2 | medium | [#607](https://github.com/pinax-network/firehose-parquet/pull/607), [#622](https://github.com/pinax-network/firehose-parquet/pull/622) | Failed-transaction rules and outcome columns for Solana, Tron, Antelope and NEAR. |
| [#551](https://github.com/pinax-network/firehose-parquet/issues/551) | N3 | low | [#552](https://github.com/pinax-network/firehose-parquet/pull/552) | Flaky partition-boundary test gets its own temporary directory. |
| [#562](https://github.com/pinax-network/firehose-parquet/issues/562) | N4 | high | [#566](https://github.com/pinax-network/firehose-parquet/pull/566), [#615](https://github.com/pinax-network/firehose-parquet/pull/615) | Firehose credentials are selected by the resolved provider host; explicit selectors warn when a credential leaves Pinax. |
| [#565](https://github.com/pinax-network/firehose-parquet/issues/565) | N5 | medium | [#598](https://github.com/pinax-network/firehose-parquet/pull/598) | Safe fixed-size base58 encoder for 32- and 64-byte values. |
| [#567](https://github.com/pinax-network/firehose-parquet/issues/567) | - | - | [#569](https://github.com/pinax-network/firehose-parquet/pull/569) | Ten compatible dependency advisories resolved. |
| [#568](https://github.com/pinax-network/firehose-parquet/issues/568) | - | - | [#577](https://github.com/pinax-network/firehose-parquet/pull/577) | Arrow/Parquet upgrade removes the vulnerable Thrift dependency. |
| [#572](https://github.com/pinax-network/firehose-parquet/issues/572) | - | - | [#576](https://github.com/pinax-network/firehose-parquet/pull/576) | The final mapper flush on completion is checkpointed. |
| [#578](https://github.com/pinax-network/firehose-parquet/issues/578) | - | - | [#580](https://github.com/pinax-network/firehose-parquet/pull/580) | Local Parquet parts are published atomically after sync; the standalone-file writer is superseded by #643 (L5b), and the protected store keeps its durable-directory and no-replace primitives. |
| [#617](https://github.com/pinax-network/firehose-parquet/issues/617) | N6 | high | [#623](https://github.com/pinax-network/firehose-parquet/pull/623) | `.env` is read from the working directory only; S3 writes need an explicit `s3://` destination. |
| [#636](https://github.com/pinax-network/firehose-parquet/issues/636) | - | - | [#675](https://github.com/pinax-network/firehose-parquet/pull/675) | The owner record guards fireparq's writer and its own state only (`.fireparq-ingest/`, `_fireparq/`, its uncommitted parts); the Delta tables are shared through their logs, and `deltalake` OPTIMIZE, VACUUM and checkpoints run beside a live `build` (tested during catch-up and across a restart, local and loopback S3); the README gives the bucket policy for the writer and the maintenance user ([record](636-delta-ownership.md)). |
| [#643](https://github.com/pinax-network/firehose-parquet/issues/643) (L5b, L7) | - | - | [#672](https://github.com/pinax-network/firehose-parquet/pull/672) | The plain-Parquet output is removed (`OutputWriter`, `ParquetTableWriter`'s local and S3 writers and the standalone atomic publication); no command reads a table by walking it: `validate` reads the active files of a pinned Delta snapshot (same checks; OPTIMIZE's tombstoned files and checkpoints are never read as data), `scan` is removed in favor of DuckDB, Polars and a tested README summary from the log, `inspect` reads one file, and the protected-root walker skips `_delta_log/` ([record](643-l5b-l7-readers.md)). |
| [#643](https://github.com/pinax-network/firehose-parquet/issues/643) (L4) | - | - | [#675](https://github.com/pinax-network/firehose-parquet/pull/675) | Recovery closes every crash row of the Delta commits: each start reads every table's `txn` and refuses a log ahead of authority; a Committed transaction is rolled forward into exactly the tables whose `txn` lacks it, `blocks` last, reading only their parts, and a missing one fails closed with the journal kept (§4.1); no part is read once every log or authority holds the transaction, so OPTIMIZE and VACUUM cannot break a restart; a log commit with an unknown outcome no longer latches the S3 owner, since `txn` resolves it; `recovery recover` rolls forward too; real-binary crash tests for every row on local disk and loopback S3, with `deltalake` maintenance between crash and restart ([record](643-l4-delta-recovery.md)). |
| [#643](https://github.com/pinax-network/firehose-parquet/issues/643) (L5a) | - | - | [#670](https://github.com/pinax-network/firehose-parquet/pull/670) | `merge` (and its journal and #655 intent record), `truncate` and `verify` (with its `_fireparq/` registry and reports, its row encoding and its docs) are removed with no compatibility path, as is every ownership and discovery path only they used; the `deltalake` CronJob compacts, a bad dataset is rebuilt into a new root, and #666 brings `verify` back over Delta snapshots ([record](643-l5a-removals.md)). |
| [#643](https://github.com/pinax-network/firehose-parquet/issues/643) (L8, L9) | - | - | [#673](https://github.com/pinax-network/firehose-parquet/pull/673) | CI reads every Delta table of its EVM (final and non-final) and Solana datasets through the log with DuckDB 1.5.5 `delta_scan` (its `delta` extension checksum-pinned) and Polars `scan_delta`, after a checkpoint: exact rows, only the log's files, `date` pruning, Delta types; the reference `deltalake` 1.6.6 maintenance job (`scripts/delta_maintenance.py`: OPTIMIZE of closed dates, lite VACUUM or a full one at >= 168 h, the checkpoint after VACUUM, log cleanup) runs over and over beside a real `build` on local disk and loopback S3 with no writer failure or conflict, exact rows and intact `txn`; example CronJobs; an opt-in anonymous check of a deployment bucket; the spike and its CI job are removed ([record](643-l8-l9-engines-maintenance.md)). |
| [#643](https://github.com/pinax-network/firehose-parquet/issues/643) (L3) | - | - | [#671](https://github.com/pinax-network/firehose-parquet/pull/671) | Every `build` writes Delta tables: one per mapper table, created at the first start and validated on resume (properties, `fireparq.*` identity, protocol, partition, schema); each Committed transaction commits its parts as they are (journaled `add` with statistics, `txn` = last ordinal) to every table with rows, `blocks` last, before authority advances; the S3 log uses a single-attempt conditional-put client. The `txn`-gated recovery roll-forward is L4 ([record](643-l3-delta-commits.md)). |
| [#643](https://github.com/pinax-network/firehose-parquet/issues/643) (L2) | - | - | [#669](https://github.com/pinax-network/firehose-parquet/pull/669) | Every part is a Delta data file: one checked mapping per flush, before the journal, turns `UInt64` into a checked `long` (a value above `i64::MAX` refuses the flush) or, for each chain's listed amounts and unchecked values, `decimal(20,0)`; enums become `string`, timestamps microseconds, and `date` the partition column only; mapper epoch `v3` ([record](643-l2-delta-types.md)). |
| [#647](https://github.com/pinax-network/firehose-parquet/issues/647) | - | - | [#650](https://github.com/pinax-network/firehose-parquet/pull/650) | The cursor mirror, partition index, Merkle registry and verify reports live in `<dataset root>/_fireparq/`, which every walker reserves; legacy root registries and indexes are refused instead of shadowed; the index parts are superseded by #653, and the registry and report parts by #643 (L5a) ([record](647-fireparq-artifact-dir.md)). |
| [#648](https://github.com/pinax-network/firehose-parquet/issues/648) | - | - | [#649](https://github.com/pinax-network/firehose-parquet/pull/649) | Non-final rows carry a durable `stream_ordinal`; the README documents a tested canonical live view, the two-bucket union and the live + final deployment ([record](648-stream-ordinal.md)). |
| [#652](https://github.com/pinax-network/firehose-parquet/issues/652) | - | - | [#660](https://github.com/pinax-network/firehose-parquet/pull/660) | Every table is `<table>/date=YYYY-MM-DD/`, equal to its `date` column; `build --partition` and `rollup` are removed, the mapper epoch refuses pre-release protected roots before Blocks, and CI reads real output with DuckDB and Polars ([record](652-date-partition-key.md)). |
| [#653](https://github.com/pinax-network/firehose-parquet/issues/653) | - | - | [#662](https://github.com/pinax-network/firehose-parquet/pull/662) | The `partitions` subcommands, `_fireparq/partitions.parquet`, the finality and Fetch probes that only the index used, and `--cursor-template` are removed with no compatibility path; Delta log metadata (#643) replaces the index, and a new `build` root must be empty ([record](653-remove-partitions.md)). |
| [#654](https://github.com/pinax-network/firehose-parquet/issues/654) | - | - | [#656](https://github.com/pinax-network/firehose-parquet/pull/656) | `--output` is the dataset root, used exactly as given: `build` no longer appends `<chain_name>`, and the opt-in `{chain}` placeholder names a directory after the network; a template that resolves to another root is refused before Blocks ([record](654-output-template.md)). |
| [#655](https://github.com/pinax-network/firehose-parquet/issues/655) | - | - | [#668](https://github.com/pinax-network/firehose-parquet/pull/668) | A resumed `build` reads only its control records and lists no data objects, on S3 and on local disk; nested and enclosing datasets are checked in full when a dataset is created and through its ancestors on resume; merge journals are looked for only after an interrupted `merge` left `.fireparq-ingest/merge-intent.json` (removed with `merge` by #643 L5a); remaining listings have per-request timeouts and no total deadline; new startup listing metrics ([record](655-resume-cost.md)). |
| [#659](https://github.com/pinax-network/firehose-parquet/issues/659) | - | - | [#664](https://github.com/pinax-network/firehose-parquet/pull/664) | `--flush-interval-secs` applies only at the chain head: a pace detector (block time against the wall clock, with hysteresis) suspends it while `build` catches up, so a catch-up flushes by size; `firehose_parquet_catching_up` and a `pace` label on `flushes_total`, with no new flag ([record](659-adaptive-flush.md)). |

### Validation follow-up PRs

Independent validation of the merged fixes found gaps that were fixed without a
separate issue:

| PR | Scope | Outcome |
|---|---|---|
| [#574](https://github.com/pinax-network/firehose-parquet/pull/574) | Backlog | Audit priorities preserved and crash-recovery acceptance criteria defined ([record](backlog-roadmap.md)). |
| [#615](https://github.com/pinax-network/firehose-parquet/pull/615) | Platform | Zero disables `--flush-rows` / `--flush-interval-secs`; credential-selector warnings; `.env.example` drift test; cargo-deny advisory gate ([record](validation-misc-followups.md)). |
| [#616](https://github.com/pinax-network/firehose-parquet/pull/616) | EVM | Golden regression covers the owned mapping path and an EIP-7702 block ([record](validation-misc-followups-evm.md)). |
| [#618](https://github.com/pinax-network/firehose-parquet/pull/618) | Partitions | Millisecond `validate`, live `partitions build` retries, millisecond index time columns, v2 `partitions validate`; the partitions parts are superseded by #653 ([record](validation-misc-followups-partitions.md)). |
| [#619](https://github.com/pinax-network/firehose-parquet/pull/619) | Ingestion | `--cursor none`, `--cursor-override` refusal for real builds, real-binary ingestion regressions ([record](validation-ingest-followups.md)). |
| [#621](https://github.com/pinax-network/firehose-parquet/pull/621) | Verify | Struct roots, `verify` beside `build`, configured-artifact exclusion, unfinished merge/rollup refusal; superseded by #643 (L5a), which removes `verify` ([record](validation-verify-followups.md)). |
| [#624](https://github.com/pinax-network/firehose-parquet/pull/624) | Maintenance | Crash-safe rollup journal, copy ownership marker and value-metadata checks; superseded by #652 and #643 (L5a), which remove `rollup` and `merge` ([record](maintenance-safety-followups.md)). |
| [#639](https://github.com/pinax-network/firehose-parquet/pull/639) | Pre-release polish | Crash-test hooks gated out of release builds, `recovery` reads `AWS_ENDPOINT_URL_S3`, `partitions build --output` / `--network` / `--stop-block` validated at parse time (removed with `partitions` by #653), help text updated for protected resume, weekly advisory scan. |
| [#640](https://github.com/pinax-network/firehose-parquet/pull/640) | S3 ownership | A failed S3 `build` releases bucket ownership when every request had a definite outcome; retained owners print the exact recovery commands ([record](s3-owner-safe-release.md)). |
| [#641](https://github.com/pinax-network/firehose-parquet/pull/641) | Startup log | The `missing_blocks` startup summary line no longer claims probing. |

Release tooling fixes in the same range: [#627](https://github.com/pinax-network/firehose-parquet/pull/627)
restores the release workflow's toolchain setup and adds a dry-run dispatch;
[#628](https://github.com/pinax-network/firehose-parquet/pull/628) allows build-only
Docker runs before a release.

### Follow-ups

| Item | Status |
|---|---|
| [#625](https://github.com/pinax-network/firehose-parquet/issues/625) | Open. Every known StreamingFast NEAR producer leaves block state changes empty, so NEAR `state_changes` stays empty. |
| [#630](https://github.com/pinax-network/firehose-parquet/issues/630) | Open. #516 stage B: decouple gRPC receive and mapping from the writer ([design](516-concurrency-design.md#stage-b-follow-up)). |
| [#658](https://github.com/pinax-network/firehose-parquet/issues/658) | Open. Writer throughput on fast chains: step 1 (benchmark and baseline) is recorded ([record](658-live-flush-benchmark.md)); removing the measured round trips is next. |
| [#635](https://github.com/pinax-network/firehose-parquet/issues/635) | Open. Protected datasets are bound to their output location; a copied or moved dataset cannot be maintained or resumed, and there is no relocation procedure. |
| [#637](https://github.com/pinax-network/firehose-parquet/issues/637) | Open. A malformed `./.env` breaks `--help` and `--version`, because it is loaded before argument parsing. |
| [#631](https://github.com/pinax-network/firehose-parquet/issues/631) | Open. NEAR, Tron and Cosmos output was compared with RPC and public sources, not with a Firehose stream of those chains ([#506](506-near-public-qualification.md), [#509](509-tron-rpc-qualification.md), [#510](510-cosmos-values.md)). |
| [#632](https://github.com/pinax-network/firehose-parquet/issues/632) | Open. object_store 0.14 is needed to clear RUSTSEC-2026-0194/0195 (quick-xml 0.38); both advisories are ignored with a reason in `deny.toml`. |
| [#633](https://github.com/pinax-network/firehose-parquet/issues/633) | Open. Replace the unmaintained `backoff` and `bincode` crates (RUSTSEC-2025-0012, RUSTSEC-2025-0141, ignored with a reason in `deny.toml`). |
| [#634](https://github.com/pinax-network/firehose-parquet/issues/634) | Open. No retained real-data block records a gas change, so the golden regression has no real `gas_changes` rows ([record](validation-misc-followups-evm.md)). |

## After v1.0.0

Production fixes after the v1.0.0 release, with their patch release.

| Issue | Release | PR(s) | Outcome |
|---|---|---|---|
| [#678](https://github.com/pinax-network/firehose-parquet/issues/678) | [v1.0.1](../releases/v1.0.1.md) | - | Ceph RGW 19.2 compares `If-Match` with the unquoted ETag and refused every correct quoted CAS, so S3 ownership could not be acquired. The canary now chooses the ETag form per store (as returned, else a full unquoted rerun that must pass every step, else fail closed), and every conditional request through the owner uses it: owner record, control state, S3 cursor mirror, pinned part reads ([record](rgw-if-match-etag.md)). |

## Records by area

### Ingestion safety and durability

- [#468: proposed crash/replay recovery design](468-crash-recovery-design.md)
- [#468: accepted transaction implementation plan](468-ingestion-transaction-plan.md)
- [#468: staged ownership and durable state implementation](468-stage1-ownership.md)
- [#468: conditional S3 ownership qualification](468-s3-ownership.md)
- [#678: Ceph RGW 19.2 `If-Match` ETag form, chosen by the canary](rgw-if-match-etag.md)
- [#468: single-attempt remote mutations](468-s3-mutation-attempts.md)
- [#468/#591: release S3 ownership after a provably safe build failure](s3-owner-safe-release.md)
- [#468: transaction records and accepted-event frontier](468-transaction-records.md)
- [#468: protected writer preparation and publication](468-protected-writer-preparation.md)
- [#468: all-table controller and physical recovery](468-transaction-controller.md)
- [#468: protected cursor mirror](468-protected-cursor-mirror.md)
- [#468: protected maintenance and recovery](468-protected-maintenance.md) (maintenance parts superseded by #643 L5a)
- [#468: ingestion session assembly](468-ingestion-session.md)
- [#468: verification artifact ownership](468-verify-ownership.md) (superseded by #643 L5a)
- [#468: complete runtime, migration and live recovery qualification](468-ingestion-runtime.md)
- [#468: live table equality and recovered-state evidence](468-live-comparison.json)
- [#643 L3: Delta commits of every transaction, table creation and validation](643-l3-delta-commits.md)
- [#643 L4: Delta recovery gated by `txn`, the log-commit latch decision, and every crash row tested](643-l4-delta-recovery.md)
- [#636: ownership beside `deltalake` maintenance, and the bucket policy](636-delta-ownership.md)
- [Validation follow-ups for protected ingestion (#464, #465, #466, #468, #469, #470, #472, #572, #578)](validation-ingest-followups.md)
- [#467: stable startup destinations](467-endpoint-info.md)
- [#469: durable cursor persistence](469-durable-cursor-saves.md)
- [#470: exact S3 cursor buckets](470-s3-cursor-buckets.md)
- [#473: responsive shutdown](473-responsive-shutdown.md)
- [#474: append-only non-final streams and query limits](474-non-final-streams.md)
- [#648: durable per-row `stream_ordinal`, the canonical live view and live bucket expiry](648-stream-ordinal.md)
- [#475: bounded metrics and stream readiness](475-metrics-readiness.md)
- [#476: timestamp and streamed identity validation](476-timestamp-validation.md)
- [#477: single-partition writer contract](477-writer-partition-contract.md) (`OutputWriter` superseded by #643 L5b)
- [#572: final completion checkpoints](572-final-completion-checkpoint.md)
- [#578: atomic local Parquet publication](578-atomic-local-parquet.md) (standalone writer superseded by #643 L5b)
- [#655: resume cost independent of data size, and the overlap argument](655-resume-cost.md) (merge-intent part superseded by #643 L5a)

### Security and configuration

- [#562: provider-scoped credentials](562-provider-credentials.md)
- [#617: explicit S3 write destinations and working-directory env files](617-explicit-s3-writes.md)
- [#654: `--output` is the dataset root, with an opt-in `{chain}` placeholder](654-output-template.md)
- [#567: compatible dependency security refresh](dependency-security-refresh.md)
- [#568: Arrow/Parquet security and compatibility](568-arrow-parquet-security.md)
- [Validation follow-ups 1-9: platform fixes, and index of all 17 findings](validation-misc-followups.md)

### Maintenance, verify and partitions

- [#485: partition probe reliability](485-partition-probe-reliability.md) (superseded by #653)
- [#486: exact finalized partition coverage and strict consumers](486-partition-index-design.md) (superseded by #653)
- [Validation follow-ups 10-15: validate precision, live partition retries, index time type and v2 validation](validation-misc-followups-partitions.md) (partitions parts superseded by #653)
- [Verify follow-ups: Struct roots, verify beside build, registry scan exclusion, merge refusal](validation-verify-followups.md) (superseded by #643 L5a)
- [Rollup crash safety, copy ownership and value metadata (#478, #479, #480, #522 follow-ups)](maintenance-safety-followups.md) (superseded by #652 and #643 L5a)
- [#647: dataset artifacts under `_fireparq/`](647-fireparq-artifact-dir.md) (registry and report parts superseded by #643 L5a, walker reservations by #643 L5b/L7)
- [#652: one `date=YYYY-MM-DD` partition key, `rollup` removed, DuckDB and Polars engine test](652-date-partition-key.md)
- [#653: `partitions.parquet` and the `partitions` subcommands removed, replaced by Delta log metadata](653-remove-partitions.md)
- [#643 L5a: `merge`, `truncate` and `verify` removed](643-l5a-removals.md)
- [#643 L5b/L7: plain-Parquet output and readers removed; `validate` on Delta snapshots, `scan` removed, `inspect` one file](643-l5b-l7-readers.md)
- [#643 L8, L9: engine CI on the Delta tables, and the `deltalake` maintenance job beside `build`](643-l8-l9-engines-maintenance.md)

### Chain schemas and values

- [#643 L2: Delta column types at the flush boundary, per-chain `decimal(20,0)` columns](643-l2-delta-types.md)
- [#498: EVM log indices and optional tables](498-evm-log-indices.md)
- [#499: offline EVM golden-block regression](499-evm-golden-fixture.md)
- [Validation follow-ups 16-17: owned EVM mapping path and EIP-7702 golden block](validation-misc-followups-evm.md)
- [#500: stable Solana reward indices](500-solana-reward-index.md)
- [#501: conservative Solana vote classification](501-solana-vote-classification.md)
- [#502: explicit Solana instruction positions](502-solana-instruction-order.md)
- [#503: Binary Solana payloads and account-index lists](503-solana-binary-payloads.md)
- [#550: Solana parent transaction outcome context](550-solana-execution-context.md)
- [#504: Beacon live mapping qualification](504-beacon-qualification.md)
- [#505: Beacon value semantics and migration](505-beacon-values.md)
- [#506: original NEAR Firehose quota blocker](506-near-qualification.md)
- [#506: NEAR bounded public-source qualification](506-near-public-qualification.md)
- [#507: NEAR status semantics, state-change attribution and encoding](507-near-status-state-changes.md)
- [#508: Antelope database-operation joins](508-antelope-db-joins.md)
- [#509: Tron contracts, receipts and internal values](509-tron-contract-fields.md)
- [#509: bounded native RPC qualification and retained evidence](509-tron-rpc-qualification.md)
- [#550: Tron, Antelope and NEAR failed-transaction rules and qualification](550-non-evm-failed-transactions.md)
- [#510: Cosmos event order, unknown results and SDK metadata](510-cosmos-values.md)
- [#511: Bitcoin amounts and input metadata](511-bitcoin-values.md)

### Performance

- [#513: EVM decimal fast path](513-evm-decimal-fast-path.md)
- [#515: adaptive compressed flush sizing and summed mapper limit](515-adaptive-flush-sizing.md)
- [#516: bounded ingestion concurrency design and stage B follow-up](516-concurrency-design.md)
- [#516: stage A bounded flush concurrency, tests and benchmark](516-bounded-flush-concurrency.md)
- [#659: adaptive flush interval: size-based flushes while catching up](659-adaptive-flush.md)
- [#517: measured gRPC receive transport](517-grpc-transport.md)
- [#518: owned protobuf byte buffers](518-owned-protobuf-bytes.md)
- [#519: Parquet lookup properties and explicit compression](519-parquet-lookup-properties.md)
- [#520: bounded native S3 ingestion, memory and wire qualification](520-bounded-s3-ingestion.md)
- [#522: streaming rollup memory and remote range reads](522-streaming-rollup.md)
- [#523: bounded S3 maintenance concurrency](523-s3-maintenance-concurrency.md) (superseded by #643 L5a)
- [#524: projected validation and partition boundary performance](524-validation-performance.md)
- [#565: safe fixed-width Base58 conversion](565-fixed-base58.md)
- [#658: writer catch-up throughput, head-cadence margin and commit phases on Robinhood and Arbitrum One (v1.0.0 baseline)](658-live-flush-benchmark.md)

### Structure

- [#525: ingestion setup, runtime and flush decomposition](525-ingestion-decomposition.md)
- [#526: chain profiles and shared mapper helpers](526-chain-profile.md)
- [#527: shared AWS options and S3 construction](527-shared-aws-configuration.md)
- [#528: CLI module extraction and unchanged-item proof](528-cli-modules.md)
- [#529: shared local/S3 maintenance engines and byte-identical equivalence](529-maintenance-engine.md) (maintenance engines superseded by #643 L5a)
- [#530: shared authenticated gRPC clients](530-grpc-client-deduplication.md)

### Process

- [Backlog priorities and preserved work](backlog-roadmap.md)

## Verified lifecycle outcomes

| Issue | PR | Verified outcome |
|---|---|---|
| [#562](https://github.com/pinax-network/firehose-parquet/issues/562) | [#566](https://github.com/pinax-network/firehose-parquet/pull/566) | Merged; issue closed; credential isolation tests and CI passed. |
| [#467](https://github.com/pinax-network/firehose-parquet/issues/467) | [#570](https://github.com/pinax-network/firehose-parquet/pull/570) | Merged as `774cc65` on 2026-09-25; issue closed; offline failure tests, bounded live Ethereum output/cursor check and CI passed. |
| [#469](https://github.com/pinax-network/firehose-parquet/issues/469) | [#571](https://github.com/pinax-network/firehose-parquet/pull/571) | Merged as `62d7b10` on 2026-09-25; issue closed; 703 tests, bounded live Ethereum checkpoint check and CI passed. Separate skipped-final-save bug remains tracked in #572. |
| [#567](https://github.com/pinax-network/firehose-parquet/issues/567) | [#569](https://github.com/pinax-network/firehose-parquet/pull/569) | Merged as `f3e99f3` on 2026-09-25; issue closed; GitHub rescan confirmed ten alerts closed. At that rescan, only Thrift alert 11 remained, tracked by [#568](https://github.com/pinax-network/firehose-parquet/issues/568). |
| [#470](https://github.com/pinax-network/firehose-parquet/issues/470) | [#573](https://github.com/pinax-network/firehose-parquet/pull/573) | Merged as `fa791ac`; issue closed; 715 combined tests and CI passed, including bucket isolation and signed request destinations. |
| [#498](https://github.com/pinax-network/firehose-parquet/issues/498) | [#575](https://github.com/pinax-network/firehose-parquet/pull/575) | Merged as `c25b9e9`; issue closed; documented query verified against 1,438 live-sample logs; independent review and CI passed. |
| [#572](https://github.com/pinax-network/firehose-parquet/issues/572) | [#576](https://github.com/pinax-network/firehose-parquet/pull/576) | Merged as `ad17332`; issue closed; 720 combined tests, bounded live Solana final checkpoint and CI passed. |
| [#568](https://github.com/pinax-network/firehose-parquet/issues/568) | [#577](https://github.com/pinax-network/firehose-parquet/pull/577) | Merged as `2793421`; issue closed; 723 combined tests, old-file compatibility and 14-table live comparison passed. GitHub marked the remaining alert fixed at 2026-09-25 15:54:54 UTC; zero open alerts verified. |
| [#504](https://github.com/pinax-network/firehose-parquet/issues/504) | [#560](https://github.com/pinax-network/firehose-parquet/pull/560) | Merged as `c88abcc`; issue closed; 730 integrated tests, targeted live BLS/slashing field comparisons and CI passed. |
| [#477](https://github.com/pinax-network/firehose-parquet/issues/477) | [#581](https://github.com/pinax-network/firehose-parquet/pull/581) | Merged as `3f74ebf`; issue closed; 732 tests, 14-table live equality check and CI passed. |
| [#473](https://github.com/pinax-network/firehose-parquet/issues/473) | [#579](https://github.com/pinax-network/firehose-parquet/pull/579) | Merged as `fdb1f98`; issue closed; 739 tests and CI passed, plus combined validation with #477 and atomic publication. |
| [#578](https://github.com/pinax-network/firehose-parquet/issues/578) | [#580](https://github.com/pinax-network/firehose-parquet/pull/580) | Merged as `73257c2`; issue closed; 751 combined tests, atomic publication fault tests, two-block live equality check and CI passed. #468 remains open. |
| [#500](https://github.com/pinax-network/firehose-parquet/issues/500) | [#582](https://github.com/pinax-network/firehose-parquet/pull/582) | Merged as `803ffc3`; issue closed; 752 combined tests, two-block Solana comparison across flush windows and CI passed. |
| [#476](https://github.com/pinax-network/firehose-parquet/issues/476) | [#584](https://github.com/pinax-network/firehose-parquet/pull/584) | Merged as `34cb6e2`; issue closed; 763 tests, malformed metadata and calendar-boundary regressions, 14-table live comparison and CI passed. |
| [#485](https://github.com/pinax-network/firehose-parquet/issues/485) | [#583](https://github.com/pinax-network/firehose-parquet/pull/583) | Merged as `3cf984b`; issue closed; 779 combined tests, local gRPC/CLI failure scenarios, two-probe live check and CI passed. |
| [#502](https://github.com/pinax-network/firehose-parquet/issues/502) | [#585](https://github.com/pinax-network/firehose-parquet/pull/585) | Merged as `0b38efc`; issue closed; 780 tests, all 6,179 instruction positions checked against raw Solana data, 15,832 prior-column rows unchanged and CI passed. |
| [#501](https://github.com/pinax-network/firehose-parquet/issues/501) | [#586](https://github.com/pinax-network/firehose-parquet/pull/586) | Merged as `a6a1712`; issue closed; 783 tests, independent raw classification and both vote-detail output modes checked on two slots, prior rows unchanged and CI passed. |
| [#475](https://github.com/pinax-network/firehose-parquet/issues/475) | [#587](https://github.com/pinax-network/firehose-parquet/pull/587) | Merged as `79793c3`; issue closed; 787 tests, real CLI readiness/buffer/cursor regression, independent review and CI passed. |
| [#499](https://github.com/pinax-network/firehose-parquet/issues/499) | [#588](https://github.com/pinax-network/firehose-parquet/pull/588) | Merged as `806bf60`; issue closed; 788 workspace tests, capture-auth regression, independent raw fixture review and CI passed. |
| [#511](https://github.com/pinax-network/firehose-parquet/issues/511) | [#589](https://github.com/pinax-network/firehose-parquet/pull/589) | Merged as `4b0f72f`; issue closed; 793 workspace tests, independent raw comparison of 3,904 outputs and 4,387 inputs, review and CI passed. |
| [#508](https://github.com/pinax-network/firehose-parquet/issues/508) | [#590](https://github.com/pinax-network/firehose-parquet/pull/590) | Merged as `21de6af`; issue closed; 796 tests, all old columns preserved across 24 live EOS rows, ten raw-verified database joins, independent review and CI passed. |
| [#486](https://github.com/pinax-network/firehose-parquet/issues/486) | [#592](https://github.com/pinax-network/firehose-parquet/pull/592) | Merged as `d5e1419`; issue closed; 855 tests, independent review, bounded finalized Ethereum coverage/resume comparison and CI passed. |
| [#503](https://github.com/pinax-network/firehose-parquet/issues/503) | [#593](https://github.com/pinax-network/firehose-parquet/pull/593) | Merged as `d417e0c` on 2026-09-25; issue closed; 857 tests, all 19,714 retained raw-sample rows compared, bounded offline release benchmarks, independent review and CI passed. |
| [#513](https://github.com/pinax-network/firehose-parquet/issues/513) | [#596](https://github.com/pinax-network/firehose-parquet/pull/596) | Merged as `2a3724e` on 2026-09-25; issue closed; 869 tests, independent arbitrary-size equivalence, bounded release benchmarks, review and CI passed. Original stopped-agent work remains preserved. |
| [#505](https://github.com/pinax-network/firehose-parquet/issues/505) | [#594](https://github.com/pinax-network/firehose-parquet/pull/594) | Merged as `b8d6834` on 2026-09-25; issue closed; 862 tests, one-slot decimal fee and six-blob live equality check, independent review and CI passed. |
| [#474](https://github.com/pinax-network/firehose-parquet/issues/474) | [#597](https://github.com/pinax-network/firehose-parquet/pull/597) | Merged as `1f2d252` on 2026-09-25; issue closed; 873 tests, real CLI fork-stream checks, executed query examples, review and CI passed. |
| [#565](https://github.com/pinax-network/firehose-parquet/issues/565) | [#598](https://github.com/pinax-network/firehose-parquet/pull/598) | Merged as `cd6e011` on 2026-09-25; issue closed; 877 tests, independent equivalence review, 13.4–14.4x conversion/append benchmarks and CI passed. |
| [#524](https://github.com/pinax-network/firehose-parquet/issues/524) | [#599](https://github.com/pinax-network/firehose-parquet/pull/599) | Merged as `f555898` on 2026-09-25; issue closure verified; 882 tests, projected-read/partition-check regressions, 1.8–2.7x measured local validation improvements and CI passed. |
| #468 prerequisite | [#591](https://github.com/pinax-network/firehose-parquet/pull/591) | Historical ownership/control foundation merged as `78ceb98`; 848 tests and CI passed. This stage alone did not provide all-table crash/replay protection. |
| [#468](https://github.com/pinax-network/firehose-parquet/issues/468) | [#600](https://github.com/pinax-network/firehose-parquet/pull/600) | Merged as `e4bd9cf` on 2026-09-25; issue closure verified. 995 workspace tests plus the CI capture example and final CI passed. Actual interrupted Writing rollback, 14-table/12,298-row live equality, completed-bound no-op and deleted-mirror repair passed. |
| [#510](https://github.com/pinax-network/firehose-parquet/issues/510) | [#601](https://github.com/pinax-network/firehose-parquet/pull/601) | Merged as `9379883` on 2026-09-25; issue closure verified; 1,003 workspace tests, independent Cosmos RPC-backed source/value comparison and CI passed. Firehose producer transport remains unqualified. |
| [#517](https://github.com/pinax-network/firehose-parquet/issues/517) | [#602](https://github.com/pinax-network/firehose-parquet/pull/602) | Merged as `b364681` on 2026-09-25; issue closure verified; 1,007 tests, guarded local receive-window benchmarks, independent review and CI passed. |
| [#530](https://github.com/pinax-network/firehose-parquet/issues/530) | [#603](https://github.com/pinax-network/firehose-parquet/pull/603) | Merged as `39d49f6` on 2026-09-25; issue closure verified; 1,010 tests, real local authenticated RPC/retry regressions, independent review and CI passed. |
| [#522](https://github.com/pinax-network/firehose-parquet/issues/522) | [#604](https://github.com/pinax-network/firehose-parquet/pull/604) | Merged as `6cae796` on 2026-09-25; issue closure verified. Guarded 36-run rollup comparisons, complete row/schema equality, bounded range-read/error tests, independent review and CI passed. |
| [#515](https://github.com/pinax-network/firehose-parquet/issues/515) | [#605](https://github.com/pinax-network/firehose-parquet/pull/605) | Merged as `137ab325` on 2026-09-25; issue closure verified. Retained and synthetic sizing evidence, successful-commit feedback and memory-trigger tests, 228 current-main integration checks, independent review and CI passed. |
| [#518](https://github.com/pinax-network/firehose-parquet/issues/518) | [#606](https://github.com/pinax-network/firehose-parquet/pull/606) | Merged as `9eddfd4` on 2026-09-25; issue closure verified. All-chain owned/borrowed equality, 75 guarded offline samples, 1,029 combined tests, independent review and CI passed. |
| [#550](https://github.com/pinax-network/firehose-parquet/issues/550), Solana slice | [#607](https://github.com/pinax-network/firehose-parquet/pull/607) | Merged as `955b8b2` on 2026-09-25; issue remains open. Parent outcome context added; all legacy values preserved in 300 table comparisons, 1,030 tests and CI passed. Other chains remain outstanding. |
| [#519](https://github.com/pinax-network/firehose-parquet/issues/519) | [#608](https://github.com/pinax-network/firehose-parquet/pull/608) | Merged as `11ac02c` on 2026-09-25; issue closure verified. Shared bounded lookup properties, 120 schema/row-preserving encodes and 3,510 exact-result lookup probes, independent review and CI passed. |
| [#523](https://github.com/pinax-network/firehose-parquet/issues/523) | [#609](https://github.com/pinax-network/firehose-parquet/pull/609) | Merged as `e4f990f` on 2026-09-25; issue closure verified. 1,054 workspace tests, 128 current-main integration tests, exact-result synthetic delete/read benchmarks, independent review and CI passed. |
| [#527](https://github.com/pinax-network/firehose-parquet/issues/527) | [#610](https://github.com/pinax-network/firehose-parquet/pull/610) | Merged as `270af16` on 2026-09-25; issue closure verified. All 80 AWS option definitions unchanged, 1,039 workspace tests, 44 final S3 integration checks, independent review and CI passed. |
| [#506](https://github.com/pinax-network/firehose-parquet/issues/506) | [#559](https://github.com/pinax-network/firehose-parquet/pull/559) | Merged as `c6db643` on 2026-09-25; issue closure verified. 1,073 workspace tests on integrated main `81f5b79`, a three-request NearData/archival-RPC capture of block 150000000, a 20-case independent offline comparison and PR CI passed. State changes remain with #507. |
| [#509](https://github.com/pinax-network/firehose-parquet/issues/509) | [#595](https://github.com/pinax-network/firehose-parquet/pull/595) | Merged as `5de4f16` on 2026-09-25; issue closure verified. 1,067 workspace tests, two bounded native RPC reads, 377,560 raw-source value checks, 136,720 legacy-value checks, physical schema checks, independent review and CI passed. RPC-backed mapper qualification only; Firehose transport and #550 remain separate. |
| [#525](https://github.com/pinax-network/firehose-parquet/issues/525) | [#611](https://github.com/pinax-network/firehose-parquet/pull/611) | Merged as `ab0888e` on 2026-09-25; issue closure verified. 1,058 workspace tests, final main integration, exact 13-part/5,050-row CLI replay and durable-state parity, independent review and CI passed. |

The #468 design PR accidentally triggered GitHub auto-closure through a negative
sentence containing a recognized closing phrase. On 2026-09-25 the PR text was
corrected and #468 was reopened; its open state was verified. The proposal and
single-file durability work did not satisfy full crash/replay recovery acceptance.
The later complete runtime in PR #600 satisfied that acceptance and closed #468.

## Adding a record

For each new fix, add an issue-specific record here and reference it in the PR.
Record the real test and live-data evidence, including unavailable qualification,
without credentials, private cursor values or generated datasets. Merge only
after review and current integration checks pass, then verify issue closure and
any external recovery condition, such as a dependency security rescan.

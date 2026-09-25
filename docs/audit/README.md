# Audit implementation records

These records document the diagnosis, implementation decisions, validation and
limits of each audit fix. The linked GitHub issues and PRs provide the current
lifecycle state; a local implementation or passing test alone is not closure.

- [#562: provider-scoped credentials](562-provider-credentials.md)
- [#467: stable startup destinations](467-endpoint-info.md)
- [#567: compatible dependency security refresh](dependency-security-refresh.md)
- [#469: durable cursor persistence](469-durable-cursor-saves.md)
- [#470: exact S3 cursor buckets](470-s3-cursor-buckets.md)
- [#498: EVM log indices and optional tables](498-evm-log-indices.md)
- [#568: Arrow/Parquet security and compatibility](568-arrow-parquet-security.md)
- [#572: final completion checkpoints](572-final-completion-checkpoint.md)
- [#506: NEAR qualification blocked by quota](506-near-qualification.md)
- [#504: Beacon live mapping qualification](504-beacon-qualification.md)

- [Backlog priorities and preserved work](backlog-roadmap.md)
- [#468: proposed crash/replay recovery design](468-crash-recovery-design.md)

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

For each next fix, add an issue-specific record here and reference it in the PR.
Record the real test and live-data evidence, including unavailable qualification,
without credentials, private cursor values or generated datasets. Merge only
after review and current integration checks pass, then verify issue closure and
any external recovery condition, such as a dependency security rescan.

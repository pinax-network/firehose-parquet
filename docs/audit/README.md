# Audit implementation records

These records document the diagnosis, implementation decisions, validation and
limits of each audit fix. The linked GitHub issues and PRs provide the current
lifecycle state; a local implementation or passing test alone is not closure.

- [#562: provider-scoped credentials](562-provider-credentials.md)
- [#467: stable startup destinations](467-endpoint-info.md)
- [#567: compatible dependency security refresh](dependency-security-refresh.md)
- [#469: durable cursor persistence](469-durable-cursor-saves.md)
- [#578: atomic local Parquet publication](578-atomic-local-parquet.md)
- [#470: exact S3 cursor buckets](470-s3-cursor-buckets.md)
- [#498: EVM log indices and optional tables](498-evm-log-indices.md)
- [#499: offline EVM golden-block regression](499-evm-golden-fixture.md)
- [#511: Bitcoin amounts and input metadata](511-bitcoin-values.md)
- [#508: Antelope database-operation joins](508-antelope-db-joins.md)
- [#568: Arrow/Parquet security and compatibility](568-arrow-parquet-security.md)
- [#572: final completion checkpoints](572-final-completion-checkpoint.md)
- [#506: NEAR qualification blocked by quota](506-near-qualification.md)
- [#504: Beacon live mapping qualification](504-beacon-qualification.md)
- [#485: partition probe reliability](485-partition-probe-reliability.md)
- [#486: exact finalized partition coverage and strict consumers](486-partition-index-design.md)
- [#500: stable Solana reward indices](500-solana-reward-index.md)
- [#501: conservative Solana vote classification](501-solana-vote-classification.md)
- [#502: explicit Solana instruction positions](502-solana-instruction-order.md)
- [#503: Binary Solana payloads and account-index lists](503-solana-binary-payloads.md)
- [#473: responsive shutdown](473-responsive-shutdown.md)
- [#474: append-only non-final streams and query limits](474-non-final-streams.md)
- [#477: single-partition writer contract](477-writer-partition-contract.md)
- [#475: bounded metrics and stream readiness](475-metrics-readiness.md)

- [#476: timestamp and streamed identity validation](476-timestamp-validation.md)

- [Backlog priorities and preserved work](backlog-roadmap.md)
- [#468: proposed crash/replay recovery design](468-crash-recovery-design.md)
- [#468: accepted transaction implementation plan](468-ingestion-transaction-plan.md)
- [#468: staged ownership and durable state implementation](468-stage1-ownership.md)
- [#468: conditional S3 ownership qualification](468-s3-ownership.md)
- [#468: single-attempt remote mutations](468-s3-mutation-attempts.md)
- [#468: verification artifact ownership](468-verify-ownership.md)

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
| #468 prerequisite | [#591](https://github.com/pinax-network/firehose-parquet/pull/591) | Ownership/control foundation merged as `78ceb98`; 848 tests and CI passed. Full ingestion crash/replay protection remains incomplete; issue #468 stays open. |

The #468 design PR accidentally triggered GitHub auto-closure through a negative
sentence containing a recognized closing phrase. On 2026-09-25 the PR text was
corrected and #468 was reopened; its open state was verified. The proposal and
single-file durability work do not satisfy full crash/replay recovery acceptance.

For each next fix, add an issue-specific record here and reference it in the PR.
Record the real test and live-data evidence, including unavailable qualification,
without credentials, private cursor values or generated datasets. Merge only
after review and current integration checks pass, then verify issue closure and
any external recovery condition, such as a dependency security rescan.

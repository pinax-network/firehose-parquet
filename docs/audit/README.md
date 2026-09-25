# Audit implementation records

These records document the diagnosis, implementation decisions, validation and
limits of each audit fix. The linked GitHub issues and PRs provide the current
lifecycle state; a local implementation or passing test alone is not closure.

- [#562: provider-scoped credentials](562-provider-credentials.md)
- [#467: stable startup destinations](467-endpoint-info.md)
- [#567: compatible dependency security refresh](dependency-security-refresh.md)

## Verified lifecycle outcomes

| Issue | PR | Verified outcome |
|---|---|---|
| [#562](https://github.com/pinax-network/firehose-parquet/issues/562) | [#566](https://github.com/pinax-network/firehose-parquet/pull/566) | Merged; issue closed; credential isolation tests and CI passed. |
| [#567](https://github.com/pinax-network/firehose-parquet/issues/567) | [#569](https://github.com/pinax-network/firehose-parquet/pull/569) | Merged as `f3e99f3` on 2026-09-25; issue closed; GitHub rescan confirmed ten alerts closed. Only Thrift alert 11 remains, tracked by [#568](https://github.com/pinax-network/firehose-parquet/issues/568). |

For each next fix, add an issue-specific record here and reference it in the PR.
Record the real test and live-data evidence, including unavailable qualification,
without credentials, private cursor values or generated datasets. Merge only
after review and current integration checks pass, then verify issue closure and
any external recovery condition, such as a dependency security rescan.

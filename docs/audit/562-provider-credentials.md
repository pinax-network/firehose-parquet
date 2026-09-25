# Provider-scoped Firehose credentials (#562)

## Diagnosis and implementation

Built-in aliases span Pinax and StreamingFast, but all configured legacy
credentials were previously attached to either provider. The fix selects
credentials from the actual resolved destination, after endpoint overrides.
Known HTTPS hosts on port 443 receive their provider's credentials. Legacy
`SUBSTREAMS_*` variables are Pinax-only fallbacks. Custom destinations require
explicit per-header environment-variable selectors.

Startup logs record host/provider/variable names without secret values. README,
environment examples, network guidance and release notes document migration.

## Validation and outcome

The complete workspace suite passed with 692 tests and three ignored benchmarks;
binary, shell completions and final GitHub CI passed. Regressions cover every
built-in host, explicit selectors, request metadata, custom/lookalike hosts,
userinfo, insecure or invalid ports, whitespace and log redaction.

The first CI run exposed a tracing callsite-cache race in the log-capture test.
The capture now runs in an isolated test process and verifies exactly one test
passed. Thirty repeated authentication-test runs and the complete suite passed
after that fix. Production credential selection did not require modification.

[PR #566](https://github.com/pinax-network/firehose-parquet/pull/566) merged as
`7cd615cf53dfbdafc48db156feaf89d4b9ee4543` on 2026-09-25.
[Issue #562](https://github.com/pinax-network/firehose-parquet/issues/562) closed
automatically; both states and the commit's presence on main were verified.

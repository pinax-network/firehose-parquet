# #527: shared AWS options and S3 construction

Issue: [#527](https://github.com/pinax-network/firehose-parquet/issues/527).
This refactor consolidates configuration after the ownership, retry and endpoint
fixes, preserving their distinct contracts. It performs no live S3 operation.

## Diagnosis

On main `11ac02c`, thirteen CLI declarations still repeated the same five AWS
options, with a fourteenth declaration in recovery. Twelve binary dispatch arms
manually reconstructed `AwsConfig`. Ingestion and maintenance already shared
endpoint interpretation after #470, but file inspection retained its own raw
`AmazonS3Builder::with_endpoint` path. A bucket-bound endpoint could therefore
produce a different destination for inspection.

The apparent client duplication also carried intentional differences. Ingestion
uses explicit credential validation at startup and a zero-retry mutation client;
maintenance reads allow unsigned access without an access key and retain default
read retries; maintenance mutations retain that credential policy but disable
retries. Inspection historically used the normal credential-provider chain.
Collapsing all those modes into one implicit default would change behavior.

## Implementation and compatibility

`AwsArgs` defines the five options once and flattens into common build arguments,
all maintenance/partition commands and recovery. `From<&AwsArgs> for AwsConfig`
replaces the twelve dispatch reconstructions. Recovery retains its historical
`AWS_ENDPOINT_URL` environment binding through an explicit command override;
other commands keep `AWS_ENDPOINT_URL_S3`. Command-line values still win over
environment values. Secret values remain hidden in help. Descriptions/headings
are consistently presented as AWS/S3 access options.

`AwsConfig` lives beside the shared builder in `s3.rs`, with the existing
`cli::AwsConfig` path re-exported. `From<&Config>` centralizes conversion from
resolved ingestion settings. There is one production `AmazonS3Builder::new`
call, shared by every data, cursor, maintenance and inspection path. Explicit
operation and credential policies retain the previous behavior:

| Caller | Missing access key | Transport retry policy |
|---|---|---|
| Ingestion data/cursor client | Existing provider-chain behavior; ingestion's explicit validation remains earlier | Zero retries |
| Maintenance read | Unsigned access | Existing read defaults |
| Maintenance mutation | Unsigned access | Zero retries |
| File inspection | Existing provider-chain behavior | Existing read defaults |

Every mode now applies the same bucket-bound AWS/Tigris endpoint checks and
service-endpoint path style. Inspection consequently honors virtual-hosted
endpoints and rejects a different requested bucket before use. Explicit cursor
URIs still choose their own bucket. No endpoint fallback, credential broadening,
new mutation retry, upload timeout change or recovery/ownership relaxation is
introduced.

The Rust CLI structs and enum variants now contain `aws: AwsArgs` instead of five
individual fields; downstream struct literals and matches must adapt. `Config`
and `AwsConfig` retain their field layout. CLI flag spellings, environment names,
value parsers, defaults, required/value-count behavior and actions remain the
same. Recovery's deliberately redacted `Debug` implementation is unchanged.

## Verification

A baseline snapshot from main `955b8b2` and the changed tree compares all **80 AWS
option definitions across 16 command paths** byte for byte, including recovery's
three paths. Main #608 changes compression options only and does not alter that
AWS baseline. The committed [option contract](527-aws-cli-contract.json) contains
only definitions and environment variable names, never environment values.
The reusable `snapshot_aws_cli` example regenerates the same representation:

```sh
cargo run -p firehose-parquet --example snapshot_aws_cli --locked > /tmp/aws-cli.json
cmp docs/audit/527-aws-cli-contract.json /tmp/aws-cli.json
```

A new parser regression sets synthetic environment values, exercises common
arguments, inspection and recovery, and verifies all five explicit CLI overrides,
resolved `AwsConfig` values, distinct recovery endpoint binding and help secrecy.
Existing credential validation/default tests continue to exercise build behavior.

Signed URL tests cover all four access policies over ten AWS, Tigris and custom
endpoint/bucket combinations, and reject seven mismatched bucket-bound endpoints
for every policy. They do not send those URLs to a provider. Existing native
loopback HTTP tests prove a single PUT/DELETE attempt after accepted-but-lost,
500 and timed-out responses; cursor publication retains the same failure and
metric contract. Read retries remain active. An added anonymous loopback read
checks both requests lack Authorization while still retrying the first lost
response. No ambient credential is needed for these fixtures.

The initial all-target workspace check and exact CLI comparison passed. On
main `11ac02c` plus the refactor, the new parser test and all 26 S3/ownership/control
focused tests passed, and the option snapshot matched again. Full workspace,
CI capture example, binary build, formatting and shell-completion checks are the
final merge gate. On `2c7dbd5` atop actual main `11ac02c`, **1,039 workspace
tests passed**, zero failed, nine ignored. The CI capture example passed one test
with one ignored. Binary build, formatting and Bash/Zsh/Fish completion checks
passed. Independent review found no blocker and independently rechecked the
80-option snapshot (SHA-256
`411026780c2145400d542f014d9fdb3fa5d39f9289cdd481f0e68e3c73a844b6`).
Fresh CI must pass on the submitted head before merge; issue closure is verified
only after publication and merge.

Actual main `e4f990f` (#609) is integrated. It adds the shared delete module and
new callers/tests; AWS policy source merged without conflicts. Fresh native
S3/read integration checks and PR CI gate the final combined head.

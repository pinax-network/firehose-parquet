# Dependency security refresh (2026-09-25)

## Scope and evidence

The GitHub Dependabot API reported 11 open alerts against `Cargo.lock` on
2026-09-25: four high, three medium, and four low. The starting revision was
`7cd615c` (the merge of credential-scoping PR #566). This change updates six
package/version selections to releases outside ten of those alert ranges.
It leaves one explicitly tracked Thrift alert unresolved; it does not claim
that GitHub has rescanned or closed any alert.

Only `Cargo.lock` changes dependency selection. The Arrow/Parquet major version,
workspace manifests, application source, and Rust toolchain remain unchanged.
The toolchain is pinned to Rust 1.93; validation used Rust 1.93.1. The updated
`time` crate requires Rust 1.88, within that pin.

## Advisory disposition

Alert numbers link to this repository's Dependabot records. The fixed column is
the first fixed release reported by the advisory for the applicable version
line; the selected column is the version in this change's lockfile.

| Alert | Severity | Advisory | Package | Before | First fixed | Selected / disposition |
|---|---|---|---|---|---|---|
| [12](https://github.com/pinax-network/firehose-parquet/security/dependabot/12) | High | [GHSA-4w2j-m93h-cj5j](https://github.com/advisories/GHSA-4w2j-m93h-cj5j) | quinn-proto | 0.11.13 | 0.11.15 | 0.11.15 |
| [11](https://github.com/pinax-network/firehose-parquet/security/dependabot/11) | Medium | [GHSA-2f9f-gq7v-9h6m / CVE-2026-43868](https://github.com/advisories/GHSA-2f9f-gq7v-9h6m) | thrift | 0.17.0 | 0.23.0 | **0.17.0; unresolved**, see below |
| [9](https://github.com/pinax-network/firehose-parquet/security/dependabot/9) | High | [GHSA-82j2-j2ch-gfr8](https://github.com/advisories/GHSA-82j2-j2ch-gfr8) | rustls-webpki | 0.103.9 | 0.103.13 | 0.103.13 |
| [8](https://github.com/pinax-network/firehose-parquet/security/dependabot/8) | Low | [GHSA-cq8v-f236-94qc](https://github.com/advisories/GHSA-cq8v-f236-94qc) | rand | 0.8.5 | 0.8.6 | 0.8.6 |
| [7](https://github.com/pinax-network/firehose-parquet/security/dependabot/7) | Low | [GHSA-cq8v-f236-94qc](https://github.com/advisories/GHSA-cq8v-f236-94qc) | rand | 0.9.2 | 0.9.3 | 0.9.3 |
| [6](https://github.com/pinax-network/firehose-parquet/security/dependabot/6) | Low | [GHSA-xgp8-3hg3-c2mh](https://github.com/advisories/GHSA-xgp8-3hg3-c2mh) | rustls-webpki | 0.103.9 | 0.103.12 | 0.103.13 |
| [5](https://github.com/pinax-network/firehose-parquet/security/dependabot/5) | Low | [GHSA-965h-392x-2mh5](https://github.com/advisories/GHSA-965h-392x-2mh5) | rustls-webpki | 0.103.9 | 0.103.12 | 0.103.13 |
| [4](https://github.com/pinax-network/firehose-parquet/security/dependabot/4) | Medium | [GHSA-pwjx-qhcg-rvj4](https://github.com/advisories/GHSA-pwjx-qhcg-rvj4) | rustls-webpki | 0.103.9 | 0.103.10 | 0.103.13 |
| [3](https://github.com/pinax-network/firehose-parquet/security/dependabot/3) | High | [GHSA-vvp9-7p8x-rfvv / CVE-2026-32829](https://github.com/advisories/GHSA-vvp9-7p8x-rfvv) | lz4_flex | 0.12.0 | 0.12.1 | 0.12.1 |
| [2](https://github.com/pinax-network/firehose-parquet/security/dependabot/2) | High | [GHSA-6xvm-j4wr-6v98 / CVE-2026-31812](https://github.com/advisories/GHSA-6xvm-j4wr-6v98) | quinn-proto | 0.11.13 | 0.11.14 | 0.11.15 |
| [1](https://github.com/pinax-network/firehose-parquet/security/dependabot/1) | Medium | [GHSA-r6v5-fh4h-64xc / CVE-2026-25727](https://github.com/advisories/GHSA-r6v5-fh4h-64xc) | time | 0.3.36 | 0.3.47 | 0.3.47 |

The `time` update also requires lockfile changes to `time-core` 0.1.8,
`time-macros` 0.2.27, `deranged` 0.5.8 and `num-conv` 0.2.2. No other package
versions were changed.

## Reachability assessment

- `lz4_flex` is active through Parquet 58. Its block-decompression API is used
  when maintenance/read commands open LZ4-compressed pages, even though the CLI
  does not offer LZ4 output compression. Parquet's inspected raw/Hadoop codecs
  initialize destination buffers with zeroes; information disclosure was not
  demonstrated. The dependency is updated rather than relying on that detail.
- `rustls-webpki` is active through tonic and reqwest/object_store TLS. Alert 9
  requires opt-in CRL processing with attacker-influenced bytes. No CRL setup
  was found in the application or its tonic/object_store configuration path.
  That limits this particular panic scenario, not the need to update the
  certificate-verification dependency or resolve the other webpki advisories.
- `quinn-proto` is present in the lockfile through optional reqwest HTTP/3
  dependencies, but `cargo tree --workspace --target all -e features -i
  quinn-proto` reported no active reverse dependency. The current workspace
  features do not enable QUIC. Both locked QUIC advisories are still fixed so
  future feature selection does not inherit these versions.
- The `time` advisory concerns RFC 2822 parsing; no use of that format was found
  in application source. The `rand` advisory requires a custom logger that
  re-enters the thread RNG under specific feature/logging conditions. This
  refresh does not claim either scenario was reproduced in the application.

## Remaining Thrift alert and upstream migration

`parquet 58.0.0` requires `thrift ^0.17`. The latest inspected 58.x release,
[58.4.0](https://github.com/apache/arrow-rs/blob/58.4.0/parquet/Cargo.toml), still
requires `^0.17`, so a compatible lockfile update cannot select the fixed 0.23.0.
Adding a direct `thrift = "0.23"` dependency would retain a separate vulnerable
0.17 copy rather than satisfy Parquet's requirement.

[Upstream PR #10208](https://github.com/apache/arrow-rs/pull/10208) proposed the
0.23 update but was closed without merging. Its author's reported tests are
not a released 58.x fix. [Upstream PR #9962](https://github.com/apache/arrow-rs/pull/9962)
removed the deprecated `parquet::format` API and the Apache Thrift dependency
for 59.0.0. This workspace has no direct uses of `parquet::format`,
`parquet::thrift`, or the `thrift` crate; inspected Parquet 58 metadata/page
readers use its separate `parquet_thrift` implementation. That distinction
limits the evidence for reachability of the Apache Thrift advisory; the locked
package remains affected and the alert remains unresolved.

An upstream upgrade is viable as follow-up work, but it should include a
coordinated Arrow/Parquet migration and file/schema regression validation.
Prefer evaluating **60.0.0 or later**: the separate metadata-list allocation
fix [#10979](https://github.com/apache/arrow-rs/pull/10979) landed for 60.0.0,
while the requested 59.x backport [#11186](https://github.com/apache/arrow-rs/issues/11186)
was still open when checked. Merely removing the Thrift package by moving to
59.x would not establish safe handling of malformed Parquet metadata. The
60.0.0 workspace declares Rust 1.88, compatible with this repository's 1.93
pin, but application compatibility and output behavior still need testing.

This focused refresh neither vendors a modified Parquet dependency nor forces
an untested Arrow/Parquet major upgrade. Keep alert 11 open until an actual fix
or dependency removal is merged and verified.

## Reproduction and validation

The initial alert inventory was obtained with:

```sh
gh api repos/pinax-network/firehose-parquet/dependabot/alerts \
  --paginate -X GET -F state=open
```

The minimal selections were applied sequentially:

```sh
cargo update -p lz4_flex --precise 0.12.1
cargo update -p rustls-webpki --precise 0.103.13
cargo update -p quinn-proto --precise 0.11.15
cargo update -p time --precise 0.3.47
cargo update -p rand@0.8.5 --precise 0.8.6
cargo update -p rand@0.9.2 --precise 0.9.3
```

Completed on the updated lockfile with Rust 1.93.1:

- `cargo test --workspace --locked -j4`: **692 passed, zero failed, three
  existing benchmarks ignored** (110 + 181 + 398 + 3). This includes the
  workspace's Arrow/Parquet schema round trips, maintenance, cursor and
  provider-scoping tests.
- `cargo build --bin fireparq --locked -j4`: passed.
- `fireparq completions bash`, `zsh`, and `fish`: passed.
- `cargo fmt --all --check` and `git diff --check`: passed.
- `cargo tree --locked --offline` confirmed the updated active LZ4/webpki
  dependencies, the remaining Thrift dependency, and no active Quinn path
  with the workspace's selected features across targets.

Builds used `CARGO_PROFILE_DEV_DEBUG=0`, `CARGO_PROFILE_TEST_DEBUG=0`, and four
build jobs. The unchanged `transactions_processed` assignment in
`blocks/src/bin/main.rs` still produces an unused-assignment warning.

This is a comparison with the refreshed GitHub alert inventory, not a claim
of a complete vulnerability audit: `cargo-audit` is not installed in the local
environment. GitHub alert closure must be checked after merge and its next
security scan.

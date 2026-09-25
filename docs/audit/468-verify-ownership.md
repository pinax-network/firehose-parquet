# Common ownership for verification artifacts

## Scope

Verification that runs root checks can insert missing registry rows even when
`--update-registry` is absent. Those runs, and any run that writes a local or
published JSON report, now acquire common ownership of the input plus every
registry/report destination before the authoritative scan. The guard remains
held through registry publication and both report destinations.

Protocol-only verification with no artifact output retains its read-only
behavior. It does not acquire ownership or require S3 write permissions. As
before, that read-only path does not promise a snapshot of a concurrently
changing dataset. An unused explicit registry option alone does not turn it
into a mutating operation.

This is stage-1 mutator coverage. It does not make registry/report publication
a multi-artifact transaction or complete the ingestion replay journal; #468
remains outstanding.

## Discovery and publication

1. Validate the hash option and resolve the input path.
2. Perform a lightweight path listing to locate the default chain root. No
   rows are hashed and no registry snapshot is trusted at this stage.
3. Collect the source, default or explicit registry, local JSON report and
   default or explicit published-report scopes in one plan. `--report-json`
   remains a local filesystem path, even if its literal spelling resembles
   an S3 URI.
4. Acquire one `DatasetOwnership`. Its existing scope reduction handles
   nested local paths and shared S3 buckets; verification never recursively
   acquires another common guard.
5. Re-list and scan the source under ownership. Reject a changed chain root
   before publishing artifacts. Read registry/cursor information only from
   this guarded stage.
6. Revalidate local path identities immediately before registry publication,
   local JSON report publication, and published-report publication. This uses
   the shared no-create guard API and its nested-symlink checks.
7. Release explicitly after complete success. Errors retain remote `Owned`
   records and mark their mutation state uncertain. Local OS locks are released
   when the operation ends. A verification finding such as a root mismatch can
   still be a completed operation with a successfully written diagnostic report.

The existing local registry file lock remains for compatibility with older
verify callers, inside the common guard. Local JSON reports now use the same
atomic-file helper as published reports. Their parent-directory requirements
are preserved. A later artifact failure may follow a successful registry write;
there is no claim that an error rolls back an already published artifact.

## S3 behavior and compatibility

Registry and report writes use `AwsConfig::build_s3_client_for_mutation`, which
disables transport retries. Registry writes make one conditional Create or
versioned Update. Missing/unusable versions, conditional conflicts and
unsupported conditional operations return errors. The old retry loop and
unconditional overwrite fallback are removed. Reports make one PUT with no
application retry. Separate read clients retain their normal retry policy.

This intentionally removes automatic reconciliation of concurrent registry
changes: another owner or an external update causes a visible failure. Missing
conditional-write support no longer silently permits lost registry updates.
After an ambiguous response, the accepted artifact may exist while the owner
remains `Owned`; a later read does not establish remote-request quiescence or
authorize release. The recovery requirements in `468-s3-ownership.md` apply.

The public `verify_parquet` signature and report/Parquet schemas are unchanged.
Mutating verification now requires ownership permissions on source and artifact
locations and fails closed on filesystems/providers unsupported by the shared
guard. Read-only protocol verification remains available without those
mutation permissions. Runtime default ingestion protection is not enabled by
this change.

## Validation

The focused verification suite passed **49 tests** on the combined ownership
foundation `a6714e6`, single-attempt mutation client change and no-create guard
follow-up `737ef8b` (local equivalent `a4b4434`). Existing root/protocol fixtures,
network registries, fail-fast behavior, local registry compatibility and
prefetch behavior remain covered.

Seven new ownership regressions cover:

- Conflicts on source, default/explicit registry and both external report
  destinations prevent every artifact publication.
- All scopes remain held while a real registry commit is paused, then through
  successful report writes, and are released afterward.
- Files added after the planning listing are included by the authoritative
  scan; a changed planned layout refuses publication.
- Replacing a held report directory's inode is detected before the first
  registry or report publication.
- Protocol-only/no-output verification remains read-only; adding a report
  requires ownership.
- Stateful S3 registry/report failures issue one artifact PUT and retain the
  exact owner. Both unsupported writes and accepted-but-lost responses are
  exercised through the production operation-completion wrapper; successful
  publication releases ownership. The accepted artifact remains visible in
  the lost-response case.

The existing store regression now verifies stale Create/Update and missing
version refusal, without retry or overwrite. Actual AmazonS3 transport timeout,
lost-response and server-error behavior is separately exercised by the tests
documented in `468-s3-mutation-attempts.md`. This verification change uses a
stateful fake for owner/artifact fault injection, not a production S3 bucket.

Validation used the whole-process Cargo lock, shared Arrow 60 target and four
jobs. `cargo fmt --all`, `cargo test -p firehose-parquet verify:: --locked -j4`
and `git diff --check` passed. Log: `/tmp/fireparq-verify-ownership-focused.log`.

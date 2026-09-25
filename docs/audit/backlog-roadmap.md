# Audit priorities and preserved work

Snapshot reviewed on 2026-09-25. All 45 initially open issue bodies and both
pending PRs were read. Priority reflects risks in the current implementation;
issue labels and green CI alone are not acceptance evidence. See the
[audit record index](README.md) for completed changes and their verified outcomes.

## Delivery order

1. Credential destination isolation (#562), compatible security updates (#567),
   stable startup identity (#467), exact S3 cursor destinations (#470), and
   fatal persistence failures (#469). These affect where credentials and durable
   state go and whether ingestion can safely continue. PRs #566, #569, #570, #571 and #573
   are merged; their linked issues are closed.
2. The Arrow/Parquet security migration (#568, PR #577) and separately
   reproduced missing final checkpoint (#572, PR #576) are now merged and
   closed. Old-file compatibility, malformed-footer and live-output checks
   passed; GitHub reports no open dependency alerts after rescan.
3. Crash/replay publication (#468). The [design](468-crash-recovery-design.md)
   explains why deterministic range filenames alone cannot prevent duplicates
   when replay chooses different timer or size boundaries. The writer split path has been removed (#477, PR #581); establish an
   all-table durable frontier next. The common ownership/control foundation is
   merged in PR #591; it does not yet provide protected ingestion.
   Complete this before ingestion concurrency (#516).
4. Shutdown recovery (#473, PR #579) and malformed identity/timestamp handling
   (#476, PR #584) are merged. Probe (#485) work was recovered into PR #583,
   passed 779 combined tests with the timestamp fix and CI, and merged as
   `3cf984b`; its issue is closed. Correct partition-index completeness/non-monotonic timestamps (#486)
   is merged as PR #592 after 855 tests, bounded finalized-source verification and CI. Health/metrics semantics (#475, PR #587) are merged
   after 787 tests, real CLI regression and CI.
5. The initial EVM golden fixture (#499, PR #588) is merged and runs offline
   in CI. Continue schema fixes and add field-specific fixtures as needed. Prefer
   correctness and measured performance improvements before broad refactors.

## Existing PRs and remaining qualification

| PR / issue | Review finding | Still needed before closure |
|---|---|---|
| #559 / #506, NEAR joins and events | No actionable code defect found at `8056a07`. Same-block `tx_hash` is intentional and documented. | Current-main validation and a raw/output live comparison for action/log counts, order, receipt lineage and exact event strings. One fresh request on 2026-09-25 was again rejected by StreamingFast egress quota. See the [qualification record](506-near-qualification.md); no further requests were made. |
| #560 / #504, Beacon coverage | No actionable code defect found at `26d96f9`. Existing live comparisons cover withdrawals, committee bits, graffiti and all three request types. | The [targeted live BLS/slashing comparisons](504-beacon-qualification.md) now pass, including the integrated Arrow/Parquet 60 build and 730-test suite. CI passed; merged as `c88abcc` and issue closure verified. Capella BLS changes remain absent from the upstream protobuf. |

The NEAR historical CI predates newer shared-encoding and durability changes.
Its issue remains open until current integration and live qualification pass.
Beacon acceptance was checked against its integrated head before merge.

## Previous agent work

The original checkout contains no tracked edits; its untracked `.claude/`
directory contains the prior worktrees. They were inspected and preserved.
The previous agent process was still present during inspection; a stopped or
idle UI does not prove a worktree can be safely reused.

| Issue | Existing branch | Preserved state at inspection |
|---|---|---|
| #473 | `audit/473-shutdown-responsive` | Six modified files; no branch-only commit. |
| #485 | `audit/485-partitions-probing` | One unpublished WIP commit plus two modified files. |
| #513 | `audit/513-bigint-decimal-fast-path` | Modified Rust files and untracked decimal module; worktree locked to the prior agent. |
| #522 | `audit/522-rollup-streaming` | Modified merge/journal code; the branch name does not establish a finished rollup implementation. |
| #500, #508 | `audit/500-solana-reward-index`, `audit/508-antelope-db-ops` | Clean placeholders with no implementation changes found. |

Copy or patch preserved work into a new isolated checkout before resuming it.
Recheck its source state first, retain attribution, review the complete diff,
and test against current main. Never reset or discard the previous worktree.

## Remaining backlog groups

- Schema/data correctness: #505 (Beacon numeric/blob/null semantics), #507,
  #509 and #510 (NEAR, Tron and Cosmos). Solana native payload types (#503) merged
  as `d417e0c` in PR #593 after 857 tests, 19,714 raw-sample row comparisons, release
  benchmarks and CI; issue closure is verified. Antelope #508 is merged in
  PR #590 after raw join checks, byte-for-byte legacy-column comparison and CI. Bitcoin #511 is merged
  in PR #589 after raw integer/input qualification and CI. #550 needs
  explicit failed-effect semantics per chain and live fixtures. #498 documents
  EVM indices and upstream-empty tables. Solana reward indices (#500, PR #582)
  are now merged and closed after regression and live comparison. Vote
  classification (#501, PR #586) and explicit instruction positions (#502,
  PR #585) also passed raw-data comparisons and are merged with issues closed.
- Operational contracts: #474 covers reversible stream options/output semantics;
  #475 metrics/readiness is merged. The #477 writer simplification and
  #476 malformed-metadata handling are merged; the #468 journal is in progress.
- Performance: #513 was recovered into an isolated worktree, with bounded exhaustive
  and sampled equivalence tests, a reproducible conversion benchmark and 869 passing
  integrated tests; PR/CI pending.
  The original locked work is preserved. Recover #522 before duplicating that work. Measure
  #565, #515, #520 and #524 on representative data; #503's native-type work and
  bounded release benchmark are complete. #516 depends on durable
  commit ordering; #517-#519 and #523 need the specific throughput, memory,
  file-size or lookup evidence requested by their issues.
- Structure: #525-#530 follow correctness work. Scope must be refreshed against
  main; for example, #530's original auth-duplication description is partly
  obsolete after the shared provider-scoped auth helper.

For each issue, document diagnosis, selected behavior, reproduction/regression,
integration results, qualification limits and verified closure. Never equate a
proposal, local WIP, benchmark assertion or unmerged PR with completion.

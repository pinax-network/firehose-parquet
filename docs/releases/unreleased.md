# firehose-parquet: unreleased

Changes merged since [v1.1.2](v1.1.2.md). Fold this file into
`docs/releases/vX.Y.Z.md` when the next release is cut, then reset it to this
template.

Add one entry per change under the matching heading, with its issue and PR
(`#N` refs are fine). Say what changed for operators or data consumers, what
they must do (for example rebuild into a new output root), and link the record
in `docs/audit/` or elsewhere when there is one. Remove headings that stay
empty when the release is cut.

## Breaking changes

## New features

## Fixes

## Performance

## Internal

- **A local ownership guard unlocks its directories when it is dropped**, before
  closing them (#706). A process spawned while the guard was held keeps a copy
  of each locked descriptor until it execs, and a `flock` lock belongs to the
  open file description that the copy shares. So the lock could outlive the
  guard, and a new acquisition right after failed with "lock acquisition
  failed because the operation would block". In the test suite, where tests
  run child processes beside each other, that made
  `nested_symlinks_fail_before_mutation_but_explicit_root_aliases_work` fail
  now and then. `fireparq` itself starts no child processes, so it wasn't
  affected. `a_dropped_guard_releases_its_locks_while_inherited_descriptors_live`
  holds a copy of the descriptors and acquires again.

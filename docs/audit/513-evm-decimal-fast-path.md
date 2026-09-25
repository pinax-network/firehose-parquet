# EVM decimal conversion (#513)

## Recovering the stopped work

The prior agent left modified EVM mapper/module files and an untracked
`decimal.rs` in the locked `audit/513-bigint-decimal-fast-path` worktree at
`5e347d0c65832c0d1412b574370b2e212d3bbe47`. On 2026-09-25, that work was inspected,
then its exact tracked patch and untracked module were copied into a fresh
`codex/evm-decimal-fast-path-513` worktree on current main. The patch applied
without conflicts. The original worktree, lock and process were not changed.
SHA-256 before and after the copy agreed for all three original files:

| Original file | SHA-256 |
|---|---|
| mapper.rs | `a53ebbf365668a8fc5474adf0928c7cde8e27f910b5658830b6e9dc9df2dd32a` |
| mod.rs | `b8f5d9b30b1e5fa9921e0311f0456a72d26826043ae502859f79f46e88291119` |
| decimal.rs | `e4bdf1b92968e11f277c142c2064703be5cdebacc6060fe9fc88a71e12851712` |

The tracked recovery patch hash is
`5e56ffdbcf2fed02fc7bd548e6c2220591776a1c6c12a9459fbabb4f4a4f65b8`.
The original implementation and tests are retained with added independent-oracle,
builder-state and fair comparative benchmark coverage. Attribution belongs to
the preserved prior agent work; this is its recovery and qualification.

## Implementation and unchanged contract

The old hand-written BigUint converted each incoming byte by multiplying a
vector of decimal digits, repeatedly inserting/removing at the front, then
allocating the final string. It also copied source bytes into an intermediate
value. That ran for transaction values, gas prices/caps, header fees/difficulty,
call values, balance changes and authorization chain IDs.

The recovered formatter trims leading zero bytes, handles values up to 16 bytes
through u128, and divides larger values as u64 limbs by `10^19`. Digit pairs are
written backwards into a stack buffer for values up to 32 significant bytes
(78 decimal digits), then appended directly into Arrow StringBuilder. No
intermediate heap string/vector is needed on that path; the Arrow builder itself
still allocates as required. Larger inputs retain an arbitrary-length heap
fallback, preserving accepted input semantics instead of truncating to 256 bits.
Header values are borrowed instead of cloning protobuf BigInts.

Every input is unsigned big-endian, just as before. Empty/all-zero values output
`"0"`; optional absent fields remain null, while present empty BigInts remain
zero. Existing schemas, strings, row order, encodings and verification roots are
unchanged. No generic decoder or non-EVM mapper uses the private new helper.

## Equivalence checks

The legacy algorithm remains only as a test/benchmark reference. Tests cover:

- Every possible one-byte and two-byte input (exhaustive for those domains).
- 2,000 deterministic random values at every length from zero through 40 bytes.
- Leading zero prefixes, maximum values, every relevant byte-length transition,
  powers of ten and their immediate neighbors across chunk boundaries.
- Builder append beside existing values and nulls, a 256-byte fallback value,
  and reuse after flushing.
- An independent `num-bigint` oracle over deterministic inputs at every length
  from zero through 256 bytes. This dependency already exists through the merged
  Beacon work; no package or dependency change is introduced by this fix.

These bounded exhaustive and sampled checks do not claim exhaustive enumeration
of the full 256-bit or arbitrary-length domain. The retained Ethereum golden
block test also passed without any expectation changes: exact table counts and
reviewed field values across eight mapper configurations, including null/empty
and decimal cases. It runs on the original raw payload from #499 and needs no
new live request. The first focused command used a module filter that did not
select this integration test; the explicit `--test evm_golden` run then executed
and passed it. Integrated main `b8d6834` (including Beacon #505); the full locked
workspace passed **869 tests**, with **6 intentional skips** (the existing five
child/helper cases plus this manual benchmark). The capture-auth example passed
its separate regression. Formatting and workspace build passed, and independent
review found no correctness blocker. No expectations in the golden fixture were
changed. The only build warning is the inherited final-backfill
`transactions_processed` assignment.

## Reproducible conversion benchmark

Measured on Apple M1 Max, macOS arm64, Rust 1.93.1 release profile. Each case has
one warmup per implementation and seven samples of 100,000 iterations, alternating
old/new order. **Both** timings include conversion and append into equivalently
preallocated Arrow builders. Builder construction and final flush are outside
the timer. The reference reproduces the legacy decimal arithmetic but omits the
old wrapper's source-byte `to_vec()` copy, so its baseline is slightly cheaper
than the former full production helper; the comparison is conservative. Cargo compilation and other task builds were excluded using the
shared whole-process lock. The measured EVM code is commit `6aeaafa` (the run used
the identical pre-commit source tree).

| Input | Legacy median ns/value | Direct append median ns/value | Speedup |
|---|---:|---:|---:|
| 4-byte gas price | 182.511 | 16.946 | 10.8× |
| 10-byte balance | 559.280 | 28.175 | 19.9× |
| 16-byte value | 1,340.257 | 55.203 | 24.3× |
| 20-byte value | 1,991.633 | 74.232 | 26.8× |
| 32-byte maximum | 4,706.099 | 183.313 | 25.7× |

Exact input hex and all samples are in [513-benchmark.json](513-benchmark.json).
This is a conversion microbenchmark, not a whole-block ingestion throughput,
network/storage speed, RSS, or universal hardware claim.

```sh
cargo test --release -p blocks decimal::tests::bench_decimal_conversion \
  --locked -- --ignored --nocapture
```

The benchmark is intentionally ignored in normal CI; deterministic equivalence
checks run normally. No production storage or live source calls are part of this
change. Full integration, independent review, CI and actual GitHub lifecycle are
recorded separately from the local performance result.

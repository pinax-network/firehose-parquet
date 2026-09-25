# Fixed-width Base58 encoding (#565)

## Diagnosis and dependency decision

The shared Base58 writer used `bs58` for every input. Solana public keys and
signatures retain Base58 after #503 moved opaque payloads to Binary, so the
32/64-byte cases still pay for a general byte-at-a-time conversion on each value.

The evaluated [five8 1.0.0 release](https://docs.rs/crate/five8/1.0.0) is small:
MIT, `no_std`, no build script, and one normal dependency (`five8_core`). Both
packages already exist transitively in the lockfile. Its release source is
`311279fb81461df36e730458a0463a6a322042d7`; upstream main was
`6da4b702a83ade02076ca3e349ab4222a2c5637a` when inspected on 2026-09-25, with the
most recent push in July 2025. It has differential tests, but includes unsafe
scalar indexing and a compile-time AVX2 path. The latter forms
`out_ptr.offset(-skip)` before the start of a caller's ordinary standalone output
array, before masked stores in both encoders. [The pinned implementation](https://github.com/kevinheavey/five8/blob/311279fb81461df36e730458a0463a6a322042d7/crates/five8/src/encode.rs)
and [Rust's pointer-offset safety contract](https://doc.rust-lang.org/std/primitive.pointer.html#method.offset)
show why masked-out writes do not satisfy the in-allocation arithmetic requirement.
This is a source-review finding, not a reproduced hardware crash or exploit.
Enabling AVX2 through target flags can select that implementation automatically.

We therefore add neither a direct dependency nor an output-padding workaround.
Existing transitive dependencies are unchanged; this work does not audit or fix
all of their other callers. A short safe integer specialization is maintained in
this repository instead. It uses no copied lookup tables, SIMD, raw pointers,
unchecked indexing or new package. `#![forbid(unsafe_code)]` protects its module.
The tradeoff is ownership of this small conversion and its regression tests.

## Algorithm and compatibility

Exactly 32 and 64 bytes dispatch to fixed arrays of eight or sixteen big-endian
u32 limbs. Long division by `58^5` emits five Base58 characters per pass into a
90-byte stack buffer. This outperformed the initially evaluated u64/`58^10`
prototype on the measured machine; the u128-wide divisions in that prototype
were more expensive. No platform-specific path is used.

The arithmetic bounds are explicit: remainder is less than `58^5 < 2^30`, so
`(remainder << 32) | limb` is less than `2^62` and also less than `58^5 * 2^32`.
Thus the quotient fits u32 and the next remainder preserves the invariant.
A 512-bit value needs at most 88 Base58 digits, or eighteen five-digit chunks,
so the 90-byte buffer is sufficient. Numerical padding is removed, then exactly
one `1` is appended per leading zero input byte, including entirely zero inputs.
All buffer indexing remains checked. Endianness is explicit and portable.

All other lengths continue through `bs58`, including the 25-byte checksummed
Tron address payload. The decoder, alphabet, public APIs, identifier encodings,
Arrow schemas and output strings are unchanged. Scalar/list Arrow columns still
reuse their existing scratch vectors. Conversion needs no additional heap
allocation beyond those output buffers. Existing output prefixes are preserved.
There is no data migration.

## Equivalence and integration evidence

The unchanged `bs58` implementation is the independent differential reference:

- All 65,536 low-two-byte values at each supported width (131,072 inputs).
- Every byte value at each input position (24,576 inputs), all-max suffixes and
  every possible leading-zero prefix, including all-zero values.
- 8,192 deterministic random values per width, varied leading zeros, and decoder
  round trips. Powers of 58 and their immediate neighbors exercise digit and
  five-digit chunk boundaries.
- Prefix-preserving dispatch at all lengths 0–80 plus 127, 128, 255, 256 and
  1,024, and independent Tron checksum/output comparisons.
- Existing scalar/list Arrow comparisons and reuse/flush tests run on mixed
  lengths and every encoding without changed expectations.

These are bounded exhaustive subdomains and sampled large domains, not an
exhaustive enumeration of 256/512-bit inputs. At code commit `0c0db81`, all
26 focused encoding tests passed; the two manual benchmarks were ignored.
Full workspace and release benchmark results are recorded below after the final
integration run. No new public Firehose call or production write is needed for
this byte-identical conversion change.

## Reproducible benchmark

The ignored `encode::tests::bench_fixed_width_base58` test compares the actual
production dispatcher with its previous `bs58::encode(...).onto(...)` path.
Both encode into reusable scratch and append to identically preallocated Arrow
StringBuilders. Builder construction and final flush are outside the timer.
Each length uses 4,096 deterministic inputs, including varied zero prefixes;
both paths warm up for 10,000 values, then take seven alternating-order samples
of 100,000 values. The 25-byte case measures the retained fallback as a control.
The whole-command Cargo lock excludes concurrent task compilation from timings.

On Apple M1 Max, macOS arm64, Rust 1.93.1 release profile, code `0c0db81`:

| Input | bs58 median ns/value | Production median ns/value | Speedup |
|---|---:|---:|---:|
| 25-byte fallback control | 769.860 | 770.823 | 1.00× |
| 32-byte key | 1,165.927 | 86.906 | 13.42× |
| 64-byte signature | 4,421.259 | 306.670 | 14.42× |

All samples and measurement metadata are in [565-benchmark.json](565-benchmark.json).
The fallback differs by less than 0.2%, within run-to-run measurement noise.

```sh
cargo test --release -p firehose-parquet --lib \
  encode::tests::bench_fixed_width_base58 --locked -- --ignored --nocapture
```

Results apply to conversion plus Arrow append on the stated hardware. They do
not claim end-to-end ingestion throughput, storage/network speed, lower RSS or
equal gains on every CPU.

Real `POST https://api.hyperliquid.xyz/info` responses, recorded on 2026-10-08
between 16:45 and 16:51 UTC, then trimmed so that tests stay small. Trimming only
removes entries; it never edits a value.

| File | Request | Trimmed to |
|---|---|---|
| `spotMeta.json` | `spotMeta` | tokens 0, 1, 2, 40 (`fullName`, no EVM contract), 150, 235, 268, 360, 437 and one USDH-quoted base; the pairs between them |
| `perpDexs.json` | `perpDexs` | the default dex (`null`), `xyz` and `flx`; list fields cut to two items |
| `allPerpMetas.json` | `allPerpMetas` | the first three dexes; 5, 3 and 2 assets (MATIC is delisted); one margin table each |
| `outcomeMeta_t0.json`, `outcomeMeta_t1.json` | `outcomeMeta`, about five minutes apart | outcomes 1472 and 1473 (question 198), and the short-lived 9973–9975 (t0 only, settled by t1) and 9976–9977 (t1 only); question 198 with only the kept outcome ids; one deployer |
| `settledOutcome_<id>.json` | `settledOutcome` | whole responses: `null` for live 1473 and not-yet-settled 9976; 0 (legacy, USDH), 9973–9975, and 7768 (with its settled question 373) and 9174 recorded at 11:23 UTC; 8976 (a binary price outcome without a question, the one the HyperCore fixture lake settles) recorded on 2026-10-14 at 18:23 UTC |
| `tokenDetails_<name>.json` | `tokenDetails` | whole responses for HSEI, USDC (no deployer) and ASI |
| `fundingHistory_<coin>.json` | `fundingHistory` from 2026-01-01 | the first six hours of BTC, `xyz:XYZ100` and the delisted MATIC |

`src/testing.rs` (`FakeInfo`) answers from them: in this crate's tests, and,
through `blocks/tests/common/info.rs`, as a loopback `/info` for the real
binary.

## `golden-vectors.json`

Generated on 2026-10-11 by running k8s-parquet PR #82's Python job (commit
6a4e4d3, deltalake 1.6.6, pyarrow 25.0.1) on these fixtures with
`observed_at = 2026-10-08T16:50:00Z`. The generator was a scratch tool and is
not in this repository. The file is the parity oracle: the Delta schema and
properties of every table, 76 rows (every typed column, `payload_json` and
`payload_sha256`), canonical JSON, request bodies and subjects, and the value
parsers' and CRC-32 hours' vectors. `tests/golden.rs` checks them.

One vector records the generator, not the job: the `parse_decimal` result of
`9999999999999999999999999999.9999999999` was scaled with Python's default
28-digit decimal context, so its `unscaled_i128` is `10^38` rounded from the
job's exact `10^38 - 1`. `tests/golden.rs` checks that value at 28 significant
digits.

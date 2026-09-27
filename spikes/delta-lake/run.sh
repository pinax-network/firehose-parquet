#!/usr/bin/env bash
# Delta Lake spike for #643 (docs/design/delta-lake.md, "Spike"). Runs every check:
#
#   1. cargo tests on local disk and an in-memory store;
#   2. the same tests on a loopback S3 server (moto, 127.0.0.1 only) with conditional puts;
#   3. OPTIMIZE / VACUUM / checkpoint / log cleanup with the `deltalake` Python package
#      while the writer appends, on local disk and on loopback S3;
#   4. reads of the result with Polars `scan_delta` and the DuckDB CLI (`delta_scan`),
#      anonymously on S3, plus a probe of a physical `date` column in the files.
#
# Usage (from anywhere):
#   DELTA_SPIKE_PYTHON=/path/to/venv/bin/python \
#   DELTA_SPIKE_DUCKDB="/path/to/duckdb-1.5.5" \
#   DELTA_SPIKE_DUCKDB_SIGNED="/path/to/duckdb-1.1.1" \
#   spikes/delta-lake/run.sh
#
# The venv comes from requirements.txt, for example:
#   uv venv --python 3.12 /tmp/delta-spike && \
#   uv pip install --python /tmp/delta-spike/bin/python --require-hashes -r spikes/delta-lake/requirements.txt
#
# DELTA_SPIKE_DUCKDB lists CLIs that read S3 anonymously; DELTA_SPIKE_DUCKDB_SIGNED lists
# CLIs that read S3 with placeholder credentials (DuckDB 1.1.1's delta extension fails
# anonymous reads once a checkpoint exists). Both read local tables. Every command runs
# with a cleared environment (PATH, HOME and the Rust/Cargo homes only), in a temp dir,
# and talks to no endpoint other than the loopback server.
set -euo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
PY=${DELTA_SPIKE_PYTHON:?set DELTA_SPIKE_PYTHON to a python with requirements.txt installed}
WORK=$(mktemp -d "${TMPDIR:-/tmp}/delta-spike.XXXXXX")
TARGET=${CARGO_TARGET_DIR:-$HERE/target}
MOTO_PID=
cleanup() {
  [ -n "$MOTO_PID" ] && kill "$MOTO_PID" 2>/dev/null || true
  rm -rf "$WORK"
}
trap cleanup EXIT

clean() {
  env -i PATH="$PATH" HOME="$HOME" TMPDIR="${TMPDIR:-/tmp}" \
    CARGO_HOME="${CARGO_HOME:-$HOME/.cargo}" RUSTUP_HOME="${RUSTUP_HOME:-$HOME/.rustup}" \
    CARGO_TARGET_DIR="$TARGET" CARGO_TERM_COLOR="${CARGO_TERM_COLOR:-auto}" \
    ${CARGO_INCREMENTAL:+CARGO_INCREMENTAL="$CARGO_INCREMENTAL"} \
    ${CARGO_PROFILE_DEV_DEBUG:+CARGO_PROFILE_DEV_DEBUG="$CARGO_PROFILE_DEV_DEBUG"} \
    ${CARGO_PROFILE_TEST_DEBUG:+CARGO_PROFILE_TEST_DEBUG="$CARGO_PROFILE_TEST_DEBUG"} \
    ${S3_ENV:+DELTA_SPIKE_S3_ENDPOINT="$S3_ENV"} "$@"
}

# DuckDB CLI paths contain no spaces; each is installed with the delta extension first,
# retrying transient download errors.
DUCK_ARGS=()
for d in ${DELTA_SPIKE_DUCKDB:-}; do DUCK_ARGS+=(--duckdb "$d"); done
for d in ${DELTA_SPIKE_DUCKDB_SIGNED:-}; do DUCK_ARGS+=(--duckdb-signed "$d"); done
for d in ${DELTA_SPIKE_DUCKDB:-} ${DELTA_SPIKE_DUCKDB_SIGNED:-}; do
  for attempt in 1 2 3; do
    clean "$d" -c "SET extension_directory='$WORK/duckdb-extensions'; INSTALL delta; LOAD delta;" && break
    [ "$attempt" = 3 ] && { echo "cannot install the delta extension for $d" >&2; exit 1; }
    sleep 5
  done
done

cd "$HERE"
echo "== toolchain: $(clean rustc --version)"
echo "== 1. cargo tests (local disk, in-memory store)"
clean cargo test --locked -- --nocapture

echo "== 2. cargo tests on loopback S3"
PORT=$("$PY" -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1])')
clean "$PY" py/loopback_s3.py --port "$PORT" > "$WORK/moto.out" 2> "$WORK/moto.log" &
MOTO_PID=$!
for _ in $(seq 1 100); do grep -q http "$WORK/moto.out" 2>/dev/null && break; sleep 0.1; done
grep -q http "$WORK/moto.out" || { cat "$WORK/moto.log" >&2; exit 1; }
EP=http://127.0.0.1:$PORT
S3_ENV=$EP clean cargo test --locked --test spike -- --nocapture

BIN=$TARGET/debug/delta-lake-spike
echo "== 3a. concurrent maintenance, local disk"
clean "$PY" py/concurrent_maintenance.py --bin "$BIN" --lake "$WORK/local" --transactions 120 --interval-ms 20
echo "== 3b. concurrent maintenance, loopback S3"
clean "$PY" py/concurrent_maintenance.py --bin "$BIN" --lake s3://delta-spike/conc --s3-endpoint "$EP" \
  --transactions 80 --interval-ms 20

echo "== 4a. reads, local disk (compacted, vacuumed, checkpointed)"
clean "$PY" py/read_check.py --lake "$WORK/local" --transactions 120 --blocks 20 \
  --extension-dir "$WORK/duckdb-extensions" ${DUCK_ARGS[@]+"${DUCK_ARGS[@]}"}
echo "== 4b. reads, loopback S3 (anonymous Polars; DuckDB as configured)"
clean "$PY" py/read_check.py --lake s3://delta-spike/conc --s3-endpoint "$EP" --transactions 80 --blocks 20 \
  --extension-dir "$WORK/duckdb-extensions" ${DUCK_ARGS[@]+"${DUCK_ARGS[@]}"}
echo "== 4c. reads of files that also keep the date column (fireparq's current layout)"
clean "$BIN" write --lake "$WORK/physical-date" --transactions 2 --variant physical-date
clean "$PY" py/read_check.py --lake "$WORK/physical-date" --transactions 2 --blocks 20 \
  --extension-dir "$WORK/duckdb-extensions" ${DUCK_ARGS[@]+"${DUCK_ARGS[@]}"}
echo "== all spike checks passed"

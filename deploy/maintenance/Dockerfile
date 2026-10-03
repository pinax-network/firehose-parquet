# The Delta maintenance job (#643): the `fireparq-maintenance` binary
# (maintenance/: order-keeping OPTIMIZE, delta-rs VACUUM, checkpoints and log cleanup),
# configured by environment variables (docs/delta-maintenance.md). Published
# as ghcr.io/pinax-network/firehose-parquet-maintenance next to the fireparq
# image (.github/workflows/docker-publish.yml).
FROM rust:1.98-bookworm AS builder
WORKDIR /app
COPY . .
# Only this crate enables delta-rs's DataFusion; it needs no protoc.
RUN cargo build --release --locked -p fireparq-maintenance --bin fireparq-maintenance

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/fireparq-maintenance /usr/local/bin/
USER 65534:65534
ENTRYPOINT ["fireparq-maintenance"]

FROM rust:1.98-bookworm AS builder
WORKDIR /app
RUN apt-get update && apt-get install -y protobuf-compiler && rm -rf /var/lib/apt/lists/*
COPY . .
# `-p blocks` resolves features for `fireparq` alone, as the release tarballs do;
# without it every workspace member's features apply to the image (#698).
RUN cargo build --release --locked -p blocks --bin fireparq

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/fireparq /usr/local/bin/
ENTRYPOINT ["fireparq"]

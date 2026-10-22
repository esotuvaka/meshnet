FROM rust:bookworm AS builder

WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src

RUN cargo build --locked --release

FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    ca-certificates \
    iputils-ping \
    iproute2 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /src/target/release/meshnet /usr/local/bin/meshnet

ENTRYPOINT ["meshnet"]

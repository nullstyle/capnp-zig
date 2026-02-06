# Cap'n Proto Rust RPC container

FROM rust:1.82-bookworm AS builder

RUN apt-get update && apt-get install -y \
    capnproto \
    pkg-config \
    g++ \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY tests/e2e/rust/Cargo.toml /app/Cargo.toml
COPY tests/e2e/rust/Cargo.lock /app/Cargo.lock
COPY tests/e2e/rust/build.rs /app/build.rs
COPY tests/e2e/rust/src /app/src
COPY tests/e2e/schemas /app/schemas

RUN cargo build --release

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /app/target/release/e2e-rpc-test /app/e2e-rpc-test

RUN mkdir -p /app/bin && \
    printf '#!/bin/sh\nexec /app/e2e-rpc-test server "$@"\n' > /app/bin/server && \
    printf '#!/bin/sh\nexec /app/e2e-rpc-test client "$@"\n' > /app/bin/client && \
    chmod +x /app/bin/server /app/bin/client

COPY tests/e2e/docker/entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

ENTRYPOINT ["/app/entrypoint.sh"]
CMD ["server", "--port", "4003", "--schema", "game_world"]

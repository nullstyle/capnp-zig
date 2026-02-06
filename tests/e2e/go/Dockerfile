# Cap'n Proto Go RPC container
# Uses vendored capnproto.org/go/capnp/v3 for deterministic builds.

FROM golang:1.25.6-bookworm AS builder

# Install capnp compiler
RUN apt-get update && apt-get install -y \
    capnproto \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /repo/tests/e2e/go

# Copy module manifests and the vendored go-capnp module first for better layer caching.
COPY tests/e2e/go/go.mod tests/e2e/go/go.sum /repo/tests/e2e/go/
COPY vendor/ext/go-capnp /repo/vendor/ext/go-capnp

# Build capnpc-go from the vendored module and install standard schema includes.
RUN GOBIN=/usr/local/bin go install /repo/vendor/ext/go-capnp/capnpc-go
RUN mkdir -p /usr/local/share/go-capnp-std \
    && cp -r /repo/vendor/ext/go-capnp/std/* /usr/local/share/go-capnp-std/

# Resolve dependencies from go.mod/go.sum before copying the full source tree.
RUN go mod download \
    && go mod verify

# Copy Go source and schemas.
COPY tests/e2e/go/cmd/ /repo/tests/e2e/go/cmd/
COPY tests/e2e/go/internal/servers/ /repo/tests/e2e/go/internal/servers/
COPY tests/e2e/go/schemas/ /repo/tests/e2e/go/schemas/

# Generate Go bindings from schemas
RUN mkdir -p /repo/tests/e2e/go/internal/gametypes \
             /repo/tests/e2e/go/internal/gameworld \
             /repo/tests/e2e/go/internal/chat \
             /repo/tests/e2e/go/internal/inventory \
             /repo/tests/e2e/go/internal/matchmaking

RUN capnp compile -ogo:/repo/tests/e2e/go/internal/gametypes \
      -I /usr/local/share/go-capnp-std \
      --src-prefix=/repo/tests/e2e/go/schemas \
      /repo/tests/e2e/go/schemas/game_types.capnp

RUN capnp compile -ogo:/repo/tests/e2e/go/internal/gameworld \
      -I /usr/local/share/go-capnp-std \
      -I /repo/tests/e2e/go/schemas \
      --src-prefix=/repo/tests/e2e/go/schemas \
      /repo/tests/e2e/go/schemas/game_world.capnp

RUN capnp compile -ogo:/repo/tests/e2e/go/internal/chat \
      -I /usr/local/share/go-capnp-std \
      -I /repo/tests/e2e/go/schemas \
      --src-prefix=/repo/tests/e2e/go/schemas \
      /repo/tests/e2e/go/schemas/chat.capnp

RUN capnp compile -ogo:/repo/tests/e2e/go/internal/inventory \
      -I /usr/local/share/go-capnp-std \
      -I /repo/tests/e2e/go/schemas \
      --src-prefix=/repo/tests/e2e/go/schemas \
      /repo/tests/e2e/go/schemas/inventory.capnp

RUN capnp compile -ogo:/repo/tests/e2e/go/internal/matchmaking \
      -I /usr/local/share/go-capnp-std \
      -I /repo/tests/e2e/go/schemas \
      --src-prefix=/repo/tests/e2e/go/schemas \
      /repo/tests/e2e/go/schemas/matchmaking.capnp

# Build server/client binaries.
RUN CGO_ENABLED=0 go build -o /app/bin/server ./cmd/server
RUN CGO_ENABLED=0 go build -o /app/bin/client ./cmd/client

# Runtime stage
FROM debian:bookworm-slim

COPY --from=builder /app/bin/server /app/bin/server
COPY --from=builder /app/bin/client /app/bin/client

# Schema files for reference
COPY tests/e2e/schemas/ /app/schemas/

COPY tests/e2e/docker/entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

WORKDIR /app

ENTRYPOINT ["/app/entrypoint.sh"]
CMD ["server", "--port", "4001"]

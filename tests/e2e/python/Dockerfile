# Cap'n Proto Python RPC container
# Uses pycapnp and the game-domain e2e RPC scenarios.

FROM python:3.12-bookworm

RUN apt-get update && apt-get install -y \
    capnproto \
    libcapnp-dev \
    pkg-config \
    g++ \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY tests/e2e/python/requirements.txt /app/python/requirements.txt
RUN pip install --no-cache-dir -r /app/python/requirements.txt

COPY tests/e2e/schemas/ /app/schemas/
COPY tests/e2e/python/ /app/python/

RUN mkdir -p /app/bin && \
    printf '#!/bin/sh\nexec python3 /app/python/server.py "$@"\n' > /app/bin/server && \
    printf '#!/bin/sh\nexec python3 /app/python/client.py "$@"\n' > /app/bin/client && \
    chmod +x /app/bin/server /app/bin/client

ENV SCHEMA_DIR=/app/schemas

COPY tests/e2e/docker/entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

ENTRYPOINT ["/app/entrypoint.sh"]
CMD ["server", "--port", "4002", "--schema", "game_world"]

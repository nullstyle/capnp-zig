# Cap'n Proto C++ RPC container
# Builds capnproto from source (vendored in repo), then builds e2e test binaries.

FROM ubuntu:24.04 AS builder

RUN apt-get update && apt-get install -y \
    build-essential \
    clang \
    g++-14 \
    cmake \
    autoconf \
    automake \
    libtool \
    pkg-config \
    libssl-dev \
    zlib1g-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Copy the vendored capnproto source
COPY vendor/ext/capnproto /build/capnproto

# Build capnproto from source using cmake
WORKDIR /build/capnproto/c++
RUN cmake -B build \
      -DCMAKE_C_COMPILER=/usr/bin/clang \
      -DCMAKE_CXX_COMPILER=/usr/bin/clang++ \
      -DCMAKE_INSTALL_PREFIX=/usr/local \
      -DCMAKE_BUILD_TYPE=Release \
      -DBUILD_TESTING=OFF \
    && cmake --build build -j$(nproc) \
    && cmake --install build

# Now build the e2e C++ server and client
WORKDIR /build/e2e

COPY tests/e2e/schemas/ /build/e2e/schemas/
COPY tests/e2e/cpp/ /build/e2e/cpp/

RUN rm -rf /build/e2e/cpp/build \
    && cd cpp \
    && cmake -S . -B build \
      -DCMAKE_C_COMPILER=/usr/bin/clang \
      -DCMAKE_CXX_COMPILER=/usr/bin/clang++ \
      -DCMAKE_PREFIX_PATH=/usr/local \
      -DCMAKE_BUILD_TYPE=Release \
    && cmake --build build -j$(nproc)

# Runtime stage
FROM ubuntu:24.04

RUN apt-get update && apt-get install -y \
    libstdc++6 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /usr/local/lib/libcapnp* /usr/local/lib/
COPY --from=builder /usr/local/lib/libkj* /usr/local/lib/
COPY --from=builder /usr/local/lib/libcapnpc* /usr/local/lib/

RUN ldconfig

WORKDIR /app

# Copy built binaries
COPY --from=builder /build/e2e/cpp/build/e2e_server /app/bin/server
COPY --from=builder /build/e2e/cpp/build/e2e_client /app/bin/client

# Schema files for reference
COPY tests/e2e/schemas/ /app/schemas/

# The entrypoint script handles server/client mode
COPY tests/e2e/docker/entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

ENTRYPOINT ["/app/entrypoint.sh"]
CMD ["server", "--port", "4000"]

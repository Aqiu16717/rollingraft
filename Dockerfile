# RollingRaft Docker Image
# Multi-stage build for optimized size

# Build stage
FROM ubuntu:22.04 AS builder

# Install build dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    cmake \
    git \
    libprotobuf-dev \
    libsnappy-dev \
    libssl-dev \
    pkg-config \
    protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /build

# Copy source code
COPY . .

# Initialize submodules (if needed)
RUN git submodule update --init --recursive || true

# Build with all optimizations and install to a staging prefix
RUN cmake -B build \
    -DCMAKE_BUILD_TYPE=Release \
    -DBUILD_TESTING=ON \
    -DBUILD_EXAMPLES=ON \
    -DBUILD_BENCHMARK=ON \
    -DCMAKE_INSTALL_PREFIX=/build/install \
    && cmake --build build -j$(nproc) \
    && cmake --install build

# Runtime stage
FROM ubuntu:22.04 AS runtime

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    libprotobuf23 \
    libsnappy1v5 \
    libssl3 \
    curl \
    netcat \
    iputils-ping \
    && rm -rf /var/lib/apt/lists/*

# Create app directory
WORKDIR /app

# Copy installed binaries and libraries from builder
COPY --from=builder /build/install/ /app/

# Copy wait-for-cluster helper script
COPY --from=builder /build/scripts/wait-for-cluster.sh /app/bin/
RUN chmod +x /app/bin/wait-for-cluster.sh

# Copy test TLS certificates used by integration tests
COPY --from=builder /build/tests/certs/ /build/tests/certs/

# Copy test certificates used by integration tests
COPY --from=builder /build/tests/certs /build/tests/certs

# Preserve the configure-time path compiled into the node identity tests.
COPY --from=builder /build/build/generated-node-certs /build/build/generated-node-certs

# Create data directory
RUN mkdir -p /app/data

# Expose common Raft ports
EXPOSE 8000-8010

# Default command
CMD ["/app/bin/example_counter_server", "--help"]

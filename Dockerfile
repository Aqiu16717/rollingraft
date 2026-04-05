# RollingRaft Docker Image
# Multi-stage build for optimized size

# Build stage
FROM ubuntu:22.04 AS builder

# Install build dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    cmake \
    git \
    libsnappy-dev \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /build

# Copy source code
COPY . .

# Initialize submodules (if needed)
RUN git submodule update --init --recursive || true

# Build with all optimizations
RUN cmake -B build \
    -DCMAKE_BUILD_TYPE=Release \
    -DBUILD_TESTING=ON \
    -DBUILD_EXAMPLES=ON \
    -DBUILD_BENCHMARK=ON \
    && cmake --build build -j$(nproc)

# Runtime stage
FROM ubuntu:22.04 AS runtime

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    libsnappy1v5 \
    curl \
    netcat \
    iputils-ping \
    && rm -rf /var/lib/apt/lists/*

# Create app directory
WORKDIR /app

# Copy binaries from builder
COPY --from=builder /build/build/librollingraft.a /app/lib/
COPY --from=builder /build/build/example/example_counter_server /app/bin/
COPY --from=builder /build/build/example/example_counter_client /app/bin/
COPY --from=builder /build/build/tests/unit_tests /app/bin/
COPY --from=builder /build/build/tests/integration_tests /app/bin/
COPY --from=builder /build/build/benchmark/benchmark_client /app/bin/
COPY --from=builder /build/build/benchmark/benchmark_latency_curve /app/bin/

# Create data directory
RUN mkdir -p /app/data

# Expose common Raft ports
EXPOSE 8000-8010

# Default command
CMD ["/app/bin/example_counter_server", "--help"]

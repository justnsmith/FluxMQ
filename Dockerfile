# ── Stage 1: Builder ──────────────────────────────────────────────────────────
FROM ubuntu:24.04 AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    g++ \
    cmake \
    make \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY . .
RUN cmake -S . -B build -DCMAKE_BUILD_TYPE=Release \
 && cmake --build build --target fluxmq -j"$(nproc)" \
 && ln -sf src/fluxmq build/fluxmq

# ── Stage 2: Runtime ──────────────────────────────────────────────────────────
FROM ubuntu:24.04

# curl is used for the healthcheck (GET /metrics).
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /src/build/fluxmq /usr/local/bin/fluxmq
COPY docker/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Default directories — override via FLUXMQ_DATA_DIR / FLUXMQ_CLUSTER_DIR.
RUN mkdir -p /data /cluster
VOLUME ["/data", "/cluster"]

# Broker binary protocol port (default 9092) + metrics HTTP port (default port+1).
EXPOSE 9092
EXPOSE 9093

ENTRYPOINT ["/entrypoint.sh"]

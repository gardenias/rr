# Lab image: kv-store (with real Kafka ingest via rdkafka) + rr-admin + rr-perf-bench.
# Build context must be the REPO ROOT (compose sets context: ../..):
#   docker build -f deploy/lab/Dockerfile -t rr-lab:dev .
#
# 仅本地实验环境 / LOCAL LAB ONLY — the resulting image runs with
# allow_insecure and no TLS. Never deploy it outside a local lab.
ARG RUST_VERSION=1.98
ARG DEBIAN_VERSION=bookworm

FROM rust:${RUST_VERSION}-${DEBIAN_VERSION} AS builder

# cmake + protobuf-compiler: rdkafka (cmake-build, bundled librdkafka) and
# tonic/prost codegen. libclang-dev: bindgen via rdkafka-sys.
# libsasl2-dev: rdkafka's `sasl` feature links the system libsasl2.
RUN apt-get update && apt-get install -y --no-install-recommends \
        libclang-dev \
        cmake \
        protobuf-compiler \
        ca-certificates \
        pkg-config \
        libsasl2-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
COPY tests ./tests
COPY benches ./benches
COPY examples ./examples
COPY tools ./tools

# BuildKit cache mounts keep the cargo registry + target dir across builds,
# so adding/rebuilding a binary later is incremental instead of a full
# librdkafka + workspace recompile. Binaries are copied out of the cache
# mount into /out (cache mounts are not part of the layer).
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release -p kv-store --bin kv-store --features rdkafka \
    && cargo build --release -p rr-admin --bin rr-admin \
    && cargo build --release -p rr-perf-bench --bin rr-perf-bench \
    && if [ -f tools/rr-lab-producer/Cargo.toml ]; then \
         cargo build --release -p rr-lab-producer --bin rr-lab-producer; \
       fi \
    && mkdir -p /out \
    && cp target/release/kv-store target/release/rr-admin target/release/rr-perf-bench /out/ \
    && if [ -f target/release/rr-lab-producer ]; then \
         cp target/release/rr-lab-producer /out/; \
       fi

FROM debian:${DEBIAN_VERSION}-slim AS runtime

RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates \
        libssl3 \
        libgcc-s1 \
        libsasl2-2 \
        curl \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd -r rr --gid 10001 \
    && useradd -r -g rr --uid 10001 --home-dir /var/lib/rr --shell /usr/sbin/nologin rr \
    && mkdir -p /var/lib/rr/data \
    && chown -R rr:rr /var/lib/rr

# /out always contains kv-store/rr-admin/rr-perf-bench, plus rr-lab-producer
# when tools/rr-lab-producer exists in the workspace.
COPY --from=builder /out/ /usr/local/bin/

USER rr:rr
WORKDIR /var/lib/rr

# client+raft gRPC / admin gRPC / HTTP observability
EXPOSE 51001 52001 9100

HEALTHCHECK --interval=10s --timeout=3s --start-period=20s --retries=3 \
    CMD ["curl", "-fsS", "http://127.0.0.1:9100/ready"]

ENTRYPOINT ["/usr/local/bin/kv-store"]

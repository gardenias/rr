# Must match the workspace MSRV (Cargo.toml: rust-version) and rust-toolchain.toml.
ARG RUST_VERSION=1.88
ARG DEBIAN_VERSION=bookworm

FROM rust:${RUST_VERSION}-${DEBIAN_VERSION} AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
        libclang-dev \
        cmake \
        protobuf-compiler \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src

COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
COPY tests ./tests
COPY benches ./benches
COPY examples ./examples
# tools/rr-perf-bench is a workspace member; without it cargo refuses to load
# the workspace manifest at all.
COPY tools ./tools

RUN cargo build --release \
        -p rr-server --bin rr-server --features rocks \
    && cargo build --release \
        -p rr-admin --bin rr-admin

FROM debian:${DEBIAN_VERSION}-slim AS runtime

# curl is required by HEALTHCHECK below (bookworm-slim ships without it).
RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates \
        libssl3 \
        libgcc-s1 \
        curl \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd -r rr --gid 10001 \
    && useradd -r -g rr --uid 10001 --home-dir /var/lib/rr --shell /usr/sbin/nologin rr \
    && mkdir -p /var/lib/rr/data /etc/rr \
    && chown -R rr:rr /var/lib/rr /etc/rr

COPY --from=builder /src/target/release/rr-server /usr/local/bin/rr-server
COPY --from=builder /src/target/release/rr-admin  /usr/local/bin/rr-admin

USER rr:rr
WORKDIR /var/lib/rr

VOLUME ["/var/lib/rr/data"]

EXPOSE 51001
EXPOSE 52001
EXPOSE 9100

# Readiness is served by the HTTP observability server on 0.0.0.0:9100
# (metrics_port): /ready returns 200 only after start() completes, 503 during
# startup/shutdown — see crates/rr-server/src/health.rs. Used by compose
# service_healthy; k8s uses its own probes instead.
HEALTHCHECK --interval=10s --timeout=3s --start-period=15s --retries=3 \
    CMD ["curl", "-fsS", "http://127.0.0.1:9100/ready"]

ENTRYPOINT ["/usr/local/bin/rr-server"]
CMD ["serve", "--config", "/etc/rr/config.toml"]

# Combined Leona swarm node image: DefraDB plus context broker.
FROM rust:1.97-bookworm AS builder
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY tools/swarm-monitor/Cargo.toml tools/swarm-monitor/Cargo.toml
COPY tools/swarm-monitor/src tools/swarm-monitor/src
RUN cargo build --release -p leona_context_broker

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*
COPY --from=sourcenetwork/defradb:1.0.0-rc1 /defradb /usr/local/bin/defradb
COPY --from=builder /build/target/release/leona_context_broker /usr/local/bin/leona_context_broker
COPY docker/node-entrypoint.sh /usr/local/bin/node-entrypoint.sh
RUN chmod +x /usr/local/bin/node-entrypoint.sh
EXPOSE 8080 9181 9171
ENTRYPOINT ["/usr/local/bin/node-entrypoint.sh"]

FROM rust:1.82-bookworm AS builder
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release

# axum/tokio have no native deps beyond glibc, so a slim runtime (not
# distroless) keeps this simple without needing a musl target.
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates wget \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /src/target/release/ranking-service /app/ranking-service
WORKDIR /app
EXPOSE 4003
ENTRYPOINT ["/app/ranking-service"]

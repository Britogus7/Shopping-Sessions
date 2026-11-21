FROM rust:1.89-bookworm AS builder

WORKDIR /src
ENV RUSTFLAGS="-C target-cpu=x86-64-v3"
COPY Cargo.toml /src/Cargo.toml
RUN mkdir -p src && echo "fn main() {}" > src/main.rs && cargo build --release && rm -rf src

COPY src /src/src
RUN touch src/main.rs && cargo build --release

FROM debian:bookworm-slim

RUN useradd --uid 1000 --no-create-home --home-dir /scratch app
COPY --from=builder /src/target/release/shopping-sessions /app/shopping-sessions
RUN chmod +x /app/shopping-sessions

WORKDIR /app
CMD ["/app/shopping-sessions"]

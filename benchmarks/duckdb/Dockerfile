FROM rust:1.76-slim-bullseye as builder

WORKDIR /app

# Copy manifests
COPY ./Cargo.toml ./Cargo.toml
COPY ./Cargo.lock ./Cargo.lock
COPY ./common/Cargo.toml ./common/Cargo.toml
COPY ./benchmarks/duckdb/Cargo.toml ./benchmarks/duckdb/Cargo.toml

# Create dummy source files for dependencies caching
RUN mkdir -p common/src && \
    echo "pub fn dummy() {}" > common/src/lib.rs && \
    mkdir -p benchmarks/duckdb/src && \
    echo "fn main() {}" > benchmarks/duckdb/src/main.rs

# Build dependencies only to cache them
RUN cargo build --release --package benchmarks-duckdb

# Now copy the actual source code
COPY ./common ./common
COPY ./benchmarks/duckdb ./benchmarks/duckdb

# Build the application
RUN touch benchmarks/duckdb/src/main.rs common/src/lib.rs && \
    cargo build --release --package benchmarks-duckdb

# Create the runtime image
FROM debian:bullseye-slim

WORKDIR /app

# Copy the binary from the builder
COPY --from=builder /app/target/release/benchmarks-duckdb /app/benchmarks-duckdb

# Create necessary directories
RUN mkdir -p /app/data

# Set the entrypoint
ENTRYPOINT ["/app/benchmarks-duckdb"] 
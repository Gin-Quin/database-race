FROM rust:1.74-slim as builder

WORKDIR /app

# Copy the entire project
COPY . .

# Build the project
RUN cargo build --release -p benchmarks-sqlite

# Create a smaller runtime image
FROM debian:bookworm-slim

WORKDIR /app

# Install runtime dependencies
RUN apt-get update && apt-get install -y libssl-dev ca-certificates && rm -rf /var/lib/apt/lists/*

# Copy the built binary
COPY --from=builder /app/target/release/benchmarks-sqlite /app/benchmarks-sqlite

# Create data directory
RUN mkdir -p /app/data

# Expose the server port
EXPOSE 3001

# Run the benchmark server
CMD ["/app/benchmarks-sqlite"] 
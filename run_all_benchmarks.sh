#!/bin/bash

# Start all database services
echo "Starting database services..."
docker-compose up -d

# Wait for databases to initialize
echo "Waiting for databases to initialize..."
sleep 10

# Run all benchmark services in Docker
echo "Running all benchmarks..."
docker-compose up -d sqlite-benchmark duckdb-benchmark rocksdb-benchmark

# Wait for benchmarks to complete
echo "All benchmark services are running. Access results at:"
echo "- SQLite: http://localhost:3001/results"
echo "- DuckDB: http://localhost:3002/results"
echo "- RocksDB: http://localhost:3003/results"
echo "- PostgreSQL: http://localhost:3004/results (run with cargo run -p benchmarks-postgres)"
echo "- SurrealDB: http://localhost:3005/results (run with cargo run -p benchmarks-surrealdb)"
echo "- KuZu: http://localhost:3006/results (run with cargo run -p benchmarks-kuzu)"

# Instructions for running locally
echo ""
echo "To run benchmarks locally instead of in Docker:"
echo "cargo run -p benchmarks-sqlite"
echo "cargo run -p benchmarks-postgres"
echo "cargo run -p benchmarks-surrealdb"
echo "cargo run -p benchmarks-kuzu"
echo "cargo run -p benchmarks-duckdb"
echo "cargo run -p benchmarks-rocksdb" 
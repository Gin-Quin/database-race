# Database Race

This project benchmarks multiple database systems with a standardized test suite.

## Databases Tested

- SQLite
- KuZu
- SurrealDB
- PostgreSQL
- DuckDB
- RocksDB

## Test Schema

The benchmark uses a simple schema with 3 tables:
- Users
- Products
- Orders

## Benchmark Tests

Each database is tested with the following operations:
1. Inserting a single entry many times
2. Inserting many entries in a single query
3. Reading one entry from its id many times
4. Reading many entries all at once from an array of ids
5. Reading many entries from a column search
6. Reading entries with 1 join
7. Reading entries with 2 joins
8. Updating one field in one single entry
9. Updating one field in many entries at once
10. Updating several fields in one single entry
11. Updating several fields in many entries at once

## Running the Benchmarks

1. Start the database services:
   ```
   docker-compose up -d
   ```

2. Run a specific benchmark:
   ```
   cargo run -p benchmarks-sqlite
   ```

3. Run all benchmarks:
   ```
   ./run_all_benchmarks.sh
   ```

## Accessing Results

Each database benchmark exposes results via a REST API endpoint on the following ports:
- SQLite: http://localhost:3001/results
- DuckDB: http://localhost:3002/results
- RocksDB: http://localhost:3003/results
- PostgreSQL: http://localhost:3004/results
- SurrealDB: http://localhost:3005/results
- KuZu: http://localhost:3006/results

## Project Structure

```
database-race/
├── Cargo.toml
├── docker-compose.yml
├── run_all_benchmarks.sh
└── benchmarks/
    ├── sqlite/
    ├── kuzu/
    ├── surrealdb/
    ├── postgres/
    ├── duckdb/
    └── rocksdb/
```

## Configuration

Each benchmark can be configured with:
- Number of operations
- Parallelism (CPU cores)
- Data size

See the configuration section in each benchmark's README for details. 
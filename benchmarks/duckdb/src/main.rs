use anyhow::Result;
mod duckdb_benchmark;

use crate::duckdb_benchmark::DuckdbBenchmark;
use common::server::run_server;

#[tokio::main]
async fn main() -> Result<()> {
	println!("Starting DuckDB benchmark");
	// Create a new DuckDB benchmark with 1 CPU core initially
	let benchmark = DuckdbBenchmark::new(4).await?;
	println!("Benchmark created");

	// Run the server on port 3002
	run_server(benchmark, 3002).await?;

	Ok(())
}

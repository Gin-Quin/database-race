use anyhow::Result;
mod sqlite_benchmark;

use crate::sqlite_benchmark::SqliteBenchmark;
use common::server::run_server;

#[tokio::main]
async fn main() -> Result<()> {
	println!("Starting SQLite benchmark");
	// Create a new SQLite benchmark with 4 CPU cores
	let benchmark = SqliteBenchmark::new(1).await?;
	println!("Benchmark created");

	// Run the server on port 3001
	run_server(benchmark, 3001).await?;

	Ok(())
}

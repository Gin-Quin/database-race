use anyhow::Result;
mod rocksdb_benchmark;

use crate::rocksdb_benchmark::RocksDBBenchmark;
use common::server::run_server;

#[tokio::main]
async fn main() -> Result<()> {
	println!("Starting RocksDB benchmark");
	let benchmark = RocksDBBenchmark::new(4).await?;
	println!("Benchmark created");

	run_server(benchmark, 3003).await?;

	Ok(())
}

use std::time::{ Duration, Instant };
use async_trait::async_trait;
use chrono::Utc;
use rand::Rng;
use uuid::Uuid;
use anyhow::Result;

use crate::models::{
	BenchmarkResult,
	BenchmarkResults,
	Order,
	OrderWithDetails,
	Product,
	User,
};

#[async_trait]
pub trait DatabaseBenchmark {
	/// Initialize the database with schema and needed setup
	async fn init(&self) -> Result<()>;

	/// Generate test data
	async fn generate_test_data(&self, count: usize) -> Result<()>;

	/// Clean up any data from previous benchmarks
	async fn cleanup(&self) -> Result<()>;

	/// Database name
	fn database_name(&self) -> String;

	/// Configure number of CPU cores to use (if supported)
	fn set_cpu_count(&mut self, count: usize);

	/// Get current CPU core count setting
	fn get_cpu_count(&self) -> usize;

	/// Test 1: Insert single entry many times
	async fn insert_single_many_times(&self, count: usize) -> Result<BenchmarkResult>;

	/// Test 2: Insert many entries at once
	async fn insert_many_at_once(&self, count: usize) -> Result<BenchmarkResult>;

	/// Test 3: Read single entry by ID many times
	async fn read_by_id_many_times(&self, count: usize) -> Result<BenchmarkResult>;

	/// Test 4: Read many entries by ID at once
	async fn read_many_by_ids(&self, count: usize) -> Result<BenchmarkResult>;

	/// Test 5: Read entries by column value
	async fn read_by_column_search(&self, count: usize) -> Result<BenchmarkResult>;

	/// Test 6: Read entries with one join
	async fn read_with_one_join(&self, count: usize) -> Result<BenchmarkResult>;

	/// Test 7: Read entries with two joins
	async fn read_with_two_joins(&self, count: usize) -> Result<BenchmarkResult>;

	/// Test 8: Update single field in one entry
	async fn update_single_field_one_entry(
		&self,
		count: usize
	) -> Result<BenchmarkResult>;

	/// Test 9: Update single field in many entries
	async fn update_single_field_many_entries(
		&self,
		count: usize
	) -> Result<BenchmarkResult>;

	/// Test 10: Update multiple fields in one entry
	async fn update_multiple_fields_one_entry(
		&self,
		count: usize
	) -> Result<BenchmarkResult>;

	/// Test 11: Update multiple fields in many entries
	async fn update_multiple_fields_many_entries(
		&self,
		count: usize
	) -> Result<BenchmarkResult>;

	/// Run all benchmarks with the given operation count
	async fn run_all_benchmarks(&self) -> Result<BenchmarkResults> {
		println!("Running all benchmarks");
		let mut results = Vec::new();

		// Run all 11 benchmark tests
		results.push(self.insert_single_many_times(20_00).await?);
		results.push(self.insert_many_at_once(10_00).await?);
		results.push(self.read_by_id_many_times(10_00).await?);
		results.push(self.read_many_by_ids(20_00).await?);
		results.push(self.read_by_column_search(20_00).await?);
		results.push(self.read_with_one_join(20_00).await?);
		results.push(self.read_with_two_joins(20_00).await?);
		results.push(self.update_single_field_one_entry(5_00).await?);
		results.push(self.update_single_field_many_entries(10_00).await?);
		results.push(self.update_multiple_fields_one_entry(2_00).await?);
		results.push(self.update_multiple_fields_many_entries(50_00).await?);

		Ok(BenchmarkResults {
			database: self.database_name(),
			results,
			timestamp: Utc::now(),
		})
	}
}

// Helper function to measure execution time and create benchmark result
pub async fn measure_execution<F, Fut>(
	database_name: &str,
	test_name: &str,
	operations: usize,
	cpu_count: usize,
	f: F
)
	-> Result<BenchmarkResult>
	where F: FnOnce() -> Fut, Fut: std::future::Future<Output = Result<()>>
{
	let start = Instant::now();
	f().await?;
	let duration = start.elapsed();

	let duration_ms = duration.as_millis() as u64;
	let operations_per_second = if duration_ms > 0 {
		(operations as f64) / ((duration_ms as f64) / 1000.0)
	} else {
		operations as f64 // Avoid division by zero
	};

	Ok(BenchmarkResult {
		database: database_name.to_string(),
		test_name: test_name.to_string(),
		operations,
		duration_ms,
		operations_per_second,
		cpu_count,
		timestamp: Utc::now(),
	})
}

// Helper functions to generate random data for benchmarks
pub fn generate_random_user() -> User {
	let mut rng = rand::thread_rng();

	User {
		id: Uuid::new_v4(),
		name: format!("User {}", rng.gen_range(1000..9999)),
		email: format!("user{}@example.com", rng.gen_range(1000..9999)),
		created_at: Utc::now(),
		active: rng.gen_bool(0.9),
	}
}

pub fn generate_random_product() -> Product {
	let mut rng = rand::thread_rng();

	Product {
		id: Uuid::new_v4(),
		name: format!("Product {}", rng.gen_range(1000..9999)),
		description: format!("Description for product {}", rng.gen_range(1000..9999)),
		price: (rng.gen_range(100..10000) as f64) / 100.0,
		stock: rng.gen_range(0..1000),
		created_at: Utc::now(),
	}
}

pub fn generate_random_order(user_id: Uuid, product_id: Uuid) -> Order {
	let mut rng = rand::thread_rng();
	let quantity = rng.gen_range(1..10);
	let price = (rng.gen_range(1000..10000) as f64) / 100.0;

	Order {
		id: Uuid::new_v4(),
		user_id,
		product_id,
		quantity,
		total_price: price * (quantity as f64),
		created_at: Utc::now(),
	}
}

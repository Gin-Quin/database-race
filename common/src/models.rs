use chrono::{ DateTime, Utc };
use serde::{ Deserialize, Serialize };
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct User {
	pub id: Uuid,
	pub name: String,
	pub email: String,
	pub created_at: DateTime<Utc>,
	pub active: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Product {
	pub id: Uuid,
	pub name: String,
	pub description: String,
	pub price: f64,
	pub stock: i32,
	pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Order {
	pub id: Uuid,
	pub user_id: Uuid,
	pub product_id: Uuid,
	pub quantity: i32,
	pub total_price: f64,
	pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderWithDetails {
	pub id: Uuid,
	pub quantity: i32,
	pub total_price: f64,
	pub created_at: DateTime<Utc>,
	pub user: User,
	pub product: Product,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchmarkResult {
	pub database: String,
	pub test_name: String,
	pub operations: usize,
	pub duration_ms: u64,
	pub operations_per_second: f64,
	pub cpu_count: usize,
	pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchmarkResults {
	pub database: String,
	pub results: Vec<BenchmarkResult>,
	pub timestamp: DateTime<Utc>,
}

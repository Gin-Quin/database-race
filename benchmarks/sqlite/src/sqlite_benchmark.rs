use anyhow::Result;
use async_trait::async_trait;
use common::{
	benchmark::{
		measure_execution,
		DatabaseBenchmark,
		generate_random_order,
		generate_random_product,
		generate_random_user,
	},
	models::{ BenchmarkResult, Product, User },
};
use rusqlite::{ params, OptionalExtension };
use tokio_rusqlite::Connection as AsyncConnection;
use std::path::Path;
use uuid::Uuid;

pub struct SqliteBenchmark {
	db_path: String,
	cpu_count: usize,
}

impl SqliteBenchmark {
	pub async fn new(cpu_count: usize) -> Result<Self> {
		let db_path = "./data/sqlite-benchmark.db".to_string();

		// Create data directory if it doesn't exist
		let data_dir = Path::new("./data");
		if !data_dir.exists() {
			std::fs::create_dir_all(data_dir)?;
		}

		// Create a new instance
		let benchmark = Self { db_path, cpu_count };

		// Initialize database
		benchmark.init().await?;

		Ok(benchmark)
	}

	// Helper to get an async connection
	async fn get_async_connection(&self) -> Result<AsyncConnection> {
		let conn = AsyncConnection::open(&self.db_path).await?;

		// Enable WAL mode and other optimizations
		conn.call(|conn| {
			println!("Setting PRAGMA journal_mode = WAL");
			let _ = conn.prepare("PRAGMA journal_mode = WAL")?.query([])?;

			println!("Setting PRAGMA synchronous = NORMAL");
			// These don't return results, so execute is fine
			conn.execute("PRAGMA synchronous = NORMAL", [])?;

			println!("Setting PRAGMA cache_size = 100000");
			conn.execute(&format!("PRAGMA cache_size = {}", 100000), [])?;

			println!("Setting PRAGMA busy_timeout = 5000");
			let _ = conn.prepare("PRAGMA busy_timeout = 5000")?.query([])?;

			println!("Setting PRAGMA mmap_size = 30000000000");
			let _ = conn.prepare("PRAGMA mmap_size = 30000000000")?.query([])?;

			Ok(())
		}).await?;

		Ok(conn)
	}
}

#[async_trait]
impl DatabaseBenchmark for SqliteBenchmark {
	async fn init(&self) -> Result<()> {
		let conn = self.get_async_connection().await?;

		conn.call(|conn| {
			// Create users table
			conn.execute(
				"CREATE TABLE IF NOT EXISTS users (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    email TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    active INTEGER NOT NULL
                )",
				[]
			)?;

			// Create products table
			conn.execute(
				"CREATE TABLE IF NOT EXISTS products (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    description TEXT NOT NULL,
                    price REAL NOT NULL,
                    stock INTEGER NOT NULL,
                    created_at TEXT NOT NULL
                )",
				[]
			)?;

			// Create orders table with foreign keys
			conn.execute(
				"CREATE TABLE IF NOT EXISTS orders (
                    id TEXT PRIMARY KEY,
                    user_id TEXT NOT NULL,
                    product_id TEXT NOT NULL,
                    quantity INTEGER NOT NULL,
                    total_price REAL NOT NULL,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY (user_id) REFERENCES users (id),
                    FOREIGN KEY (product_id) REFERENCES products (id)
                )",
				[]
			)?;

			// Create indexes
			conn.execute(
				"CREATE INDEX IF NOT EXISTS idx_users_email ON users (email)",
				[]
			)?;
			conn.execute(
				"CREATE INDEX IF NOT EXISTS idx_products_name ON products (name)",
				[]
			)?;
			conn.execute(
				"CREATE INDEX IF NOT EXISTS idx_orders_user_id ON orders (user_id)",
				[]
			)?;
			conn.execute(
				"CREATE INDEX IF NOT EXISTS idx_orders_product_id ON orders (product_id)",
				[]
			)?;

			Ok(())
		}).await?;

		Ok(())
	}

	async fn generate_test_data(&self, count: usize) -> Result<()> {
		let conn = self.get_async_connection().await?;

		// Generate users
		let users: Vec<User> = (0..count).map(|_| generate_random_user()).collect();

		// Generate products
		let products: Vec<Product> = (0..count)
			.map(|_| generate_random_product())
			.collect();

		// Generate orders (one per user/product pair for simplicity)
		let mut orders = Vec::with_capacity(count);
		for i in 0..count {
			let user_id = users[i % users.len()].id;
			let product_id = products[i % products.len()].id;
			orders.push(generate_random_order(user_id, product_id));
		}

		// Insert all data
		conn.call(move |conn| {
			// Use a transaction for better performance
			let tx = conn.transaction()?;

			// Insert users
			for user in &users {
				tx.execute(
					"INSERT INTO users (id, name, email, created_at, active) VALUES (?, ?, ?, ?, ?)",
					params![
						user.id.to_string(),
						user.name,
						user.email,
						user.created_at.to_rfc3339(),
						user.active as i32
					]
				)?;
			}

			// Insert products
			for product in &products {
				tx.execute(
					"INSERT INTO products (id, name, description, price, stock, created_at) VALUES (?, ?, ?, ?, ?, ?)",
					params![
						product.id.to_string(),
						product.name,
						product.description,
						product.price,
						product.stock,
						product.created_at.to_rfc3339()
					]
				)?;
			}

			// Insert orders
			for order in &orders {
				tx.execute(
					"INSERT INTO orders (id, user_id, product_id, quantity, total_price, created_at) VALUES (?, ?, ?, ?, ?, ?)",
					params![
						order.id.to_string(),
						order.user_id.to_string(),
						order.product_id.to_string(),
						order.quantity,
						order.total_price,
						order.created_at.to_rfc3339()
					]
				)?;
			}

			// Commit the transaction
			tx.commit()?;

			Ok(())
		}).await?;

		Ok(())
	}

	async fn cleanup(&self) -> Result<()> {
		let conn = self.get_async_connection().await?;

		conn.call(|conn| {
			// Use a transaction for better performance
			let tx = conn.transaction()?;

			// Delete all data
			tx.execute("DELETE FROM orders", [])?;
			tx.execute("DELETE FROM products", [])?;
			tx.execute("DELETE FROM users", [])?;

			// Commit the transaction
			tx.commit()?;

			Ok(())
		}).await?;

		Ok(())
	}

	fn database_name(&self) -> String {
		"SQLite".to_string()
	}

	fn set_cpu_count(&mut self, count: usize) {
		self.cpu_count = count;
	}

	fn get_cpu_count(&self) -> usize {
		self.cpu_count
	}

	async fn insert_single_many_times(&self, count: usize) -> Result<BenchmarkResult> {
		let conn = self.get_async_connection().await?;

		measure_execution(
			&self.database_name(),
			"Insert Single Many Times",
			count,
			self.cpu_count,
			|| async {
				conn.call(move |conn| {
					for _ in 0..count {
						let user = generate_random_user();
						conn.execute(
							"INSERT INTO users (id, name, email, created_at, active) VALUES (?, ?, ?, ?, ?)",
							params![
								user.id.to_string(),
								user.name,
								user.email,
								user.created_at.to_rfc3339(),
								user.active as i32
							]
						)?;
					}
					Ok(())
				}).await.map_err(anyhow::Error::from)
			}
		).await
	}

	async fn insert_many_at_once(&self, count: usize) -> Result<BenchmarkResult> {
		let conn = self.get_async_connection().await?;

		measure_execution(
			&self.database_name(),
			"Insert Many At Once",
			count,
			self.cpu_count,
			|| async {
				// Generate users
				let users: Vec<User> = (0..count)
					.map(|_| generate_random_user())
					.collect();

				conn.call(move |conn| {
					let tx = conn.transaction()?;

					for user in &users {
						tx.execute(
							"INSERT INTO users (id, name, email, created_at, active) VALUES (?, ?, ?, ?, ?)",
							params![
								user.id.to_string(),
								user.name,
								user.email,
								user.created_at.to_rfc3339(),
								user.active as i32
							]
						)?;
					}

					tx.commit()?;
					Ok(())
				}).await.map_err(anyhow::Error::from)
			}
		).await
	}

	async fn read_by_id_many_times(&self, count: usize) -> Result<BenchmarkResult> {
		let conn = self.get_async_connection().await?;

		// First get a list of IDs to fetch
		let ids = conn
			.call(move |conn| {
				let mut stmt = conn.prepare("SELECT id FROM users LIMIT ?")?;
				let ids: Result<Vec<String>, _> = stmt
					.query_map([count], |row| row.get(0))?
					.collect();

				Ok(ids?)
			}).await
			.map_err(anyhow::Error::from)?;

		measure_execution(
			&self.database_name(),
			"Read By ID Many Times",
			count,
			self.cpu_count,
			|| async {
				let ids_clone = ids.clone();

				conn.call(move |conn| {
					for i in 0..count {
						let id = &ids_clone[i % ids_clone.len()];

						let _: Option<(String, String, String, String, bool)> = conn
							.query_row(
								"SELECT id, name, email, created_at, active FROM users WHERE id = ?",
								[id],
								|row| {
									Ok((
										row.get(0)?,
										row.get(1)?,
										row.get(2)?,
										row.get(3)?,
										row.get::<_, i32>(4)? == 1,
									))
								}
							)
							.optional()?;
					}

					Ok(())
				}).await.map_err(anyhow::Error::from)
			}
		).await
	}

	async fn read_many_by_ids(&self, count: usize) -> Result<BenchmarkResult> {
		let conn = self.get_async_connection().await?;

		// First get a list of IDs to fetch
		let ids = conn
			.call(move |conn| {
				let mut stmt = conn.prepare("SELECT id FROM users LIMIT ?")?;
				let ids: Result<Vec<String>, _> = stmt
					.query_map([count], |row| row.get(0))?
					.collect();

				Ok(ids?)
			}).await
			.map_err(anyhow::Error::from)?;

		measure_execution(
			&self.database_name(),
			"Read Many By IDs",
			count,
			self.cpu_count,
			|| async {
				let ids_clone = ids.clone();

				conn.call(move |conn| {
					// Build a query with all IDs
					// Note: This is not the most efficient way to do this in SQLite
					// A better approach would use a temporary table or multiple parameters
					// But this is a simple demonstration
					let placeholders = std::iter
						::repeat("?")
						.take(ids_clone.len())
						.collect::<Vec<_>>()
						.join(",");

					let query =
						format!("SELECT id, name, email, created_at, active FROM users WHERE id IN ({})", placeholders);

					let mut stmt = conn.prepare(&query)?;

					let params: Vec<&dyn rusqlite::ToSql> = ids_clone
						.iter()
						.map(|id| id as &dyn rusqlite::ToSql)
						.collect();

					let _rows = stmt.query_map(params.as_slice(), |row| {
						Ok((
							row.get::<_, String>(0)?,
							row.get::<_, String>(1)?,
							row.get::<_, String>(2)?,
							row.get::<_, String>(3)?,
							row.get::<_, i32>(4)? == 1,
						))
					})?;

					// We need to collect to actually process the rows
					let _results: Vec<_> = _rows.collect::<Result<Vec<_>, _>>()?;

					Ok(())
				}).await.map_err(anyhow::Error::from)
			}
		).await
	}

	async fn read_by_column_search(&self, count: usize) -> Result<BenchmarkResult> {
		let conn = self.get_async_connection().await?;

		measure_execution(
			&self.database_name(),
			"Read By Column Search",
			count,
			self.cpu_count,
			|| async {
				conn.call(move |conn| {
					let mut stmt = conn.prepare(
						"SELECT id, name, email, created_at, active FROM users WHERE email LIKE ? LIMIT ?"
					)?;

					let _results: Vec<_> = stmt
						.query_map(params!["%example.com%", count], |row| {
							Ok((
								row.get::<_, String>(0)?,
								row.get::<_, String>(1)?,
								row.get::<_, String>(2)?,
								row.get::<_, String>(3)?,
								row.get::<_, i32>(4)? == 1,
							))
						})?
						.collect::<Result<Vec<_>, _>>()?;

					Ok(())
				}).await.map_err(anyhow::Error::from)
			}
		).await
	}

	async fn read_with_one_join(&self, count: usize) -> Result<BenchmarkResult> {
		let conn = self.get_async_connection().await?;

		measure_execution(
			&self.database_name(),
			"Read With One Join",
			count,
			self.cpu_count,
			|| async {
				conn.call(move |conn| {
					let query =
						"
						SELECT o.id, o.quantity, o.total_price, o.created_at,
							   u.id, u.name, u.email, u.created_at, u.active
						FROM orders o
						JOIN users u ON o.user_id = u.id
						LIMIT ?
					";

					let mut stmt = conn.prepare(query)?;

					let _results: Vec<_> = stmt
						.query_map([count], |row| {
							Ok((
								// Order data
								row.get::<_, String>(0)?,
								row.get::<_, i32>(1)?,
								row.get::<_, f64>(2)?,
								row.get::<_, String>(3)?,
								// User data
								row.get::<_, String>(4)?,
								row.get::<_, String>(5)?,
								row.get::<_, String>(6)?,
								row.get::<_, String>(7)?,
								row.get::<_, i32>(8)? == 1,
							))
						})?
						.collect::<Result<Vec<_>, _>>()?;

					Ok(())
				}).await.map_err(anyhow::Error::from)
			}
		).await
	}

	async fn read_with_two_joins(&self, count: usize) -> Result<BenchmarkResult> {
		let conn = self.get_async_connection().await?;

		measure_execution(
			&self.database_name(),
			"Read With Two Joins",
			count,
			self.cpu_count,
			|| async {
				conn.call(move |conn| {
					let query =
						"
						SELECT o.id, o.quantity, o.total_price, o.created_at,
							   u.id, u.name, u.email, u.created_at, u.active,
							   p.id, p.name, p.description, p.price, p.stock, p.created_at
						FROM orders o
						JOIN users u ON o.user_id = u.id
						JOIN products p ON o.product_id = p.id
						LIMIT ?
					";

					let mut stmt = conn.prepare(query)?;

					let _results: Vec<_> = stmt
						.query_map([count], |row| {
							Ok((
								// Order data
								row.get::<_, String>(0)?,
								row.get::<_, i32>(1)?,
								row.get::<_, f64>(2)?,
								row.get::<_, String>(3)?,
								// User data
								row.get::<_, String>(4)?,
								row.get::<_, String>(5)?,
								row.get::<_, String>(6)?,
								row.get::<_, String>(7)?,
								row.get::<_, i32>(8)? == 1,
								// Product data
								row.get::<_, String>(9)?,
								row.get::<_, String>(10)?,
								row.get::<_, String>(11)?,
								row.get::<_, f64>(12)?,
								row.get::<_, i32>(13)?,
								row.get::<_, String>(14)?,
							))
						})?
						.collect::<Result<Vec<_>, _>>()?;

					Ok(())
				}).await.map_err(anyhow::Error::from)
			}
		).await
	}

	async fn update_single_field_one_entry(
		&self,
		count: usize
	) -> Result<BenchmarkResult> {
		let conn = self.get_async_connection().await?;

		// Get a random user ID to update
		let user_id = conn
			.call(
				|conn| -> Result<String, tokio_rusqlite::Error> {
					conn.query_row("SELECT id FROM users LIMIT 1", [], |row| {
						row.get::<_, String>(0)
					}).map_err(tokio_rusqlite::Error::from)
				}
			).await
			.map_err(anyhow::Error::from)?;

		measure_execution(
			&self.database_name(),
			"Update Single Field One Entry",
			count,
			self.cpu_count,
			|| async {
				let user_id_clone = user_id.clone();

				conn.call(move |conn| {
					for i in 0..count {
						conn.execute(
							"UPDATE users SET active = ? WHERE id = ?",
							params![i % 2 == 0, user_id_clone]
						)?;
					}

					Ok(())
				}).await.map_err(anyhow::Error::from)
			}
		).await
	}

	async fn update_single_field_many_entries(
		&self,
		count: usize
	) -> Result<BenchmarkResult> {
		let conn = self.get_async_connection().await?;

		measure_execution(
			&self.database_name(),
			"Update Single Field Many Entries",
			count,
			self.cpu_count,
			|| async {
				conn.call(move |conn| {
					conn.execute(
						"UPDATE users SET active = ? WHERE id IN (SELECT id FROM users LIMIT ?)",
						params![true, count]
					)?;

					Ok(())
				}).await.map_err(anyhow::Error::from)
			}
		).await
	}

	async fn update_multiple_fields_one_entry(
		&self,
		count: usize
	) -> Result<BenchmarkResult> {
		let conn = self.get_async_connection().await?;

		// Get a random product ID to update
		let product_id = conn
			.call(
				|conn| -> Result<String, tokio_rusqlite::Error> {
					conn.query_row("SELECT id FROM products LIMIT 1", [], |row| {
						row.get::<_, String>(0)
					}).map_err(tokio_rusqlite::Error::from)
				}
			).await
			.map_err(anyhow::Error::from)?;

		measure_execution(
			&self.database_name(),
			"Update Multiple Fields One Entry",
			count,
			self.cpu_count,
			|| async {
				let product_id_clone = product_id.clone();

				conn.call(move |conn| {
					for i in 0..count {
						let new_price = 10.0 + ((i as f64) % 100.0);
						let new_stock = 100 + (i % 50);

						conn.execute(
							"UPDATE products SET price = ?, stock = ?, description = ? WHERE id = ?",
							params![
								new_price,
								new_stock,
								format!("Updated description {}", i),
								product_id_clone
							]
						)?;
					}

					Ok(())
				}).await.map_err(anyhow::Error::from)
			}
		).await
	}

	async fn update_multiple_fields_many_entries(
		&self,
		count: usize
	) -> Result<BenchmarkResult> {
		let conn = self.get_async_connection().await?;

		measure_execution(
			&self.database_name(),
			"Update Multiple Fields Many Entries",
			count,
			self.cpu_count,
			|| async {
				conn.call(move |conn| {
					// Using a transaction for better performance
					let tx = conn.transaction()?;

					// Get product IDs to update
					let product_ids = {
						let mut stmt = tx.prepare("SELECT id FROM products LIMIT ?")?;
						let ids: Vec<String> = stmt
							.query_map([count], |row| row.get(0))?
							.collect::<Result<Vec<_>, _>>()?;
						ids
					};

					// Update each product with new values
					let update_time = chrono::Utc::now().to_rfc3339();

					for id in product_ids {
						tx.execute(
							"UPDATE products SET price = price * 1.1, stock = stock + 10, description = ?, created_at = ? WHERE id = ?",
							params![
								format!("Bulk updated description {}", Uuid::new_v4()),
								update_time,
								id
							]
						)?;
					}

					// Commit the transaction
					tx.commit()?;

					Ok(())
				}).await.map_err(anyhow::Error::from)
			}
		).await
	}
}

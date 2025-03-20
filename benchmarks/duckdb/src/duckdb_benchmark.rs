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
	models::{ BenchmarkResult, Order, OrderWithDetails, Product, User },
};
use duckdb::{ Connection, params };
use std::path::Path;
use std::sync::Arc;
use tokio::sync::Mutex;
use uuid::Uuid;

pub struct DuckdbBenchmark {
	pub db_path: String,
	cpu_count: usize,
	// We need a mutex to safely share the connection across async functions
	conn: Arc<Mutex<Connection>>,
}

impl DuckdbBenchmark {
	pub async fn new(cpu_count: usize) -> Result<Self> {
		let db_path = std::env
			::current_dir()?
			.join("data/duckdb-benchmark.db")
			.to_string_lossy()
			.to_string();

		println!("Database path: {}", db_path);

		// Create data directory if it doesn't exist
		let data_dir = Path::new("./data");
		if !data_dir.exists() {
			std::fs::create_dir_all(data_dir)?;
		}

		// Create the connection to DuckDB
		let conn = Connection::open(&db_path)?;

		// Configure DuckDB
		println!("Setting threads to {}", cpu_count);
		conn.execute(&format!("SET threads TO {}", cpu_count), [])?;

		// Enable parallel execution
		println!("Enabling object cache");
		conn.execute("PRAGMA enable_object_cache", [])?;

		// Set memory limit
		println!("Setting memory limit to 4GB");
		conn.execute("PRAGMA memory_limit='4GB'", [])?;

		// Wrap the connection in Arc<Mutex> for safe sharing
		let conn = Arc::new(Mutex::new(conn));

		// Create a new instance
		let benchmark = Self { db_path, cpu_count, conn };

		// Initialize database
		benchmark.init().await?;

		Ok(benchmark)
	}

	// Helper to run blocking database operations in a way that works with async/await
	async fn run_blocking<F, T>(&self, f: F) -> Result<T>
		where F: FnOnce(&mut Connection) -> Result<T> + Send + 'static, T: Send + 'static
	{
		let conn = self.conn.clone();
		let result = tokio::task::spawn_blocking(move || {
			let mut conn = conn.blocking_lock();
			f(&mut conn)
		}).await??;

		Ok(result)
	}
}

#[async_trait]
impl DatabaseBenchmark for DuckdbBenchmark {
	async fn init(&self) -> Result<()> {
		println!("Initializing database");
		let result = self.run_blocking(|conn| {
			// Create users table
			conn.execute(
				"CREATE TABLE IF NOT EXISTS users (
                    id VARCHAR,
                    name VARCHAR NOT NULL,
                    email VARCHAR NOT NULL,
                    created_at VARCHAR NOT NULL,
                    active BOOLEAN NOT NULL
                )",
				[]
			)?;

			// Create products table
			conn.execute(
				"CREATE TABLE IF NOT EXISTS products (
                    id VARCHAR,
                    name VARCHAR NOT NULL,
                    description VARCHAR NOT NULL,
                    price DOUBLE NOT NULL,
                    stock INTEGER NOT NULL,
                    created_at VARCHAR NOT NULL
                )",
				[]
			)?;

			// Create orders table with foreign keys
			conn.execute(
				"CREATE TABLE IF NOT EXISTS orders (
                    id VARCHAR,
                    user_id VARCHAR NOT NULL,
                    product_id VARCHAR NOT NULL,
                    quantity INTEGER NOT NULL,
                    total_price DOUBLE NOT NULL,
                    created_at VARCHAR NOT NULL,
                )",
				[]
			)?;

			Ok(())
		}).await;

		println!("Database initialized");
		result
	}

	async fn generate_test_data(&self, count: usize) -> Result<()> {
		println!("Generating test data for {} users", count);
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

		// Clone the collections for the async closure
		let users_clone = users.clone();
		let products_clone = products.clone();
		let orders_clone = orders.clone();

		self.run_blocking(move |conn| {
			// Use a transaction for better performance
			let tx = conn.transaction()?;

			// Insert users
			for user in &users_clone {
				tx.execute(
					"INSERT INTO users (id, name, email, created_at, active) VALUES (?, ?, ?, ?, ?)",
					params![
						user.id.to_string(),
						user.name,
						user.email,
						user.created_at.to_rfc3339(),
						user.active
					]
				)?;
			}

			// Insert products
			for product in &products_clone {
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
			for order in &orders_clone {
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

			tx.commit()?;
			Ok(())
		}).await
	}

	async fn cleanup(&self) -> Result<()> {
		self.run_blocking(|conn| {
			// Empty all tables first
			conn.execute("DELETE FROM orders", [])?;
			conn.execute("DELETE FROM products", [])?;
			conn.execute("DELETE FROM users", [])?;

			// Now drop the tables in correct order
			// conn.execute("DROP TABLE IF EXISTS orders", [])?;
			// conn.execute("DROP TABLE IF EXISTS products", [])?;
			// conn.execute("DROP TABLE IF EXISTS users", [])?;

			Ok(())
		}).await
	}

	fn database_name(&self) -> String {
		"DuckDB".to_string()
	}

	fn set_cpu_count(&mut self, count: usize) {
		println!("Setting CPU count to {}", count);
		self.cpu_count = count;
		let conn = self.conn.clone();
		// Update the thread count in DuckDB
		tokio::spawn(async move {
			let _ = tokio::task::spawn_blocking(move || {
				let conn = conn.blocking_lock();
				let _ = conn.execute(&format!("SET threads TO {}", count), []);
			}).await;
		});
	}

	fn get_cpu_count(&self) -> usize {
		self.cpu_count
	}

	async fn insert_single_many_times(&self, count: usize) -> Result<BenchmarkResult> {
		println!("Inserting {} users", count);
		measure_execution(
			&self.database_name(),
			"insert_single_many_times",
			count,
			self.cpu_count,
			|| async {
				let conn = self.conn.clone();

				tokio::task::spawn_blocking(move || {
					let mut conn = conn.blocking_lock();
					let tx = conn.transaction()?;

					for _ in 0..count {
						let user = generate_random_user();
						tx.execute(
							"INSERT INTO users (id, name, email, created_at, active) VALUES (?, ?, ?, ?, ?)",
							params![
								user.id.to_string(),
								user.name,
								user.email,
								user.created_at.to_rfc3339(),
								user.active
							]
						)?;
					}

					tx.commit()?;
					Ok(())
				}).await?
			}
		).await
	}

	async fn insert_many_at_once(&self, count: usize) -> Result<BenchmarkResult> {
		println!("Inserting {} products", count);
		measure_execution(
			&self.database_name(),
			"insert_many_at_once",
			count,
			self.cpu_count,
			|| async {
				// Generate products for insertion
				let products: Vec<Product> = (0..count)
					.map(|_| generate_random_product())
					.collect();

				let products_clone = products.clone();

				let conn = self.conn.clone();
				tokio::task::spawn_blocking(move || {
					let mut conn = conn.blocking_lock();

					// Start a transaction
					let tx = conn.transaction()?;

					// Prepare the statement outside the loop for better performance
					let mut stmt = tx.prepare(
						"INSERT INTO products (id, name, description, price, stock, created_at) VALUES (?, ?, ?, ?, ?, ?)"
					)?;

					// Insert all products
					for product in &products_clone {
						stmt.execute(
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

					// Commit the transaction
					tx.commit()?;
					Ok(())
				}).await?
			}
		).await
	}

	async fn read_by_id_many_times(&self, count: usize) -> Result<BenchmarkResult> {
		println!("Reading {} users", count);
		// First, get a list of user IDs to query
		let user_ids = self.run_blocking(move |conn| {
			let mut stmt = conn.prepare("SELECT id FROM users LIMIT ?")?;
			let user_ids: Vec<String> = stmt
				.query_map([count as i64], |row| row.get(0))?
				.collect::<Result<Vec<_>, _>>()
				.map_err(|e| anyhow::anyhow!(e))?;
			Ok(user_ids)
		}).await?;

		// If we don't have enough users, generate some test data
		if user_ids.len() < 10 {
			self.generate_test_data(100).await?;
		}

		// Get the user_ids again if needed
		let user_ids = if user_ids.is_empty() {
			self.run_blocking(move |conn| {
				let mut stmt = conn.prepare("SELECT id FROM users LIMIT ?")?;
				let user_ids: Vec<String> = stmt
					.query_map([count as i64], |row| row.get(0))?
					.collect::<Result<Vec<_>, _>>()
					.map_err(|e| anyhow::anyhow!(e))?;
				Ok(user_ids)
			}).await?
		} else {
			user_ids
		};

		measure_execution(
			&self.database_name(),
			"read_by_id_many_times",
			count,
			self.cpu_count,
			|| async {
				let conn = self.conn.clone();
				let user_ids = user_ids.clone(); // Clone for the closure

				tokio::task::spawn_blocking(move || {
					let conn = conn.blocking_lock();
					let mut stmt = conn.prepare("SELECT * FROM users WHERE id = ?")?;

					for i in 0..count {
						// Cycle through the available IDs
						let user_id = &user_ids[i % user_ids.len()];

						// Query the user
						let _user = stmt.query_row([user_id], |row| {
							Ok(User {
								id: Uuid::parse_str(&row.get::<_, String>(0)?).unwrap(),
								name: row.get(1)?,
								email: row.get(2)?,
								created_at: chrono::DateTime
									::parse_from_rfc3339(&row.get::<_, String>(3)?)
									.unwrap()
									.with_timezone(&chrono::Utc),
								active: row.get(4)?,
							})
						})?;
					}

					Ok(())
				}).await?
			}
		).await
	}

	async fn read_many_by_ids(&self, count: usize) -> Result<BenchmarkResult> {
		// First, get a batch of user IDs
		let mut user_ids = self.run_blocking(move |conn| {
			let mut stmt = conn.prepare("SELECT id FROM users LIMIT ?")?;
			let user_ids: Vec<String> = stmt
				.query_map([count as i64], |row| row.get(0))?
				.collect::<Result<Vec<_>, _>>()
				.map_err(|e| anyhow::anyhow!(e))?;
			Ok(user_ids)
		}).await?;

		// If we don't have enough users, generate some test data
		if user_ids.len() < 100 {
			self.generate_test_data(500).await?;

			// Get user_ids again
			user_ids = self.run_blocking(move |conn| {
				let mut stmt = conn.prepare("SELECT id FROM users LIMIT ?")?;
				let user_ids: Vec<String> = stmt
					.query_map([count as i64], |row| row.get(0))?
					.collect::<Result<Vec<_>, _>>()
					.map_err(|e| anyhow::anyhow!(e))?;
				Ok(user_ids)
			}).await?;
		}

		// Create batches of IDs
		let batch_size = 200;
		let num_batches = (count + batch_size - 1) / batch_size; // Ceiling division
		let batches: Vec<Vec<String>> = (0..num_batches)
			.map(|i| {
				let start = i * batch_size;
				let end = std::cmp::min(start + batch_size, user_ids.len());
				user_ids[start..end].to_vec()
			})
			.collect();

		measure_execution(
			&self.database_name(),
			"read_many_by_ids",
			count,
			self.cpu_count,
			|| async {
				let conn = self.conn.clone();
				let batches = batches.clone();

				tokio::task::spawn_blocking(move || {
					let conn = conn.blocking_lock();

					for batch in batches {
						// Create placeholders for the IN clause
						let placeholders = batch
							.iter()
							.enumerate()
							.map(|(i, _)| format!("?{}", i + 1))
							.collect::<Vec<String>>()
							.join(",");

						let query =
							format!("SELECT * FROM users WHERE id IN ({})", placeholders);

						// Convert batch to params format
						let param_values: Vec<_> = batch
							.iter()
							.map(|id| id.as_str())
							.collect();

						// Execute the query using named parameters
						let mut stmt = conn.prepare(&query)?;
						let _users = stmt
							.query_map(
								duckdb::params_from_iter(param_values.iter()),
								|row| {
									Ok(User {
										id: Uuid::parse_str(
											&row.get::<_, String>(0)?
										).unwrap(),
										name: row.get(1)?,
										email: row.get(2)?,
										created_at: chrono::DateTime
											::parse_from_rfc3339(
												&row.get::<_, String>(3)?
											)
											.unwrap()
											.with_timezone(&chrono::Utc),
										active: row.get(4)?,
									})
								}
							)?
							.collect::<Result<Vec<_>, _>>()?;
					}

					Ok(())
				}).await?
			}
		).await
	}

	async fn read_by_column_search(&self, count: usize) -> Result<BenchmarkResult> {
		measure_execution(
			&self.database_name(),
			"read_by_column_search",
			count,
			self.cpu_count,
			|| async {
				let conn = self.conn.clone();

				tokio::task::spawn_blocking(move || {
					let conn = conn.blocking_lock();

					// Get a list of different email domains to search for
					let mut stmt = conn.prepare(
						"SELECT DISTINCT substring(email FROM position('@' IN email) + 1) as domain FROM users LIMIT 50"
					)?;
					let domains: Vec<String> = stmt
						.query_map([], |row| row.get(0))?
						.collect::<Result<Vec<_>, _>>()
						.map_err(|e| anyhow::anyhow!(e))?;

					if domains.is_empty() {
						return Ok(()); // No data to search
					}

					// Perform searches for each domain
					let mut stmt = conn.prepare(
						"SELECT * FROM users WHERE email LIKE ?"
					)?;

					let iterations = count / domains.len() + 1; // Ensure we run enough iterations

					for _ in 0..iterations {
						for domain in &domains {
							let pattern = format!("%@{}", domain);
							let _users = stmt
								.query_map([pattern], |row| {
									Ok(User {
										id: Uuid::parse_str(
											&row.get::<_, String>(0)?
										).unwrap(),
										name: row.get(1)?,
										email: row.get(2)?,
										created_at: chrono::DateTime
											::parse_from_rfc3339(
												&row.get::<_, String>(3)?
											)
											.unwrap()
											.with_timezone(&chrono::Utc),
										active: row.get(4)?,
									})
								})?
								.collect::<Result<Vec<_>, _>>()?;

							if count <= iterations * domains.len() {
								break;
							}
						}
					}

					Ok(())
				}).await?
			}
		).await
	}

	async fn read_with_one_join(&self, count: usize) -> Result<BenchmarkResult> {
		measure_execution(
			&self.database_name(),
			"read_with_one_join",
			count,
			self.cpu_count,
			|| async {
				let conn = self.conn.clone();

				tokio::task::spawn_blocking(move || {
					let conn = conn.blocking_lock();

					// Get a batch of product IDs to query
					let mut stmt = conn.prepare("SELECT id FROM products LIMIT 100")?;
					let product_ids: Vec<String> = stmt
						.query_map([], |row| row.get(0))?
						.collect::<Result<Vec<_>, _>>()
						.map_err(|e| anyhow::anyhow!(e))?;

					if product_ids.is_empty() {
						return Ok(()); // No products to query
					}

					// Query with join between orders and products
					let query =
						"
                        SELECT o.*, p.name, p.description, p.price, p.stock, p.created_at 
                        FROM orders o
                        JOIN products p ON o.product_id = p.id
                        WHERE o.product_id = ?
                        LIMIT 100
                    ";

					let mut stmt = conn.prepare(query)?;

					let iterations = count / product_ids.len() + 1;

					for _ in 0..iterations {
						for product_id in &product_ids {
							let _results = stmt
								.query_map([product_id], |row| {
									let order_id = Uuid::parse_str(
										&row.get::<_, String>(0)?
									).unwrap();
									let user_id = Uuid::parse_str(
										&row.get::<_, String>(1)?
									).unwrap();
									let product_id = Uuid::parse_str(
										&row.get::<_, String>(2)?
									).unwrap();
									let quantity: i32 = row.get(3)?;
									let total_price: f64 = row.get(4)?;
									let order_created_at = chrono::DateTime
										::parse_from_rfc3339(&row.get::<_, String>(5)?)
										.unwrap()
										.with_timezone(&chrono::Utc);

									let product_name: String = row.get(6)?;
									let product_description: String = row.get(7)?;
									let product_price: f64 = row.get(8)?;
									let product_stock: i32 = row.get(9)?;
									let product_created_at = chrono::DateTime
										::parse_from_rfc3339(&row.get::<_, String>(10)?)
										.unwrap()
										.with_timezone(&chrono::Utc);

									let product = Product {
										id: product_id,
										name: product_name,
										description: product_description,
										price: product_price,
										stock: product_stock,
										created_at: product_created_at,
									};

									Ok((
										order_id,
										user_id,
										quantity,
										total_price,
										order_created_at,
										product,
									))
								})?
								.collect::<Result<Vec<_>, _>>()?;

							if count <= iterations * product_ids.len() {
								break;
							}
						}
					}

					Ok(())
				}).await?
			}
		).await
	}

	async fn read_with_two_joins(&self, count: usize) -> Result<BenchmarkResult> {
		measure_execution(
			&self.database_name(),
			"read_with_two_joins",
			count,
			self.cpu_count,
			|| async {
				let conn = self.conn.clone();

				tokio::task::spawn_blocking(move || {
					let conn = conn.blocking_lock();

					// Get a batch of order IDs to query
					let mut stmt = conn.prepare("SELECT id FROM orders LIMIT 100")?;
					let order_ids: Vec<String> = stmt
						.query_map([], |row| row.get(0))?
						.collect::<Result<Vec<_>, _>>()
						.map_err(|e| anyhow::anyhow!(e))?;

					if order_ids.is_empty() {
						return Ok(()); // No orders to query
					}

					// Query with two joins: orders → users and orders → products
					let query =
						"
                        SELECT 
                            o.id, o.quantity, o.total_price, o.created_at,
                            u.id, u.name, u.email, u.created_at, u.active,
                            p.id, p.name, p.description, p.price, p.stock, p.created_at
                        FROM orders o
                        JOIN users u ON o.user_id = u.id
                        JOIN products p ON o.product_id = p.id
                        WHERE o.id = ?
                    ";

					let mut stmt = conn.prepare(query)?;

					let iterations = count / order_ids.len() + 1;

					for _ in 0..iterations {
						for order_id in &order_ids {
							let _results = stmt
								.query_map([order_id], |row| {
									let order_id = Uuid::parse_str(
										&row.get::<_, String>(0)?
									).unwrap();
									let quantity: i32 = row.get(1)?;
									let total_price: f64 = row.get(2)?;
									let order_created_at = chrono::DateTime
										::parse_from_rfc3339(&row.get::<_, String>(3)?)
										.unwrap()
										.with_timezone(&chrono::Utc);

									let user_id = Uuid::parse_str(
										&row.get::<_, String>(4)?
									).unwrap();
									let user_name: String = row.get(5)?;
									let user_email: String = row.get(6)?;
									let user_created_at = chrono::DateTime
										::parse_from_rfc3339(&row.get::<_, String>(7)?)
										.unwrap()
										.with_timezone(&chrono::Utc);
									let user_active: bool = row.get(8)?;

									let product_id = Uuid::parse_str(
										&row.get::<_, String>(9)?
									).unwrap();
									let product_name: String = row.get(10)?;
									let product_description: String = row.get(11)?;
									let product_price: f64 = row.get(12)?;
									let product_stock: i32 = row.get(13)?;
									let product_created_at = chrono::DateTime
										::parse_from_rfc3339(&row.get::<_, String>(14)?)
										.unwrap()
										.with_timezone(&chrono::Utc);

									let user = User {
										id: user_id,
										name: user_name,
										email: user_email,
										created_at: user_created_at,
										active: user_active,
									};

									let product = Product {
										id: product_id,
										name: product_name,
										description: product_description,
										price: product_price,
										stock: product_stock,
										created_at: product_created_at,
									};

									let order_with_details = OrderWithDetails {
										id: order_id,
										quantity,
										total_price,
										created_at: order_created_at,
										user,
										product,
									};

									Ok(order_with_details)
								})?
								.collect::<Result<Vec<_>, _>>()?;

							if count <= iterations * order_ids.len() {
								break;
							}
						}
					}

					Ok(())
				}).await?
			}
		).await
	}

	async fn update_single_field_one_entry(
		&self,
		count: usize
	) -> Result<BenchmarkResult> {
		// First, get a product ID to update
		let product_id = self.run_blocking(|conn| {
			let id = conn
				.query_row("SELECT id FROM products LIMIT 1", [], |row|
					row.get::<_, String>(0)
				)
				.map_err(|e| anyhow::anyhow!(e))?;
			Ok(id)
		}).await?;

		measure_execution(
			&self.database_name(),
			"update_single_field_one_entry",
			count,
			self.cpu_count,
			|| async {
				let conn = self.conn.clone();
				let product_id = product_id.clone();

				tokio::task::spawn_blocking(move || {
					let conn = conn.blocking_lock();

					for i in 0..count {
						// Update the same product many times, changing its stock
						let new_stock = (i as i32) % 1000;

						conn.execute(
							"UPDATE products SET stock = ? WHERE id = ?",
							params![new_stock, product_id]
						)?;
					}

					Ok(())
				}).await?
			}
		).await
	}

	async fn update_single_field_many_entries(
		&self,
		count: usize
	) -> Result<BenchmarkResult> {
		measure_execution(
			&self.database_name(),
			"update_single_field_many_entries",
			count,
			self.cpu_count,
			|| async {
				let conn = self.conn.clone();

				tokio::task::spawn_blocking(move || {
					let mut conn = conn.blocking_lock();

					// Get product IDs for updating
					let mut stmt = conn.prepare("SELECT id FROM products LIMIT ?")?;
					let products: Vec<String> = stmt
						.query_map([count as i64], |row| row.get(0))?
						.collect::<Result<Vec<_>, _>>()
						.map_err(|e| anyhow::anyhow!(e))?;

					if products.is_empty() {
						return Ok(()); // No products to update
					}

					// Start a transaction for better performance
					let tx = conn.transaction()?;

					for (i, product_id) in products.iter().enumerate() {
						// Update each product's stock
						let new_stock = ((i as i32) % 1000) + 1;

						tx.execute(
							"UPDATE products SET stock = ? WHERE id = ?",
							params![new_stock, product_id]
						)?;
					}

					tx.commit()?;
					Ok(())
				}).await?
			}
		).await
	}

	async fn update_multiple_fields_one_entry(
		&self,
		count: usize
	) -> Result<BenchmarkResult> {
		// Get a user to update
		let user_id = self.run_blocking(|conn| {
			let id = conn
				.query_row("SELECT id FROM users LIMIT 1", [], |row|
					row.get::<_, String>(0)
				)
				.map_err(|e| anyhow::anyhow!(e))?;
			Ok(id)
		}).await?;

		measure_execution(
			&self.database_name(),
			"update_multiple_fields_one_entry",
			count,
			self.cpu_count,
			|| async {
				let conn = self.conn.clone();
				let user_id = user_id.clone();

				tokio::task::spawn_blocking(move || {
					let conn = conn.blocking_lock();

					for i in 0..count {
						// Update multiple fields of the same user
						let new_name = format!("Updated User {}", i);
						let new_email = format!("updated{}@example.com", i);
						let new_active = i % 2 == 0;

						conn.execute(
							"UPDATE users SET name = ?, email = ?, active = ? WHERE id = ?",
							params![new_name, new_email, new_active, user_id]
						)?;
					}

					Ok(())
				}).await?
			}
		).await
	}

	async fn update_multiple_fields_many_entries(
		&self,
		count: usize
	) -> Result<BenchmarkResult> {
		measure_execution(
			&self.database_name(),
			"update_multiple_fields_many_entries",
			count,
			self.cpu_count,
			|| async {
				let conn = self.conn.clone();

				tokio::task::spawn_blocking(move || {
					let mut conn = conn.blocking_lock();

					// Get a batch of order IDs to update
					let mut stmt = conn.prepare("SELECT id FROM orders LIMIT ?")?;
					let orders: Vec<String> = stmt
						.query_map([count as i64], |row| row.get(0))?
						.collect::<Result<Vec<_>, _>>()
						.map_err(|e| anyhow::anyhow!(e))?;

					if orders.is_empty() {
						return Ok(()); // No orders to update
					}

					// Start a transaction for better performance
					let tx = conn.transaction()?;

					for (i, order_id) in orders.iter().enumerate() {
						// Update quantity and total_price
						let new_quantity = ((i as i32) % 10) + 1;
						let new_total_price =
							(new_quantity as f64) * 9.99 + ((i as f64) % 10.0);

						tx.execute(
							"UPDATE orders SET quantity = ?, total_price = ? WHERE id = ?",
							params![new_quantity, new_total_price, order_id]
						)?;
					}

					tx.commit()?;
					Ok(())
				}).await?
			}
		).await
	}
}

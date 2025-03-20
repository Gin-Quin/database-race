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
	models::{ BenchmarkResult, Order, Product, User, OrderWithDetails },
};
use rocksdb::{ DB, ColumnFamilyDescriptor, Options, IteratorMode, WriteBatch };
use std::path::Path;
use std::sync::Arc;
use tokio::sync::Mutex;
use serde::{ Serialize, Deserialize };
use uuid::Uuid;
use bincode;

const USERS_CF: &str = "users";
const PRODUCTS_CF: &str = "products";
const ORDERS_CF: &str = "orders";
const USERS_EMAIL_INDEX_CF: &str = "users_email_index";
const PRODUCTS_NAME_INDEX_CF: &str = "products_name_index";
const ORDERS_USER_ID_INDEX_CF: &str = "orders_user_id_index";
const ORDERS_PRODUCT_ID_INDEX_CF: &str = "orders_product_id_index";

pub struct RocksDBBenchmark {
	db: Arc<Mutex<DB>>,
	db_path: String,
	cpu_count: usize,
}

impl RocksDBBenchmark {
	pub async fn new(cpu_count: usize) -> Result<Self> {
		let db_path = "./data/rocksdb-benchmark";

		// Create data directory if it doesn't exist
		let data_dir = Path::new("./data");
		if !data_dir.exists() {
			std::fs::create_dir_all(data_dir)?;
		}

		// Create DB options
		let mut opts = Options::default();
		opts.create_if_missing(true);
		opts.increase_parallelism(cpu_count as i32);
		opts.set_max_background_jobs(4);
		opts.set_compression_type(rocksdb::DBCompressionType::Lz4);

		// Define column families
		let cf_names = vec![
			USERS_CF,
			PRODUCTS_CF,
			ORDERS_CF,
			USERS_EMAIL_INDEX_CF,
			PRODUCTS_NAME_INDEX_CF,
			ORDERS_USER_ID_INDEX_CF,
			ORDERS_PRODUCT_ID_INDEX_CF
		];

		let cf_descriptors: Vec<ColumnFamilyDescriptor> = cf_names
			.iter()
			.map(|name| {
				let mut cf_opts = Options::default();
				cf_opts.set_max_write_buffer_number(4);
				cf_opts.set_target_file_size_base(64 * 1024 * 1024); // 64MB
				cf_opts.set_level_compaction_dynamic_level_bytes(true);

				ColumnFamilyDescriptor::new(*name, cf_opts)
			})
			.collect();

		// Try to open DB with all CFs, if it doesn't exist, create it
		let db = match DB::open_cf_descriptors(&opts, &db_path, cf_descriptors) {
			Ok(db) => db,
			Err(_) => {
				// Create DB with default column family
				let db = DB::open(&opts, &db_path)?;

				// Create all column families
				for cf_name in cf_names {
					let mut cf_opts = Options::default();
					cf_opts.set_max_write_buffer_number(4);
					cf_opts.set_target_file_size_base(64 * 1024 * 1024); // 64MB
					cf_opts.set_level_compaction_dynamic_level_bytes(true);

					db.create_cf(cf_name, &cf_opts)?;
				}
				db
			}
		};

		Ok(Self {
			db: Arc::new(Mutex::new(db)),
			db_path: db_path.to_string(),
			cpu_count,
		})
	}

	// Helper functions to serialize and deserialize data
	fn serialize<T: Serialize>(value: &T) -> Result<Vec<u8>> {
		Ok(bincode::serialize(value)?)
	}

	fn deserialize<'a, T: Deserialize<'a>>(bytes: &'a [u8]) -> Result<T> {
		Ok(bincode::deserialize(bytes)?)
	}
}

#[async_trait]
impl DatabaseBenchmark for RocksDBBenchmark {
	async fn init(&self) -> Result<()> {
		// No schema setup needed for RocksDB as it's a key-value store
		// Column families are already created in the constructor
		Ok(())
	}

	async fn generate_test_data(&self, count: usize) -> Result<()> {
		let db = self.db.lock().await;

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

		// Get column family handles
		let users_cf = db.cf_handle(USERS_CF).unwrap();
		let products_cf = db.cf_handle(PRODUCTS_CF).unwrap();
		let orders_cf = db.cf_handle(ORDERS_CF).unwrap();
		let users_email_index_cf = db.cf_handle(USERS_EMAIL_INDEX_CF).unwrap();
		let products_name_index_cf = db.cf_handle(PRODUCTS_NAME_INDEX_CF).unwrap();
		let orders_user_id_index_cf = db.cf_handle(ORDERS_USER_ID_INDEX_CF).unwrap();
		let orders_product_id_index_cf = db
			.cf_handle(ORDERS_PRODUCT_ID_INDEX_CF)
			.unwrap();

		// Create a write batch for better performance
		let mut batch = WriteBatch::default();

		// Insert users and create email index
		for user in &users {
			let key = user.id.to_string();
			let value = Self::serialize(user)?;
			batch.put_cf(&users_cf, key.as_bytes(), &value);

			// Email index
			batch.put_cf(
				&users_email_index_cf,
				format!("{}:{}", user.email, user.id).as_bytes(),
				&[]
			);
		}

		// Insert products and create name index
		for product in &products {
			let key = product.id.to_string();
			let value = Self::serialize(product)?;
			batch.put_cf(&products_cf, key.as_bytes(), &value);

			// Name index
			batch.put_cf(
				&products_name_index_cf,
				format!("{}:{}", product.name, product.id).as_bytes(),
				&[]
			);
		}

		// Insert orders and create indexes
		for order in &orders {
			let key = order.id.to_string();
			let value = Self::serialize(order)?;
			batch.put_cf(&orders_cf, key.as_bytes(), &value);

			// User ID index
			batch.put_cf(
				&orders_user_id_index_cf,
				format!("{}:{}", order.user_id, order.id).as_bytes(),
				&[]
			);

			// Product ID index
			batch.put_cf(
				&orders_product_id_index_cf,
				format!("{}:{}", order.product_id, order.id).as_bytes(),
				&[]
			);
		}

		// Write all data at once
		db.write(batch)?;

		Ok(())
	}

	async fn cleanup(&self) -> Result<()> {
		let db = self.db.lock().await;

		// Clear all column families
		let cf_names = vec![
			USERS_CF,
			PRODUCTS_CF,
			ORDERS_CF,
			USERS_EMAIL_INDEX_CF,
			PRODUCTS_NAME_INDEX_CF,
			ORDERS_USER_ID_INDEX_CF,
			ORDERS_PRODUCT_ID_INDEX_CF
		];

		for cf_name in cf_names {
			let cf = db.cf_handle(cf_name).unwrap();

			// Iterate over all keys and delete them
			let iter = db.iterator_cf(&cf, IteratorMode::Start);
			let mut batch = WriteBatch::default();

			for result in iter {
				let (key, _) = result?;
				batch.delete_cf(&cf, &key);
			}

			db.write(batch)?;
		}

		Ok(())
	}

	fn database_name(&self) -> String {
		"RocksDB".to_string()
	}

	fn set_cpu_count(&mut self, count: usize) {
		self.cpu_count = count;
	}

	fn get_cpu_count(&self) -> usize {
		self.cpu_count
	}

	async fn insert_single_many_times(&self, count: usize) -> Result<BenchmarkResult> {
		let db_arc = self.db.clone();

		measure_execution(
			&self.database_name(),
			"Insert Single Many Times",
			count,
			self.cpu_count,
			|| async {
				let db = db_arc.lock().await;
				let users_cf = db.cf_handle(USERS_CF).unwrap();
				let users_email_index_cf = db.cf_handle(USERS_EMAIL_INDEX_CF).unwrap();

				for _ in 0..count {
					let user = generate_random_user();
					let key = user.id.to_string();
					let value = Self::serialize(&user)?;

					// Insert user
					db.put_cf(&users_cf, key.as_bytes(), &value)?;

					// Email index
					db.put_cf(
						&users_email_index_cf,
						format!("{}:{}", user.email, user.id).as_bytes(),
						&[]
					)?;
				}

				Ok(())
			}
		).await
	}

	async fn insert_many_at_once(&self, count: usize) -> Result<BenchmarkResult> {
		let db_arc = self.db.clone();

		measure_execution(
			&self.database_name(),
			"Insert Many At Once",
			count,
			self.cpu_count,
			|| async {
				let users: Vec<User> = (0..count)
					.map(|_| generate_random_user())
					.collect();

				let db = db_arc.lock().await;
				let users_cf = db.cf_handle(USERS_CF).unwrap();
				let users_email_index_cf = db.cf_handle(USERS_EMAIL_INDEX_CF).unwrap();

				let mut batch = WriteBatch::default();

				for user in &users {
					let key = user.id.to_string();
					let value = Self::serialize(user)?;

					// Insert user
					batch.put_cf(&users_cf, key.as_bytes(), &value);

					// Email index
					batch.put_cf(
						&users_email_index_cf,
						format!("{}:{}", user.email, user.id).as_bytes(),
						&[]
					);
				}

				db.write(batch)?;

				Ok(())
			}
		).await
	}

	async fn read_by_id_many_times(&self, count: usize) -> Result<BenchmarkResult> {
		let db_arc = self.db.clone();

		// First get a list of IDs to fetch
		let db = db_arc.lock().await;
		let mut ids = Vec::with_capacity(count);

		{
			let users_cf = db.cf_handle(USERS_CF).unwrap();
			let iter = db.iterator_cf(&users_cf, IteratorMode::Start);

			for (i, result) in iter.enumerate() {
				if i >= count {
					break;
				}

				let (key, _) = result?;
				ids.push(String::from_utf8(key.to_vec())?);
			}
		}

		drop(db); // Release the lock

		measure_execution(
			&self.database_name(),
			"Read By ID Many Times",
			count,
			self.cpu_count,
			|| async {
				let db = db_arc.lock().await;
				let users_cf = db.cf_handle(USERS_CF).unwrap();

				for i in 0..count {
					let id = &ids[i % ids.len()];

					let value = db.get_cf(&users_cf, id.as_bytes())?;

					if let Some(bytes) = value {
						let _user: User = Self::deserialize(&bytes)?;
					}
				}

				Ok(())
			}
		).await
	}

	async fn read_many_by_ids(&self, count: usize) -> Result<BenchmarkResult> {
		let db_arc = self.db.clone();

		// First get a list of IDs to fetch
		let db = db_arc.lock().await;
		let mut ids = Vec::with_capacity(count);

		{
			let users_cf = db.cf_handle(USERS_CF).unwrap();
			let iter = db.iterator_cf(&users_cf, IteratorMode::Start);

			for (i, result) in iter.enumerate() {
				if i >= count {
					break;
				}

				let (key, _) = result?;
				ids.push(String::from_utf8(key.to_vec())?);
			}
		}

		drop(db); // Release the lock

		measure_execution(
			&self.database_name(),
			"Read Many By IDs",
			count,
			self.cpu_count,
			|| async {
				let db = db_arc.lock().await;
				let users_cf = db.cf_handle(USERS_CF).unwrap();

				let mut users = Vec::with_capacity(ids.len());

				for id in &ids {
					let value = db.get_cf(&users_cf, id.as_bytes())?;

					if let Some(bytes) = value {
						let user: User = Self::deserialize(&bytes)?;
						users.push(user);
					}
				}

				Ok(())
			}
		).await
	}

	async fn read_by_column_search(&self, count: usize) -> Result<BenchmarkResult> {
		let db_arc = self.db.clone();

		measure_execution(
			&self.database_name(),
			"Read By Column Search",
			count,
			self.cpu_count,
			|| async {
				let db = db_arc.lock().await;
				let users_email_index_cf = db.cf_handle(USERS_EMAIL_INDEX_CF).unwrap();
				let users_cf = db.cf_handle(USERS_CF).unwrap();

				// Scan through email index
				let iter = db.iterator_cf(&users_email_index_cf, IteratorMode::Start);
				let mut users = Vec::with_capacity(count);

				for (i, result) in iter.enumerate() {
					if i >= count {
						break;
					}

					let (key, _) = result?;
					let key_str = String::from_utf8(key.to_vec())?;

					// Extract user ID from the index key (format: "email:id")
					if key_str.contains("example.com") {
						let user_id = key_str.split(':').nth(1).unwrap_or_default();

						let value = db.get_cf(&users_cf, user_id.as_bytes())?;

						if let Some(bytes) = value {
							let user: User = Self::deserialize(&bytes)?;
							users.push(user);

							if users.len() >= count {
								break;
							}
						}
					}
				}

				Ok(())
			}
		).await
	}

	async fn read_with_one_join(&self, count: usize) -> Result<BenchmarkResult> {
		let db_arc = self.db.clone();

		measure_execution(
			&self.database_name(),
			"Read With One Join",
			count,
			self.cpu_count,
			|| async {
				let db = db_arc.lock().await;
				let orders_cf = db.cf_handle(ORDERS_CF).unwrap();
				let users_cf = db.cf_handle(USERS_CF).unwrap();

				// Get orders
				let iter = db.iterator_cf(&orders_cf, IteratorMode::Start);
				let mut results = Vec::with_capacity(count);

				for (i, result) in iter.enumerate() {
					if i >= count {
						break;
					}

					let (_, value) = result?;
					let order: Order = Self::deserialize(&value)?;

					// Get the associated user (this is the "join")
					let user_key = order.user_id.to_string();
					let user_value = db.get_cf(&users_cf, user_key.as_bytes())?;

					if let Some(user_bytes) = user_value {
						let user: User = Self::deserialize(&user_bytes)?;

						// Combine order and user (similar to a join result)
						results.push((order, user));
					}
				}

				Ok(())
			}
		).await
	}

	async fn read_with_two_joins(&self, count: usize) -> Result<BenchmarkResult> {
		let db_arc = self.db.clone();

		measure_execution(
			&self.database_name(),
			"Read With Two Joins",
			count,
			self.cpu_count,
			|| async {
				let db = db_arc.lock().await;
				let orders_cf = db.cf_handle(ORDERS_CF).unwrap();
				let users_cf = db.cf_handle(USERS_CF).unwrap();
				let products_cf = db.cf_handle(PRODUCTS_CF).unwrap();

				// Get orders
				let iter = db.iterator_cf(&orders_cf, IteratorMode::Start);
				let mut results = Vec::with_capacity(count);

				for (i, result) in iter.enumerate() {
					if i >= count {
						break;
					}

					let (_, value) = result?;
					let order: Order = Self::deserialize(&value)?;

					// Get the associated user (first "join")
					let user_key = order.user_id.to_string();
					let user_value = db.get_cf(&users_cf, user_key.as_bytes())?;

					// Get the associated product (second "join")
					let product_key = order.product_id.to_string();
					let product_value = db.get_cf(&products_cf, product_key.as_bytes())?;

					if
						let (Some(user_bytes), Some(product_bytes)) = (
							user_value,
							product_value,
						)
					{
						let user: User = Self::deserialize(&user_bytes)?;
						let product: Product = Self::deserialize(&product_bytes)?;

						// Combine order, user, and product (similar to a join result)
						results.push(OrderWithDetails {
							id: order.id,
							quantity: order.quantity,
							total_price: order.total_price,
							created_at: order.created_at,
							user,
							product,
						});
					}
				}

				Ok(())
			}
		).await
	}

	async fn update_single_field_one_entry(
		&self,
		count: usize
	) -> Result<BenchmarkResult> {
		let db_arc = self.db.clone();

		// Get a random user ID to update
		let db = db_arc.lock().await;
		let user_id;

		{
			let users_cf = db.cf_handle(USERS_CF).unwrap();
			let iter = db.iterator_cf(&users_cf, IteratorMode::Start);

			user_id = match iter.take(1).next() {
				Some(Ok((key, _))) => String::from_utf8(key.to_vec())?,
				_ => {
					return Err(anyhow::anyhow!("No users found for update"));
				}
			};
		}

		drop(db); // Release the lock

		measure_execution(
			&self.database_name(),
			"Update Single Field One Entry",
			count,
			self.cpu_count,
			|| async {
				let db = db_arc.lock().await;
				let users_cf = db.cf_handle(USERS_CF).unwrap();

				for i in 0..count {
					// Read the user
					let value = db.get_cf(&users_cf, user_id.as_bytes())?;

					if let Some(bytes) = value {
						let mut user: User = Self::deserialize(&bytes)?;

						// Update the active field
						user.active = i % 2 == 0;

						// Write back
						db.put_cf(
							&users_cf,
							user_id.as_bytes(),
							Self::serialize(&user)?
						)?;
					}
				}

				Ok(())
			}
		).await
	}

	async fn update_single_field_many_entries(
		&self,
		count: usize
	) -> Result<BenchmarkResult> {
		let db_arc = self.db.clone();

		// Get user IDs to update
		let db = db_arc.lock().await;
		let mut user_ids = Vec::with_capacity(count);

		{
			let users_cf = db.cf_handle(USERS_CF).unwrap();
			let iter = db.iterator_cf(&users_cf, IteratorMode::Start);

			for (i, result) in iter.enumerate() {
				if i >= count {
					break;
				}

				let (key, _) = result?;
				user_ids.push(String::from_utf8(key.to_vec())?);
			}
		}

		drop(db); // Release the lock

		measure_execution(
			&self.database_name(),
			"Update Single Field Many Entries",
			count,
			self.cpu_count,
			|| async {
				let db = db_arc.lock().await;
				let users_cf = db.cf_handle(USERS_CF).unwrap();
				let mut batch = WriteBatch::default();

				for user_id in &user_ids {
					// Read the user
					let value = db.get_cf(&users_cf, user_id.as_bytes())?;

					if let Some(bytes) = value {
						let mut user: User = Self::deserialize(&bytes)?;

						// Update the active field
						user.active = true;

						// Add to batch
						batch.put_cf(
							&users_cf,
							user_id.as_bytes(),
							Self::serialize(&user)?
						);
					}
				}

				// Write all updates at once
				db.write(batch)?;

				Ok(())
			}
		).await
	}

	async fn update_multiple_fields_one_entry(
		&self,
		count: usize
	) -> Result<BenchmarkResult> {
		let db_arc = self.db.clone();

		// Get a random product ID to update
		let db = db_arc.lock().await;
		let product_id;

		{
			let products_cf = db.cf_handle(PRODUCTS_CF).unwrap();
			let iter = db.iterator_cf(&products_cf, IteratorMode::Start);

			product_id = match iter.take(1).next() {
				Some(Ok((key, _))) => String::from_utf8(key.to_vec())?,
				_ => {
					return Err(anyhow::anyhow!("No products found for update"));
				}
			};
		}

		drop(db); // Release the lock

		measure_execution(
			&self.database_name(),
			"Update Multiple Fields One Entry",
			count,
			self.cpu_count,
			|| async {
				let db = db_arc.lock().await;
				let products_cf = db.cf_handle(PRODUCTS_CF).unwrap();

				for i in 0..count {
					// Read the product
					let value = db.get_cf(&products_cf, product_id.as_bytes())?;

					if let Some(bytes) = value {
						let mut product: Product = Self::deserialize(&bytes)?;

						// Update multiple fields
						product.price = 10.0 + ((i as f64) % 100.0);
						product.stock = 100 + ((i as i32) % 50);
						product.description = format!("Updated description {}", i);

						// Write back
						db.put_cf(
							&products_cf,
							product_id.as_bytes(),
							Self::serialize(&product)?
						)?;
					}
				}

				Ok(())
			}
		).await
	}

	async fn update_multiple_fields_many_entries(
		&self,
		count: usize
	) -> Result<BenchmarkResult> {
		let db_arc = self.db.clone();

		// Get product IDs to update
		let db = db_arc.lock().await;
		let mut product_ids = Vec::with_capacity(count);

		{
			let products_cf = db.cf_handle(PRODUCTS_CF).unwrap();
			let iter = db.iterator_cf(&products_cf, IteratorMode::Start);

			for (i, result) in iter.enumerate() {
				if i >= count {
					break;
				}

				let (key, _) = result?;
				product_ids.push(String::from_utf8(key.to_vec())?);
			}
		}

		drop(db); // Release the lock

		measure_execution(
			&self.database_name(),
			"Update Multiple Fields Many Entries",
			count,
			self.cpu_count,
			|| async {
				let db = db_arc.lock().await;
				let products_cf = db.cf_handle(PRODUCTS_CF).unwrap();
				let mut batch = WriteBatch::default();

				let update_time = chrono::Utc::now();

				for product_id in &product_ids {
					// Read the product
					let value = db.get_cf(&products_cf, product_id.as_bytes())?;

					if let Some(bytes) = value {
						let mut product: Product = Self::deserialize(&bytes)?;

						// Update multiple fields
						product.price *= 1.1;
						product.stock += 10;
						product.description = format!(
							"Bulk updated description {}",
							Uuid::new_v4()
						);
						product.created_at = update_time;

						// Add to batch
						batch.put_cf(
							&products_cf,
							product_id.as_bytes(),
							Self::serialize(&product)?
						);
					}
				}

				// Write all updates at once
				db.write(batch)?;

				Ok(())
			}
		).await
	}
}

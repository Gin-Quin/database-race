use axum::{ routing::get, Router, Json, http::StatusCode, extract::State };
use std::sync::{ Arc, Mutex };
use tokio::net::TcpListener;
use anyhow::Result;
use std::net::SocketAddr;
use tracing::{ info, error };

use crate::{ models::BenchmarkResults, benchmark::DatabaseBenchmark };

// Shared state for the API
pub struct AppState<T: DatabaseBenchmark + Send + Sync + 'static> {
	pub benchmark: Arc<T>,
	pub results: Mutex<Option<BenchmarkResults>>,
}

// Run the API server with the provided benchmark implementation
pub async fn run_server<T: DatabaseBenchmark + Send + Sync + 'static>(
	benchmark: T,
	port: u16
) -> Result<()> {
	// Initialize tracing
	tracing_subscriber::fmt::init();

	// Create shared state
	let state = Arc::new(AppState {
		benchmark: Arc::new(benchmark),
		results: Mutex::new(None),
	});

	// Build our router
	let app = Router::new()
		.route("/", get(root_handler))
		.route("/results", get(results_handler::<T>))
		.route("/run", get(run_benchmark_handler::<T>))
		.with_state(state);

	// Run the server
	let addr = SocketAddr::from(([0, 0, 0, 0], port));
	info!("Server listening on {}", addr);

	let listener = TcpListener::bind(addr).await?;
	axum::serve(listener, app).await?;

	Ok(())
}

// Root handler
async fn root_handler() -> &'static str {
	"Database Benchmark API. Use /run to run benchmarks and /results to view results."
}

// Run benchmarks handler
async fn run_benchmark_handler<T: DatabaseBenchmark + Send + Sync + 'static>(State(
	state,
): State<Arc<AppState<T>>>) -> Result<Json<BenchmarkResults>, StatusCode> {
	info!("Running benchmark handler");
	// Initialize the database
	state.benchmark.init().await.map_err(|e| {
		error!("Database initialization failed: {:?}", e);
		StatusCode::INTERNAL_SERVER_ERROR
	})?;

	// Clean up previous data
	info!("Cleaning up previous data");
	state.benchmark.cleanup().await.map_err(|e| {
		error!("Cleanup failed: {:?}", e);
		StatusCode::INTERNAL_SERVER_ERROR
	})?;

	// Generate test data - 1000 records of each type
	info!("Generating test data");
	state.benchmark.generate_test_data(1000).await.map_err(|e| {
		error!("Test data generation failed: {:?}", e);
		StatusCode::INTERNAL_SERVER_ERROR
	})?;

	// Run all benchmarks with 1000 operations each
	info!("Running all benchmarks");
	let results = state.benchmark.run_all_benchmarks().await.map_err(|e| {
		error!("Benchmark execution failed: {:?}", e);
		StatusCode::INTERNAL_SERVER_ERROR
	})?;

	// Store the results
	info!("Storing results");
	{
		let mut results_lock = state.results.lock().unwrap();
		*results_lock = Some(results.clone());
	}

	info!("Results stored");
	Ok(Json(results))
}

// Results handler
async fn results_handler<T: DatabaseBenchmark + Send + Sync + 'static>(State(
	state,
): State<Arc<AppState<T>>>) -> Result<Json<BenchmarkResults>, StatusCode> {
	info!("Results handler");
	let results_lock = state.results.lock().unwrap();

	if let Some(results) = &*results_lock {
		Ok(Json(results.clone()))
	} else {
		Err(StatusCode::NOT_FOUND)
	}
}

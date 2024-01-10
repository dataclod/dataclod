use tikv_jemallocator::Jemalloc;

#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

const DEFAULT_CORE_PORT: &str = "9973";

#[tokio::main]
async fn main() {
    let core_addr = format!(
        "{}:{}",
        "0.0.0.0",
        std::env::var("DATACLOD_PORT").unwrap_or(DEFAULT_CORE_PORT.to_string())
    );
    server::postgres::server(core_addr).await
}

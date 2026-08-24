mod auth;
mod handler;
mod parser;
mod server;
mod types;
mod utils;

use std::sync::Arc;

use server::PostgresBackend;
use tokio::net::TcpListener;

pub async fn server(tcp_addr: String) {
    serve(tcp_addr, Arc::new(PostgresBackend::new())).await
}

pub async fn server_with_context(tcp_addr: String, ctx: Arc<dataclod::QueryContext>) {
    serve(tcp_addr, Arc::new(PostgresBackend::with_context(ctx))).await
}

async fn serve(tcp_addr: String, factory: Arc<PostgresBackend>) {
    let listener = TcpListener::bind(&tcp_addr)
        .await
        .expect("Failed to bind TCP listener");
    loop {
        let (incoming_socket, _) = listener.accept().await.expect("Failed to accept socket");
        let factory_ref = factory.clone();

        tokio::spawn(async move {
            pgwire::tokio::process_socket(incoming_socket, None, factory_ref).await
        });
    }
}

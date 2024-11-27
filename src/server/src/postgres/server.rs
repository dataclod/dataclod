use std::sync::Arc;

use tokio::net::TcpListener;

use super::handler_factory::PostgresBackendFactory;

pub async fn server(tcp_addr: String) {
    let factory = Arc::new(PostgresBackendFactory::new());

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

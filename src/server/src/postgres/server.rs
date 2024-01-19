use std::collections::HashMap;
use std::sync::Arc;

use pgwire::api::auth::ServerParameterProvider;
use pgwire::api::{ClientInfo, MakeHandler};
use tokio::net::TcpListener;

use super::auth_source::DataClodAuthSource;
use super::make_handler::{MakeDataClodStartupHandler, MakePostgresBackend};

const PG_VERSION: &str = "9.0";

pub struct DataClodParameterProvider {}

impl ServerParameterProvider for DataClodParameterProvider {
    fn server_parameters<C>(&self, _client: &C) -> Option<HashMap<String, String>>
    where
        C: ClientInfo,
    {
        Some(HashMap::from([
            ("server_version".to_owned(), PG_VERSION.to_owned()),
            ("server_encoding".to_owned(), "UTF8".to_owned()),
            ("client_encoding".to_owned(), "UTF8".to_owned()),
            ("DateStyle".to_owned(), "ISO YMD".to_owned()),
            ("integer_datetimes".to_owned(), "on".to_owned()),
        ]))
    }
}

pub async fn server(tcp_addr: String) {
    let listener = TcpListener::bind(&tcp_addr)
        .await
        .expect("Failed to bind TCP listener");

    let authenticator = Arc::new(MakeDataClodStartupHandler::new(
        Arc::new(DataClodAuthSource),
        Arc::new(DataClodParameterProvider {}),
    ));
    let processor = Arc::new(MakePostgresBackend::new());

    loop {
        let (incoming_socket, _) = listener.accept().await.expect("Failed to accept socket");
        let authenticator_ref = authenticator.make();
        let processor_ref = processor.make();

        tokio::spawn(async move {
            pgwire::tokio::process_socket(
                incoming_socket,
                None,
                authenticator_ref,
                processor_ref.clone(),
                processor_ref,
            )
            .await
        });
    }
}

use std::collections::HashMap;
use std::sync::Arc;

use pgwire::api::auth::ServerParameterProvider;
use pgwire::api::{ClientInfo, MakeHandler};
use tokio::net::TcpListener;

use super::auth_source::DataClodAuthSource;
use super::make_handler::{MakeDataClodStartupHandler, MakePostgresBackend};

pub struct DataClodParameterProvider {
    version: &'static str,
}

impl DataClodParameterProvider {
    fn new() -> Self {
        Self {
            version: env!("CARGO_PKG_VERSION"),
        }
    }
}

impl ServerParameterProvider for DataClodParameterProvider {
    fn server_parameters<C>(&self, _client: &C) -> Option<HashMap<String, String>>
    where
        C: ClientInfo,
    {
        Some(HashMap::from([
            ("server_version".to_owned(), self.version.to_owned()),
            ("server_encoding".to_owned(), "UTF8".to_owned()),
            ("client_encoding".to_owned(), "UTF8".to_owned()),
            ("DateStyle".to_owned(), "ISO YMD".to_owned()),
            ("integer_datetimes".to_owned(), "on".to_owned()),
        ]))
    }
}

pub async fn server(tcp_addr: String) {
    let listener = TcpListener::bind(&tcp_addr).await.unwrap();

    let authenticator = Arc::new(MakeDataClodStartupHandler::new(
        Arc::new(DataClodAuthSource),
        Arc::new(DataClodParameterProvider::new()),
    ));
    let processor = Arc::new(MakePostgresBackend::new());

    loop {
        let (incoming_socket, _) = listener.accept().await.unwrap();
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

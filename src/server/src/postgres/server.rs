use std::sync::Arc;

use dataclod::QueryContext;
use pgwire::api::PgWireServerHandlers;
use pgwire::api::auth::StartupHandler;
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use tokio::sync::Mutex;

use super::auth::{DataClodAuthSource, DataClodParameterProvider, DataClodStartupHandler};
use super::handler::{ExtendedPostgresBackend, SimplePostgresBackend};

pub struct PostgresBackend {
    pub simple_handler: Arc<SimplePostgresBackend>,
    pub extended_handler: Arc<ExtendedPostgresBackend>,
}

impl PostgresBackend {
    pub fn new() -> Self {
        let ctx = Arc::new(QueryContext::new());
        Self {
            simple_handler: Arc::new(SimplePostgresBackend::new(ctx.clone())),
            extended_handler: Arc::new(ExtendedPostgresBackend::new(ctx)),
        }
    }
}

impl PgWireServerHandlers for PostgresBackend {
    fn simple_query_handler(&self) -> Arc<impl SimpleQueryHandler> {
        self.simple_handler.clone()
    }

    fn extended_query_handler(&self) -> Arc<impl ExtendedQueryHandler> {
        self.extended_handler.clone()
    }

    fn startup_handler(&self) -> Arc<impl StartupHandler> {
        Arc::new(DataClodStartupHandler {
            auth_source: Arc::new(DataClodAuthSource),
            parameter_provider: Arc::new(DataClodParameterProvider),
            cached_password: Mutex::new(vec![]),
        })
    }
}

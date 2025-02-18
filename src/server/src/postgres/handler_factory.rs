use std::sync::Arc;

use dataclod::QueryContext;
use pgwire::api::copy::NoopCopyHandler;
use pgwire::api::{NoopErrorHandler, PgWireServerHandlers};
use tokio::sync::Mutex;

use super::auth::{DataClodAuthSource, DataClodParameterProvider, DataClodStartupHandler};
use super::query_handler::{ExtendedPostgresBackend, SimplePostgresBackend};

pub struct PostgresBackendFactory {
    pub simple_handler: Arc<SimplePostgresBackend>,
    pub extended_handler: Arc<ExtendedPostgresBackend>,
}

impl PostgresBackendFactory {
    pub fn new() -> Self {
        let ctx = Arc::new(QueryContext::new());
        Self {
            simple_handler: Arc::new(SimplePostgresBackend::new(ctx.clone())),
            extended_handler: Arc::new(ExtendedPostgresBackend::new(ctx)),
        }
    }
}

impl PgWireServerHandlers for PostgresBackendFactory {
    type CopyHandler = NoopCopyHandler;
    type ErrorHandler = NoopErrorHandler;
    type ExtendedQueryHandler = ExtendedPostgresBackend;
    type SimpleQueryHandler = SimplePostgresBackend;
    type StartupHandler = DataClodStartupHandler<DataClodAuthSource, DataClodParameterProvider>;

    fn simple_query_handler(&self) -> Arc<Self::SimpleQueryHandler> {
        self.simple_handler.clone()
    }

    fn extended_query_handler(&self) -> Arc<Self::ExtendedQueryHandler> {
        self.extended_handler.clone()
    }

    fn startup_handler(&self) -> Arc<Self::StartupHandler> {
        Arc::new(DataClodStartupHandler {
            auth_source: Arc::new(DataClodAuthSource),
            parameter_provider: Arc::new(DataClodParameterProvider),
            cached_password: Mutex::new(vec![]),
        })
    }

    fn copy_handler(&self) -> Arc<Self::CopyHandler> {
        Arc::new(NoopCopyHandler)
    }

    fn error_handler(&self) -> Arc<Self::ErrorHandler> {
        Arc::new(NoopErrorHandler)
    }
}

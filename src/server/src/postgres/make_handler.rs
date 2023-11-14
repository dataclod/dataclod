use std::sync::Arc;

use datafusion::prelude::SessionContext;
use pgwire::api::auth::{AuthSource, ServerParameterProvider};
use pgwire::api::store::MemPortalStore;
use pgwire::api::MakeHandler;
use tokio::sync::Mutex;

use super::query_handler::PostgresBackend;
use super::query_parser::DataClotQueryParser;
use super::startup_handler::DataClotStartupHandler;

pub struct MakePostgresBackend {
    session_context: Arc<SessionContext>,
    query_parser: Arc<DataClotQueryParser>,
}

impl MakePostgresBackend {
    pub fn new() -> Self {
        let core = SessionContext::new();
        Self {
            session_context: Arc::new(core),
            query_parser: Arc::new(DataClotQueryParser {}),
        }
    }
}

impl MakeHandler for MakePostgresBackend {
    type Handler = Arc<PostgresBackend>;

    fn make(&self) -> Self::Handler {
        Arc::new(PostgresBackend {
            session_context: self.session_context.clone(),
            portal_store: Arc::new(MemPortalStore::new()),
            query_parser: self.query_parser.clone(),
        })
    }
}

#[derive(Debug)]
pub struct MakeDataClotStartupHandler<A, P> {
    auth_source: Arc<A>,
    parameter_provider: Arc<P>,
}

impl<A, P> MakeDataClotStartupHandler<A, P> {
    pub fn new(auth_source: Arc<A>, parameter_provider: Arc<P>) -> Self {
        Self {
            auth_source,
            parameter_provider,
        }
    }
}

impl<A, P> MakeHandler for MakeDataClotStartupHandler<A, P>
where
    A: AuthSource,
    P: ServerParameterProvider,
{
    type Handler = Arc<DataClotStartupHandler<A, P>>;

    fn make(&self) -> Self::Handler {
        Arc::new(DataClotStartupHandler {
            auth_source: self.auth_source.clone(),
            parameter_provider: self.parameter_provider.clone(),
            cached_password: Mutex::new(vec![]),
        })
    }
}

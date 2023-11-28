use std::sync::Arc;

use datafusion::execution::context::SessionConfig;
use datafusion::prelude::SessionContext;
use datafusion_util::information_schema::with_pg_catalog;
use pgwire::api::auth::{AuthSource, ServerParameterProvider};
use pgwire::api::store::MemPortalStore;
use pgwire::api::MakeHandler;
use tokio::sync::Mutex;

use super::query_handler::PostgresBackend;
use super::query_parser::DataClodQueryParser;
use super::startup_handler::DataClodStartupHandler;

pub struct MakePostgresBackend {
    session_context: Arc<SessionContext>,
    query_parser: Arc<DataClodQueryParser>,
}

impl MakePostgresBackend {
    pub fn new() -> Self {
        let cfg = SessionConfig::new().with_information_schema(true);
        let ctx = SessionContext::new_with_config(cfg);
        with_pg_catalog(&ctx).unwrap();

        Self {
            session_context: Arc::new(ctx),
            query_parser: Arc::new(DataClodQueryParser {}),
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
pub struct MakeDataClodStartupHandler<A, P> {
    auth_source: Arc<A>,
    parameter_provider: Arc<P>,
}

impl<A, P> MakeDataClodStartupHandler<A, P> {
    pub fn new(auth_source: Arc<A>, parameter_provider: Arc<P>) -> Self {
        Self {
            auth_source,
            parameter_provider,
        }
    }
}

impl<A, P> MakeHandler for MakeDataClodStartupHandler<A, P>
where
    A: AuthSource,
    P: ServerParameterProvider,
{
    type Handler = Arc<DataClodStartupHandler<A, P>>;

    fn make(&self) -> Self::Handler {
        Arc::new(DataClodStartupHandler {
            auth_source: self.auth_source.clone(),
            parameter_provider: self.parameter_provider.clone(),
            cached_password: Mutex::new(vec![]),
        })
    }
}

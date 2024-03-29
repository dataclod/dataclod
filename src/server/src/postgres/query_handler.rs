use std::sync::Arc;

use async_trait::async_trait;
use dataclod::QueryContext;
use datafusion::sql::parser::Statement;
use pgwire::api::portal::{Format, Portal};
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler, StatementOrPortal};
use pgwire::api::results::{DescribeResponse, Response};
use pgwire::api::store::MemPortalStore;
use pgwire::api::ClientInfo;
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use tracing::debug;

use super::query_parser::DataClodQueryParser;
use super::types::{encode_dataframe, encode_parameters, encode_schema};

pub struct PostgresBackend {
    pub session_context: Arc<QueryContext>,
    pub portal_store: Arc<MemPortalStore<Statement>>,
    pub query_parser: Arc<DataClodQueryParser>,
}

#[async_trait]
impl SimpleQueryHandler for PostgresBackend {
    async fn do_query<'a, C>(
        &self, _client: &mut C, query: &'a str,
    ) -> PgWireResult<Vec<Response<'a>>>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        debug!("simple query: {}", query);
        let ctx = self.session_context.as_ref();

        let df = ctx
            .sql(query)
            .await
            .map_err(|e| PgWireError::ApiError(e.into()))?;
        let resp = encode_dataframe(df, &Format::UnifiedText).await?;

        Ok(vec![Response::Query(resp)])
    }
}

#[async_trait]
impl ExtendedQueryHandler for PostgresBackend {
    type QueryParser = DataClodQueryParser;
    type Statement = Statement;

    fn query_parser(&self) -> Arc<Self::QueryParser> {
        self.query_parser.clone()
    }

    async fn do_query<'a, C>(
        &self, _client: &mut C, portal: &'a Portal<Self::Statement>, _max_rows: usize,
    ) -> PgWireResult<Response<'a>>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        debug!("extend query: {}", portal.statement.statement);
        let ctx = self.session_context.as_ref();

        let stmt = &portal.statement.statement;
        let df = ctx
            .sql(&stmt.to_string())
            .await
            .map_err(|e| PgWireError::ApiError(e.into()))?;

        // TODO: better error handling
        if portal.statement.parameter_types.is_empty() {
            let resp = encode_dataframe(df, &Format::UnifiedText).await?;
            Ok(Response::Query(resp))
        } else {
            let parameters = encode_parameters(portal)?;
            let df = df
                .with_param_values(parameters)
                .map_err(|e| PgWireError::ApiError(e.into()))?;
            let resp = encode_dataframe(df, &Format::UnifiedText).await?;
            Ok(Response::Query(resp))
        }
    }

    async fn do_describe<C>(
        &self, _client: &mut C, target: StatementOrPortal<'_, Self::Statement>,
    ) -> PgWireResult<DescribeResponse>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let ctx = self.session_context.as_ref();

        match target {
            StatementOrPortal::Statement(statement) => {
                let stmt = &statement.statement;
                debug!("describe statement: {}", stmt);

                let plan = ctx
                    .state()
                    .statement_to_plan(stmt.clone())
                    .await
                    .map_err(|e| {
                        PgWireError::UserError(Box::new(ErrorInfo::new(
                            "ERROR".to_owned(),
                            "XX000".to_owned(),
                            format!("Failed to create logical plan: {e}"),
                        )))
                    })?;
                let schema = plan.schema();

                let param_types = statement.parameter_types.clone();
                let fields = encode_schema(schema, &Format::UnifiedBinary)?;
                Ok(DescribeResponse::new(Some(param_types), fields))
            }
            StatementOrPortal::Portal(portal) => {
                let stmt = &portal.statement.statement;
                debug!("describe portal: {}", stmt);

                let plan = ctx
                    .state()
                    .statement_to_plan(stmt.clone())
                    .await
                    .map_err(|e| {
                        PgWireError::UserError(Box::new(ErrorInfo::new(
                            "ERROR".to_owned(),
                            "XX000".to_owned(),
                            format!("Failed to create logical plan: {e}"),
                        )))
                    })?;
                if portal.statement.parameter_types.is_empty() {
                    let schema = plan.schema();

                    let format = &portal.result_column_format;
                    let fields = encode_schema(schema, format)?;
                    Ok(DescribeResponse::new(None, fields))
                } else {
                    // XXX: need with_param_values here?
                    let parameters = encode_parameters(portal)?;
                    let plan = plan.with_param_values(parameters).map_err(|e| {
                        PgWireError::UserError(Box::new(ErrorInfo::new(
                            "ERROR".to_owned(),
                            "XX000".to_owned(),
                            format!("Failed to create logical plan: {e}"),
                        )))
                    })?;
                    let schema = plan.schema();

                    let format = &portal.result_column_format;
                    let fields = encode_schema(schema, format)?;
                    Ok(DescribeResponse::new(None, fields))
                }
            }
        }
    }
}

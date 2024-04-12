use std::sync::Arc;

use async_trait::async_trait;
use dataclod::QueryContext;
use datafusion::sql::parser::Statement;
use pgwire::api::portal::{Format, Portal};
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{DescribePortalResponse, DescribeStatementResponse, Response};
use pgwire::api::stmt::StoredStatement;
use pgwire::api::ClientInfo;
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use tracing::debug;

use super::query_parser::DataClodQueryParser;
use super::types::{encode_dataframe, encode_parameters, encode_schema};

pub struct PostgresBackend {
    pub session_context: Arc<QueryContext>,
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

        let df = self
            .session_context
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

        let df = self
            .session_context
            .sql(&portal.statement.statement.to_string())
            .await
            .map_err(|e| PgWireError::ApiError(e.into()))?;

        // TODO: better error handling
        if portal.statement.parameter_types.is_empty() {
            let resp = encode_dataframe(df, &portal.result_column_format).await?;
            Ok(Response::Query(resp))
        } else {
            let parameters = encode_parameters(portal)?;
            let df = df
                .with_param_values(parameters)
                .map_err(|e| PgWireError::ApiError(e.into()))?;
            let resp = encode_dataframe(df, &portal.result_column_format).await?;
            Ok(Response::Query(resp))
        }
    }

    async fn do_describe_statement<C>(
        &self, _client: &mut C, stmt: &StoredStatement<Self::Statement>,
    ) -> PgWireResult<DescribeStatementResponse>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        debug!("describe statement: {}", stmt.statement);

        let plan = self
            .session_context
            .state()
            .statement_to_plan(stmt.statement.clone())
            .await
            .map_err(|e| {
                PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(),
                    "XX000".to_owned(),
                    format!("Failed to create logical plan: {e}"),
                )))
            })?;
        let schema = plan.schema();

        let param_types = stmt.parameter_types.clone();
        let fields = encode_schema(schema, &Format::UnifiedBinary)?;
        Ok(DescribeStatementResponse::new(param_types, fields))
    }

    async fn do_describe_portal<C>(
        &self, _client: &mut C, portal: &Portal<Self::Statement>,
    ) -> PgWireResult<DescribePortalResponse>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        debug!("describe portal: {}", portal.statement.statement);

        let plan = self
            .session_context
            .state()
            .statement_to_plan(portal.statement.statement.clone())
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
            let fields = encode_schema(schema, &portal.result_column_format)?;
            Ok(DescribePortalResponse::new(fields))
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
            let fields = encode_schema(schema, &portal.result_column_format)?;
            Ok(DescribePortalResponse::new(fields))
        }
    }
}

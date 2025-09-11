use std::sync::Arc;

use async_trait::async_trait;
use dataclod::QueryContext;
use datafusion::sql::parser::Statement;
use pgwire::api::portal::{Format, Portal};
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{DescribePortalResponse, DescribeStatementResponse, Response, Tag};
use pgwire::api::stmt::StoredStatement;
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use tracing::debug;

use super::parser::DataClodQueryParser;
use super::types::{encode_dataframe, encode_parameters, encode_schema};

const DEFAULT_ROW_LIMIT: usize = 1024;

pub struct SimplePostgresBackend {
    pub session_context: Arc<QueryContext>,
}

impl SimplePostgresBackend {
    pub fn new(session_context: Arc<QueryContext>) -> Self {
        Self { session_context }
    }
}

#[async_trait]
impl SimpleQueryHandler for SimplePostgresBackend {
    async fn do_query<'a, C>(
        &self, _client: &mut C, query: &str,
    ) -> PgWireResult<Vec<Response<'a>>> {
        debug!("simple query: {}", query);

        let stmt =
            dataclod::sql_to_statement(query).map_err(|e| PgWireError::ApiError(Box::new(e)))?;

        if let Statement::Statement(stmt) = stmt {
            match stmt.as_ref() {
                datafusion::sql::sqlparser::ast::Statement::StartTransaction { .. } => {
                    return Ok(vec![Response::TransactionStart(Tag::new("BEGIN"))]);
                }
                datafusion::sql::sqlparser::ast::Statement::Commit { .. } => {
                    return Ok(vec![Response::Execution(Tag::new("COMMIT"))]);
                }
                datafusion::sql::sqlparser::ast::Statement::Rollback { .. } => {
                    return Ok(vec![Response::TransactionEnd(Tag::new("ROLLBACK"))]);
                }
                _ => {}
            }
        }

        if query.eq_ignore_ascii_case("begin") {
            return Ok(vec![Response::TransactionStart(Tag::new("BEGIN"))]);
        } else if query.eq_ignore_ascii_case("commit") {
            return Ok(vec![Response::Execution(Tag::new("COMMIT"))]);
        } else if query.eq_ignore_ascii_case("abort") || query.eq_ignore_ascii_case("rollback") {
            return Ok(vec![Response::TransactionEnd(Tag::new("ROLLBACK"))]);
        }

        let df = self
            .session_context
            .sql(query)
            .await
            .map_err(|e| PgWireError::ApiError(e.into()))?;
        let resp = encode_dataframe(df, &Format::UnifiedText, DEFAULT_ROW_LIMIT).await?;

        Ok(vec![Response::Query(resp)])
    }
}

pub struct ExtendedPostgresBackend {
    pub session_context: Arc<QueryContext>,
    pub query_parser: Arc<DataClodQueryParser>,
}

impl ExtendedPostgresBackend {
    pub fn new(session_context: Arc<QueryContext>) -> Self {
        Self {
            session_context,
            query_parser: Arc::new(DataClodQueryParser),
        }
    }
}

#[async_trait]
impl ExtendedQueryHandler for ExtendedPostgresBackend {
    type QueryParser = DataClodQueryParser;
    type Statement = Statement;

    fn query_parser(&self) -> Arc<Self::QueryParser> {
        self.query_parser.clone()
    }

    async fn do_query<'a, C>(
        &self, _client: &mut C, portal: &Portal<Self::Statement>, max_rows: usize,
    ) -> PgWireResult<Response<'a>> {
        debug!("extend query: {}", portal.statement.statement);

        if let Statement::Statement(stmt) = &portal.statement.statement {
            match stmt.as_ref() {
                datafusion::sql::sqlparser::ast::Statement::StartTransaction { .. } => {
                    return Ok(Response::TransactionStart(Tag::new("BEGIN")));
                }
                datafusion::sql::sqlparser::ast::Statement::Commit { .. } => {
                    return Ok(Response::Execution(Tag::new("COMMIT")));
                }
                datafusion::sql::sqlparser::ast::Statement::Rollback { .. } => {
                    return Ok(Response::TransactionEnd(Tag::new("ROLLBACK")));
                }
                _ => {}
            }
        }

        let df = self
            .session_context
            .sql(&portal.statement.statement.to_string())
            .await
            .map_err(|e| PgWireError::ApiError(e.into()))?;

        // TODO: better error handling
        if portal.statement.parameter_types.is_empty() {
            let resp = encode_dataframe(df, &portal.result_column_format, max_rows).await?;
            Ok(Response::Query(resp))
        } else {
            let parameters = encode_parameters(portal)?;
            let df = df
                .with_param_values(parameters)
                .map_err(|e| PgWireError::ApiError(e.into()))?;
            let resp = encode_dataframe(df, &portal.result_column_format, max_rows).await?;
            Ok(Response::Query(resp))
        }
    }

    async fn do_describe_statement<C>(
        &self, _client: &mut C, stmt: &StoredStatement<Self::Statement>,
    ) -> PgWireResult<DescribeStatementResponse> {
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
    ) -> PgWireResult<DescribePortalResponse> {
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

use async_trait::async_trait;
use dataclod::sql_to_statement;
use datafusion::sql::parser::Statement;
use pgwire::api::Type;
use pgwire::api::stmt::QueryParser;
use pgwire::error::{PgWireError, PgWireResult};

pub struct DataClodQueryParser;

#[async_trait]
impl QueryParser for DataClodQueryParser {
    type Statement = Statement;

    async fn parse_sql<C>(
        &self, _client: &C, sql: &str, _types: &[Option<Type>],
    ) -> PgWireResult<Self::Statement> {
        sql_to_statement(sql).map_err(|e| PgWireError::ApiError(Box::new(e)))
    }
}

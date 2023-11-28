use async_trait::async_trait;
use datafusion::sql::parser::Statement;
use pgwire::api::stmt::QueryParser;
use pgwire::api::Type;
use pgwire::error::{PgWireError, PgWireResult};
use query::sql_to_statement;

pub struct DataClodQueryParser;

#[async_trait]
impl QueryParser for DataClodQueryParser {
    type Statement = Statement;

    async fn parse_sql(&self, sql: &str, _types: &[Type]) -> PgWireResult<Self::Statement> {
        sql_to_statement(sql).map_err(|e| PgWireError::ApiError(Box::new(e)))
    }
}

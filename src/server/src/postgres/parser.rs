use std::sync::Arc;

use async_trait::async_trait;
use dataclod::QueryContext;
use datafusion::sql::parser::Statement;
use pgwire::api::Type;
use pgwire::api::portal::Format;
use pgwire::api::results::FieldInfo;
use pgwire::api::stmt::QueryParser;
use pgwire::error::{PgWireError, PgWireResult};

pub struct DataClodQueryParser {
    pub session_context: Arc<QueryContext>,
}

#[async_trait]
impl QueryParser for DataClodQueryParser {
    type Statement = Statement;

    async fn parse_sql<C>(
        &self, _client: &C, sql: &str, _types: &[Option<Type>],
    ) -> PgWireResult<Self::Statement> {
        self.session_context
            .sql_to_statement(sql)
            .map_err(|e| PgWireError::ApiError(e.into_boxed_dyn_error()))
    }

    fn get_parameter_types(&self, _stmt: &Self::Statement) -> PgWireResult<Vec<Type>> {
        Ok(vec![])
    }

    fn get_result_schema(
        &self, _stmt: &Self::Statement, column_format: Option<&Format>,
    ) -> PgWireResult<Vec<FieldInfo>> {
        Ok(vec![])
    }
}

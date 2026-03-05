use std::sync::Arc;

use async_trait::async_trait;
use dataclod::QueryContext;
use datafusion::logical_expr::LogicalPlan;
use pgwire::api::Type;
use pgwire::api::portal::Format;
use pgwire::api::results::FieldInfo;
use pgwire::api::stmt::QueryParser;
use pgwire::error::{PgWireError, PgWireResult};

use crate::postgres::types::{encode_schema, into_pg_type};

pub struct DataClodQueryParser {
    pub session_context: Arc<QueryContext>,
}

#[async_trait]
impl QueryParser for DataClodQueryParser {
    type Statement = LogicalPlan;

    async fn parse_sql<C>(
        &self, _client: &C, sql: &str, _types: &[Option<Type>],
    ) -> PgWireResult<Self::Statement> {
        self.session_context
            .create_logical_plan(sql)
            .await
            .map_err(|e| PgWireError::ApiError(e.into_boxed_dyn_error()))
    }

    fn get_parameter_types(&self, stmt: &Self::Statement) -> PgWireResult<Vec<Type>> {
        let parameter_types = stmt
            .get_parameter_types()
            .map_err(|e| PgWireError::ApiError(Box::new(e)))?;
        parameter_types
            .values()
            .map(|v| {
                match v {
                    Some(dt) => into_pg_type(dt),
                    None => Ok(Type::UNKNOWN),
                }
            })
            .collect()
    }

    fn get_result_schema(
        &self, stmt: &Self::Statement, column_format: Option<&Format>,
    ) -> PgWireResult<Vec<FieldInfo>> {
        let schema = stmt.schema();
        let format = column_format.unwrap_or(&Format::UnifiedBinary);
        encode_schema(schema, format)
    }
}

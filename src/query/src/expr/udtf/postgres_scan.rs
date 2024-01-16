use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{plan_err, DataFusionError, ScalarValue};
use datafusion::datasource::function::TableFunctionImpl;
use datafusion::datasource::TableProvider;
use datafusion::error::Result;
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::{Expr, TableType};
use datafusion::physical_plan::ExecutionPlan;

pub struct PostgresScanUDTF {}

impl TableFunctionImpl for PostgresScanUDTF {
    fn call(&self, exprs: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        if exprs.len() != 3 {
            return plan_err!("postgres_scan takes 3 arguments");
        }

        let dsn = &exprs[0];
        let schema = &exprs[1];
        let table = &exprs[2];

        match (dsn, schema, table) {
            (
                Expr::Literal(ScalarValue::Utf8(Some(dsn))),
                Expr::Literal(ScalarValue::Utf8(Some(schema))),
                Expr::Literal(ScalarValue::Utf8(Some(table))),
            ) => {
                Ok(Arc::new(PostgresScanTable {
                    dsn: dsn.to_owned(),
                    schema: schema.to_owned(),
                    table: table.to_owned(),
                }))
            }
            _ => plan_err!("postgres_scan arguments must be string literals"),
        }
    }
}

struct PostgresScanTable {
    dsn: String,
    schema: String,
    table: String,
}

#[async_trait]
impl TableProvider for PostgresScanTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        todo!()
    }

    fn table_type(&self) -> TableType {
        TableType::Temporary
    }

    async fn scan(
        &self, _state: &SessionState, _projection: Option<&Vec<usize>>, _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        todo!()
    }
}

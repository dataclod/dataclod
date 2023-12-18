use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::Int64Array;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{exec_err, DataFusionError, ScalarValue};
use datafusion::datasource::function::TableFunctionImpl;
use datafusion::datasource::TableProvider;
use datafusion::error::Result;
use datafusion::execution::context::{ExecutionProps, SessionState};
use datafusion::logical_expr::{Expr, TableType};
use datafusion::optimizer::simplify_expressions::{ExprSimplifier, SimplifyContext};
use datafusion::physical_plan::memory::MemoryExec;
use datafusion::physical_plan::ExecutionPlan;

struct GenerateSeriesTable<T: Send + Sync> {
    start: T,
    stop: T,
    step: T,
}

#[async_trait]
impl TableProvider for GenerateSeriesTable<i64> {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new(
            "generate_series",
            DataType::Int64,
            true,
        )]))
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self, _state: &SessionState, projection: Option<&Vec<usize>>, _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let schema = self.schema();
        let array: Int64Array = (self.start..=self.stop)
            .step_by(self.step as usize)
            .collect();
        let batches = vec![RecordBatch::try_new(schema, vec![Arc::new(array)]).unwrap()];
        Ok(Arc::new(MemoryExec::try_new(
            &[batches],
            self.schema(),
            projection.cloned(),
        )?))
    }
}

pub struct GenerateSeriesUDTF {}

impl TableFunctionImpl for GenerateSeriesUDTF {
    fn call(&self, exprs: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        let execution_props = ExecutionProps::new();
        let info = SimplifyContext::new(&execution_props);
        let start = ExprSimplifier::new(info).simplify(exprs[0].clone())?;
        let execution_props = ExecutionProps::new();
        let info = SimplifyContext::new(&execution_props);
        let stop = ExprSimplifier::new(info).simplify(exprs[1].clone())?;
        let execution_props = ExecutionProps::new();
        let step = if exprs.len() == 3 {
            let info = SimplifyContext::new(&execution_props);
            ExprSimplifier::new(info).simplify(exprs[2].clone())?
        } else {
            Expr::Literal(ScalarValue::Int64(Some(1)))
        };

        match (start, stop, step) {
            (
                Expr::Literal(ScalarValue::Int64(Some(start))),
                Expr::Literal(ScalarValue::Int64(Some(stop))),
                Expr::Literal(ScalarValue::Int64(Some(step))),
            ) => Ok(Arc::new(GenerateSeriesTable { start, stop, step })),
            _ => exec_err!("Limit must be an integer"),
        }
    }
}

#[cfg(test)]
mod tests {
    use datafusion::execution::context::SessionContext;

    use super::*;

    #[tokio::test]
    async fn test_generate_series() -> Result<()> {
        let ctx = SessionContext::new();

        ctx.register_udtf("generate_series", Arc::new(GenerateSeriesUDTF {}));

        let df = ctx.sql("SELECT * FROM generate_series(1, 1);").await?;
        df.show().await?;

        Ok(())
    }
}

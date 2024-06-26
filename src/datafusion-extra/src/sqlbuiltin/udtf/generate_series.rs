use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::Int64Array;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{plan_err, ScalarValue};
use datafusion::datasource::function::TableFunctionImpl;
use datafusion::datasource::TableProvider;
use datafusion::error::Result as DFResult;
use datafusion::execution::context::{ExecutionProps, SessionState};
use datafusion::logical_expr::{Expr, TableType};
use datafusion::optimizer::simplify_expressions::{ExprSimplifier, SimplifyContext};
use datafusion::physical_plan::memory::MemoryExec;
use datafusion::physical_plan::ExecutionPlan;

pub struct GenerateSeriesUDTF;

impl TableFunctionImpl for GenerateSeriesUDTF {
    fn call(&self, exprs: &[Expr]) -> DFResult<Arc<dyn TableProvider>> {
        if exprs.len() < 2 || exprs.len() > 3 {
            return plan_err!("generate_series takes 2 or 3 arguments");
        }

        let execution_props = ExecutionProps::new();
        let info = SimplifyContext::new(&execution_props);
        let simplifier = ExprSimplifier::new(info);
        let start = simplifier.simplify(exprs[0].clone())?;
        let stop = simplifier.simplify(exprs[1].clone())?;
        let step = if exprs.len() == 3 {
            simplifier.simplify(exprs[2].clone())?
        } else {
            Expr::Literal(ScalarValue::Int64(Some(1)))
        };

        match (start, stop, step) {
            (
                Expr::Literal(ScalarValue::Int64(Some(start))),
                Expr::Literal(ScalarValue::Int64(Some(stop))),
                Expr::Literal(ScalarValue::Int64(Some(step))),
            ) => Ok(Arc::new(GenerateSeriesTable { start, stop, step })),
            _ => plan_err!("generate_series arguments must be integer literals"),
        }
    }
}

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
            false,
        )]))
    }

    fn table_type(&self) -> TableType {
        TableType::Temporary
    }

    async fn scan(
        &self, _state: &SessionState, projection: Option<&Vec<usize>>, _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let schema = self.schema();
        let array: Int64Array = (self.start..=self.stop)
            .step_by(self.step as usize)
            .collect();
        let batch = RecordBatch::try_new(schema, vec![Arc::new(array)])?;
        Ok(Arc::new(MemoryExec::try_new(
            &[vec![batch]],
            self.schema(),
            projection.cloned(),
        )?))
    }
}

#[cfg(test)]
mod tests {
    use datafusion::assert_batches_eq;
    use datafusion::execution::context::SessionContext;

    use super::*;

    #[tokio::test]
    async fn test_generate_series() -> DFResult<()> {
        let ctx = SessionContext::new();

        ctx.register_udtf("generate_series", Arc::new(GenerateSeriesUDTF {}));

        let df = ctx.sql("SELECT * FROM generate_series(1, 4)").await?;
        let batches = df.collect().await?;
        assert_batches_eq!(
            &[
                "+-----------------+",
                "| generate_series |",
                "+-----------------+",
                "| 1               |",
                "| 2               |",
                "| 3               |",
                "| 4               |",
                "+-----------------+",
            ],
            &batches
        );

        Ok(())
    }
}

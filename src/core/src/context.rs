use anyhow::Result;
use datafusion::dataframe::DataFrame;
use datafusion::execution::context::{SessionConfig, SessionContext, SessionState};
use datafusion::logical_expr::{LogicalPlan, LogicalPlanBuilder, Statement};

pub struct QueryContext {
    inner: SessionContext,
}

impl Default for QueryContext {
    fn default() -> Self {
        Self::new()
    }
}

impl QueryContext {
    pub fn new() -> Self {
        let config = SessionConfig::new().with_information_schema(true).set_bool(
            "datafusion.execution.skip_physical_aggregate_schema_check",
            true,
        );
        let ctx = SessionContext::new_with_config(config);
        datafusion_extra::catalog::with_pg_catalog(&ctx).expect("Failed to register pg_catalog");
        datafusion_extra::sqlbuiltin::register_udf(&ctx);
        crate::expr::register_udtf(&ctx);

        Self { inner: ctx }
    }

    pub fn state(&self) -> SessionState {
        self.inner.state()
    }

    pub async fn sql(&self, sql: &str) -> Result<DataFrame> {
        let plan = self.state().create_logical_plan(sql).await?;
        let df = self.execute_logical_plan(plan).await?;
        Ok(df)
    }

    pub async fn execute_logical_plan(&self, plan: LogicalPlan) -> Result<DataFrame> {
        match plan {
            LogicalPlan::Statement(Statement::SetVariable(_)) => self.return_empty_dataframe(),

            plan => {
                let df = self.inner.execute_logical_plan(plan).await?;
                Ok(df)
            }
        }
    }

    fn return_empty_dataframe(&self) -> Result<DataFrame> {
        let plan = LogicalPlanBuilder::empty(false).build()?;
        Ok(DataFrame::new(self.state(), plan))
    }
}

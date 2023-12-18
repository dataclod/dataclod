use anyhow::Result;
use datafusion::dataframe::DataFrame;
use datafusion::execution::context::{SessionContext, SessionState};
use datafusion::logical_expr::{LogicalPlan, LogicalPlanBuilder, Statement};

pub struct QueryContext {
    inner: SessionContext,
}

impl QueryContext {
    pub fn new(ctx: SessionContext) -> Self {
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

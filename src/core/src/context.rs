use std::sync::Arc;

use anyhow::Result;
use datafusion::dataframe::DataFrame;
use datafusion::execution::context::{SessionConfig, SessionContext};
use datafusion::logical_expr::{LogicalPlan, LogicalPlanBuilder, Statement};
use datafusion::sql::parser::Statement as SqlStatement;
use parking_lot::RwLock;

use crate::rewrite::StatementRewrite;
use crate::state::QueryState;

const DEFAULT_DIALECT: &str = "postgres";

pub struct QueryContext {
    inner: SessionContext,
    state: Arc<RwLock<QueryState>>,
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
        datafusion_extra::spatial::register_spatial_udfs(&ctx);
        crate::expr::register_udtf(&ctx);
        let state = Arc::new(RwLock::new(QueryState::new()));
        let mut ctx = Self { inner: ctx, state };
        crate::rewrite::register_postgres_stmt_rewrites(&mut ctx);
        ctx
    }

    pub async fn sql(&self, sql: &str) -> Result<DataFrame> {
        let plan = self.create_logical_plan(sql).await?;
        let df = self.execute_logical_plan(plan).await?;
        Ok(df)
    }

    pub async fn create_logical_plan(&self, sql: &str) -> Result<LogicalPlan> {
        let statement = self.sql_to_statement(sql)?;
        let rewrite_stmt = self.statement_rewrite(statement)?;
        let plan = self.statement_to_plan(rewrite_stmt).await?;
        Ok(plan)
    }

    fn statement_rewrite(&self, stmt: SqlStatement) -> Result<SqlStatement> {
        let binding = self.state.read();
        let rewrites = binding.stmt_rewrites();
        let mut stmt = stmt;
        for rewrite in rewrites {
            stmt = rewrite.rewrite(stmt)?;
        }
        Ok(stmt)
    }

    fn sql_to_statement(&self, sql: &str) -> Result<SqlStatement> {
        let statement = self.inner.state().sql_to_statement(sql, DEFAULT_DIALECT)?;
        Ok(statement)
    }

    pub async fn statement_to_plan(&self, statement: SqlStatement) -> Result<LogicalPlan> {
        let plan = self.inner.state().statement_to_plan(statement).await?;
        Ok(plan)
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
        Ok(DataFrame::new(self.inner.state(), plan))
    }
}

impl QueryContext {
    pub fn register_statement_rewrite(&self, rewrite: Arc<dyn StatementRewrite + Send + Sync>) {
        self.state.write().add_stmt_rewrite(rewrite);
    }
}

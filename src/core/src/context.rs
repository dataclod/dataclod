use std::sync::Arc;

use anyhow::{Context, Result};
use datafusion::config::Dialect;
use datafusion::dataframe::DataFrame;
use datafusion::execution::SessionStateBuilder;
use datafusion::execution::context::{SessionConfig, SessionContext};
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::logical_expr::{LogicalPlan, LogicalPlanBuilder, ScalarUDF, Statement};
use datafusion::sql::parser::Statement as SqlStatement;
use parking_lot::RwLock;

use crate::cluster::ClusterConfig;
use crate::dataclod_url::install_dataclod_url_catalog;
use crate::rewrite::StatementRewrite;
use crate::runtime::make_runtime_from_cluster;
use crate::state::QueryState;

#[derive(Clone)]
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
        Self::try_new(QueryContextOptions::standalone())
            .expect("Failed to build standalone QueryContext")
    }

    pub fn try_new(opts: QueryContextOptions) -> Result<Self> {
        Self::try_new_with_builder(opts, Ok)
    }

    /// `configure` runs after the spatial planner is registered and before
    /// `build()`.
    pub fn try_new_with_builder<F>(opts: QueryContextOptions, configure: F) -> Result<Self>
    where
        F: FnOnce(SessionStateBuilder) -> Result<SessionStateBuilder>,
    {
        let config = execution_session_config();
        let config = datafusion_spatial::add_dataclod_option_extension(config);
        let runtime = match &opts.runtime {
            Some(rt) => rt.clone(),
            None => make_runtime_from_cluster(&opts.cluster)?,
        };
        let mut builder = SessionStateBuilder::new()
            .with_config(config)
            .with_runtime_env(runtime)
            .with_default_features();
        // Spatial planner replaces QueryPlanner. datafusion-distributed rustdoc says
        // to call with_distributed_planner *before* with_query_planner; the
        // implementation wraps via mem::take, so spatial must be installed first.
        builder = datafusion_spatial::register_planner(builder);
        builder = configure(builder)?;
        let ctx = SessionContext::new_with_state(builder.build());
        register_session_udfs(&ctx);
        register_session_catalog_and_udtf(&ctx)?;
        let ctx = install_dataclod_url_catalog(ctx, &opts.cluster.data_dir);
        let mut ctx = Self {
            inner: ctx,
            state: Arc::new(RwLock::new(QueryState::new())),
        };
        crate::rewrite::register_postgres_stmt_rewrites(&mut ctx);
        Ok(ctx)
    }

    pub fn session_context(&self) -> &SessionContext {
        &self.inner
    }

    pub fn runtime_env(&self) -> Arc<RuntimeEnv> {
        self.inner.runtime_env()
    }

    pub fn allow_set_prefix(&self, prefix: &'static str) {
        self.state.write().allow_set_prefix(prefix);
    }

    pub async fn sql(&self, sql: &str) -> Result<DataFrame> {
        let plan = self.create_logical_plan(sql).await?;
        let df = self.execute_logical_plan(plan).await?;
        Ok(df)
    }

    pub async fn stmt(&self, stmt: SqlStatement) -> Result<DataFrame> {
        let plan = self.statement_to_plan(stmt).await?;
        let df = self.execute_logical_plan(plan).await?;
        Ok(df)
    }

    pub async fn create_logical_plan(&self, sql: &str) -> Result<LogicalPlan> {
        let statement = self.sql_to_statement(sql)?;
        let plan = self.statement_to_plan(statement).await?;
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

    pub fn sql_to_statement(&self, sql: &str) -> Result<SqlStatement> {
        let statement = self
            .inner
            .state()
            .sql_to_statement(sql, &Dialect::PostgreSQL)?;
        self.statement_rewrite(statement)
    }

    pub async fn statement_to_plan(&self, statement: SqlStatement) -> Result<LogicalPlan> {
        let plan = self.inner.state().statement_to_plan(statement).await?;
        Ok(plan)
    }

    pub async fn execute_logical_plan(&self, plan: LogicalPlan) -> Result<DataFrame> {
        match plan {
            LogicalPlan::Statement(Statement::SetVariable(ref stmt)) => {
                if self.state.read().allows_set_variable(&stmt.variable) {
                    let df = self.inner.execute_logical_plan(plan).await?;
                    Ok(df)
                } else {
                    self.return_empty_dataframe()
                }
            }
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

#[derive(Clone, Debug)]
pub struct QueryContextOptions {
    pub cluster: ClusterConfig,
    pub runtime: Option<Arc<RuntimeEnv>>,
}

impl QueryContextOptions {
    pub fn standalone() -> Self {
        Self {
            cluster: ClusterConfig::standalone(),
            runtime: None,
        }
    }
}

pub fn collect_scalar_udfs() -> Vec<Arc<ScalarUDF>> {
    let mut fns = datafusion_sqlbuiltin::sqlbuiltin_udfs();
    fns.extend(datafusion_spatial::spatial_udfs());
    fns
}

fn execution_session_config() -> SessionConfig {
    SessionConfig::new().with_information_schema(true).set_bool(
        "datafusion.execution.skip_physical_aggregate_schema_check",
        true,
    )
}

fn register_session_udfs(ctx: &SessionContext) {
    datafusion_sqlbuiltin::register_sqlbuiltin_udfs(ctx);
    datafusion_spatial::register_spatial_udfs(ctx);
}

fn register_session_catalog_and_udtf(ctx: &SessionContext) -> Result<()> {
    datafusion_catalog_extra::with_pg_catalog(ctx).context("register pg_catalog")?;
    crate::expr::register_udtf(ctx);
    Ok(())
}

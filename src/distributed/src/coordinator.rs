use std::sync::Arc;

use anyhow::{Context, Result};
use async_trait::async_trait;
use dataclod::ClusterConfig;
use datafusion::execution::SessionStateBuilder;
use datafusion::execution::context::QueryPlanner;
use datafusion::execution::session_state::SessionState;
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_distributed::{DistributedExt, SessionStateBuilderExt};

use crate::encode_fallback::dry_run_encode_stages;
use crate::non_distributable::DataclodNonDistributable;
use crate::resolver::StaticWorkerResolver;

#[derive(Debug)]
pub struct EncodeSafeQueryPlanner {
    inner: Arc<dyn QueryPlanner + Send + Sync>,
    fallback_spatial: Arc<dyn QueryPlanner + Send + Sync>,
}

#[async_trait]
impl QueryPlanner for EncodeSafeQueryPlanner {
    async fn create_physical_plan(
        &self, logical_plan: &LogicalPlan, session_state: &SessionState,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        let planned = self
            .inner
            .create_physical_plan(logical_plan, session_state)
            .await?;
        match dry_run_encode_stages(&planned, session_state) {
            Ok(()) => Ok(planned),
            Err(err) => {
                tracing::warn!(error = %err, "distributed plan not encodable; falling back");
                self.fallback_spatial
                    .create_physical_plan(logical_plan, session_state)
                    .await
            }
        }
    }
}

pub(crate) fn apply_coordinator_session(
    mut builder: SessionStateBuilder, cfg: &ClusterConfig,
) -> Result<SessionStateBuilder> {
    let resolver = StaticWorkerResolver::parse(&cfg.worker_urls)?;
    let fallback_spatial = builder
        .query_planner()
        .clone()
        .context("spatial query planner must be registered before apply_coordinator_session")?;
    let builder = builder
        .with_distributed_worker_resolver(resolver)
        .with_distributed_task_estimator(DataclodNonDistributable)
        .with_distributed_planner();
    Ok(wrap_encode_safe(builder, fallback_spatial))
}

#[cfg(test)]
pub(crate) fn apply_coordinator_session_raw(
    builder: SessionStateBuilder, cfg: &ClusterConfig,
) -> Result<SessionStateBuilder> {
    let resolver = StaticWorkerResolver::parse(&cfg.worker_urls)?;
    Ok(builder
        .with_distributed_worker_resolver(resolver)
        .with_distributed_task_estimator(DataclodNonDistributable)
        .with_distributed_planner())
}

fn wrap_encode_safe(
    mut builder: SessionStateBuilder, fallback_spatial: Arc<dyn QueryPlanner + Send + Sync>,
) -> SessionStateBuilder {
    let inner = builder
        .query_planner()
        .take()
        .expect("with_distributed_planner just installed");
    builder.with_query_planner(Arc::new(EncodeSafeQueryPlanner {
        inner,
        fallback_spatial,
    }))
}

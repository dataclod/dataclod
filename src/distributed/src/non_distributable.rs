use std::sync::Arc;

use datafusion::common::Result;
use datafusion::config::ConfigOptions;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_distributed::{TaskEstimation, TaskEstimator};

pub struct DataclodNonDistributable;

pub fn is_non_distributable_plan_name(name: &str) -> bool {
    matches!(
        name,
        "SpatialJoinExec"
            | "ProbeShuffleExec"
            | "SqlExec"
            | "DuckSqlExec"
            | "MySQLSQLExec"
            | "SQLiteSqlExec"
    )
}

impl TaskEstimator for DataclodNonDistributable {
    fn task_estimation(
        &self, plan: &Arc<dyn ExecutionPlan>, _cfg: &ConfigOptions,
    ) -> Option<TaskEstimation> {
        is_non_distributable_plan_name(plan.name()).then_some(TaskEstimation::maximum(1))
    }

    fn scale_up_leaf_node(
        &self, _plan: &Arc<dyn ExecutionPlan>, _task_count: usize, _cfg: &ConfigOptions,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        Ok(None)
    }
}

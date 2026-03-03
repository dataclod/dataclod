use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::Schema;
use datafusion::common::{DFSchema, JoinSide, Result, exec_err, plan_err};
use datafusion::execution::context::QueryPlanner;
use datafusion::execution::session_state::{SessionState, SessionStateBuilder};
use datafusion::logical_expr::LogicalPlan;
use datafusion::logical_expr::logical_plan::UserDefinedLogicalNode;
use datafusion::physical_expr::create_physical_expr;
use datafusion::physical_plan::joins::NestedLoopJoinExec;
use datafusion::physical_plan::joins::utils::JoinFilter;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties, Partitioning};
use datafusion::physical_planner::{DefaultPhysicalPlanner, ExtensionPlanner, PhysicalPlanner};

use crate::join::exec::SpatialJoinExec;
use crate::join::planner::logical_plan_node::SpatialJoinPlanNode;
use crate::join::planner::spatial_expr_utils::{
    is_spatial_predicate_supported, transform_join_filter,
};
use crate::join::spatial_predicate::SpatialPredicate;
use crate::option::DataClodOptions;

/// Registers a query planner that can produce [`SpatialJoinExec`] from a
/// logical extension node.
pub fn register_spatial_join_planner(builder: SessionStateBuilder) -> SessionStateBuilder {
    let extension_planners: Vec<Arc<dyn ExtensionPlanner + Send + Sync>> =
        vec![Arc::new(SpatialJoinExtensionPlanner {})];
    builder.with_query_planner(Arc::new(DataClodSpatialQueryPlanner { extension_planners }))
}

/// Query planner that enables `DataClod`'s spatial join planning.
///
/// Installs an [`ExtensionPlanner`] that recognizes `SpatialJoinPlanNode` and
/// produces `SpatialJoinExec` when supported and enabled.
struct DataClodSpatialQueryPlanner {
    extension_planners: Vec<Arc<dyn ExtensionPlanner + Send + Sync>>,
}

impl fmt::Debug for DataClodSpatialQueryPlanner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DataClodSpatialQueryPlanner").finish()
    }
}

#[async_trait]
impl QueryPlanner for DataClodSpatialQueryPlanner {
    async fn create_physical_plan(
        &self, logical_plan: &LogicalPlan, session_state: &SessionState,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let physical_planner =
            DefaultPhysicalPlanner::with_extension_planners(self.extension_planners.clone());
        physical_planner
            .create_physical_plan(logical_plan, session_state)
            .await
    }
}

/// Physical planner hook for `SpatialJoinPlanNode`.
struct SpatialJoinExtensionPlanner;

#[async_trait]
impl ExtensionPlanner for SpatialJoinExtensionPlanner {
    async fn plan_extension(
        &self, _planner: &dyn PhysicalPlanner, node: &dyn UserDefinedLogicalNode,
        logical_inputs: &[&LogicalPlan], physical_inputs: &[Arc<dyn ExecutionPlan>],
        session_state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        let Some(spatial_node) = node.as_any().downcast_ref::<SpatialJoinPlanNode>() else {
            return Ok(None);
        };

        let Some(ext) = session_state
            .config_options()
            .extensions
            .get::<DataClodOptions>()
        else {
            return exec_err!("DataClodOptions not found in session state extensions");
        };

        if !ext.spatial_join.enable {
            return exec_err!("Spatial join is disabled in DataClodOptions");
        }

        if logical_inputs.len() != 2 || physical_inputs.len() != 2 {
            return plan_err!("SpatialJoinPlanNode expects 2 inputs");
        }

        let join_type = &spatial_node.join_type;

        let (physical_left, physical_right) =
            (physical_inputs[0].clone(), physical_inputs[1].clone());

        let join_filter = logical_join_filter_to_physical(
            spatial_node,
            session_state,
            &physical_left,
            &physical_right,
        )?;

        let Some((spatial_predicate, remainder)) = transform_join_filter(&join_filter) else {
            let nlj = NestedLoopJoinExec::try_new(
                physical_left,
                physical_right,
                Some(join_filter),
                join_type,
                None,
            )?;
            return Ok(Some(Arc::new(nlj)));
        };

        if !is_spatial_predicate_supported(
            &spatial_predicate,
            &physical_left.schema(),
            &physical_right.schema(),
        )? {
            let nlj = NestedLoopJoinExec::try_new(
                physical_left,
                physical_right,
                Some(join_filter),
                join_type,
                None,
            )?;
            return Ok(Some(Arc::new(nlj)));
        }

        let should_swap = join_type.supports_swap()
            && should_swap_join_order(physical_left.as_ref(), physical_right.as_ref())?;

        // Repartition the probe side when enabled. This breaks spatial locality in
        // sorted/skewed datasets, leading to more balanced workloads during
        // out-of-core spatial join. We determine which pre-swap input will be
        // the probe AFTER any potential swap, and repartition it here.
        // swap_inputs() will then carry the RepartitionExec to the correct
        // child position.
        let (physical_left, physical_right) = if ext.spatial_join.repartition_probe_side {
            repartition_probe_side(
                physical_left,
                physical_right,
                &spatial_predicate,
                should_swap,
            )?
        } else {
            (physical_left, physical_right)
        };

        let exec = SpatialJoinExec::try_new(
            physical_left,
            physical_right,
            spatial_predicate,
            remainder,
            join_type,
            None,
            &ext.spatial_join,
        )?;

        if should_swap {
            exec.swap_inputs().map(Some)
        } else {
            Ok(Some(Arc::new(exec)))
        }
    }
}

fn should_swap_join_order(left: &dyn ExecutionPlan, right: &dyn ExecutionPlan) -> Result<bool> {
    let left_stats = left.partition_statistics(None)?;
    let right_stats = right.partition_statistics(None)?;

    match (
        left_stats.total_byte_size.get_value(),
        right_stats.total_byte_size.get_value(),
    ) {
        (Some(l), Some(r)) => Ok(l > r),
        _ => {
            match (
                left_stats.num_rows.get_value(),
                right_stats.num_rows.get_value(),
            ) {
                (Some(l), Some(r)) => Ok(l > r),
                _ => Ok(false),
            }
        }
    }
}

/// This function is mostly taken from the match arm for handling
/// `LogicalPlan::Join` i<https://github.com/apache/datafusion/blob/51.0.0/datafusion/core/src/physical_planner.rs#L1144-L1245>45
fn logical_join_filter_to_physical(
    plan_node: &SpatialJoinPlanNode, session_state: &SessionState,
    physical_left: &Arc<dyn ExecutionPlan>, physical_right: &Arc<dyn ExecutionPlan>,
) -> Result<JoinFilter> {
    let SpatialJoinPlanNode {
        left,
        right,
        filter,
        ..
    } = plan_node;

    let left_df_schema = left.schema();
    let right_df_schema = right.schema();

    // Extract columns from filter expression and saved in a HashSet
    let cols = filter.column_refs();

    // Collect left & right field indices, the field indices are sorted in ascending
    // order
    let mut left_field_indices = cols
        .iter()
        .filter_map(|c| left_df_schema.index_of_column(c).ok())
        .collect::<Vec<_>>();
    left_field_indices.sort_unstable();

    let mut right_field_indices = cols
        .iter()
        .filter_map(|c| right_df_schema.index_of_column(c).ok())
        .collect::<Vec<_>>();
    right_field_indices.sort_unstable();

    // Collect DFFields and Fields required for intermediate schemas
    let (filter_df_fields, filter_fields): (Vec<_>, Vec<_>) = left_field_indices
        .clone()
        .into_iter()
        .map(|i| {
            (
                left_df_schema.qualified_field(i),
                physical_left.schema().field(i).clone(),
            )
        })
        .chain(right_field_indices.clone().into_iter().map(|i| {
            (
                right_df_schema.qualified_field(i),
                physical_right.schema().field(i).clone(),
            )
        }))
        .unzip();
    let filter_df_fields = filter_df_fields
        .into_iter()
        .map(|(qualifier, field)| (qualifier.cloned(), Arc::new(field.clone())))
        .collect::<Vec<_>>();

    let metadata: HashMap<_, _> = left_df_schema
        .metadata()
        .clone()
        .into_iter()
        .chain(right_df_schema.metadata().clone())
        .collect();

    // Construct intermediate schemas used for filtering data and
    // convert logical expression to physical according to filter schema
    let filter_df_schema = DFSchema::new_with_metadata(filter_df_fields, metadata.clone())?;
    let filter_schema = Schema::new_with_metadata(filter_fields, metadata);

    let filter_expr =
        create_physical_expr(filter, &filter_df_schema, session_state.execution_props())?;
    let column_indices = JoinFilter::build_column_indices(left_field_indices, right_field_indices);

    let join_filter = JoinFilter::new(filter_expr, column_indices, Arc::new(filter_schema));
    Ok(join_filter)
}

/// Repartition the probe side of a spatial join using `RoundRobinBatch`
/// partitioning.
///
/// The purpose is to break spatial locality in sorted or skewed datasets, which
/// can cause imbalanced partitions when running out-of-core spatial join. The
/// number of partitions is preserved; only the distribution of rows across
/// partitions is shuffled.
///
/// The `should_swap` parameter indicates whether `swap_inputs()` will be called
/// after `SpatialJoinExec` is constructed.
fn repartition_probe_side(
    mut physical_left: Arc<dyn ExecutionPlan>, mut physical_right: Arc<dyn ExecutionPlan>,
    spatial_predicate: &SpatialPredicate, should_swap: bool,
) -> Result<(Arc<dyn ExecutionPlan>, Arc<dyn ExecutionPlan>)> {
    let probe_plan = {
        // For Relation/Distance predicates, probe is always Right after swap.
        // If should_swap, the current left will be moved to the right (probe) by
        // swap_inputs().
        if should_swap {
            &mut physical_left
        } else {
            &mut physical_right
        }
    };

    let num_partitions = probe_plan.output_partitioning().partition_count();
    *probe_plan = Arc::new(RepartitionExec::try_new(
        probe_plan.clone(),
        Partitioning::RoundRobinBatch(num_partitions),
    )?);

    Ok((physical_left, physical_right))
}

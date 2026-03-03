use std::sync::Arc;

use datafusion::common::tree_node::Transformed;
use datafusion::common::{NullEquality, Result};
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::logical_expr::logical_plan::Extension;
use datafusion::logical_expr::{BinaryExpr, Expr, Filter, Join, JoinType, LogicalPlan, Operator};
use datafusion::optimizer::{ApplyOrder, OptimizerConfig, OptimizerRule};

use crate::join::planner::logical_plan_node::SpatialJoinPlanNode;
use crate::join::planner::spatial_expr_utils::{
    collect_spatial_predicate_names, is_spatial_predicate,
};
use crate::option::DataClodOptions;

/// Register only the logical spatial join optimizer rule.
///
/// This enables building `Join(filter=...)` from patterns like
/// `Filter(CrossJoin)`. It intentionally does not register any physical plan
/// rewrite rules.
pub fn register_spatial_join_logical_optimizer(
    session_state_builder: SessionStateBuilder,
) -> SessionStateBuilder {
    session_state_builder
        .with_optimizer_rule(Arc::new(MergeSpatialProjectionIntoJoin))
        .with_optimizer_rule(Arc::new(SpatialJoinLogicalRewrite))
}
/// Logical optimizer rule that enables spatial join planning.
///
/// This rule turns eligible `Join(filter=...)` nodes into a
/// `SpatialJoinPlanNode` extension.
#[derive(Default, Debug)]
struct SpatialJoinLogicalRewrite;

impl OptimizerRule for SpatialJoinLogicalRewrite {
    fn name(&self) -> &str {
        "spatial_join_logical_rewrite"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::BottomUp)
    }

    fn supports_rewrite(&self) -> bool {
        true
    }

    fn rewrite(
        &self, plan: LogicalPlan, config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        let options = config.options();
        let Some(ext) = options.extensions.get::<DataClodOptions>() else {
            return Ok(Transformed::no(plan));
        };
        if !ext.spatial_join.enable {
            return Ok(Transformed::no(plan));
        }

        let LogicalPlan::Join(join) = &plan else {
            return Ok(Transformed::no(plan));
        };

        // only rewrite joins that already have a spatial predicate in `filter`.
        let Some(filter) = join.filter.as_ref() else {
            return Ok(Transformed::no(plan));
        };

        let spatial_predicate_names = collect_spatial_predicate_names(filter);
        if spatial_predicate_names.is_empty() {
            return Ok(Transformed::no(plan));
        }

        // Join with with equi-join condition and spatial join condition.
        if !join.on.is_empty() {
            return Ok(Transformed::no(plan));
        }

        // Build new filter expression including equi-join conditions
        let filter = filter.clone();
        let eq_op = if join.null_equality == NullEquality::NullEqualsNothing {
            Operator::Eq
        } else {
            Operator::IsNotDistinctFrom
        };
        let filter = join.on.iter().fold(filter, |acc, (l, r)| {
            let eq_expr = Expr::BinaryExpr(BinaryExpr::new(
                Box::new(l.clone()),
                eq_op,
                Box::new(r.clone()),
            ));
            Expr::and(acc, eq_expr)
        });

        let schema = join.schema.clone();
        let node = SpatialJoinPlanNode {
            left: join.left.as_ref().clone(),
            right: join.right.as_ref().clone(),
            join_type: join.join_type,
            filter,
            schema,
            join_constraint: join.join_constraint,
            null_equality: join.null_equality,
        };

        Ok(Transformed::yes(LogicalPlan::Extension(Extension {
            node: Arc::new(node),
        })))
    }
}

/// Logical optimizer rule that enables spatial join planning.
///
/// This rule turns eligible `Filter(Join(filter=...))` nodes into a
/// `Join(filter=...)` node, so that the spatial join can be rewritten later by
/// [`SpatialJoinLogicalRewrite`].
#[derive(Debug, Default)]
struct MergeSpatialProjectionIntoJoin;

impl OptimizerRule for MergeSpatialProjectionIntoJoin {
    fn name(&self) -> &str {
        "merge_spatial_filter_into_join"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::BottomUp)
    }

    /// Try to rewrite the plan containing a spatial Filter on top of a cross
    /// join without on or filter to a theta-join with filter. For instance,
    /// the following query plan:
    ///
    /// ```text
    /// Filter: st_intersects(l.geom, _scalar_sq_1.geom)
    ///   Left Join (no on, no filter):
    ///     TableScan: l projection=[id, geom]
    ///     SubqueryAlias: __scalar_sq_1
    ///       Projection: r.geom
    ///         Filter: r.id = Int32(1)
    ///           TableScan: r projection=[id, geom]
    /// ```
    ///
    /// will be rewritten to
    ///
    /// ```text
    /// Inner Join: Filter: st_intersects(l.geom, _scalar_sq_1.geom)
    ///   TableScan: l projection=[id, geom]
    ///   SubqueryAlias: __scalar_sq_1
    ///     Projection: r.geom
    ///       Filter: r.id = Int32(1)
    ///         TableScan: r projection=[id, geom]
    /// ```
    ///
    /// This is for enabling this logical join operator to be converted to a
    /// [`SpatialJoinPlanNode`] by`SpatialJoinLogicalRewrite`te], so that it
    /// could subsequently be optimized to a `SpatialJoin` physical node.
    fn rewrite(
        &self, plan: LogicalPlan, config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        let options = config.options();
        let Some(extension) = options.extensions.get::<DataClodOptions>() else {
            return Ok(Transformed::no(plan));
        };
        if !extension.spatial_join.enable {
            return Ok(Transformed::no(plan));
        }

        let LogicalPlan::Filter(Filter {
            predicate, input, ..
        }) = &plan
        else {
            return Ok(Transformed::no(plan));
        };
        if !is_spatial_predicate(predicate) {
            return Ok(Transformed::no(plan));
        }

        let LogicalPlan::Join(Join {
            left,
            right,
            on,
            filter,
            join_type,
            join_constraint,
            null_equality,
            ..
        }) = input.as_ref()
        else {
            return Ok(Transformed::no(plan));
        };

        // Check if this is a suitable join for rewriting
        if !matches!(
            join_type,
            JoinType::Inner | JoinType::Left | JoinType::Right
        ) || !on.is_empty()
            || filter.is_some()
        {
            return Ok(Transformed::no(plan));
        }

        let rewritten_plan = Join::try_new(
            left.clone(),
            right.clone(),
            on.clone(),
            Some(predicate.clone()),
            JoinType::Inner,
            *join_constraint,
            *null_equality,
        )?;

        Ok(Transformed::yes(LogicalPlan::Join(rewritten_plan)))
    }
}

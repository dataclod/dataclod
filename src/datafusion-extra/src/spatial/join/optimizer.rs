use std::sync::Arc;

use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{HashMap, JoinSide, Result};
use datafusion::config::ConfigOptions;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::logical_expr::{Filter, Join, JoinType, LogicalPlan, Operator};
use datafusion::optimizer::{ApplyOrder, OptimizerConfig, OptimizerRule};
use datafusion::physical_expr::expressions::{BinaryExpr, Column};
use datafusion::physical_expr::{PhysicalExpr, ScalarFunctionExpr};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_optimizer::sanity_checker::SanityCheckPlan;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::joins::NestedLoopJoinExec;
use datafusion::physical_plan::joins::utils::{ColumnIndex, JoinFilter};
use datafusion::prelude::Expr;

use crate::spatial::join::exec::SpatialJoinExec;
use crate::spatial::join::spatial_predicate::{
    DistancePredicate, RelationPredicate, SpatialPredicate, SpatialRelationType,
};

/// Physical planner extension for spatial joins
///
/// This extension recognizes nested loop join operations with spatial
/// predicates and converts them to `SpatialJoinExec`, which is specially
/// optimized for spatial joins.
#[derive(Debug)]
pub struct SpatialJoinOptimizer;

impl PhysicalOptimizerRule for SpatialJoinOptimizer {
    fn optimize(
        &self, plan: Arc<dyn ExecutionPlan>, _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let transformed = plan.transform_up(|plan| self.try_optimize_join(plan))?;
        Ok(transformed.data)
    }

    /// A human readable name for this optimizer rule
    fn name(&self) -> &str {
        "spatial_join_optimizer"
    }

    /// A flag to indicate whether the physical planner should valid the rule
    /// will not change the schema of the plan after the rewriting.
    /// Some of the optimization rules might change the nullable properties of
    /// the schema and should disable the schema check.
    fn schema_check(&self) -> bool {
        true
    }
}

impl OptimizerRule for SpatialJoinOptimizer {
    fn name(&self) -> &str {
        "spatial_join_optimizer"
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
    /// `NestedLoopJoin` physical node with a spatial predicate, so that it
    /// could subsequently be optimized to a `SpatialJoin` physical node.
    /// Please refer to the `PhysicalOptimizerRule` implementation of this
    /// struct and [`SpatialJoinOptimizer::try_optimize_join`] for details.
    fn rewrite(
        &self, plan: LogicalPlan, _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
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

/// Check if a given logical expression contains a spatial predicate component
/// or not. We assume that the given `expr` evaluates to a boolean value and
/// originates from a filter logical node.
fn is_spatial_predicate(expr: &Expr) -> bool {
    fn is_distance_expr(expr: &Expr) -> bool {
        let Expr::ScalarFunction(datafusion::logical_expr::expr::ScalarFunction { func, .. }) =
            expr
        else {
            return false;
        };
        func.name().to_lowercase() == "st_distance"
    }

    match expr {
        Expr::BinaryExpr(datafusion::logical_expr::expr::BinaryExpr { left, right, op }) => {
            match op {
                Operator::And => is_spatial_predicate(left) || is_spatial_predicate(right),
                Operator::Lt | Operator::LtEq => is_distance_expr(left),
                Operator::Gt | Operator::GtEq => is_distance_expr(right),
                _ => false,
            }
        }
        Expr::ScalarFunction(datafusion::logical_expr::expr::ScalarFunction { func, .. }) => {
            let func_name = func.name().to_lowercase();
            matches!(
                func_name.as_str(),
                "st_intersects"
                    | "st_contains"
                    | "st_within"
                    | "st_covers"
                    | "st_covered_by"
                    | "st_coveredby"
                    | "st_touches"
                    | "st_crosses"
                    | "st_overlaps"
                    | "st_equals"
                    | "st_dwithin"
                    | "st_knn"
            )
        }
        _ => false,
    }
}

impl SpatialJoinOptimizer {
    /// Rewrite `plan` containing `NestedLoopJoinExec` with spatial predicates
    /// to `SpatialJoinExec`.
    fn try_optimize_join(
        &self, plan: Arc<dyn ExecutionPlan>,
    ) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
        // Check if this is a NestedLoopJoinExec that we can convert to spatial join
        if let Some(nested_loop_join) = plan.as_any().downcast_ref::<NestedLoopJoinExec>()
            && let Some(spatial_join) = self.try_convert_to_spatial_join(nested_loop_join)?
        {
            return Ok(Transformed::yes(spatial_join));
        }

        // No optimization applied, return the original plan
        Ok(Transformed::no(plan))
    }

    /// Try to convert a `NestedLoopJoinExec` with spatial predicates as join
    /// condition to a `SpatialJoinExec`. `SpatialJoinExec` executes the query
    /// using an optimized algorithm, which is more efficient than
    /// `NestedLoopJoinExec`.
    fn try_convert_to_spatial_join(
        &self, nested_loop_join: &NestedLoopJoinExec,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        if let Some(join_filter) = nested_loop_join.filter()
            && let Some((spatial_predicate, remainder)) = transform_join_filter(join_filter)
        {
            // The left side of the nested loop join is required to have only one partition,
            // while SpatialJoinExec does not have that requirement.
            // SpatialJoinExec can consume the streams on the build side in parallel
            // when the build side has multiple partitions.
            // If the left side is a CoalescePartitionsExec, we can drop the
            // CoalescePartitionsExec and directly use the input.
            let left = nested_loop_join.left();
            let left = if let Some(coalesce_partitions) =
                left.as_any().downcast_ref::<CoalescePartitionsExec>()
            {
                // Remove unnecessary CoalescePartitionsExec for spatial joins
                coalesce_partitions.input()
            } else {
                left
            };

            let left = left.clone();
            let right = nested_loop_join.right().clone();
            let join_type = nested_loop_join.join_type();

            // Create the spatial join
            let spatial_join = SpatialJoinExec::try_new(
                left,
                right,
                spatial_predicate,
                remainder,
                join_type,
                nested_loop_join.projection().cloned(),
            )?;

            return Ok(Some(Arc::new(spatial_join)));
        }

        Ok(None)
    }
}

/// Helper function to register the spatial join optimizer with a session state
pub fn register_spatial_join_optimizer(
    session_state_builder: SessionStateBuilder,
) -> SessionStateBuilder {
    session_state_builder
        .with_optimizer_rule(Arc::new(SpatialJoinOptimizer))
        .with_physical_optimizer_rule(Arc::new(SpatialJoinOptimizer))
        .with_physical_optimizer_rule(Arc::new(SanityCheckPlan::new()))
}

/// Transform the join filter to a spatial predicate and a remainder.
///
///   * The spatial predicate is a spatial predicate that is extracted from the
///     join filter.
///   * The remainder is everything other than the spatial predicate.
///
/// The remainder may reference fewer columns than the original join filter. If
/// that's the case, the columns that are not referenced by the remainder will
/// be pruned.
fn transform_join_filter(
    join_filter: &JoinFilter,
) -> Option<(SpatialPredicate, Option<JoinFilter>)> {
    let (spatial_predicate, remainder) =
        extract_spatial_predicate(join_filter.expression(), join_filter.column_indices())?;

    let remainder = remainder
        .as_ref()
        .map(|remainder| replace_join_filter_expr(remainder, join_filter));

    Some((spatial_predicate, remainder))
}

/// Extract the spatial predicate from the join filter. The extracted spatial
/// predicate and the remaining filter are returned.
fn extract_spatial_predicate(
    expr: &Arc<dyn PhysicalExpr>, column_indices: &[ColumnIndex],
) -> Option<(SpatialPredicate, Option<Arc<dyn PhysicalExpr>>)> {
    if let Some(scalar_fn) = expr.as_any().downcast_ref::<ScalarFunctionExpr>()
        && let Some(relation_predicate) = match_relation_predicate(scalar_fn, column_indices)
    {
        return Some((SpatialPredicate::Relation(relation_predicate), None));
    }

    if let Some(distance_predicate) = match_distance_predicate(expr, column_indices) {
        return Some((SpatialPredicate::Distance(distance_predicate), None));
    }

    if let Some(binary_expr) = expr.as_any().downcast_ref::<BinaryExpr>() {
        if !matches!(binary_expr.op(), Operator::And) {
            return None;
        }

        let left = binary_expr.left();
        let right = binary_expr.right();

        // Try to extract the spatial predicate from the left side
        if let Some((spatial_predicate, remainder)) =
            extract_spatial_predicate(left, column_indices)
        {
            let combined_remainder = match remainder {
                Some(remainder) => {
                    Arc::new(BinaryExpr::new(remainder, Operator::And, right.clone()))
                }
                None => right.clone(),
            };
            return Some((spatial_predicate, Some(combined_remainder)));
        }

        // Left side is not a spatial predicate, try to extract the spatial predicate
        // from the right side
        if let Some((spatial_predicate, remainder)) =
            extract_spatial_predicate(right, column_indices)
        {
            let combined_remainder = match remainder {
                Some(remainder) => {
                    Arc::new(BinaryExpr::new(left.clone(), Operator::And, remainder))
                }
                None => left.clone(),
            };
            return Some((spatial_predicate, Some(combined_remainder)));
        }
    }

    None
}

/// Match the scalar function expression to a spatial relation predicate such as
/// `ST_Intersects(lhs.geom`, rhs.geom). The input arguments of the ST_ function
/// should reference columns from different sides.
fn match_relation_predicate(
    scalar_fn: &ScalarFunctionExpr, column_indices: &[ColumnIndex],
) -> Option<RelationPredicate> {
    if let Some(relation_type) = SpatialRelationType::from_name(scalar_fn.fun().name()) {
        // Try to find the expressions that evaluates to the arguments of the spatial
        // function
        let args = scalar_fn.args();
        assert!(args.len() >= 2);
        let arg0 = &args[0];
        let arg1 = &args[1];

        // Try to find the expressions that evaluates to the arguments of the spatial
        // function
        let arg0_refs = collect_column_references(arg0, column_indices);
        let arg1_refs = collect_column_references(arg1, column_indices);

        let (arg0_side, arg1_side) = resolve_column_reference_sides(&arg0_refs, &arg1_refs)?;
        let arg0_reprojected =
            reproject_column_references_for_side(arg0, column_indices, arg0_side);
        let arg1_reprojected =
            reproject_column_references_for_side(arg1, column_indices, arg1_side);

        return match (arg0_side, arg1_side) {
            (JoinSide::Left, JoinSide::Right) => {
                Some(RelationPredicate::new(
                    arg0_reprojected,
                    arg1_reprojected,
                    relation_type,
                ))
            }
            (JoinSide::Right, JoinSide::Left) => {
                // The spatial predicate needs to be inverted
                Some(RelationPredicate::new(
                    arg1_reprojected,
                    arg0_reprojected,
                    relation_type.invert(),
                ))
            }
            _ => None,
        };
    }
    None
}

/// Match the scalar function expression to a distance predicate such as
/// `ST_DWithin(geom1`, geom2, distance) or `ST_Distance(geom1`, geom2) <=
/// distance. The geometry input arguments of the ST_ function should reference
/// columns from different sides. The distance input argument should not
/// reference columns from both sides simultaneously.
fn match_distance_predicate(
    expr: &Arc<dyn PhysicalExpr>, column_indices: &[ColumnIndex],
) -> Option<DistancePredicate> {
    // There are 3 forms of distance predicates:
    // 1. st_dwithin(geom1, geom2, distance)
    // 2. st_distance(geom1, geom2) <= distance or st_distance(geom1, geom2) <
    //    distance
    // 3. distance >= st_distance(geom1, geom2) or distance > st_distance(geom1,
    //    geom2)
    let (arg0, arg1, distance_bound_expr) =
        if let Some(binary_expr) = expr.as_any().downcast_ref::<BinaryExpr>() {
            // handle case 2. and 3.
            let left = binary_expr.left();
            let right = binary_expr.right();
            let (st_distance_expr, distance_bound_expr) = match *binary_expr.op() {
                Operator::Lt | Operator::LtEq => (left, right),
                Operator::Gt | Operator::GtEq => (right, left),
                _ => return None,
            };

            if let Some(st_distance_expr) = st_distance_expr
                .as_any()
                .downcast_ref::<ScalarFunctionExpr>()
            {
                if st_distance_expr.fun().name() != "st_distance" {
                    return None;
                }

                let args = st_distance_expr.args();
                assert!(args.len() >= 2);
                (&args[0], &args[1], distance_bound_expr)
            } else {
                return None;
            }
        } else if let Some(st_dwithin_expr) = expr.as_any().downcast_ref::<ScalarFunctionExpr>() {
            // handle case 1.
            if st_dwithin_expr.fun().name() != "st_dwithin" {
                return None;
            }

            let args = st_dwithin_expr.args();
            assert!(args.len() >= 3);
            (&args[0], &args[1], &args[2])
        } else {
            return None;
        };

    // Try to find the expressions that evaluates to the arguments of the spatial
    // function
    let arg0_refs = collect_column_references(arg0, column_indices);
    let arg1_refs = collect_column_references(arg1, column_indices);
    let arg_dist_refs = collect_column_references(distance_bound_expr, column_indices);

    let arg_dist_side = side_of_column_references(&arg_dist_refs)?;
    let (arg0_side, arg1_side) = resolve_column_reference_sides(&arg0_refs, &arg1_refs)?;

    let arg0_reprojected = reproject_column_references_for_side(arg0, column_indices, arg0_side);
    let arg1_reprojected = reproject_column_references_for_side(arg1, column_indices, arg1_side);
    let arg_dist_reprojected =
        reproject_column_references_for_side(distance_bound_expr, column_indices, arg_dist_side);

    match (arg0_side, arg1_side) {
        (JoinSide::Left, JoinSide::Right) => {
            Some(DistancePredicate::new(
                arg0_reprojected,
                arg1_reprojected,
                arg_dist_reprojected,
                arg_dist_side,
            ))
        }
        (JoinSide::Right, JoinSide::Left) => {
            Some(DistancePredicate::new(
                arg1_reprojected,
                arg0_reprojected,
                arg_dist_reprojected,
                arg_dist_side,
            ))
        }
        _ => None,
    }
}

fn collect_column_references(
    expr: &Arc<dyn PhysicalExpr>, column_indices: &[ColumnIndex],
) -> Vec<ColumnIndex> {
    let mut collected_column_indices = Vec::with_capacity(column_indices.len());

    expr.apply(|node| {
        if let Some(column) = node.as_any().downcast_ref::<Column>() {
            let intermediate_index = column.index();
            let column_info = &column_indices[intermediate_index];
            collected_column_indices.push(column_info.clone());
        }

        Ok(datafusion::common::tree_node::TreeNodeRecursion::Continue)
    })
    .expect("Failed to collect column references");

    collected_column_indices
}

fn resolve_column_reference_sides(
    left_refs: &[ColumnIndex], right_refs: &[ColumnIndex],
) -> Option<(JoinSide, JoinSide)> {
    let left_side = side_of_column_references(left_refs)?;
    let right_side = side_of_column_references(right_refs)?;

    if left_side != right_side {
        Some((left_side, right_side))
    } else {
        None
    }
}

fn side_of_column_references(column_indices: &[ColumnIndex]) -> Option<JoinSide> {
    match column_indices.first() {
        Some(first) => {
            let first_side = first.side;
            if column_indices
                .iter()
                .all(|col_idx| col_idx.side == first_side)
            {
                Some(first_side)
            } else {
                // Referencing both sides simultaneously
                None
            }
        }
        None => Some(JoinSide::None),
    }
}

fn reproject_column_references(
    expr: &Arc<dyn PhysicalExpr>, index_map: &HashMap<usize, usize>,
) -> Arc<dyn PhysicalExpr> {
    expr.clone()
        .transform_down(|node| {
            // Check if this is a Column expression
            if let Some(column) = node.as_any().downcast_ref::<Column>() {
                let old_index = column.index();
                if let Some(&new_index) = index_map.get(&old_index) {
                    // Create a new Column with the mapped index
                    let new_column = Arc::new(Column::new(column.name(), new_index));
                    return Ok(Transformed::yes(new_column));
                }
            }

            // For all other expressions, continue with the default traversal
            Ok(Transformed::no(node))
        })
        .unwrap_or_else(|_| Transformed::no(expr.clone()))
        .data
}

fn reproject_column_references_for_side(
    expr: &Arc<dyn PhysicalExpr>, column_indices: &[ColumnIndex], side: JoinSide,
) -> Arc<dyn PhysicalExpr> {
    if side == JoinSide::None {
        return expr.clone();
    }

    let index_mapping: HashMap<usize, usize> = column_indices
        .iter()
        .enumerate()
        .filter_map(|(i, col_idx)| (col_idx.side == side).then_some((i, col_idx.index)))
        .collect();

    reproject_column_references(expr, &index_mapping)
}

/// Replace the join filter expression with a new expression. The replaced join
/// filter expression may reference fewer columns than the original join filter
/// expression. If that's the case, the columns that are not referenced by the
/// replaced join filter expression will be pruned.
fn replace_join_filter_expr(expr: &Arc<dyn PhysicalExpr>, join_filter: &JoinFilter) -> JoinFilter {
    let column_indices = join_filter.column_indices();
    let column_refs = collect_column_references(expr, column_indices);

    // column_refs could be a subset of column_indices. If that's the case, we can
    // prune column_indices to only include the columns that are referenced by
    // the remainder.
    let referenced_columns: Vec<_> = column_indices
        .iter()
        .enumerate()
        .filter(|(_, col_idx)| column_refs.contains(col_idx))
        .collect();

    let pruned_column_indices: Vec<_> = referenced_columns
        .iter()
        .map(|(_, col_idx)| (*col_idx).clone())
        .collect();

    let column_index_mapping: HashMap<_, _> = referenced_columns
        .iter()
        .enumerate()
        .map(|(new_idx, (old_idx, _))| (*old_idx, new_idx))
        .collect();

    let project: Vec<_> = referenced_columns
        .iter()
        .map(|(old_idx, _)| *old_idx)
        .collect();

    let pruned_schema = join_filter
        .schema()
        .project(&project)
        .expect("Failed to project schema");
    let remainder_reprojected = reproject_column_references(expr, &column_index_mapping);
    JoinFilter::new(
        remainder_reprojected,
        pruned_column_indices,
        Arc::new(pruned_schema),
    )
}

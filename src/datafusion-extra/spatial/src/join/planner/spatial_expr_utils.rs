use std::collections::HashSet;
use std::sync::Arc;

use datafusion::arrow::datatypes::Schema;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{HashMap, JoinSide, Result, ScalarValue};
use datafusion::logical_expr::{Expr, Operator};
use datafusion::physical_expr::expressions::{BinaryExpr, Column, Literal};
use datafusion::physical_expr::{PhysicalExpr, ScalarFunctionExpr};
use datafusion::physical_plan::joins::utils::{ColumnIndex, JoinFilter};

use crate::join::spatial_predicate::{
    DistancePredicate, RelationPredicate, SpatialPredicate, SpatialRelationType,
};
use crate::utils::{ParsedDistancePredicate, parse_distance_predicate};

/// Collect the names of spatial predicates appeared in expr. We assume that the
/// given `expr` evaluates to a boolean value and originates from a filter
/// logical node.
pub fn collect_spatial_predicate_names(expr: &Expr) -> HashSet<String> {
    fn collect(expr: &Expr, acc: &mut HashSet<String>) {
        match expr {
            Expr::BinaryExpr(datafusion::logical_expr::expr::BinaryExpr { left, right, op }) => {
                match op {
                    Operator::And => {
                        collect(left, acc);
                        collect(right, acc);
                    }
                    Operator::Lt | Operator::LtEq if is_distance_expr(left) => {
                        acc.insert("st_dwithin".to_owned());
                    }
                    Operator::Gt | Operator::GtEq if is_distance_expr(right) => {
                        acc.insert("st_dwithin".to_owned());
                    }
                    _ => (),
                }
            }
            Expr::ScalarFunction(datafusion::logical_expr::expr::ScalarFunction {
                func, ..
            }) => {
                let func_name = func.name().to_lowercase();
                if matches!(
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
                ) {
                    acc.insert(func_name);
                }
            }
            _ => (),
        }
    }

    fn is_distance_expr(expr: &Expr) -> bool {
        let Expr::ScalarFunction(datafusion::logical_expr::expr::ScalarFunction { func, .. }) =
            expr
        else {
            return false;
        };
        func.name().to_lowercase() == "st_distance"
    }

    let mut acc = HashSet::new();
    collect(expr, &mut acc);
    acc
}

/// Check if a given logical expression contains a spatial predicate component
/// or not. We assume that the given `expr` evaluates to a boolean value and
/// originates from a filter logical node.
pub fn is_spatial_predicate(expr: &Expr) -> bool {
    let pred_names = collect_spatial_predicate_names(expr);
    !pred_names.is_empty()
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
pub fn transform_join_filter(
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
/// `ST_Intersects(lhs.geom, rhs.geom)`. The input arguments of the ST_ function
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
/// `ST_DWithin(geom1, geom2, distance)` or `ST_Distance(geom1, geom2) <=
/// distance`. The geometry input arguments of the ST_ function should reference
/// columns from different sides. The distance input argument should not
/// reference columns from both sides simultaneously.
fn match_distance_predicate(
    expr: &Arc<dyn PhysicalExpr>, column_indices: &[ColumnIndex],
) -> Option<DistancePredicate> {
    let ParsedDistancePredicate {
        arg0,
        arg1,
        arg_distance,
    } = parse_distance_predicate(expr)?;

    // Try to find the expressions that evaluates to the arguments of the spatial
    // function
    let arg0_refs = collect_column_references(&arg0, column_indices);
    let arg1_refs = collect_column_references(&arg1, column_indices);
    let arg_dist_refs = collect_column_references(&arg_distance, column_indices);

    let arg_dist_side = side_of_column_references(&arg_dist_refs)?;
    let (arg0_side, arg1_side) = resolve_column_reference_sides(&arg0_refs, &arg1_refs)?;

    let arg0_reprojected = reproject_column_references_for_side(&arg0, column_indices, arg0_side);
    let arg1_reprojected = reproject_column_references_for_side(&arg1, column_indices, arg1_side);
    let arg_dist_reprojected =
        reproject_column_references_for_side(&arg_distance, column_indices, arg_dist_side);

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

/// Extract a literal u32 value from an expression.
/// Returns None if the expression is not a literal integer or if it's out of
/// u32 range.
fn extract_literal_u32(expr: &Arc<dyn PhysicalExpr>) -> Option<u32> {
    let literal = expr.as_any().downcast_ref::<Literal>()?;
    match literal.value() {
        ScalarValue::UInt32(Some(val)) => Some(*val),
        ScalarValue::Int32(Some(val)) if *val >= 0 => Some(*val as u32),
        ScalarValue::Int64(Some(val)) if *val >= 0 && *val <= u32::MAX as i64 => Some(*val as u32),
        ScalarValue::UInt64(Some(val)) if *val <= u32::MAX as u64 => Some(*val as u32),
        _ => None,
    }
}

/// Extract a literal boolean value from an expression.
/// Returns None if the expression is not a literal boolean.
fn extract_literal_bool(expr: &Arc<dyn PhysicalExpr>) -> Option<bool> {
    let literal = expr.as_any().downcast_ref::<Literal>()?;
    match literal.value() {
        ScalarValue::Boolean(Some(val)) => Some(*val),
        _ => None,
    }
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

pub fn is_spatial_predicate_supported(
    spatial_predicate: &SpatialPredicate, left_schema: &Schema, right_schema: &Schema,
) -> Result<bool> {
    /// Only spatial predicates working with planar geometry are supported for
    /// optimization. Geography (spherical) types are explicitly excluded
    /// and will not trigger optimized spatial joins.
    fn is_geometry_type_supported(expr: &Arc<dyn PhysicalExpr>, schema: &Schema) -> Result<bool> {
        let left_return_field = expr.return_field(schema)?;
        Ok(left_return_field
            .metadata()
            .get("target_type")
            .is_some_and(|t| t.to_lowercase().ends_with("geometry")))
    }

    match spatial_predicate {
        SpatialPredicate::Relation(RelationPredicate { left, right, .. })
        | SpatialPredicate::Distance(DistancePredicate { left, right, .. }) => {
            Ok(is_geometry_type_supported(left, left_schema)?
                && is_geometry_type_supported(right, right_schema)?)
        }
    }
}

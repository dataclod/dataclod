mod logical_plan_node;
mod optimizer;
mod physical_planner;
mod spatial_expr_utils;

use datafusion::execution::SessionStateBuilder;

/// Register `DataClod` spatial join planning hooks.
///
/// Enables logical rewrites (to surface join filters) and a query planner
/// extension that can plan `SpatialJoinExec`. This is the primary entry point
/// to leveraging the spatial join implementation provided by this crate and
/// ensures joins created by SQL or using a `DataFrame` API that meet certain
/// conditions (e.g. contain a spatial predicate as a join condition) are
/// executed using the `SpatialJoinExec`.
pub fn register_planner(state_builder: SessionStateBuilder) -> SessionStateBuilder {
    // Enable the logical rewrite that turns Filter(CrossJoin) into Join(filter=...)
    let state_builder = optimizer::register_spatial_join_logical_optimizer(state_builder);

    // Enable planning SpatialJoinExec via an extension node during
    // logical->physical planning.
    physical_planner::register_spatial_join_planner(state_builder)
}

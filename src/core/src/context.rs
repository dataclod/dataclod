use std::sync::Arc;

use anyhow::Result;
use datafusion::dataframe::DataFrame;
use datafusion::execution::context::{SessionConfig, SessionContext, SessionState};
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::logical_expr::{LogicalPlan, LogicalPlanBuilder, Statement};
use datafusion::optimizer::common_subexpr_eliminate::CommonSubexprEliminate;
use datafusion::optimizer::decorrelate_predicate_subquery::DecorrelatePredicateSubquery;
use datafusion::optimizer::eliminate_cross_join::EliminateCrossJoin;
use datafusion::optimizer::eliminate_duplicated_expr::EliminateDuplicatedExpr;
use datafusion::optimizer::eliminate_filter::EliminateFilter;
use datafusion::optimizer::eliminate_join::EliminateJoin;
use datafusion::optimizer::eliminate_limit::EliminateLimit;
use datafusion::optimizer::eliminate_nested_union::EliminateNestedUnion;
use datafusion::optimizer::eliminate_one_union::EliminateOneUnion;
use datafusion::optimizer::eliminate_outer_join::EliminateOuterJoin;
use datafusion::optimizer::extract_equijoin_predicate::ExtractEquijoinPredicate;
use datafusion::optimizer::filter_null_join_keys::FilterNullJoinKeys;
use datafusion::optimizer::optimize_projections::OptimizeProjections;
use datafusion::optimizer::propagate_empty_relation::PropagateEmptyRelation;
use datafusion::optimizer::push_down_filter::PushDownFilter;
use datafusion::optimizer::push_down_limit::PushDownLimit;
use datafusion::optimizer::replace_distinct_aggregate::ReplaceDistinctWithAggregate;
use datafusion::optimizer::rewrite_disjunctive_predicate::RewriteDisjunctivePredicate;
use datafusion::optimizer::scalar_subquery_to_join::ScalarSubqueryToJoin;
use datafusion::optimizer::single_distinct_to_groupby::SingleDistinctToGroupBy;
use datafusion::optimizer::unwrap_cast_in_comparison::UnwrapCastInComparison;

pub struct QueryContext {
    inner: SessionContext,
}

impl Default for QueryContext {
    fn default() -> Self {
        Self::new()
    }
}

impl QueryContext {
    pub fn new() -> Self {
        let config = SessionConfig::new().with_information_schema(true);
        let runtime = Arc::new(RuntimeEnv::default());
        // HACK: remove SimplifyExpressions from default optimizer rules beaceuse of
        // datafusion's bug
        let state = SessionState::new_with_config_rt(config, runtime).with_optimizer_rules(vec![
            Arc::new(EliminateNestedUnion::new()),
            Arc::new(UnwrapCastInComparison::new()),
            Arc::new(ReplaceDistinctWithAggregate::new()),
            Arc::new(EliminateJoin::new()),
            Arc::new(DecorrelatePredicateSubquery::new()),
            Arc::new(ScalarSubqueryToJoin::new()),
            Arc::new(ExtractEquijoinPredicate::new()),
            Arc::new(RewriteDisjunctivePredicate::new()),
            Arc::new(EliminateDuplicatedExpr::new()),
            Arc::new(EliminateFilter::new()),
            Arc::new(EliminateCrossJoin::new()),
            Arc::new(CommonSubexprEliminate::new()),
            Arc::new(EliminateLimit::new()),
            Arc::new(PropagateEmptyRelation::new()),
            Arc::new(EliminateOneUnion::new()),
            Arc::new(FilterNullJoinKeys::default()),
            Arc::new(EliminateOuterJoin::new()),
            Arc::new(PushDownLimit::new()),
            Arc::new(PushDownFilter::new()),
            Arc::new(SingleDistinctToGroupBy::new()),
            Arc::new(UnwrapCastInComparison::new()),
            Arc::new(CommonSubexprEliminate::new()),
            Arc::new(OptimizeProjections::new()),
        ]);
        let ctx = SessionContext::new_with_state(state);
        datafusion_extra::catalog::with_pg_catalog(&ctx).expect("Failed to register pg_catalog");
        datafusion_extra::sqlbuiltin::register_udtf(&ctx);
        datafusion_extra::sqlbuiltin::register_udf(&ctx);
        crate::expr::register_udtf(&ctx);

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

use std::fmt::Formatter;
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{JoinSide, JoinType, Result, project_schema};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::equivalence::{ProjectionMapping, join_equivalence_properties};
use datafusion::physical_plan::common::can_project;
use datafusion::physical_plan::execution_plan::EmissionType;
use datafusion::physical_plan::expressions::{BinaryExpr, Column};
use datafusion::physical_plan::joins::utils::{
    ColumnIndex, JoinFilter, build_join_schema, check_join_is_valid, reorder_output_after_swap,
    swap_join_projection,
};
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::projection::{
    EmbeddedProjection, ProjectionExec, try_embed_projection,
};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, Partitioning,
    PhysicalExpr, PlanProperties,
};
use parking_lot::Mutex;

use crate::join::index::spatial_index::SpatialIndex;
use crate::join::index::spatial_index_builder::SpatialJoinBuildMetrics;
use crate::join::prepare::{SpatialJoinComponents, SpatialJoinComponentsBuilder};
use crate::join::spatial_predicate::{SpatialPredicate, SpatialPredicateTrait};
use crate::join::stream::{SpatialJoinProbeMetrics, SpatialJoinStream};
use crate::join::utils::join_utils::{
    JoinPushdownData, asymmetric_join_output_partitioning, boundedness_from_children,
    compute_join_emission_type, try_pushdown_through_join,
};
use crate::join::utils::once_fut::OnceAsync;
use crate::option::{DataClodOptions, SpatialJoinOptions};

/// Type alias for build and probe execution plans
type BuildProbePlans<'a> = (&'a Arc<dyn ExecutionPlan>, &'a Arc<dyn ExecutionPlan>);

/// Physical execution plan for performing spatial joins between two tables. It
/// uses a spatial index to speed up the join operation.
///
/// ## Algorithm Overview
///
/// The spatial join execution follows a hash-join-like pattern:
/// 1. **Build Phase**: The left (smaller) table geometries are indexed using a
///    spatial index
/// 2. **Probe Phase**: Each geometry from the right table is used to query the
///    spatial index
/// 3. **Refinement**: Candidate pairs from the index are refined using exact
///    spatial predicates
/// 4. **Output**: Matching pairs are combined according to the specified join
///    type
#[derive(Debug)]
pub struct SpatialJoinExec {
    /// left (build) side which gets hashed
    pub left: Arc<dyn ExecutionPlan>,
    /// right (probe) side which are filtered by the hash table
    pub right: Arc<dyn ExecutionPlan>,
    /// Primary spatial join condition (the expression in the ON clause of the
    /// join)
    pub on: SpatialPredicate,
    /// Additional filters which are applied while finding matching rows. It
    /// could contain part of the ON clause, or expressions in the WHERE
    /// clause.
    pub filter: Option<JoinFilter>,
    /// How the join is performed (`OUTER`, `INNER`, etc)
    pub join_type: JoinType,
    /// The schema after join. Please be careful when using this schema,
    /// if there is a projection, the schema isn't the same as the output
    /// schema.
    join_schema: SchemaRef,
    metrics: ExecutionPlanMetricsSet,
    /// The projection indices of the columns in the output schema of join
    projection: Option<Vec<usize>>,
    /// Information of index and left / right placement of columns
    column_indices: Vec<ColumnIndex>,
    /// Cache holding plan properties like equivalences, output partitioning
    /// etc.
    cache: PlanProperties,
    /// Once future for creating the partitioned index provider shared by all
    /// probe partitions. This future runs only once before probing starts,
    /// and can be disposed by the last finished stream so the provider does
    /// not outlive the execution plan unnecessarily.
    once_async_spatial_join_components: Arc<Mutex<Option<OnceAsync<SpatialJoinComponents>>>>,
    /// A random seed for making random procedures in spatial join deterministic
    seed: u64,
}

impl SpatialJoinExec {
    /// Try to create a new [`SpatialJoinExec`]
    pub fn try_new(
        left: Arc<dyn ExecutionPlan>, right: Arc<dyn ExecutionPlan>, on: SpatialPredicate,
        filter: Option<JoinFilter>, join_type: &JoinType, projection: Option<Vec<usize>>,
        options: &SpatialJoinOptions,
    ) -> Result<Self> {
        let seed = options
            .debug
            .random_seed
            .unwrap_or(fastrand::u64(0..0xFFFF));
        Self::try_new_internal(left, right, on, filter, join_type, projection, seed)
    }

    /// Create a new `SpatialJoinExec` with additional options
    #[allow(clippy::too_many_arguments)]
    pub fn try_new_internal(
        left: Arc<dyn ExecutionPlan>, right: Arc<dyn ExecutionPlan>, on: SpatialPredicate,
        filter: Option<JoinFilter>, join_type: &JoinType, projection: Option<Vec<usize>>,
        seed: u64,
    ) -> Result<Self> {
        let left_schema = left.schema();
        let right_schema = right.schema();
        check_join_is_valid(&left_schema, &right_schema, &[])?;
        let (join_schema, column_indices) =
            build_join_schema(&left_schema, &right_schema, join_type);
        let join_schema = Arc::new(join_schema);
        let cache = Self::compute_properties(
            &left,
            &right,
            join_schema.clone(),
            *join_type,
            projection.as_ref(),
        )?;

        Ok(SpatialJoinExec {
            left,
            right,
            on,
            filter,
            join_type: *join_type,
            join_schema,
            column_indices,
            projection,
            metrics: Default::default(),
            cache,
            once_async_spatial_join_components: Arc::new(Mutex::new(None)),
            seed,
        })
    }

    /// How the join is performed
    pub fn join_type(&self) -> &JoinType {
        &self.join_type
    }

    /// Does this join has a projection on the joined columns
    pub fn contains_projection(&self) -> bool {
        self.projection.is_some()
    }

    /// Returns a new `ExecutionPlan` that runs `NestedLoopsJoins` with the left
    /// and right inputs swapped.
    ///
    /// # Notes:
    ///
    /// This function should be called BEFORE inserting any repartitioning
    /// operators on the join's children. Check
    /// [`super::HashJoinExec::swap_inputs`] for more details.
    pub fn swap_inputs(&self) -> Result<Arc<dyn ExecutionPlan>> {
        let left_schema = self.left.schema();
        let right_schema = self.right.schema();

        let swapped_on = self.on.swap_for_swapped_children();

        let swapped_projection = swap_join_projection(
            left_schema.fields().len(),
            right_schema.fields().len(),
            self.projection.as_ref(),
            &self.join_type,
        );

        let swapped_join = SpatialJoinExec::try_new_internal(
            self.right.clone(),
            self.left.clone(),
            swapped_on,
            self.filter.as_ref().map(|f| f.swap()),
            &self.join_type.swap(),
            swapped_projection,
            self.seed,
        )?;

        let swapped_join: Arc<dyn ExecutionPlan> = Arc::new(swapped_join);

        match self.join_type {
            JoinType::LeftAnti
            | JoinType::LeftSemi
            | JoinType::RightAnti
            | JoinType::RightSemi
            | JoinType::LeftMark
            | JoinType::RightMark => Ok(swapped_join),
            _ if self.contains_projection() => Ok(swapped_join),
            _ => {
                reorder_output_after_swap(swapped_join, left_schema.as_ref(), right_schema.as_ref())
            }
        }
    }

    pub fn with_projection(&self, projection: Option<Vec<usize>>) -> Result<Self> {
        // check if the projection is valid
        can_project(&self.schema(), projection.as_ref())?;
        let projection = match projection {
            Some(projection) => {
                match &self.projection {
                    Some(p) => Some(projection.iter().map(|i| p[*i]).collect()),
                    None => Some(projection),
                }
            }
            None => None,
        };
        SpatialJoinExec::try_new_internal(
            self.left.clone(),
            self.right.clone(),
            self.on.clone(),
            self.filter.clone(),
            &self.join_type,
            projection,
            self.seed,
        )
    }

    /// This function creates the cache object that stores the plan properties
    /// such as schema, equivalence properties, ordering, partitioning, etc.
    fn compute_properties(
        left: &Arc<dyn ExecutionPlan>, right: &Arc<dyn ExecutionPlan>, schema: SchemaRef,
        join_type: JoinType, projection: Option<&Vec<usize>>,
    ) -> Result<PlanProperties> {
        let mut eq_properties = join_equivalence_properties(
            left.equivalence_properties().clone(),
            right.equivalence_properties().clone(),
            &join_type,
            schema.clone(),
            &[false, false],
            None,
            // Pass extracted equality conditions to preserve equivalences
            &[],
        )?;

        let probe_side = JoinSide::Right;
        let mut output_partitioning =
            asymmetric_join_output_partitioning(left, right, &join_type, probe_side)?;

        if let Some(projection) = projection {
            // construct a map from the input expressions to the output expression of the
            // Projection
            let projection_mapping = ProjectionMapping::from_indices(projection, &schema)?;
            let out_schema = project_schema(&schema, Some(projection))?;
            output_partitioning = output_partitioning.project(&projection_mapping, &eq_properties);
            eq_properties = eq_properties.project(&projection_mapping, out_schema);
        }

        let emission_type = compute_join_emission_type(left, right, join_type, probe_side);

        Ok(PlanProperties::new(
            eq_properties,
            output_partitioning,
            emission_type,
            boundedness_from_children([left, right]),
        ))
    }
}

impl DisplayAs for SpatialJoinExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                let display_on = format!(", on={}", self.on);
                let display_filter = self
                    .filter
                    .as_ref()
                    .map_or_else(|| "".to_owned(), |f| format!(", filter={}", f.expression()));
                let display_projections = if self.contains_projection() {
                    format!(
                        ", projection=[{}]",
                        self.projection
                            .as_ref()
                            .unwrap()
                            .iter()
                            .map(|index| {
                                format!(
                                    "{}@{}",
                                    self.join_schema.fields().get(*index).unwrap().name(),
                                    index
                                )
                            })
                            .collect::<Vec<_>>()
                            .join(", ")
                    )
                } else {
                    "".to_owned()
                };
                write!(
                    f,
                    "SpatialJoinExec: join_type={:?}{}{}{}",
                    self.join_type, display_on, display_filter, display_projections
                )
            }
            DisplayFormatType::TreeRender => {
                if *self.join_type() != JoinType::Inner {
                    writeln!(f, "join_type={:?}", self.join_type)
                } else {
                    Ok(())
                }
            }
        }
    }
}

impl ExecutionPlan for SpatialJoinExec {
    fn name(&self) -> &str {
        "SpatialJoinExec"
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![false, false]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.left, &self.right]
    }

    /// Tries to push `projection` down through `SpatialJoinExec`. If possible,
    /// performs the pushdown and returns a new [`SpatialJoinExec`] as the
    /// top plan which has projections as its children. Otherwise, returns
    /// `None`.
    fn try_swapping_with_projection(
        &self, projection: &ProjectionExec,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        // TODO: currently if there is projection in SpatialJoinExec, we can't push down
        // projection to left or right input. Maybe we can pushdown the mixed
        // projection later. This restriction is inherited from
        // NestedLoopJoinExec and HashJoinExec in DataFusion.
        if self.contains_projection() {
            return Ok(None);
        }

        if let Some(JoinPushdownData {
            projected_left_child,
            projected_right_child,
            join_filter,
            join_on,
        }) = try_pushdown_through_join(
            projection,
            &self.left,
            &self.right,
            &self.join_schema,
            self.join_type,
            self.filter.as_ref(),
            &self.on,
        )? {
            let new_exec = SpatialJoinExec::try_new_internal(
                Arc::new(projected_left_child),
                Arc::new(projected_right_child),
                join_on,
                join_filter,
                &self.join_type,
                None,
                self.seed,
            )?;
            Ok(Some(Arc::new(new_exec)))
        } else {
            try_embed_projection(projection, self)
        }
    }

    fn with_new_children(
        self: Arc<Self>, children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let new_exec = SpatialJoinExec::try_new_internal(
            children[0].clone(),
            children[1].clone(),
            self.on.clone(),
            self.filter.clone(),
            &self.join_type,
            self.projection.clone(),
            self.seed,
        )?;
        Ok(Arc::new(new_exec))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn execute(
        &self, partition: usize, context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let session_config = context.session_config();

        // Determine build/probe plans based on predicate type.
        // For regular spatial joins, left is always build and right is always probe.
        let (build_plan, probe_plan, probe_side) = (&self.left, &self.right, JoinSide::Right);

        // Determine which input index corresponds to the probe side for ordering checks
        let probe_input_index = 1;

        // A OnceFut for preparing the spatial join components once.
        let once_fut_spatial_join_components = {
            let mut once_async = self.once_async_spatial_join_components.lock();
            once_async
                .get_or_insert(OnceAsync::default())
                .try_once(|| {
                    let num_partitions = build_plan.output_partitioning().partition_count();
                    let mut build_streams = Vec::with_capacity(num_partitions);
                    for k in 0..num_partitions {
                        let stream = build_plan.execute(k, context.clone())?;
                        build_streams.push(stream);
                    }

                    let probe_thread_count = probe_plan.output_partitioning().partition_count();
                    let spatial_join_components_builder = SpatialJoinComponentsBuilder::new(
                        context.clone(),
                        build_plan.schema(),
                        self.on.clone(),
                        self.join_type,
                        probe_thread_count,
                        self.metrics.clone(),
                        self.seed,
                    );
                    Ok(spatial_join_components_builder.build(build_streams))
                })?
        };

        let column_indices_after_projection = match &self.projection {
            Some(projection) => {
                projection
                    .iter()
                    .map(|i| self.column_indices[*i].clone())
                    .collect()
            }
            None => self.column_indices.clone(),
        };

        let probe_stream = probe_plan.execute(partition, context.clone())?;

        let probe_side_ordered = self.maintains_input_order()[probe_input_index]
            && probe_plan.output_ordering().is_some();

        Ok(Box::pin(SpatialJoinStream::new(
            partition,
            self.schema(),
            &self.on,
            self.filter.clone(),
            self.join_type,
            probe_stream,
            column_indices_after_projection,
            probe_side_ordered,
            session_config,
            context.runtime_env(),
            &self.metrics,
            once_fut_spatial_join_components,
            self.once_async_spatial_join_components.clone(),
        )))
    }
}

impl EmbeddedProjection for SpatialJoinExec {
    fn with_projection(&self, projection: Option<Vec<usize>>) -> Result<Self> {
        self.with_projection(projection)
    }
}

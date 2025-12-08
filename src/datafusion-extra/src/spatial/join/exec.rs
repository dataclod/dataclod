use std::fmt::Formatter;
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{JoinType, Result, project_schema};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::equivalence::{ProjectionMapping, join_equivalence_properties};
use datafusion::physical_plan::execution_plan::EmissionType;
use datafusion::physical_plan::joins::utils::{
    ColumnIndex, JoinFilter, build_join_schema, check_join_is_valid,
};
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
};
use parking_lot::Mutex;

use crate::spatial::join::index::{SpatialIndex, SpatialJoinBuildMetrics, build_index};
use crate::spatial::join::once_fut::OnceAsync;
use crate::spatial::join::option::SpatialJoinOptions;
use crate::spatial::join::spatial_predicate::SpatialPredicate;
use crate::spatial::join::stream::{SpatialJoinProbeMetrics, SpatialJoinStream};
use crate::spatial::join::utils::{asymmetric_join_output_partitioning, boundedness_from_children};

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
    /// This futures run only once before the spatial index probing phase. It
    /// can also be disposed by the last finished stream so that the spatial
    /// index does not have to live as long as `SpatialJoinExec`.
    once_async_spatial_index: Arc<Mutex<Option<OnceAsync<SpatialIndex>>>>,
}

impl SpatialJoinExec {
    // Try to create a new [`SpatialJoinExec`]
    pub fn try_new(
        left: Arc<dyn ExecutionPlan>, right: Arc<dyn ExecutionPlan>, on: SpatialPredicate,
        filter: Option<JoinFilter>, join_type: &JoinType, projection: Option<Vec<usize>>,
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
            Arc::clone(&join_schema),
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
            once_async_spatial_index: Arc::new(Mutex::new(None)),
        })
    }

    /// How the join is performed
    pub fn join_type(&self) -> &JoinType {
        &self.join_type
    }

    /// Returns a vector indicating whether the left and right inputs maintain
    /// their order. The first element corresponds to the left input, and
    /// the second to the right.
    ///
    /// The left (build-side) input's order may change, but the right
    /// (probe-side) input's order is maintained for INNER, RIGHT, RIGHT
    /// ANTI, and RIGHT SEMI joins.
    ///
    /// Maintaining the right input's order helps optimize the nodes down the
    /// pipeline (See [`ExecutionPlan::maintains_input_order`]).
    ///
    /// This is a separate method because it is also called when computing
    /// properties, before a [`NestedLoopJoinExec`] is created. It also
    /// takes [`JoinType`] as an argument, as opposed to `Self`, for the
    /// same reason.
    fn maintains_input_order(join_type: JoinType) -> Vec<bool> {
        vec![
            false,
            matches!(
                join_type,
                JoinType::Inner | JoinType::Right | JoinType::RightAnti | JoinType::RightSemi
            ),
        ]
    }

    /// Does this join has a projection on the joined columns
    pub fn contains_projection(&self) -> bool {
        self.projection.is_some()
    }

    /// This function creates the cache object that stores the plan properties
    /// such as schema, equivalence properties, ordering, partitioning, etc.
    ///
    /// NOTICE: The implementation of this function should be identical to the
    /// one in
    /// [`datafusion_physical_plan::physical_plan::join::NestedLoopJoinExec::compute_properties`].
    /// This is because SpatialJoinExec is transformed from NestedLoopJoinExec
    /// in physical plan optimization phase. If the properties are not the
    /// same, the plan will be incorrect.
    fn compute_properties(
        left: &Arc<dyn ExecutionPlan>, right: &Arc<dyn ExecutionPlan>, schema: SchemaRef,
        join_type: JoinType, projection: Option<&Vec<usize>>,
    ) -> Result<PlanProperties> {
        let mut eq_properties = join_equivalence_properties(
            left.equivalence_properties().clone(),
            right.equivalence_properties().clone(),
            &join_type,
            Arc::clone(&schema),
            &[false, false],
            None,
            // No on columns (equi-join condition) in spatial join
            &[],
        )?;

        let mut output_partitioning = asymmetric_join_output_partitioning(left, right, &join_type)?;

        if let Some(projection) = projection {
            // construct a map from the input expressions to the output expression of the
            // Projection
            let projection_mapping = ProjectionMapping::from_indices(projection, &schema)?;
            let out_schema = project_schema(&schema, Some(projection))?;
            output_partitioning = output_partitioning.project(&projection_mapping, &eq_properties);
            eq_properties = eq_properties.project(&projection_mapping, out_schema);
        }

        let emission_type = if left.boundedness().is_unbounded() {
            EmissionType::Final
        } else if right.pipeline_behavior() == EmissionType::Incremental {
            match join_type {
                // If we only need to generate matched rows from the probe side,
                // we can emit rows incrementally.
                JoinType::Inner
                | JoinType::LeftSemi
                | JoinType::RightSemi
                | JoinType::Right
                | JoinType::RightAnti => EmissionType::Incremental,
                // If we need to generate unmatched rows from the *build side*,
                // we need to emit them at the end.
                JoinType::Left
                | JoinType::LeftAnti
                | JoinType::LeftMark
                | JoinType::RightMark
                | JoinType::Full => EmissionType::Both,
            }
        } else {
            right.pipeline_behavior()
        };

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
        Self::maintains_input_order(self.join_type)
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.left, &self.right]
    }

    fn with_new_children(
        self: Arc<Self>, children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(SpatialJoinExec {
            left: children[0].clone(),
            right: children[1].clone(),
            on: self.on.clone(),
            filter: self.filter.clone(),
            join_type: self.join_type,
            join_schema: self.join_schema.clone(),
            column_indices: self.column_indices.clone(),
            projection: self.projection.clone(),
            metrics: Default::default(),
            cache: self.cache.clone(),
            once_async_spatial_index: Arc::new(Mutex::new(None)),
        }))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn execute(
        &self, partition: usize, context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let session_config = context.session_config();
        let target_output_batch_size = session_config.options().execution.batch_size;
        let options = SpatialJoinOptions::default();

        let once_fut_spatial_index = {
            let mut once_async = self.once_async_spatial_index.lock();
            once_async
                .get_or_insert(OnceAsync::default())
                .try_once(|| {
                    let build_side = &self.left;

                    let num_partitions = build_side.output_partitioning().partition_count();
                    let mut build_streams = Vec::with_capacity(num_partitions);
                    let mut build_metrics = Vec::with_capacity(num_partitions);
                    for k in 0..num_partitions {
                        let stream = build_side.execute(k, Arc::clone(&context))?;
                        build_streams.push(stream);
                        build_metrics.push(SpatialJoinBuildMetrics::new(k, &self.metrics));
                    }

                    let probe_thread_count = self.right.output_partitioning().partition_count();

                    Ok(build_index(
                        build_side.schema(),
                        build_streams,
                        self.on.clone(),
                        options.clone(),
                        build_metrics,
                        Arc::clone(context.memory_pool()),
                        self.join_type,
                        probe_thread_count,
                    ))
                })?
        };

        // update column indices to reflect the projection
        let column_indices_after_projection = match &self.projection {
            Some(projection) => {
                projection
                    .iter()
                    .map(|i| self.column_indices[*i].clone())
                    .collect()
            }
            None => self.column_indices.clone(),
        };

        let join_metrics = SpatialJoinProbeMetrics::new(partition, &self.metrics);
        let probe_stream = self.right.execute(partition, Arc::clone(&context))?;

        // Right side has an order and it is maintained during operation.
        let probe_side_ordered =
            self.maintains_input_order()[1] && self.right.output_ordering().is_some();

        Ok(Box::pin(SpatialJoinStream::new(
            self.schema(),
            &self.on,
            self.filter.clone(),
            self.join_type,
            probe_stream,
            column_indices_after_projection,
            probe_side_ordered,
            join_metrics,
            options,
            target_output_batch_size,
            once_fut_spatial_index,
            Arc::clone(&self.once_async_spatial_index),
        )))
    }
}

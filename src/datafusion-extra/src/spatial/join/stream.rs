use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::common::{JoinSide, Result};
use datafusion::logical_expr::JoinType;
use datafusion::physical_plan::joins::utils::{ColumnIndex, JoinFilter, StatefulStreamResult};
use datafusion::physical_plan::metrics::{self, ExecutionPlanMetricsSet, MetricBuilder};
use datafusion::physical_plan::{RecordBatchStream, SendableRecordBatchStream, handle_state};
use futures::ready;
use futures::stream::StreamExt;
use futures::task::Poll;

use crate::spatial::join::index::SpatialIndex;
use crate::spatial::join::once_fut::OnceFut;
use crate::spatial::join::option::SpatialJoinOptions;
use crate::spatial::join::spatial_predicate::{
    GeometryBatchResult, SpatialPredicate, SpatialPredicateEvaluator,
};
use crate::spatial::join::utils::{
    JoinedRowsIndices, adjust_indices_by_join_type, apply_join_filter_to_indices,
    build_batch_from_indices,
};
use crate::spatial::join::wkb_array::{WkbArrayAccess, create_wkb_array_access};

/// Stream for producing spatial join result batches.
pub(crate) struct SpatialJoinStream {
    /// Input schema
    schema: Arc<Schema>,
    /// join filter
    filter: Option<JoinFilter>,
    /// type of the join
    join_type: JoinType,
    /// The stream of the probe side
    probe_stream: SendableRecordBatchStream,
    /// Information of index and left / right placement of columns
    column_indices: Vec<ColumnIndex>,
    /// Join execution metrics
    join_metrics: SpatialJoinMetrics,
    /// Current state of the stream
    state: SpatialJoinStreamState,
    /// Options for the spatial join
    #[allow(unused)]
    options: SpatialJoinOptions,
    /// Once future for the spatial index
    once_partial_leaf_nodes: OnceFut<SpatialIndex>,
    /// The spatial index
    spatial_index: Option<Arc<SpatialIndex>>,
    /// The `on` spatial predicate evaluator
    evaluator: Arc<dyn SpatialPredicateEvaluator>,
}

impl SpatialJoinStream {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        schema: Arc<Schema>, on: &SpatialPredicate, filter: Option<JoinFilter>,
        join_type: JoinType, probe_stream: SendableRecordBatchStream,
        column_indices: Vec<ColumnIndex>, join_metrics: SpatialJoinMetrics,
        options: SpatialJoinOptions, once_partial_leaf_nodes: OnceFut<SpatialIndex>,
    ) -> Self {
        let evaluator = on.evaluator(options.clone());
        Self {
            schema,
            filter,
            join_type,
            probe_stream,
            column_indices,
            join_metrics,
            state: SpatialJoinStreamState::WaitBuildIndex,
            options,
            once_partial_leaf_nodes,
            spatial_index: None,
            evaluator,
        }
    }
}

/// Metrics for the spatial join.
#[derive(Clone, Debug, Default)]
pub(crate) struct SpatialJoinMetrics {
    /// Total time for collecting build-side of join
    pub(crate) build_time: metrics::Time,
    /// Number of batches consumed by build-side
    pub(crate) build_input_batches: metrics::Count,
    /// Number of rows consumed by build-side
    pub(crate) build_input_rows: metrics::Count,
    /// Memory used by build-side in bytes
    pub(crate) build_mem_used: metrics::Gauge,
    /// Total time for joining probe-side batches to the build-side batches
    pub(crate) join_time: metrics::Time,
    /// Number of batches consumed by probe-side of this operator
    pub(crate) probe_input_batches: metrics::Count,
    /// Number of rows consumed by probe-side this operator
    pub(crate) probe_input_rows: metrics::Count,
    /// Number of batches produced by this operator
    pub(crate) output_batches: metrics::Count,
    /// Number of rows produced by this operator
    pub(crate) output_rows: metrics::Count,
    /// Number of result candidates retrieved by querying the spatial index
    pub(crate) join_result_candidates: metrics::Count,
    /// Number of join results before filtering
    pub(crate) join_result_count: metrics::Count,
}

impl SpatialJoinMetrics {
    pub fn new(partition: usize, metrics: &ExecutionPlanMetricsSet) -> Self {
        Self {
            build_time: MetricBuilder::new(metrics).subset_time("build_time", partition),
            build_input_batches: MetricBuilder::new(metrics)
                .counter("build_input_batches", partition),
            build_input_rows: MetricBuilder::new(metrics).counter("build_input_rows", partition),
            build_mem_used: MetricBuilder::new(metrics).gauge("build_mem_used", partition),
            join_time: MetricBuilder::new(metrics).subset_time("join_time", partition),
            probe_input_batches: MetricBuilder::new(metrics)
                .counter("probe_input_batches", partition),
            probe_input_rows: MetricBuilder::new(metrics).counter("probe_input_rows", partition),
            output_batches: MetricBuilder::new(metrics).counter("output_batches", partition),
            output_rows: MetricBuilder::new(metrics).counter("output_rows", partition),
            join_result_candidates: MetricBuilder::new(metrics)
                .counter("join_result_candidates", partition),
            join_result_count: MetricBuilder::new(metrics).counter("join_result_count", partition),
        }
    }
}

/// This enumeration represents various states of the nested loop join
/// algorithm.
#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
pub(crate) enum SpatialJoinStreamState {
    /// The initial mode: waiting for the spatial index to be built
    WaitBuildIndex,
    /// Indicates that build-side has been collected, and stream is ready for
    /// fetching probe-side
    FetchProbeBatch,
    /// Indicates that we're processing a probe batch using the batch iterator
    ProcessProbeBatch(SpatialJoinBatchIterator),
    /// Indicates that probe-side has been fully processed
    ExhaustedProbeSide,
    /// Indicates that SpatialJoinStream execution is completed
    Completed,
}

impl SpatialJoinStream {
    fn poll_next_impl(
        &mut self, cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Result<RecordBatch>>> {
        loop {
            return match &mut self.state {
                SpatialJoinStreamState::WaitBuildIndex => {
                    handle_state!(ready!(self.wait_build_index(cx)))
                }
                SpatialJoinStreamState::FetchProbeBatch => {
                    handle_state!(ready!(self.fetch_probe_batch(cx)))
                }
                SpatialJoinStreamState::ProcessProbeBatch(_) => {
                    handle_state!(ready!(self.process_probe_batch()))
                }
                SpatialJoinStreamState::ExhaustedProbeSide => {
                    handle_state!(ready!(self.process_unmatched_build_batch()))
                }
                SpatialJoinStreamState::Completed => Poll::Ready(None),
            };
        }
    }

    fn wait_build_index(
        &mut self, cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<StatefulStreamResult<Option<RecordBatch>>>> {
        let index = ready!(self.once_partial_leaf_nodes.get_shared(cx))?;
        self.spatial_index = Some(index);
        self.state = SpatialJoinStreamState::FetchProbeBatch;
        Poll::Ready(Ok(StatefulStreamResult::Continue))
    }

    fn fetch_probe_batch(
        &mut self, cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<StatefulStreamResult<Option<RecordBatch>>>> {
        let result = self.probe_stream.poll_next_unpin(cx);
        match result {
            Poll::Ready(Some(Ok(batch))) => {
                match self.create_spatial_join_iterator(&batch) {
                    Ok(iterator) => {
                        self.state = SpatialJoinStreamState::ProcessProbeBatch(iterator);
                        Poll::Ready(Ok(StatefulStreamResult::Continue))
                    }
                    Err(e) => Poll::Ready(Err(e)),
                }
            }
            Poll::Ready(Some(Err(e))) => Poll::Ready(Err(e)),
            Poll::Ready(None) => {
                self.state = SpatialJoinStreamState::ExhaustedProbeSide;
                Poll::Ready(Ok(StatefulStreamResult::Continue))
            }
            Poll::Pending => Poll::Pending,
        }
    }

    fn process_probe_batch(&mut self) -> Poll<Result<StatefulStreamResult<Option<RecordBatch>>>> {
        let timer = self.join_metrics.join_time.timer();

        // Extract the necessary data first to avoid borrowing conflicts
        let (batch_opt, is_complete) = match &mut self.state {
            SpatialJoinStreamState::ProcessProbeBatch(iterator) => {
                let batch_opt = match iterator.next_batch(
                    &self.schema,
                    self.filter.as_ref(),
                    self.join_type,
                    &self.column_indices,
                    JoinSide::Left,
                ) {
                    Ok(opt) => opt,
                    Err(e) => {
                        return Poll::Ready(Err(e));
                    }
                };
                let is_complete = iterator.is_complete();
                (batch_opt, is_complete)
            }
            _ => unreachable!(),
        };

        let result = match batch_opt {
            Some(batch) => {
                // Check if iterator is complete
                if is_complete {
                    self.state = SpatialJoinStreamState::FetchProbeBatch;
                }
                batch
            }
            None => {
                // Iterator finished, move to next probe batch
                self.state = SpatialJoinStreamState::FetchProbeBatch;
                return Poll::Ready(Ok(StatefulStreamResult::Continue));
            }
        };

        timer.done();
        Poll::Ready(Ok(StatefulStreamResult::Ready(Some(result))))
    }

    fn process_unmatched_build_batch(
        &mut self,
    ) -> Poll<Result<StatefulStreamResult<Option<RecordBatch>>>> {
        // TODO: process unmatched build batch to support outer joins
        self.state = SpatialJoinStreamState::Completed;
        Poll::Ready(Ok(StatefulStreamResult::Continue))
    }

    fn create_spatial_join_iterator(
        &self, probe_batch: &RecordBatch,
    ) -> Result<SpatialJoinBatchIterator> {
        let num_rows = probe_batch.num_rows();
        self.join_metrics.probe_input_batches.add(1);
        self.join_metrics.probe_input_rows.add(num_rows);

        // Get the spatial index
        let spatial_index = self
            .spatial_index
            .as_ref()
            .expect("Spatial index should be available");

        // Evaluate the probe side geometry expression to get geometry array
        let evaluator = self.evaluator.as_ref();
        let geometry_batch_result = evaluator.evaluate_probe(probe_batch)?;

        SpatialJoinBatchIterator::new(
            spatial_index.clone(),
            probe_batch.clone(),
            geometry_batch_result,
            self.evaluator.clone(),
            Arc::new(self.join_metrics.clone()),
            self.options.max_batch_size,
        )
    }
}

impl futures::Stream for SpatialJoinStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        self.poll_next_impl(cx)
    }
}

impl RecordBatchStream for SpatialJoinStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

/// Iterator that processes spatial join results in configurable batch sizes
pub(crate) struct SpatialJoinBatchIterator {
    /// Current accumulated build batch positions
    build_batch_positions: Vec<(i32, i32)>,
    /// Current accumulated probe indices
    probe_indices: Vec<u32>,
    /// Maximum batch size before yielding a result
    max_batch_size: usize,
    /// The spatial index reference
    spatial_index: Arc<SpatialIndex>,
    /// The probe batch being processed
    probe_batch: RecordBatch,
    /// Current probe row index being processed
    current_probe_idx: usize,
    /// The rects from evaluating the probe batch
    rects: Vec<(usize, geo_types::Rect)>,
    /// Current rect index being processed
    current_rect_idx: usize,
    /// Distance for spatial predicates
    distance: Option<datafusion::logical_expr::ColumnarValue>,
    /// The spatial predicate evaluator
    evaluator: Arc<dyn SpatialPredicateEvaluator>,
    /// Join metrics for tracking performance
    join_metrics: Arc<SpatialJoinMetrics>,
    /// Whether iteration is complete
    is_complete: bool,
    /// WKB array accessor for efficient indexed access
    wkb_accessor: Box<dyn WkbArrayAccess>,
}

impl SpatialJoinBatchIterator {
    pub(crate) fn new(
        spatial_index: Arc<SpatialIndex>, probe_batch: RecordBatch,
        geometry_batch_result: GeometryBatchResult, evaluator: Arc<dyn SpatialPredicateEvaluator>,
        join_metrics: Arc<SpatialJoinMetrics>, max_batch_size: usize,
    ) -> Result<Self> {
        let GeometryBatchResult {
            geometry_array,
            rects,
            distance,
        } = geometry_batch_result;

        // Create WKB array accessor for efficient indexed access
        let wkb_accessor = create_wkb_array_access(&geometry_array)?;

        Ok(Self {
            build_batch_positions: Vec::new(),
            probe_indices: Vec::new(),
            max_batch_size,
            spatial_index,
            probe_batch,
            current_probe_idx: 0,
            rects,
            current_rect_idx: 0,
            distance,
            evaluator,
            join_metrics,
            is_complete: false,
            wkb_accessor,
        })
    }

    pub fn next_batch(
        &mut self, schema: &Schema, filter: Option<&JoinFilter>, join_type: JoinType,
        column_indices: &[ColumnIndex], build_side: JoinSide,
    ) -> Result<Option<RecordBatch>> {
        // Process probe rows incrementally until we have enough results or finish
        let initial_size = self.build_batch_positions.len();
        let num_rows = self.wkb_accessor.len();

        // Process from current position until we hit batch size limit or complete
        while self.current_probe_idx < num_rows && !self.is_complete {
            // Get WKB for current probe index
            let wkb_opt = self.wkb_accessor.get_wkb_at(self.current_probe_idx)?;

            let Some(wkb) = wkb_opt else {
                // Move to next probe index if current is null
                self.current_probe_idx += 1;
                continue;
            };

            // Process all rects for this probe index
            while self.current_rect_idx < self.rects.len()
                && self.rects[self.current_rect_idx].0 == self.current_probe_idx
            {
                let rect = &self.rects[self.current_rect_idx].1;
                self.current_rect_idx += 1;

                let join_result_metrics = self.spatial_index.query(
                    &wkb,
                    rect,
                    &self.distance,
                    self.evaluator.as_ref(),
                    &mut self.build_batch_positions,
                )?;

                self.probe_indices.extend(std::iter::repeat_n(
                    self.current_probe_idx as u32,
                    join_result_metrics.count,
                ));

                self.join_metrics
                    .join_result_candidates
                    .add(join_result_metrics.candidate_count);
                self.join_metrics
                    .join_result_count
                    .add(join_result_metrics.count);

                // Check if we've accumulated enough results
                if self.build_batch_positions.len() >= self.max_batch_size {
                    // Don't increment current_probe_idx yet as we may have more rects for this
                    // probe index
                    if self.current_rect_idx >= self.rects.len()
                        || self.rects[self.current_rect_idx].0 != self.current_probe_idx
                    {
                        // No more rects for this probe index, move to next
                        self.current_probe_idx += 1;
                    }
                    break;
                }
            }

            // If we've processed all rects for this probe index, move to next
            if self.current_rect_idx >= self.rects.len()
                || self.rects[self.current_rect_idx].0 != self.current_probe_idx
            {
                self.current_probe_idx += 1;
            }

            // Early exit if we have enough results
            if self.build_batch_positions.len() >= self.max_batch_size {
                break;
            }
        }

        // Check if we've finished processing all probe rows
        if self.current_probe_idx >= num_rows {
            self.is_complete = true;
        }

        // Return accumulated results if we have any new ones or if we're complete
        if self.build_batch_positions.len() > initial_size || self.is_complete {
            let joined_indices = JoinedRowsIndices::new(
                std::mem::take(&mut self.build_batch_positions),
                std::mem::take(&mut self.probe_indices),
            );

            // Process the joined indices to create a RecordBatch
            let batch = self.process_joined_indices_to_batch(
                &joined_indices,
                schema,
                filter,
                join_type,
                column_indices,
                build_side,
            )?;

            Ok(Some(batch))
        } else {
            Ok(None)
        }
    }

    /// Check if the iterator has finished processing
    pub fn is_complete(&self) -> bool {
        self.is_complete
    }

    /// Process joined indices and create a RecordBatch
    fn process_joined_indices_to_batch(
        &self, joined_indices: &JoinedRowsIndices, schema: &Schema, filter: Option<&JoinFilter>,
        join_type: JoinType, column_indices: &[ColumnIndex], build_side: JoinSide,
    ) -> Result<RecordBatch> {
        if joined_indices.is_empty() {
            let empty_batch = RecordBatch::new_empty(Arc::new(schema.clone()));
            self.join_metrics.output_batches.add(1);
            return Ok(empty_batch);
        }

        // Get only the build batches that are actually needed
        let mut batch_index_map: HashMap<i32, i32> = HashMap::new();
        let mut needed_batch_indices: Vec<i32> = Vec::new();

        for (batch_idx, _) in &joined_indices.build {
            if !batch_index_map.contains_key(batch_idx) {
                let new_idx = needed_batch_indices.len() as i32;
                batch_index_map.insert(*batch_idx, new_idx);
                needed_batch_indices.push(*batch_idx);
            }
        }

        // Fetch only needed batches
        let mut build_batches: Vec<&RecordBatch> = Vec::new();
        for &batch_idx in &needed_batch_indices {
            // SAFETY: batch_idx is retrieved by calling spatial_index.query, so the index
            // must be valid.
            let batch = unsafe { self.spatial_index.get_indexed_batch(batch_idx as usize) };
            build_batches.push(batch);
        }

        // Remap build_batch_positions to use new batch indices (in-place)
        let mut remapped_joined_indices = joined_indices.clone();
        for (batch_idx, _) in &mut remapped_joined_indices.build {
            *batch_idx = batch_index_map[batch_idx];
        }

        let joined_indices = match filter {
            Some(filter) => {
                apply_join_filter_to_indices(
                    &build_batches,
                    &remapped_joined_indices,
                    &self.probe_batch,
                    filter,
                    build_side,
                )?
            }
            None => remapped_joined_indices,
        };

        // If no rows pass the filters, return empty batch
        if joined_indices.is_empty() {
            let empty_batch = RecordBatch::new_empty(Arc::new(schema.clone()));
            self.join_metrics.output_batches.add(1);
            return Ok(empty_batch);
        }

        let joined_indices = adjust_indices_by_join_type(
            &joined_indices,
            0..self.probe_batch.num_rows(),
            join_type,
            true,
        );

        // Build the final result batch
        let result_batch = build_batch_from_indices(
            schema,
            &build_batches,
            &joined_indices.build,
            &self.probe_batch,
            &joined_indices.probe,
            column_indices,
            build_side,
        )?;

        // Update metrics with actual output
        self.join_metrics.output_batches.add(1);
        self.join_metrics.output_rows.add(result_batch.num_rows());

        Ok(result_batch)
    }
}

// Manual Debug implementation for SpatialJoinBatchIterator
impl std::fmt::Debug for SpatialJoinBatchIterator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SpatialJoinBatchIterator")
            .field("max_batch_size", &self.max_batch_size)
            .field("current_probe_idx", &self.current_probe_idx)
            .field("current_rect_idx", &self.current_rect_idx)
            .field("is_complete", &self.is_complete)
            .field(
                "build_batch_positions_len",
                &self.build_batch_positions.len(),
            )
            .field("probe_indices_len", &self.probe_indices.len())
            .finish()
    }
}

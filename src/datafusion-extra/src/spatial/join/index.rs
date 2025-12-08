use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use datafusion::arrow::array::{BooleanBufferBuilder, RecordBatch};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::runtime::JoinSet;
use datafusion::common::utils::proxy::VecAllocExt;
use datafusion::common::{DataFusionError, JoinType, Result};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryPool, MemoryReservation};
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_plan::metrics::{self, ExecutionPlanMetricsSet, MetricBuilder};
use futures::StreamExt;
use geo_index::rtree::sort::STRSort;
use geo_index::rtree::{RTree, RTreeBuilder, RTreeIndex};
use geo_types::Rect;
use parking_lot::Mutex;
use wkb::reader::Wkb;

use crate::spatial::geometry::analyze::AnalyzeAccumulator;
use crate::spatial::join::concurrent_reservation::ConcurrentReservation;
use crate::spatial::join::operand_evaluator::{
    EvaluatedGeometryArray, OperandEvaluator, create_operand_evaluator,
};
use crate::spatial::join::option::{ExecutionMode, SpatialJoinOptions};
use crate::spatial::join::refine::{IndexQueryResultRefiner, create_refiner};
use crate::spatial::join::spatial_predicate::SpatialPredicate;
use crate::spatial::join::utils::need_produce_result_in_final;
use crate::spatial::statistics::GeoStatistics;

// Type aliases for better readability
type SpatialRTree = RTree<f64>;
type DataIdToBatchPos = Vec<(i32, i32)>;
type RTreeBuildResult = (SpatialRTree, DataIdToBatchPos);

/// The prealloc size for the refiner reservation. This is used to reduce the
/// frequency of growing the reservation when updating the refiner memory
/// reservation.
const REFINER_RESERVATION_PREALLOC_SIZE: usize = 10 * 1024 * 1024; // 10MB

/// Metrics for the build phase of the spatial join.
#[derive(Clone, Debug, Default)]
pub(crate) struct SpatialJoinBuildMetrics {
    /// Total time for collecting build-side of join
    pub(crate) build_time: metrics::Time,
    /// Number of batches consumed by build-side
    pub(crate) build_input_batches: metrics::Count,
    /// Number of rows consumed by build-side
    pub(crate) build_input_rows: metrics::Count,
    /// Memory used by build-side in bytes
    pub(crate) build_mem_used: metrics::Gauge,
}

impl SpatialJoinBuildMetrics {
    pub fn new(partition: usize, metrics: &ExecutionPlanMetricsSet) -> Self {
        Self {
            build_time: MetricBuilder::new(metrics).subset_time("build_time", partition),
            build_input_batches: MetricBuilder::new(metrics)
                .counter("build_input_batches", partition),
            build_input_rows: MetricBuilder::new(metrics).counter("build_input_rows", partition),
            build_mem_used: MetricBuilder::new(metrics).gauge("build_mem_used", partition),
        }
    }
}

/// Builder for constructing a SpatialIndex from geometry batches.
///
/// This builder handles:
/// 1. Accumulating geometry batches to be indexed
/// 2. Building the spatial R-tree index
/// 3. Setting up memory tracking and visited bitmaps
/// 4. Configuring prepared geometries based on execution mode
pub(crate) struct SpatialIndexBuilder {
    spatial_predicate: SpatialPredicate,
    options: SpatialJoinOptions,
    join_type: JoinType,
    probe_threads_count: usize,
    metrics: SpatialJoinBuildMetrics,

    /// Batches to be indexed
    indexed_batches: Vec<IndexedBatch>,

    /// Memory reservation for tracking the memory usage of the spatial index
    reservation: MemoryReservation,

    /// Statistics for indexed geometries
    stats: GeoStatistics,

    /// Memory pool for managing the memory usage of the spatial index
    memory_pool: Arc<dyn MemoryPool>,
}

impl SpatialIndexBuilder {
    /// Create a new builder with the given configuration.
    pub fn new(
        spatial_predicate: SpatialPredicate, options: SpatialJoinOptions, join_type: JoinType,
        probe_threads_count: usize, memory_pool: Arc<dyn MemoryPool>,
        metrics: SpatialJoinBuildMetrics,
    ) -> Self {
        let consumer = MemoryConsumer::new("SpatialJoinIndex");
        let reservation = consumer.register(&memory_pool);

        Self {
            spatial_predicate,
            options,
            join_type,
            probe_threads_count,
            metrics,
            indexed_batches: Vec::new(),
            reservation,
            stats: GeoStatistics::empty(),
            memory_pool,
        }
    }

    /// Add a geometry batch to be indexed.
    ///
    /// This method accumulates geometry batches that will be used to build the
    /// spatial index. Each batch contains processed geometry data along
    /// with memory usage information.
    pub fn add_batch(&mut self, indexed_batch: IndexedBatch) {
        let in_mem_size = indexed_batch.in_mem_size();
        self.indexed_batches.push(indexed_batch);
        self.reservation.grow(in_mem_size);
        self.metrics.build_mem_used.add(in_mem_size);
    }

    pub fn with_stats(&mut self, stats: GeoStatistics) -> &mut Self {
        self.stats.merge(&stats);
        self
    }

    /// Build the spatial R-tree index from collected geometry batches.
    fn build_rtree(&mut self) -> RTreeBuildResult {
        let build_timer = self.metrics.build_time.timer();

        let num_rects = self
            .indexed_batches
            .iter()
            .map(|batch| batch.rects().len())
            .sum::<usize>();

        let mut rtree_builder = RTreeBuilder::<f64>::new(num_rects as u32);
        let mut batch_pos_vec = vec![(0, 0); num_rects];
        let rtree_mem_estimate = num_rects * RTREE_MEMORY_ESTIMATE_PER_RECT;

        self.reservation
            .grow(batch_pos_vec.allocated_size() + rtree_mem_estimate);

        for (batch_idx, batch) in self.indexed_batches.iter().enumerate() {
            let rects = batch.rects();
            for (idx, rect) in rects {
                let min = rect.min();
                let max = rect.max();
                let data_idx = rtree_builder.add(min.x, min.y, max.x, max.y);
                batch_pos_vec[data_idx as usize] = (batch_idx as i32, *idx as i32);
            }
        }

        let rtree = rtree_builder.finish::<STRSort>();
        build_timer.done();

        self.metrics.build_mem_used.add(self.reservation.size());

        (rtree, batch_pos_vec)
    }

    /// Build visited bitmaps for tracking left-side indices in outer joins.
    fn build_visited_bitmaps(&mut self) -> Result<Option<Mutex<Vec<BooleanBufferBuilder>>>> {
        if !need_produce_result_in_final(self.join_type) {
            return Ok(None);
        }

        let mut bitmaps = Vec::with_capacity(self.indexed_batches.len());
        let mut total_buffer_size = 0;

        for batch in &self.indexed_batches {
            let batch_rows = batch.batch.num_rows();
            let buffer_size = batch_rows.div_ceil(8);
            total_buffer_size += buffer_size;

            let mut bitmap = BooleanBufferBuilder::new(batch_rows);
            bitmap.append_n(batch_rows, false);
            bitmaps.push(bitmap);
        }

        self.reservation.try_grow(total_buffer_size)?;
        self.metrics.build_mem_used.add(total_buffer_size);

        Ok(Some(Mutex::new(bitmaps)))
    }

    /// Create an rtree data index to consecutive index mapping.
    fn build_geom_idx_vec(&mut self, batch_pos_vec: &Vec<(i32, i32)>) -> Vec<usize> {
        let mut num_geometries = 0;
        let mut batch_idx_offset = Vec::with_capacity(self.indexed_batches.len() + 1);
        batch_idx_offset.push(0);
        for batch in &self.indexed_batches {
            num_geometries += batch.batch.num_rows();
            batch_idx_offset.push(num_geometries);
        }

        let mut geom_idx_vec = Vec::with_capacity(batch_pos_vec.len());
        self.reservation.grow(geom_idx_vec.allocated_size());
        for (batch_idx, row_idx) in batch_pos_vec {
            // Convert (batch_idx, row_idx) to a linear, sequential index
            let batch_offset = batch_idx_offset[*batch_idx as usize];
            let prepared_idx = batch_offset + *row_idx as usize;
            geom_idx_vec.push(prepared_idx);
        }

        geom_idx_vec
    }

    /// Finish building and return the completed SpatialIndex.
    pub fn finish(mut self, schema: SchemaRef) -> Result<SpatialIndex> {
        if self.indexed_batches.is_empty() {
            return Ok(SpatialIndex::empty(
                self.spatial_predicate,
                schema,
                self.options,
                AtomicUsize::new(self.probe_threads_count),
                self.reservation,
            ));
        }

        let evaluator = create_operand_evaluator(&self.spatial_predicate, self.options.clone());
        let num_geoms = self
            .indexed_batches
            .iter()
            .map(|batch| batch.batch.num_rows())
            .sum::<usize>();

        let (rtree, batch_pos_vec) = self.build_rtree();
        let geom_idx_vec = self.build_geom_idx_vec(&batch_pos_vec);
        let visited_left_side = self.build_visited_bitmaps()?;

        let refiner = create_refiner(
            &self.spatial_predicate,
            self.options.clone(),
            num_geoms,
            self.stats,
        );
        let consumer = MemoryConsumer::new("SpatialJoinRefiner");
        let refiner_reservation = consumer.register(&self.memory_pool);
        let refiner_reservation =
            ConcurrentReservation::new(REFINER_RESERVATION_PREALLOC_SIZE, refiner_reservation);

        Ok(SpatialIndex {
            schema,
            evaluator,
            refiner,
            refiner_reservation,
            rtree,
            data_id_to_batch_pos: batch_pos_vec,
            indexed_batches: self.indexed_batches,
            geom_idx_vec,
            visited_left_side,
            probe_threads_counter: AtomicUsize::new(self.probe_threads_count),
            reservation: self.reservation,
        })
    }
}

pub(crate) struct SpatialIndex {
    schema: SchemaRef,

    /// The spatial predicate evaluator for the spatial predicate.
    evaluator: Arc<dyn OperandEvaluator>,

    /// The refiner for refining the index query results.
    refiner: Arc<dyn IndexQueryResultRefiner>,

    /// Memory reservation for tracking the memory usage of the refiner
    refiner_reservation: ConcurrentReservation,

    /// R-tree index for the geometry batches. It takes MBRs as query windows
    /// and returns data indexes. These data indexes should be translated
    /// using `data_id_to_batch_pos` to get the original geometry batch
    /// index and row index, or translated using `prepared_geom_idx_vec`
    /// to get the prepared geometries array index.
    rtree: RTree<f64>,

    /// Indexed batches containing evaluated geometry arrays. It contains the
    /// original record batches and geometry arrays obtained by evaluating
    /// the geometry expression on the build side.
    indexed_batches: Vec<IndexedBatch>,
    /// An array for translating rtree data index to geometry batch index and
    /// row index
    data_id_to_batch_pos: Vec<(i32, i32)>,

    /// An array for translating rtree data index to consecutive index. Each
    /// geometry may be indexed by multiple boxes, so there could be
    /// multiple data indexes for the same geometry. A mapping for squashing
    /// the index makes it easier for persisting per-geometry auxiliary data for
    /// evaluating the spatial predicate. This is extensively used by the
    /// spatial predicate evaluators for storing prepared geometries.
    geom_idx_vec: Vec<usize>,

    /// Shared bitmap builders for visited left indices, one per batch
    visited_left_side: Option<Mutex<Vec<BooleanBufferBuilder>>>,

    /// Counter of running probe-threads, potentially able to update `bitmap`.
    /// Each time a probe thread finished probing the index, it will decrement
    /// the counter. The last finished probe thread will produce the extra
    /// output batches for unmatched build side when running left-outer
    /// joins. See also [`report_probe_completed`].
    probe_threads_counter: AtomicUsize,

    /// Memory reservation for tracking the memory usage of the spatial index
    /// Cleared on `SpatialIndex` drop
    #[expect(dead_code)]
    reservation: MemoryReservation,
}

/// Indexed batch containing the original record batch and the evaluated
/// geometry array.
pub(crate) struct IndexedBatch {
    batch: RecordBatch,
    geom_array: EvaluatedGeometryArray,
}

impl IndexedBatch {
    pub fn in_mem_size(&self) -> usize {
        // NOTE: sometimes `geom_array` will reuse the memory of `batch`, especially
        // when the expression for evaluating the geometry is a simple column
        // reference. In this case, the in_mem_size will be overestimated.
        self.batch.get_array_memory_size() + self.geom_array.in_mem_size()
    }

    pub fn wkb(&self, idx: usize) -> Option<&Wkb<'_>> {
        let wkbs = self.geom_array.wkbs();
        wkbs[idx].as_ref()
    }

    pub fn rects(&self) -> &Vec<(usize, Rect)> {
        &self.geom_array.rects
    }

    pub fn distance(&self) -> &Option<ColumnarValue> {
        &self.geom_array.distance
    }
}

pub struct JoinResultMetrics {
    pub count: usize,
    pub candidate_count: usize,
}

impl SpatialIndex {
    fn empty(
        spatial_predicate: SpatialPredicate, schema: SchemaRef, options: SpatialJoinOptions,
        probe_threads_counter: AtomicUsize, mut reservation: MemoryReservation,
    ) -> Self {
        let evaluator = create_operand_evaluator(&spatial_predicate, options.clone());
        let refiner = create_refiner(&spatial_predicate, options, 0, GeoStatistics::empty());
        let refiner_reservation = reservation.split(0);
        let refiner_reservation = ConcurrentReservation::new(0, refiner_reservation);
        let rtree = RTreeBuilder::<f64>::new(0).finish::<STRSort>();
        Self {
            schema,
            evaluator,
            refiner,
            refiner_reservation,
            rtree,
            data_id_to_batch_pos: Vec::new(),
            indexed_batches: Vec::new(),
            geom_idx_vec: Vec::new(),
            visited_left_side: None,
            probe_threads_counter,
            reservation,
        }
    }

    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// Get the batch at the given index.
    pub fn get_indexed_batch(&self, batch_idx: usize) -> &RecordBatch {
        &self.indexed_batches[batch_idx].batch
    }

    /// Query the spatial index with a probe geometry to find matching
    /// build-side geometries.
    ///
    /// This method implements a two-phase spatial join query:
    /// 1. **Filter phase**: Uses the R-tree index with the probe geometry's
    ///    bounding rectangle to quickly identify candidate geometries that
    ///    might satisfy the spatial predicate
    /// 2. **Refinement phase**: Evaluates the exact spatial predicate on
    ///    candidates to determine actual matches
    ///
    /// # Arguments
    /// * `probe_wkb` - The probe geometry in WKB format
    /// * `probe_rect` - The minimum bounding rectangle of the probe geometry
    /// * `distance` - Optional distance parameter for distance-based spatial
    ///   predicates
    /// * `build_batch_positions` - Output vector that will be populated with
    ///   (batch_idx, row_idx) pairs for each matching build-side geometry
    ///
    /// # Returns
    /// * `JoinResultMetrics` containing the number of actual matches (`count`)
    ///   and the number of candidates from the filter phase (`candidate_count`)
    pub fn query(
        &self, probe_wkb: &Wkb, probe_rect: &Rect, distance: &Option<ColumnarValue>,
        build_batch_positions: &mut Vec<(i32, i32)>,
    ) -> Result<JoinResultMetrics> {
        let min = probe_rect.min();
        let max = probe_rect.max();
        let mut candidates = self.rtree.search(min.x, min.y, max.x, max.y);
        if candidates.is_empty() {
            return Ok(JoinResultMetrics {
                count: 0,
                candidate_count: 0,
            });
        }

        // Sort and dedup candidates to avoid duplicate results when we index one
        // geometry using several boxes.
        candidates.sort_unstable();
        candidates.dedup();

        // Refine the candidates retrieved from the r-tree index by evaluating the
        // actual spatial predicate
        self.refine(probe_wkb, &candidates, distance, build_batch_positions)
    }

    fn refine(
        &self, probe_wkb: &Wkb, candidates: &[u32], distance: &Option<ColumnarValue>,
        build_batch_positions: &mut Vec<(i32, i32)>,
    ) -> Result<JoinResultMetrics> {
        let candidate_count = candidates.len();
        let mut index_query_results = Vec::with_capacity(candidate_count);
        for data_idx in candidates {
            let pos = self.data_id_to_batch_pos[*data_idx as usize];
            let (batch_idx, row_idx) = pos;
            let indexed_batch = &self.indexed_batches[batch_idx as usize];
            let build_wkb = indexed_batch.wkb(row_idx as usize);
            let Some(build_wkb) = build_wkb else {
                continue;
            };
            let distance = self.evaluator.resolve_distance(
                indexed_batch.distance(),
                distance,
                row_idx as usize,
            )?;
            let geom_idx = self.geom_idx_vec[*data_idx as usize];
            index_query_results.push(IndexQueryResult {
                wkb: build_wkb,
                distance,
                geom_idx,
                position: pos,
            });
        }

        let results = self.refiner.refine(probe_wkb, &index_query_results)?;
        let num_results = results.len();
        build_batch_positions.extend(results);

        // Update refiner memory reservation
        self.refiner_reservation.resize(self.refiner.mem_usage())?;

        Ok(JoinResultMetrics {
            count: num_results,
            candidate_count,
        })
    }

    /// Check if the index needs more probe statistics to determine the optimal
    /// execution mode.
    ///
    /// # Returns
    /// * `bool` - `true` if the index needs more probe statistics, `false`
    ///   otherwise.
    pub fn need_more_probe_stats(&self) -> bool {
        self.refiner.need_more_probe_stats()
    }

    /// Merge the probe statistics into the index.
    ///
    /// # Arguments
    /// * `stats` - The probe statistics to merge.
    pub fn merge_probe_stats(&self, stats: GeoStatistics) {
        self.refiner.merge_probe_stats(stats);
    }

    /// Get the bitmaps for tracking visited left-side indices. The bitmaps will
    /// be updated by the spatial join stream when producing output batches
    /// during index probing phase.
    pub fn visited_left_side(&self) -> Option<&Mutex<Vec<BooleanBufferBuilder>>> {
        self.visited_left_side.as_ref()
    }

    /// Decrements counter of running threads, and returns `true`
    /// if caller is the last running thread
    pub fn report_probe_completed(&self) -> bool {
        self.probe_threads_counter.fetch_sub(1, Ordering::Relaxed) == 1
    }

    /// Get the memory usage of the refiner in bytes.
    pub fn get_refiner_mem_usage(&self) -> usize {
        self.refiner.mem_usage()
    }

    /// Get the actual execution mode used by the refiner
    pub fn get_actual_execution_mode(&self) -> ExecutionMode {
        self.refiner.actual_execution_mode()
    }
}

pub struct IndexQueryResult<'a, 'b> {
    pub wkb: &'b Wkb<'a>,
    pub distance: Option<f64>,
    pub geom_idx: usize,
    pub position: (i32, i32),
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn build_index(
    mut build_schema: SchemaRef, build_streams: Vec<SendableRecordBatchStream>,
    spatial_predicate: SpatialPredicate, options: SpatialJoinOptions,
    metrics_vec: Vec<SpatialJoinBuildMetrics>, memory_pool: Arc<dyn MemoryPool>,
    join_type: JoinType, probe_threads_count: usize,
) -> Result<SpatialIndex> {
    // Handle empty streams case
    if build_streams.is_empty() {
        let consumer = MemoryConsumer::new("SpatialJoinIndex");
        let reservation = consumer.register(&memory_pool);
        return Ok(SpatialIndex::empty(
            spatial_predicate,
            build_schema,
            options,
            AtomicUsize::new(probe_threads_count),
            reservation,
        ));
    }

    // Update schema from the first stream
    build_schema = build_streams.first().unwrap().schema();
    let metrics = metrics_vec.first().unwrap().clone();
    let evaluator = create_operand_evaluator(&spatial_predicate, options.clone());

    // Spawn all tasks to scan all build streams concurrently
    let mut join_set = JoinSet::new();
    let collect_statistics = matches!(options.execution_mode, ExecutionMode::Speculative(_));
    for (partition, (stream, metrics)) in build_streams.into_iter().zip(metrics_vec).enumerate() {
        let per_task_evaluator = Arc::clone(&evaluator);
        let consumer = MemoryConsumer::new(format!("SpatialJoinFetchBuild[{partition}]"));
        let reservation = consumer.register(&memory_pool);
        join_set.spawn(async move {
            collect_build_partition(
                stream,
                per_task_evaluator.as_ref(),
                &metrics,
                reservation,
                collect_statistics,
            )
            .await
        });
    }

    // Process each task as it completes and add batches to builder
    let results = join_set.join_all().await;

    // Create the builder to build the index
    let mut builder = SpatialIndexBuilder::new(
        spatial_predicate,
        options,
        join_type,
        probe_threads_count,
        memory_pool.clone(),
        metrics,
    );
    for result in results {
        let build_partition =
            result.map_err(|e| DataFusionError::Execution(format!("Task join error: {e}")))?;
        // Add each geometry batch to the builder
        for indexed_batch in build_partition.batches {
            builder.add_batch(indexed_batch);
        }
        builder.with_stats(build_partition.stats);
        // build_partition.reservation will be dropped here.
    }

    // Finish building the index
    builder.finish(build_schema)
}

struct BuildPartition {
    batches: Vec<IndexedBatch>,
    stats: GeoStatistics,

    /// Memory reservation for tracking the memory usage of the build partition
    /// Cleared on `BuildPartition` drop
    #[allow(dead_code)]
    reservation: MemoryReservation,
}

async fn collect_build_partition(
    mut stream: SendableRecordBatchStream, evaluator: &dyn OperandEvaluator,
    metrics: &SpatialJoinBuildMetrics, mut reservation: MemoryReservation,
    collect_statistics: bool,
) -> Result<BuildPartition> {
    let mut batches = Vec::new();
    let mut analyzer = AnalyzeAccumulator::new();

    while let Some(batch) = stream.next().await {
        let build_timer = metrics.build_time.timer();
        let batch = batch?;

        metrics.build_input_rows.add(batch.num_rows());
        metrics.build_input_batches.add(1);

        let geom_array = evaluator.evaluate_build(&batch)?;
        let indexed_batch = IndexedBatch { batch, geom_array };

        // Update statistics for each geometry in the batch
        if collect_statistics {
            for wkb in indexed_batch.geom_array.wkbs().iter().flatten() {
                analyzer.update_statistics(wkb, wkb.buf().len())?;
            }
        }

        let in_mem_size = indexed_batch.in_mem_size();
        batches.push(indexed_batch);

        reservation.grow(in_mem_size);
        metrics.build_mem_used.add(in_mem_size);
        build_timer.done();
    }

    Ok(BuildPartition {
        batches,
        stats: analyzer.finish(),
        reservation,
    })
}

/// Rough estimate for in-memory size of the rtree per rect in bytes
const RTREE_MEMORY_ESTIMATE_PER_RECT: usize = 60;

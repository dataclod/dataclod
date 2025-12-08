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
use futures::StreamExt;
use geo_index::rtree::sort::STRSort;
use geo_index::rtree::{RTree, RTreeBuilder, RTreeIndex};
use geo_types::Rect;
use parking_lot::Mutex;
use wkb::reader::Wkb;

use crate::spatial::join::option::{ExecutionMode, SpatialJoinOptions};
use crate::spatial::join::prep_geom_array::{OwnedPreparedGeometry, PreparedGeometryArray};
use crate::spatial::join::spatial_predicate::{
    EvaluatedGeometryArray, SpatialPredicate, SpatialPredicateEvaluator,
};
use crate::spatial::join::stream::SpatialJoinMetrics;
use crate::spatial::join::utils::need_produce_result_in_final;

// Type aliases for better readability
type SpatialRTree = RTree<f64>;
type DataIdToBatchPos = Vec<(i32, i32)>;
type RTreeBuildResult = (SpatialRTree, DataIdToBatchPos);

/// Builder for constructing a SpatialIndex from geometry batches.
///
/// This builder handles:
/// 1. Accumulating geometry batches to be indexed
/// 2. Building the spatial R-tree index
/// 3. Setting up memory tracking and visited bitmaps
/// 4. Configuring prepared geometries based on execution mode
pub(crate) struct SpatialIndexBuilder {
    options: SpatialJoinOptions,
    join_type: JoinType,
    probe_threads_count: usize,
    metrics: SpatialJoinMetrics,

    /// Batches to be indexed
    indexed_batches: Vec<IndexedBatch>,
    /// Memory reservation for tracking the memory usage of the spatial index
    reservation: MemoryReservation,
}

impl SpatialIndexBuilder {
    /// Create a new builder with the given configuration.
    pub fn new(
        options: SpatialJoinOptions, join_type: JoinType, probe_threads_count: usize,
        memory_pool: Arc<dyn MemoryPool>, metrics: SpatialJoinMetrics,
    ) -> Self {
        let consumer = MemoryConsumer::new("SpatialJoinIndex");
        let reservation = consumer.register(&memory_pool);

        Self {
            options,
            join_type,
            probe_threads_count,
            metrics,
            indexed_batches: Vec::new(),
            reservation,
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

    /// Create rtree data index to prepared geometry array mapping when needed
    fn build_prepared_geometries_array(
        &mut self, batch_pos_vec: &Vec<(i32, i32)>,
    ) -> Option<(PreparedGeometryArray, Vec<usize>)> {
        if !matches!(
            self.options.execution_mode,
            ExecutionMode::PrepareBuild | ExecutionMode::Speculative(_)
        ) {
            return None;
        }

        let mut num_geometries = 0;
        let mut batch_idx_offset = Vec::with_capacity(self.indexed_batches.len() + 1);
        batch_idx_offset.push(0);
        for batch in &self.indexed_batches {
            num_geometries += batch.batch.num_rows();
            batch_idx_offset.push(num_geometries);
        }

        let prepared_geometries = PreparedGeometryArray::new(num_geometries);
        let mut prepared_geom_idx_vec = Vec::with_capacity(batch_pos_vec.len());
        for (batch_idx, row_idx) in batch_pos_vec {
            // Convert (batch_idx, row_idx) to a linear, sequential index
            let batch_offset = batch_idx_offset[*batch_idx as usize];
            let prepared_idx = batch_offset + *row_idx as usize;
            prepared_geom_idx_vec.push(prepared_idx);
        }

        Some((prepared_geometries, prepared_geom_idx_vec))
    }

    /// Finish building and return the completed SpatialIndex.
    pub fn finish(mut self, schema: SchemaRef) -> Result<SpatialIndex> {
        if self.indexed_batches.is_empty() {
            return Ok(SpatialIndex::empty(
                schema,
                AtomicUsize::new(self.probe_threads_count),
                self.reservation,
            ));
        }

        let (rtree, batch_pos_vec) = self.build_rtree();
        let visited_left_side = self.build_visited_bitmaps()?;
        let (prepared_geom_array, prepared_geom_idx_vec) =
            match self.build_prepared_geometries_array(&batch_pos_vec) {
                Some((prepared_geom_array, prepared_geom_idx_vec)) => {
                    (Some(prepared_geom_array), prepared_geom_idx_vec)
                }
                None => (None, Vec::new()),
            };

        Ok(SpatialIndex {
            schema,
            rtree,
            data_id_to_batch_pos: batch_pos_vec,
            indexed_batches: self.indexed_batches,
            prepared_geometries: prepared_geom_array,
            prepared_geom_idx_vec,
            visited_left_side,
            options: self.options,
            probe_threads_counter: AtomicUsize::new(self.probe_threads_count),
            reservation: self.reservation,
        })
    }
}

pub(crate) struct SpatialIndex {
    schema: SchemaRef,

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

    /// Prepared geometries for the build side
    prepared_geometries: Option<PreparedGeometryArray>,
    /// An array for translating rtree data index to prepared geometries array
    /// index
    prepared_geom_idx_vec: Vec<usize>,

    /// Shared bitmap builders for visited left indices, one per batch
    visited_left_side: Option<Mutex<Vec<BooleanBufferBuilder>>>,

    /// Options for spatial join execution
    options: SpatialJoinOptions,

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
        schema: SchemaRef, probe_threads_counter: AtomicUsize, reservation: MemoryReservation,
    ) -> Self {
        let rtree_builder = RTreeBuilder::<f64>::new(0);
        let rtree = rtree_builder.finish::<STRSort>();
        Self {
            schema,
            rtree,
            data_id_to_batch_pos: Vec::new(),
            indexed_batches: Vec::new(),
            prepared_geometries: None,
            prepared_geom_idx_vec: Vec::new(),
            visited_left_side: None,
            options: SpatialJoinOptions::default(),
            probe_threads_counter,
            reservation,
        }
    }

    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// Get the batch at the given index.
    ///
    /// # Safety
    ///
    /// The index must be valid. It should be a batch index value retrieved from
    /// `query`.
    pub unsafe fn get_indexed_batch(&self, batch_idx: usize) -> &RecordBatch {
        unsafe { &self.indexed_batches.get_unchecked(batch_idx).batch }
    }

    pub fn query(
        &self, probe_wkb: &Wkb, probe_rect: &Rect, distance: &Option<ColumnarValue>,
        evaluator: &dyn SpatialPredicateEvaluator, build_batch_positions: &mut Vec<(i32, i32)>,
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

        match self.options.execution_mode {
            ExecutionMode::PrepareBuild => {
                self.refine_prepare_build(
                    probe_wkb,
                    &candidates,
                    evaluator,
                    distance,
                    build_batch_positions,
                )
            }
            ExecutionMode::PrepareProbe => {
                self.refine_prepare_probe(
                    probe_wkb,
                    &candidates,
                    evaluator,
                    distance,
                    build_batch_positions,
                )
            }
            ExecutionMode::PrepareNone => {
                self.refine_prepare_none(
                    probe_wkb,
                    &candidates,
                    evaluator,
                    distance,
                    build_batch_positions,
                )
            }
            ExecutionMode::Speculative(_) => {
                unimplemented!("ExecutionMode::Speculative for spatial joins is not implemented")
            }
        }
    }

    fn refine_prepare_none(
        &self, probe_wkb: &Wkb, candidates: &[u32], evaluator: &dyn SpatialPredicateEvaluator,
        distance: &Option<ColumnarValue>, build_batch_positions: &mut Vec<(i32, i32)>,
    ) -> Result<JoinResultMetrics> {
        let candidate_count = candidates.len();
        let batch_positions = candidates
            .iter()
            .map(|c| unsafe { *self.data_id_to_batch_pos.get_unchecked(*c as usize) })
            .collect::<Vec<_>>();

        let mut num_results = 0;

        for (batch_idx, row_idx) in batch_positions {
            let indexed_batch = &self.indexed_batches[batch_idx as usize];
            let build_wkb = indexed_batch.wkb(row_idx as usize);
            let Some(build_wkb) = build_wkb else {
                continue;
            };
            let distance =
                evaluator.resolve_distance(indexed_batch.distance(), distance, row_idx as usize)?;
            if evaluator.evaluate_predicate(build_wkb, probe_wkb, distance)? {
                num_results += 1;
                build_batch_positions.push((batch_idx, row_idx));
            }
        }

        Ok(JoinResultMetrics {
            count: num_results,
            candidate_count,
        })
    }

    fn refine_prepare_probe(
        &self, probe_wkb: &Wkb, candidates: &[u32], evaluator: &dyn SpatialPredicateEvaluator,
        distance: &Option<ColumnarValue>, build_batch_positions: &mut Vec<(i32, i32)>,
    ) -> Result<JoinResultMetrics> {
        let candidate_count = candidates.len();
        let batch_positions = candidates
            .iter()
            .map(|c| unsafe { *self.data_id_to_batch_pos.get_unchecked(*c as usize) })
            .collect::<Vec<_>>();

        let mut num_results = 0;
        let prep_geom = OwnedPreparedGeometry::try_from_wkb(probe_wkb.buf())?;

        for (batch_idx, row_idx) in batch_positions {
            let indexed_batch = &self.indexed_batches[batch_idx as usize];
            let build_wkb = indexed_batch.wkb(row_idx as usize);
            let Some(build_wkb) = build_wkb else {
                continue;
            };
            let distance =
                evaluator.resolve_distance(indexed_batch.distance(), distance, row_idx as usize)?;
            let build_geom = geos::Geometry::new_from_wkb(build_wkb.buf())
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            if evaluator.evaluate_predicate_prepare_right(&build_geom, &prep_geom, distance)? {
                num_results += 1;
                build_batch_positions.push((batch_idx, row_idx));
            }
        }

        Ok(JoinResultMetrics {
            count: num_results,
            candidate_count,
        })
    }

    fn refine_prepare_build(
        &self, probe_wkb: &Wkb, candidates: &[u32], evaluator: &dyn SpatialPredicateEvaluator,
        distance: &Option<ColumnarValue>, build_batch_positions: &mut Vec<(i32, i32)>,
    ) -> Result<JoinResultMetrics> {
        let candidate_count = candidates.len();
        let probe_geom: geos::Geometry = geos::Geometry::new_from_wkb(probe_wkb.buf())
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let prepared_geometries = self.prepared_geometries.as_ref().unwrap();
        let mut num_results = 0;
        for data_index in candidates {
            let (batch_idx, row_idx) = unsafe {
                *self
                    .data_id_to_batch_pos
                    .get_unchecked(*data_index as usize)
            };
            let indexed_batch = &self.indexed_batches[batch_idx as usize];
            let wkb = indexed_batch.wkb(row_idx as usize);
            let Some(wkb) = wkb else {
                continue;
            };

            let distance =
                evaluator.resolve_distance(indexed_batch.distance(), distance, row_idx as usize)?;

            let prepared_idx = self.prepared_geom_idx_vec[*data_index as usize];
            let owned_prep_geom = prepared_geometries.get_or_create(prepared_idx, || {
                OwnedPreparedGeometry::try_from_wkb(wkb.buf())
            })?;

            if evaluator.evaluate_predicate_prepare_left(owned_prep_geom, &probe_geom, distance)? {
                build_batch_positions.push((batch_idx, row_idx));
                num_results += 1;
            }
        }

        Ok(JoinResultMetrics {
            count: num_results,
            candidate_count,
        })
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
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn build_index(
    mut build_schema: SchemaRef, build_streams: Vec<SendableRecordBatchStream>,
    spatial_predicate: SpatialPredicate, options: SpatialJoinOptions,
    metrics_vec: Vec<SpatialJoinMetrics>, memory_pool: Arc<dyn MemoryPool>, join_type: JoinType,
    probe_threads_count: usize,
) -> Result<SpatialIndex> {
    // Handle empty streams case
    if build_streams.is_empty() {
        let consumer = MemoryConsumer::new("SpatialJoinIndex");
        let reservation = consumer.register(&memory_pool);
        return Ok(SpatialIndex::empty(
            build_schema,
            AtomicUsize::new(probe_threads_count),
            reservation,
        ));
    }

    // Update schema from the first stream
    build_schema = build_streams.first().unwrap().schema();
    let metrics = metrics_vec.first().unwrap().clone();
    let evaluator = spatial_predicate.evaluator(options.clone());

    // Spawn all tasks to scan all build streams concurrently
    let mut join_set = JoinSet::new();
    for (partition, (stream, metrics)) in build_streams.into_iter().zip(metrics_vec).enumerate() {
        let per_task_evaluator = Arc::clone(&evaluator);
        let consumer = MemoryConsumer::new(format!("SpatialJoinFetchBuild[{partition}]"));
        let reservation = consumer.register(&memory_pool);
        join_set.spawn(async move {
            collect_build_partition(stream, per_task_evaluator.as_ref(), &metrics, reservation)
                .await
        });
    }

    // Process each task as it completes and add batches to builder
    let results = join_set.join_all().await;

    // Create the builder to build the index
    let mut builder = SpatialIndexBuilder::new(
        options,
        join_type,
        probe_threads_count,
        memory_pool.clone(),
        metrics,
    );
    for result in results {
        let (partition_batches, _collect_reservation) =
            result.map_err(|e| DataFusionError::Execution(format!("Task join error: {e}")))?;
        // Add each geometry batch to the builder
        for indexed_batch in partition_batches {
            builder.add_batch(indexed_batch);
        }
        // collect_reservation will be dropped here, builder manages its own
        // memory
    }

    // Finish building the index
    builder.finish(build_schema)
}

async fn collect_build_partition(
    mut stream: SendableRecordBatchStream, evaluator: &dyn SpatialPredicateEvaluator,
    metrics: &SpatialJoinMetrics, mut reservation: MemoryReservation,
) -> Result<(Vec<IndexedBatch>, MemoryReservation)> {
    let mut batches = Vec::new();

    while let Some(batch) = stream.next().await {
        let build_timer = metrics.build_time.timer();
        let batch = batch?;

        metrics.build_input_rows.add(batch.num_rows());
        metrics.build_input_batches.add(1);

        let geom_array = evaluator.evaluate_build(&batch)?;
        let indexed_batch = IndexedBatch { batch, geom_array };

        let in_mem_size = indexed_batch.in_mem_size();
        batches.push(indexed_batch);

        reservation.grow(in_mem_size);
        metrics.build_mem_used.add(in_mem_size);
        build_timer.done();
    }

    Ok((batches, reservation))
}

/// Rough estimate for in-memory size of the rtree per rect in bytes
const RTREE_MEMORY_ESTIMATE_PER_RECT: usize = 60;

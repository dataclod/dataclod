use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use datafusion::arrow::array::{Array, ArrayRef, BooleanBufferBuilder, RecordBatch};
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

use crate::spatial::join::option::SpatialJoinOptions;
use crate::spatial::join::spatial_predicate::{
    GeometryBatchResult, SpatialPredicate, SpatialPredicateEvaluator,
};
use crate::spatial::join::stream::SpatialJoinMetrics;
use crate::spatial::join::utils::need_produce_result_in_final;
use crate::spatial::join::wkb_array::GetGeo;

pub(crate) struct SpatialIndex {
    schema: SchemaRef,
    rtree: RTree<f64>,
    batch_pos_vec: Vec<(i32, i32)>,
    geometry_batches: Vec<GeometryBatch>,
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

#[derive(Debug)]
pub(crate) struct GeometryBatch {
    batch: RecordBatch,
    rects: Vec<(usize, Rect)>,
    geometry_array: ArrayRef,
    distance: Option<ColumnarValue>,
}

impl GeometryBatch {
    pub fn in_mem_size(&self) -> usize {
        let rect_in_mem_size = self.rects.allocated_size();
        let distance_in_mem_size = match &self.distance {
            Some(ColumnarValue::Array(array)) => array.get_array_memory_size(),
            _ => 8,
        };
        self.batch.get_array_memory_size()
            + self.geometry_array.get_array_memory_size()
            + rect_in_mem_size
            + distance_in_mem_size
    }
}

pub struct JoinResultMetrics {
    pub count: usize,
    pub candidate_count: usize,
}

impl SpatialIndex {
    pub fn new(
        schema: SchemaRef, rtree: RTree<f64>, batch_pos_vec: Vec<(i32, i32)>,
        geometry_batches: Vec<GeometryBatch>,
        visited_left_side: Option<Mutex<Vec<BooleanBufferBuilder>>>,
        probe_threads_counter: AtomicUsize, reservation: MemoryReservation,
    ) -> Self {
        Self {
            schema,
            rtree,
            batch_pos_vec,
            geometry_batches,
            visited_left_side,
            probe_threads_counter,
            reservation,
        }
    }

    pub fn empty(
        schema: SchemaRef, probe_threads_counter: AtomicUsize, reservation: MemoryReservation,
    ) -> Self {
        let rtree_builder = RTreeBuilder::<f64>::new(0);
        let rtree = rtree_builder.finish::<STRSort>();
        Self {
            schema,
            rtree,
            batch_pos_vec: Vec::new(),
            geometry_batches: Vec::new(),
            visited_left_side: None,
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
        unsafe { &self.geometry_batches.get_unchecked(batch_idx).batch }
    }

    pub fn query(
        &self, probe_wkb: &Wkb, probe_rect: &Rect, distance: &Option<ColumnarValue>,
        evaluator: &dyn SpatialPredicateEvaluator, build_batch_positions: &mut Vec<(i32, i32)>,
    ) -> Result<JoinResultMetrics> {
        let min = probe_rect.min();
        let max = probe_rect.max();
        let candidates = self.rtree.search(min.x, min.y, max.x, max.y);
        let candidate_count = candidates.len();
        let mut batch_positions = candidates
            .into_iter()
            .map(|c| unsafe { *self.batch_pos_vec.get_unchecked(c as usize) })
            .collect::<Vec<_>>();
        batch_positions.sort_unstable();

        let mut num_results = 0;
        let mut matched_build_indices = Vec::new(); // Track matched indices for bitmap update

        for (batch_idx, row_idx) in batch_positions {
            let geometry_batch = &self.geometry_batches[batch_idx as usize];
            // SAFETY: row_idx comes from the spatial index query result, so it must be
            // valid
            let build_wkb = geometry_batch.geometry_array.get_wkb(row_idx as usize)?;
            let Some(build_wkb) = build_wkb else {
                continue;
            };
            let distance =
                evaluator.resolve_distance(&geometry_batch.distance, distance, row_idx as usize)?;
            if evaluator.evaluate_predicate(&build_wkb, probe_wkb, distance)? {
                num_results += 1;
                build_batch_positions.push((batch_idx, row_idx));
                matched_build_indices.push((batch_idx, row_idx));
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
    let consumer = MemoryConsumer::new("SpatialJoinIndex");
    let mut reservation = consumer.register(&memory_pool);

    let num_partitions = build_streams.len();
    if num_partitions == 0 {
        return Ok(SpatialIndex::empty(
            build_schema,
            AtomicUsize::new(probe_threads_count),
            reservation,
        ));
    }
    build_schema = build_streams.first().unwrap().schema();

    let metrics = metrics_vec.first().unwrap().clone();

    let evaluator = spatial_predicate.evaluator(options.clone());

    // Spawn all tasks to scan all build streams concurrently.
    // Collect all tasks into FuturesUnordered to process them as they complete.
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

    // Process each task as it completes
    let results = join_set.join_all().await;

    let build_timer = metrics.build_time.timer();
    let mut geometry_batches = Vec::new();
    let mut num_rects = 0;
    for result in results {
        let (partition_batches, collect_reservation) =
            result.map_err(|e| DataFusionError::Execution(format!("Task join error: {e}")))?;
        num_rects += partition_batches
            .iter()
            .map(|batch| batch.rects.len())
            .sum::<usize>();
        reservation.grow(collect_reservation.size());
        geometry_batches.extend(partition_batches);
        // collect_reservation will be dropped here
    }

    // Now all tasks have completed and results are in partial_results
    let mut rtree_builder = RTreeBuilder::<f64>::new(num_rects as u32);
    let mut batch_pos_vec = vec![(0, 0); num_rects];
    let rtree_mem_estimate = num_rects * RTREE_MEMORY_ESTIMATE_PER_RECT; // rough estimate for in-memory size of the rtree
    reservation.grow(batch_pos_vec.allocated_size() + rtree_mem_estimate);
    for (batch_idx, batch) in geometry_batches.iter().enumerate() {
        batch.rects.iter().for_each(|(idx, rect)| {
            let min = rect.min();
            let max = rect.max();
            let data_idx = rtree_builder.add(min.x, min.y, max.x, max.y);
            batch_pos_vec[data_idx as usize] = (batch_idx as i32, *idx as i32);
        });
    }

    let rtree = rtree_builder.finish::<STRSort>();
    build_timer.done();

    metrics.build_mem_used.add(reservation.size());

    // Initialize bitmaps for tracking visited left-side indices if needed (one per
    // batch)
    let visited_left_side = if need_produce_result_in_final(join_type) {
        let mut bitmaps = Vec::with_capacity(geometry_batches.len());
        let mut total_buffer_size = 0;

        for batch in &geometry_batches {
            let batch_rows = batch.batch.num_rows();
            let buffer_size = batch_rows.div_ceil(8);
            total_buffer_size += buffer_size;

            let mut bitmap = BooleanBufferBuilder::new(batch_rows);
            bitmap.append_n(batch_rows, false);
            bitmaps.push(bitmap);
        }

        reservation.try_grow(total_buffer_size)?;
        metrics.build_mem_used.add(total_buffer_size);
        Some(Mutex::new(bitmaps))
    } else {
        None
    };

    Ok(SpatialIndex::new(
        build_schema,
        rtree,
        batch_pos_vec,
        geometry_batches,
        visited_left_side,
        AtomicUsize::new(probe_threads_count),
        reservation,
    ))
}

async fn collect_build_partition(
    mut stream: SendableRecordBatchStream, evaluator: &dyn SpatialPredicateEvaluator,
    metrics: &SpatialJoinMetrics, mut reservation: MemoryReservation,
) -> Result<(Vec<GeometryBatch>, MemoryReservation)> {
    let mut batches = Vec::new();

    while let Some(batch) = stream.next().await {
        let build_timer = metrics.build_time.timer();
        let batch = batch?;

        metrics.build_input_rows.add(batch.num_rows());
        metrics.build_input_batches.add(1);

        let GeometryBatchResult {
            geometry_array,
            rects,
            distance,
        } = evaluator.evaluate_build(&batch)?;

        let geometry_batch = GeometryBatch {
            batch,
            rects,
            geometry_array,
            distance,
        };

        let in_mem_size = geometry_batch.in_mem_size();
        batches.push(geometry_batch);

        reservation.grow(in_mem_size);
        metrics.build_mem_used.add(in_mem_size);
        build_timer.done();
    }

    Ok((batches, reservation))
}

/// Rough estimate for in-memory size of the rtree per rect in bytes
const RTREE_MEMORY_ESTIMATE_PER_RECT: usize = 60;

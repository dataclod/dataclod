use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, RecordBatch};
use datafusion::common::runtime::JoinSet;
use datafusion::common::utils::proxy::VecAllocExt;
use datafusion::common::{DataFusionError, Result};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryPool, MemoryReservation};
use datafusion::logical_expr::ColumnarValue;
use futures::StreamExt;
use geo_index::rtree::sort::STRSort;
use geo_index::rtree::{RTree, RTreeBuilder, RTreeIndex};
use geo_types::Rect;
use wkb::reader::Wkb;

use crate::spatial::join::option::SpatialJoinOptions;
use crate::spatial::join::spatial_predicate::{
    GeometryBatchResult, SpatialPredicate, SpatialPredicateEvaluator,
};
use crate::spatial::join::stream::SpatialJoinMetrics;
use crate::spatial::join::wkb_array::GetGeo;

pub(crate) struct SpatialIndex {
    rtree: RTree<f64>,
    batch_pos_vec: Vec<(i32, i32)>,
    geometry_batches: Vec<GeometryBatch>,
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
        rtree: RTree<f64>, batch_pos_vec: Vec<(i32, i32)>, geometry_batches: Vec<GeometryBatch>,
        reservation: MemoryReservation,
    ) -> Self {
        Self {
            rtree,
            batch_pos_vec,
            geometry_batches,
            reservation,
        }
    }

    pub fn empty(reservation: MemoryReservation) -> Self {
        let rtree_builder = RTreeBuilder::<f64>::new(0);
        let rtree = rtree_builder.finish::<STRSort>();
        Self {
            rtree,
            batch_pos_vec: Vec::new(),
            geometry_batches: Vec::new(),
            reservation,
        }
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
            }
        }

        Ok(JoinResultMetrics {
            count: num_results,
            candidate_count,
        })
    }
}

pub(crate) async fn build_index(
    build_streams: Vec<SendableRecordBatchStream>, spatial_predicate: SpatialPredicate,
    options: SpatialJoinOptions, metrics_vec: Vec<SpatialJoinMetrics>,
    memory_pool: Arc<dyn MemoryPool>,
) -> Result<SpatialIndex> {
    let consumer = MemoryConsumer::new("SpatialJoinIndex");
    let mut reservation = consumer.register(&memory_pool);

    let num_partitions = build_streams.len();
    if num_partitions == 0 {
        return Ok(SpatialIndex::empty(reservation));
    }

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

    Ok(SpatialIndex::new(
        rtree,
        batch_pos_vec,
        geometry_batches,
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

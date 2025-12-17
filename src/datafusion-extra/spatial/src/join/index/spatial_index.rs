use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use datafusion::arrow::array::{BooleanBufferBuilder, RecordBatch};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::Result;
use datafusion::execution::memory_pool::MemoryReservation;
use geo_index::rtree::sort::HilbertSort;
use geo_index::rtree::{RTree, RTreeBuilder, RTreeIndex};
use geo_types::Rect;
use parking_lot::Mutex;
use wkb::reader::Wkb;

use crate::join::evaluated_batch::EvaluatedBatch;
use crate::join::index::{IndexQueryResult, QueryResultMetrics};
use crate::join::operand_evaluator::{OperandEvaluator, create_operand_evaluator};
use crate::join::refine::{IndexQueryResultRefiner, create_refiner};
use crate::join::spatial_predicate::SpatialPredicate;
use crate::join::utils::concurrent_reservation::ConcurrentReservation;
use crate::option::{ExecutionMode, SpatialJoinOptions};
use crate::statistics::GeoStatistics;

pub struct SpatialIndex {
    pub schema: SchemaRef,

    /// The spatial predicate evaluator for the spatial predicate.
    pub evaluator: Arc<dyn OperandEvaluator>,

    /// The refiner for refining the index query results.
    pub refiner: Arc<dyn IndexQueryResultRefiner>,

    /// Memory reservation for tracking the memory usage of the refiner
    pub refiner_reservation: ConcurrentReservation,

    /// R-tree index for the geometry batches. It takes MBRs as query windows
    /// and returns data indexes. These data indexes should be translated
    /// using `data_id_to_batch_pos` to get the original geometry batch
    /// index and row index, or translated using `prepared_geom_idx_vec`
    /// to get the prepared geometries array index.
    pub rtree: RTree<f32>,

    /// Indexed batches containing evaluated geometry arrays. It contains the
    /// original record batches and geometry arrays obtained by evaluating
    /// the geometry expression on the build side.
    pub indexed_batches: Vec<EvaluatedBatch>,
    /// An array for translating rtree data index to geometry batch index and
    /// row index
    pub data_id_to_batch_pos: Vec<(i32, i32)>,

    /// An array for translating rtree data index to consecutive index. Each
    /// geometry may be indexed by multiple boxes, so there could be
    /// multiple data indexes for the same geometry. A mapping for squashing
    /// the index makes it easier for persisting per-geometry auxiliary data for
    /// evaluating the spatial predicate. This is extensively used by the
    /// spatial predicate evaluators for storing prepared geometries.
    pub geom_idx_vec: Vec<usize>,

    /// Shared bitmap builders for visited left indices, one per batch
    pub visited_left_side: Option<Mutex<Vec<BooleanBufferBuilder>>>,

    /// Counter of running probe-threads, potentially able to update `bitmap`.
    /// Each time a probe thread finished probing the index, it will decrement
    /// the counter. The last finished probe thread will produce the extra
    /// output batches for unmatched build side when running left-outer
    /// joins. See also [`report_probe_completed`].
    pub probe_threads_counter: AtomicUsize,
}

impl SpatialIndex {
    pub fn empty(
        spatial_predicate: SpatialPredicate, schema: SchemaRef, options: SpatialJoinOptions,
        probe_threads_counter: AtomicUsize, mut reservation: MemoryReservation,
    ) -> Self {
        let evaluator = create_operand_evaluator(&spatial_predicate);
        let refiner = create_refiner(
            options.spatial_library,
            &spatial_predicate,
            options,
            0,
            GeoStatistics::empty(),
        );
        let refiner_reservation = reservation.split(0);
        let refiner_reservation = ConcurrentReservation::new(0, refiner_reservation);
        let rtree = RTreeBuilder::<f32>::new(0).finish::<HilbertSort>();
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
    ///   (`batch_idx`, `row_idx`) pairs for each matching build-side geometry
    ///
    /// # Returns
    /// * `JoinResultMetrics` containing the number of actual matches (`count`)
    ///   and the number of candidates from the filter phase (`candidate_count`)
    pub fn query(
        &self, probe_wkb: &Wkb, probe_rect: &Rect<f32>, distance: &Option<f64>,
        build_batch_positions: &mut Vec<(i32, i32)>,
    ) -> Result<QueryResultMetrics> {
        let min = probe_rect.min();
        let max = probe_rect.max();
        let mut candidates = self.rtree.search(min.x, min.y, max.x, max.y);
        if candidates.is_empty() {
            return Ok(QueryResultMetrics {
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
        &self, probe_wkb: &Wkb, candidates: &[u32], distance: &Option<f64>,
        build_batch_positions: &mut Vec<(i32, i32)>,
    ) -> Result<QueryResultMetrics> {
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
                row_idx as usize,
                distance,
            )?;
            let geom_idx = self.geom_idx_vec[*data_idx as usize];
            index_query_results.push(IndexQueryResult {
                wkb: build_wkb,
                distance,
                geom_idx,
                position: pos,
            });
        }

        if index_query_results.is_empty() {
            return Ok(QueryResultMetrics {
                count: 0,
                candidate_count,
            });
        }

        let results = self.refiner.refine(probe_wkb, &index_query_results)?;
        let num_results = results.len();
        build_batch_positions.extend(results);

        // Update refiner memory reservation
        self.refiner_reservation.resize(self.refiner.mem_usage())?;

        Ok(QueryResultMetrics {
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

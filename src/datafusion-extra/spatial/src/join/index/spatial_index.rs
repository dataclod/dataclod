use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use datafusion::arrow::array::{BooleanBufferBuilder, RecordBatch};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::runtime::JoinSet;
use datafusion::common::{Result, exec_err};
use datafusion::error::DataFusionError;
use geo_index::rtree::sort::HilbertSort;
use geo_index::rtree::{RTree, RTreeBuilder, RTreeIndex};
use parking_lot::Mutex;
use wkb::reader::Wkb;

use crate::join::evaluated_batch::EvaluatedBatch;
use crate::join::index::{IndexQueryResult, QueryResultMetrics};
use crate::join::operand_evaluator::{
    OperandEvaluator, create_operand_evaluator, distance_value_at,
};
use crate::join::refine::{IndexQueryResultRefiner, create_refiner};
use crate::join::spatial_predicate::SpatialPredicate;
use crate::option::{ExecutionMode, SpatialJoinOptions};
use crate::statistics::GeoStatistics;

pub struct SpatialIndex {
    pub schema: SchemaRef,
    pub options: SpatialJoinOptions,

    /// The spatial predicate evaluator for the spatial predicate.
    pub evaluator: Arc<dyn OperandEvaluator>,

    /// The refiner for refining the index query results.
    pub refiner: Arc<dyn IndexQueryResultRefiner>,

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

    /// Shared bitmap builders for visited build side indices, one per batch
    pub visited_build_side: Option<Mutex<Vec<BooleanBufferBuilder>>>,

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
        probe_threads_counter: AtomicUsize,
    ) -> Self {
        let evaluator = create_operand_evaluator(&spatial_predicate);
        let refiner = create_refiner(
            options.spatial_library,
            &spatial_predicate,
            options.clone(),
            0,
            GeoStatistics::empty(),
        );
        let rtree = RTreeBuilder::<f32>::new(0).finish::<HilbertSort>();
        Self {
            schema,
            options,
            evaluator,
            refiner,
            rtree,
            data_id_to_batch_pos: Vec::new(),
            indexed_batches: Vec::new(),
            geom_idx_vec: Vec::new(),
            visited_build_side: None,
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

    /// Query the spatial index with a batch of probe geometries to find
    /// matching build-side geometries.
    ///
    /// This method iterates over the probe geometries in the given range of the
    /// evaluated batch. For each probe geometry, it performs the two-phase
    /// spatial join query:
    /// 1. **Filter phase**: Uses the R-tree index with the probe geometry's
    ///    bounding rectangle to quickly identify candidate geometries.
    /// 2. **Refinement phase**: Evaluates the exact spatial predicate on
    ///    candidates to determine actual matches.
    ///
    /// # Arguments
    /// * `evaluated_batch` - The batch containing probe geometries and their
    ///   bounding rectangles
    /// * `range` - The range of rows in the evaluated batch to process.
    /// * `max_result_size` - The maximum number of results to collect before
    ///   stopping. If the number of results exceeds this limit, the method
    ///   returns early.
    /// * `build_batch_positions` - Output vector that will be populated with
    ///   (`batch_idx`, `row_idx`) pairs for each matching build-side geometry.
    /// * `probe_indices` - Output vector that will be populated with the probe
    ///   row index (in `evaluated_batch`) for each match appended to
    ///   `build_batch_positions`. This means the probe index is repeated `N`
    ///   times when a probe geometry produces `N` matches, keeping
    ///   `probe_indices.len()` in sync with `build_batch_positions.len()`.
    ///
    /// # Returns
    /// * A tuple containing:
    ///   - `QueryResultMetrics`: Aggregated metrics (total matches and
    ///     candidates) for the processed rows
    ///   - `usize`: The index of the next row to process (exclusive end of the
    ///     processed range)
    pub async fn query_batch(
        self: &Arc<Self>, evaluated_batch: &Arc<EvaluatedBatch>, range: Range<usize>,
        max_result_size: usize, build_batch_positions: &mut Vec<(i32, i32)>,
        probe_indices: &mut Vec<u32>,
    ) -> Result<(QueryResultMetrics, usize)> {
        if range.is_empty() {
            return Ok((
                QueryResultMetrics {
                    count: 0,
                    candidate_count: 0,
                },
                range.start,
            ));
        }

        let rects = evaluated_batch.rects();
        let dist = evaluated_batch.distance();
        let mut total_candidates_count = 0;
        let mut total_count = 0;
        let mut current_row_idx = range.start;
        for row_idx in range {
            current_row_idx = row_idx;
            let Some(probe_rect) = rects[row_idx] else {
                continue;
            };

            let min = probe_rect.min();
            let max = probe_rect.max();
            let mut candidates = self.rtree.search(min.x, min.y, max.x, max.y);
            if candidates.is_empty() {
                continue;
            }

            let Some(probe_wkb) = evaluated_batch.wkb(row_idx) else {
                return exec_err!("Failed to get WKB for row {} in evaluated batch", row_idx);
            };

            // Sort and dedup candidates to avoid duplicate results when we index one
            // geometry using several boxes.
            candidates.sort_unstable();
            candidates.dedup();

            let distance = match dist {
                Some(dist_array) => distance_value_at(dist_array, row_idx)?,
                None => None,
            };

            // Refine the candidates retrieved from the r-tree index by evaluating the
            // actual spatial predicate
            let refine_chunk_size = self.options.parallel_refinement_chunk_size;
            if refine_chunk_size == 0 || candidates.len() < refine_chunk_size * 2 {
                // For small candidate sets, use refine synchronously
                let metrics =
                    self.refine(probe_wkb, &candidates, &distance, build_batch_positions)?;
                probe_indices.extend(std::iter::repeat_n(row_idx as u32, metrics.count));
                total_count += metrics.count;
                total_candidates_count += metrics.candidate_count;
            } else {
                // For large candidate sets, spawn several tasks to parallelize refinement
                let (metrics, positions) = self
                    .refine_concurrently(
                        evaluated_batch,
                        row_idx,
                        &candidates,
                        distance,
                        refine_chunk_size,
                    )
                    .await?;
                build_batch_positions.extend(positions);
                probe_indices.extend(std::iter::repeat_n(row_idx as u32, metrics.count));
                total_count += metrics.count;
                total_candidates_count += metrics.candidate_count;
            }

            if total_count >= max_result_size {
                break;
            }
        }

        let end_idx = current_row_idx + 1;
        Ok((
            QueryResultMetrics {
                count: total_count,
                candidate_count: total_candidates_count,
            },
            end_idx,
        ))
    }

    async fn refine_concurrently(
        self: &Arc<Self>, evaluated_batch: &Arc<EvaluatedBatch>, row_idx: usize,
        candidates: &[u32], distance: Option<f64>, refine_chunk_size: usize,
    ) -> Result<(QueryResultMetrics, Vec<(i32, i32)>)> {
        let mut join_set = JoinSet::new();
        for (i, chunk) in candidates.chunks(refine_chunk_size).enumerate() {
            let cloned_evaluated_batch = evaluated_batch.clone();
            let chunk = chunk.to_vec();
            let index_ref = self.clone();
            join_set.spawn(async move {
                let Some(probe_wkb) = cloned_evaluated_batch.wkb(row_idx) else {
                    return (
                        i,
                        exec_err!("Failed to get WKB for row {} in evaluated batch", row_idx),
                    );
                };
                let mut local_positions: Vec<(i32, i32)> = Vec::with_capacity(chunk.len());
                let res = index_ref.refine(probe_wkb, &chunk, &distance, &mut local_positions);
                (i, res.map(|r| (r, local_positions)))
            });
        }

        // Collect the results in order
        let mut refine_results = Vec::with_capacity(join_set.len());
        refine_results.resize_with(join_set.len(), || None);
        while let Some(res) = join_set.join_next().await {
            let (chunk_idx, refine_res) =
                res.map_err(|e| DataFusionError::External(Box::new(e)))?;
            let (metrics, positions) = refine_res?;
            refine_results[chunk_idx] = Some((metrics, positions));
        }

        let mut total_metrics = QueryResultMetrics {
            count: 0,
            candidate_count: 0,
        };
        let mut all_positions = Vec::with_capacity(candidates.len());
        for res in refine_results {
            let (metrics, positions) = res.expect("All chunks should be processed");
            total_metrics.count += metrics.count;
            total_metrics.candidate_count += metrics.candidate_count;
            all_positions.extend(positions);
        }

        Ok((total_metrics, all_positions))
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
    pub fn visited_build_side(&self) -> Option<&Mutex<Vec<BooleanBufferBuilder>>> {
        self.visited_build_side.as_ref()
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

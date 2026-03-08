use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, RecordBatch};
use datafusion::arrow::compute;
use datafusion::common::{Result, ScalarValue, exec_err};
use datafusion::config::SpillCompression;
use datafusion::execution::disk_manager::RefCountedTempFile;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_plan::metrics::SpillMetrics;
use futures::StreamExt;

use crate::geometry::analyze::AnalyzeAccumulator;
use crate::geometry::bounding_box::BoundingBox;
use crate::geometry::interval::IntervalTrait;
use crate::join::evaluated_batch::EvaluatedBatch;
use crate::join::evaluated_batch::spill::EvaluatedBatchSpillWriter;
use crate::join::evaluated_batch::stream::SendableEvaluatedBatchStream;
use crate::join::operand_evaluator::EvaluatedGeometryArray;
use crate::join::partitioning::partition_slots::PartitionSlots;
use crate::join::partitioning::util::geo_rect_to_bbox;
use crate::join::partitioning::{PartitionedSide, SpatialPartition, SpatialPartitioner};
use crate::statistics::GeoStatistics;

/// Result emitted after a stream is spatially repartitioned.
#[derive(Debug)]
pub struct SpilledPartitions {
    slots: PartitionSlots,
    partitions: Vec<Option<SpilledPartition>>,
}

/// Metadata and spill files produced for a single spatial partition.
///
/// A `SpilledPartition` corresponds to one logical spatial partition (including
/// special partitions such as `None` or `Multi`) after the stream repartitioner
/// has flushed in-memory data to disk. It tracks the set of temporary spill
/// files that hold the partition's rows, along with aggregated geospatial
/// statistics and the total number of rows written.
#[derive(Debug, Clone)]
pub struct SpilledPartition {
    /// Temporary spill files containing the rows assigned to this partition.
    spill_files: Vec<Arc<RefCountedTempFile>>,
    /// Aggregated geospatial statistics computed over all rows in this
    /// partition.
    geo_statistics: GeoStatistics,
    /// Total number of rows that were written into `spill_files`.
    num_rows: usize,
}

impl SpilledPartition {
    /// Construct a spilled partition from finalized spill files and aggregated
    /// statistics.
    pub fn new(
        spill_files: Vec<Arc<RefCountedTempFile>>, geo_statistics: GeoStatistics, num_rows: usize,
    ) -> Self {
        Self {
            spill_files,
            geo_statistics,
            num_rows,
        }
    }

    /// Create an empty spilled partition (no files, empty stats, zero rows).
    pub fn empty() -> Self {
        Self::new(Vec::new(), GeoStatistics::empty(), 0)
    }

    /// Spill files produced for this partition.
    pub fn spill_files(&self) -> &[Arc<RefCountedTempFile>] {
        &self.spill_files
    }

    /// Aggregated geospatial statistics for this partition.
    pub fn geo_statistics(&self) -> &GeoStatistics {
        &self.geo_statistics
    }

    /// Total number of rows assigned to this partition.
    pub fn num_rows(&self) -> usize {
        self.num_rows
    }

    /// Bounding box, if available from accumulated statistics.
    pub fn bounding_box(&self) -> Option<&BoundingBox> {
        self.geo_statistics.bbox()
    }

    /// Consume this value and return only the spill files.
    pub fn into_spill_files(self) -> Vec<Arc<RefCountedTempFile>> {
        self.spill_files
    }

    /// Consume this value and return `(spill_files, geo_statistics, num_rows)`.
    pub fn into_inner(self) -> (Vec<Arc<RefCountedTempFile>>, GeoStatistics, usize) {
        (self.spill_files, self.geo_statistics, self.num_rows)
    }
}

impl SpilledPartitions {
    /// Construct a new spilled-partitions container for the provided slots.
    ///
    /// `partitions` must contain one entry for every slot in `slots`.
    pub fn new(slots: PartitionSlots, partitions: Vec<SpilledPartition>) -> Self {
        assert_eq!(partitions.len(), slots.total_slots());
        let partitions = partitions.into_iter().map(Some).collect();
        Self { slots, partitions }
    }

    /// Number of regular partitions
    pub fn num_regular_partitions(&self) -> usize {
        self.slots.num_regular_partitions()
    }

    /// Get the slots for mapping spatial partitions to sequential 0-based
    /// indexes
    pub fn slots(&self) -> PartitionSlots {
        self.slots
    }

    /// Retrieve the spilled partition for a given spatial partition.
    pub fn spilled_partition(&self, partition: SpatialPartition) -> Result<&SpilledPartition> {
        let Some(slot) = self.slots.slot(partition) else {
            return exec_err!(
                "Invalid partition {:?} for {} regular partitions",
                partition,
                self.slots.num_regular_partitions()
            );
        };
        match &self.partitions[slot] {
            Some(spilled_partition) => Ok(spilled_partition),
            None => {
                exec_err!(
                    "Spilled partition {:?} has already been taken away",
                    partition
                )
            }
        }
    }

    /// Consume this structure into concrete spilled partitions.
    pub fn into_spilled_partitions(self) -> Result<Vec<SpilledPartition>> {
        let mut partitions = Vec::with_capacity(self.partitions.len());
        for partition_opt in self.partitions {
            match partition_opt {
                Some(partition) => partitions.push(partition),
                None => {
                    return exec_err!(
                        "Some of the spilled partitions have already been taken away"
                    );
                }
            }
        }
        Ok(partitions)
    }

    /// Get a clone of the spill files in specified partition without consuming
    /// it. This is mainly for retrieving the Multi partition, which may be
    /// scanned multiple times.
    pub fn get_spilled_partition(&self, partition: SpatialPartition) -> Result<SpilledPartition> {
        let Some(slot) = self.slots.slot(partition) else {
            return exec_err!(
                "Invalid partition {:?} for {} regular partitions",
                partition,
                self.slots.num_regular_partitions()
            );
        };
        match &self.partitions[slot] {
            Some(spilled_partition) => Ok(spilled_partition.clone()),
            None => {
                exec_err!(
                    "Spilled partition {:?} has already been taken away",
                    partition
                )
            }
        }
    }

    /// Take the spill files in specified partition from it without consuming
    /// this value. This is mainly for retrieving the regular partitions,
    /// which will only be scanned once.
    pub fn take_spilled_partition(
        &mut self, partition: SpatialPartition,
    ) -> Result<SpilledPartition> {
        let Some(slot) = self.slots.slot(partition) else {
            return exec_err!(
                "Invalid partition {:?} for {} regular partitions",
                partition,
                self.slots.num_regular_partitions()
            );
        };
        match std::mem::take(&mut self.partitions[slot]) {
            Some(spilled_partition) => Ok(spilled_partition),
            None => {
                exec_err!(
                    "Spilled partition {:?} has already been taken away",
                    partition
                )
            }
        }
    }

    /// Are the spill files still present and can they be taken away?
    pub fn can_take_spilled_partition(&self, partition: SpatialPartition) -> bool {
        let Some(slot) = self.slots.slot(partition) else {
            return false;
        };
        self.partitions[slot].is_some()
    }

    /// Write debug info for this spilled partitions
    pub fn debug_print(&self, f: &mut impl std::fmt::Write) -> std::fmt::Result {
        for k in 0..self.slots.total_slots() {
            if let Some(spilled_partition) = &self.partitions[k] {
                let bbox_str = if let Some(bbox) = spilled_partition.bounding_box() {
                    format!(
                        "x: [{:.6}, {:.6}], y: [{:.6}, {:.6}]",
                        bbox.x().lo(),
                        bbox.x().hi(),
                        bbox.y().lo(),
                        bbox.y().hi()
                    )
                } else {
                    "None".to_owned()
                };
                let spill_files = spilled_partition.spill_files();
                let spill_file_sizes = spill_files
                    .iter()
                    .map(|sp| {
                        sp.inner()
                            .as_file()
                            .metadata()
                            .map(|m| m.len())
                            .unwrap_or(0)
                    })
                    .collect::<Vec<_>>();
                writeln!(
                    f,
                    "Partition {:?}: {} spill file(s), num non-empty geoms: {:?}, bbox: {}, spill file sizes: {:?}",
                    self.slots.partition(k),
                    spilled_partition.spill_files().len(),
                    spilled_partition
                        .geo_statistics()
                        .total_geometries()
                        .unwrap_or_default(),
                    bbox_str,
                    spill_file_sizes,
                )?;
            } else {
                writeln!(f, "Partition {k}: already taken away")?;
            }
        }
        Ok(())
    }

    /// Return debug info for this spilled partitions as a string.
    pub fn debug_str(&self) -> String {
        let mut output = String::new();
        let _ = self.debug_print(&mut output);
        output
    }
}

/// Incremental (stateful) repartitioner for an [`EvaluatedBatch`] stream.
///
/// This type assigns each incoming row to a [`SpatialPartition`] (based on a
/// [`SpatialPartitioner`]) and writes the partitioned output into spill files.
///
/// It buffers incoming data and keeps per-partition spill writers open across
/// batches to amortize setup cost. Flushing is controlled via
/// `buffer_bytes_threshold`, and output is optionally chunked to approximately
/// `target_batch_size` rows per partition batch.
pub struct StreamRepartitioner {
    runtime_env: Arc<RuntimeEnv>,
    partitioner: Box<dyn SpatialPartitioner>,
    partitioned_side: PartitionedSide,
    slots: PartitionSlots,
    /// Spill files for each spatial partition.
    /// The None and Multi partitions should be None when repartitioning the
    /// build side.
    spill_registry: Vec<Option<EvaluatedBatchSpillWriter>>,
    /// Geospatial statistics for each spatial partition.
    geo_stats_accumulators: Vec<AnalyzeAccumulator>,
    /// Number of rows in each spatial partition.
    num_rows: Vec<usize>,
    slot_assignments: Vec<Vec<(usize, usize)>>,
    row_assignments_buffer: Vec<SpatialPartition>,
    spill_compression: SpillCompression,
    spill_metrics: SpillMetrics,
    buffer_bytes_threshold: usize,
    target_batch_size: usize,
    spilled_batch_in_memory_size_threshold: Option<usize>,
    pending_batches: Vec<EvaluatedBatch>,
    pending_bytes: usize,
}

/// Builder for configuring and constructing a [`StreamRepartitioner`].
///
/// Defaults are chosen to be safe and explicit:
/// - `spill_compression`: [`SpillCompression::Uncompressed`]
/// - `buffer_bytes_threshold`: `0` (flush on every inserted batch)
/// - `target_batch_size`: `0` (do not chunk; emit one batch per partition
///   flush)
/// - `spilled_batch_in_memory_size_threshold`: `None`
pub struct StreamRepartitionerBuilder {
    runtime_env: Arc<RuntimeEnv>,
    partitioner: Box<dyn SpatialPartitioner>,
    partitioned_side: PartitionedSide,
    spill_compression: SpillCompression,
    spill_metrics: SpillMetrics,
    buffer_bytes_threshold: usize,
    target_batch_size: usize,
    spilled_batch_in_memory_size_threshold: Option<usize>,
}

impl StreamRepartitionerBuilder {
    /// Set spill compression applied to newly created per-partition spill
    /// files.
    pub fn spill_compression(mut self, spill_compression: SpillCompression) -> Self {
        self.spill_compression = spill_compression;
        self
    }

    /// Set the in-memory buffering threshold (in bytes).
    ///
    /// When the buffered in-memory size meets/exceeds this threshold, pending
    /// rows are flushed to partition writers.
    pub fn buffer_bytes_threshold(mut self, buffer_bytes_threshold: usize) -> Self {
        self.buffer_bytes_threshold = buffer_bytes_threshold;
        self
    }

    /// Set the target maximum number of rows per flushed batch (per partition).
    ///
    /// A value of `0` disables chunking.
    pub fn target_batch_size(mut self, target_batch_size: usize) -> Self {
        self.target_batch_size = target_batch_size;
        self
    }

    /// Set an optional threshold used by spill writers to decide whether to
    /// keep a batch in memory vs. spilling.
    pub fn spilled_batch_in_memory_size_threshold(
        mut self, spilled_batch_in_memory_size_threshold: Option<usize>,
    ) -> Self {
        self.spilled_batch_in_memory_size_threshold = spilled_batch_in_memory_size_threshold;
        self
    }

    /// Build a [`StreamRepartitioner`] with the configured parameters.
    pub fn build(self) -> StreamRepartitioner {
        let slots = PartitionSlots::new(self.partitioner.num_regular_partitions());
        let slot_count = slots.total_slots();
        StreamRepartitioner {
            runtime_env: self.runtime_env,
            partitioner: self.partitioner,
            partitioned_side: self.partitioned_side,
            slots,
            spill_registry: (0..slot_count).map(|_| None).collect(),
            geo_stats_accumulators: (0..slot_count).map(|_| AnalyzeAccumulator::new()).collect(),
            num_rows: vec![0; slot_count],
            slot_assignments: (0..slot_count).map(|_| Vec::new()).collect(),
            row_assignments_buffer: Vec::new(),
            spill_compression: self.spill_compression,
            spill_metrics: self.spill_metrics,
            buffer_bytes_threshold: self.buffer_bytes_threshold,
            target_batch_size: self.target_batch_size,
            spilled_batch_in_memory_size_threshold: self.spilled_batch_in_memory_size_threshold,
            pending_batches: Vec::new(),
            pending_bytes: 0,
        }
    }
}

impl StreamRepartitioner {
    /// Start building a new [`StreamRepartitioner`].
    ///
    /// This captures the required configuration (runtime, partitioner, side,
    /// and spill metrics). Optional parameters can then be set on the
    /// returned builder.
    pub fn builder(
        runtime_env: Arc<RuntimeEnv>, partitioner: Box<dyn SpatialPartitioner>,
        partitioned_side: PartitionedSide, spill_metrics: SpillMetrics,
    ) -> StreamRepartitionerBuilder {
        StreamRepartitionerBuilder {
            runtime_env,
            partitioner,
            partitioned_side,
            spill_compression: SpillCompression::Uncompressed,
            spill_metrics,
            buffer_bytes_threshold: 0,
            target_batch_size: 0,
            spilled_batch_in_memory_size_threshold: None,
        }
    }

    /// Repartition a stream of evaluated batches into per-partition spill
    /// files.
    ///
    /// This consumes the repartitioner and returns [`SpilledPartitions`] once
    /// the input stream is exhausted.
    pub async fn repartition_stream(
        mut self, mut stream: SendableEvaluatedBatchStream,
    ) -> Result<SpilledPartitions> {
        while let Some(batch_result) = stream.next().await {
            let batch = batch_result?;
            self.repartition_batch(batch)?;
        }
        self.finish()
    }

    /// Route a single evaluated batch into its corresponding spill writers.
    ///
    /// This runs the spatial partitioner to compute row assignments, buffers
    /// the batch, and may flush pending buffered data depending on
    /// configuration.
    pub fn repartition_batch(&mut self, batch: EvaluatedBatch) -> Result<()> {
        let mut row_assignments = std::mem::take(&mut self.row_assignments_buffer);
        assign_rows(
            &batch,
            self.partitioner.as_ref(),
            self.partitioned_side,
            &mut row_assignments,
        )?;
        self.insert_repartitioned_batch(batch, &row_assignments)?;
        self.row_assignments_buffer = row_assignments;
        Ok(())
    }

    /// Insert batch with row assignments into the repartitioner. The spatial
    /// partitioner does not need to be invoked in this method. This is
    /// useful when the batch has already been partitioned by calling
    /// `assign_rows`.
    ///
    /// `row_assignments` must have the same length as the batch row count and
    /// contain only partitions valid for the configured
    /// [`SpatialPartitioner`].
    pub fn insert_repartitioned_batch(
        &mut self, batch: EvaluatedBatch, row_assignments: &[SpatialPartition],
    ) -> Result<()> {
        let batch_idx = self.pending_batches.len();
        self.pending_bytes += batch.in_mem_size()?;
        self.pending_batches.push(batch);
        let batch_ref = &self.pending_batches[batch_idx];
        assert_eq!(row_assignments.len(), batch_ref.num_rows());
        for (row_idx, partition) in row_assignments.iter().enumerate() {
            let Some(slot_idx) = self.slots.slot(*partition) else {
                return exec_err!(
                    "Invalid partition {:?} for {} regular partitions",
                    partition,
                    self.slots.num_regular_partitions()
                );
            };
            if let Some(wkb) = batch_ref.wkb(row_idx) {
                self.geo_stats_accumulators[slot_idx].update_statistics(wkb)?;
            }
            self.slot_assignments[slot_idx].push((batch_idx, row_idx));
            self.num_rows[slot_idx] += 1;
        }
        let threshold = self.buffer_bytes_threshold;
        if threshold == 0 || self.pending_bytes >= threshold {
            self.flush_pending_batches()?;
        }
        Ok(())
    }

    fn flush_pending_batches(&mut self) -> Result<()> {
        if self.pending_batches.is_empty() {
            debug_assert!(
                self.slot_assignments
                    .iter()
                    .all(|assignments| assignments.is_empty())
            );
            return Ok(());
        }

        let pending_batches = std::mem::take(&mut self.pending_batches);
        self.pending_bytes = 0;

        let record_batches: Vec<&RecordBatch> =
            pending_batches.iter().map(|batch| &batch.batch).collect();
        let geom_arrays: Vec<&EvaluatedGeometryArray> = pending_batches
            .iter()
            .map(|batch| &batch.geom_array)
            .collect();

        let mut slot_assignments = std::mem::take(&mut self.slot_assignments);

        for (slot_idx, assignments) in slot_assignments.iter_mut().enumerate() {
            if assignments.is_empty() {
                continue;
            }
            let chunk_cap = if self.target_batch_size == 0 {
                assignments.len()
            } else {
                self.target_batch_size
            }
            .max(1);
            for chunk in assignments.chunks(chunk_cap) {
                let sliced_batch =
                    interleave_evaluated_batch(&record_batches, &geom_arrays, chunk)?;
                let writer = self.ensure_writer(slot_idx, &sliced_batch)?;
                writer.append(&sliced_batch)?;
            }

            assignments.clear();
        }

        self.slot_assignments = slot_assignments;
        Ok(())
    }

    /// Seal every partition and return their associated spill files and bounds.
    ///
    /// This flushes any buffered rows, closes all partition writers, and
    /// returns a [`SpilledPartitions`] summary.
    pub fn finish(mut self) -> Result<SpilledPartitions> {
        self.flush_pending_batches()?;
        let slot_count = self.slots.total_slots();
        let mut spilled_partition_vec = Vec::with_capacity(slot_count);
        for ((writer_opt, accumulator), num_rows) in self
            .spill_registry
            .into_iter()
            .zip(self.geo_stats_accumulators)
            .zip(self.num_rows)
        {
            let spilled_partition = if let Some(writer) = writer_opt {
                let spill_files = vec![Arc::new(writer.finish()?)];
                let geo_statistics = accumulator.finish();
                SpilledPartition::new(spill_files, geo_statistics, num_rows)
            } else {
                SpilledPartition::empty()
            };
            spilled_partition_vec.push(spilled_partition);
        }

        Ok(SpilledPartitions::new(self.slots, spilled_partition_vec))
    }

    fn ensure_writer(
        &mut self, slot_idx: usize, batch: &EvaluatedBatch,
    ) -> Result<&mut EvaluatedBatchSpillWriter> {
        if self.spill_registry[slot_idx].is_none() {
            self.spill_registry[slot_idx] = Some(EvaluatedBatchSpillWriter::try_new(
                self.runtime_env.clone(),
                batch.schema(),
                "streaming repartitioner",
                self.spill_compression,
                self.spill_metrics.clone(),
                self.spilled_batch_in_memory_size_threshold,
            )?);
        }
        Ok(self.spill_registry[slot_idx]
            .as_mut()
            .expect("writer inserted above"))
    }
}

/// Populate `assignments` with the spatial partition for every row in `batch`,
/// reusing the provided buffer to avoid repeated allocations. The vector length
/// after this call matches `batch.rects().len()` and each entry records which
/// [`SpatialPartition`] the corresponding row belongs to.
pub fn assign_rows(
    batch: &EvaluatedBatch, partitioner: &dyn SpatialPartitioner,
    partitioned_side: PartitionedSide, assignments: &mut Vec<SpatialPartition>,
) -> Result<()> {
    assignments.clear();
    assignments.reserve(batch.rects().len());

    match partitioned_side {
        PartitionedSide::BuildSide => {
            let mut cnt = 0;
            let num_regular_partitions = partitioner.num_regular_partitions() as u32;
            for rect_opt in batch.rects() {
                let partition = match rect_opt {
                    Some(rect) => partitioner.partition_no_multi(&geo_rect_to_bbox(rect))?,
                    None => {
                        // Round-robin empty geometries through regular partitions to avoid
                        // overloading a single slot when the build side is mostly empty.
                        let p = SpatialPartition::Regular(cnt);
                        cnt = (cnt + 1) % num_regular_partitions;
                        p
                    }
                };
                assignments.push(partition);
            }
        }
        PartitionedSide::ProbeSide => {
            for rect_opt in batch.rects() {
                let partition = match rect_opt {
                    Some(rect) => partitioner.partition(&geo_rect_to_bbox(rect))?,
                    None => SpatialPartition::None,
                };
                assignments.push(partition);
            }
        }
    }

    Ok(())
}

/// Build a new [`EvaluatedBatch`] by interleaving rows from the provided
/// `record_batches`/`geom_arrays` inputs according to `assignments`. Each pair
/// in `assignments` identifies the source batch index and row index that should
/// appear in the output in order, ensuring the geometry metadata stays aligned
/// with the Arrow row data.
pub fn interleave_evaluated_batch(
    record_batches: &[&RecordBatch], geom_arrays: &[&EvaluatedGeometryArray],
    indices: &[(usize, usize)],
) -> Result<EvaluatedBatch> {
    if record_batches.is_empty() || geom_arrays.is_empty() {
        return exec_err!("interleave_evaluated_batch requires at least one batch");
    }
    let batch = compute::interleave_record_batch(record_batches, indices)?;
    let geom_array = interleave_geometry_array(geom_arrays, indices)?;
    Ok(EvaluatedBatch { batch, geom_array })
}

fn interleave_geometry_array(
    geom_arrays: &[&EvaluatedGeometryArray], indices: &[(usize, usize)],
) -> Result<EvaluatedGeometryArray> {
    if geom_arrays.is_empty() {
        return exec_err!("interleave_geometry_array requires at least one batch");
    }
    let value_refs: Vec<&dyn Array> = geom_arrays
        .iter()
        .map(|geom| geom.geometry_array.as_ref())
        .collect();
    let geometry_array = compute::interleave(&value_refs, indices)?;

    let distance = interleave_distance_columns(geom_arrays, indices)?;

    let mut result = EvaluatedGeometryArray::new(geometry_array);
    result.distance = distance;
    Ok(result)
}

fn interleave_distance_columns(
    geom_arrays: &[&EvaluatedGeometryArray], assignments: &[(usize, usize)],
) -> Result<Option<ColumnarValue>> {
    // Check consistency and determine if we need array conversion
    let mut first_value: Option<&ColumnarValue> = None;
    let mut needs_array = false;
    let mut all_null = true;
    let mut first_scalar: Option<&ScalarValue> = None;

    for geom in geom_arrays {
        match &geom.distance {
            Some(value) => {
                if first_value.is_none() {
                    first_value = Some(value);
                }

                match value {
                    ColumnarValue::Array(array) => {
                        needs_array = true;
                        if all_null && array.logical_null_count() != array.len() {
                            all_null = false;
                        }
                    }
                    ColumnarValue::Scalar(scalar) => {
                        if let Some(first) = first_scalar {
                            if first != scalar {
                                needs_array = true;
                            }
                        } else {
                            first_scalar = Some(scalar);
                        }
                        if !scalar.is_null() {
                            all_null = false;
                        }
                    }
                }
            }
            None => {
                if first_value.is_some() && !all_null {
                    return exec_err!("Inconsistent distance metadata across batches");
                }
            }
        }
    }

    if all_null {
        return Ok(None);
    }

    let Some(distance_value) = first_value else {
        return Ok(None);
    };

    // If all scalars match, return scalar
    if !needs_array && let ColumnarValue::Scalar(value) = distance_value {
        return Ok(Some(ColumnarValue::Scalar(value.clone())));
    }

    // Convert to arrays and interleave
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(geom_arrays.len());
    for geom in geom_arrays {
        match &geom.distance {
            Some(ColumnarValue::Array(array)) => arrays.push(array.clone()),
            Some(ColumnarValue::Scalar(value)) => {
                arrays.push(value.to_array_of_size(geom.geometry_array.len())?);
            }
            None => {
                return exec_err!("Inconsistent distance metadata across batches");
            }
        }
    }

    let array_refs: Vec<&dyn Array> = arrays.iter().map(|array| array.as_ref()).collect();
    let array = compute::interleave(&array_refs, assignments)?;
    Ok(Some(ColumnarValue::Array(array)))
}

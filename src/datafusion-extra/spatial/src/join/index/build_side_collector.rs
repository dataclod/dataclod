use std::sync::Arc;

use datafusion::common::runtime::JoinSet;
use datafusion::common::{Result, exec_err};
use datafusion::config::SpillCompression;
use datafusion::error::DataFusionError;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::execution::memory_pool::MemoryReservation;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::physical_plan::metrics::{
    self, ExecutionPlanMetricsSet, MetricBuilder, SpillMetrics,
};
use futures::StreamExt;

use crate::geometry::analyze::{AnalyzeAccumulator, analyze_geometry};
use crate::join::evaluated_batch::EvaluatedBatch;
use crate::join::evaluated_batch::evaluate::create_evaluated_build_stream;
use crate::join::evaluated_batch::spill::EvaluatedBatchSpillWriter;
use crate::join::evaluated_batch::stream::SendableEvaluatedBatchStream;
use crate::join::evaluated_batch::stream::external::ExternalEvaluatedBatchStream;
use crate::join::evaluated_batch::stream::in_mem::InMemoryEvaluatedBatchStream;
use crate::join::index::spatial_index_builder::SpatialIndexBuilder;
use crate::join::operand_evaluator::{OperandEvaluator, create_operand_evaluator};
use crate::join::spatial_predicate::SpatialPredicate;
use crate::join::utils::bbox_sampler::{BoundingBoxSampler, BoundingBoxSamples};
use crate::option::SpatialJoinOptions;
use crate::statistics::GeoStatistics;

pub struct BuildPartition {
    pub num_rows: usize,
    pub build_side_batch_stream: SendableEvaluatedBatchStream,
    pub geo_statistics: GeoStatistics,

    /// Subset of build-side bounding boxes kept for building partitioners (e.g.
    /// KDB partitioner) when the indexed data cannot be fully loaded into
    /// memory.
    pub bbox_samples: BoundingBoxSamples,

    /// The estimated memory usage of building spatial index from all the data
    /// collected in this partition. The estimated memory used by the global
    /// spatial index will be the sum of these per-partition estimation.
    pub estimated_spatial_index_memory_usage: usize,

    /// Memory reservation for tracking the maximum memory usage when collecting
    /// the build side. This reservation won't be freed even when spilling is
    /// triggered. We deliberately only grow the memory reservation to probe
    /// the amount of memory available for loading spatial index into memory.
    /// The size of this reservation will be used to determine the maximum size
    /// of each spatial partition, as well as how many spatial partitions to
    /// create.
    pub reservation: MemoryReservation,

    /// Metrics collected during the build side collection phase
    pub metrics: CollectBuildSideMetrics,
}

/// A collector for evaluating the spatial expression on build side batches and
/// collect them as asynchronous streams with additional statistics. The
/// asynchronous streams could then be fed into the spatial index builder to
/// build an in-memory or external spatial index, depending on the statistics
/// collected by the collector.
#[derive(Clone)]
pub struct BuildSideBatchesCollector {
    spatial_predicate: SpatialPredicate,
    spatial_join_options: SpatialJoinOptions,
    evaluator: Arc<dyn OperandEvaluator>,
    runtime_env: Arc<RuntimeEnv>,
    spill_compression: SpillCompression,
}

#[derive(Clone)]
pub struct CollectBuildSideMetrics {
    /// Number of batches collected
    num_batches: metrics::Count,
    /// Number of rows collected
    num_rows: metrics::Count,
    /// Total in-memory size of batches collected. If the batches were spilled,
    /// this size is the in-memory size if we load all batches into memory.
    /// This does not represent the in-memory size of the resulting
    /// `BuildPartition`.
    total_size_bytes: metrics::Gauge,
    /// Total time taken to collect and process the build side batches. This
    /// does not include the time awaiting for batches from the input
    /// stream.
    time_taken: metrics::Time,
    /// Spill metrics of build partitions collecting phase
    spill_metrics: SpillMetrics,
}

impl CollectBuildSideMetrics {
    pub fn new(partition: usize, metrics: &ExecutionPlanMetricsSet) -> Self {
        Self {
            num_batches: MetricBuilder::new(metrics).counter("build_input_batches", partition),
            num_rows: MetricBuilder::new(metrics).counter("build_input_rows", partition),
            total_size_bytes: MetricBuilder::new(metrics)
                .gauge("build_input_total_size_bytes", partition),
            time_taken: MetricBuilder::new(metrics)
                .subset_time("build_input_collection_time", partition),
            spill_metrics: SpillMetrics::new(metrics, partition),
        }
    }

    pub fn spill_metrics(&self) -> SpillMetrics {
        self.spill_metrics.clone()
    }
}

impl BuildSideBatchesCollector {
    pub fn new(
        spatial_predicate: SpatialPredicate, spatial_join_options: SpatialJoinOptions,
        runtime_env: Arc<RuntimeEnv>, spill_compression: SpillCompression,
    ) -> Self {
        let evaluator = create_operand_evaluator(&spatial_predicate);
        BuildSideBatchesCollector {
            spatial_predicate,
            spatial_join_options,
            evaluator,
            runtime_env,
            spill_compression,
        }
    }

    /// Collect build-side batches from the stream into a `BuildPartition`.
    ///
    /// This method grows the given memory reservation as if an in-memory
    /// spatial index will be built for all collected batches. If the
    /// reservation cannot be grown, batches are spilled to disk and the
    /// reservation is left at its peak value.
    ///
    /// The reservation represents memory available for loading the spatial
    /// index. Across all partitions, the sum of their reservations forms a
    /// soft memory cap for subsequent spatial join operations. Reservations
    /// grown here are not released until the spatial join operator
    /// completes.
    pub async fn collect(
        &self, mut stream: SendableEvaluatedBatchStream, mut reservation: MemoryReservation,
        mut bbox_sampler: BoundingBoxSampler, metrics: CollectBuildSideMetrics,
    ) -> Result<BuildPartition> {
        let mut spill_writer_opt = None;
        let mut in_mem_batches: Vec<EvaluatedBatch> = Vec::new();
        let mut total_num_rows = 0;
        let mut total_size_bytes = 0;
        let mut analyzer = AnalyzeAccumulator::new();

        // Reserve memory for holding bbox samples. This should be a small reservation.
        // We simply return error if the reservation cannot be fulfilled, since there's
        // too little memory for the collector and proceeding will risk overshooting the
        // memory limit.
        reservation.try_grow(bbox_sampler.estimate_maximum_memory_usage())?;

        while let Some(evaluated_batch) = stream.next().await {
            let build_side_batch = evaluated_batch?;
            let _timer = metrics.time_taken.timer();

            let geom_array = &build_side_batch.geom_array;
            for wkb in geom_array.wkbs().iter().flatten() {
                let summary = analyze_geometry(wkb)
                    .map_err(|e| DataFusionError::External(e.into_boxed_dyn_error()))?;
                if !summary.bbox.is_empty() {
                    bbox_sampler.add_bbox(&summary.bbox);
                }
                analyzer.ingest_geometry_summary(&summary);
            }

            let num_rows = build_side_batch.num_rows();
            let in_mem_size = build_side_batch.in_mem_size()?;
            total_num_rows += num_rows;
            total_size_bytes += in_mem_size;

            metrics.num_batches.add(1);
            metrics.num_rows.add(num_rows);
            metrics.total_size_bytes.add(in_mem_size);

            match &mut spill_writer_opt {
                None => {
                    // Collected batches are in memory, no spilling happened for this partition
                    // before. We'll try storing this batch in memory first, and
                    // switch to writing everything to disk if we fail
                    // to grow the reservation.
                    in_mem_batches.push(build_side_batch);
                    if let Err(e) = reservation.try_grow(in_mem_size) {
                        tracing::debug!(
                            "Failed to grow reservation by {} bytes. Current reservation: {} bytes. \
                            num rows: {}, reason: {:?}, Spilling...",
                            in_mem_size,
                            reservation.size(),
                            num_rows,
                            e,
                        );
                        spill_writer_opt =
                            self.spill_in_mem_batches(&mut in_mem_batches, &metrics)?;
                    }
                }
                Some(spill_writer) => {
                    spill_writer.append(&build_side_batch)?;
                }
            }
        }

        let geo_statistics = analyzer.finish();
        let extra_mem = SpatialIndexBuilder::estimate_extra_memory_usage(
            &geo_statistics,
            &self.spatial_predicate,
            &self.spatial_join_options,
        );

        // Try to grow the reservation a bit more to account for any underestimation of
        // memory usage. We proceed even when the growth fails.
        let additional_reservation = extra_mem + (extra_mem + reservation.size()) / 5;
        if let Err(e) = reservation.try_grow(additional_reservation) {
            tracing::debug!(
                "Failed to grow reservation by {} bytes to account for spatial index building memory usage. \
                Current reservation: {} bytes. reason: {:?}",
                additional_reservation,
                reservation.size(),
                e,
            );
        }

        // If force spill is enabled, flush everything to disk regardless of whether the
        // memory is enough or not.
        if self.spatial_join_options.debug.force_spill && spill_writer_opt.is_none() {
            tracing::debug!(
                "Force spilling enabled. Spilling {} in-memory batches to disk.",
                in_mem_batches.len()
            );
            spill_writer_opt = self.spill_in_mem_batches(&mut in_mem_batches, &metrics)?;
        }

        let build_side_batch_stream: SendableEvaluatedBatchStream = match spill_writer_opt {
            Some(spill_writer) => {
                let spill_file = spill_writer.finish()?;
                if !in_mem_batches.is_empty() {
                    return exec_err!(
                        "In-memory batches should have been spilled when spill file exists"
                    );
                }
                Box::pin(ExternalEvaluatedBatchStream::try_from_spill_file(
                    Arc::new(spill_file),
                )?)
            }
            None => {
                let schema = stream.schema();
                Box::pin(InMemoryEvaluatedBatchStream::new(schema, in_mem_batches))
            }
        };

        let estimated_spatial_index_memory_usage = total_size_bytes + extra_mem;

        Ok(BuildPartition {
            num_rows: total_num_rows,
            build_side_batch_stream,
            geo_statistics,
            bbox_samples: bbox_sampler.into_samples(),
            estimated_spatial_index_memory_usage,
            reservation,
            metrics,
        })
    }

    pub async fn collect_all(
        &self, streams: Vec<SendableRecordBatchStream>, reservations: Vec<MemoryReservation>,
        metrics_vec: Vec<CollectBuildSideMetrics>, concurrent: bool, seed: u64,
    ) -> Result<Vec<BuildPartition>> {
        if streams.is_empty() {
            return Ok(Vec::new());
        }

        assert_eq!(
            streams.len(),
            reservations.len(),
            "each build stream must have a reservation"
        );
        assert_eq!(
            streams.len(),
            metrics_vec.len(),
            "each build stream must have a metrics collector"
        );

        if concurrent {
            self.collect_all_concurrently(streams, reservations, metrics_vec, seed)
                .await
        } else {
            self.collect_all_sequentially(streams, reservations, metrics_vec, seed)
                .await
        }
    }

    async fn collect_all_concurrently(
        &self, streams: Vec<SendableRecordBatchStream>, reservations: Vec<MemoryReservation>,
        metrics_vec: Vec<CollectBuildSideMetrics>, seed: u64,
    ) -> Result<Vec<BuildPartition>> {
        // Spawn task for each stream to scan all streams concurrently
        let mut join_set = JoinSet::new();
        for (partition_id, ((stream, metrics), reservation)) in streams
            .into_iter()
            .zip(metrics_vec)
            .zip(reservations)
            .enumerate()
        {
            let collector = self.clone();
            let evaluator = self.evaluator.clone();
            let bbox_sampler = BoundingBoxSampler::try_new(
                self.spatial_join_options.min_index_side_bbox_samples,
                self.spatial_join_options.max_index_side_bbox_samples,
                self.spatial_join_options
                    .target_index_side_bbox_sampling_rate,
                seed.wrapping_add(partition_id as u64),
            )?;
            join_set.spawn(async move {
                let evaluated_stream =
                    create_evaluated_build_stream(stream, evaluator, metrics.time_taken.clone());
                let result = collector
                    .collect(evaluated_stream, reservation, bbox_sampler, metrics)
                    .await;
                (partition_id, result)
            });
        }

        // Wait for all async tasks to finish. Results may be returned in arbitrary
        // order, so we need to reorder them by partition_id later.
        let results = join_set.join_all().await;

        // Reorder results according to partition ids
        let mut partitions: Vec<Option<BuildPartition>> = Vec::with_capacity(results.len());
        partitions.resize_with(results.len(), || None);
        for result in results {
            let (partition_id, partition_result) = result;
            let partition = partition_result?;
            partitions[partition_id] = Some(partition);
        }

        Ok(partitions.into_iter().map(|v| v.unwrap()).collect())
    }

    async fn collect_all_sequentially(
        &self, streams: Vec<SendableRecordBatchStream>, reservations: Vec<MemoryReservation>,
        metrics_vec: Vec<CollectBuildSideMetrics>, seed: u64,
    ) -> Result<Vec<BuildPartition>> {
        // Collect partitions sequentially (for JNI/embedded contexts)
        let mut results = Vec::with_capacity(streams.len());
        for (partition_id, ((stream, metrics), reservation)) in streams
            .into_iter()
            .zip(metrics_vec)
            .zip(reservations)
            .enumerate()
        {
            let evaluator = self.evaluator.clone();
            let bbox_sampler = BoundingBoxSampler::try_new(
                self.spatial_join_options.min_index_side_bbox_samples,
                self.spatial_join_options.max_index_side_bbox_samples,
                self.spatial_join_options
                    .target_index_side_bbox_sampling_rate,
                seed.wrapping_add(partition_id as u64),
            )?;

            let evaluated_stream =
                create_evaluated_build_stream(stream, evaluator, metrics.time_taken.clone());
            let result = self
                .collect(evaluated_stream, reservation, bbox_sampler, metrics)
                .await?;
            results.push(result);
        }
        Ok(results)
    }

    fn spill_in_mem_batches(
        &self, in_mem_batches: &mut Vec<EvaluatedBatch>, metrics: &CollectBuildSideMetrics,
    ) -> Result<Option<EvaluatedBatchSpillWriter>> {
        if in_mem_batches.is_empty() {
            return Ok(None);
        }

        let build_side_batch = &in_mem_batches[0];

        let schema = build_side_batch.schema();
        let mut spill_writer = EvaluatedBatchSpillWriter::try_new(
            self.runtime_env.clone(),
            schema,
            "spilling build side batches",
            self.spill_compression,
            metrics.spill_metrics.clone(),
            if self
                .spatial_join_options
                .spilled_batch_in_memory_size_threshold
                == 0
            {
                None
            } else {
                Some(
                    self.spatial_join_options
                        .spilled_batch_in_memory_size_threshold,
                )
            },
        )?;

        for in_mem_batch in in_mem_batches.iter() {
            spill_writer.append(in_mem_batch)?;
        }

        in_mem_batches.clear();
        Ok(Some(spill_writer))
    }
}

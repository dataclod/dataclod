use std::mem;
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::runtime::JoinSet;
use datafusion::common::{Result, exec_err};
use datafusion::execution::disk_manager::RefCountedTempFile;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::JoinType;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use fastrand::Rng;

use crate::geometry::bounding_box::BoundingBox;
use crate::join::index::build_side_collector::{
    BuildPartition, BuildSideBatchesCollector, CollectBuildSideMetrics,
};
use crate::join::index::memory_plan::{
    self, MemoryPlan, PartitionMemorySummary, compute_memory_plan,
};
use crate::join::index::partitioned_index_provider::PartitionedIndexProvider;
use crate::join::index::spatial_index_builder::SpatialJoinBuildMetrics;
use crate::join::partitioning::flat::FlatPartitioner;
use crate::join::partitioning::kdb::KDBPartitioner;
use crate::join::partitioning::rtree::RTreePartitioner;
use crate::join::partitioning::stream_repartitioner::{
    SpilledPartition, SpilledPartitions, StreamRepartitioner,
};
use crate::join::partitioning::{PartitionedSide, SpatialPartition, SpatialPartitioner};
use crate::join::probe::partitioned_stream_provider::ProbeStreamOptions;
use crate::join::spatial_predicate::SpatialPredicate;
use crate::join::utils::bbox_sampler::BoundingBoxSamples;
use crate::option::{DataClodOptions, NumSpatialPartitionsConfig};
use crate::statistics::GeoStatistics;

pub struct SpatialJoinComponents {
    pub partitioned_index_provider: Arc<PartitionedIndexProvider>,
    pub probe_stream_options: ProbeStreamOptions,
}

/// Builder for constructing `SpatialJoinComponents` from build-side streams.
///
/// Calling `build(...)` performs the full preparation flow:
/// - collect (and spill if needed) build-side batches,
/// - compute memory plan and pick single- or multi-partition mode,
/// - repartition the build side into spatial partitions in multi-partition
///   mode,
/// - create the appropriate `PartitionedIndexProvider` for creating spatial
///   indexes.
pub struct SpatialJoinComponentsBuilder {
    context: Arc<TaskContext>,
    build_schema: SchemaRef,
    spatial_predicate: SpatialPredicate,
    join_type: JoinType,
    probe_threads_count: usize,
    metrics: ExecutionPlanMetricsSet,
    seed: u64,
    dataclod_options: DataClodOptions,
}

impl SpatialJoinComponentsBuilder {
    /// The number of partitions above which the probe side uses an `RTree`
    /// partitioner instead of a flat (linear-scan) partitioner.  Benchmarks
    /// show the crossover at ~36 partitions; 48 gives a comfortable margin.
    const RTREE_PARTITION_THRESHOLD: usize = 48;

    /// Create a new builder capturing the execution context and configuration
    /// required to produce `SpatialJoinComponents` from build-side streams.
    pub fn new(
        context: Arc<TaskContext>, build_schema: SchemaRef, spatial_predicate: SpatialPredicate,
        join_type: JoinType, probe_threads_count: usize, metrics: ExecutionPlanMetricsSet,
        seed: u64,
    ) -> Self {
        let session_config = context.session_config();
        let dataclod_options = session_config
            .options()
            .extensions
            .get::<DataClodOptions>()
            .cloned()
            .unwrap_or_default();
        Self {
            context,
            build_schema,
            spatial_predicate,
            join_type,
            probe_threads_count,
            metrics,
            seed,
            dataclod_options,
        }
    }

    /// Prepare and return `SpatialJoinComponents` for the given build-side
    /// streams. This drives the end-to-end preparation flow and returns a
    /// ready-to-use `SpatialJoinComponents` for the spatial join operator.
    pub async fn build(
        mut self, build_streams: Vec<SendableRecordBatchStream>,
    ) -> Result<SpatialJoinComponents> {
        let num_partitions = build_streams.len();
        if num_partitions == 0 {
            tracing::debug!("Build side has no data. Creating empty spatial index.");
            return Ok(self.create_spatial_join_components_for_empty_build_side());
        }

        let mut rng = Rng::with_seed(self.seed);
        let mut build_partitions = self
            .collect_build_partitions(build_streams, rng.u64(0..0xFFFF))
            .await?;

        // Determine the number of spatial partitions based on the memory reserved and
        // the estimated amount of memory required for loading the entire build
        // side into a spatial index
        let memory_plan =
            compute_memory_plan(build_partitions.iter().map(PartitionMemorySummary::from))?;
        tracing::debug!("Computed memory plan for spatial join:\n{:#?}", memory_plan);
        let num_partitions = self.num_spatial_partitions(&memory_plan);

        if num_partitions == 1 {
            tracing::debug!("Running single-partitioned in-memory spatial join");
            Ok(self.create_single_partitioned_spatial_join_components(build_partitions))
        } else {
            // Collect all memory reservations grown during build side collection
            let mut reservations = Vec::with_capacity(build_partitions.len());
            for partition in &mut build_partitions {
                reservations.push(partition.reservation.take());
            }

            // Create the spatial partitioner for partitioning the build side. The actual
            // number of spatial partitions may be different from the requested
            // number of partitions due to the characteristics of the spatial
            // partitioner (e.g., KDB).
            let build_partitioner = self.create_spatial_partitioner_for_build_side(
                num_partitions,
                &mut build_partitions,
                rng.u64(0..0xFFFF),
            )?;
            let num_partitions = build_partitioner.num_regular_partitions();
            tracing::debug!("Actual number of spatial partitions: {}", num_partitions);

            // Partition the build side into multiple spatial partitions, each partition can
            // be fully loaded into an in-memory spatial index
            let partitioned_spill_files_vec = self
                .repartition_build_side(build_partitions, build_partitioner, &memory_plan)
                .await?;

            let merged_spilled_partitions = merge_spilled_partitions(partitioned_spill_files_vec)?;
            tracing::debug!(
                "Build side spatial partitions:\n{}",
                merged_spilled_partitions.debug_str()
            );

            // Sanity check: Multi and None partitions must be empty. All the geometries in
            // the build side should fall into regular partitions
            for partition in [SpatialPartition::None, SpatialPartition::Multi] {
                let spilled_partition = merged_spilled_partitions.spilled_partition(partition)?;
                if !spilled_partition.spill_files().is_empty() {
                    return exec_err!(
                        "Build side spatial partitions {:?} should be empty",
                        partition
                    );
                }
            }

            // Create the probe side partitioner matching the build side partitioner
            let probe_partitioner = self.create_spatial_partitioner_for_probe_side(
                num_partitions,
                &merged_spilled_partitions,
            )?;

            Ok(self.create_multi_partitioned_spatial_join_components(
                merged_spilled_partitions,
                probe_partitioner,
                reservations,
                &memory_plan,
            ))
        }
    }

    /// Collect build-side batches from the provided streams and return a
    /// vector of `BuildPartition` entries representing the collected data.
    /// The collector may spill to disk according to the configured options.
    async fn collect_build_partitions(
        &mut self, build_streams: Vec<SendableRecordBatchStream>, seed: u64,
    ) -> Result<Vec<BuildPartition>> {
        let runtime_env = self.context.runtime_env();
        let session_config = self.context.session_config();
        let spill_compression = session_config.spill_compression();

        let num_partitions = build_streams.len();
        let mut collect_metrics_vec = Vec::with_capacity(num_partitions);
        let mut reservations = Vec::with_capacity(num_partitions);
        let memory_pool = self.context.memory_pool();
        for k in 0..num_partitions {
            let consumer = MemoryConsumer::new(format!("SpatialJoinCollectBuildSide[{k}]"))
                .with_can_spill(true);
            let reservation = consumer.register(memory_pool);
            reservations.push(reservation);
            collect_metrics_vec.push(CollectBuildSideMetrics::new(k, &self.metrics));
        }

        let collector = BuildSideBatchesCollector::new(
            self.spatial_predicate.clone(),
            self.dataclod_options.spatial_join.clone(),
            runtime_env.clone(),
            spill_compression,
        );
        let build_partitions = collector
            .collect_all(
                build_streams,
                reservations,
                collect_metrics_vec.clone(),
                self.dataclod_options
                    .spatial_join
                    .concurrent_build_side_collection,
                seed,
            )
            .await?;

        Ok(build_partitions)
    }

    /// Construct a `SpatialPartitioner` (e.g. KDB) from collected samples so
    /// the build and probe sides can be partitioned spatially across
    /// `num_partitions`.
    fn create_spatial_partitioner_for_build_side(
        &self, num_partitions: usize, build_partitions: &mut Vec<BuildPartition>, seed: u64,
    ) -> Result<Box<dyn SpatialPartitioner>> {
        let build_partitioner: Box<dyn SpatialPartitioner> = {
            // Use spatial partitioners to partition the build side and the probe side, this
            // will reduce the amount of work needed for probing each
            // partitioned index. The KDB partitioner is built using the
            // collected bounding box samples.
            let mut bbox_samples = BoundingBoxSamples::empty();
            let mut geo_stats = GeoStatistics::empty();
            let mut rng = Rng::with_seed(seed);
            for partition in build_partitions {
                let samples = mem::take(&mut partition.bbox_samples);
                bbox_samples = bbox_samples.combine(samples, &mut rng);
                geo_stats.merge(&partition.geo_statistics);
            }

            let extent = geo_stats.bbox().cloned().unwrap_or(BoundingBox::empty());
            let mut samples = bbox_samples.take_samples();
            let max_items_per_node = 1.max(samples.len() / num_partitions);
            let max_levels = num_partitions;

            tracing::debug!(
                "Number of samples: {}, max_items_per_node: {}, max_levels: {}",
                samples.len(),
                max_items_per_node,
                max_levels
            );
            rng.shuffle(&mut samples);
            let kdb_partitioner =
                KDBPartitioner::build(samples.into_iter(), max_items_per_node, max_levels, extent)?;
            tracing::debug!(
                "Built KDB spatial partitioner with {} partitions",
                num_partitions
            );
            tracing::debug!(
                "KDB partitioner debug info:\n{}",
                kdb_partitioner.debug_str()
            );

            Box::new(kdb_partitioner)
        };

        Ok(build_partitioner)
    }

    /// Construct a `SpatialPartitioner` for partitioning the probe side.
    /// Uses a flat linear-scan partitioner when the number of partitions is
    /// small, and switches to an RTree-based partitioner for larger counts.
    fn create_spatial_partitioner_for_probe_side(
        &self, num_partitions: usize, merged_spilled_partitions: &SpilledPartitions,
    ) -> Result<Box<dyn SpatialPartitioner>> {
        let probe_partitioner: Box<dyn SpatialPartitioner> = {
            // Collect partition bounding boxes from the spilled partitions
            let mut partition_bounds = Vec::with_capacity(num_partitions);
            for k in 0..num_partitions {
                let partition = SpatialPartition::Regular(k as u32);
                let partition_bound = merged_spilled_partitions
                    .spilled_partition(partition)?
                    .bounding_box()
                    .cloned()
                    .unwrap_or(BoundingBox::empty());
                partition_bounds.push(partition_bound);
            }

            if num_partitions <= Self::RTREE_PARTITION_THRESHOLD {
                Box::new(FlatPartitioner::new(partition_bounds))
            } else {
                Box::new(RTreePartitioner::try_new(partition_bounds)?)
            }
        };
        Ok(probe_partitioner)
    }

    /// Repartition the collected build-side partitions using the provided
    /// `SpatialPartitioner`. Returns the spilled partitions for each spatial
    /// partition.
    async fn repartition_build_side(
        &self, build_partitions: Vec<BuildPartition>,
        build_partitioner: Box<dyn SpatialPartitioner>, memory_plan: &MemoryPlan,
    ) -> Result<Vec<SpilledPartitions>> {
        // Spawn each task for each build partition to repartition the data using the
        // spatial partitioner for the build/indexed side
        let runtime_env = self.context.runtime_env();
        let session_config = self.context.session_config();
        let target_batch_size = session_config.batch_size();
        let spill_compression = session_config.spill_compression();
        let spilled_batch_in_memory_size_threshold = self.spilled_batch_in_memory_size_threshold();
        let memory_for_intermittent_usage = self.memory_for_intermittent_usage(memory_plan);

        let mut join_set = JoinSet::new();
        let buffer_bytes_threshold = memory_for_intermittent_usage / build_partitions.len();
        for partition in build_partitions {
            let stream = partition.build_side_batch_stream;
            let metrics = &partition.metrics;
            let spill_metrics = metrics.spill_metrics();
            let runtime_env = runtime_env.clone();
            let partitioner = build_partitioner.box_clone();
            join_set.spawn(async move {
                StreamRepartitioner::builder(
                    runtime_env,
                    partitioner,
                    PartitionedSide::BuildSide,
                    spill_metrics,
                )
                .spill_compression(spill_compression)
                .buffer_bytes_threshold(buffer_bytes_threshold)
                .target_batch_size(target_batch_size)
                .spilled_batch_in_memory_size_threshold(spilled_batch_in_memory_size_threshold)
                .build()
                .repartition_stream(stream)
                .await
            });
        }

        let results = join_set.join_all().await;
        let partitioned_spill_files_vec = results.into_iter().collect::<Result<Vec<_>>>()?;
        Ok(partitioned_spill_files_vec)
    }

    fn create_spatial_join_components_for_empty_build_side(self) -> SpatialJoinComponents {
        let session_config = self.context.session_config();
        let target_batch_size = session_config.batch_size();
        let spilled_batch_in_memory_size_threshold = self.spilled_batch_in_memory_size_threshold();

        let partitioned_index_provider = PartitionedIndexProvider::new_empty(
            self.build_schema,
            self.spatial_predicate,
            self.dataclod_options.spatial_join,
            self.join_type,
            self.probe_threads_count,
            SpatialJoinBuildMetrics::new(0, &self.metrics),
        );

        let probe_stream_options = ProbeStreamOptions {
            partitioner: None,
            target_batch_rows: target_batch_size,
            spill_compression: session_config.spill_compression(),
            buffer_bytes_threshold: 0,
            spilled_batch_in_memory_size_threshold,
        };

        SpatialJoinComponents {
            partitioned_index_provider: Arc::new(partitioned_index_provider),
            probe_stream_options,
        }
    }

    fn create_single_partitioned_spatial_join_components(
        self, build_partitions: Vec<BuildPartition>,
    ) -> SpatialJoinComponents {
        let session_config = self.context.session_config();
        let target_batch_size = session_config.batch_size();
        let spilled_batch_in_memory_size_threshold = self.spilled_batch_in_memory_size_threshold();
        let spill_compression = session_config.spill_compression();

        let partitioned_index_provider = PartitionedIndexProvider::new_single_partition(
            self.build_schema,
            self.spatial_predicate,
            self.dataclod_options.spatial_join,
            self.join_type,
            self.probe_threads_count,
            build_partitions,
            SpatialJoinBuildMetrics::new(0, &self.metrics),
        );

        let probe_stream_options = ProbeStreamOptions {
            partitioner: None,
            target_batch_rows: target_batch_size,
            spill_compression,
            buffer_bytes_threshold: 0,
            spilled_batch_in_memory_size_threshold,
        };

        SpatialJoinComponents {
            partitioned_index_provider: Arc::new(partitioned_index_provider),
            probe_stream_options,
        }
    }

    fn create_multi_partitioned_spatial_join_components(
        self, merged_spilled_partitions: SpilledPartitions,
        probe_partitioner: Box<dyn SpatialPartitioner>, reservations: Vec<MemoryReservation>,
        memory_plan: &MemoryPlan,
    ) -> SpatialJoinComponents {
        let session_config = self.context.session_config();
        let target_batch_size = session_config.batch_size();
        let spilled_batch_in_memory_size_threshold = self.spilled_batch_in_memory_size_threshold();
        let spill_compression = session_config.spill_compression();
        let memory_for_intermittent_usage = self.memory_for_intermittent_usage(memory_plan);

        let partitioned_index_provider = PartitionedIndexProvider::new_multi_partition(
            self.build_schema,
            self.spatial_predicate,
            self.dataclod_options.spatial_join,
            self.join_type,
            self.probe_threads_count,
            merged_spilled_partitions,
            SpatialJoinBuildMetrics::new(0, &self.metrics),
            reservations,
        );

        let buffer_bytes_threshold = memory_for_intermittent_usage / self.probe_threads_count;
        let probe_stream_options = ProbeStreamOptions::new(
            Some(probe_partitioner),
            target_batch_size,
            spill_compression,
            buffer_bytes_threshold,
            spilled_batch_in_memory_size_threshold,
        );

        SpatialJoinComponents {
            partitioned_index_provider: Arc::new(partitioned_index_provider),
            probe_stream_options,
        }
    }

    fn num_spatial_partitions(&self, memory_plan: &MemoryPlan) -> usize {
        match self
            .dataclod_options
            .spatial_join
            .debug
            .num_spatial_partitions
        {
            NumSpatialPartitionsConfig::Auto => memory_plan.num_partitions,
            NumSpatialPartitionsConfig::Fixed(n) => {
                tracing::debug!("Override number of spatial partitions to {}", n);
                n
            }
        }
    }

    fn spilled_batch_in_memory_size_threshold(&self) -> Option<usize> {
        if self
            .dataclod_options
            .spatial_join
            .spilled_batch_in_memory_size_threshold
            == 0
        {
            None
        } else {
            Some(
                self.dataclod_options
                    .spatial_join
                    .spilled_batch_in_memory_size_threshold,
            )
        }
    }

    fn memory_for_intermittent_usage(&self, memory_plan: &MemoryPlan) -> usize {
        match self
            .dataclod_options
            .spatial_join
            .debug
            .memory_for_intermittent_usage
        {
            Some(value) => {
                tracing::debug!("Override memory for intermittent usage to {}", value);
                value
            }
            None => memory_plan.memory_for_intermittent_usage,
        }
    }
}

/// Aggregate the spill files and bounds of each spatial partition collected
/// from all build partitions
fn merge_spilled_partitions(
    spilled_partitions_vec: Vec<SpilledPartitions>,
) -> Result<SpilledPartitions> {
    let Some(first) = spilled_partitions_vec.first() else {
        return exec_err!("spilled_partitions_vec cannot be empty");
    };

    let slots = first.slots();
    let total_slots = slots.total_slots();
    let mut merged_spill_files: Vec<Vec<Arc<RefCountedTempFile>>> =
        (0..total_slots).map(|_| Vec::new()).collect();
    let mut partition_geo_stats: Vec<GeoStatistics> =
        (0..total_slots).map(|_| GeoStatistics::empty()).collect();
    let mut partition_num_rows: Vec<usize> = (0..total_slots).map(|_| 0).collect();

    for spilled_partitions in spilled_partitions_vec {
        let partitions = spilled_partitions.into_spilled_partitions()?;
        for (slot_idx, partition) in partitions.into_iter().enumerate() {
            let (spill_files, geo_stats, num_rows) = partition.into_inner();
            partition_geo_stats[slot_idx].merge(&geo_stats);
            merged_spill_files[slot_idx].extend(spill_files);
            partition_num_rows[slot_idx] += num_rows;
        }
    }

    let merged_partitions = merged_spill_files
        .into_iter()
        .zip(partition_geo_stats)
        .zip(partition_num_rows)
        .map(|((spill_files, geo_stats), num_rows)| {
            SpilledPartition::new(spill_files, geo_stats, num_rows)
        })
        .collect();

    Ok(SpilledPartitions::new(slots, merged_partitions))
}

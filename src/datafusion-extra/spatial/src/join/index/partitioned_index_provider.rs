use std::ops::DerefMut;
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::runtime::JoinSet;
use datafusion::common::{DataFusionError, Result, SharedResult, exec_err};
use datafusion::execution::memory_pool::MemoryReservation;
use datafusion::logical_expr::JoinType;
use futures::StreamExt;
use parking_lot::Mutex;
use tokio::sync::mpsc;

use crate::join::evaluated_batch::stream::external::ExternalEvaluatedBatchStream;
use crate::join::index::build_side_collector::BuildPartition;
use crate::join::index::spatial_index::SpatialIndex;
use crate::join::index::spatial_index_builder::{SpatialIndexBuilder, SpatialJoinBuildMetrics};
use crate::join::partitioning::SpatialPartition;
use crate::join::partitioning::stream_repartitioner::{SpilledPartition, SpilledPartitions};
use crate::join::spatial_predicate::SpatialPredicate;
use crate::join::utils::disposable_async_cell::DisposableAsyncCell;
use crate::option::SpatialJoinOptions;

pub struct PartitionedIndexProvider {
    schema: SchemaRef,
    spatial_predicate: SpatialPredicate,
    options: SpatialJoinOptions,
    join_type: JoinType,
    probe_threads_count: usize,
    metrics: SpatialJoinBuildMetrics,

    /// Data on the build side to build index for
    data: BuildSideData,

    /// Async cells for indexes, one per regular partition
    index_cells: Vec<DisposableAsyncCell<SharedResult<Arc<SpatialIndex>>>>,

    /// The memory reserved in the build side collection phase. We'll hold them
    /// until we don't need to build spatial indexes.
    _reservations: Vec<MemoryReservation>,
}

pub enum BuildSideData {
    SinglePartition(Mutex<Option<Vec<BuildPartition>>>),
    MultiPartition(Mutex<SpilledPartitions>),
}

impl PartitionedIndexProvider {
    #[allow(clippy::too_many_arguments)]
    pub fn new_multi_partition(
        schema: SchemaRef, spatial_predicate: SpatialPredicate, options: SpatialJoinOptions,
        join_type: JoinType, probe_threads_count: usize,
        partitioned_spill_files: SpilledPartitions, metrics: SpatialJoinBuildMetrics,
        reservations: Vec<MemoryReservation>,
    ) -> Self {
        let num_partitions = partitioned_spill_files.num_regular_partitions();
        let index_cells = (0..num_partitions)
            .map(|_| DisposableAsyncCell::new())
            .collect();
        Self {
            schema,
            spatial_predicate,
            options,
            join_type,
            probe_threads_count,
            metrics,
            data: BuildSideData::MultiPartition(Mutex::new(partitioned_spill_files)),
            index_cells,
            _reservations: reservations,
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new_single_partition(
        schema: SchemaRef, spatial_predicate: SpatialPredicate, options: SpatialJoinOptions,
        join_type: JoinType, probe_threads_count: usize, mut build_partitions: Vec<BuildPartition>,
        metrics: SpatialJoinBuildMetrics,
    ) -> Self {
        let reservations = build_partitions
            .iter_mut()
            .map(|p| p.reservation.take())
            .collect();
        let index_cells = vec![DisposableAsyncCell::new()];
        Self {
            schema,
            spatial_predicate,
            options,
            join_type,
            probe_threads_count,
            metrics,
            data: BuildSideData::SinglePartition(Mutex::new(Some(build_partitions))),
            index_cells,
            _reservations: reservations,
        }
    }

    pub fn new_empty(
        schema: SchemaRef, spatial_predicate: SpatialPredicate, options: SpatialJoinOptions,
        join_type: JoinType, probe_threads_count: usize, metrics: SpatialJoinBuildMetrics,
    ) -> Self {
        let build_partitions = Vec::new();
        Self::new_single_partition(
            schema,
            spatial_predicate,
            options,
            join_type,
            probe_threads_count,
            build_partitions,
            metrics,
        )
    }

    pub fn num_regular_partitions(&self) -> usize {
        self.index_cells.len()
    }

    pub async fn build_or_wait_for_index(
        &self, partition_id: u32,
    ) -> Option<Result<Arc<SpatialIndex>>> {
        let Some(cell) = self.index_cells.get(partition_id as usize) else {
            return Some(exec_err!(
                "partition_id {} exceeds {} partitions",
                partition_id,
                self.index_cells.len()
            ));
        };
        if !cell.is_empty() {
            return get_index_from_cell(cell).await;
        }

        let res_index = {
            let opt_res_index = self.maybe_build_index(partition_id).await;
            match opt_res_index {
                Some(res_index) => res_index,
                None => {
                    // The build side data for building the index has already been consumed by
                    // someone else, we just need to wait for the task consumed
                    // the data to finish building the index.
                    return get_index_from_cell(cell).await;
                }
            }
        };

        match res_index {
            Ok(idx) => {
                if let Err(e) = cell.set(Ok(idx.clone())) {
                    // This is probably because the cell has been disposed. No one
                    // will get the index from the cell so this failure is not a big deal.
                    tracing::debug!("Cannot set the index into the async cell: {:?}", e);
                }
                Some(Ok(idx))
            }
            Err(err) => {
                let err_arc = Arc::new(err);
                if let Err(e) = cell.set(Err(err_arc.clone())) {
                    tracing::debug!(
                        "Cannot set the index build error into the async cell: {:?}",
                        e
                    );
                }
                Some(Err(DataFusionError::Shared(err_arc)))
            }
        }
    }

    async fn maybe_build_index(&self, partition_id: u32) -> Option<Result<Arc<SpatialIndex>>> {
        match &self.data {
            BuildSideData::SinglePartition(build_partition_opt) => {
                if partition_id != 0 {
                    return Some(exec_err!(
                        "partition_id for single-partition index is not 0"
                    ));
                }

                // consume the build side data for building the index
                let build_partition_opt = {
                    let mut locked = build_partition_opt.lock();
                    std::mem::take(locked.deref_mut())
                };

                let Some(build_partition) = build_partition_opt else {
                    // already consumed by previous attempts, the result should be present in the
                    // channel.
                    return None;
                };
                Some(self.build_index_for_single_partition(build_partition).await)
            }
            BuildSideData::MultiPartition(partitioned_spill_files) => {
                // consume this partition of build side data for building index
                let spilled_partition = {
                    let mut locked = partitioned_spill_files.lock();
                    let partition = SpatialPartition::Regular(partition_id);
                    if !locked.can_take_spilled_partition(partition) {
                        // already consumed by previous attempts, the result should be present in
                        // the channel.
                        return None;
                    }
                    match locked.take_spilled_partition(partition) {
                        Ok(spilled_partition) => spilled_partition,
                        Err(e) => return Some(Err(e)),
                    }
                };
                Some(
                    self.build_index_for_spilled_partition(spilled_partition)
                        .await,
                )
            }
        }
    }

    pub async fn wait_for_index(&self, partition_id: u32) -> Option<Result<Arc<SpatialIndex>>> {
        let Some(cell) = self.index_cells.get(partition_id as usize) else {
            return Some(exec_err!(
                "partition_id {} exceeds {} partitions",
                partition_id,
                self.index_cells.len()
            ));
        };

        get_index_from_cell(cell).await
    }

    pub fn dispose_index(&self, partition_id: u32) {
        if let Some(cell) = self.index_cells.get(partition_id as usize) {
            cell.dispose();
        }
    }

    pub fn num_loaded_indexes(&self) -> usize {
        self.index_cells
            .iter()
            .filter(|index_cell| index_cell.is_set())
            .count()
    }

    async fn build_index_for_single_partition(
        &self, build_partitions: Vec<BuildPartition>,
    ) -> Result<Arc<SpatialIndex>> {
        let mut index_builder = SpatialIndexBuilder::new(
            self.schema.clone(),
            self.spatial_predicate.clone(),
            self.options.clone(),
            self.join_type,
            self.probe_threads_count,
            self.metrics.clone(),
        );

        for build_partition in build_partitions {
            let stream = build_partition.build_side_batch_stream;
            let geo_statistics = build_partition.geo_statistics;
            index_builder.add_stream(stream, geo_statistics).await?;
        }

        let index = index_builder.finish();
        Ok(Arc::new(index))
    }

    async fn build_index_for_spilled_partition(
        &self, spilled_partition: SpilledPartition,
    ) -> Result<Arc<SpatialIndex>> {
        let mut index_builder = SpatialIndexBuilder::new(
            self.schema.clone(),
            self.spatial_predicate.clone(),
            self.options.clone(),
            self.join_type,
            self.probe_threads_count,
            self.metrics.clone(),
        );

        // Spawn tasks to load indexed batches from spilled files concurrently
        let (spill_files, geo_statistics, _) = spilled_partition.into_inner();
        let mut join_set: JoinSet<Result<(), DataFusionError>> = JoinSet::new();
        let (tx, mut rx) = mpsc::channel(spill_files.len() * 2 + 1);
        for spill_file in spill_files {
            let tx = tx.clone();
            join_set.spawn(async move {
                let result = async {
                    let mut stream = ExternalEvaluatedBatchStream::try_from_spill_file(spill_file)?;
                    while let Some(batch) = stream.next().await {
                        let indexed_batch = batch?;
                        if tx.send(Ok(indexed_batch)).await.is_err() {
                            return Ok(());
                        }
                    }
                    Ok::<(), DataFusionError>(())
                }
                .await;
                if let Err(e) = result {
                    let _ = tx.send(Err(e)).await;
                }
                Ok(())
            });
        }
        drop(tx);

        // Collect the loaded indexed batches and add them to the index builder
        while let Some(res) = rx.recv().await {
            let batch = res?;
            index_builder.add_batch(batch)?;
        }

        // Ensure all tasks completed successfully
        while let Some(res) = join_set.join_next().await {
            if let Err(e) = res {
                if e.is_panic() {
                    std::panic::resume_unwind(e.into_panic());
                }
                return Err(DataFusionError::External(Box::new(e)));
            }
        }

        index_builder.merge_stats(geo_statistics);

        let index = index_builder.finish();
        Ok(Arc::new(index))
    }
}

async fn get_index_from_cell(
    cell: &DisposableAsyncCell<SharedResult<Arc<SpatialIndex>>>,
) -> Option<Result<Arc<SpatialIndex>>> {
    match cell.get().await {
        Some(Ok(index)) => Some(Ok(index)),
        Some(Err(shared_err)) => Some(Err(DataFusionError::Shared(shared_err))),
        None => None,
    }
}

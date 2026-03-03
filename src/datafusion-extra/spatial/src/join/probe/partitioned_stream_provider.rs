use std::ops::DerefMut;
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{DataFusionError, Result, exec_err};
use datafusion::config::SpillCompression;
use datafusion::execution::runtime_env::RuntimeEnv;
use parking_lot::Mutex;

use crate::join::evaluated_batch::stream::SendableEvaluatedBatchStream;
use crate::join::evaluated_batch::stream::external::ExternalEvaluatedBatchStream;
use crate::join::partitioning::stream_repartitioner::{SpilledPartitions, StreamRepartitioner};
use crate::join::partitioning::{PartitionedSide, SpatialPartition, SpatialPartitioner};
use crate::join::probe::ProbeStreamMetrics;
use crate::join::probe::first_pass_stream::FirstPassStream;
use crate::join::probe::non_partitioned_stream::NonPartitionedStream;

/// Configuration options for creating a probe-side stream provider.
///
/// When a `partitioner` is provided, the provider performs an initial first
/// pass that repartitions and spills the probe-side input into per-partition
/// spill files. Subsequent calls can then open a stream for a specific
/// [`SpatialPartition`].
pub struct ProbeStreamOptions {
    /// Optional spatial partitioner.
    ///
    /// - `None` means the probe side is treated as a single, non-partitioned
    ///   stream and only [`SpatialPartition::Regular(0)`] is supported.
    /// - `Some(_)` enables partitioned streaming with a warm-up (first) pass.
    ///
    /// The `Mutex` is used here to make [`ProbeStreamOptions`] (and its
    /// contained options) `Send + Sync` so it can be shared/cloned into
    /// `SpatialJoinExec` and across tasks. The partitioner itself is
    /// treated as a cloneable prototype and is not intended to be
    /// used by multiple tasks concurrently via this shared `Mutex`.
    pub partitioner: Option<Mutex<Box<dyn SpatialPartitioner>>>,
    /// Target number of rows per output batch produced by the partitioning
    /// stream.
    pub target_batch_rows: usize,
    /// Spill compression to use when writing partition spill files.
    pub spill_compression: SpillCompression,
    /// Threshold (in bytes) before buffered repartitioned data is spilled.
    pub buffer_bytes_threshold: usize,
    /// Optional upper bound for the size of spilled batches. Large spilled
    /// batches will be split into smaller ones to avoid excessive memory
    /// usage during re-reading.
    pub spilled_batch_in_memory_size_threshold: Option<usize>,
}

impl ProbeStreamOptions {
    pub fn new(
        partitioner: Option<Box<dyn SpatialPartitioner>>, target_batch_rows: usize,
        spill_compression: SpillCompression, buffer_bytes_threshold: usize,
        spilled_batch_in_memory_size_threshold: Option<usize>,
    ) -> Self {
        let partitioner = partitioner.map(Mutex::new);
        Self {
            partitioner,
            target_batch_rows,
            spill_compression,
            buffer_bytes_threshold,
            spilled_batch_in_memory_size_threshold,
        }
    }
}

impl Clone for ProbeStreamOptions {
    fn clone(&self) -> Self {
        let cloned_partitioner = self
            .partitioner
            .as_ref()
            .map(|p| Mutex::new(p.lock().box_clone()));
        Self {
            partitioner: cloned_partitioner,
            target_batch_rows: self.target_batch_rows,
            spill_compression: self.spill_compression,
            buffer_bytes_threshold: self.buffer_bytes_threshold,
            spilled_batch_in_memory_size_threshold: self.spilled_batch_in_memory_size_threshold,
        }
    }
}

/// Provides probe-side streams for a given [`SpatialPartition`].
///
/// For partitioned joins this provider is a small state machine:
/// it first runs the first pass to materialize per-partition spill files, then
/// serves per-partition streams from those spill files.
pub struct PartitionedProbeStreamProvider {
    state: Arc<Mutex<ProbeStreamState>>,
    runtime_env: Arc<RuntimeEnv>,
    options: ProbeStreamOptions,
    schema: SchemaRef,
    metrics: ProbeStreamMetrics,
}

enum ProbeStreamState {
    /// Initial state: we still own the original source stream and have not
    /// started consuming it.
    Pending {
        source: SendableEvaluatedBatchStream,
    },
    /// First pass is currently running.
    ///
    /// The provider is consuming the source stream and repartitioning/spilling
    /// it into per-partition spill files.
    FirstPass,
    /// First pass completed successfully and per-partition spill files are
    /// available.
    SubsequentPass { manifest: ProbePartitionManifest },
    /// Non-partitioned mode: the single probe stream has been consumed and
    /// cannot be replayed.
    NonPartitionedConsumed,
    /// Terminal failure state.
    ///
    /// Any later interaction with the provider will surface this error.
    Failed(Arc<DataFusionError>),
}

impl PartitionedProbeStreamProvider {
    /// Create a new provider from a probe-side evaluated batch stream.
    pub fn new(
        runtime_env: Arc<RuntimeEnv>, options: ProbeStreamOptions,
        source: SendableEvaluatedBatchStream, metrics: ProbeStreamMetrics,
    ) -> Self {
        let schema = source.schema();
        Self {
            state: Arc::new(Mutex::new(ProbeStreamState::Pending { source })),
            runtime_env,
            options,
            schema,
            metrics,
        }
    }

    /// Return a probe-side stream for the requested [`SpatialPartition`].
    ///
    /// - In non-partitioned mode (`options.partitioner == None`), only
    ///   [`SpatialPartition::Regular(0)`] is supported.
    /// - In partitioned mode, `Regular(0)` triggers the warm-up (first) pass;
    ///   all other partitions are served from the spill manifest created by the
    ///   first pass.
    pub fn stream_for(&self, partition: SpatialPartition) -> Result<SendableEvaluatedBatchStream> {
        match partition {
            SpatialPartition::None => {
                exec_err!("SpatialPartition::None should be handled via outer join logic")
            }
            SpatialPartition::Regular(0) => self.first_pass_stream(),
            SpatialPartition::Regular(_) | SpatialPartition::Multi => {
                if self.options.partitioner.is_none() {
                    exec_err!("Non-partitioned probe stream only supports Regular(0)")
                } else {
                    self.subsequent_pass_stream(partition)
                }
            }
        }
    }

    fn first_pass_stream(&self) -> Result<SendableEvaluatedBatchStream> {
        if self.options.partitioner.is_none() {
            return self.non_partitioned_first_pass_stream();
        }

        let schema = self.schema.clone();
        let mut state_guard = self.state.lock();
        match std::mem::replace(&mut *state_guard, ProbeStreamState::FirstPass) {
            ProbeStreamState::Pending { source } => {
                let partitioner_for_stream = self
                    .options
                    .partitioner
                    .as_ref()
                    .expect("Partitioned first pass requires a partitioner")
                    .lock()
                    .box_clone();
                let partitioner_for_repartitioner = partitioner_for_stream.box_clone();
                let repartitioner = StreamRepartitioner::builder(
                    self.runtime_env.clone(),
                    partitioner_for_repartitioner,
                    PartitionedSide::ProbeSide,
                    self.metrics.spill_metrics.clone(),
                )
                .spill_compression(self.options.spill_compression)
                .buffer_bytes_threshold(self.options.buffer_bytes_threshold)
                .target_batch_size(self.options.target_batch_rows)
                .spilled_batch_in_memory_size_threshold(
                    self.options.spilled_batch_in_memory_size_threshold,
                )
                .build();

                let state = self.state.clone();
                let callback = move |res: Result<SpilledPartitions>| {
                    let mut guard = state.lock();
                    *guard = match res {
                        Ok(mut spills) => {
                            let mut s = String::new();
                            if spills.debug_print(&mut s).is_ok() {
                                tracing::debug!("Probe side spilled partitions:\n{}", s);
                            }

                            // Sanity check: Regular(0) and None should be empty
                            let mut check_empty = |partition: SpatialPartition| -> Result<()> {
                                let spilled = spills.take_spilled_partition(partition)?;
                                if !spilled.into_spill_files().is_empty() {
                                    return exec_err!(
                                        "{:?} partition should not have spilled data",
                                        partition
                                    );
                                }
                                Ok(())
                            };

                            match check_empty(SpatialPartition::Regular(0))
                                .and_then(|_| check_empty(SpatialPartition::None))
                            {
                                Ok(_) => {
                                    ProbeStreamState::SubsequentPass {
                                        manifest: ProbePartitionManifest::new(schema, spills),
                                    }
                                }
                                Err(err) => ProbeStreamState::Failed(Arc::new(err)),
                            }
                        }
                        Err(err) => {
                            let err_arc = Arc::new(err);
                            ProbeStreamState::Failed(err_arc)
                        }
                    };
                    Ok(())
                };

                let first_pass = FirstPassStream::new(
                    source,
                    repartitioner,
                    partitioner_for_stream,
                    self.metrics.clone(),
                    callback,
                );
                Ok(Box::pin(first_pass))
            }
            ProbeStreamState::FirstPass => {
                exec_err!("First pass already running for partitioned probe stream")
            }
            ProbeStreamState::SubsequentPass { .. } => {
                exec_err!("First pass already completed")
            }
            ProbeStreamState::NonPartitionedConsumed => {
                exec_err!("Non-partitioned probe stream already consumed")
            }
            ProbeStreamState::Failed(err) => Err(DataFusionError::Shared(err)),
        }
    }

    fn non_partitioned_first_pass_stream(&self) -> Result<SendableEvaluatedBatchStream> {
        let mut state_guard = self.state.lock();
        match std::mem::replace(&mut *state_guard, ProbeStreamState::NonPartitionedConsumed) {
            ProbeStreamState::Pending { source } => {
                Ok(Box::pin(NonPartitionedStream::new(
                    source,
                    self.metrics.clone(),
                )))
            }
            ProbeStreamState::NonPartitionedConsumed => {
                exec_err!("Non-partitioned probe stream already consumed")
            }
            ProbeStreamState::Failed(err) => Err(DataFusionError::Shared(err)),
            _ => exec_err!("Non-partitioned probe stream is not available"),
        }
    }

    fn subsequent_pass_stream(
        &self, partition: SpatialPartition,
    ) -> Result<SendableEvaluatedBatchStream> {
        if self.options.partitioner.is_none() {
            return exec_err!("Non-partitioned probe stream cannot serve additional partitions");
        }
        let mut locked = self.state.lock();
        let manifest = match locked.deref_mut() {
            ProbeStreamState::SubsequentPass { manifest } => manifest,
            ProbeStreamState::Failed(err) => return Err(DataFusionError::Shared(err.clone())),
            _ => return exec_err!("Partitioned probe stream first pass not finished"),
        };

        {
            // let mut manifest = manifest.lock();
            manifest.stream_for(partition)
        }
    }

    /// Return the number of rows available for `partition`.
    ///
    /// This is only available after the first pass has completed.
    pub fn get_partition_row_count(&self, partition: SpatialPartition) -> Result<usize> {
        let mut locked = self.state.lock();
        let manifest = match locked.deref_mut() {
            ProbeStreamState::SubsequentPass { manifest } => manifest,
            ProbeStreamState::Failed(err) => return Err(DataFusionError::Shared(err.clone())),
            _ => return exec_err!("Partitioned probe stream first pass not finished"),
        };
        manifest.get_partition_row_count(partition)
    }
}

/// Spill manifest produced by the probe-side first pass.
///
/// Stores (and hands out) per-partition spill files that can be replayed as
/// [`SendableEvaluatedBatchStream`]s.
struct ProbePartitionManifest {
    schema: SchemaRef,
    slots: SpilledPartitions,
}

impl ProbePartitionManifest {
    fn new(schema: SchemaRef, spills: SpilledPartitions) -> Self {
        Self {
            schema,
            slots: spills,
        }
    }

    fn get_partition_row_count(&self, partition: SpatialPartition) -> Result<usize> {
        let spilled = self.slots.get_spilled_partition(partition)?;
        Ok(spilled.num_rows())
    }

    fn stream_for(&mut self, partition: SpatialPartition) -> Result<SendableEvaluatedBatchStream> {
        match partition {
            SpatialPartition::Regular(0) => {
                exec_err!("Partition 0 is only available during the first pass")
            }
            SpatialPartition::None => {
                exec_err!("Should not request a probe stream for SpatialPartition::None")
            }
            SpatialPartition::Regular(_) => {
                let spilled = self.slots.take_spilled_partition(partition)?;
                Ok(Box::pin(
                    ExternalEvaluatedBatchStream::try_from_spill_files(
                        self.schema.clone(),
                        spilled.into_spill_files(),
                    )?,
                ))
            }
            SpatialPartition::Multi => {
                let spilled = self.slots.get_spilled_partition(partition)?;
                Ok(Box::pin(
                    ExternalEvaluatedBatchStream::try_from_spill_files(
                        self.schema.clone(),
                        spilled.into_spill_files(),
                    )?,
                ))
            }
        }
    }
}

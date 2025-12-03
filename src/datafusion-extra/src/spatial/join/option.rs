/// Configuration options for spatial join execution.
///
/// This struct controls various aspects of how spatial joins are performed,
/// including prepared geometry usage and output batch sizing.
#[derive(Debug, Clone)]
pub struct SpatialJoinOptions {
    /// The execution mode determining how prepared geometries are used
    pub prepare_execution_mode: PrepareExecutionMode,
    /// Maximum number of rows in each output batch from spatial join
    /// processing. When the accumulated join results reach this size, a
    /// batch is emitted.
    ///
    /// Larger batch sizes can improve throughput but increase memory usage.
    /// Smaller batch sizes reduce memory pressure but may decrease performance.
    ///
    /// Default is 8192 rows per batch.
    pub max_batch_size: usize,
}

impl Default for SpatialJoinOptions {
    fn default() -> Self {
        Self {
            prepare_execution_mode: PrepareExecutionMode::None,
            max_batch_size: 8192,
        }
    }
}

/// Execution mode for spatial join operations, controlling prepared geometry
/// usage.
///
/// Prepared geometries are pre-processed spatial objects that can significantly
/// improve performance for spatial predicate evaluation when the same geometry
/// is used multiple times in comparisons.
///
/// The choice of execution mode depends on the specific characteristics of your
/// spatial join workload, as well as the spatial relation predicate between the
/// two tables. Some of the spatial relation computations cannot be accelerated
/// by prepared geometries at all (for example, ST_Touches, ST_Crosses,
/// ST_DWithin).
#[derive(Debug, Clone)]
pub enum PrepareExecutionMode {
    /// Create prepared geometries for the build side (left/smaller table).
    Build,

    /// Create prepared geometries for the probe side (right/larger table).
    Probe,

    /// Don't use prepared geometries for spatial predicate evaluation.
    None,
}

/// Default minimum number of analyzed geometries for speculative execution mode
/// to select an optimal execution mode.
pub const DEFAULT_SPECULATIVE_THRESHOLD: usize = 1024;

/// Default minimum number of points per geometry to use prepared geometries for
/// the build side.
pub const DEFAULT_MIN_POINTS_FOR_BUILD_PREPARATION: usize = 64;

/// Configuration options for spatial join.
///
/// This struct controls various aspects of how spatial joins are performed,
/// including prepared geometry usage.
#[derive(Debug, Clone)]
pub struct SpatialJoinOptions {
    /// The execution mode determining how prepared geometries are used
    pub execution_mode: ExecutionMode,
    /// The minimum number of points per geometry to use prepared geometries for
    /// the build side.
    pub min_points_for_build_preparation: usize,
}

impl Default for SpatialJoinOptions {
    fn default() -> Self {
        Self {
            execution_mode: ExecutionMode::Speculative(DEFAULT_SPECULATIVE_THRESHOLD),
            min_points_for_build_preparation: DEFAULT_MIN_POINTS_FOR_BUILD_PREPARATION,
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
#[derive(Debug, Clone, PartialEq, Copy)]
#[allow(dead_code)]
pub enum ExecutionMode {
    /// Don't use prepared geometries for spatial predicate evaluation.
    PrepareNone,

    /// Create prepared geometries for the build side (left/smaller table).
    PrepareBuild,

    /// Create prepared geometries for the probe side (right/larger table).
    PrepareProbe,

    /// Automatically choose the best execution mode based on the
    /// characteristics of first few geometries on the probe side.
    Speculative(usize),
}

impl ExecutionMode {
    /// Convert the execution mode to a usize value.
    ///
    /// This is used to show the execution mode in the metrics. We use a gauge
    /// value to represent the execution mode.
    pub fn as_gauge(&self) -> usize {
        match self {
            ExecutionMode::PrepareNone => 0,
            ExecutionMode::PrepareBuild => 1,
            ExecutionMode::PrepareProbe => 2,
            ExecutionMode::Speculative(_) => 3,
        }
    }
}

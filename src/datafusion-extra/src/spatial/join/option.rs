/// Configuration options for spatial join.
///
/// This struct controls various aspects of how spatial joins are performed,
/// including prepared geometry usage.
#[derive(Debug, Clone)]
pub struct SpatialJoinOptions {
    /// The execution mode determining how prepared geometries are used
    pub execution_mode: ExecutionMode,
}

impl Default for SpatialJoinOptions {
    fn default() -> Self {
        Self {
            execution_mode: ExecutionMode::PrepareNone,
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
#[derive(Debug, Clone, PartialEq)]
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

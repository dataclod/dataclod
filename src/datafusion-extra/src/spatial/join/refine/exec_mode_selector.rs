use std::sync::{Arc, OnceLock};

use parking_lot::Mutex;

use crate::spatial::join::option::ExecutionMode;
use crate::spatial::statistics::GeoStatistics;

/// Trait for selecting the optimal execution mode based on build and probe
/// statistics. This allows for plugging in different selection strategies that
/// can consider various factors such as spatial predicate type, geometry
/// complexity, etc.
pub trait SelectOptimalMode: Send + Sync {
    /// Select the optimal execution mode based on build and probe statistics.
    ///
    /// # Arguments
    /// * `build_stats` - Statistics from the build side geometries
    /// * `probe_stats` - Statistics from the probe side geometries
    ///
    /// # Returns
    /// * `ExecutionMode` - The optimal execution mode to use
    fn select(&self, build_stats: &GeoStatistics, probe_stats: &GeoStatistics) -> ExecutionMode;

    /// Select the optimal execution mode based on build statistics.
    ///
    /// # Arguments
    /// * `build_stats` - Statistics from the build side geometries
    ///
    /// # Returns
    /// * `Option<ExecutionMode>` - The optimal execution mode to use, or `None`
    ///   if the execution mode cannot be determined without probe statistics.
    fn select_without_probe_stats(&self, build_stats: &GeoStatistics) -> Option<ExecutionMode>;
}

/// Select the optimal execution mode for the refinement phase of spatial join
/// using the build-side and partial probe-side statistics.
pub struct ExecModeSelector {
    /// The build-side statistics.
    build_stats: GeoStatistics,
    /// The partial probe-side statistics.
    probe_stats: Mutex<GeoStatistics>,
    /// The minimum number of probe-side geometry to analyze before selecting
    /// the execution mode.
    min_required_count: usize,
    /// The optimal execution mode selected.
    optimal_mode: OnceLock<ExecutionMode>,
    /// The strategy for selecting the optimal execution mode.
    selector: Arc<dyn SelectOptimalMode>,
}

impl ExecModeSelector {
    pub fn new(
        build_stats: GeoStatistics, min_required_count: usize, selector: Arc<dyn SelectOptimalMode>,
    ) -> Self {
        Self {
            build_stats,
            probe_stats: Mutex::new(GeoStatistics::empty()),
            min_required_count,
            optimal_mode: OnceLock::new(),
            selector,
        }
    }

    pub fn merge_probe_stats(&self, stats: GeoStatistics) {
        let mut probe_stats = self.probe_stats.lock();
        probe_stats.merge(&stats);
        let analyzed_count = probe_stats.total_geometries().unwrap_or(0) as usize;
        if analyzed_count >= self.min_required_count {
            self.optimal_mode
                .get_or_init(|| self.select_optimal_mode(&probe_stats));
        }
    }

    pub fn optimal_mode(&self) -> Option<ExecutionMode> {
        self.optimal_mode.get().copied()
    }

    fn select_optimal_mode(&self, probe_stats: &GeoStatistics) -> ExecutionMode {
        self.selector.select(&self.build_stats, probe_stats)
    }
}

/// Get the current execution mode or update it with the optimal mode if it is
/// not set. If optimal mode is not selected yet, return the default execution
/// mode without updating the execution mode.
pub fn get_or_update_execution_mode(
    exec_mode: &OnceLock<ExecutionMode>, exec_mode_selector: &Option<ExecModeSelector>,
    default_exec_mode: ExecutionMode,
) -> ExecutionMode {
    if let Some(mode) = exec_mode.get() {
        *mode
    } else if let Some(selector) = exec_mode_selector {
        if let Some(mode) = selector.optimal_mode() {
            *exec_mode.get_or_init(|| mode)
        } else {
            default_exec_mode
        }
    } else {
        default_exec_mode
    }
}

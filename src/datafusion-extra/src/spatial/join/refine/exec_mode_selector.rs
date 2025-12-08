use std::sync::OnceLock;

use parking_lot::Mutex;

use crate::spatial::join::option::ExecutionMode;
use crate::spatial::statistics::GeoStatistics;

/// Select the optimal execution mode for the refinement phase of spatial join
/// using the build-side and partial probe-side statistics.
pub(crate) struct ExecModeSelector<F: Fn(&GeoStatistics, &GeoStatistics) -> ExecutionMode> {
    /// The build-side statistics.
    build_stats: GeoStatistics,
    /// The partial probe-side statistics.
    probe_stats: Mutex<GeoStatistics>,
    /// The minimum number of probe-side geometry to analyze before selecting
    /// the execution mode.
    min_required_count: usize,
    /// The optimal execution mode selected.
    optimal_mode: OnceLock<ExecutionMode>,
    /// The function to select the optimal execution mode.
    func: F,
}

impl<F: Fn(&GeoStatistics, &GeoStatistics) -> ExecutionMode> ExecModeSelector<F> {
    pub(crate) fn new(build_stats: GeoStatistics, min_required_count: usize, func: F) -> Self {
        Self {
            build_stats,
            probe_stats: Mutex::new(GeoStatistics::empty()),
            min_required_count,
            optimal_mode: OnceLock::new(),
            func,
        }
    }

    pub(crate) fn merge_probe_stats(&self, stats: GeoStatistics) {
        let mut probe_stats = self.probe_stats.lock();
        probe_stats.merge(&stats);
        let analyzed_count = probe_stats.total_geometries().unwrap_or(0) as usize;
        if analyzed_count >= self.min_required_count {
            self.optimal_mode
                .get_or_init(|| self.select_optimal_mode(&probe_stats));
        }
    }

    pub(crate) fn optimal_mode(&self) -> Option<ExecutionMode> {
        self.optimal_mode.get().copied()
    }

    fn select_optimal_mode(&self, probe_stats: &GeoStatistics) -> ExecutionMode {
        (self.func)(&self.build_stats, probe_stats)
    }
}

pub type SelectorFunc = fn(&GeoStatistics, &GeoStatistics) -> ExecutionMode;

pub(crate) fn create_exec_mode_selector(
    build_stats: GeoStatistics, exec_mode: ExecutionMode, func: SelectorFunc,
) -> Option<ExecModeSelector<SelectorFunc>> {
    if let ExecutionMode::Speculative(n) = exec_mode {
        Some(ExecModeSelector::new(build_stats, n, func))
    } else {
        None
    }
}

/// Get the current execution mode or update it with the optimal mode if it is
/// not set. If optimal mode is not selected yet, return the default execution
/// mode without updating the execution mode.
pub(crate) fn get_or_update_execution_mode(
    exec_mode: &OnceLock<ExecutionMode>,
    exec_mode_selector: &Option<ExecModeSelector<SelectorFunc>>, default_exec_mode: ExecutionMode,
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

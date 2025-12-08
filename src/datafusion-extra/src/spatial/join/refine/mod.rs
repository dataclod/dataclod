pub mod geos;

use std::sync::Arc;

use datafusion::common::Result;
use wkb::reader::Wkb;

use crate::spatial::join::index::IndexQueryResult;
use crate::spatial::join::option::SpatialJoinOptions;
use crate::spatial::join::spatial_predicate::SpatialPredicate;

/// Trait for refining spatial index query results by evaluating exact geometric
/// predicates.
///
/// This trait represents the second phase of the two-phase spatial join
/// algorithm:
/// 1. **Filter phase**: R-tree index identifies candidate geometries based on
///    bounding rectangles
/// 2. **Refinement phase**: This trait evaluates exact spatial predicates on
///    candidates
///
/// The refinement phase eliminates false positives from the filter phase,
/// ensuring that only geometries that truly satisfy the spatial predicate are
/// returned. Different spatial libraries (Geo, GEOS, TG) provide their own
/// implementations with varying performance characteristics and geometric
/// predicate support.
pub(crate) trait IndexQueryResultRefiner: Send + Sync {
    /// Refine index query results by evaluating the exact spatial predicate.
    ///
    /// Takes a probe geometry and a list of candidate build-side geometries
    /// from the R-tree index, then evaluates the configured spatial
    /// predicate (e.g., intersects, contains, within, distance) to
    /// determine which candidates are actual matches.
    ///
    /// # Arguments
    /// * `probe` - The probe geometry in WKB format to test against candidates
    /// * `index_query_results` - Candidate geometries from the R-tree filter
    ///   phase, containing WKB data, optional distance parameters, and position
    ///   information
    ///
    /// # Returns
    /// * `Vec<(i32, i32)>` - Vector of (batch_index, row_index) pairs for
    ///   geometries that satisfy the spatial predicate
    ///
    /// # Performance
    /// This method may use prepared geometries or other optimizations based on
    /// the execution mode. The implementation should handle empty
    /// geometries gracefully by skipping them rather than failing.
    fn refine(
        &self, probe: &Wkb<'_>, index_query_results: &[IndexQueryResult],
    ) -> Result<Vec<(i32, i32)>>;

    /// Get the current memory usage of the refiner in bytes.
    ///
    /// Used for memory tracking and reservation management. Implementations
    /// should account for prepared geometry caches, internal data
    /// structures, and temporary computation buffers.
    ///
    /// # Returns
    /// * `usize` - Current memory usage in bytes
    fn mem_usage(&self) -> usize;
}

/// Create a spatial predicate refiner for the specified geometry library.
///
/// This factory function instantiates the appropriate refiner implementation
/// based on the selected spatial library backend. Each library provides
/// different trade-offs in terms of performance, memory usage, and geometric
/// predicate support.
///
/// # Arguments
/// * `library` - The spatial library backend to use for geometric computations
/// * `predicate` - The spatial predicate to evaluate (e.g., intersects,
///   contains, distance)
/// * `options` - Configuration options including execution mode and
///   optimization settings
/// * `num_build_geoms` - Total number of build-side geometries, used to size
///   prepared geometry caches when using preparation-based execution modes
///
/// # Returns
/// * `Arc<dyn IndexQueryResultRefiner>` - Thread-safe refiner implementation
///   for the specified library
pub(crate) fn create_refiner(
    predicate: &SpatialPredicate, options: SpatialJoinOptions, num_build_geoms: usize,
) -> Arc<dyn IndexQueryResultRefiner> {
    Arc::new(geos::GeosRefiner::new(predicate, options, num_build_geoms))
}

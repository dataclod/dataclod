use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};

use datafusion::common::{DataFusionError, Result, internal_err};
use geos::{Geom, PreparedGeometry};
use parking_lot::Mutex;
use wkb::reader::Wkb;

use crate::spatial::geos_ext::GEOSWkbFactory;
use crate::spatial::join::index::IndexQueryResult;
use crate::spatial::join::init_once_array::InitOnceArray;
use crate::spatial::join::option::{ExecutionMode, SpatialJoinOptions};
use crate::spatial::join::refine::IndexQueryResultRefiner;
use crate::spatial::join::refine::exec_mode_selector::{
    ExecModeSelector, SelectOptimalMode, get_or_update_execution_mode,
};
use crate::spatial::join::spatial_predicate::{
    RelationPredicate, SpatialPredicate, SpatialRelationType,
};
use crate::spatial::statistics::GeoStatistics;

/// GEOS-specific optimal mode selector that chooses the best execution mode
/// based on geometry complexity statistics.
struct GeosOptimalModeSelector {
    predicate: SpatialPredicate,
    min_points_for_build_preparation: f64,
}

impl GeosOptimalModeSelector {
    /// Both PrepareBuild and PrepareProbe works for ST_Intersects. We select
    /// PrepareBuild when the build side is more complex than the probe side
    /// and the build side is complex enough. This is because using prepared
    /// geometries on the build side has large overhead, using it
    /// on not so complex geometries will not worth it.
    fn select_intersects(&self, build_mean_points: f64, probe_mean_points: f64) -> ExecutionMode {
        if build_mean_points > probe_mean_points
            && build_mean_points >= self.min_points_for_build_preparation
        {
            ExecutionMode::PrepareBuild
        } else {
            ExecutionMode::PrepareProbe
        }
    }

    /// We may use PrepareBuild to evaluate the spatial predicate faster, but
    /// only when the build side is complex enough. Otherwise the overhead
    /// of using prepared geometries on the build side may not worth it. We
    /// don't select PrepareProbe here because it does not work with ST_Contains
    /// and ST_Covers.
    fn select_contains_covers(&self, build_mean_points: f64) -> ExecutionMode {
        if build_mean_points >= self.min_points_for_build_preparation {
            ExecutionMode::PrepareBuild
        } else {
            ExecutionMode::PrepareNone
        }
    }
}

impl SelectOptimalMode for GeosOptimalModeSelector {
    fn select(&self, build_stats: &GeoStatistics, probe_stats: &GeoStatistics) -> ExecutionMode {
        let build_mean_points = build_stats.mean_points_per_geometry().unwrap_or(0.0);
        let probe_mean_points = probe_stats.mean_points_per_geometry().unwrap_or(0.0);
        if matches!(
            &self.predicate,
            SpatialPredicate::Relation(RelationPredicate {
                relation_type: SpatialRelationType::Intersects,
                ..
            })
        ) {
            self.select_intersects(build_mean_points, probe_mean_points)
        } else {
            self.select_without_probe_stats(build_stats)
                .unwrap_or(ExecutionMode::PrepareNone)
        }
    }

    fn select_without_probe_stats(&self, build_stats: &GeoStatistics) -> Option<ExecutionMode> {
        match &self.predicate {
            SpatialPredicate::Distance(_) => Some(ExecutionMode::PrepareNone),
            SpatialPredicate::Relation(predicate) => {
                match predicate.relation_type {
                    SpatialRelationType::Intersects => {
                        // Need probe side statistics to determine optimal execution mode.
                        None
                    }
                    SpatialRelationType::Contains | SpatialRelationType::Covers => {
                        let build_mean_points =
                            build_stats.mean_points_per_geometry().unwrap_or(0.0);
                        Some(self.select_contains_covers(build_mean_points))
                    }
                    SpatialRelationType::Within | SpatialRelationType::CoveredBy => {
                        Some(ExecutionMode::PrepareProbe)
                    }
                    _ => Some(ExecutionMode::PrepareNone),
                }
            }
        }
    }
}

/// A refiner that uses the GEOS library to evaluate spatial predicates.
pub(crate) struct GeosRefiner {
    evaluator: Box<dyn GeosPredicateEvaluator>,
    prepared_geoms: InitOnceArray<Option<OwnedPreparedGeometry>>,
    mem_usage: AtomicUsize,
    exec_mode: OnceLock<ExecutionMode>,
    exec_mode_selector: Option<ExecModeSelector>,
}

/// A wrapper around a GEOS Geometry and its corresponding PreparedGeometry.
///
/// This struct solves the self-referential lifetime problem by using unsafe
/// transmutation to extend the PreparedGeometry lifetime to 'static. This is
/// safe because:
/// 1. The PreparedGeometry is created from self.geometry, which lives as long
///    as self
/// 2. The PreparedGeometry is stored in self and will be dropped before
///    self.geometry
/// 3. We only return references, never move the PreparedGeometry out
///
/// The PreparedGeometry is protected by a Mutex because it has internal mutable
/// state that is not thread-safe.
pub(crate) struct OwnedPreparedGeometry {
    geometry: geos::Geometry,
    /// PreparedGeometry references the original geometry `geometry` it is
    /// created from. The GEOS objects are allocated on the heap so moving
    /// `OwnedPreparedGeometry` does not move the underlying GEOS object, so
    /// we don't need to worry about pinning.
    ///
    /// `PreparedGeometry` is not thread-safe, because it has some lazily
    /// initialized internal states, so we need to use a `Mutex` to protect
    /// it.
    prepared_geometry: Mutex<PreparedGeometry<'static>>,
}

impl OwnedPreparedGeometry {
    /// Create a new OwnedPreparedGeometry from a GEOS Geometry.
    pub fn try_new(geometry: geos::Geometry) -> Result<Self> {
        let prepared = geometry.to_prepared_geom().map_err(|e| {
            DataFusionError::Execution(format!("Failed to create prepared geometry: {e}"))
        })?;

        // SAFETY: We're extending the lifetime of PreparedGeometry to 'static.
        // This is safe because:
        // 1. The PreparedGeometry is created from self.geometry, which lives as long as
        //    self
        // 2. The PreparedGeometry is stored in self.prepared_geometry, which also lives
        //    as long as self
        // 3. We only return references to the PreparedGeometry, never move it out
        // 4. The PreparedGeometry will be dropped when self is dropped, before
        //    self.geometry
        let prepared_static: PreparedGeometry<'static> = unsafe { std::mem::transmute(prepared) };

        Ok(Self {
            geometry,
            prepared_geometry: Mutex::new(prepared_static),
        })
    }

    /// Create a new OwnedPreparedGeometry from a Wkb value.
    pub fn try_from_wkb(wkb: &Wkb) -> Result<Self> {
        let geometry = wkb_to_geos_geometry(wkb)?;
        Self::try_new(geometry)
    }

    /// Get access to the prepared geometry via a Mutex.
    ///
    /// The returned reference has a lifetime tied to &self, which ensures
    /// memory safety. The 'static lifetime on PreparedGeometry indicates it
    /// doesn't borrow from external data.
    pub fn prepared(&self) -> &Mutex<PreparedGeometry<'static>> {
        &self.prepared_geometry
    }

    /// Get the original geometry (for testing purposes).
    pub fn geometry(&self) -> &geos::Geometry {
        &self.geometry
    }
}

// Thread-local GEOS WKB factory for reusing GEOSWkbFactory objects. This avoids
// some memory allocation/deallocation overhead for each `wkb_to_geos_geometry`
// call.
thread_local! {
    static GEOS_WKB_FACTORY: GEOSWkbFactory = GEOSWkbFactory::new();
}

fn wkb_to_geos_geometry(wkb: &Wkb) -> Result<geos::Geometry> {
    GEOS_WKB_FACTORY.with(|factory| {
        factory.create(wkb).map_err(|e| {
            DataFusionError::Execution(format!("Failed to create geometry from WKB: {e}"))
        })
    })
}

impl GeosRefiner {
    pub fn new(
        predicate: &SpatialPredicate, options: SpatialJoinOptions, num_build_geoms: usize,
        build_stats: GeoStatistics,
    ) -> Self {
        let evaluator: Box<dyn GeosPredicateEvaluator> = create_evaluator(predicate);

        let exec_mode = OnceLock::new();
        let exec_mode_selector = match options.execution_mode {
            ExecutionMode::Speculative(n) => {
                let selector = GeosOptimalModeSelector {
                    predicate: predicate.clone(),
                    min_points_for_build_preparation: options.min_points_for_build_preparation
                        as f64,
                };
                if let Some(mode) = selector.select_without_probe_stats(&build_stats) {
                    exec_mode.set(mode).unwrap();
                    None
                } else {
                    Some(ExecModeSelector::new(build_stats, n, Arc::new(selector)))
                }
            }
            _ => {
                exec_mode.set(options.execution_mode).unwrap();
                None
            }
        };

        let prepared_geom_array_size =
            if matches!(exec_mode.get(), Some(ExecutionMode::PrepareBuild) | None) {
                num_build_geoms
            } else {
                0
            };

        let prepared_geoms = InitOnceArray::new(prepared_geom_array_size);
        let mem_usage = prepared_geoms.allocated_size();

        Self {
            evaluator,
            prepared_geoms,
            mem_usage: AtomicUsize::new(mem_usage),
            exec_mode,
            exec_mode_selector,
        }
    }

    fn refine_prepare_none(
        &self, probe: &Wkb<'_>, index_query_results: &[IndexQueryResult],
    ) -> Result<Vec<(i32, i32)>> {
        let mut build_batch_positions = Vec::with_capacity(index_query_results.len());
        let probe_geom = wkb_to_geos_geometry(probe)?;

        for index_result in index_query_results {
            if self
                .evaluator
                .evaluate(index_result.wkb, &probe_geom, index_result.distance)?
            {
                build_batch_positions.push(index_result.position);
            }
        }
        Ok(build_batch_positions)
    }

    fn refine_prepare_build(
        &self, probe: &Wkb<'_>, index_query_results: &[IndexQueryResult],
    ) -> Result<Vec<(i32, i32)>> {
        let mut build_batch_positions = Vec::with_capacity(index_query_results.len());
        let probe_geom = wkb_to_geos_geometry(probe)?;

        for index_result in index_query_results {
            let (prepared_geom, is_newly_created) = self
                .prepared_geoms
                .get_or_create(index_result.geom_idx, || {
                    OwnedPreparedGeometry::try_from_wkb(index_result.wkb).map(Some)
                })?;
            let Some(prepared_geom) = prepared_geom else {
                continue;
            };
            if is_newly_created {
                // TODO: This ia a rough estimate of the memory usage of the prepared geometry
                // and may not be accurate.
                let prep_geom_size = index_result.wkb.buf().len() * 4;
                self.mem_usage.fetch_add(prep_geom_size, Ordering::Relaxed);
            }
            if self.evaluator.evaluate_prepare_build(
                prepared_geom,
                &probe_geom,
                index_result.distance,
            )? {
                build_batch_positions.push(index_result.position);
            }
        }
        Ok(build_batch_positions)
    }

    fn refine_prepare_probe(
        &self, probe: &Wkb<'_>, index_query_results: &[IndexQueryResult],
    ) -> Result<Vec<(i32, i32)>> {
        let mut build_batch_positions = Vec::with_capacity(index_query_results.len());
        let probe_prepared = OwnedPreparedGeometry::try_from_wkb(probe)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        for index_result in index_query_results {
            if self.evaluator.evaluate_prepare_probe(
                index_result.wkb,
                &probe_prepared,
                index_result.distance,
            )? {
                build_batch_positions.push(index_result.position);
            }
        }
        Ok(build_batch_positions)
    }
}

impl IndexQueryResultRefiner for GeosRefiner {
    fn refine(
        &self, probe: &Wkb<'_>, index_query_results: &[IndexQueryResult],
    ) -> Result<Vec<(i32, i32)>> {
        let exec_mode = self.actual_execution_mode();
        match exec_mode {
            ExecutionMode::PrepareNone => self.refine_prepare_none(probe, index_query_results),
            ExecutionMode::PrepareBuild => self.refine_prepare_build(probe, index_query_results),
            ExecutionMode::PrepareProbe => self.refine_prepare_probe(probe, index_query_results),
            ExecutionMode::Speculative(_) => {
                internal_err!(
                    "Speculative execution mode should be translated to other execution modes"
                )
            }
        }
    }

    fn mem_usage(&self) -> usize {
        self.mem_usage.load(Ordering::Relaxed)
    }

    fn actual_execution_mode(&self) -> ExecutionMode {
        get_or_update_execution_mode(
            &self.exec_mode,
            &self.exec_mode_selector,
            ExecutionMode::PrepareProbe,
        )
    }

    fn need_more_probe_stats(&self) -> bool {
        self.exec_mode.get().is_none()
    }

    fn merge_probe_stats(&self, stats: GeoStatistics) {
        if let Some(selector) = self.exec_mode_selector.as_ref() {
            selector.merge_probe_stats(stats);
        }
    }
}

trait GeosPredicateEvaluator: Send + Sync {
    fn evaluate(&self, build: &Wkb, probe: &geos::Geometry, distance: Option<f64>) -> Result<bool>;

    fn evaluate_prepare_build(
        &self, build: &OwnedPreparedGeometry, probe: &geos::Geometry, distance: Option<f64>,
    ) -> Result<bool>;

    fn evaluate_prepare_probe(
        &self, build: &Wkb, probe: &OwnedPreparedGeometry, distance: Option<f64>,
    ) -> Result<bool>;
}

fn create_evaluator(predicate: &SpatialPredicate) -> Box<dyn GeosPredicateEvaluator> {
    match predicate {
        SpatialPredicate::Distance(_) => Box::new(GeosDistance),
        SpatialPredicate::Relation(predicate) => {
            match predicate.relation_type {
                SpatialRelationType::Intersects => Box::new(GeosIntersects),
                SpatialRelationType::Contains => Box::new(GeosContains),
                SpatialRelationType::Within => Box::new(GeosWithin),
                SpatialRelationType::Covers => Box::new(GeosCovers),
                SpatialRelationType::CoveredBy => Box::new(GeosCoveredBy),
                SpatialRelationType::Touches => Box::new(GeosTouches),
                SpatialRelationType::Crosses => Box::new(GeosCrosses),
                SpatialRelationType::Overlaps => Box::new(GeosOverlaps),
                SpatialRelationType::Equals => Box::new(GeosEquals),
            }
        }
    }
}

struct GeosDistance;

impl GeosPredicateEvaluator for GeosDistance {
    fn evaluate(&self, build: &Wkb, probe: &geos::Geometry, distance: Option<f64>) -> Result<bool> {
        let Some(distance) = distance else {
            return Ok(false);
        };
        let build_geom = wkb_to_geos_geometry(build)?;
        let dist = build_geom
            .distance(probe)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        Ok(dist <= distance)
    }

    fn evaluate_prepare_build(
        &self, build: &OwnedPreparedGeometry, probe: &geos::Geometry, distance: Option<f64>,
    ) -> Result<bool> {
        let Some(distance) = distance else {
            return Ok(false);
        };
        let build_geom = build.geometry();
        let dist = build_geom
            .distance(probe)
            .map_err(|e| datafusion::common::DataFusionError::External(Box::new(e)))?;
        Ok(dist <= distance)
    }

    fn evaluate_prepare_probe(
        &self, build: &Wkb, probe: &OwnedPreparedGeometry, distance: Option<f64>,
    ) -> Result<bool> {
        let Some(distance) = distance else {
            return Ok(false);
        };
        let build_geom = wkb_to_geos_geometry(build)?;
        let probe_geom = probe.geometry();
        let dist = build_geom
            .distance(probe_geom)
            .map_err(|e| datafusion::common::DataFusionError::External(Box::new(e)))?;
        Ok(dist <= distance)
    }
}

// GeosEquals needs special handling since it uses covers + covered_by
#[derive(Debug)]
struct GeosEquals;

impl GeosPredicateEvaluator for GeosEquals {
    fn evaluate(
        &self, build: &Wkb, probe: &geos::Geometry, _distance: Option<f64>,
    ) -> Result<bool> {
        let build_geom = wkb_to_geos_geometry(build)?;
        let result = build_geom
            .equals(probe)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        Ok(result)
    }

    fn evaluate_prepare_build(
        &self, build: &OwnedPreparedGeometry, probe: &geos::Geometry, _distance: Option<f64>,
    ) -> Result<bool> {
        let equals = build
            .geometry()
            .equals(probe)
            .map_err(|e| datafusion::common::DataFusionError::External(Box::new(e)))?;
        Ok(equals)
    }

    fn evaluate_prepare_probe(
        &self, build: &Wkb, probe: &OwnedPreparedGeometry, _distance: Option<f64>,
    ) -> Result<bool> {
        let build_geom = wkb_to_geos_geometry(build)?;
        let equals = probe
            .geometry()
            .equals(&build_geom)
            .map_err(|e| datafusion::common::DataFusionError::External(Box::new(e)))?;
        Ok(equals)
    }
}

/// Macro to generate relation evaluators that use GEOS methods
macro_rules! impl_geos_evaluator {
    ($struct_name:ident, $geos_method:ident $(,)?) => {
        #[derive(Debug)]
        struct $struct_name;

        impl GeosPredicateEvaluator for $struct_name {
            fn evaluate(
                &self, build: &Wkb, probe: &geos::Geometry, _distance: Option<f64>,
            ) -> Result<bool> {
                let build_geom = wkb_to_geos_geometry(build)?;
                let result = build_geom
                    .$geos_method(probe)
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
                Ok(result)
            }

            fn evaluate_prepare_build(
                &self, build: &OwnedPreparedGeometry, probe: &geos::Geometry,
                _distance: Option<f64>,
            ) -> Result<bool> {
                let prepared = build.prepared().lock();
                prepared
                    .$geos_method(probe)
                    .map_err(|e| datafusion::common::DataFusionError::External(Box::new(e)))
            }

            fn evaluate_prepare_probe(
                &self, build: &Wkb, probe: &OwnedPreparedGeometry, _distance: Option<f64>,
            ) -> Result<bool> {
                let build_geom = wkb_to_geos_geometry(build)?;
                let prepared = probe.prepared().lock();
                prepared
                    .$geos_method(&build_geom)
                    .map_err(|e| datafusion::common::DataFusionError::External(Box::new(e)))
            }
        }
    };
}

// Generate GEOS-based evaluators using the macro
impl_geos_evaluator!(GeosIntersects, intersects);
impl_geos_evaluator!(GeosContains, contains);
impl_geos_evaluator!(GeosWithin, within);
impl_geos_evaluator!(GeosTouches, touches);
impl_geos_evaluator!(GeosCrosses, crosses);
impl_geos_evaluator!(GeosOverlaps, overlaps);
impl_geos_evaluator!(GeosCovers, covers);
impl_geos_evaluator!(GeosCoveredBy, covered_by);

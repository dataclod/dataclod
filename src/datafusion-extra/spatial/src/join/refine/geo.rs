use std::sync::{Arc, OnceLock};

use datafusion::common::{Result, internal_err};
use geo::{Contains, Intersects, Relate, Within};
use wkb::reader::Wkb;

use crate::geo_ext::item_to_geometry;
use crate::join::index::IndexQueryResult;
use crate::join::refine::IndexQueryResultRefiner;
use crate::join::refine::exec_mode_selector::{
    ExecModeSelector, SelectOptimalMode, get_or_update_execution_mode,
};
use crate::join::spatial_predicate::{SpatialPredicate, SpatialRelationType};
use crate::option::{ExecutionMode, SpatialJoinOptions};
use crate::statistics::GeoStatistics;

/// Geo-specific optimal mode selector that chooses the best execution mode
/// based on probe-side geometry complexity.
struct GeoOptimalModeSelector {
    predicate: SpatialPredicate,
}

impl SelectOptimalMode for GeoOptimalModeSelector {
    fn select(&self, _build_stats: &GeoStatistics, probe_stats: &GeoStatistics) -> ExecutionMode {
        match self.predicate {
            SpatialPredicate::Distance(_) => ExecutionMode::PrepareNone,
            SpatialPredicate::Relation(_) => {
                // We only support PrepareProbe and PrepareNone. Only the stats from the probe
                // side is used to select the execution mode.
                let probe_mean_points_per_geometry =
                    probe_stats.mean_points_per_geometry().unwrap_or(0.0);
                if probe_mean_points_per_geometry <= 50.0 {
                    ExecutionMode::PrepareNone
                } else {
                    ExecutionMode::PrepareProbe
                }
            }
        }
    }

    fn select_without_probe_stats(&self, _build_stats: &GeoStatistics) -> Option<ExecutionMode> {
        match self.predicate {
            SpatialPredicate::Distance(_) => Some(ExecutionMode::PrepareNone),
            _ => None,
        }
    }
}

/// A refiner that uses the geo library to evaluate spatial predicates.
pub(crate) struct GeoRefiner {
    evaluator: Box<dyn GeoPredicateEvaluator>,
    exec_mode: OnceLock<ExecutionMode>,
    exec_mode_selector: Option<ExecModeSelector>,
}

impl GeoRefiner {
    pub fn new(
        predicate: &SpatialPredicate, options: SpatialJoinOptions, build_stats: GeoStatistics,
    ) -> Self {
        let evaluator = create_evaluator(predicate);

        let exec_mode = OnceLock::new();
        let exec_mode_selector = match options.execution_mode {
            ExecutionMode::Speculative(n) => {
                let selector = GeoOptimalModeSelector {
                    predicate: predicate.clone(),
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

        Self {
            evaluator,
            exec_mode,
            exec_mode_selector,
        }
    }

    fn refine_prepare_none(
        &self, probe: &Wkb<'_>, index_query_results: &[IndexQueryResult],
    ) -> Result<Vec<(i32, i32)>> {
        let mut build_batch_positions = Vec::with_capacity(index_query_results.len());
        for index_result in index_query_results {
            if self
                .evaluator
                .evaluate(index_result.wkb, probe, index_result.distance)?
            {
                build_batch_positions.push(index_result.position);
            }
        }
        Ok(build_batch_positions)
    }

    fn refine_prepare_probe(
        &self, probe: &Wkb<'_>, index_query_results: &[IndexQueryResult],
    ) -> Result<Vec<(i32, i32)>> {
        let mut build_batch_positions = Vec::with_capacity(index_query_results.len());
        let Ok(probe_geom) = item_to_geometry(probe) else {
            return Ok(Vec::new());
        };
        let probe_geom = geo::PreparedGeometry::from(probe_geom);

        for index_result in index_query_results {
            if self.evaluator.evaluate_prepare_probe(
                index_result.wkb,
                &probe_geom,
                index_result.distance,
            )? {
                build_batch_positions.push(index_result.position);
            }
        }
        Ok(build_batch_positions)
    }
}

impl IndexQueryResultRefiner for GeoRefiner {
    fn refine(
        &self, probe: &Wkb<'_>, index_query_results: &[IndexQueryResult],
    ) -> Result<Vec<(i32, i32)>> {
        let exec_mode = self.actual_execution_mode();
        match exec_mode {
            ExecutionMode::PrepareNone => self.refine_prepare_none(probe, index_query_results),
            ExecutionMode::PrepareBuild => {
                // Prepare build mode is not implemented for geo, because geo's prepared
                // geometry is not thread-safe and runs slowly, we suggest using
                // other libraries that have faster prepared geometry, such as
                // tg or GEOS.
                self.refine_prepare_none(probe, index_query_results)
            }
            ExecutionMode::PrepareProbe => self.refine_prepare_probe(probe, index_query_results),
            ExecutionMode::Speculative(_) => {
                internal_err!(
                    "Speculative execution mode should be translated to other execution modes"
                )
            }
        }
    }

    fn mem_usage(&self) -> usize {
        0
    }

    fn actual_execution_mode(&self) -> ExecutionMode {
        get_or_update_execution_mode(
            &self.exec_mode,
            &self.exec_mode_selector,
            ExecutionMode::PrepareNone,
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

trait GeoPredicateEvaluator: Send + Sync {
    fn evaluate(&self, build: &Wkb, probe: &Wkb, distance: Option<f64>) -> Result<bool>;

    fn evaluate_prepare_probe(
        &self, build: &Wkb, probe: &geo::PreparedGeometry<'static, geo_types::Geometry>,
        distance: Option<f64>,
    ) -> Result<bool>;
}

fn create_evaluator(predicate: &SpatialPredicate) -> Box<dyn GeoPredicateEvaluator> {
    match predicate {
        SpatialPredicate::Distance(_) => Box::new(GeoDistance),
        SpatialPredicate::Relation(predicate) => {
            match predicate.relation_type {
                SpatialRelationType::Intersects => Box::new(GeoIntersects),
                SpatialRelationType::Contains => Box::new(GeoContains),
                SpatialRelationType::Within => Box::new(GeoWithin),
                SpatialRelationType::Covers => Box::new(GeoCovers),
                SpatialRelationType::CoveredBy => Box::new(GeoCoveredBy),
                SpatialRelationType::Touches => Box::new(GeoTouches),
                SpatialRelationType::Crosses => Box::new(GeoCrosses),
                SpatialRelationType::Overlaps => Box::new(GeoOverlaps),
                SpatialRelationType::Equals => Box::new(GeoEquals),
            }
        }
    }
}

struct GeoIntersects;

impl GeoPredicateEvaluator for GeoIntersects {
    fn evaluate(&self, build: &Wkb, probe: &Wkb, _distance: Option<f64>) -> Result<bool> {
        let Ok(build_geom) = item_to_geometry(build) else {
            return Ok(false);
        };
        let Ok(probe_geom) = item_to_geometry(probe) else {
            return Ok(false);
        };
        Ok(build_geom.intersects(&probe_geom))
    }

    fn evaluate_prepare_probe(
        &self, build: &Wkb, probe: &geo::PreparedGeometry<'static, geo_types::Geometry>,
        _distance: Option<f64>,
    ) -> Result<bool> {
        let Ok(build_geom) = item_to_geometry(build) else {
            return Ok(false);
        };
        Ok(probe.relate(&build_geom).is_intersects())
    }
}

struct GeoContains;

impl GeoPredicateEvaluator for GeoContains {
    fn evaluate(&self, build: &Wkb, probe: &Wkb, _distance: Option<f64>) -> Result<bool> {
        let Ok(build_geom) = item_to_geometry(build) else {
            return Ok(false);
        };
        let Ok(probe_geom) = item_to_geometry(probe) else {
            return Ok(false);
        };
        Ok(build_geom.contains(&probe_geom))
    }

    fn evaluate_prepare_probe(
        &self, build: &Wkb, probe: &geo::PreparedGeometry<'static, geo_types::Geometry>,
        _distance: Option<f64>,
    ) -> Result<bool> {
        let Ok(build_geom) = item_to_geometry(build) else {
            return Ok(false);
        };
        Ok(probe.relate(&build_geom).is_within())
    }
}

struct GeoWithin;

impl GeoPredicateEvaluator for GeoWithin {
    fn evaluate(&self, build: &Wkb, probe: &Wkb, _distance: Option<f64>) -> Result<bool> {
        let Ok(build_geom) = item_to_geometry(build) else {
            return Ok(false);
        };
        let Ok(probe_geom) = item_to_geometry(probe) else {
            return Ok(false);
        };
        Ok(build_geom.is_within(&probe_geom))
    }

    fn evaluate_prepare_probe(
        &self, build: &Wkb, probe: &geo::PreparedGeometry<'static, geo_types::Geometry>,
        _distance: Option<f64>,
    ) -> Result<bool> {
        let Ok(build_geom) = item_to_geometry(build) else {
            return Ok(false);
        };
        Ok(probe.relate(&build_geom).is_contains())
    }
}

struct GeoDistance;

impl GeoPredicateEvaluator for GeoDistance {
    fn evaluate(&self, _build: &Wkb, _probe: &Wkb, _distance: Option<f64>) -> Result<bool> {
        Ok(false)
    }

    fn evaluate_prepare_probe(
        &self, _build: &Wkb, _probe: &geo::PreparedGeometry<'static, geo_types::Geometry>,
        _distance: Option<f64>,
    ) -> Result<bool> {
        Ok(false)
    }
}

/// Macro to generate relation evaluators that use the `relate()` method
macro_rules! impl_relate_evaluator {
    ($struct_name:ident, $geo_method:ident $(,)?) => {
        #[derive(Debug)]
        struct $struct_name;

        impl GeoPredicateEvaluator for $struct_name {
            fn evaluate(&self, build: &Wkb, probe: &Wkb, _distance: Option<f64>) -> Result<bool> {
                let Ok(build_geom) = item_to_geometry(build) else {
                    return Ok(false);
                };
                let Ok(probe_geom) = item_to_geometry(probe) else {
                    return Ok(false);
                };
                Ok(build_geom.relate(&probe_geom).$geo_method())
            }

            fn evaluate_prepare_probe(
                &self, build: &Wkb, probe: &geo::PreparedGeometry<'static, geo_types::Geometry>,
                _distance: Option<f64>,
            ) -> Result<bool> {
                let Ok(build_geom) = item_to_geometry(build) else {
                    return Ok(false);
                };
                Ok(probe.relate(&build_geom).$geo_method())
            }
        }
    };
}

// Generate relate-based evaluators using the macro
impl_relate_evaluator!(GeoTouches, is_touches);
impl_relate_evaluator!(GeoCrosses, is_crosses);
impl_relate_evaluator!(GeoOverlaps, is_overlaps);
impl_relate_evaluator!(GeoCovers, is_covers);
impl_relate_evaluator!(GeoCoveredBy, is_coveredby);
impl_relate_evaluator!(GeoEquals, is_equal_topo);

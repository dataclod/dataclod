use std::marker::PhantomData;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};

use datafusion::common::{DataFusionError, Result, internal_err};
use sedona_tg::tg;
use wkb::reader::Wkb;

use crate::join::index::IndexQueryResult;
use crate::join::refine::IndexQueryResultRefiner;
use crate::join::refine::exec_mode_selector::{
    ExecModeSelector, SelectOptimalMode, get_or_update_execution_mode,
};
use crate::join::spatial_predicate::{RelationPredicate, SpatialPredicate, SpatialRelationType};
use crate::join::utils::init_once_array::InitOnceArray;
use crate::option::{ExecutionMode, SpatialJoinOptions, TgIndexType};
use crate::statistics::GeoStatistics;

/// TG-specific optimal mode selector that chooses the best execution mode
/// based on geometry complexity and TG library characteristics.
struct TgOptimalModeSelector {
    predicate: SpatialPredicate,
}

impl TgOptimalModeSelector {
    fn select_intersects(
        &self, build_stats: &GeoStatistics, probe_stats: &GeoStatistics,
    ) -> ExecutionMode {
        let build_mean_points_per_geometry = build_stats.mean_points_per_geometry().unwrap_or(0.0);
        let probe_mean_points_per_geometry = probe_stats.mean_points_per_geometry().unwrap_or(0.0);

        let max_mean_points_per_geometry =
            build_mean_points_per_geometry.max(probe_mean_points_per_geometry);
        if max_mean_points_per_geometry <= 32.0 {
            // If the mean points per geometry is less than 32, the geometries are not
            // complex enough to benefit from prepared geometries. TG itself
            // will skip creating index for such geometries. Please refer to
            // `default_index_spread` in tg.c for more details.
            //
            // We select PrepareProbe here because we want TG to automatically figure out
            // whether to create index for each individual probe geometry. We
            // don't use PrepareBuild because it will take lots of memory
            // storing not-prepared geometries, while it does not trade much for the
            // performance.
            ExecutionMode::PrepareProbe
        } else {
            // Choose a more complex side to prepare the geometries
            if build_mean_points_per_geometry > probe_mean_points_per_geometry {
                ExecutionMode::PrepareBuild
            } else {
                ExecutionMode::PrepareProbe
            }
        }
    }

    fn select_contains_covers(&self, build_stats: &GeoStatistics) -> ExecutionMode {
        let build_mean_points = build_stats.mean_points_per_geometry().unwrap_or(0.0);
        if build_mean_points >= 32.0 {
            ExecutionMode::PrepareBuild
        } else {
            ExecutionMode::PrepareNone
        }
    }
}

impl SelectOptimalMode for TgOptimalModeSelector {
    fn select(&self, build_stats: &GeoStatistics, probe_stats: &GeoStatistics) -> ExecutionMode {
        if matches!(
            &self.predicate,
            SpatialPredicate::Relation(RelationPredicate {
                relation_type: SpatialRelationType::Intersects,
                ..
            })
        ) {
            self.select_intersects(build_stats, probe_stats)
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
                        // Both PrepareBuild and PrepareProbe can be used for intersects predicate.
                        // We need statistics from the probe side to select the optimal execution
                        // mode.
                        None
                    }
                    SpatialRelationType::Contains | SpatialRelationType::Covers => {
                        // PrepareBuild is the only execution mode that works for Contains and
                        // Covers. However, it needs additional memory so it
                        // is not always beneficial to use it.
                        Some(self.select_contains_covers(build_stats))
                    }
                    SpatialRelationType::Within | SpatialRelationType::CoveredBy => {
                        Some(ExecutionMode::PrepareProbe)
                    }
                    _ => {
                        // Other predicates cannot be accelerated by prepared geometries.
                        Some(ExecutionMode::PrepareNone)
                    }
                }
            }
        }
    }
}

/// A refiner that uses the tiny geometry library to evaluate spatial
/// predicates.
pub(crate) struct TgRefiner {
    evaluator: Box<dyn TgPredicateEvaluator>,
    prepared_geoms: InitOnceArray<Option<tg::Geom>>,
    index_type: tg::IndexType,
    mem_usage: AtomicUsize,
    exec_mode: OnceLock<ExecutionMode>,
    exec_mode_selector: Option<ExecModeSelector>,
}

impl TgRefiner {
    pub fn try_new(
        predicate: &SpatialPredicate, options: SpatialJoinOptions, num_build_geoms: usize,
        build_stats: GeoStatistics,
    ) -> Result<Self> {
        let evaluator: Box<dyn TgPredicateEvaluator> = create_evaluator(predicate)?;
        let index_type = match options.tg.index_type {
            TgIndexType::Natural => tg::IndexType::Natural,
            TgIndexType::YStripes => tg::IndexType::YStripes,
        };

        let exec_mode = OnceLock::new();
        let exec_mode_selector = match options.execution_mode {
            ExecutionMode::Speculative(n) => {
                let selector = TgOptimalModeSelector {
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

        let prepared_geom_array_size =
            if matches!(exec_mode.get(), Some(ExecutionMode::PrepareBuild) | None) {
                num_build_geoms
            } else {
                0
            };

        let prepared_geoms = InitOnceArray::new(prepared_geom_array_size);
        let mem_usage = prepared_geoms.allocated_size();
        Ok(Self {
            evaluator,
            prepared_geoms,
            index_type,
            mem_usage: AtomicUsize::new(mem_usage),
            exec_mode,
            exec_mode_selector,
        })
    }

    fn refine_prepare_build(
        &self, probe: &wkb::reader::Wkb<'_>, index_query_results: &[IndexQueryResult],
    ) -> Result<Vec<(i32, i32)>> {
        let mut build_batch_positions = Vec::with_capacity(index_query_results.len());
        let probe_geom = tg::Geom::parse_wkb(probe.buf(), self.index_type)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        for index_result in index_query_results {
            let (build_geom, is_newly_created) =
                self.prepared_geoms
                    .get_or_create(index_result.geom_idx, || {
                        let geom = tg::Geom::parse_wkb(index_result.wkb.buf(), self.index_type)
                            .map_err(|e| DataFusionError::External(Box::new(e)))?;
                        Ok(Some(geom))
                    })?;
            let Some(build_geom) = build_geom else {
                continue;
            };
            if is_newly_created {
                let prep_geom_size = build_geom.memsize();
                self.mem_usage.fetch_add(prep_geom_size, Ordering::Relaxed);
            }
            if self
                .evaluator
                .evaluate(build_geom, &probe_geom, index_result.distance)?
            {
                build_batch_positions.push(index_result.position);
            }
        }
        Ok(build_batch_positions)
    }

    fn refine_not_prepare_build(
        &self, probe: &wkb::reader::Wkb<'_>, index_query_results: &[IndexQueryResult],
        probe_index_type: tg::IndexType,
    ) -> Result<Vec<(i32, i32)>> {
        let mut build_batch_positions = Vec::with_capacity(index_query_results.len());
        let probe_geom = tg::Geom::parse_wkb(probe.buf(), probe_index_type)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        for index_result in index_query_results {
            let build_geom = tg::Geom::parse_wkb(index_result.wkb.buf(), tg::IndexType::Unindexed)
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            if self
                .evaluator
                .evaluate(&build_geom, &probe_geom, index_result.distance)?
            {
                build_batch_positions.push(index_result.position);
            }
        }
        Ok(build_batch_positions)
    }
}

impl IndexQueryResultRefiner for TgRefiner {
    fn refine(
        &self, probe: &Wkb<'_>, index_query_results: &[IndexQueryResult],
    ) -> Result<Vec<(i32, i32)>> {
        let exec_mode = self.actual_execution_mode();
        match exec_mode {
            ExecutionMode::PrepareNone => {
                self.refine_not_prepare_build(probe, index_query_results, tg::IndexType::Unindexed)
            }
            ExecutionMode::PrepareBuild => self.refine_prepare_build(probe, index_query_results),
            ExecutionMode::PrepareProbe => {
                self.refine_not_prepare_build(probe, index_query_results, self.index_type)
            }
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

trait TgPredicateEvaluator: Send + Sync {
    fn evaluate(&self, build: &tg::Geom, probe: &tg::Geom, distance: Option<f64>) -> Result<bool>;
}

struct TgPredicateEvaluatorImpl<Op: tg::BinaryPredicate + Send + Sync> {
    _marker: PhantomData<Op>,
}

impl<Op: tg::BinaryPredicate + Send + Sync> TgPredicateEvaluatorImpl<Op> {
    pub fn new() -> Self {
        Self {
            _marker: PhantomData,
        }
    }
}

impl<Op: tg::BinaryPredicate + Send + Sync> TgPredicateEvaluator for TgPredicateEvaluatorImpl<Op> {
    fn evaluate(&self, build: &tg::Geom, probe: &tg::Geom, _distance: Option<f64>) -> Result<bool> {
        Ok(Op::evaluate(build, probe))
    }
}

fn create_evaluator(predicate: &SpatialPredicate) -> Result<Box<dyn TgPredicateEvaluator>> {
    let evaluator: Box<dyn TgPredicateEvaluator> = match predicate {
        SpatialPredicate::Distance(_) => {
            return Err(DataFusionError::Internal(
                "Distance predicate is not supported for TG".to_owned(),
            ));
        }
        SpatialPredicate::Relation(predicate) => {
            match predicate.relation_type {
                SpatialRelationType::Intersects => {
                    Box::new(TgPredicateEvaluatorImpl::<tg::Intersects>::new())
                }
                SpatialRelationType::Contains => {
                    Box::new(TgPredicateEvaluatorImpl::<tg::Contains>::new())
                }
                SpatialRelationType::Within => {
                    Box::new(TgPredicateEvaluatorImpl::<tg::Within>::new())
                }
                SpatialRelationType::Covers => {
                    Box::new(TgPredicateEvaluatorImpl::<tg::Covers>::new())
                }
                SpatialRelationType::CoveredBy => {
                    Box::new(TgPredicateEvaluatorImpl::<tg::CoveredBy>::new())
                }
                SpatialRelationType::Touches => {
                    Box::new(TgPredicateEvaluatorImpl::<tg::Touches>::new())
                }
                SpatialRelationType::Equals => {
                    Box::new(TgPredicateEvaluatorImpl::<tg::Equals>::new())
                }
                _ => {
                    return Err(DataFusionError::Internal(
                        "Unsupported spatial relation type for TG".to_owned(),
                    ));
                }
            }
        }
    };
    Ok(evaluator)
}

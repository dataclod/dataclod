use core::fmt;
use std::mem::transmute;
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, AsArray, Float64Array, RecordBatch};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::utils::proxy::VecAllocExt;
use datafusion::common::{DataFusionError, JoinSide, Result, ScalarValue};
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_expr::PhysicalExpr;
use float_next_after::NextAfter;
use geo::{BoundingRect, coord};
use geo_index::rtree::util::f64_box_to_f32;
use geo_traits::to_geo::ToGeoGeometry;
use geo_types::Rect;
use wkb::reader::Wkb;

use crate::join::spatial_predicate::{DistancePredicate, RelationPredicate, SpatialPredicate};

/// Operand evaluator is for evaluating the operands of a spatial predicate. It
/// can be a distance operand evaluator or a relation operand evaluator.
pub trait OperandEvaluator: fmt::Debug + Send + Sync {
    /// Evaluate the spatial predicate operand on the build side.
    fn evaluate_build(&self, batch: &RecordBatch) -> Result<EvaluatedGeometryArray> {
        let geom_expr = self.build_side_expr()?;
        evaluate_with_rects(batch, &geom_expr)
    }

    /// Evaluate the spatial predicate operand on the probe side.
    fn evaluate_probe(&self, batch: &RecordBatch) -> Result<EvaluatedGeometryArray> {
        let geom_expr = self.probe_side_expr()?;
        evaluate_with_rects(batch, &geom_expr)
    }

    /// Resolve the distance operand for a given row.
    fn resolve_distance(
        &self, _build_distance: &Option<ColumnarValue>, _build_row_idx: usize,
        _probe_distance: &Option<f64>,
    ) -> Result<Option<f64>> {
        Ok(None)
    }

    /// Get the expression for the build side.
    fn build_side_expr(&self) -> Result<Arc<dyn PhysicalExpr>>;

    /// Get the expression for the probe side.
    fn probe_side_expr(&self) -> Result<Arc<dyn PhysicalExpr>>;
}

/// Create a spatial predicate evaluator for the spatial predicate.
pub fn create_operand_evaluator(predicate: &SpatialPredicate) -> Arc<dyn OperandEvaluator> {
    match predicate {
        SpatialPredicate::Distance(predicate) => {
            Arc::new(DistanceOperandEvaluator::new(predicate.clone()))
        }
        SpatialPredicate::Relation(predicate) => {
            Arc::new(RelationOperandEvaluator::new(predicate.clone()))
        }
    }
}

/// Result of evaluating a geometry batch.
pub struct EvaluatedGeometryArray {
    /// The array of geometries produced by evaluating the geometry expression.
    pub geometry_array: ArrayRef,
    /// The rects of the geometries in the geometry array. The length of this
    /// array is equal to the number of geometries. The rects will be None
    /// for empty or null geometries.
    pub rects: Vec<Option<Rect<f32>>>,
    /// The distance value produced by evaluating the distance expression.
    pub distance: Option<ColumnarValue>,
    /// WKBs of the geometries in `geometry_array`. The wkb values reference
    /// buffers inside the geometry array, but we'll only allow accessing
    /// Wkb<'a> where 'a is the lifetime of the `GeometryBatchResult` to make
    /// the interfaces safe. The buffers in `geometry_array` are allocated on
    /// the heap and won't be moved when the `GeometryBatchResult` is moved,
    /// so we don't need to worry about pinning.
    wkbs: Vec<Option<Wkb<'static>>>,
}

impl EvaluatedGeometryArray {
    pub fn new(geometry_array: ArrayRef) -> Self {
        let num_rows = geometry_array.len();
        let mut rect_vec = Vec::with_capacity(num_rows);
        let mut wkbs = Vec::with_capacity(num_rows);
        let wkb_array = geometry_array.as_binary_view();
        wkb_array.iter().for_each(|wkb_opt| {
            let wkb_opt = wkb_opt.and_then(|wkb| Wkb::try_new(wkb).ok());
            let rect_opt = wkb_opt.as_ref().and_then(|wkb| {
                wkb.to_geometry().bounding_rect().map(|rect| {
                    let min = rect.min();
                    let max = rect.max();
                    // f64_box_to_f32 will ensure the resulting `f32` box is no smaller than the
                    // `f64` box.
                    let (min_x, min_y, max_x, max_y) = f64_box_to_f32(min.x, min.y, max.x, max.y);
                    Rect::new(coord! { x: min_x, y: min_y }, coord! { x: max_x, y: max_y })
                })
            });
            wkbs.push(wkb_opt);
            rect_vec.push(rect_opt);
        });

        // Safety: The wkbs must reference buffers inside the `geometry_array`. Since
        // the `geometry_array` and `wkbs` are both owned by the
        // `EvaluatedGeometryArray`, so they have the same lifetime. We'll never have a
        // situation where the `EvaluatedGeometryArray` is dropped while the `wkbs` are
        // still in use (guaranteed by the scope of the `wkbs` field and lifetime
        // signature of the `wkbs` method).
        let wkbs = wkbs
            .into_iter()
            .map(|wkb| wkb.map(|wkb| unsafe { transmute(wkb) }))
            .collect();
        Self {
            geometry_array,
            rects: rect_vec,
            distance: None,
            wkbs,
        }
    }

    /// Get the WKBs of the geometries in the geometry array.
    pub fn wkbs(&self) -> &Vec<Option<Wkb<'_>>> {
        // The returned WKBs are guaranteed to be valid for the lifetime of the
        // GeometryBatchResult, because the WKBs reference buffers inside
        // `geometry_array`, which is guaranteed to be valid for the lifetime of
        // the GeometryBatchResult. We shorten the lifetime of the WKBs from 'static
        // to '_, so that the caller can use the WKBs without worrying about the
        // lifetime.
        &self.wkbs
    }

    pub fn in_mem_size(&self) -> usize {
        let distance_in_mem_size = match &self.distance {
            Some(ColumnarValue::Array(array)) => array.get_array_memory_size(),
            _ => 8,
        };

        // Note: this is not an accurate, because wkbs has inner Vecs. However, the size
        // of inner vecs should be small, so the inaccuracy does not matter too
        // much.
        let wkb_vec_size = self.wkbs.allocated_size();

        self.geometry_array.get_array_memory_size()
            + self.rects.allocated_size()
            + distance_in_mem_size
            + wkb_vec_size
    }
}

/// Evaluator for a relation predicate.
#[derive(Debug)]
struct RelationOperandEvaluator {
    inner: RelationPredicate,
}

impl RelationOperandEvaluator {
    pub fn new(inner: RelationPredicate) -> Self {
        Self { inner }
    }
}

/// Evaluator for a distance predicate.
#[derive(Debug)]
struct DistanceOperandEvaluator {
    inner: DistancePredicate,
}

impl DistanceOperandEvaluator {
    pub fn new(inner: DistancePredicate) -> Self {
        Self { inner }
    }
}

fn evaluate_with_rects(
    batch: &RecordBatch, geom_expr: &Arc<dyn PhysicalExpr>,
) -> Result<EvaluatedGeometryArray> {
    let geometry_columnar_value = geom_expr.evaluate(batch)?;
    let num_rows = batch.num_rows();
    let geometry_array = geometry_columnar_value.to_array(num_rows)?;
    Ok(EvaluatedGeometryArray::new(geometry_array))
}

impl DistanceOperandEvaluator {
    fn evaluate_with_rects(
        &self, batch: &RecordBatch, geom_expr: &Arc<dyn PhysicalExpr>, side: JoinSide,
    ) -> Result<EvaluatedGeometryArray> {
        let mut result = evaluate_with_rects(batch, geom_expr)?;

        let should_expand = match side {
            JoinSide::Left => self.inner.distance_side == JoinSide::Left,
            JoinSide::Right => self.inner.distance_side != JoinSide::Left,
            JoinSide::None => unreachable!(),
        };

        if !should_expand {
            return Ok(result);
        }

        // Expand the vec by distance
        let distance_columnar_value = self.inner.distance.evaluate(batch)?;
        // No timezone conversion needed for distance; pass None as cast_options
        // explicitly.
        let distance_columnar_value = distance_columnar_value.cast_to(&DataType::Float64, None)?;
        match &distance_columnar_value {
            ColumnarValue::Scalar(ScalarValue::Float64(Some(distance))) => {
                result.rects.iter_mut().for_each(|rect_opt| {
                    let Some(rect) = rect_opt else {
                        return;
                    };
                    expand_rect_in_place(rect, *distance);
                });
            }
            ColumnarValue::Scalar(ScalarValue::Float64(None)) => {
                // Distance expression evaluates to NULL, the resulting distance should be NULL
                // as well.
                result.rects.clear();
            }
            ColumnarValue::Array(array) => {
                if let Some(array) = array.as_any().downcast_ref::<Float64Array>() {
                    for (geom_idx, rect_opt) in result.rects.iter_mut().enumerate() {
                        if array.is_valid(geom_idx) {
                            let dist = array.value(geom_idx);
                            let Some(rect) = rect_opt else {
                                continue;
                            };
                            expand_rect_in_place(rect, dist);
                        }
                    }
                } else {
                    return Err(DataFusionError::Internal(
                        "Distance columnar value is not a Float64Array".to_owned(),
                    ));
                }
            }
            _ => {
                return Err(DataFusionError::Internal(
                    "Distance columnar value is not a Float64".to_owned(),
                ));
            }
        }

        result.distance = Some(distance_columnar_value);
        Ok(result)
    }
}

pub fn distance_value_at(distance_columnar_value: &ColumnarValue, i: usize) -> Result<Option<f64>> {
    match distance_columnar_value {
        ColumnarValue::Scalar(ScalarValue::Float64(dist_opt)) => Ok(*dist_opt),
        ColumnarValue::Array(array) => {
            if let Some(array) = array.as_any().downcast_ref::<Float64Array>() {
                if array.is_null(i) {
                    Ok(None)
                } else {
                    Ok(Some(array.value(i)))
                }
            } else {
                Err(DataFusionError::Internal(
                    "Distance columnar value is not a Float64Array".to_owned(),
                ))
            }
        }
        _ => {
            Err(DataFusionError::Internal(
                "Distance columnar value is not a Float64".to_owned(),
            ))
        }
    }
}

fn expand_rect_in_place(rect: &mut Rect<f32>, distance: f64) {
    let mut min = rect.min();
    let mut max = rect.max();
    let mut distance_f32 = distance as f32;
    // distance_f32 may be smaller than the original f64 value due to loss of
    // precision. We need to expand the rect using next_after to ensure that the
    // rect expansion is always inclusive, otherwise we may miss some query
    // results.
    if (distance_f32 as f64) < distance {
        distance_f32 = distance_f32.next_after(f32::INFINITY);
    }
    min.x -= distance_f32;
    min.y -= distance_f32;
    max.x += distance_f32;
    max.y += distance_f32;
    rect.set_min(min);
    rect.set_max(max);
}

impl OperandEvaluator for DistanceOperandEvaluator {
    fn evaluate_build(&self, batch: &RecordBatch) -> Result<EvaluatedGeometryArray> {
        let geom_expr = self.build_side_expr()?;
        self.evaluate_with_rects(batch, &geom_expr, JoinSide::Left)
    }

    fn evaluate_probe(&self, batch: &RecordBatch) -> Result<EvaluatedGeometryArray> {
        let geom_expr = self.probe_side_expr()?;
        self.evaluate_with_rects(batch, &geom_expr, JoinSide::Right)
    }

    fn build_side_expr(&self) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(self.inner.left.clone())
    }

    fn probe_side_expr(&self) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(self.inner.right.clone())
    }

    fn resolve_distance(
        &self, build_distance: &Option<ColumnarValue>, build_row_idx: usize,
        probe_distance: &Option<f64>,
    ) -> Result<Option<f64>> {
        match self.inner.distance_side {
            JoinSide::Left => {
                let Some(distance) = build_distance else {
                    return Ok(None);
                };
                distance_value_at(distance, build_row_idx)
            }
            JoinSide::Right | JoinSide::None => Ok(*probe_distance),
        }
    }
}

impl OperandEvaluator for RelationOperandEvaluator {
    fn build_side_expr(&self) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(self.inner.left.clone())
    }

    fn probe_side_expr(&self) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(self.inner.right.clone())
    }
}

//! Utility functions for spatial partitioning.

use datafusion::common::{DataFusionError, Result};
use geo::{Coord, CoordNum, Rect};
use geo_index::rtree::util::f64_box_to_f32;

use crate::geometry::bounding_box::BoundingBox;
use crate::geometry::interval::IntervalTrait;

/// Convert a `BoundingBox` to f32 coordinates
///
/// # Arguments
/// * `bbox` - The `BoundingBox` to convert
///
/// # Returns
/// A tuple of (`min_x`, `min_y`, `max_x`, `max_y`) as f32 values
///
/// # Errors
/// Returns an error if the bounding box has wraparound coordinates (e.g.,
/// crossing the anti-meridian)
pub fn bbox_to_f32_rect(bbox: &BoundingBox) -> Result<Option<(f32, f32, f32, f32)>> {
    // Check for wraparound coordinates
    if bbox.x().is_wraparound() {
        return Err(DataFusionError::Execution(
            "BoundingBox has wraparound coordinates, which is not supported yet".to_owned(),
        ));
    }

    let min_x = bbox.x().lo();
    let min_y = bbox.y().lo();
    let max_x = bbox.x().hi();
    let max_y = bbox.y().hi();

    if min_x <= max_x && min_y <= max_y {
        Ok(Some(f64_box_to_f32(min_x, min_y, max_x, max_y)))
    } else {
        Ok(None)
    }
}

/// Convert a [`BoundingBox`] into a [`Rect<f32>`] with the same adjusted bounds
/// as [`bbox_to_f32_rect`].
pub fn bbox_to_geo_rect(bbox: &BoundingBox) -> Result<Option<Rect<f32>>> {
    if let Some((min_x, min_y, max_x, max_y)) = bbox_to_f32_rect(bbox)? {
        Ok(Some(make_rect(min_x, min_y, max_x, max_y)))
    } else {
        Ok(None)
    }
}

/// Convert a [`Rect<f32>`] into a [`BoundingBox`].
pub fn geo_rect_to_bbox(rect: &Rect<f32>) -> BoundingBox {
    let min = rect.min();
    let max = rect.max();
    BoundingBox::xy((min.x as f64, max.x as f64), (min.y as f64, max.y as f64))
}

/// Creates a `Rect` from four coordinate values representing the bounding box.
///
/// This is a convenience function that constructs a `geo::Rect` from individual
/// coordinate components.
pub fn make_rect<T: CoordNum>(xmin: T, ymin: T, xmax: T, ymax: T) -> Rect<T> {
    Rect::new(Coord { x: xmin, y: ymin }, Coord { x: xmax, y: ymax })
}

/// Returns `true` if two rectangles intersect (including touching edges).
pub fn rects_intersect(a: &Rect<f32>, b: &Rect<f32>) -> bool {
    let (a_min, a_max) = (a.min(), a.max());
    let (b_min, b_max) = (b.min(), b.max());

    a_min.x <= b_max.x && a_max.x >= b_min.x && a_min.y <= b_max.y && a_max.y >= b_min.y
}

/// Returns the intersection area between two rectangles.
pub fn rect_intersection_area(a: &Rect<f32>, b: &Rect<f32>) -> f32 {
    if !rects_intersect(a, b) {
        return 0.0;
    }

    let (a_min, a_max) = (a.min(), a.max());
    let (b_min, b_max) = (b.min(), b.max());

    let min_x = a_min.x.max(b_min.x);
    let min_y = a_min.y.max(b_min.y);
    let max_x = a_max.x.min(b_max.x);
    let max_y = a_max.y.min(b_max.y);

    (max_x - min_x).max(0.0) * (max_y - min_y).max(0.0)
}

/// Returns `true` if the rectangle contains the given point.
pub fn rect_contains_point(rect: &Rect<f32>, point: &Coord<f32>) -> bool {
    let min = rect.min();
    let max = rect.max();
    point.x >= min.x && point.x <= max.x && point.y >= min.y && point.y <= max.y
}

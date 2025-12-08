use anyhow::Result;
use geo_traits::{
    CoordTrait, GeometryCollectionTrait, GeometryTrait, GeometryType, LineStringTrait,
    MultiLineStringTrait, MultiPointTrait, MultiPolygonTrait, PointTrait, PolygonTrait,
};

use crate::spatial::geometry::interval::Interval;

/// Update a pair of intervals for x and y bounds
///
/// Useful for updating bounds in-place when accumulating
/// bounds for statistics or function implementations.
pub fn geo_traits_update_xy_bounds(
    geom: impl GeometryTrait<T = f64>, x: &mut Interval, y: &mut Interval,
) -> Result<()> {
    match geom.as_type() {
        GeometryType::Point(pt) => {
            if let Some(coord) = PointTrait::coord(pt) {
                x.update_value(coord.x());
                y.update_value(coord.y());
            }
        }
        GeometryType::LineString(ls) => {
            for coord in ls.coords() {
                x.update_value(coord.x());
                y.update_value(coord.y());
            }
        }
        GeometryType::Polygon(pl) => {
            if let Some(exterior) = pl.exterior() {
                for coord in exterior.coords() {
                    x.update_value(coord.x());
                    y.update_value(coord.y());
                }
            }

            for interior in pl.interiors() {
                for coord in interior.coords() {
                    x.update_value(coord.x());
                    y.update_value(coord.y());
                }
            }
        }
        GeometryType::MultiPoint(multi_pt) => {
            for pt in multi_pt.points() {
                geo_traits_update_xy_bounds(pt, x, y)?;
            }
        }
        GeometryType::MultiLineString(multi_ls) => {
            for ls in multi_ls.line_strings() {
                geo_traits_update_xy_bounds(ls, x, y)?;
            }
        }
        GeometryType::MultiPolygon(multi_pl) => {
            for pl in multi_pl.polygons() {
                geo_traits_update_xy_bounds(pl, x, y)?;
            }
        }
        GeometryType::GeometryCollection(collection) => {
            for geom in collection.geometries() {
                geo_traits_update_xy_bounds(geom, x, y)?;
            }
        }
        _ => {
            return Err(anyhow::anyhow!(
                "GeometryType not supported for XY bounds".to_owned(),
            ));
        }
    }

    Ok(())
}

use geo_traits::{
    GeometryCollectionTrait, GeometryTrait, GeometryType, LineStringTrait, MultiLineStringTrait,
    MultiPointTrait, MultiPolygonTrait, PointTrait, PolygonTrait,
};

/// Counts the number of points in a geometry
pub fn count_points<G: GeometryTrait>(geom: &G) -> i64 {
    match geom.as_type() {
        GeometryType::Point(pt) => PointTrait::coord(pt).is_some() as i64,
        GeometryType::MultiPoint(mp) => mp.num_points() as i64,
        GeometryType::LineString(ls) => ls.num_coords() as i64,
        GeometryType::Line(_) => 2,
        GeometryType::Polygon(poly) => {
            let mut count = 0;
            if let Some(exterior) = poly.exterior() {
                count += exterior.num_coords();
            }
            for interior in poly.interiors() {
                count += interior.num_coords();
            }
            count as i64
        }
        GeometryType::MultiLineString(mls) => {
            let mut count = 0;
            for i in 0..mls.num_line_strings() {
                if let Some(ls) = mls.line_string(i) {
                    count += ls.num_coords();
                }
            }
            count as i64
        }
        GeometryType::MultiPolygon(mp) => {
            let mut count = 0;
            for i in 0..mp.num_polygons() {
                if let Some(poly) = mp.polygon(i) {
                    if let Some(exterior) = poly.exterior() {
                        count += exterior.num_coords();
                    }
                    for interior in poly.interiors() {
                        count += interior.num_coords();
                    }
                }
            }
            count as i64
        }
        GeometryType::GeometryCollection(gc) => {
            let mut count = 0;
            for g in gc.geometries() {
                count += count_points(&g);
            }
            count
        }
        _ => 0,
    }
}

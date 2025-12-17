use datafusion::common::{Result, not_impl_err};
use geo_traits::GeometryType::{
    GeometryCollection, LineString, MultiLineString, MultiPoint, MultiPolygon, Point, Polygon,
};
use geo_traits::to_geo::{
    ToGeoLineString, ToGeoMultiLineString, ToGeoMultiPoint, ToGeoMultiPolygon, ToGeoPoint,
    ToGeoPolygon,
};
use geo_traits::{GeometryCollectionTrait, GeometryTrait};
use geo_types::Geometry;

/// Convert a [`GeometryTrait`] into a [Geometry]
///
/// This implementation avoid issues with some versions of the Rust compiler in
/// release mode. Note that [Geometry] does not support all valid
/// [`GeometryTrait`] objects (notably: the empty point and a multipoint with an
/// empty child).
///
/// This implementation does not currently support arbitrarily recursive
/// `GeometryCollections` (the recursion for which is the reason some versions
/// of the Rust compiler fail to compile the version of this function in the
/// geo-traits crate). This implementation limits the recursion to 1 level deep
/// (e.g., GEOMETRYCOLLECTION (...)).
pub fn item_to_geometry(geo: impl GeometryTrait<T = f64>) -> Result<Geometry> {
    if let Some(geo) = to_geometry(geo) {
        Ok(geo)
    } else {
        not_impl_err!(
            "geo kernel implementation on {}, {}, or {} not supported",
            "MULTIPOINT with EMPTY child",
            "POINT EMPTY",
            "GEOMETRYCOLLECTION"
        )
    }
}

// GeometryCollection causes issues because it has a recursive definition and
// won't work with cargo run --release. Thus, we need our own version of this
// that works around this problem by processing GeometryCollection using a free
// function instead of relying on trait resolver.
// See also https://github.com/geoarrow/geoarrow-rs/pull/956.
fn to_geometry(item: impl GeometryTrait<T = f64>) -> Option<Geometry> {
    match item.as_type() {
        Point(geom) => geom.try_to_point().map(Geometry::Point),
        LineString(geom) => Some(Geometry::LineString(geom.to_line_string())),
        Polygon(geom) => Some(Geometry::Polygon(geom.to_polygon())),
        MultiPoint(geom) => geom.try_to_multi_point().map(Geometry::MultiPoint),
        MultiLineString(geom) => Some(Geometry::MultiLineString(geom.to_multi_line_string())),
        MultiPolygon(geom) => Some(Geometry::MultiPolygon(geom.to_multi_polygon())),
        GeometryCollection(geom) => geometry_collection_to_geometry(geom),
        _ => None,
    }
}

fn geometry_collection_to_geometry<GC: GeometryCollectionTrait<T = f64>>(
    geom: &GC,
) -> Option<Geometry> {
    let geometries = geom
        .geometries()
        .filter_map(|child| {
            match child.as_type() {
                Point(geom) => geom.try_to_point().map(Geometry::Point),
                LineString(geom) => Some(Geometry::LineString(geom.to_line_string())),
                Polygon(geom) => Some(Geometry::Polygon(geom.to_polygon())),
                MultiPoint(geom) => geom.try_to_multi_point().map(Geometry::MultiPoint),
                MultiLineString(geom) => {
                    Some(Geometry::MultiLineString(geom.to_multi_line_string()))
                }
                MultiPolygon(geom) => Some(Geometry::MultiPolygon(geom.to_multi_polygon())),
                GeometryCollection(geom) => geometry_collection_to_geometry(geom),
                _ => None,
            }
        })
        .collect::<Vec<_>>();

    // If any child conversions failed, also return None
    if geometries.len() != geom.num_geometries() {
        return None;
    }

    Some(Geometry::GeometryCollection(geo_types::GeometryCollection(
        geometries,
    )))
}

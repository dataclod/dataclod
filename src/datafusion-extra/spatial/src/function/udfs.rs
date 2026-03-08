use datafusion_udf_macros::{StructType, function};
use geos::{CoordSeq, Geom, Geometry};
use geozero::wkb::Ewkb;
use geozero::{CoordDimensions, ToGeo, ToWkb};
use lwgeom::LWGeom;
use tg_geom::Geom as TgGeom;

use crate::geo_ext::GeoExt;
use crate::geos_ext::GeosExt;

#[function("ST_Area(Geometry) -> Float64")]
pub fn st_area(wkb: Option<&[u8]>) -> Option<f64> {
    Geometry::new_from_wkb(wkb?).ok()?.area().ok()
}

#[function("ST_Length(Geometry) -> Float64")]
pub fn st_length(wkb: Option<&[u8]>) -> Option<f64> {
    Geometry::new_from_wkb(wkb?).ok()?.length().ok()
}

#[function("ST_X(Geometry) -> Float64")]
pub fn st_x(wkb: Option<&[u8]>) -> Option<f64> {
    Geometry::new_from_wkb(wkb?).ok()?.get_x().ok()
}

#[function("ST_Y(Geometry) -> Float64")]
pub fn st_y(wkb: Option<&[u8]>) -> Option<f64> {
    Geometry::new_from_wkb(wkb?).ok()?.get_y().ok()
}

#[function("ST_XMin(Geometry) -> Float64")]
pub fn st_xmin(wkb: Option<&[u8]>) -> Option<f64> {
    Geometry::new_from_wkb(wkb?).ok()?.get_x_min().ok()
}

#[function("ST_XMax(Geometry) -> Float64")]
pub fn st_xmax(wkb: Option<&[u8]>) -> Option<f64> {
    Geometry::new_from_wkb(wkb?).ok()?.get_x_max().ok()
}

#[function("ST_YMin(Geometry) -> Float64")]
pub fn st_ymin(wkb: Option<&[u8]>) -> Option<f64> {
    Geometry::new_from_wkb(wkb?).ok()?.get_y_min().ok()
}

#[function("ST_YMax(Geometry) -> Float64")]
pub fn st_ymax(wkb: Option<&[u8]>) -> Option<f64> {
    Geometry::new_from_wkb(wkb?).ok()?.get_y_max().ok()
}

#[function("ST_IsEmpty(Geometry) -> Boolean")]
pub fn st_isempty(wkb: Option<&[u8]>) -> Option<bool> {
    Geometry::new_from_wkb(wkb?).ok()?.is_empty().ok()
}

#[function("ST_IsValid(Geometry) -> Boolean")]
pub fn st_isvalid(wkb: Option<&[u8]>) -> Option<bool> {
    Geometry::new_from_wkb(wkb?).ok()?.is_valid().ok()
}

#[function("ST_NPoints(Geometry) -> UInt64")]
pub fn st_npoints(wkb: Option<&[u8]>) -> Option<u64> {
    Geometry::new_from_wkb(wkb?)
        .ok()?
        .get_num_coordinates()
        .ok()
        .map(|n| n as u64)
}

#[function("ST_SRID(Geometry) -> UInt64")]
pub fn st_srid(wkb: Option<&[u8]>) -> Option<u64> {
    Geometry::new_from_wkb(wkb?)
        .ok()?
        .get_srid()
        .ok()
        .map(|s| s as u64)
}

#[function("ST_NumPoints(Geometry) -> UInt64")]
pub fn st_numpoints(wkb: Option<&[u8]>) -> Option<u64> {
    Geometry::new_from_wkb(wkb?)
        .ok()?
        .get_num_points()
        .ok()
        .map(|n| n as u64)
}

#[function("ST_AsText(Geometry) -> Utf8View")]
pub fn st_astext(wkb: Option<&[u8]>) -> Option<String> {
    Geometry::new_from_wkb(wkb?).ok()?.as_text().ok()
}

#[function("ST_AsEWKT(Geometry) -> Utf8View")]
pub fn st_asewkt(wkb: Option<&[u8]>) -> Option<String> {
    Geometry::new_from_wkb(wkb?).ok()?.as_ewkt().ok()
}

#[function("ST_AsGeoJSON(Geometry) -> Utf8View")]
pub fn st_asgeojson(wkb: Option<&[u8]>) -> Option<String> {
    Geometry::new_from_wkb(wkb?).ok()?.to_geojson().ok()
}

#[function("ST_GeometryType(Geometry) -> Utf8View")]
pub fn st_geometrytype(wkb: Option<&[u8]>) -> Option<String> {
    Geometry::new_from_wkb(wkb?).ok()?.get_type().ok()
}

#[function("ST_Centroid(Geometry) -> Geometry")]
pub fn st_centroid(wkb: Option<&[u8]>) -> Option<Vec<u8>> {
    Geometry::new_from_wkb(wkb?)
        .ok()?
        .get_centroid()
        .ok()?
        .to_ewkb()
        .ok()
}

#[function("ST_ConvexHull(Geometry) -> Geometry")]
pub fn st_convexhull(wkb: Option<&[u8]>) -> Option<Vec<u8>> {
    Geometry::new_from_wkb(wkb?)
        .ok()?
        .convex_hull()
        .ok()?
        .to_ewkb()
        .ok()
}

#[function("ST_Envelope(Geometry) -> Geometry")]
pub fn st_envelope(wkb: Option<&[u8]>) -> Option<Vec<u8>> {
    Geometry::new_from_wkb(wkb?)
        .ok()?
        .envelope()
        .ok()?
        .to_ewkb()
        .ok()
}

#[function("ST_Boundary(Geometry) -> Geometry")]
pub fn st_boundary(wkb: Option<&[u8]>) -> Option<Vec<u8>> {
    Geometry::new_from_wkb(wkb?)
        .ok()?
        .boundary()
        .ok()?
        .to_ewkb()
        .ok()
}

#[function("ST_MakeValid(Geometry) -> Geometry")]
pub fn st_makevalid(wkb: Option<&[u8]>) -> Option<Vec<u8>> {
    Geometry::new_from_wkb(wkb?)
        .ok()?
        .make_valid()
        .ok()?
        .to_ewkb()
        .ok()
}

#[function("ST_BuildArea(Geometry) -> Geometry")]
pub fn st_buildarea(wkb: Option<&[u8]>) -> Option<Vec<u8>> {
    Geometry::new_from_wkb(wkb?)
        .ok()?
        .build_area()
        .ok()?
        .to_ewkb()
        .ok()
}

#[function("ST_Contains(Geometry, Geometry) -> Boolean")]
pub fn st_contains(wkb1: Option<&[u8]>, wkb2: Option<&[u8]>) -> Option<bool> {
    Some(
        TgGeom::from_wkb(wkb1?)
            .ok()?
            .contains(&TgGeom::from_wkb(wkb2?).ok()?),
    )
}

#[function("ST_Intersects(Geometry, Geometry) -> Boolean")]
pub fn st_intersects(wkb1: Option<&[u8]>, wkb2: Option<&[u8]>) -> Option<bool> {
    Some(
        TgGeom::from_wkb(wkb1?)
            .ok()?
            .intersects(&TgGeom::from_wkb(wkb2?).ok()?),
    )
}

#[function("ST_Within(Geometry, Geometry) -> Boolean")]
pub fn st_within(wkb1: Option<&[u8]>, wkb2: Option<&[u8]>) -> Option<bool> {
    Some(
        TgGeom::from_wkb(wkb1?)
            .ok()?
            .within(&TgGeom::from_wkb(wkb2?).ok()?),
    )
}

#[function("ST_Equals(Geometry, Geometry) -> Boolean")]
pub fn st_equals(wkb1: Option<&[u8]>, wkb2: Option<&[u8]>) -> Option<bool> {
    Geometry::new_from_wkb(wkb1?)
        .ok()?
        .equals(&Geometry::new_from_wkb(wkb2?).ok()?)
        .ok()
}

#[function("ST_Covers(Geometry, Geometry) -> Boolean")]
pub fn st_covers(wkb1: Option<&[u8]>, wkb2: Option<&[u8]>) -> Option<bool> {
    Some(
        TgGeom::from_wkb(wkb1?)
            .ok()?
            .covers(&TgGeom::from_wkb(wkb2?).ok()?),
    )
}

#[function("ST_CoveredBy(Geometry, Geometry) -> Boolean")]
pub fn st_coveredby(wkb1: Option<&[u8]>, wkb2: Option<&[u8]>) -> Option<bool> {
    Some(
        TgGeom::from_wkb(wkb1?)
            .ok()?
            .coveredby(&TgGeom::from_wkb(wkb2?).ok()?),
    )
}

#[function("ST_Touches(Geometry, Geometry) -> Boolean")]
pub fn st_touches(wkb1: Option<&[u8]>, wkb2: Option<&[u8]>) -> Option<bool> {
    Some(
        TgGeom::from_wkb(wkb1?)
            .ok()?
            .touches(&TgGeom::from_wkb(wkb2?).ok()?),
    )
}

#[function("ST_Overlaps(Geometry, Geometry) -> Boolean")]
pub fn st_overlaps(wkb1: Option<&[u8]>, wkb2: Option<&[u8]>) -> Option<bool> {
    Geometry::new_from_wkb(wkb1?)
        .ok()?
        .overlaps(&Geometry::new_from_wkb(wkb2?).ok()?)
        .ok()
}

#[function("BBox_Intersects(Geometry, Geometry) -> Boolean")]
pub fn bbox_intersects(wkb1: Option<&[u8]>, wkb2: Option<&[u8]>) -> Option<bool> {
    Some(
        TgGeom::from_wkb(wkb1?)
            .ok()?
            .rect()
            .intersects_rect(TgGeom::from_wkb(wkb2?).ok()?.rect()),
    )
}

#[function("ST_Distance(Geometry, Geometry) -> Float64")]
pub fn st_distance(wkb1: Option<&[u8]>, wkb2: Option<&[u8]>) -> Option<f64> {
    Geometry::new_from_wkb(wkb1?)
        .ok()?
        .distance(&Geometry::new_from_wkb(wkb2?).ok()?)
        .ok()
}

#[function("ST_Intersection(Geometry, Geometry) -> Geometry")]
pub fn st_intersection(wkb1: Option<&[u8]>, wkb2: Option<&[u8]>) -> Option<Vec<u8>> {
    Geometry::new_from_wkb(wkb1?)
        .ok()?
        .intersection(&Geometry::new_from_wkb(wkb2?).ok()?)
        .ok()?
        .to_ewkb()
        .ok()
}

#[function("ST_Difference(Geometry, Geometry) -> Geometry")]
pub fn st_difference(wkb1: Option<&[u8]>, wkb2: Option<&[u8]>) -> Option<Vec<u8>> {
    Geometry::new_from_wkb(wkb1?)
        .ok()?
        .difference(&Geometry::new_from_wkb(wkb2?).ok()?)
        .ok()?
        .to_ewkb()
        .ok()
}

#[function("ST_Union(Geometry, Geometry) -> Geometry")]
pub fn st_union(wkb1: Option<&[u8]>, wkb2: Option<&[u8]>) -> Option<Vec<u8>> {
    Geometry::new_from_wkb(wkb1?)
        .ok()?
        .union(&Geometry::new_from_wkb(wkb2?).ok()?)
        .ok()?
        .to_ewkb()
        .ok()
}

#[function("ST_Split(Geometry, Geometry) -> Geometry")]
pub fn st_split(wkb1: Option<&[u8]>, wkb2: Option<&[u8]>) -> Option<Vec<u8>> {
    LWGeom::from_ewkb(wkb1?)
        .ok()?
        .split(&LWGeom::from_ewkb(wkb2?).ok()?)
        .as_ewkb()
        .ok()
}

#[derive(StructType)]
struct Box2D {
    xmin: f64,
    ymin: f64,
    xmax: f64,
    ymax: f64,
}

impl TryFrom<&Geometry> for Box2D {
    type Error = geos::Error;

    fn try_from(geom: &Geometry) -> Result<Self, Self::Error> {
        Ok(Self {
            xmin: geom.get_x_min()?,
            ymin: geom.get_y_min()?,
            xmax: geom.get_x_max()?,
            ymax: geom.get_y_max()?,
        })
    }
}

#[function("Box2D(Geometry) -> Struct Box2D")]
pub fn box2d(wkb: Option<&[u8]>) -> Option<Box2D> {
    Box2D::try_from(&Geometry::new_from_wkb(wkb?).ok()?).ok()
}

#[function("ST_Buffer(Geometry, Float64) -> Geometry")]
#[function("ST_Buffer(Geometry, Float64, Int64) -> Geometry")]
pub fn st_buffer(
    wkb: Option<&[u8]>, radius: Option<f64>, quadsegs: Option<i64>,
) -> Option<Vec<u8>> {
    Geometry::new_from_wkb(wkb?)
        .ok()?
        .buffer(radius?, quadsegs.unwrap_or(8) as i32)
        .ok()?
        .to_ewkb()
        .ok()
}

#[function("ST_Simplify(Geometry, Float64) -> Geometry")]
pub fn st_simplify(wkb: Option<&[u8]>, tolerance: Option<f64>) -> Option<Vec<u8>> {
    Geometry::new_from_wkb(wkb?)
        .ok()?
        .simplify(tolerance?)
        .ok()?
        .to_ewkb()
        .ok()
}

#[function("ST_SimplifyPreserveTopology(Geometry, Float64) -> Geometry")]
pub fn st_simplifypreservetopology(wkb: Option<&[u8]>, tolerance: Option<f64>) -> Option<Vec<u8>> {
    Geometry::new_from_wkb(wkb?)
        .ok()?
        .topology_preserve_simplify(tolerance?)
        .ok()?
        .to_ewkb()
        .ok()
}

#[function("ST_SimplifyVW(Geometry, Float64) -> Geometry")]
pub fn st_simplifyvw(wkb: Option<&[u8]>, tolerance: Option<f64>) -> Option<Vec<u8>> {
    let wkb = wkb?;
    let tolerance = tolerance?;
    let geos_geom = Geometry::new_from_wkb(wkb).ok()?;
    let geo_geom = Ewkb(wkb.to_vec()).to_geo().ok()?;
    geo_geom.st_simplifyvw(tolerance).and_then(|geom| {
        geom.to_ewkb(CoordDimensions::xy(), geos_geom.get_srid().ok())
            .ok()
    })
}

#[function("ST_SetSRID(Geometry, Int64) -> Geometry")]
pub fn st_setsrid(wkb: Option<&[u8]>, srid: Option<i64>) -> Option<Vec<u8>> {
    let mut geom = Geometry::new_from_wkb(wkb?).ok()?;
    geom.set_srid(srid? as i32);
    geom.to_ewkb().ok()
}

#[function("ST_DWithin(Geometry, Geometry, Float64) -> Boolean")]
pub fn st_dwithin(wkb1: Option<&[u8]>, wkb2: Option<&[u8]>, distance: Option<f64>) -> Option<bool> {
    Geometry::new_from_wkb(wkb1?)
        .ok()?
        .dwithin(&Geometry::new_from_wkb(wkb2?).ok()?, distance?)
        .ok()
}

#[function("ST_Translate(Geometry, Float64, Float64) -> Geometry")]
pub fn st_translate(wkb: Option<&[u8]>, dx: Option<f64>, dy: Option<f64>) -> Option<Vec<u8>> {
    Geometry::new_from_wkb(wkb?)
        .ok()?
        .st_translate(dx?, dy?)
        .ok()?
        .to_ewkb()
        .ok()
}

#[function("ST_ConcaveHull(Geometry, Float64) -> Geometry")]
#[function("ST_ConcaveHull(Geometry, Float64, Boolean) -> Geometry")]
pub fn st_concavehull(
    wkb: Option<&[u8]>, pctconvex: Option<f64>, allow_holes: Option<bool>,
) -> Option<Vec<u8>> {
    Geometry::new_from_wkb(wkb?)
        .ok()?
        .concave_hull(pctconvex?, allow_holes.unwrap_or(false))
        .ok()?
        .to_ewkb()
        .ok()
}

#[function("ST_MakePoint(Float64, Float64) -> Geometry")]
pub fn st_makepoint(x: Option<f64>, y: Option<f64>) -> Option<Vec<u8>> {
    CoordSeq::new_from_vec(&[&[x?, y?]])
        .ok()?
        .create_point()
        .ok()?
        .to_ewkb()
        .ok()
}

#[function("ST_MakeEnvelope(Float64, Float64, Float64, Float64) -> Geometry")]
#[function("ST_MakeEnvelope(Float64, Float64, Float64, Float64, Int64) -> Geometry")]
pub fn st_makeenvelope(
    xmin: Option<f64>, ymin: Option<f64>, xmax: Option<f64>, ymax: Option<f64>, srid: Option<i64>,
) -> Option<Vec<u8>> {
    let mut geom = Geometry::make_envelope(xmin?, ymin?, xmax?, ymax?).ok()?;
    if let Some(srid) = srid {
        geom.set_srid(srid as i32);
    }
    geom.to_ewkb().ok()
}

#[function("ST_TileEnvelope(Int64, Int64, Int64) -> Geometry")]
#[function("ST_TileEnvelope(Int64, Int64, Int64, Geometry) -> Geometry")]
#[function("ST_TileEnvelope(Int64, Int64, Int64, Geometry, Float64) -> Geometry")]
pub fn st_tileenvelope(
    zoom: Option<i64>, x: Option<i64>, y: Option<i64>, bounds: Option<&[u8]>, margin: Option<f64>,
) -> Option<Vec<u8>> {
    let bounds = bounds.and_then(|wkb| LWGeom::from_ewkb(wkb).ok());
    LWGeom::tile_envelope(zoom? as i32, x? as i32, y? as i32, bounds.as_ref(), margin)
        .ok()?
        .as_ewkb()
        .ok()
}

#[function("ST_GeomFromText(Utf8View) -> Geometry")]
#[function("ST_GeomFromText(Utf8View, Int64) -> Geometry")]
pub fn st_geomfromtext(wkt: Option<&str>, srid: Option<i64>) -> Option<Vec<u8>> {
    let mut geom = Geometry::new_from_wkt(wkt?).ok()?;
    if let Some(srid) = srid {
        geom.set_srid(srid as i32);
    }
    geom.to_ewkb().ok()
}

#[function("ST_GeomFromEWKT(Utf8View) -> Geometry")]
pub fn st_geomfromewkt(ewkt: Option<&str>) -> Option<Vec<u8>> {
    Geometry::new_from_ewkt(ewkt?).ok()?.to_ewkb().ok()
}

#[function("ST_GeomFromWKB(BinaryView) -> Geometry")]
#[function("ST_GeomFromWKB(BinaryView, Int64) -> Geometry")]
pub fn st_geomfromwkb(wkb: Option<&[u8]>, srid: Option<i64>) -> Option<Vec<u8>> {
    let mut geom = Geometry::new_from_wkb(wkb?).ok()?;
    if let Some(srid) = srid {
        geom.set_srid(srid as i32);
    }
    geom.to_ewkb().ok()
}

#[function("ST_GeomFromEWKB(BinaryView) -> Geometry")]
pub fn st_geomfromewkb(ewkb: Option<&[u8]>) -> Option<Vec<u8>> {
    Geometry::new_from_wkb(ewkb?).ok()?.to_ewkb().ok()
}

#[function("ST_GeomFromGeoJSON(Utf8View) -> Geometry")]
pub fn st_geomfromgeojson(geojson: Option<&str>) -> Option<Vec<u8>> {
    Geometry::new_from_geojson(geojson?).ok()?.to_ewkb().ok()
}

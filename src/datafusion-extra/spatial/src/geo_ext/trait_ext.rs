use geo::{Geodesic, Geometry, InterpolatePoint, SimplifyVw};
use geo_types::{Coord, LineString, MultiLineString, MultiPolygon, Point, Polygon};

pub trait GeoExt {
    fn st_simplifyvw(&self, concavity: f64) -> Option<Geometry>;
    fn st_geodesic_segmentize(&self, max_distance: f64) -> Option<Geometry>;
}

impl GeoExt for Geometry {
    fn st_simplifyvw(&self, tolerance: f64) -> Option<Geometry> {
        match self {
            Geometry::LineString(line_string) => Some(line_string.simplify_vw(tolerance).into()),
            Geometry::Polygon(polygon) => Some(polygon.simplify_vw(tolerance).into()),
            Geometry::MultiLineString(multi_line_string) => {
                Some(multi_line_string.simplify_vw(tolerance).into())
            }
            Geometry::MultiPolygon(multi_polygon) => {
                Some(multi_polygon.simplify_vw(tolerance).into())
            }
            _ => None,
        }
    }

    fn st_geodesic_segmentize(&self, max_distance: f64) -> Option<Geometry> {
        if !max_distance.is_finite() || max_distance <= 0.0 {
            return None;
        }

        match self {
            Geometry::LineString(line_string) => {
                Some(Geometry::LineString(densify_line_string(
                    line_string,
                    max_distance,
                )))
            }
            Geometry::Polygon(polygon) => {
                Some(Geometry::Polygon(densify_polygon(polygon, max_distance)))
            }
            Geometry::MultiPolygon(multi_polygon) => {
                let polys = multi_polygon
                    .0
                    .iter()
                    .map(|poly| densify_polygon(poly, max_distance))
                    .collect();
                Some(Geometry::MultiPolygon(MultiPolygon(polys)))
            }
            Geometry::MultiLineString(multi_line_string) => {
                let lines = multi_line_string
                    .0
                    .iter()
                    .map(|line| densify_line_string(line, max_distance))
                    .collect();
                Some(Geometry::MultiLineString(MultiLineString(lines)))
            }
            _ => None,
        }
    }
}

fn densify_polygon(polygon: &Polygon<f64>, max_distance: f64) -> Polygon<f64> {
    let mut exterior = densify_line_string(polygon.exterior(), max_distance);
    ensure_closed(&mut exterior);

    let interiors = polygon
        .interiors()
        .iter()
        .map(|ring| {
            let mut densified = densify_line_string(ring, max_distance);
            ensure_closed(&mut densified);
            densified
        })
        .collect();

    Polygon::new(exterior, interiors)
}

fn densify_line_string(line_string: &LineString<f64>, max_distance: f64) -> LineString<f64> {
    if line_string.0.len() < 2 {
        return line_string.clone();
    }

    let mut coords = Vec::with_capacity(line_string.0.len());
    for (idx, segment) in line_string.0.windows(2).enumerate() {
        let start = segment[0];
        let end = segment[1];
        if idx == 0 {
            coords.push(start);
        }

        let start_point = Point::new(start.x, start.y);
        let end_point = Point::new(end.x, end.y);
        let intermediates = Geodesic.points_along_line(start_point, end_point, max_distance, false);
        for point in intermediates {
            coords.push(Coord {
                x: point.x(),
                y: point.y(),
            });
        }
        coords.push(end);
    }

    LineString(coords)
}

fn ensure_closed(line_string: &mut LineString<f64>) {
    if let (Some(first), Some(last)) = (line_string.0.first().cloned(), line_string.0.last())
        && first != *last
    {
        line_string.0.push(first);
    }
}

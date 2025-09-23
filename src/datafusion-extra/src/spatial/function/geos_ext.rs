use geos::{CoordSeq, GResult, Geom, Geometry, WKBWriter, WKTWriter};
use proj4rs::Proj;
use proj4rs::adaptors::transform_xy;

#[allow(dead_code)]
pub trait GeosExt: Geom {
    fn to_ewkb(&self) -> GResult<Vec<u8>>;
    fn as_text(&self) -> GResult<String>;
    fn as_ewkt(&self) -> GResult<String>;
    fn new_from_ewkt(ewkt: &str) -> GResult<Geometry>;

    fn make_envelope(xmin: f64, ymin: f64, xmax: f64, ymax: f64) -> GResult<Geometry>;

    fn st_transform(&self, src_proj: &Proj, dst_proj: &Proj) -> GResult<Geometry>;
    fn st_translate(&self, x_offset: f64, y_offset: f64) -> GResult<Geometry>;

    fn bbox_overlaps<G: GeosExt>(&self, other: &G) -> GResult<bool>;
    fn bbox_contains<G: GeosExt>(&self, other: &G) -> GResult<bool>;
    fn bbox_same<G: GeosExt>(&self, other: &G) -> GResult<bool>;

    fn st_intersects<G: GeosExt>(&self, other: &G) -> Option<bool>;
    fn st_contains<G: GeosExt>(&self, other: &G) -> Option<bool>;
    fn st_coveredby<G: GeosExt>(&self, other: &G) -> Option<bool>;
    fn st_covers<G: GeosExt>(&self, other: &G) -> Option<bool>;
    fn st_equals<G: GeosExt>(&self, other: &G) -> Option<bool>;
    fn st_overlaps<G: GeosExt>(&self, other: &G) -> Option<bool>;
    fn st_touches<G: GeosExt>(&self, other: &G) -> Option<bool>;
    fn st_within<G: GeosExt>(&self, other: &G) -> Option<bool>;
}

impl GeosExt for Geometry {
    fn to_ewkb(&self) -> GResult<Vec<u8>> {
        let mut writer = WKBWriter::new()?;
        writer.set_include_SRID(true);
        writer.write_wkb(self)
    }

    fn as_text(&self) -> GResult<String> {
        WKTWriter::new().and_then(|mut w| {
            w.set_trim(true);
            w.write(self)
        })
    }

    fn as_ewkt(&self) -> GResult<String> {
        WKTWriter::new()
            .and_then(|mut w| {
                w.set_trim(true);
                w.write(self)
            })
            .map(|s| {
                if let Ok(srid) = self.get_srid() {
                    format!("SRID={srid};{s}")
                } else {
                    s
                }
            })
    }

    fn new_from_ewkt(ewkt: &str) -> GResult<Geometry> {
        let parts: Vec<&str> = ewkt.split(';').collect();
        let parsed_srid: Option<libc::c_int> = if ewkt.starts_with("SRID=") && parts.len() == 2 {
            parts[0].replace("SRID=", "").parse().ok()
        } else {
            None
        };
        let geom_part = if parts.len() == 2 { parts[1] } else { parts[0] };

        let mut geom = Geometry::new_from_wkt(geom_part)?;
        if let Some(srid) = parsed_srid {
            geom.set_srid(srid)
        }

        Ok(geom)
    }

    fn make_envelope(xmin: f64, ymin: f64, xmax: f64, ymax: f64) -> GResult<Geometry> {
        CoordSeq::new_from_vec(&[
            &[xmin, ymin],
            &[xmin, ymax],
            &[xmax, ymax],
            &[xmax, ymin],
            &[xmin, ymin],
        ])
        .and_then(Geometry::create_linear_ring)
        .and_then(|exterior| Geometry::create_polygon(exterior, Vec::new()))
    }

    fn st_transform(&self, src_proj: &Proj, dst_proj: &Proj) -> GResult<Geometry> {
        self.transform_xy(|x, y| {
            let (new_x, new_y) = if src_proj.is_latlong() {
                (x.to_radians(), y.to_radians())
            } else {
                (x, y)
            };

            let (new_x, new_y) = transform_xy(src_proj, dst_proj, new_x, new_y)
                .map_err(|e| geos::Error::GenericError(e.to_string()))?;
            if dst_proj.is_latlong() {
                Ok((new_x.to_degrees(), new_y.to_degrees()))
            } else {
                Ok((new_x, new_y))
            }
        })
    }

    fn st_translate(&self, delta_x: f64, delta_y: f64) -> GResult<Geometry> {
        self.transform_xy(|x, y| Ok((x + delta_x, y + delta_y)))
    }

    fn bbox_overlaps<G: GeosExt>(&self, other: &G) -> GResult<bool> {
        if self.get_x_max()? < other.get_x_min()?
            || self.get_y_max()? < other.get_y_min()?
            || self.get_x_min()? > other.get_x_max()?
            || self.get_y_min()? > other.get_y_max()?
        {
            Ok(false)
        } else {
            Ok(true)
        }
    }

    fn bbox_contains<G: GeosExt>(&self, other: &G) -> GResult<bool> {
        if self.get_x_max()? < other.get_x_max()?
            || self.get_y_max()? < other.get_y_max()?
            || self.get_x_min()? > other.get_x_min()?
            || self.get_y_min()? > other.get_y_min()?
        {
            Ok(false)
        } else {
            Ok(true)
        }
    }

    fn bbox_same<G: GeosExt>(&self, other: &G) -> GResult<bool> {
        // TODO(xhwhis): approximately equal
        if self.get_x_max()? == other.get_x_max()?
            && self.get_y_max()? == other.get_y_max()?
            && self.get_x_min()? == other.get_x_min()?
            && self.get_y_min()? == other.get_y_min()?
        {
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn st_intersects<G: GeosExt>(&self, other: &G) -> Option<bool> {
        if let Ok(overlaps) = self.bbox_overlaps(other) {
            if overlaps {
                self.to_prepared_geom()
                    .and_then(|geom| geom.intersects(other))
                    .ok()
            } else {
                Some(false)
            }
        } else {
            None
        }
    }

    fn st_contains<G: GeosExt>(&self, other: &G) -> Option<bool> {
        if let Ok(contains) = self.bbox_contains(other) {
            if contains {
                self.to_prepared_geom()
                    .and_then(|geom| geom.contains(other))
                    .ok()
            } else {
                Some(false)
            }
        } else {
            None
        }
    }

    fn st_coveredby<G: GeosExt>(&self, other: &G) -> Option<bool> {
        if let Ok(contains) = other.bbox_contains(self) {
            if contains {
                self.to_prepared_geom()
                    .and_then(|geom| geom.covered_by(other))
                    .ok()
            } else {
                Some(false)
            }
        } else {
            None
        }
    }

    fn st_covers<G: GeosExt>(&self, other: &G) -> Option<bool> {
        if let Ok(contains) = other.bbox_contains(self) {
            if contains {
                self.to_prepared_geom()
                    .and_then(|geom| geom.covers(other))
                    .ok()
            } else {
                Some(false)
            }
        } else {
            None
        }
    }

    fn st_equals<G: GeosExt>(&self, other: &G) -> Option<bool> {
        if let Ok(same) = self.bbox_same(other) {
            if same {
                self.equals(other).ok()
            } else {
                Some(false)
            }
        } else {
            None
        }
    }

    fn st_overlaps<G: GeosExt>(&self, other: &G) -> Option<bool> {
        if let Ok(overlaps) = self.bbox_overlaps(other) {
            if overlaps {
                self.to_prepared_geom()
                    .and_then(|geom| geom.overlaps(other))
                    .ok()
            } else {
                Some(false)
            }
        } else {
            None
        }
    }

    fn st_touches<G: GeosExt>(&self, other: &G) -> Option<bool> {
        if let Ok(overlaps) = self.bbox_overlaps(other) {
            if overlaps {
                self.to_prepared_geom()
                    .and_then(|geom| geom.touches(other))
                    .ok()
            } else {
                Some(false)
            }
        } else {
            None
        }
    }

    fn st_within<G: GeosExt>(&self, other: &G) -> Option<bool> {
        if let Ok(contains) = other.bbox_contains(self) {
            if contains {
                self.to_prepared_geom()
                    .and_then(|geom| geom.within(other))
                    .ok()
            } else {
                Some(false)
            }
        } else {
            None
        }
    }
}

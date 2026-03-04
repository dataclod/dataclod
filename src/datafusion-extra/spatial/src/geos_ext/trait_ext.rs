use geos::{CoordSeq, GResult, Geom, Geometry, WKBWriter, WKTWriter};
use proj4rs::Proj;
use proj4rs::adaptors::transform_xy;

pub trait GeosExt {
    fn to_ewkb(&self) -> GResult<Vec<u8>>;
    fn as_text(&self) -> GResult<String>;
    fn as_ewkt(&self) -> GResult<String>;
    fn new_from_ewkt(ewkt: &str) -> GResult<Geometry>;

    fn make_envelope(xmin: f64, ymin: f64, xmax: f64, ymax: f64) -> GResult<Geometry>;

    fn st_transform(&self, src_proj: &Proj, dst_proj: &Proj) -> GResult<Geometry>;
    fn st_translate(&self, x_offset: f64, y_offset: f64) -> GResult<Geometry>;
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
        if ewkt.starts_with(['s', 'S']) {
            if let Some((srid_part, wkt_part)) = ewkt.split_once(';') {
                let srid = srid_part
                    .to_uppercase()
                    .strip_prefix("SRID")
                    .and_then(|srid_part| srid_part.trim().strip_prefix('='))
                    .and_then(|srid_part| srid_part.trim().parse().ok());
                if let Some(srid) = srid {
                    let mut geom = Geometry::new_from_wkt(wkt_part)?;
                    geom.set_srid(srid);
                    Ok(geom)
                } else {
                    Err(geos::Error::GenericError("invalid SRID".to_owned()))
                }
            } else {
                Err(geos::Error::GenericError("invalid SRID".to_owned()))
            }
        } else {
            Geometry::new_from_wkt(ewkt)
        }
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
}

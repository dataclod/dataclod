mod geom_from_ewkt;
mod geom_from_text;
mod geos_ext;
mod intersects;

use datafusion::prelude::SessionContext;

pub fn register_spatial_udfs(ctx: &SessionContext) {
    ctx.register_udf(intersects::st_intersects());
    ctx.register_udf(geom_from_ewkt::st_geomfromewkt());
    ctx.register_udf(geom_from_text::st_geomfromtext());
}

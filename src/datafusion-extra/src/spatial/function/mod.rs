mod intersects;

use datafusion::prelude::SessionContext;

pub fn register_spatial_udfs(ctx: &SessionContext) {
    ctx.register_udf(intersects::intersects());
}

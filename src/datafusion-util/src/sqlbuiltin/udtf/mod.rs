mod generate_series;

use std::sync::Arc;

use datafusion::execution::context::SessionContext;

pub fn register_udtf(ctx: &SessionContext) {
    ctx.register_udtf(
        "generate_series",
        Arc::new(generate_series::GenerateSeriesUDTF {}),
    );
}

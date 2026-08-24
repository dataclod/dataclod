mod st_asmvtgeom;
mod st_transform;
mod udfs;

use std::sync::Arc;

use datafusion::logical_expr::ScalarUDF;
use datafusion::prelude::SessionContext;

pub fn spatial_udfs() -> Vec<Arc<ScalarUDF>> {
    vec![
        Arc::new(udfs::STAreaUDF::udf()),
        Arc::new(udfs::STLengthUDF::udf()),
        Arc::new(udfs::STXUDF::udf()),
        Arc::new(udfs::STYUDF::udf()),
        Arc::new(udfs::STXMinUDF::udf()),
        Arc::new(udfs::STXMaxUDF::udf()),
        Arc::new(udfs::STYMinUDF::udf()),
        Arc::new(udfs::STYMaxUDF::udf()),
        Arc::new(udfs::STIsEmptyUDF::udf()),
        Arc::new(udfs::STIsValidUDF::udf()),
        Arc::new(udfs::STNPointsUDF::udf()),
        Arc::new(udfs::STSRIDUDF::udf()),
        Arc::new(udfs::STNumPointsUDF::udf()),
        Arc::new(udfs::STAsTextUDF::udf()),
        Arc::new(udfs::STAsEWKTUDF::udf()),
        Arc::new(udfs::STAsGeoJSONUDF::udf()),
        Arc::new(udfs::STGeometryTypeUDF::udf()),
        Arc::new(udfs::STCentroidUDF::udf()),
        Arc::new(udfs::STConvexHullUDF::udf()),
        Arc::new(udfs::STEnvelopeUDF::udf()),
        Arc::new(udfs::STBoundaryUDF::udf()),
        Arc::new(udfs::STMakeValidUDF::udf()),
        Arc::new(udfs::STBuildAreaUDF::udf()),
        Arc::new(udfs::STContainsUDF::udf()),
        Arc::new(udfs::STIntersectsUDF::udf()),
        Arc::new(udfs::STWithinUDF::udf()),
        Arc::new(udfs::STEqualsUDF::udf()),
        Arc::new(udfs::STCoversUDF::udf()),
        Arc::new(udfs::STCoveredByUDF::udf()),
        Arc::new(udfs::STTouchesUDF::udf()),
        Arc::new(udfs::STOverlapsUDF::udf()),
        Arc::new(udfs::BBoxIntersectsUDF::udf()),
        Arc::new(udfs::STDistanceUDF::udf()),
        Arc::new(udfs::STIntersectionUDF::udf()),
        Arc::new(udfs::STDifferenceUDF::udf()),
        Arc::new(udfs::STUnionUDF::udf()),
        Arc::new(udfs::STSplitUDF::udf()),
        Arc::new(udfs::Box2DUDF::udf()),
        Arc::new(udfs::STBufferUDF::udf()),
        Arc::new(udfs::STSimplifyUDF::udf()),
        Arc::new(udfs::STSimplifyPreserveTopologyUDF::udf()),
        Arc::new(udfs::STSimplifyVWUDF::udf()),
        Arc::new(udfs::STSetSRIDUDF::udf()),
        Arc::new(udfs::STDWithinUDF::udf()),
        Arc::new(udfs::STTranslateUDF::udf()),
        Arc::new(udfs::STConcaveHullUDF::udf()),
        Arc::new(udfs::STMakePointUDF::udf()),
        Arc::new(udfs::STMakeEnvelopeUDF::udf()),
        Arc::new(udfs::STTileEnvelopeUDF::udf()),
        Arc::new(udfs::STGeomFromTextUDF::udf()),
        Arc::new(udfs::STGeomFromEWKTUDF::udf()),
        Arc::new(udfs::STGeomFromWKBUDF::udf()),
        Arc::new(udfs::STGeomFromEWKBUDF::udf()),
        Arc::new(udfs::STGeomFromGeoJSONUDF::udf()),
        Arc::new(st_asmvtgeom::st_asmvtgeom()),
        Arc::new(st_transform::st_transform()),
    ]
}

pub fn register_spatial_udfs(ctx: &SessionContext) {
    for udf in spatial_udfs() {
        ctx.register_udf(udf.as_ref().clone());
    }
}

pub mod build_side_collector;
pub mod spatial_index;
pub mod spatial_index_builder;

use wkb::reader::Wkb;

/// The result of a spatial index query
pub struct IndexQueryResult<'a, 'b> {
    pub wkb: &'b Wkb<'a>,
    pub distance: Option<f64>,
    pub geom_idx: usize,
    pub position: (i32, i32),
}

/// The metrics for a spatial index query
#[derive(Debug)]
pub struct QueryResultMetrics {
    pub count: usize,
    pub candidate_count: usize,
}

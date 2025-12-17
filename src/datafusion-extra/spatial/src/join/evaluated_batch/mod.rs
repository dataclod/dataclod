pub mod stream;

use datafusion::arrow::array::RecordBatch;
use datafusion::logical_expr::ColumnarValue;
use geo::Rect;
use wkb::reader::Wkb;

use crate::join::operand_evaluator::EvaluatedGeometryArray;

/// `EvaluatedBatch` contains the original record batch from the input stream
/// and the evaluated geometry array.
pub struct EvaluatedBatch {
    /// Original record batch polled from the stream
    pub batch: RecordBatch,
    /// Evaluated geometry array, containing the geometry array containing
    /// geometries to be joined, rects of joined geometries, evaluated
    /// distance columnar values if we are running a distance join, etc.
    pub geom_array: EvaluatedGeometryArray,
}

impl EvaluatedBatch {
    pub fn in_mem_size(&self) -> usize {
        // NOTE: sometimes `geom_array` will reuse the memory of `batch`, especially
        // when the expression for evaluating the geometry is a simple column
        // reference. In this case, the in_mem_size will be overestimated. It is
        // a conservative estimation so there's no risk of running out of memory
        // because of underestimation.
        self.batch.get_array_memory_size() + self.geom_array.in_mem_size()
    }

    pub fn num_rows(&self) -> usize {
        self.batch.num_rows()
    }

    pub fn wkb(&self, idx: usize) -> Option<&Wkb<'_>> {
        let wkbs = self.geom_array.wkbs();
        wkbs[idx].as_ref()
    }

    pub fn rects(&self) -> &Vec<Option<Rect<f32>>> {
        &self.geom_array.rects
    }

    pub fn distance(&self) -> &Option<ColumnarValue> {
        &self.geom_array.distance
    }
}

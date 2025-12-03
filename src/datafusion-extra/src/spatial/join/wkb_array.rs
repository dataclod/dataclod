use datafusion::arrow::array::{Array, ArrayRef, BinaryArray};
use datafusion::common::cast::as_binary_array;
use datafusion::common::{DataFusionError, Result};
use wkb::reader::Wkb;

pub(crate) trait GetGeo {
    fn get_wkb(&self, row_idx: usize) -> Result<Option<Wkb<'_>>>;
}

impl GetGeo for ArrayRef {
    fn get_wkb(&self, row_idx: usize) -> Result<Option<Wkb<'_>>> {
        let byte_array = as_binary_array(self)?;
        if byte_array.is_null(row_idx) {
            return Ok(None);
        }
        let wkb = Wkb::try_new(byte_array.value(row_idx))
            .map_err(|e| DataFusionError::Internal(format!("Failed to parse WKB: {e}")))?;
        Ok(Some(wkb))
    }
}

/// Trait for indexed WKB access with minimal dispatch overhead
pub(crate) trait WkbArrayAccess: Send + Sync + std::fmt::Debug {
    /// Get the WKB item at the specified index, returns None if index is beyond
    /// bounds
    fn get_wkb_at(&self, index: usize) -> Result<Option<Wkb<'_>>>;

    /// Get the total number of items
    fn len(&self) -> usize;
}

/// Indexed access for Binary arrays (WKB)
#[derive(Debug)]
pub(crate) struct BinaryWkbAccess {
    array: BinaryArray,
}

impl BinaryWkbAccess {
    pub(crate) fn new(array: BinaryArray) -> Self {
        Self { array }
    }
}

impl WkbArrayAccess for BinaryWkbAccess {
    fn get_wkb_at(&self, index: usize) -> Result<Option<Wkb<'_>>> {
        if self.array.is_null(index) {
            return Ok(None);
        }

        let bytes = self.array.value(index);
        let geometry =
            wkb::reader::read_wkb(bytes).map_err(|err| DataFusionError::External(Box::new(err)))?;
        Ok(Some(geometry))
    }

    fn len(&self) -> usize {
        self.array.len()
    }
}

/// Create a WKB array accessor based on the datafabric type and array
pub(crate) fn create_wkb_array_access(array: &ArrayRef) -> Result<Box<dyn WkbArrayAccess>> {
    let binary_array = as_binary_array(array)?;
    Ok(Box::new(BinaryWkbAccess::new(binary_array.clone())))
}

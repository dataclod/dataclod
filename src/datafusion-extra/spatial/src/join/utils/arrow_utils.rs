use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayData, ArrayRef, BinaryViewArray, ListArray, RecordBatch, StringViewArray,
    StructArray, make_array,
};
use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::arrow::error::ArrowError;
use datafusion::common::{Result, exec_err};

/// Checks if the schema contains any view types (`Utf8View` or `BinaryView`).
/// Batches with view types may need special handling (e.g. compaction) before
/// spilling or holding in memory for extended periods.
pub fn schema_contains_view_types(schema: &SchemaRef) -> bool {
    schema
        .flattened_fields()
        .iter()
        .any(|field| matches!(field.data_type(), DataType::Utf8View | DataType::BinaryView))
}

/// Reconstruct `batch` to organize the payload buffers of each
/// `StringViewArray` and `BinaryViewArray` in sequential order by calling
/// `gc()` on them.
///
/// Note this is a workaround until <https://github.com/apache/arrow-rs/issues/7185> is
/// available.
///
/// # Rationale
///
/// The `interleave` kernel does not reconstruct the inner buffers of view
/// arrays by default, leading to non-sequential payload locations. A single
/// payload buffer might be shared by multiple `RecordBatch`es or multiple rows
/// in the same batch might reference scattered locations in a large buffer.
///
/// When writing each batch to disk, the writer has to write all referenced
/// buffers. This causes extra disk reads and writes, and potentially execution
/// failure (e.g. No space left on device).
///
/// # Example
///
/// Before interleaving:
/// batch1 -> buffer1 (large)
/// batch2 -> buffer2 (large)
///
/// `interleaved_batch` -> buffer1 (sparse access)
///                   -> buffer2 (sparse access)
///
/// Then when spilling the interleaved batch, the writer has to write both
/// buffer1 and buffer2 entirely, even if only a few bytes are used.
pub fn compact_batch(batch: RecordBatch) -> Result<RecordBatch> {
    let mut new_columns: Vec<Arc<dyn Array>> = Vec::with_capacity(batch.num_columns());
    let mut arr_mutated = false;

    for array in batch.columns() {
        let (new_array, mutated) = compact_array(array.clone())?;
        new_columns.push(new_array);
        arr_mutated |= mutated;
    }

    if arr_mutated {
        Ok(RecordBatch::try_new(batch.schema(), new_columns)?)
    } else {
        Ok(batch)
    }
}

/// Recursively compacts view arrays in `array` by calling `gc()` on them.
/// Returns a tuple of the potentially new array and a boolean indicating
/// whether any compaction was performed.
pub fn compact_array(array: ArrayRef) -> Result<(ArrayRef, bool)> {
    if let Some(view_array) = array.as_any().downcast_ref::<StringViewArray>() {
        return Ok((Arc::new(view_array.gc()), true));
    }
    if let Some(view_array) = array.as_any().downcast_ref::<BinaryViewArray>() {
        return Ok((Arc::new(view_array.gc()), true));
    }

    // Fast path for non-nested arrays
    if !array.data_type().is_nested() {
        return Ok((array, false));
    }

    // Avoid ArrayData -> ArrayRef roundtrips for commonly used data types,
    // including StructArray and ListArray.

    if let Some(struct_array) = array.as_any().downcast_ref::<StructArray>() {
        let mut mutated = false;
        let mut new_columns: Vec<ArrayRef> = Vec::with_capacity(struct_array.num_columns());
        for col in struct_array.columns() {
            let (new_col, col_mutated) = compact_array(col.clone())?;
            mutated |= col_mutated;
            new_columns.push(new_col);
        }

        if !mutated {
            return Ok((array, false));
        }

        let rebuilt = StructArray::new(
            struct_array.fields().clone(),
            new_columns,
            struct_array.nulls().cloned(),
        );
        return Ok((Arc::new(rebuilt), true));
    }

    if let Some(list_array) = array.as_any().downcast_ref::<ListArray>() {
        let (new_values, mutated) = compact_array(list_array.values().clone())?;
        if !mutated {
            return Ok((array, false));
        }

        let DataType::List(field) = list_array.data_type() else {
            // Defensive: this downcast should only succeed for DataType::List.
            return exec_err!(
                "ListArray has non-List data type: {:?}",
                list_array.data_type()
            );
        };

        let rebuilt = ListArray::new(
            field.clone(),
            list_array.offsets().clone(),
            new_values,
            list_array.nulls().cloned(),
        );
        return Ok((Arc::new(rebuilt), true));
    }

    // For nested arrays (Map/Dictionary/etc.), recurse into children via ArrayData.
    let data = array.to_data();
    if data.child_data().is_empty() {
        return Ok((array, false));
    }

    let mut mutated = false;
    let mut new_child_data = Vec::with_capacity(data.child_data().len());
    for child in data.child_data() {
        let child_array = make_array(child.clone());
        let (new_child_array, child_mutated) = compact_array(child_array)?;
        mutated |= child_mutated;
        new_child_data.push(new_child_array.to_data());
    }

    if !mutated {
        return Ok((array, false));
    }

    // Rebuild this array with identical buffers/nulls but replaced child_data.
    let mut builder = data.into_builder();
    builder = builder.child_data(new_child_data);
    let new_data = builder.build()?;
    Ok((make_array(new_data), true))
}

/// Estimate the in-memory size of a given `RecordBatch`. This function
/// estimates the size as if the underlying buffers were copied to somewhere
/// else and not shared.
pub fn get_record_batch_memory_size(batch: &RecordBatch) -> Result<usize> {
    let mut total_size = 0;

    for array in batch.columns() {
        let array_data = array.to_data();
        total_size += get_array_data_memory_size(&array_data)?;
    }

    Ok(total_size)
}

/// Estimate the in-memory size of a given Arrow array. This function estimates
/// the size as if the underlying buffers were copied to somewhere else and not
/// shared, including the sizes of each `BinaryView` item (which is otherwise
/// not counted by `array_data.get_slice_memory_size()`).
pub fn get_array_memory_size(array: &ArrayRef) -> Result<usize> {
    let array_data = array.to_data();
    let size = get_array_data_memory_size(&array_data)?;
    Ok(size)
}

/// The maximum number of bytes that can be stored inline in a byte view.
///
/// See [`ByteView`] and [`GenericByteViewArray`] for more information on the
/// layout of the views.
///
/// [`GenericByteViewArray`]: https://docs.rs/arrow/latest/arrow/array/struct.GenericByteViewArray.html
pub const MAX_INLINE_VIEW_LEN: u32 = 12;

/// Compute the memory usage of `array_data` and its children recursively.
fn get_array_data_memory_size(array_data: &ArrayData) -> core::result::Result<usize, ArrowError> {
    // The `ArrayData::get_slice_memory_size` method does not account for the memory
    // used by the values of BinaryView/Utf8View arrays, so we need to compute
    // that using `get_binary_view_value_size` and add that to the total size.
    Ok(get_binary_view_value_size(array_data)? + array_data.get_slice_memory_size()?)
}

fn get_binary_view_value_size(array_data: &ArrayData) -> Result<usize, ArrowError> {
    let mut result: usize = 0;
    let array_data_type = array_data.data_type();

    if matches!(array_data_type, DataType::BinaryView | DataType::Utf8View) {
        // The views buffer contains length view structures with the following layout:
        // https://arrow.apache.org/docs/format/Columnar.html#variable-size-binary-view-layout
        //
        // * Short strings, length <= 12
        // | Bytes 0-3  | Bytes 4-15                            |
        // |------------|---------------------------------------|
        // | length     | data (padded with 0)                  |
        //
        // * Long strings, length > 12
        // | Bytes 0-3  | Bytes 4-7  | Bytes 8-11 | Bytes 12-15 |
        // |------------|------------|------------|-------------|
        // | length     | prefix     | buf. index | offset      |
        let views = &array_data.buffer::<u128>(0)[..array_data.len()];
        result = views
            .iter()
            .map(|v| {
                let len = *v as u32;
                if len > MAX_INLINE_VIEW_LEN {
                    len as usize
                } else {
                    0
                }
            })
            .sum();
    }

    // If this was not a BinaryView/Utf8View array, count the bytes of any
    // BinaryView/Utf8View children, taking into account the slice of this array
    // that applies to the child.
    for child in array_data.child_data() {
        result += get_binary_view_value_size(child)?;
    }
    Ok(result)
}

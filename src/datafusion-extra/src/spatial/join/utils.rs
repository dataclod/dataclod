// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

/// Most of the code in this module are copied from the
/// `datafusion_physical_plan::joins::utils` module. https://github.com/apache/datafusion/blob/48.0.0/datafusion/physical-plan/src/joins/utils.rs
/// We made some slight modification to reference a collection of batches in the
/// build side instead of one giant concatenated batch.
use std::{ops::Range, sync::Arc};

use datafusion::arrow::array::{
    Array, BooleanArray, BooleanBufferBuilder, RecordBatch, RecordBatchOptions, UInt32Array,
    new_null_array,
};
use datafusion::arrow::compute;
use datafusion::arrow::datatypes::{DataType, Schema};
use datafusion::common::cast::as_boolean_array;
use datafusion::common::{JoinSide, Result};
use datafusion::logical_expr::JoinType;
use datafusion::physical_expr::Partitioning;
use datafusion::physical_plan::execution_plan::Boundedness;
use datafusion::physical_plan::joins::utils::{
    ColumnIndex, JoinFilter, adjust_right_output_partitioning,
};
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};

/// The indices of the rows from the build and probe sides in the joined result.
#[derive(Clone, Debug)]
pub struct JoinedRowsIndices {
    /// The indices of the rows in the build side.
    /// The first element is the batch index in the spatial index, the second
    /// element is the row index in the batch.
    pub build: Vec<(i32, i32)>,
    /// The indices of the rows in the probe side.
    pub probe: Vec<u32>,
}

impl JoinedRowsIndices {
    /// Creates a new JoinedRowsIndices with the given build and probe indices.
    pub fn new(build: Vec<(i32, i32)>, probe: Vec<u32>) -> Self {
        debug_assert_eq!(
            build.len(),
            probe.len(),
            "Build and probe indices must have the same length"
        );
        Self { build, probe }
    }

    /// Creates an empty JoinedRowsIndices with no rows.
    pub fn empty() -> Self {
        Self {
            build: Vec::new(),
            probe: Vec::new(),
        }
    }

    /// Checks if this JoinedRowsIndices is empty (has no rows).
    pub fn is_empty(&self) -> bool {
        self.build.is_empty() && self.probe.is_empty()
    }
}

pub(crate) fn apply_join_filter_to_indices(
    build_batches: &[&RecordBatch], joined_indices: &JoinedRowsIndices, probe_batch: &RecordBatch,
    filter: &JoinFilter, build_side: JoinSide,
) -> Result<JoinedRowsIndices> {
    if joined_indices.is_empty() {
        return Ok(joined_indices.clone());
    }

    // Create intermediate batch for filter evaluation
    let intermediate_batch = build_batch_from_indices(
        filter.schema(),
        build_batches,
        &joined_indices.build,
        probe_batch,
        &joined_indices.probe,
        filter.column_indices(),
        build_side,
    )?;

    let filter_result = filter
        .expression()
        .evaluate(&intermediate_batch)?
        .into_array(intermediate_batch.num_rows())?;
    let mask = as_boolean_array(&filter_result)?;

    // Filter the positions and indices based on the mask
    let mut filtered_build_positions = Vec::with_capacity(mask.len());
    let mut filtered_probe_indices = Vec::with_capacity(mask.len());

    for i in 0..mask.len() {
        if mask.value(i) {
            filtered_build_positions.push(joined_indices.build[i]);
            filtered_probe_indices.push(joined_indices.probe[i]);
        }
    }

    Ok(JoinedRowsIndices {
        build: filtered_build_positions,
        probe: filtered_probe_indices,
    })
}

/// Returns a new [RecordBatch] by combining the `left` and `right` according to
/// `indices`. The resulting batch has [Schema] `schema`.
pub(crate) fn build_batch_from_indices(
    schema: &Schema,
    build_batches: &[&RecordBatch],
    build_batch_positions: &[(i32, i32)], // (batch_idx, row_idx) pairs
    probe_batch: &RecordBatch,
    probe_indices: &[u32], // probe row indices
    column_indices: &[ColumnIndex],
    build_side: JoinSide,
) -> Result<RecordBatch> {
    let num_rows = build_batch_positions.len();
    debug_assert_eq!(num_rows, probe_indices.len());

    if schema.fields().is_empty() {
        let options = RecordBatchOptions::new()
            .with_match_field_names(true)
            .with_row_count(Some(num_rows));

        return Ok(RecordBatch::try_new_with_options(
            Arc::new(schema.clone()),
            vec![],
            &options,
        )?);
    }

    // build the columns of the new [RecordBatch]:
    // 1. pick whether the column is from the left or right
    // 2. based on the pick, `take` items from the different RecordBatches
    let mut columns: Vec<Arc<dyn Array>> = Vec::with_capacity(schema.fields().len());

    for column_index in column_indices {
        let array = if column_index.side == JoinSide::None {
            // LeftMark join, the mark column is a true if the indices is not null,
            // otherwise it will be false For now, assume all probe indices are
            // valid (not null)
            Arc::new(BooleanArray::from(vec![true; num_rows]))
        } else if column_index.side == build_side {
            // Build side - use interleave to efficiently gather from multiple batches
            if build_batch_positions.is_empty() {
                // Shouldn't happen, but handle gracefully
                new_null_array(&DataType::Int32, num_rows)
            } else {
                // Find unique batch indices that we actually need
                let mut needed_batches = std::collections::HashSet::new();
                for &(batch_idx, _) in build_batch_positions {
                    needed_batches.insert(batch_idx as usize);
                }

                // Create a mapping from original batch_idx to array position
                let mut batch_idx_to_array_idx = std::collections::HashMap::new();
                let mut arrays = Vec::with_capacity(needed_batches.len());

                for &batch_idx in &needed_batches {
                    batch_idx_to_array_idx.insert(batch_idx, arrays.len());
                    arrays.push(build_batches[batch_idx].column(column_index.index).as_ref());
                }

                // Create indices for interleave: (array_index_in_arrays, row_index) pairs
                let indices: Vec<(usize, usize)> = build_batch_positions
                    .iter()
                    .map(|&(batch_idx, row_idx)| {
                        let array_idx = batch_idx_to_array_idx[&(batch_idx as usize)];
                        (array_idx, row_idx as usize)
                    })
                    .collect();

                // Use interleave to efficiently gather values
                compute::interleave(&arrays, &indices)?
            }
        } else {
            // Probe side
            let array = probe_batch.column(column_index.index);
            if array.is_empty() {
                new_null_array(array.data_type(), num_rows)
            } else {
                let indices_array = UInt32Array::from(probe_indices.to_vec());
                compute::take(array.as_ref(), &indices_array, None)?
            }
        };
        columns.push(array);
    }
    Ok(RecordBatch::try_new(Arc::new(schema.clone()), columns)?)
}

/// The input is the matched indices for left and right and
/// adjust the indices according to the join type
pub(crate) fn adjust_indices_by_join_type(
    joined_indices: &JoinedRowsIndices, adjust_range: Range<usize>, join_type: JoinType,
    preserve_order_for_right: bool,
) -> JoinedRowsIndices {
    match join_type {
        JoinType::Inner => {
            // matched
            joined_indices.clone()
        }
        JoinType::Left => {
            // matched
            joined_indices.clone()
            // unmatched left row will be produced in the end of loop, and it
            // has been set in the left visited bitmap
        }
        JoinType::Right => {
            // combine the matched and unmatched right result together
            append_right_indices(joined_indices, adjust_range, preserve_order_for_right)
        }
        JoinType::Full => append_right_indices(joined_indices, adjust_range, false),
        JoinType::RightSemi => {
            // need to remove the duplicated record in the right side
            let right_indices = get_semi_indices(adjust_range, &joined_indices.probe);
            // the left_indices will not be used later for the `right semi` join
            JoinedRowsIndices {
                build: joined_indices.build.clone(),
                probe: right_indices,
            }
        }
        JoinType::RightAnti => {
            // need to remove the duplicated record in the right side
            // get the anti index for the right side
            let right_indices = get_anti_indices(adjust_range, &joined_indices.probe);
            // the left_indices will not be used later for the `right anti` join
            JoinedRowsIndices {
                build: joined_indices.build.clone(),
                probe: right_indices,
            }
        }
        JoinType::LeftSemi | JoinType::LeftAnti | JoinType::LeftMark | JoinType::RightMark => {
            // matched or unmatched left row will be produced in the end of loop
            // When visit the right batch, we can output the matched left row and don't need
            // to wait the end of loop
            JoinedRowsIndices::empty()
        }
    }
}

/// Appends right indices to left indices based on the specified order mode.
///
/// The function operates in two modes:
/// 1. If `preserve_order_for_right` is true, probe matched and unmatched
///    indices are inserted in order using the `append_probe_indices_in_order()`
///    method.
/// 2. Otherwise, unmatched probe indices are simply appended after matched
///    ones.
///
/// # Parameters
/// - `left_indices`: UInt64Array of left indices.
/// - `right_indices`: UInt32Array of right indices.
/// - `adjust_range`: Range to adjust the right indices.
/// - `preserve_order_for_right`: Boolean flag to determine the mode of
///   operation.
///
/// # Returns
/// A tuple of updated `UInt64Array` and `UInt32Array`.
pub(crate) fn append_right_indices(
    joined_indices: &JoinedRowsIndices, adjust_range: Range<usize>, preserve_order_for_right: bool,
) -> JoinedRowsIndices {
    if preserve_order_for_right {
        let (build, probe) = append_probe_indices_in_order(
            &joined_indices.build,
            &joined_indices.probe,
            adjust_range,
        );
        JoinedRowsIndices { build, probe }
    } else {
        let right_unmatched_indices = get_anti_indices(adjust_range, &joined_indices.probe);

        if right_unmatched_indices.is_empty() {
            joined_indices.clone()
        } else {
            // `into_builder()` can fail here when there is nothing to be filtered and
            // left_indices or right_indices has the same reference to the cached indices.
            // In that case, we use a slower alternative.

            // the new left indices: left_indices + null placeholders for unmatched right
            let mut new_left_indices =
                Vec::with_capacity(joined_indices.build.len() + right_unmatched_indices.len());
            new_left_indices.extend_from_slice(&joined_indices.build);
            new_left_indices.extend(std::iter::repeat_n((-1, -1), right_unmatched_indices.len()));

            // the new right indices: right_indices + right_unmatched_indices
            let mut new_right_indices =
                Vec::with_capacity(joined_indices.probe.len() + right_unmatched_indices.len());
            new_right_indices.extend_from_slice(&joined_indices.probe);
            new_right_indices.extend_from_slice(&right_unmatched_indices);

            JoinedRowsIndices {
                build: new_left_indices,
                probe: new_right_indices,
            }
        }
    }
}

/// Returns `range` indices which are not present in `input_indices`
pub(crate) fn get_anti_indices(range: Range<usize>, input_indices: &[u32]) -> Vec<u32> {
    let mut bitmap = BooleanBufferBuilder::new(range.len());
    bitmap.append_n(range.len(), false);
    input_indices
        .iter()
        .map(|&v| v as usize)
        .filter(|v| range.contains(v))
        .for_each(|v| {
            bitmap.set_bit(v - range.start, true);
        });

    let offset = range.start;

    // get the anti index
    (range)
        .filter_map(|idx| (!bitmap.get_bit(idx - offset)).then_some(idx as u32))
        .collect()
}

/// Returns intersection of `range` and `input_indices` omitting duplicates
pub(crate) fn get_semi_indices(range: Range<usize>, input_indices: &[u32]) -> Vec<u32> {
    let mut bitmap = BooleanBufferBuilder::new(range.len());
    bitmap.append_n(range.len(), false);
    input_indices
        .iter()
        .map(|&v| v as usize)
        .filter(|v| range.contains(v))
        .for_each(|v| {
            bitmap.set_bit(v - range.start, true);
        });

    let offset = range.start;

    // get the semi index
    (range)
        .filter_map(|idx| (bitmap.get_bit(idx - offset)).then_some(idx as u32))
        .collect()
}

/// Appends probe indices in order by considering the given build indices.
///
/// This function constructs new build and probe indices by iterating through
/// the provided indices, and appends any missing values between previous and
/// current probe index with a corresponding null build index.
///
/// # Parameters
///
/// - `build_indices`: `PrimitiveArray` of `UInt64Type` containing build
///   indices.
/// - `probe_indices`: `PrimitiveArray` of `UInt32Type` containing probe
///   indices.
/// - `range`: The range of indices to consider.
///
/// # Returns
///
/// A tuple of two arrays:
/// - A `PrimitiveArray` of `UInt64Type` with the newly constructed build
///   indices.
/// - A `PrimitiveArray` of `UInt32Type` with the newly constructed probe
///   indices.
fn append_probe_indices_in_order(
    left_indices: &[(i32, i32)], // (batch_idx, row_idx) pairs
    right_indices: &[u32],       // probe row indices
    range: Range<usize>,
) -> (Vec<(i32, i32)>, Vec<u32>) {
    // Builders for new indices:
    let mut new_left_indices = Vec::with_capacity(range.len());
    let mut new_right_indices = Vec::with_capacity(range.len());
    // Set previous index as the start index for the initial loop:
    let mut prev_index = range.start as u32;
    // Zip the two iterators.
    debug_assert!(left_indices.len() == right_indices.len());
    for (i, &right_index) in right_indices.iter().enumerate() {
        // Append values between previous and current probe index with null build index:
        for value in prev_index..right_index {
            new_right_indices.push(value);
            new_left_indices.push((-1, -1)); // Placeholder for null build index
        }
        // Append current indices:
        new_right_indices.push(right_index);
        new_left_indices.push(left_indices[i]); // Use actual left index
        // Set current probe index as previous for the next iteration:
        prev_index = right_index + 1;
    }
    // Append remaining probe indices after the last valid probe index with null
    // build index.
    for value in prev_index..range.end as u32 {
        new_right_indices.push(value);
        new_left_indices.push((-1, -1)); // Placeholder for null build index
    }
    // Build arrays and return:
    (new_left_indices.into_iter().collect(), new_right_indices)
}

pub(crate) fn asymmetric_join_output_partitioning(
    left: &Arc<dyn ExecutionPlan>, right: &Arc<dyn ExecutionPlan>, join_type: &JoinType,
) -> Result<Partitioning> {
    match join_type {
        JoinType::Inner | JoinType::Right => {
            adjust_right_output_partitioning(
                right.output_partitioning(),
                left.schema().fields().len(),
            )
        }
        JoinType::RightSemi | JoinType::RightAnti => Ok(right.output_partitioning().clone()),
        JoinType::Left
        | JoinType::LeftSemi
        | JoinType::LeftAnti
        | JoinType::Full
        | JoinType::LeftMark
        | JoinType::RightMark => {
            Ok(Partitioning::UnknownPartitioning(
                right.output_partitioning().partition_count(),
            ))
        }
    }
}

/// This function is copied from
/// [`datafusion_physical_plan::physical_plan::execution_plan::boundedness_from_children`].
/// It is used to determine the boundedness of the join operator based on the
/// boundedness of its children.
pub(crate) fn boundedness_from_children(
    children: impl IntoIterator<Item = &Arc<dyn ExecutionPlan>>,
) -> Boundedness {
    let mut unbounded_with_finite_mem = false;

    for child in children {
        match child.boundedness() {
            Boundedness::Unbounded {
                requires_infinite_memory: true,
            } => {
                return Boundedness::Unbounded {
                    requires_infinite_memory: true,
                };
            }
            Boundedness::Unbounded {
                requires_infinite_memory: false,
            } => {
                unbounded_with_finite_mem = true;
            }
            Boundedness::Bounded => {}
        }
    }

    if unbounded_with_finite_mem {
        Boundedness::Unbounded {
            requires_infinite_memory: false,
        }
    } else {
        Boundedness::Bounded
    }
}

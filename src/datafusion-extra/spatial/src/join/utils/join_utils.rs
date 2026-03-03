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
/// `datafusion_physical_plan::joins::utils` module. <https://github.com/apache/datafusion/blob/50.2.0/datafusion/physical-plan/src/joins/utils.rs>
use std::{ops::Range, sync::Arc};

use datafusion::arrow::array::{
    Array, ArrowPrimitiveType, BooleanBufferBuilder, Float64Array, NativeAdapter, PrimitiveArray,
    RecordBatch, RecordBatchOptions, UInt32Array, UInt32Builder, UInt64Array, UInt64Builder,
    downcast_array, new_null_array,
};
use datafusion::arrow::buffer::NullBuffer;
use datafusion::arrow::compute::{self, take};
use datafusion::arrow::datatypes::{ArrowNativeType, Schema, SchemaRef, UInt32Type, UInt64Type};
use datafusion::common::cast::as_boolean_array;
use datafusion::common::{JoinSide, Result};
use datafusion::logical_expr::JoinType;
use datafusion::physical_expr::Partitioning;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::joins::utils::{
    ColumnIndex, JoinFilter, adjust_right_output_partitioning,
};
use datafusion::physical_plan::projection::{
    ProjectionExec, join_allows_pushdown, join_table_borders, new_join_children,
    physical_to_column_exprs, update_join_filter,
};
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};

use crate::join::spatial_predicate::{SpatialPredicate, SpatialPredicateTrait};

/// Some type `join_type` of join need to maintain the matched indices bit map
/// for the left side, and use the bit map to generate the part of result of the
/// join.
///
/// For example of the `Left` join, in each iteration of right side, can get the
/// matched result, but need to maintain the matched indices bit map to get the
/// unmatched row for the left side.
pub fn need_produce_result_in_final(join_type: JoinType) -> bool {
    matches!(
        join_type,
        JoinType::Left
            | JoinType::LeftAnti
            | JoinType::LeftSemi
            | JoinType::LeftMark
            | JoinType::Full
    )
}

/// Determines if a bitmap is needed to track matched rows in the probe side's
/// "Multi" partition.
///
/// In a spatial partitioned join, the "Multi" partition of the probe side
/// overlaps with multiple partitions of the build side. Consequently, rows in
/// the probe "Multi" partition are processed against multiple build partitions.
///
/// For `Right`, `RightSemi`, `RightAnti`, and `Full` joins, we must track
/// whether a probe row has been matched across *any* of these interactions to
/// correctly produce results:
/// - **Right/Full Outer**: Emit probe rows that never matched any build
///   partition (checked at the last build partition).
/// - **Right Semi**: Emit a probe row the first time it matches, and suppress
///   subsequent matches (deduplication).
/// - **Right Anti**: Emit probe rows only if they never match any build
///   partition (checked at the last build partition).
pub fn need_probe_multi_partition_bitmap(join_type: JoinType) -> bool {
    matches!(
        join_type,
        JoinType::Right
            | JoinType::RightAnti
            | JoinType::RightSemi
            | JoinType::RightMark
            | JoinType::Full
    )
}

/// In the end of join execution, need to use bit map of the matched
/// indices to generate the final left and right indices.
///
/// For example:
///
/// 1. `left_bit_map`: `[true, false, true, true, false]`
/// 2. `join_type`: `Left`
///
/// The result is: `([1,4], [null, null])`
pub fn get_final_indices_from_bit_map(
    left_bit_map: &BooleanBufferBuilder, join_type: JoinType,
) -> (UInt64Array, UInt32Array) {
    let left_size = left_bit_map.len();
    if join_type == JoinType::LeftMark {
        let left_indices = (0..left_size as u64).collect::<UInt64Array>();
        let right_indices = (0..left_size)
            .map(|idx| left_bit_map.get_bit(idx).then_some(0))
            .collect::<UInt32Array>();
        return (left_indices, right_indices);
    }
    let left_indices = if join_type == JoinType::LeftSemi {
        (0..left_size)
            .filter_map(|idx| (left_bit_map.get_bit(idx)).then_some(idx as u64))
            .collect::<UInt64Array>()
    } else {
        // just for `Left`, `LeftAnti` and `Full` join
        // `LeftAnti`, `Left` and `Full` will produce the unmatched left row finally
        (0..left_size)
            .filter_map(|idx| (!left_bit_map.get_bit(idx)).then_some(idx as u64))
            .collect::<UInt64Array>()
    };
    // right_indices
    // all the element in the right side is None
    let mut builder = UInt32Builder::with_capacity(left_indices.len());
    builder.append_nulls(left_indices.len());
    let right_indices = builder.finish();
    (left_indices, right_indices)
}

pub fn adjust_indices_with_visited_info(
    left_indices: UInt64Array, right_indices: UInt32Array, adjust_range: Range<usize>,
    join_type: JoinType, preserve_order_for_right: bool,
    visited_info: Option<(&mut BooleanBufferBuilder, usize)>, produce_unmatched_probe_rows: bool,
) -> (UInt64Array, UInt32Array) {
    let Some((bitmap, offset)) = visited_info else {
        return adjust_indices_by_join_type(
            left_indices,
            right_indices,
            adjust_range,
            join_type,
            preserve_order_for_right,
        );
    };

    // Update the bitmap with the current matches first
    for idx in right_indices.values() {
        bitmap.set_bit(offset + (*idx as usize), true);
    }

    match join_type {
        JoinType::Right | JoinType::Full => {
            if !produce_unmatched_probe_rows {
                (left_indices, right_indices)
            } else {
                let unmatched_count = adjust_range
                    .clone()
                    .filter(|&i| !bitmap.get_bit(i + offset))
                    .count();

                if unmatched_count == 0 {
                    return (left_indices, right_indices);
                }

                let mut unmatched_indices = UInt32Builder::with_capacity(unmatched_count);
                for i in adjust_range {
                    if !bitmap.get_bit(i + offset) {
                        unmatched_indices.append_value(i as u32);
                    }
                }
                let unmatched_right = unmatched_indices.finish();

                let total_len = left_indices.len() + unmatched_count;
                let mut new_left_builder =
                    left_indices.into_builder().unwrap_or_else(|left_indices| {
                        let mut builder = UInt64Builder::with_capacity(total_len);
                        builder.append_slice(left_indices.values());
                        builder
                    });
                new_left_builder.append_nulls(unmatched_count);

                let mut new_right_builder =
                    right_indices
                        .into_builder()
                        .unwrap_or_else(|right_indices| {
                            let mut builder = UInt32Builder::with_capacity(total_len);
                            builder.append_slice(right_indices.values());
                            builder
                        });
                new_right_builder.append_slice(unmatched_right.values());

                (
                    UInt64Array::from(new_left_builder.finish()),
                    UInt32Array::from(new_right_builder.finish()),
                )
            }
        }
        JoinType::RightSemi => {
            if !produce_unmatched_probe_rows {
                (UInt64Array::new_null(0), UInt32Array::new_null(0))
            } else {
                let matched_count = adjust_range
                    .clone()
                    .filter(|&i| bitmap.get_bit(i + offset))
                    .count();

                let mut final_right = UInt32Builder::with_capacity(matched_count);
                for i in adjust_range {
                    if bitmap.get_bit(i + offset) {
                        final_right.append_value(i as u32);
                    }
                }

                let mut final_left = UInt64Builder::with_capacity(matched_count);
                final_left.append_nulls(matched_count);

                (final_left.finish(), final_right.finish())
            }
        }
        JoinType::RightAnti => {
            if !produce_unmatched_probe_rows {
                (UInt64Array::new_null(0), UInt32Array::new_null(0))
            } else {
                let unmatched_count = adjust_range
                    .clone()
                    .filter(|&i| !bitmap.get_bit(i + offset))
                    .count();

                let mut unmatched_indices = UInt32Builder::with_capacity(unmatched_count);
                for i in adjust_range {
                    if !bitmap.get_bit(i + offset) {
                        unmatched_indices.append_value(i as u32);
                    }
                }

                let mut final_left = UInt64Builder::with_capacity(unmatched_count);
                final_left.append_nulls(unmatched_count);

                (final_left.finish(), unmatched_indices.finish())
            }
        }
        JoinType::RightMark => {
            if !produce_unmatched_probe_rows {
                (UInt64Array::new_null(0), UInt32Array::new_null(0))
            } else {
                let range_len = adjust_range.len();
                let mut mark_bitmap = BooleanBufferBuilder::new(range_len);

                for i in adjust_range.clone() {
                    mark_bitmap.append(bitmap.get_bit(i + offset));
                }

                let right_indices = UInt32Array::from_iter_values(adjust_range.map(|i| i as u32));

                let left_indices = PrimitiveArray::new(
                    vec![0; range_len].into(),
                    Some(NullBuffer::new(mark_bitmap.finish())),
                );

                (left_indices, right_indices)
            }
        }
        _ => {
            adjust_indices_by_join_type(
                left_indices,
                right_indices,
                adjust_range,
                join_type,
                preserve_order_for_right,
            )
        }
    }
}

pub fn apply_join_filter_to_indices(
    build_input_buffer: &RecordBatch, probe_batch: &RecordBatch, build_indices: UInt64Array,
    probe_indices: UInt32Array, filter: &JoinFilter, build_side: JoinSide,
) -> Result<(UInt64Array, UInt32Array)> {
    // Forked from DataFusion 50.2.0 `apply_join_filter_to_indices`.
    // https://github.com/apache/datafusion/blob/50.2.0/datafusion/physical-plan/src/joins/utils.rs
    //
    // Changes vs upstream:
    // - Removes the `max_intermediate_size` parameter and its chunking logic.
    // - Calls our forked `build_batch_from_indices(..., join_type)` (needed for
    //   mark-join semantics).
    if build_indices.is_empty() && probe_indices.is_empty() {
        return Ok((build_indices, probe_indices));
    };

    let intermediate_batch = build_batch_from_indices(
        filter.schema(),
        build_input_buffer,
        probe_batch,
        &build_indices,
        &probe_indices,
        filter.column_indices(),
        build_side,
        JoinType::Inner,
    )?;
    let filter_result = filter
        .expression()
        .evaluate(&intermediate_batch)?
        .into_array(intermediate_batch.num_rows())?;
    let mask = as_boolean_array(&filter_result)?;

    let left_filtered = compute::filter(&build_indices, mask)?;
    let right_filtered = compute::filter(&probe_indices, mask)?;

    Ok((
        downcast_array(left_filtered.as_ref()),
        downcast_array(right_filtered.as_ref()),
    ))
}

/// Returns a new [`RecordBatch`] by combining the `left` and `right` according
/// to `indices`. The resulting batch has [Schema] `schema`.
#[allow(clippy::too_many_arguments)]
pub fn build_batch_from_indices(
    schema: &Schema, build_input_buffer: &RecordBatch, probe_batch: &RecordBatch,
    build_indices: &UInt64Array, probe_indices: &UInt32Array, column_indices: &[ColumnIndex],
    build_side: JoinSide, join_type: JoinType,
) -> Result<RecordBatch> {
    // Forked from DataFusion 50.2.0 `build_batch_from_indices`.
    // https://github.com/apache/datafusion/blob/50.2.0/datafusion/physical-plan/src/joins/utils.rs
    //
    // Changes vs upstream:
    // - Adds the `join_type` parameter so we can special-case mark joins.
    // - Fixes `RightMark` mark-column construction: for right-mark joins, the mark
    //   column must reflect match status for the *right* rows, so we build it from
    //   `build_indices` (the build-side indices) rather than `probe_indices`.
    if schema.fields().is_empty() {
        let options = RecordBatchOptions::new()
            .with_match_field_names(true)
            .with_row_count(Some(build_indices.len()));

        return Ok(RecordBatch::try_new_with_options(
            Arc::new(schema.clone()),
            Vec::new(),
            &options,
        )?);
    }

    // build the columns of the new [RecordBatch]:
    // 1. pick whether the column is from the left or right
    // 2. based on the pick, `take` items from the different RecordBatches
    let mut columns: Vec<Arc<dyn Array>> = Vec::with_capacity(schema.fields().len());

    for column_index in column_indices {
        let array = if column_index.side == JoinSide::None {
            // For mark joins, the mark column is a true if the indices is not null,
            // otherwise it will be false
            if join_type == JoinType::RightMark {
                Arc::new(compute::is_not_null(build_indices)?)
            } else {
                Arc::new(compute::is_not_null(probe_indices)?)
            }
        } else if column_index.side == build_side {
            let array = build_input_buffer.column(column_index.index);
            if array.is_empty() || build_indices.null_count() == build_indices.len() {
                // Outer join would generate a null index when finding no match at our side.
                // Therefore, it's possible we are empty but need to populate an n-length null
                // array, where n is the length of the index array.
                assert_eq!(build_indices.null_count(), build_indices.len());
                new_null_array(array.data_type(), build_indices.len())
            } else {
                take(array.as_ref(), build_indices, None)?
            }
        } else {
            let array = probe_batch.column(column_index.index);
            if array.is_empty() || probe_indices.null_count() == probe_indices.len() {
                assert_eq!(probe_indices.null_count(), probe_indices.len());
                new_null_array(array.data_type(), probe_indices.len())
            } else {
                take(array.as_ref(), probe_indices, None)?
            }
        };

        columns.push(array);
    }
    Ok(RecordBatch::try_new(Arc::new(schema.clone()), columns)?)
}

/// The input is the matched indices for left and right and
/// adjust the indices according to the join type
pub fn adjust_indices_by_join_type(
    left_indices: UInt64Array, right_indices: UInt32Array, adjust_range: Range<usize>,
    join_type: JoinType, preserve_order_for_right: bool,
) -> (UInt64Array, UInt32Array) {
    // Forked from DataFusion 50.2.0 `adjust_indices_by_join_type`.
    // https://github.com/apache/datafusion/blob/50.2.0/datafusion/physical-plan/src/joins/utils.rs
    //
    // Changes vs upstream:
    // - Fixes `RightMark` handling to match our `SpatialJoinStream` contract:
    //   `right_indices` becomes the probe row indices (`adjust_range`), and
    //   `left_indices` is a mark array (null/non-null) indicating match status.
    match join_type {
        JoinType::Inner => {
            // matched
            (left_indices, right_indices)
        }
        JoinType::Left => {
            // matched
            (left_indices, right_indices)
            // unmatched left row will be produced in the end of loop, and it
            // has been set in the left visited bitmap
        }
        JoinType::Right => {
            // combine the matched and unmatched right result together
            append_right_indices(
                left_indices,
                right_indices,
                adjust_range,
                preserve_order_for_right,
            )
        }
        JoinType::Full => append_right_indices(left_indices, right_indices, adjust_range, false),
        JoinType::RightSemi => {
            // need to remove the duplicated record in the right side
            let right_indices = get_semi_indices(adjust_range, &right_indices);
            // the left_indices will not be used later for the `right semi` join
            (left_indices, right_indices)
        }
        JoinType::RightAnti => {
            // need to remove the duplicated record in the right side
            // get the anti index for the right side
            let right_indices = get_anti_indices(adjust_range, &right_indices);
            // the left_indices will not be used later for the `right anti` join
            (left_indices, right_indices)
        }
        JoinType::RightMark => {
            let new_left_indices = get_mark_indices(&adjust_range, &right_indices);
            let new_right_indices = adjust_range.map(|i| i as u32).collect();
            (new_left_indices, new_right_indices)
        }
        JoinType::LeftSemi | JoinType::LeftAnti | JoinType::LeftMark => {
            // matched or unmatched left row will be produced in the end of loop
            // When visit the right batch, we can output the matched left row and don't need
            // to wait the end of loop
            (UInt64Array::new_null(0), UInt32Array::new_null(0))
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
/// - `left_indices`: `UInt64Array` of left indices.
/// - `right_indices`: `UInt32Array` of right indices.
/// - `adjust_range`: Range to adjust the right indices.
/// - `preserve_order_for_right`: Boolean flag to determine the mode of
///   operation.
///
/// # Returns
/// A tuple of updated `UInt64Array` and `UInt32Array`.
pub fn append_right_indices(
    left_indices: UInt64Array, right_indices: UInt32Array, adjust_range: Range<usize>,
    preserve_order_for_right: bool,
) -> (UInt64Array, UInt32Array) {
    if preserve_order_for_right {
        append_probe_indices_in_order(left_indices, right_indices, adjust_range)
    } else {
        let right_unmatched_indices = get_anti_indices(adjust_range, &right_indices);

        if right_unmatched_indices.is_empty() {
            (left_indices, right_indices)
        } else {
            // `into_builder()` can fail here when there is nothing to be filtered and
            // left_indices or right_indices has the same reference to the cached indices.
            // In that case, we use a slower alternative.

            // the new left indices: left_indices + null array
            let mut new_left_indices_builder =
                left_indices.into_builder().unwrap_or_else(|left_indices| {
                    let mut builder = UInt64Builder::with_capacity(
                        left_indices.len() + right_unmatched_indices.len(),
                    );
                    debug_assert_eq!(
                        left_indices.null_count(),
                        0,
                        "expected left indices to have no nulls"
                    );
                    builder.append_slice(left_indices.values());
                    builder
                });
            new_left_indices_builder.append_nulls(right_unmatched_indices.len());
            let new_left_indices = UInt64Array::from(new_left_indices_builder.finish());

            // the new right indices: right_indices + right_unmatched_indices
            let mut new_right_indices_builder =
                right_indices
                    .into_builder()
                    .unwrap_or_else(|right_indices| {
                        let mut builder = UInt32Builder::with_capacity(
                            right_indices.len() + right_unmatched_indices.len(),
                        );
                        debug_assert_eq!(
                            right_indices.null_count(),
                            0,
                            "expected right indices to have no nulls"
                        );
                        builder.append_slice(right_indices.values());
                        builder
                    });
            debug_assert_eq!(
                right_unmatched_indices.null_count(),
                0,
                "expected right unmatched indices to have no nulls"
            );
            new_right_indices_builder.append_slice(right_unmatched_indices.values());
            let new_right_indices = UInt32Array::from(new_right_indices_builder.finish());

            (new_left_indices, new_right_indices)
        }
    }
}

/// Returns `range` indices which are not present in `input_indices`
pub fn get_anti_indices<T: ArrowPrimitiveType>(
    range: Range<usize>, input_indices: &PrimitiveArray<T>,
) -> PrimitiveArray<T>
where
    NativeAdapter<T>: From<<T as ArrowPrimitiveType>::Native>,
{
    let bitmap = build_range_bitmap(&range, input_indices);
    let offset = range.start;

    // get the anti index
    (range)
        .filter_map(|idx| (!bitmap.get_bit(idx - offset)).then_some(T::Native::from_usize(idx)))
        .collect()
}

/// Returns intersection of `range` and `input_indices` omitting duplicates
pub fn get_semi_indices<T: ArrowPrimitiveType>(
    range: Range<usize>, input_indices: &PrimitiveArray<T>,
) -> PrimitiveArray<T>
where
    NativeAdapter<T>: From<<T as ArrowPrimitiveType>::Native>,
{
    let bitmap = build_range_bitmap(&range, input_indices);
    let offset = range.start;
    // get the semi index
    (range)
        .filter_map(|idx| (bitmap.get_bit(idx - offset)).then_some(T::Native::from_usize(idx)))
        .collect()
}

/// Returns an array for mark joins consisting of default values (zeros) with
/// null/non-null markers.
///
/// For each index in `range`:
/// - If the index appears in `input_indices`, the value is non-null (0)
/// - If the index does not appear in `input_indices`, the value is null
///
/// This is used in mark joins to indicate which rows had matches.
pub fn get_mark_indices<T: ArrowPrimitiveType, R: ArrowPrimitiveType>(
    range: &Range<usize>, input_indices: &PrimitiveArray<T>,
) -> PrimitiveArray<R>
where
    NativeAdapter<T>: From<<T as ArrowPrimitiveType>::Native>,
{
    // Forked from DataFusion 50.2.0 `get_mark_indices`.
    // https://github.com/apache/datafusion/blob/50.2.0/datafusion/physical-plan/src/joins/utils.rs
    //
    // Changes vs upstream:
    // - Generalizes the output array element type (generic `R`) so we can build
    //   mark arrays of different physical types while still using the null buffer
    //   to encode match status.
    let mut bitmap = build_range_bitmap(range, input_indices);
    PrimitiveArray::new(
        vec![R::Native::default(); range.len()].into(),
        Some(NullBuffer::new(bitmap.finish())),
    )
}

fn build_range_bitmap<T: ArrowPrimitiveType>(
    range: &Range<usize>, input: &PrimitiveArray<T>,
) -> BooleanBufferBuilder {
    let mut builder = BooleanBufferBuilder::new(range.len());
    builder.append_n(range.len(), false);

    input.iter().flatten().for_each(|v| {
        let idx = v.as_usize();
        if range.contains(&idx) {
            builder.set_bit(idx - range.start, true);
        }
    });

    builder
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
    build_indices: PrimitiveArray<UInt64Type>, probe_indices: PrimitiveArray<UInt32Type>,
    range: Range<usize>,
) -> (PrimitiveArray<UInt64Type>, PrimitiveArray<UInt32Type>) {
    // Builders for new indices:
    let mut new_build_indices = UInt64Builder::new();
    let mut new_probe_indices = UInt32Builder::new();
    // Set previous index as the start index for the initial loop:
    let mut prev_index = range.start as u32;
    // Zip the two iterators.
    debug_assert!(build_indices.len() == probe_indices.len());
    for (build_index, probe_index) in build_indices
        .values()
        .into_iter()
        .zip(probe_indices.values())
    {
        // Append values between previous and current probe index with null build index:
        for value in prev_index..*probe_index {
            new_probe_indices.append_value(value);
            new_build_indices.append_null();
        }
        // Append current indices:
        new_probe_indices.append_value(*probe_index);
        new_build_indices.append_value(*build_index);
        // Set current probe index as previous for the next iteration:
        prev_index = probe_index + 1;
    }
    // Append remaining probe indices after the last valid probe index with null
    // build index.
    for value in prev_index..range.end as u32 {
        new_probe_indices.append_value(value);
        new_build_indices.append_null();
    }
    // Build arrays and return:
    (new_build_indices.finish(), new_probe_indices.finish())
}

pub fn asymmetric_join_output_partitioning(
    left: &Arc<dyn ExecutionPlan>, right: &Arc<dyn ExecutionPlan>, join_type: &JoinType,
    probe_side: JoinSide,
) -> Result<Partitioning> {
    let result = match join_type {
        JoinType::Inner => {
            if probe_side == JoinSide::Right {
                adjust_right_output_partitioning(
                    right.output_partitioning(),
                    left.schema().fields().len(),
                )?
            } else {
                left.output_partitioning().clone()
            }
        }
        JoinType::Right => {
            if probe_side == JoinSide::Right {
                adjust_right_output_partitioning(
                    right.output_partitioning(),
                    left.schema().fields().len(),
                )?
            } else {
                Partitioning::UnknownPartitioning(left.output_partitioning().partition_count())
            }
        }
        JoinType::RightSemi | JoinType::RightAnti | JoinType::RightMark => {
            if probe_side == JoinSide::Right {
                right.output_partitioning().clone()
            } else {
                Partitioning::UnknownPartitioning(left.output_partitioning().partition_count())
            }
        }
        JoinType::Left | JoinType::LeftSemi | JoinType::LeftAnti | JoinType::LeftMark => {
            if probe_side == JoinSide::Left {
                left.output_partitioning().clone()
            } else {
                Partitioning::UnknownPartitioning(right.output_partitioning().partition_count())
            }
        }
        JoinType::Full => {
            if probe_side == JoinSide::Right {
                Partitioning::UnknownPartitioning(right.output_partitioning().partition_count())
            } else {
                Partitioning::UnknownPartitioning(left.output_partitioning().partition_count())
            }
        }
    };
    Ok(result)
}

/// This function is copied from
/// [`datafusion_physical_plan::physical_plan::execution_plan::boundedness_from_children`].
/// It is used to determine the boundedness of the join operator based on the
/// boundedness of its children.
#[allow(single_use_lifetimes)]
pub fn boundedness_from_children<'a>(
    children: impl IntoIterator<Item = &'a Arc<dyn ExecutionPlan>>,
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

pub fn compute_join_emission_type(
    left: &Arc<dyn ExecutionPlan>, right: &Arc<dyn ExecutionPlan>, join_type: JoinType,
    probe_side: JoinSide,
) -> EmissionType {
    let (build, probe) = if probe_side == JoinSide::Left {
        (right, left)
    } else {
        (left, right)
    };

    if build.boundedness().is_unbounded() {
        return EmissionType::Final;
    }

    if probe.pipeline_behavior() == EmissionType::Incremental {
        match join_type {
            // If we only need to generate matched rows from the probe side,
            // we can emit rows incrementally.
            JoinType::Inner => EmissionType::Incremental,
            JoinType::Right | JoinType::RightSemi | JoinType::RightAnti | JoinType::RightMark => {
                if probe_side == JoinSide::Right {
                    EmissionType::Incremental
                } else {
                    EmissionType::Both
                }
            }
            // If we need to generate unmatched rows from the *build side*,
            // we need to emit them at the end.
            JoinType::Left | JoinType::LeftSemi | JoinType::LeftAnti | JoinType::LeftMark => {
                if probe_side == JoinSide::Left {
                    EmissionType::Incremental
                } else {
                    EmissionType::Both
                }
            }
            JoinType::Full => EmissionType::Both,
        }
    } else {
        probe.pipeline_behavior()
    }
}

/// Data required to push down a projection through a spatial join.
/// This is mostly taken from <https://github.com/apache/datafusion/blob/51.0.0/datafusion/physical-plan/src/projection.rs>
pub struct JoinPushdownData {
    pub projected_left_child: ProjectionExec,
    pub projected_right_child: ProjectionExec,
    pub join_filter: Option<JoinFilter>,
    pub join_on: SpatialPredicate,
}

/// Push down the given `projection` through the spatial join.
/// This code is adapted from <https://github.com/apache/datafusion/blob/51.0.0/datafusion/physical-plan/src/projection.rs>
pub fn try_pushdown_through_join(
    projection: &ProjectionExec, join_left: &Arc<dyn ExecutionPlan>,
    join_right: &Arc<dyn ExecutionPlan>, join_schema: &SchemaRef, join_type: JoinType,
    join_filter: Option<&JoinFilter>, join_on: &SpatialPredicate,
) -> Result<Option<JoinPushdownData>> {
    let Some(projection_as_columns) = physical_to_column_exprs(projection.expr()) else {
        return Ok(None);
    };

    // Mark joins produce a synthetic column that does not belong to either child.
    // This synthetic `mark` column will make `new_join_children` fail, so we
    // skip pushdown for such joins. This limitation is inherited from
    // DataFusion's builtin `try_pushdown_through_join`.
    if matches!(join_type, JoinType::LeftMark | JoinType::RightMark) {
        return Ok(None);
    }

    let (far_right_left_col_ind, far_left_right_col_ind) =
        join_table_borders(join_left.schema().fields().len(), &projection_as_columns);

    if !join_allows_pushdown(
        &projection_as_columns,
        join_schema,
        far_right_left_col_ind,
        far_left_right_col_ind,
    ) {
        return Ok(None);
    }

    let (projected_left_child, projected_right_child) = new_join_children(
        &projection_as_columns,
        far_right_left_col_ind,
        far_left_right_col_ind,
        join_left,
        join_right,
    )?;

    let new_filter = if let Some(filter) = join_filter {
        let left_cols = &projection_as_columns[0..=far_right_left_col_ind as usize];
        let right_cols = &projection_as_columns[far_left_right_col_ind as usize..];
        match update_join_filter(
            left_cols,
            right_cols,
            filter,
            join_left.schema().fields().len(),
        ) {
            Some(updated) => Some(updated),
            None => return Ok(None),
        }
    } else {
        None
    };

    let projected_left_exprs = projected_left_child.expr();
    let projected_right_exprs = projected_right_child.expr();
    let Some(new_on) =
        join_on.update_for_child_projections(projected_left_exprs, projected_right_exprs)?
    else {
        return Ok(None);
    };

    Ok(Some(JoinPushdownData {
        projected_left_child,
        projected_right_child,
        join_filter: new_filter,
        join_on: new_on,
    }))
}

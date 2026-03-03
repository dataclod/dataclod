use std::cmp::max;

use datafusion::common::{DataFusionError, Result};

use crate::join::index::build_side_collector::BuildPartition;

/// The memory accounting summary of a build side partition. This is collected
/// during the build side collection phase and used to estimate the memory usage
/// for running spatial join.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PartitionMemorySummary {
    /// Number of rows in the partition.
    pub num_rows: usize,
    /// The total memory reserved when collecting this build side partition.
    pub reserved_memory: usize,
    /// The estimated memory usage for building the spatial index for all the
    /// data in this build side partition.
    pub estimated_index_memory_usage: usize,
}

impl From<&BuildPartition> for PartitionMemorySummary {
    fn from(partition: &BuildPartition) -> Self {
        Self {
            num_rows: partition.num_rows,
            reserved_memory: partition.reservation.size(),
            estimated_index_memory_usage: partition.estimated_spatial_index_memory_usage,
        }
    }
}

/// A detailed plan for memory usage during spatial join execution. The spatial
/// join could be spatial-partitioned if the reserved memory is not sufficient
/// to hold the entire spatial index.
#[derive(Debug, PartialEq, Eq)]
pub struct MemoryPlan {
    /// The total number of rows in the build side.
    pub num_rows: usize,
    /// The total memory reserved for the build side.
    pub reserved_memory: usize,
    /// The estimated memory usage for building the spatial index for the entire
    /// build side. It could be larger than [`Self::reserved_memory`], and
    /// in that case we need to partition the build side using spatial
    /// partitioning.
    pub estimated_index_memory_usage: usize,
    /// The memory budget for holding the spatial index. If the spatial join is
    /// partitioned, this is the memory budget for holding the spatial index
    /// of a single partition.
    pub memory_for_spatial_index: usize,
    /// The memory budget for intermittent usage, such as buffering data during
    /// repartitioning.
    pub memory_for_intermittent_usage: usize,
    /// The number of spatial partitions to split the build side into.
    pub num_partitions: usize,
}

/// Compute the memory plan for running spatial join based on the memory
/// summaries of build side partitions.
pub fn compute_memory_plan<I>(partition_summaries: I) -> Result<MemoryPlan>
where
    I: IntoIterator<Item = PartitionMemorySummary>,
{
    let mut num_rows = 0;
    let mut reserved_memory = 0;
    let mut estimated_index_memory_usage = 0;

    for summary in partition_summaries {
        num_rows += summary.num_rows;
        reserved_memory += summary.reserved_memory;
        estimated_index_memory_usage += summary.estimated_index_memory_usage;
    }

    if reserved_memory == 0 && num_rows > 0 {
        return Err(DataFusionError::ResourcesExhausted(
            "Insufficient memory for spatial join".to_owned(),
        ));
    }

    // Use 80% of reserved memory for holding the spatial index. The other 20% are
    // reserved for intermittent usage like repartitioning buffers.
    let memory_for_spatial_index =
        calculate_memory_for_spatial_index(reserved_memory, estimated_index_memory_usage);
    let memory_for_intermittent_usage = reserved_memory - memory_for_spatial_index;

    let num_partitions = if num_rows > 0 {
        max(
            1,
            estimated_index_memory_usage.div_ceil(memory_for_spatial_index),
        )
    } else {
        1
    };

    Ok(MemoryPlan {
        num_rows,
        reserved_memory,
        estimated_index_memory_usage,
        memory_for_spatial_index,
        memory_for_intermittent_usage,
        num_partitions,
    })
}

fn calculate_memory_for_spatial_index(
    reserved_memory: usize, estimated_index_memory_usage: usize,
) -> usize {
    if reserved_memory >= estimated_index_memory_usage {
        // Reserved memory is sufficient to hold the entire spatial index. Make sure
        // that the memory for spatial index is enough for holding the entire
        // index. The rest can be used for intermittent usage.
        estimated_index_memory_usage
    } else {
        // Reserved memory is not sufficient to hold the entire spatial index, We need
        // to partition the dataset using spatial partitioning. Use 80% of
        // reserved memory for holding the partitioned spatial index. The rest
        // is used for intermittent usage.
        let reserved_portion = reserved_memory.saturating_mul(80) / 100;
        if reserved_portion == 0 {
            reserved_memory
        } else {
            reserved_portion
        }
    }
}

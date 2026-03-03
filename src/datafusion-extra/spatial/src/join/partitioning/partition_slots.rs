use crate::join::partitioning::SpatialPartition;

#[derive(Clone, Copy, Debug)]
/// Maintains the slot mapping for all `SpatialPartition` variants, reserving
/// contiguous indices for regular partitions plus dedicated None/Multi slots.
pub struct PartitionSlots {
    num_regular: usize,
}

impl PartitionSlots {
    /// Create a slot manager for `num_regular` `SpatialPartition::Regular`
    /// entries. Two additional slots are implicitly reserved: one for
    /// `None` and one for `Multi`.
    pub fn new(num_regular: usize) -> Self {
        Self { num_regular }
    }

    /// Return the total slot count (`Regular + None + Multi`).
    pub fn total_slots(&self) -> usize {
        self.num_regular + 2
    }

    /// Convert a `SpatialPartition` into its backing slot index.
    pub fn slot(&self, partition: SpatialPartition) -> Option<usize> {
        match partition {
            SpatialPartition::Regular(id) => {
                let id = id as usize;
                if id < self.num_regular {
                    Some(id)
                } else {
                    None
                }
            }
            SpatialPartition::None => Some(self.none_slot()),
            SpatialPartition::Multi => Some(self.multi_slot()),
        }
    }

    /// Convert a slot index back into the corresponding `SpatialPartition`
    /// variant.
    pub fn partition(&self, slot: usize) -> SpatialPartition {
        if slot < self.num_regular {
            SpatialPartition::Regular(slot as u32)
        } else if slot == self.none_slot() {
            SpatialPartition::None
        } else if slot == self.multi_slot() {
            SpatialPartition::Multi
        } else {
            panic!(
                "invalid partition slot {slot} for {} regular partitions",
                self.num_regular
            );
        }
    }

    /// Number of regular partitions
    pub fn num_regular_partitions(&self) -> usize {
        self.num_regular
    }

    /// Slot dedicated to `SpatialPartition::None`.
    pub fn none_slot(&self) -> usize {
        self.num_regular
    }

    /// Slot dedicated to `SpatialPartition::Multi`.
    pub fn multi_slot(&self) -> usize {
        self.num_regular + 1
    }
}

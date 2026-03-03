//! Flat (linear scan) spatial partitioner.
//!
//! This module provides a minimal partitioner that shares the same
//! intersection semantics as [`crate::partitioning::rtree::RTreePartitioner`]
//! but avoids the `RTree` indexing overhead. It stores partition boundaries
//! in a flat array and performs a linear scan to classify each query
//! bounding box. [`FlatPartitioner`] will definitely be more efficient
//! than [`crate::partitioning::rtree::RTreePartitioner`] when the number of
//! partitions is less than 16, which is the size of R-tree's leaf nodes.
//!
//! The partitioner follows the standard spatial partition semantics:
//! - Returns [`SpatialPartition::Regular`] when exactly one boundary intersects
//!   the query bbox.
//! - Returns [`SpatialPartition::Multi`] when multiple boundaries intersect the
//!   query bbox.
//! - Returns [`SpatialPartition::None`] when no boundary intersects the query
//!   bbox.

use datafusion::common::Result;

use crate::geometry::bounding_box::BoundingBox;
use crate::geometry::interval::IntervalTrait;
use crate::join::partitioning::{SpatialPartition, SpatialPartitioner};

/// Spatial partitioner that linearly scans partition boundaries.
#[derive(Clone)]
pub struct FlatPartitioner {
    boundaries: Vec<BoundingBox>,
}

impl FlatPartitioner {
    /// Create a new flat partitioner from explicit partition boundaries.
    pub fn new(boundaries: Vec<BoundingBox>) -> Self {
        Self { boundaries }
    }
}

impl SpatialPartitioner for FlatPartitioner {
    fn num_regular_partitions(&self) -> usize {
        self.boundaries.len()
    }

    fn partition(&self, bbox: &BoundingBox) -> Result<SpatialPartition> {
        let mut first_match = None;
        for (idx, boundary) in self.boundaries.iter().enumerate() {
            if boundary.intersects(bbox) {
                if first_match.is_some() {
                    return Ok(SpatialPartition::Multi);
                }
                first_match = Some(idx as u32);
            }
        }

        Ok(match first_match {
            Some(id) => SpatialPartition::Regular(id),
            None => SpatialPartition::None,
        })
    }

    fn partition_no_multi(&self, bbox: &BoundingBox) -> Result<SpatialPartition> {
        let mut best_partition = None;
        let mut best_area = -1.0;

        for (idx, boundary) in self.boundaries.iter().enumerate() {
            if boundary.intersects(bbox) {
                let area = {
                    if let Ok(intersection) = boundary.intersection(bbox) {
                        if !intersection.x().is_wraparound() {
                            intersection.x().width() * intersection.y().width()
                        } else {
                            // Intersection has a wraparound X interval. Use a fallback
                            // area value of 0. This makes the partitioner prefer other partitions.
                            0.0
                        }
                    } else {
                        // Intersection cannot be represented as a single bbox. Use a fallback
                        // area value of 0. This makes the partitioner prefer other partitions.
                        0.0
                    }
                };
                if area > best_area {
                    best_area = area;
                    best_partition = Some(idx as u32);
                }
            }
        }

        Ok(match best_partition {
            Some(id) => SpatialPartition::Regular(id),
            None => SpatialPartition::None,
        })
    }

    fn box_clone(&self) -> Box<dyn SpatialPartitioner> {
        Box::new(self.clone())
    }
}

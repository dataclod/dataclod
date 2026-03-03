//! RTree-based spatial partitioning implementation.
//!
//! This module provides an RTree-based implementation for spatial partitioning,
//! designed for out-of-core spatial joins. Unlike KDB-tree partitioning which
//! builds the partition structure from sample data, `RTree` partitioning takes
//! a pre-defined set of partition boundaries (rectangles) and uses an `RTree`
//! index to efficiently determine which partition a given bounding box belongs
//! to.
//!
//! # Partitioning Strategy
//!
//! The `RTree` partitioner follows these rules:
//!
//! 1. **Single Partition Assignment**: Each bounding box is assigned to exactly
//!    one partition.
//! 2. **Intersection-Based Assignment**: The partitioner finds which partition
//!    boundaries intersect with the input bounding box and produces a
//!    [`SpatialPartition::Regular`] result.
//! 3. **Multi-partition Handling**: When a bbox intersects multiple partitions,
//!    it's assigned to [`SpatialPartition::Multi`].
//! 4. **None-partition Handling**: If a bbox doesn't intersect any partition
//!    boundary, it's assigned to [`SpatialPartition::None`].

use std::sync::Arc;

use datafusion::common::Result;
use geo::Rect;
use geo_index::rtree::sort::HilbertSort;
use geo_index::rtree::{RTree, RTreeBuilder, RTreeIndex};

use crate::geometry::bounding_box::BoundingBox;
use crate::join::partitioning::util::{
    bbox_to_f32_rect, bbox_to_geo_rect, make_rect, rect_intersection_area,
};
use crate::join::partitioning::{SpatialPartition, SpatialPartitioner};

/// RTree-based spatial partitioner that uses pre-defined partition boundaries.
///
/// This partitioner constructs an `RTree` index over a set of partition
/// boundaries (rectangles) and uses it to efficiently determine which partition
/// a given bounding box belongs to based on spatial intersection.
#[derive(Clone)]
pub struct RTreePartitioner {
    inner: Arc<RawRTreePartitioner>,
}

impl RTreePartitioner {
    /// Create a new `RTree` partitioner from a collection of partition
    /// boundaries.
    ///
    /// # Arguments
    /// * `boundaries` - A vector of bounding boxes representing partition
    ///   boundaries. Each bounding box defines the spatial extent of one
    ///   partition. The partition ID is the index in this vector.
    ///
    /// # Errors
    /// Returns an error if any boundary has wraparound coordinates (not
    /// supported)
    pub fn try_new(boundaries: Vec<BoundingBox>) -> Result<Self> {
        let inner = RawRTreePartitioner::try_new(boundaries)?;
        Ok(Self {
            inner: Arc::new(inner),
        })
    }

    /// Create a new `RTree` partitioner with a custom node size.
    pub fn try_new_with_node_size(boundaries: Vec<BoundingBox>, node_size: u16) -> Result<Self> {
        let inner = RawRTreePartitioner::build(boundaries, Some(node_size))?;
        Ok(Self {
            inner: Arc::new(inner),
        })
    }

    /// Return the number of levels in the underlying `RTree`.
    pub fn depth(&self) -> usize {
        self.inner.depth()
    }
}

impl SpatialPartitioner for RTreePartitioner {
    fn num_regular_partitions(&self) -> usize {
        self.inner.num_regular_partitions()
    }

    fn partition(&self, bbox: &BoundingBox) -> Result<SpatialPartition> {
        self.inner.partition(bbox)
    }

    fn partition_no_multi(&self, bbox: &BoundingBox) -> Result<SpatialPartition> {
        self.inner.partition_no_multi(bbox)
    }

    fn box_clone(&self) -> Box<dyn SpatialPartitioner> {
        Box::new(self.clone())
    }
}

struct RawRTreePartitioner {
    /// The `RTree` index storing partition boundaries as f32 rectangles
    rtree: RTree<f32>,
    /// Flat representation of partition boundaries for overlap calculations
    boundaries: Vec<Rect<f32>>,
    /// Number of partitions (excluding None and Multi)
    num_partitions: usize,
    /// Map from `RTree` index to original partition index
    partition_map: Vec<usize>,
}

impl RawRTreePartitioner {
    fn try_new(boundaries: Vec<BoundingBox>) -> Result<Self> {
        Self::build(boundaries, None)
    }

    fn build(boundaries: Vec<BoundingBox>, node_size: Option<u16>) -> Result<Self> {
        let num_partitions = boundaries.len();

        // Filter valid boundaries and keep track of original indices
        let mut valid_boundaries = Vec::with_capacity(num_partitions);
        let mut partition_map = Vec::with_capacity(num_partitions);

        for (i, bbox) in boundaries.iter().enumerate() {
            if let Some(rect) = bbox_to_f32_rect(bbox)? {
                valid_boundaries.push(rect);
                partition_map.push(i);
            }
        }

        let num_valid = valid_boundaries.len();

        // Build RTree index with partition boundaries
        let mut rtree_builder = match node_size {
            Some(size) => RTreeBuilder::<f32>::new_with_node_size(num_valid as u32, size),
            None => RTreeBuilder::<f32>::new(num_valid as u32),
        };

        let mut rects = Vec::with_capacity(num_valid);
        for (min_x, min_y, max_x, max_y) in valid_boundaries {
            rtree_builder.add(min_x, min_y, max_x, max_y);
            rects.push(make_rect(min_x, min_y, max_x, max_y));
        }

        let rtree = rtree_builder.finish::<HilbertSort>();

        Ok(RawRTreePartitioner {
            rtree,
            boundaries: rects,
            num_partitions,
            partition_map,
        })
    }

    fn num_regular_partitions(&self) -> usize {
        self.num_partitions
    }

    fn depth(&self) -> usize {
        self.rtree.num_levels()
    }

    fn partition(&self, bbox: &BoundingBox) -> Result<SpatialPartition> {
        // Convert bbox to f32 for RTree query with proper bounds handling
        let Some((min_x, min_y, max_x, max_y)) = bbox_to_f32_rect(bbox)? else {
            return Ok(SpatialPartition::None);
        };

        // Query RTree for intersecting partitions
        let intersecting_partitions = self.rtree.search(min_x, min_y, max_x, max_y);

        // Handle different cases based on number of intersecting partitions
        match intersecting_partitions.len() {
            0 => {
                // No intersection with any partition -> None
                Ok(SpatialPartition::None)
            }
            1 => {
                // Single intersection -> Regular partition
                let rtree_index = intersecting_partitions[0];
                Ok(SpatialPartition::Regular(
                    self.partition_map[rtree_index as usize] as u32,
                ))
            }
            _ => {
                // Multiple intersections -> Always return Multi
                Ok(SpatialPartition::Multi)
            }
        }
    }

    fn partition_no_multi(&self, bbox: &BoundingBox) -> Result<SpatialPartition> {
        let Some(rect) = bbox_to_geo_rect(bbox)? else {
            return Ok(SpatialPartition::None);
        };
        let min = rect.min();
        let max = rect.max();
        let intersecting_partitions = self.rtree.search(min.x, min.y, max.x, max.y);

        if intersecting_partitions.is_empty() {
            return Ok(SpatialPartition::None);
        }

        let mut best_partition = None;
        let mut best_area = -1.0_f32;
        for &partition_id in &intersecting_partitions {
            let boundary = &self.boundaries[partition_id as usize];
            let area = rect_intersection_area(boundary, &rect);
            if area > best_area {
                best_area = area;
                best_partition = Some(partition_id);
            }
        }

        Ok(match best_partition {
            Some(id) => SpatialPartition::Regular(self.partition_map[id as usize] as u32),
            None => SpatialPartition::None,
        })
    }
}

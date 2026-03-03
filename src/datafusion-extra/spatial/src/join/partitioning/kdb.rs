//! KDB-tree spatial partitioning implementation.
//!
//! This module provides a K-D-B tree (K-Dimensional B-tree) implementation for
//! spatial partitioning, which is essential for out-of-core spatial joins.
//!
//! # Partitioning Strategy
//!
//! - **Partition Assignment**:
//!   - `partition()`: Returns `SpatialPartition::Regular(id)` if the bbox
//!     intersects exactly one partition, `SpatialPartition::Multi` if it
//!     intersects multiple, or `SpatialPartition::None` if it intersects none.
//!   - `partition_no_multi()`: Assigns the bbox to the partition with the
//!     largest intersection area (Maximum Overlap Strategy), or
//!     `SpatialPartition::None` if no intersection.
//!
//! # Algorithm
//!
//! The KDB tree partitions space by recursively splitting it along alternating
//! axes:
//!
//! 1. Start with the full spatial extent
//! 2. When a node exceeds `max_items_per_node`, split it:
//!    - Choose the longer dimension (x or y)
//!    - Sort items by their minimum coordinate on that dimension
//!    - Split at the median item's coordinate
//! 3. Continue until reaching `max_levels` depth or items fit in nodes
//! 4. Assign sequential IDs to leaf nodes

use std::sync::Arc;

use datafusion::common::{Result, exec_err};
use geo::{Coord, Rect};

use crate::geometry::bounding_box::BoundingBox;
use crate::join::partitioning::util::{
    bbox_to_geo_rect, make_rect, rect_contains_point, rect_intersection_area, rects_intersect,
};
use crate::join::partitioning::{SpatialPartition, SpatialPartitioner};

/// K-D-B tree spatial partitioner implementation.
///
/// See <https://en.wikipedia.org/wiki/K-D-B-tree>
///
/// The KDB tree is a hierarchical spatial data structure that recursively
/// partitions space using axis-aligned splits. It adapts to the spatial
/// distribution of data, making it effective for spatial partitioning.
pub struct KDBTree {
    max_items_per_node: usize,
    max_levels: usize,
    extent: Rect<f32>,
    level: usize,
    items: Vec<Rect<f32>>,
    children: Option<Box<[KDBTree; 2]>>,
    leaf_id: u32,
}

impl KDBTree {
    /// Create a new KDB tree with the given parameters.
    ///
    /// # Arguments
    /// * `max_items_per_node` - Maximum number of items before splitting a node
    /// * `max_levels` - Maximum depth of the tree
    /// * `extent` - The spatial extent covered by this tree
    pub fn try_new(
        max_items_per_node: usize, max_levels: usize, extent: BoundingBox,
    ) -> Result<Self> {
        if max_items_per_node == 0 {
            return exec_err!("max_items_per_node must be greater than 0");
        }

        // extent_rect is a sentinel rect if the bounding box is empty. In that case,
        // almost all insertions will be ignored. We are free to partition the data
        // arbitrarily when the extent is empty.
        let extent_rect = bbox_to_geo_rect(&extent)?.unwrap_or(make_rect(0.0, 0.0, 0.0, 0.0));

        Ok(Self::new_with_level(
            max_items_per_node,
            max_levels,
            0,
            extent_rect,
        ))
    }

    fn new_with_level(
        max_items_per_node: usize, max_levels: usize, level: usize, extent: Rect<f32>,
    ) -> Self {
        KDBTree {
            max_items_per_node,
            max_levels,
            extent,
            level,
            items: Vec::new(),
            children: None,
            leaf_id: 0,
        }
    }

    /// Insert a bounding box into the tree.
    pub fn insert(&mut self, bbox: BoundingBox) -> Result<()> {
        if let Some(rect) = bbox_to_geo_rect(&bbox)?
            && rect_contains_point(&self.extent, &rect.min())
        {
            self.insert_rect(rect);
        }
        Ok(())
    }

    fn insert_rect(&mut self, rect: Rect<f32>) {
        if self.children.is_none() {
            if self.items.len() < self.max_items_per_node || self.level >= self.max_levels {
                // This leaf node has enough capacity, insert into it.
                self.items.push(rect);
                return;
            }

            // Try splitting over longer side. If it does not work, fallback splitting
            // over the shorter side.
            let split_x = self.extent.width() > self.extent.height();
            if !self.split(split_x) && !self.split(!split_x) {
                // Splitting neither side worked. This could happen if all envelopes are the
                // same.
                self.items.push(rect);
                return;
            }
        }

        // Insert into appropriate child
        if let Some(ref mut children) = self.children {
            let min_point = rect.min();
            for child in children.iter_mut() {
                if rect_contains_point(child.extent(), &min_point) {
                    child.insert_rect(rect);
                    break;
                }
            }
        }
    }

    /// Check if this node is a leaf node.
    pub fn is_leaf(&self) -> bool {
        self.children.is_none()
    }

    /// Get the leaf ID (only valid for leaf nodes).
    pub fn leaf_id(&self) -> u32 {
        assert!(self.is_leaf(), "leaf_id() called on non-leaf node");
        self.leaf_id
    }

    /// Get the spatial extent of this node.
    pub fn extent(&self) -> &Rect<f32> {
        &self.extent
    }

    /// Assign leaf IDs to all leaf nodes in the tree (breadth-first traversal).
    pub fn assign_leaf_ids(&mut self) {
        let mut next_id = 0;
        self.assign_leaf_ids_recursive(&mut next_id);
    }

    fn assign_leaf_ids_recursive(&mut self, next_id: &mut u32) {
        if self.is_leaf() {
            self.leaf_id = *next_id;
            *next_id += 1;
        } else if let Some(ref mut children) = self.children {
            for child in children.iter_mut() {
                child.assign_leaf_ids_recursive(next_id);
            }
        }
    }

    pub fn visit_intersecting_leaf_nodes<'a>(
        &'a self, rect: &Rect<f32>, f: &mut impl FnMut(&'a KDBTree),
    ) {
        if !rects_intersect(&self.extent, rect) {
            return;
        }

        if self.is_leaf() {
            f(self)
        } else if let Some(ref children) = self.children {
            for child in children.iter() {
                child.visit_intersecting_leaf_nodes(rect, f);
            }
        }
    }

    pub fn visit_leaf_nodes<'a>(&'a self, f: &mut impl FnMut(&'a KDBTree)) {
        if self.is_leaf() {
            f(self)
        } else if let Some(ref children) = self.children {
            for child in children.iter() {
                child.visit_leaf_nodes(f);
            }
        }
    }

    pub fn collect_leaf_nodes(&self) -> Vec<&KDBTree> {
        let mut leaf_nodes = Vec::new();
        self.visit_leaf_nodes(&mut |kdb| {
            leaf_nodes.push(kdb);
        });
        leaf_nodes
    }

    pub fn num_leaf_nodes(&self) -> usize {
        let mut num = 0;
        self.visit_leaf_nodes(&mut |_| {
            num += 1;
        });
        num
    }

    /// Drop all stored items to save memory after tree construction.
    pub fn drop_elements(&mut self) {
        self.items.clear();
        if let Some(ref mut children) = self.children {
            for child in children.iter_mut() {
                child.drop_elements();
            }
        }
    }

    /// Split this node along the specified axis.
    /// Returns true if the split was successful, false otherwise.
    /// The split would fail when too many objects are crowded at the edge of
    /// the extent. If splitting on one axis fails, we'll try the other
    /// axis. If both fail, we won't split. Please refer to
    /// [`Self::insert_rect`] for details.
    fn split(&mut self, split_x: bool) -> bool {
        // Sort items by the appropriate coordinate
        if split_x {
            self.items.sort_by(|a, b| {
                let a_min = a.min();
                let b_min = b.min();
                a_min
                    .x
                    .partial_cmp(&b_min.x)
                    .unwrap_or(std::cmp::Ordering::Equal)
                    .then_with(|| {
                        a_min
                            .y
                            .partial_cmp(&b_min.y)
                            .unwrap_or(std::cmp::Ordering::Equal)
                    })
            });
        } else {
            self.items.sort_by(|a, b| {
                let a_min = a.min();
                let b_min = b.min();
                a_min
                    .y
                    .partial_cmp(&b_min.y)
                    .unwrap_or(std::cmp::Ordering::Equal)
                    .then_with(|| {
                        a_min
                            .x
                            .partial_cmp(&b_min.x)
                            .unwrap_or(std::cmp::Ordering::Equal)
                    })
            });
        }

        // Find the split coordinate from the middle item
        let middle_idx = self.items.len() / 2;
        let middle_min = self.items[middle_idx].min();

        let split_coord = if split_x { middle_min.x } else { middle_min.y };

        let extent_min = self.extent.min();
        let extent_max = self.extent.max();

        // Check if split coordinate is valid
        if split_x {
            if split_coord <= extent_min.x || split_coord >= extent_max.x {
                // Too many objects are crowded at the edge of the extent. Can't split.
                return false;
            }
        } else if split_coord <= extent_min.y || split_coord >= extent_max.y {
            // Too many objects are crowded at the edge of the extent. Can't split.
            return false;
        }

        // Create child extents
        let (left_extent, right_extent) = if split_x {
            let left = Rect::new(
                extent_min,
                Coord {
                    x: split_coord,
                    y: extent_max.y,
                },
            );
            let right = Rect::new(
                Coord {
                    x: split_coord,
                    y: extent_min.y,
                },
                extent_max,
            );
            (left, right)
        } else {
            let left = Rect::new(
                extent_min,
                Coord {
                    x: extent_max.x,
                    y: split_coord,
                },
            );
            let right = Rect::new(
                Coord {
                    x: extent_min.x,
                    y: split_coord,
                },
                extent_max,
            );
            (left, right)
        };

        // Create children
        let mut left_child = KDBTree::new_with_level(
            self.max_items_per_node,
            self.max_levels,
            self.level + 1,
            left_extent,
        );
        let mut right_child = KDBTree::new_with_level(
            self.max_items_per_node,
            self.max_levels,
            self.level + 1,
            right_extent,
        );

        // Distribute items to children
        if split_x {
            for item in self.items.drain(..) {
                if item.min().x <= split_coord {
                    left_child.insert_rect(item);
                } else {
                    right_child.insert_rect(item);
                }
            }
        } else {
            for item in self.items.drain(..) {
                if item.min().y <= split_coord {
                    left_child.insert_rect(item);
                } else {
                    right_child.insert_rect(item);
                }
            }
        }

        self.children = Some(Box::new([left_child, right_child]));
        true
    }
}

/// KDB tree partitioner that implements the `SpatialPartitioner` trait.
#[derive(Clone)]
pub struct KDBPartitioner {
    tree: Arc<KDBTree>,
}

impl KDBPartitioner {
    /// Create a new KDB partitioner from a KDB tree.
    ///
    /// The tree should already be built and have leaf IDs assigned.
    pub fn new(tree: Arc<KDBTree>) -> Self {
        KDBPartitioner { tree }
    }

    /// Build a KDB tree from a collection of bounding boxes.
    ///
    /// # Arguments
    /// * `bboxes` - Iterator of bounding boxes to partition
    /// * `max_items_per_node` - Maximum items per node before splitting
    /// * `max_levels` - Maximum tree depth
    /// * `extent` - The spatial extent to partition
    pub fn build(
        bboxes: impl Iterator<Item = BoundingBox>, max_items_per_node: usize, max_levels: usize,
        extent: BoundingBox,
    ) -> Result<Self> {
        let mut tree = KDBTree::try_new(max_items_per_node, max_levels, extent)?;

        for bbox in bboxes {
            tree.insert(bbox)?;
        }

        tree.assign_leaf_ids();
        tree.drop_elements();

        Ok(Self::new(Arc::new(tree)))
    }

    /// Get the number of leaf partitions in the tree.
    pub fn num_partitions(&self) -> usize {
        self.tree.num_leaf_nodes()
    }

    /// Write the tree structure in human-readable format for debugging
    /// purposes.
    pub fn debug_print(&self, f: &mut impl std::fmt::Write) -> Result<()> {
        let leaves = self.tree.collect_leaf_nodes();
        for leaf in leaves {
            let extent = format!(
                "x: [{:.6}, {:.6}], y: [{:.6}, {:.6}]",
                leaf.extent().min().x,
                leaf.extent().max().x,
                leaf.extent().min().y,
                leaf.extent().max().y,
            );
            writeln!(f, "Leaf ID: {}, Extent: {}", leaf.leaf_id(), extent)?;
        }
        Ok(())
    }

    /// Return the tree structure in human-readable format for debugging
    /// purposes.
    pub fn debug_str(&self) -> String {
        let mut output = String::new();
        let _ = self.debug_print(&mut output);
        output
    }
}

impl SpatialPartitioner for KDBPartitioner {
    fn num_regular_partitions(&self) -> usize {
        self.num_partitions()
    }

    fn partition(&self, bbox: &BoundingBox) -> Result<SpatialPartition> {
        let Some(rect) = bbox_to_geo_rect(bbox)? else {
            return Ok(SpatialPartition::None);
        };
        let mut sp: Option<SpatialPartition> = None;

        self.tree.visit_intersecting_leaf_nodes(&rect, &mut |kdb| {
            if sp.is_none() {
                sp = Some(SpatialPartition::Regular(kdb.leaf_id()));
            } else {
                sp = Some(SpatialPartition::Multi)
            }
        });

        Ok(sp.unwrap_or(SpatialPartition::None))
    }

    fn partition_no_multi(&self, bbox: &BoundingBox) -> Result<SpatialPartition> {
        let Some(rect) = bbox_to_geo_rect(bbox)? else {
            return Ok(SpatialPartition::None);
        };

        let mut best_leaf_id: Option<u32> = None;
        let mut max_area = -1.0_f32;

        self.tree.visit_intersecting_leaf_nodes(&rect, &mut |kdb| {
            let area = rect_intersection_area(&rect, kdb.extent());
            if area > max_area {
                best_leaf_id = Some(kdb.leaf_id());
                max_area = area;
            }
        });

        match best_leaf_id {
            Some(leaf_id) => Ok(SpatialPartition::Regular(leaf_id)),
            None => Ok(SpatialPartition::None),
        }
    }

    fn box_clone(&self) -> Box<dyn SpatialPartitioner> {
        Box::new(self.clone())
    }
}

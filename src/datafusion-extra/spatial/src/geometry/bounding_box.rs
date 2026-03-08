use anyhow::Result;
use serde::{Deserialize, Serialize};

use crate::geometry::interval::{Interval, IntervalTrait, WraparoundInterval};

/// Bounding Box implementation with wraparound support
///
/// Conceptually, this `BoundingBox` is a [`WraparoundInterval`] (x), an
/// [Interval] (y). This `BoundingBox` intentionally separates the case where no
/// information was provided (i.e., there is no information about the presence
/// or absence of values in a given dimension) and [`Interval::empty`] (i.e., we
/// are absolutely and positively sure there are zero values present for a given
/// dimension).
///
/// This structure implements Serialize and Deserialize to support passing it
/// between query engine components where there is not yet a mechanism to do so.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BoundingBox {
    x: WraparoundInterval,
    y: Interval,
}

impl BoundingBox {
    /// Create a `BoundingBox`
    pub fn xy(x: impl Into<WraparoundInterval>, y: impl Into<Interval>) -> Self {
        Self {
            x: x.into(),
            y: y.into(),
        }
    }

    /// Create an empty `BoundingBox`
    pub fn empty() -> Self {
        Self {
            x: WraparoundInterval::empty(),
            y: Interval::empty(),
        }
    }

    /// The x interval
    pub fn x(&self) -> &WraparoundInterval {
        &self.x
    }

    /// The y interval
    pub fn y(&self) -> &Interval {
        &self.y
    }

    /// Check whether this `BoundingBox` is empty
    pub fn is_empty(&self) -> bool {
        self.x.is_empty() || self.y.is_empty()
    }

    /// Calculate intersection with another `BoundingBox`
    ///
    /// Returns true if this bounding box may intersect other or false
    /// otherwise. This method will consider Z and M dimension if and only
    /// if those dimensions are present in both bounding boxes.
    pub fn intersects(&self, other: &Self) -> bool {
        self.x.intersects_interval(&other.x) && self.y.intersects_interval(&other.y)
    }

    /// Update this `BoundingBox` to include the bounds of another
    ///
    /// This method will propagate missingness of Z or M dimensions from the two
    /// boxes (e.g., Z will be `None` if Z if `self.z().is_none()` OR
    /// `other.z().is_none()`). Note that this method is intended for
    /// accumulating bounds at the file level and is not performant for
    /// accumulating bounds for individual geometries. For this case, use
    /// a set of [Interval]s, (perhaps merging them into [`WraparoundInterval`]s
    /// at the geometry or array level if working with longitudes and
    /// latitudes and the performance overhead is acceptable).
    pub fn update_box(&mut self, other: &Self) {
        self.x = self.x.merge_interval(&other.x);
        self.y = self.y.merge_interval(&other.y);
    }

    /// Compute the intersection of this bounding box with another
    ///
    /// This method will propagate missingness of Z or M dimensions from the two
    /// boxes (e.g., Z will be `None` if Z if `self.z().is_none()` OR
    /// `other.z().is_none()`).
    pub fn intersection(&self, other: &Self) -> Result<Self> {
        Ok(Self {
            x: self.x.intersection(&other.x)?,
            y: self.y.intersection(&other.y)?,
        })
    }
}

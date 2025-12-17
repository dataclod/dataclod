use std::collections::HashSet;

use datafusion::common::{DataFusionError, Result, ScalarValue};
use serde::{Deserialize, Serialize};

use crate::geometry::bounding_box::BoundingBox;
use crate::geometry::interval::{Interval, IntervalTrait};
use crate::geometry::types::GeometryTypeAndDimensions;

/// Statistics specific to spatial data types
///
/// These statistics are an abstraction to provide sedonadb the ability to
/// perform generic pruning and optimization for datasources that have the
/// ability to provide this information. This may evolve to support more
/// fields; however, can currently express Parquet built-in `GeoStatistics`,
/// `GeoParquet` metadata, and GDAL OGR (via `GetExtent()` and `GetGeomType()`).
/// This struct can also represent partial or missing information.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GeoStatistics {
    // Core spatial statistics for pruning
    bbox: Option<BoundingBox>, /* The overall bounding box (min/max coordinates) containing all
                                * geometries */
    geometry_types: Option<HashSet<GeometryTypeAndDimensions>>, /* Set of all geometry types and
                                                                 * dimensions present */

    // Extended statistics for analysis
    total_geometries: Option<i64>, // Total count of all geometries
    total_size_bytes: Option<i64>, // Total size of all geometries in bytes
    total_points: Option<i64>,     // Total number of points/vertices across all geometries

    // Type distribution counts
    puntal_count: Option<i64>,     // Count of point-type geometries
    lineal_count: Option<i64>,     // Count of line-type geometries
    polygonal_count: Option<i64>,  // Count of polygon-type geometries
    collection_count: Option<i64>, // Count of geometry collections

    // Envelope dimensions statistics
    total_envelope_width: Option<f64>, // Sum of all envelope widths (for calculating mean width)
    total_envelope_height: Option<f64>, // Sum of all envelope heights (for calculating mean height)
}

impl GeoStatistics {
    /// Create statistics representing empty information (with zero values
    /// instead of None)
    pub fn empty() -> Self {
        Self {
            bbox: Some(BoundingBox::xy(Interval::empty(), Interval::empty())),
            geometry_types: Some(HashSet::new()), // Empty set of geometry types
            total_geometries: Some(0),            // Zero geometries
            total_size_bytes: Some(0),            // Zero bytes
            total_points: Some(0),                // Zero points
            puntal_count: Some(0),                // Zero point geometries
            lineal_count: Some(0),                // Zero line geometries
            polygonal_count: Some(0),             // Zero polygon geometries
            collection_count: Some(0),            // Zero collection geometries
            total_envelope_width: Some(0.0),      // Zero width
            total_envelope_height: Some(0.0),     // Zero height
        }
    }

    /// Update the bounding box and return self
    pub fn with_bbox(self, bbox: Option<BoundingBox>) -> Self {
        Self { bbox, ..self }
    }

    /// Update the geometry types and return self
    pub fn with_geometry_types(self, types: Option<&[GeometryTypeAndDimensions]>) -> Self {
        match types {
            Some(type_slice) => {
                let type_set: HashSet<GeometryTypeAndDimensions> =
                    type_slice.iter().cloned().collect();
                Self {
                    geometry_types: Some(type_set),
                    ..self
                }
            }
            None => {
                Self {
                    geometry_types: None,
                    ..self
                }
            }
        }
    }

    /// Get the bounding box if available
    pub fn bbox(&self) -> Option<&BoundingBox> {
        self.bbox.as_ref()
    }

    /// Get the geometry types if available
    pub fn geometry_types(&self) -> Option<&HashSet<GeometryTypeAndDimensions>> {
        self.geometry_types.as_ref()
    }

    /// Get the total number of geometries if available
    pub fn total_geometries(&self) -> Option<i64> {
        self.total_geometries
    }

    /// Get the total size in bytes if available
    pub fn total_size_bytes(&self) -> Option<i64> {
        self.total_size_bytes
    }

    /// Get the total number of points if available
    pub fn total_points(&self) -> Option<i64> {
        self.total_points
    }

    /// Get the count of puntal geometries if available
    pub fn puntal_count(&self) -> Option<i64> {
        self.puntal_count
    }

    /// Get the count of lineal geometries if available
    pub fn lineal_count(&self) -> Option<i64> {
        self.lineal_count
    }

    /// Get the count of polygonal geometries if available
    pub fn polygonal_count(&self) -> Option<i64> {
        self.polygonal_count
    }

    /// Get the count of geometry collections if available
    pub fn collection_count(&self) -> Option<i64> {
        self.collection_count
    }

    /// Get the total envelope width if available
    pub fn total_envelope_width(&self) -> Option<f64> {
        self.total_envelope_width
    }

    /// Get the total envelope height if available
    pub fn total_envelope_height(&self) -> Option<f64> {
        self.total_envelope_height
    }

    /// Calculate the mean points per geometry if possible
    pub fn mean_points_per_geometry(&self) -> Option<f64> {
        match (self.total_points, self.total_geometries) {
            (Some(points), Some(count)) if count > 0 => Some(points as f64 / count as f64),
            _ => None,
        }
    }

    /// Update the total geometries count and return self
    pub fn with_total_geometries(self, count: i64) -> Self {
        Self {
            total_geometries: Some(count),
            ..self
        }
    }

    /// Update the total size in bytes and return self
    pub fn with_total_size_bytes(self, bytes: i64) -> Self {
        Self {
            total_size_bytes: Some(bytes),
            ..self
        }
    }

    /// Update the total points count and return self
    pub fn with_total_points(self, points: i64) -> Self {
        Self {
            total_points: Some(points),
            ..self
        }
    }

    /// Update the puntal geometries count and return self
    pub fn with_puntal_count(self, count: i64) -> Self {
        Self {
            puntal_count: Some(count),
            ..self
        }
    }

    /// Update the lineal geometries count and return self
    pub fn with_lineal_count(self, count: i64) -> Self {
        Self {
            lineal_count: Some(count),
            ..self
        }
    }

    /// Update the polygonal geometries count and return self
    pub fn with_polygonal_count(self, count: i64) -> Self {
        Self {
            polygonal_count: Some(count),
            ..self
        }
    }

    /// Update the collection geometries count and return self
    pub fn with_collection_count(self, count: i64) -> Self {
        Self {
            collection_count: Some(count),
            ..self
        }
    }

    /// Update the total envelope width and return self
    pub fn with_total_envelope_width(self, width: f64) -> Self {
        Self {
            total_envelope_width: Some(width),
            ..self
        }
    }

    /// Update the total envelope height and return self
    pub fn with_total_envelope_height(self, height: f64) -> Self {
        Self {
            total_envelope_height: Some(height),
            ..self
        }
    }

    /// Update this statistics object with another one
    pub fn merge(&mut self, other: &Self) {
        // Merge bounding boxes
        if let Some(other_bbox) = &other.bbox {
            match &mut self.bbox {
                Some(bbox) => bbox.update_box(other_bbox),
                None => self.bbox = Some(other_bbox.clone()),
            }
        }

        // Merge geometry types
        if let Some(other_types) = &other.geometry_types {
            match &mut self.geometry_types {
                Some(types) => {
                    let mut new_types = types.clone();
                    new_types.extend(other_types.iter().cloned());
                    self.geometry_types = Some(new_types);
                }
                None => self.geometry_types = Some(other_types.clone()),
            }
        }

        // Merge counts and totals
        self.total_geometries =
            Self::merge_option_add(self.total_geometries, other.total_geometries);
        self.total_size_bytes =
            Self::merge_option_add(self.total_size_bytes, other.total_size_bytes);
        self.total_points = Self::merge_option_add(self.total_points, other.total_points);

        // Merge type counts
        self.puntal_count = Self::merge_option_add(self.puntal_count, other.puntal_count);
        self.lineal_count = Self::merge_option_add(self.lineal_count, other.lineal_count);
        self.polygonal_count = Self::merge_option_add(self.polygonal_count, other.polygonal_count);
        self.collection_count =
            Self::merge_option_add(self.collection_count, other.collection_count);

        // Merge envelope dimensions
        self.total_envelope_width =
            Self::merge_option_add_f64(self.total_envelope_width, other.total_envelope_width);
        self.total_envelope_height =
            Self::merge_option_add_f64(self.total_envelope_height, other.total_envelope_height);
    }

    // Helper to merge two optional integers with addition
    fn merge_option_add(a: Option<i64>, b: Option<i64>) -> Option<i64> {
        match (a, b) {
            (Some(a_val), Some(b_val)) => Some(a_val + b_val),
            _ => None,
        }
    }

    // Helper to merge two optional floats with addition
    fn merge_option_add_f64(a: Option<f64>, b: Option<f64>) -> Option<f64> {
        match (a, b) {
            (Some(a_val), Some(b_val)) => Some(a_val + b_val),
            _ => None,
        }
    }

    /// Convert this `GeoStatistics` to a `ScalarValue` for storage in
    /// `DataFusion` statistics
    pub fn to_scalar_value(&self) -> Result<ScalarValue> {
        // Serialize to JSON
        let serialized = serde_json::to_vec(self).map_err(|e| {
            DataFusionError::Internal(format!("Failed to serialize GeoStatistics: {e}"))
        })?;

        Ok(ScalarValue::Binary(Some(serialized)))
    }
}

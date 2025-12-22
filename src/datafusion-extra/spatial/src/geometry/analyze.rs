use std::sync::Arc;

use anyhow::Result;
use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, Float64Array, Float64Builder, Int64Array, Int64Builder, StructArray,
};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::Result as DFResult;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::Accumulator;
use datafusion::scalar::ScalarValue;
use geo_traits::{GeometryTrait, GeometryType};
use wkb::reader::Wkb;

use crate::geometry::bounding_box::BoundingBox;
use crate::geometry::bounds::geo_traits_update_xy_bounds;
use crate::geometry::interval::{Interval, IntervalTrait};
use crate::geometry::point_count::count_points;
use crate::geometry::types::{GeometryTypeAndDimensions, GeometryTypeId};
use crate::statistics::GeoStatistics;

/// Contains analysis results for a geometry
#[derive(Debug, Clone)]
pub struct GeometryAnalysis {
    pub point_count: i64,
    pub geometry_type: GeometryTypeAndDimensions,
    pub bbox: BoundingBox,
    pub puntal_count: i64,
    pub lineal_count: i64,
    pub polygonal_count: i64,
    pub collection_count: i64,
}

/// Analyzes a WKB geometry and returns its size, point count, dimensions, and
/// type
pub fn analyze_geometry(geom: &Wkb) -> Result<GeometryAnalysis> {
    // Get geometry type using as_type() which is public
    let geom_type = geom.as_type();
    let wkb_type_id = match geom_type {
        GeometryType::Point(_) => 1,
        GeometryType::LineString(_) => 2,
        GeometryType::Polygon(_) => 3,
        GeometryType::MultiPoint(_) => 4,
        GeometryType::MultiLineString(_) => 5,
        GeometryType::MultiPolygon(_) => 6,
        GeometryType::GeometryCollection(_) => 7,
        _ => 0,
    };

    // Handle the Result properly
    let geometry_type_id =
        GeometryTypeId::try_from_wkb_id(wkb_type_id).unwrap_or(GeometryTypeId::Geometry);

    let geometry_type = GeometryTypeAndDimensions::new(geometry_type_id, geom.dim());

    // Get point count directly using the geometry traits
    let point_count = count_points(geom);

    // Calculate bounding box using geo_traits_update_xy_bounds
    let mut x = Interval::empty();
    let mut y = Interval::empty();
    geo_traits_update_xy_bounds(geom, &mut x, &mut y)?;
    let bbox = BoundingBox::xy(x, y);

    // Determine geometry type counts directly
    let puntal_count = matches!(
        geom_type,
        GeometryType::Point(_) | GeometryType::MultiPoint(_)
    ) as i64;

    let lineal_count = matches!(
        geom_type,
        GeometryType::LineString(_) | GeometryType::Line(_) | GeometryType::MultiLineString(_)
    ) as i64;

    let polygonal_count = matches!(
        geom_type,
        GeometryType::Polygon(_) | GeometryType::MultiPolygon(_)
    ) as i64;

    let collection_count = matches!(geom_type, GeometryType::GeometryCollection(_)) as i64;

    Ok(GeometryAnalysis {
        point_count,
        geometry_type,
        bbox,
        puntal_count,
        lineal_count,
        polygonal_count,
        collection_count,
    })
}

#[derive(Debug)]
pub struct AnalyzeAccumulator {
    stats: GeoStatistics,
}

impl AnalyzeAccumulator {
    pub fn new() -> Self {
        Self {
            stats: GeoStatistics::empty(),
        }
    }

    pub fn update_statistics(&mut self, geom: &Wkb, size_bytes: usize) -> DFResult<()> {
        // Get geometry analysis information
        let analysis = analyze_geometry(geom)
            .map_err(|e| DataFusionError::External(e.into_boxed_dyn_error()))?;

        // Start with a clone of the current stats
        let mut stats = self.stats.clone();

        // Update each component of the statistics
        stats = self.update_basic_counts(stats, size_bytes);
        stats = self.update_geometry_type_counts(stats, &analysis);
        stats = self.update_point_count(stats, analysis.point_count);
        stats = self.update_envelope_info(stats, &analysis);
        stats = self.update_geometry_types(stats, analysis.geometry_type);

        // Assign the updated stats back to self.stats
        self.stats = stats;

        Ok(())
    }

    pub fn finish(self) -> GeoStatistics {
        self.stats
    }

    // Update basic counts (total geometries and size)
    fn update_basic_counts(&self, stats: GeoStatistics, size_bytes: usize) -> GeoStatistics {
        let total_geometries = stats.total_geometries().unwrap_or(0) + 1;
        stats
            .clone()
            .with_total_geometries(total_geometries)
            .with_total_size_bytes(stats.total_size_bytes().unwrap_or(0) + size_bytes as i64)
    }

    // Update geometry type counts
    fn update_geometry_type_counts(
        &self, stats: GeoStatistics, analysis: &GeometryAnalysis,
    ) -> GeoStatistics {
        // Add the counts from analysis to existing stats
        let puntal = stats.puntal_count().unwrap_or(0) + analysis.puntal_count;
        let lineal = stats.lineal_count().unwrap_or(0) + analysis.lineal_count;
        let polygonal = stats.polygonal_count().unwrap_or(0) + analysis.polygonal_count;
        let collection = stats.collection_count().unwrap_or(0) + analysis.collection_count;

        stats
            .with_puntal_count(puntal)
            .with_lineal_count(lineal)
            .with_polygonal_count(polygonal)
            .with_collection_count(collection)
    }

    // Update point count statistics
    fn update_point_count(&self, stats: GeoStatistics, point_count: i64) -> GeoStatistics {
        let total_points = stats.total_points().unwrap_or(0) + point_count;
        stats.with_total_points(total_points)
    }

    // Update envelope dimensions and bounding box
    fn update_envelope_info(
        &self, stats: GeoStatistics, analysis: &GeometryAnalysis,
    ) -> GeoStatistics {
        // The bbox is directly available on analysis, not wrapped in an Option
        let bbox = &analysis.bbox;

        // Calculate envelope width and height from the bbox
        let envelope_width = if bbox.x().is_empty() {
            0.0
        } else {
            bbox.x().width()
        };
        let envelope_height = if bbox.y().is_empty() {
            0.0
        } else {
            bbox.y().width()
        };

        // Update envelope dimensions
        let total_width = stats.total_envelope_width().unwrap_or(0.0) + envelope_width;
        let total_height = stats.total_envelope_height().unwrap_or(0.0) + envelope_height;

        let stats = stats
            .with_total_envelope_width(total_width)
            .with_total_envelope_height(total_height);

        // Update bounding box
        let existing_bbox = stats.bbox();
        if let Some(current_bbox) = existing_bbox {
            let mut updated_bbox = bbox.clone();
            updated_bbox.update_box(current_bbox);
            stats.with_bbox(Some(updated_bbox))
        } else {
            stats.with_bbox(Some(bbox.clone()))
        }
    }

    // Update geometry types
    fn update_geometry_types(
        &self, stats: GeoStatistics, geometry_type: GeometryTypeAndDimensions,
    ) -> GeoStatistics {
        let current_types = stats.geometry_types();
        let types = if let Some(existing_types) = current_types {
            let mut new_types = existing_types.clone();
            new_types.insert(geometry_type);
            Some(new_types)
        } else {
            Some(std::collections::HashSet::from([geometry_type]))
        };

        if let Some(type_set) = &types {
            let type_vec: Vec<GeometryTypeAndDimensions> = type_set.iter().cloned().collect();
            stats.with_geometry_types(Some(&type_vec))
        } else {
            stats.with_geometry_types(None)
        }
    }
}

impl Accumulator for AnalyzeAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> DFResult<()> {
        if values.is_empty() {
            return Err(DataFusionError::Internal(
                "No input arrays provided to accumulator".to_owned(),
            ));
        }
        let wkb_array = values[0].as_binary_view();
        for wkb in wkb_array {
            if let Some(wkb) = wkb
                && let Ok(wkb) = Wkb::try_new(wkb)
            {
                self.update_statistics(&wkb, wkb.buf().len())?;
            }
        }
        Ok(())
    }

    fn evaluate(&mut self) -> DFResult<ScalarValue> {
        let field_array_pairs = output_arrays(self.stats.clone());

        // Create the struct array with field values
        let struct_array = StructArray::from(field_array_pairs);

        // Return the ScalarValue::Struct with Arc<StructArray>
        Ok(ScalarValue::Struct(Arc::new(struct_array)))
    }

    fn size(&self) -> usize {
        let base_size = size_of_val(self);

        // Add approximate size for bbox if present
        let bbox_size = match self.stats.bbox() {
            Some(bbox) => size_of_val(bbox),
            None => 0,
        };

        // Add approximate size for geometry types if present
        let types_size = match self.stats.geometry_types() {
            Some(types) => {
                let elem_size = size_of::<GeometryTypeAndDimensions>();
                let capacity = types.capacity();
                capacity * elem_size
            }
            None => 0,
        };

        base_size + bbox_size + types_size
    }

    fn state(&mut self) -> DFResult<Vec<ScalarValue>> {
        if self.stats.total_geometries().unwrap_or(0) == 0 {
            // Return null if no data was processed
            return Ok(vec![ScalarValue::BinaryView(None)]);
        }

        // Serialize the statistics to JSON
        let scalar = self.stats.to_scalar_value()?;
        Ok(vec![scalar])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> DFResult<()> {
        // Check input length (expecting 1 state field)
        if states.is_empty() {
            return Err(DataFusionError::Internal(
                "No input arrays provided to accumulator in merge_batch".to_owned(),
            ));
        }

        let array = &states[0];
        let binary_array = array.as_binary_view();

        for i in 0..binary_array.len() {
            if binary_array.is_null(i) {
                continue;
            }

            let serialized = binary_array.value(i);
            let other_stats: GeoStatistics = serde_json::from_slice(serialized).map_err(|e| {
                DataFusionError::Internal(format!("Failed to deserialize stats: {e}"))
            })?;

            // Use the merge method to combine statistics
            self.stats.merge(&other_stats);
        }

        Ok(())
    }
}

fn output_arrays(stats: GeoStatistics) -> Vec<(FieldRef, ArrayRef)> {
    // Get total geometries count
    let total_geometries = stats.total_geometries().unwrap_or(0);

    // Handle bounding box values for empty collections
    let (min_x, min_y, max_x, max_y) = if let Some(bbox) = stats.bbox() {
        // Get actual values from the bbox
        (
            Some(bbox.x().lo()),
            Some(bbox.y().lo()),
            Some(bbox.x().hi()),
            Some(bbox.y().hi()),
        )
    } else if total_geometries == 0 {
        // When no geometries, use null values for bounds
        (None, None, None, None)
    } else {
        // This case shouldn't happen but default to zeros
        (Some(0.0), Some(0.0), Some(0.0), Some(0.0))
    };

    // Calculate means - use None for empty collections
    let mean_size_bytes = if total_geometries > 0 {
        Some(stats.total_size_bytes().unwrap_or(0) / total_geometries)
    } else {
        None
    };

    let mean_points = if total_geometries > 0 {
        Some(stats.total_points().unwrap_or(0) as f64 / total_geometries as f64)
    } else {
        None
    };

    // Calculate mean envelope dimensions
    let mean_envelope_width = if total_geometries > 0 {
        Some(stats.total_envelope_width().unwrap_or(0.0) / total_geometries as f64)
    } else {
        None
    };

    let mean_envelope_height = if total_geometries > 0 {
        Some(stats.total_envelope_height().unwrap_or(0.0) / total_geometries as f64)
    } else {
        None
    };

    let mean_envelope_area = if total_geometries > 0 {
        mean_envelope_width
            .zip(mean_envelope_height)
            .map(|(w, h)| w * h)
    } else {
        None
    };

    // Define output fields
    let fields = vec![
        Field::new("count", DataType::Int64, true),
        Field::new("minx", DataType::Float64, true),
        Field::new("miny", DataType::Float64, true),
        Field::new("maxx", DataType::Float64, true),
        Field::new("maxy", DataType::Float64, true),
        Field::new("mean_size_in_bytes", DataType::Int64, true),
        Field::new("mean_points_per_geometry", DataType::Float64, true),
        Field::new("puntal_count", DataType::Int64, true),
        Field::new("lineal_count", DataType::Int64, true),
        Field::new("polygonal_count", DataType::Int64, true),
        Field::new("geometrycollection_count", DataType::Int64, true),
        Field::new("mean_envelope_width", DataType::Float64, true),
        Field::new("mean_envelope_height", DataType::Float64, true),
        Field::new("mean_envelope_area", DataType::Float64, true),
    ];

    // Create arrays with proper null handling
    let values: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(vec![total_geometries])),
        Arc::new(create_float64_array(min_x)),
        Arc::new(create_float64_array(min_y)),
        Arc::new(create_float64_array(max_x)),
        Arc::new(create_float64_array(max_y)),
        Arc::new(create_int64_array(mean_size_bytes)),
        Arc::new(create_float64_array(mean_points)),
        Arc::new(Int64Array::from(vec![stats.puntal_count().unwrap_or(0)])),
        Arc::new(Int64Array::from(vec![stats.lineal_count().unwrap_or(0)])),
        Arc::new(Int64Array::from(vec![stats.polygonal_count().unwrap_or(0)])),
        Arc::new(Int64Array::from(vec![
            stats.collection_count().unwrap_or(0),
        ])),
        Arc::new(create_float64_array(mean_envelope_width)),
        Arc::new(create_float64_array(mean_envelope_height)),
        Arc::new(create_float64_array(mean_envelope_area)),
    ];

    // Pair fields with values
    fields.into_iter().map(Arc::new).zip(values).collect()
}

fn create_int64_array(v: Option<i64>) -> Int64Array {
    let mut builder = Int64Builder::new();
    builder.append_option(v);
    builder.finish()
}

fn create_float64_array(v: Option<f64>) -> Float64Array {
    let mut builder = Float64Builder::new();
    builder.append_option(v);
    builder.finish()
}

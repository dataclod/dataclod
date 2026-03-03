use std::fmt::Display;
use std::str::FromStr;

use anyhow::{Error, Result};
use geo_traits::{Dimensions, GeometryTrait};
use serde::{Deserialize, Serialize};
use serde_with::{DeserializeFromStr, SerializeDisplay};

/// Geometry types
///
/// An enumerator for the set of natively supported geometry types without
/// considering [Dimensions]. See [`GeometryTypeAndDimensions`] for a struct to
/// track both.
///
/// This is named `GeometryTypeId` such that it does not conflict with
/// [`geo_traits::GeometryType`].
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Hash, Clone, Copy)]
pub enum GeometryTypeId {
    /// Unknown or mixed geometry type
    Geometry,
    /// Point geometry type
    Point,
    /// `LineString` geometry type
    LineString,
    /// Polygon geometry type
    Polygon,
    /// `MultiPoint` geometry type
    MultiPoint,
    /// `MultiLineString` geometry type
    MultiLineString,
    /// `MultiPolygon` geometry type
    MultiPolygon,
    /// `GeometryCollection` geometry type
    GeometryCollection,
}

impl GeometryTypeId {
    /// Construct a geometry type from a WKB type integer
    ///
    /// Parses the geometry type (not dimension) component of a WKB type code
    /// (e.g., 1 for Point...7 for `GeometryCollection`).
    pub fn try_from_wkb_id(wkb_id: u32) -> Result<Self> {
        match wkb_id {
            0 => Ok(Self::Geometry),
            1 => Ok(Self::Point),
            2 => Ok(Self::LineString),
            3 => Ok(Self::Polygon),
            4 => Ok(Self::MultiPoint),
            5 => Ok(Self::MultiLineString),
            6 => Ok(Self::MultiPolygon),
            7 => Ok(Self::GeometryCollection),
            _ => Err(anyhow::anyhow!("Unknown geometry type identifier {wkb_id}")),
        }
    }

    /// WKB integer identifier
    ///
    /// The `GeometryType` portion of the WKB identifier (e.g., 1 for Point...7
    /// for `GeometryCollection`).
    pub fn wkb_id(&self) -> u32 {
        match self {
            Self::Geometry => 0,
            Self::Point => 1,
            Self::LineString => 2,
            Self::Polygon => 3,
            Self::MultiPoint => 4,
            Self::MultiLineString => 5,
            Self::MultiPolygon => 6,
            Self::GeometryCollection => 7,
        }
    }

    /// GeoJSON/GeoParquet string identifier
    ///
    /// The identifier used by `GeoJSON` and `GeoParquet` to refer to this
    /// geometry type. Use [`FromStr`] to parse such a string back into a
    /// `GeometryTypeId`.
    pub fn geojson_id(&self) -> &'static str {
        match self {
            Self::Geometry => "Geometry",
            Self::Point => "Point",
            Self::LineString => "LineString",
            Self::Polygon => "Polygon",
            Self::MultiPoint => "MultiPoint",
            Self::MultiLineString => "MultiLineString",
            Self::MultiPolygon => "MultiPolygon",
            Self::GeometryCollection => "GeometryCollection",
        }
    }
}

impl FromStr for GeometryTypeId {
    type Err = Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let value_lower = value.to_ascii_lowercase();
        match value_lower.as_str() {
            "geometry" => Ok(Self::Geometry),
            "point" => Ok(Self::Point),
            "linestring" => Ok(Self::LineString),
            "polygon" => Ok(Self::Polygon),
            "multipoint" => Ok(Self::MultiPoint),
            "multilinestring" => Ok(Self::MultiLineString),
            "multipolygon" => Ok(Self::MultiPolygon),
            "geometrycollection" => Ok(Self::GeometryCollection),
            _ => Err(anyhow::anyhow!("Invalid geometry type string: '{value}'")),
        }
    }
}

/// Geometry type and dimension
///
/// Combines a [`GeometryTypeId`] with [Dimensions] to handle cases where these
/// concepts are represented together (e.g., `GeoParquet` geometry types, WKB
/// geometry type integers, Parquet `GeoStatistics`). For sanity's sake, this
/// combined concept is also frequently just called "geometry type".
#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy, SerializeDisplay, DeserializeFromStr)]
pub struct GeometryTypeAndDimensions {
    geometry_type: GeometryTypeId,
    dimensions: Dimensions,
}

impl GeometryTypeAndDimensions {
    /// Create from [`GeometryTypeId`] and [Dimensions]
    pub fn new(geometry_type: GeometryTypeId, dimensions: Dimensions) -> Self {
        Self {
            geometry_type,
            dimensions,
        }
    }

    /// Create from [`GeometryTrait`]
    pub fn try_from_geom(geom: &impl GeometryTrait) -> Result<Self> {
        let dimensions = geom.dim();
        let geometry_type = match geom.as_type() {
            geo_traits::GeometryType::Point(_) => GeometryTypeId::Point,
            geo_traits::GeometryType::LineString(_) => GeometryTypeId::LineString,
            geo_traits::GeometryType::Polygon(_) => GeometryTypeId::Polygon,
            geo_traits::GeometryType::MultiPoint(_) => GeometryTypeId::MultiPoint,
            geo_traits::GeometryType::MultiLineString(_) => GeometryTypeId::MultiLineString,
            geo_traits::GeometryType::MultiPolygon(_) => GeometryTypeId::MultiPolygon,
            geo_traits::GeometryType::GeometryCollection(_) => GeometryTypeId::GeometryCollection,
            _ => {
                return Err(anyhow::anyhow!("Unsupported geometry type"));
            }
        };

        Ok(Self::new(geometry_type, dimensions))
    }

    /// The [`GeometryTypeId`]
    pub fn geometry_type(&self) -> GeometryTypeId {
        self.geometry_type
    }

    /// The [Dimensions]
    pub fn dimensions(&self) -> Dimensions {
        self.dimensions
    }

    /// Create from an ISO WKB integer identifier (e.g., 1001 for Point Z)
    pub fn try_from_wkb_id(wkb_id: u32) -> Result<Self> {
        let dimensions = match wkb_id / 1000 {
            0 => Dimensions::Xy,
            1 => Dimensions::Xyz,
            2 => Dimensions::Xym,
            3 => Dimensions::Xyzm,
            _ => {
                return Err(anyhow::anyhow!(
                    "Unknown dimensions in ISO WKB geometry type: {wkb_id}"
                ));
            }
        };

        let geometry_type = GeometryTypeId::try_from_wkb_id(wkb_id % 1000)?;
        Ok(Self {
            geometry_type,
            dimensions,
        })
    }

    /// ISO WKB integer identifier (e.g., 1001 for Point Z)
    pub fn wkb_id(&self) -> u32 {
        let dimensions_id = match self.dimensions {
            Dimensions::Xy => 0,
            Dimensions::Xyz => 1000,
            Dimensions::Xym => 2000,
            Dimensions::Xyzm => 3000,
            Dimensions::Unknown(n) => {
                match n {
                    2 => 0,
                    3 => 1000,
                    4 => 3000,
                    _ => {
                        // Avoid a panic unless in debug mode
                        debug_assert!(false, "Unknown dimensions in GeometryTypeAndDimensions");
                        0
                    }
                }
            }
        };

        dimensions_id + self.geometry_type.wkb_id()
    }

    /// GeoJSON/GeoParquet identifier (e.g., Point Z, `LineString`, Polygon ZM)
    pub fn geojson_id(&self) -> String {
        self.to_string()
    }
}

impl From<(GeometryTypeId, Dimensions)> for GeometryTypeAndDimensions {
    fn from(value: (GeometryTypeId, Dimensions)) -> Self {
        Self {
            geometry_type: value.0,
            dimensions: value.1,
        }
    }
}

impl Display for GeometryTypeAndDimensions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let suffix = match self.dimensions {
            Dimensions::Xy => "",
            Dimensions::Xyz => " Z",
            Dimensions::Xym => " M",
            Dimensions::Xyzm => " ZM",
            Dimensions::Unknown(_) => " Unknown",
        };

        f.write_str(self.geometry_type.geojson_id())?;
        f.write_str(suffix)
    }
}

impl FromStr for GeometryTypeAndDimensions {
    type Err = Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let mut parts = value.split_ascii_whitespace();
        let geometry_type = match parts.next() {
            Some(maybe_geometry_type) => GeometryTypeId::from_str(maybe_geometry_type)?,
            None => {
                return Err(anyhow::anyhow!("Invalid geometry type string: '{value}'"));
            }
        };

        let dimensions = match parts.next() {
            Some(maybe_dimensions) => {
                match maybe_dimensions {
                    "z" | "Z" => Dimensions::Xyz,
                    "m" | "M" => Dimensions::Xym,
                    "zm" | "ZM" => Dimensions::Xyzm,
                    _ => {
                        return Err(anyhow::anyhow!("invalid geometry type string: '{value}'"));
                    }
                }
            }
            None => Dimensions::Xy,
        };

        if parts.next().is_some() {
            return Err(anyhow::anyhow!("invalid geometry type string: '{value}'"));
        }

        Ok(Self {
            geometry_type,
            dimensions,
        })
    }
}

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

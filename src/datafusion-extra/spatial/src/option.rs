use std::fmt::Display;

use datafusion::common::{DataFusionError, Result, config_namespace};
use datafusion::config::{ConfigEntry, ConfigExtension, ConfigField, ExtensionOptions, Visit};
use datafusion::prelude::SessionConfig;
use regex::Regex;

/// Default minimum number of analyzed geometries for speculative execution mode
/// to select an optimal execution mode.
const DEFAULT_SPECULATIVE_THRESHOLD: usize = 1000;

/// Default minimum number of points per geometry to use prepared geometries for
/// the build side.
const DEFAULT_MIN_POINTS_FOR_BUILD_PREPARATION: usize = 50;

/// Helper function to register the spatial join optimizer with a session config
pub fn add_dataclod_option_extension(config: SessionConfig) -> SessionConfig {
    config.with_option_extension(DataClodOptions::default())
}

config_namespace! {
    /// Configuration options for DataClod.
    pub struct DataClodOptions {
        /// Options for spatial join
        pub spatial_join: SpatialJoinOptions, default = SpatialJoinOptions::default()
    }
}

config_namespace! {
    /// Configuration options for spatial join.
    ///
    /// This struct controls various aspects of how spatial joins are performed,
    /// including prepared geometry usage and spatial library used for evaluating
    /// spatial predicates.
    pub struct SpatialJoinOptions {
        /// Enable optimized spatial join
        pub enable: bool, default = true

        /// Spatial library to use for spatial join
        pub spatial_library: SpatialLibrary, default = SpatialLibrary::Tg

        /// Options for configuring the GEOS spatial library
        pub geos: GeosOptions, default = GeosOptions::default()

        /// Options for configuring the TG spatial library
        pub tg: TgOptions, default = TgOptions::default()

        /// The execution mode determining how prepared geometries are used
        pub execution_mode: ExecutionMode, default = ExecutionMode::Speculative(DEFAULT_SPECULATIVE_THRESHOLD)

   /// Collect build side partitions concurrently (using spawned tasks).
        /// Set to false for contexts where spawning new tasks is not supported.
        pub concurrent_build_side_collection: bool, default = true
    }
}

config_namespace! {
    /// Configuration options for the GEOS spatial library
    pub struct GeosOptions {
        /// The minimum number of points per geometry to use prepared geometries for the build side.
        pub min_points_for_build_preparation: usize, default = DEFAULT_MIN_POINTS_FOR_BUILD_PREPARATION
    }
}

config_namespace! {
    /// Configuration options for the TG spatial library
    pub struct TgOptions {
        /// The index type to use for the TG spatial library
        pub index_type: TgIndexType, default = TgIndexType::YStripes
    }
}

impl ConfigExtension for DataClodOptions {
    const PREFIX: &'static str = "dataclod";
}

impl ExtensionOptions for DataClodOptions {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }

    fn cloned(&self) -> Box<dyn ExtensionOptions> {
        Box::new(self.clone())
    }

    fn set(&mut self, key: &str, value: &str) -> Result<()> {
        <Self as ConfigField>::set(self, key, value)
    }

    fn entries(&self) -> Vec<ConfigEntry> {
        struct Visitor(Vec<ConfigEntry>);

        impl Visit for Visitor {
            fn some<V: Display>(&mut self, key: &str, value: V, description: &'static str) {
                self.0.push(ConfigEntry {
                    key: key.to_owned(),
                    value: Some(value.to_string()),
                    description,
                })
            }

            fn none(&mut self, key: &str, description: &'static str) {
                self.0.push(ConfigEntry {
                    key: key.to_owned(),
                    value: None,
                    description,
                })
            }
        }

        let mut v = Visitor(vec![]);
        self.visit(&mut v, Self::PREFIX, "");
        v.0
    }
}

/// Execution mode for spatial join operations, controlling prepared geometry
/// usage.
///
/// Prepared geometries are pre-processed spatial objects that can significantly
/// improve performance for spatial predicate evaluation when the same geometry
/// is used multiple times in comparisons.
///
/// The choice of execution mode depends on the specific characteristics of your
/// spatial join workload, as well as the spatial relation predicate between the
/// two tables. Some of the spatial relation computations cannot be accelerated
/// by prepared geometries at all (for example, `ST_Touches`, `ST_Crosses`,
/// `ST_DWithin`).
#[derive(Debug, Clone, PartialEq, Copy)]
pub enum ExecutionMode {
    /// Don't use prepared geometries for spatial predicate evaluation.
    PrepareNone,

    /// Create prepared geometries for the build side (left/smaller table).
    PrepareBuild,

    /// Create prepared geometries for the probe side (right/larger table).
    PrepareProbe,

    /// Automatically choose the best execution mode based on the
    /// characteristics of first few geometries on the probe side.
    Speculative(usize),
}

impl ExecutionMode {
    /// Convert the execution mode to a usize value.
    ///
    /// This is used to show the execution mode in the metrics. We use a gauge
    /// value to represent the execution mode.
    pub fn as_gauge(&self) -> usize {
        match self {
            ExecutionMode::PrepareNone => 0,
            ExecutionMode::PrepareBuild => 1,
            ExecutionMode::PrepareProbe => 2,
            ExecutionMode::Speculative(_) => 3,
        }
    }
}

impl ConfigField for ExecutionMode {
    fn visit<V: Visit>(&self, v: &mut V, key: &str, description: &'static str) {
        let value = match self {
            ExecutionMode::PrepareNone => "prepare_none".into(),
            ExecutionMode::PrepareBuild => "prepare_build".into(),
            ExecutionMode::PrepareProbe => "prepare_probe".into(),
            ExecutionMode::Speculative(n) => format!("auto[{n}]"),
        };
        v.some(key, value, description);
    }

    fn set(&mut self, _key: &str, value: &str) -> Result<()> {
        let value = value.to_lowercase();
        let mode = match value.as_str() {
            "prepare_none" => ExecutionMode::PrepareNone,
            "prepare_build" => ExecutionMode::PrepareBuild,
            "prepare_probe" => ExecutionMode::PrepareProbe,
            _ => {
                // Match "auto" or "auto[number]" pattern
                let auto_regex = Regex::new(r"^auto(?:\[(\d+)\])?$").unwrap();

                if let Some(captures) = auto_regex.captures(&value) {
                    // If there's a captured group (the number), use it; otherwise default to 100
                    let n = if let Some(number_match) = captures.get(1) {
                        match number_match.as_str().parse::<usize>() {
                            Ok(n) => {
                                if n == 0 {
                                    return Err(DataFusionError::Configuration(
                                        "Invalid number in auto mode: 0 is not allowed".to_owned(),
                                    ));
                                }
                                n
                            }
                            Err(_) => {
                                return Err(DataFusionError::Configuration(format!(
                                    "Invalid number in auto mode: {}",
                                    number_match.as_str()
                                )));
                            }
                        }
                    } else {
                        DEFAULT_SPECULATIVE_THRESHOLD // Default for plain "auto"
                    };
                    ExecutionMode::Speculative(n)
                } else {
                    return Err(DataFusionError::Configuration(format!(
                        "Unknown execution mode: {value}. Expected formats: prepare_none, prepare_build, prepare_probe, auto, auto[number]"
                    )));
                }
            }
        };
        *self = mode;
        Ok(())
    }
}

/// The spatial library to use for evaluating spatial predicates
#[derive(Debug, PartialEq, Clone, Copy)]
pub enum SpatialLibrary {
    /// Use georust/geo library (<https://github.com/georust/geo>)
    Geo,

    /// Use GEOS library via georust/geos (<https://github.com/georust/geos>)
    Geos,

    /// Use tiny geometry library (<https://github.com/tidwall/tg>)
    Tg,
}

impl ConfigField for SpatialLibrary {
    fn visit<V: Visit>(&self, v: &mut V, key: &str, description: &'static str) {
        let value = match self {
            SpatialLibrary::Geo => "geo",
            SpatialLibrary::Geos => "geos",
            SpatialLibrary::Tg => "tg",
        };
        v.some(key, value, description);
    }

    fn set(&mut self, _key: &str, value: &str) -> Result<()> {
        let value = value.to_lowercase();
        let library = match value.as_str() {
            "geo" => SpatialLibrary::Geo,
            "geos" => SpatialLibrary::Geos,
            "tg" => SpatialLibrary::Tg,
            _ => {
                return Err(DataFusionError::Configuration(format!(
                    "Unknown spatial library: {value}. Expected: geo, geos, tg"
                )));
            }
        };
        *self = library;
        Ok(())
    }
}

/// The index type to use for the TG spatial library
#[derive(Debug, PartialEq, Clone, Copy)]
pub enum TgIndexType {
    /// Natural index
    Natural,

    /// Y-stripes index
    YStripes,
}

impl ConfigField for TgIndexType {
    fn visit<V: Visit>(&self, v: &mut V, key: &str, description: &'static str) {
        let value = match self {
            TgIndexType::Natural => "natural",
            TgIndexType::YStripes => "ystripes",
        };
        v.some(key, value, description);
    }

    fn set(&mut self, _key: &str, value: &str) -> Result<()> {
        let value = value.to_lowercase();
        let index_type = match value.as_str() {
            "natural" => TgIndexType::Natural,
            "ystripes" => TgIndexType::YStripes,
            _ => {
                return Err(DataFusionError::Configuration(format!(
                    "Unknown TG index type: {value}. Expected: natural, ystripes"
                )));
            }
        };
        *self = index_type;
        Ok(())
    }
}

#![feature(anonymous_lifetime_in_impl_trait)]

mod function;
mod geo_ext;
mod geometry;
mod geos_ext;
mod join;
mod option;
mod statistics;

pub use function::register_spatial_udfs;
pub use join::optimizer::register_spatial_join_optimizer;
pub use option::add_dataclod_option_extension;

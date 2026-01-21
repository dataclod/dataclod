#![doc(
    html_logo_url = "https://raw.githubusercontent.com/apache/datafusion/19fe44cf2f30cbdd63d4a4f52c74055163c6cc38/docs/logos/standalone_logo/logo_original.svg",
    html_favicon_url = "https://raw.githubusercontent.com/apache/datafusion/19fe44cf2f30cbdd63d4a4f52c74055163c6cc38/docs/logos/standalone_logo/logo_original.svg"
)]
#![cfg_attr(docsrs, feature(doc_cfg))]
// Make sure fast / cheap clones on Arc are explicit:
// https://github.com/apache/datafusion/issues/11143
#![cfg_attr(not(test), deny(clippy::clone_on_ref_ptr))]

//! `DataClod` sqllogictest driver

mod engines;

pub use engines::{
    CurrentlyExecutingSqlTracker, DCSqlLogicTestError, DFColumnType, DFOutput, DataClod,
    convert_batches, convert_schema_to_types,
};

mod filters;
mod test_context;
mod util;

pub use filters::*;
pub use test_context::TestContext;
pub use util::*;

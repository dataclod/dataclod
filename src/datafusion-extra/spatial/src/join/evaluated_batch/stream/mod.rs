pub mod external;
pub mod in_mem;

use std::pin::Pin;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::Result;
use futures::Stream;

use crate::join::evaluated_batch::EvaluatedBatch;

/// A stream that produces [`EvaluatedBatch`] items. This stream may have purely
/// in-memory or out-of-core implementations. The type of the stream could be
/// queried calling `is_external()`.
pub trait EvaluatedBatchStream: Stream<Item = Result<EvaluatedBatch>> {
    /// Returns true if this stream is an external stream, where batch data were
    /// spilled to disk.
    fn is_external(&self) -> bool;

    /// Returns the schema of records produced by this `EvaluatedBatchStream`.
    ///
    /// Implementation of this trait should guarantee that all
    /// `EvaluatedBatch`'s returned by this stream should have the same
    /// schema as returned from this method.
    fn schema(&self) -> SchemaRef;
}

pub type SendableEvaluatedBatchStream = Pin<Box<dyn EvaluatedBatchStream + Send>>;

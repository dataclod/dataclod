use std::pin::Pin;
use std::task::{Context, Poll};

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::Result;
use futures::{Stream, StreamExt};

use crate::join::evaluated_batch::EvaluatedBatch;
use crate::join::evaluated_batch::stream::{EvaluatedBatchStream, SendableEvaluatedBatchStream};
use crate::join::probe::ProbeStreamMetrics;

/// A non-partitioned evaluated batch stream that simply forwards batches from
/// an inner stream, while updating probe stream metrics. This is for running
/// non-partitioned fully in-memory spatial joins.
pub struct NonPartitionedStream {
    inner: SendableEvaluatedBatchStream,
    metrics: ProbeStreamMetrics,
}

impl NonPartitionedStream {
    pub fn new(inner: SendableEvaluatedBatchStream, metrics: ProbeStreamMetrics) -> Self {
        Self { inner, metrics }
    }
}

impl Stream for NonPartitionedStream {
    type Item = Result<EvaluatedBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.inner.poll_next_unpin(cx) {
            Poll::Ready(Some(Ok(batch))) => {
                self.metrics.probe_input_batches.add(1);
                self.metrics.probe_input_rows.add(batch.num_rows());
                Poll::Ready(Some(Ok(batch)))
            }
            other => other,
        }
    }
}

impl EvaluatedBatchStream for NonPartitionedStream {
    fn is_external(&self) -> bool {
        self.inner.as_ref().get_ref().is_external()
    }

    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }
}

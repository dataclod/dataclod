use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::Result;
use datafusion::physical_plan::{SendableRecordBatchStream, metrics};
use futures::{Stream, StreamExt};

use crate::join::evaluated_batch::EvaluatedBatch;
use crate::join::evaluated_batch::stream::{EvaluatedBatchStream, SendableEvaluatedBatchStream};
use crate::join::operand_evaluator::{EvaluatedGeometryArray, OperandEvaluator};
use crate::join::utils::arrow_utils::{compact_batch, schema_contains_view_types};

/// An evaluator that can evaluate geometry expressions on record batches
/// and produces evaluated geometry arrays.
trait Evaluator: Unpin {
    fn evaluate(&self, batch: &RecordBatch) -> Result<EvaluatedGeometryArray>;
}

/// An evaluator for build-side geometry expressions.
struct BuildSideEvaluator {
    evaluator: Arc<dyn OperandEvaluator>,
}

impl Evaluator for BuildSideEvaluator {
    fn evaluate(&self, batch: &RecordBatch) -> Result<EvaluatedGeometryArray> {
        self.evaluator.evaluate_build(batch)
    }
}

/// An evaluator for probe-side geometry expressions.
struct ProbeSideEvaluator {
    evaluator: Arc<dyn OperandEvaluator>,
}

impl Evaluator for ProbeSideEvaluator {
    fn evaluate(&self, batch: &RecordBatch) -> Result<EvaluatedGeometryArray> {
        self.evaluator.evaluate_probe(batch)
    }
}

/// Wraps a `SendableRecordBatchStream` and evaluates the probe-side geometry
/// expression eagerly so downstream consumers can operate on `EvaluatedBatch`s.
struct EvaluateOperandBatchStream<E: Evaluator> {
    inner: SendableRecordBatchStream,
    evaluator: E,
    evaluation_time: metrics::Time,
    gc_view_arrays: bool,
}

impl<E: Evaluator> EvaluateOperandBatchStream<E> {
    fn new(
        inner: SendableRecordBatchStream, evaluator: E, evaluation_time: metrics::Time,
        gc_view_arrays: bool,
    ) -> Self {
        let gc_view_arrays = gc_view_arrays && schema_contains_view_types(&inner.schema());
        Self {
            inner,
            evaluator,
            evaluation_time,
            gc_view_arrays,
        }
    }
}

impl<E: Evaluator> EvaluatedBatchStream for EvaluateOperandBatchStream<E> {
    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }
}

impl<E: Evaluator> Stream for EvaluateOperandBatchStream<E> {
    type Item = Result<EvaluatedBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let self_mut = self.get_mut();
        match self_mut.inner.poll_next_unpin(cx) {
            Poll::Ready(Some(Ok(batch))) => {
                let _timer = self_mut.evaluation_time.timer();
                let batch = if self_mut.gc_view_arrays {
                    compact_batch(batch)?
                } else {
                    batch
                };
                let geom_array = self_mut.evaluator.evaluate(&batch)?;
                let evaluated = EvaluatedBatch { batch, geom_array };
                Poll::Ready(Some(Ok(evaluated)))
            }
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// Returns a `SendableEvaluatedBatchStream` that eagerly evaluates the
/// build-side geometry expression for every incoming `RecordBatch`.
pub fn create_evaluated_build_stream(
    stream: SendableRecordBatchStream, evaluator: Arc<dyn OperandEvaluator>,
    evaluation_time: metrics::Time,
) -> SendableEvaluatedBatchStream {
    // Enable gc_view_arrays for build-side since build-side batches needs to be
    // long-lived in memory during the join process. Poorly managed sparse view
    // arrays could lead to unnecessary high memory usage or excessive spilling.
    Box::pin(EvaluateOperandBatchStream::new(
        stream,
        BuildSideEvaluator { evaluator },
        evaluation_time,
        true,
    ))
}

/// Returns a `SendableEvaluatedBatchStream` that eagerly evaluates the
/// probe-side geometry expression for every incoming `RecordBatch`.
pub fn create_evaluated_probe_stream(
    stream: SendableRecordBatchStream, evaluator: Arc<dyn OperandEvaluator>,
    evaluation_time: metrics::Time,
) -> SendableEvaluatedBatchStream {
    Box::pin(EvaluateOperandBatchStream::new(
        stream,
        ProbeSideEvaluator { evaluator },
        evaluation_time,
        false,
    ))
}

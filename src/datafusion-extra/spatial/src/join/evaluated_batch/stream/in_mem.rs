use std::pin::Pin;
use std::task::{Context, Poll};
use std::vec::IntoIter;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::Result;

use crate::join::evaluated_batch::EvaluatedBatch;
use crate::join::evaluated_batch::stream::EvaluatedBatchStream;

pub struct InMemoryEvaluatedBatchStream {
    schema: SchemaRef,
    iter: IntoIter<EvaluatedBatch>,
}

impl InMemoryEvaluatedBatchStream {
    pub fn new(schema: SchemaRef, batches: Vec<EvaluatedBatch>) -> Self {
        InMemoryEvaluatedBatchStream {
            schema,
            iter: batches.into_iter(),
        }
    }
}

impl EvaluatedBatchStream for InMemoryEvaluatedBatchStream {
    fn is_external(&self) -> bool {
        false
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl futures::Stream for InMemoryEvaluatedBatchStream {
    type Item = Result<EvaluatedBatch>;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.get_mut()
            .iter
            .next()
            .map(|batch| Poll::Ready(Some(Ok(batch))))
            .unwrap_or(Poll::Ready(None))
    }
}

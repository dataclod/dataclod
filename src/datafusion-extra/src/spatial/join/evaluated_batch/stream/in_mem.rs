use std::pin::Pin;
use std::task::{Context, Poll};
use std::vec::IntoIter;

use datafusion::common::Result;

use crate::spatial::join::evaluated_batch::EvaluatedBatch;
use crate::spatial::join::evaluated_batch::stream::EvaluatedBatchStream;

pub struct InMemoryEvaluatedBatchStream {
    iter: IntoIter<EvaluatedBatch>,
}

impl InMemoryEvaluatedBatchStream {
    pub fn new(batches: Vec<EvaluatedBatch>) -> Self {
        InMemoryEvaluatedBatchStream {
            iter: batches.into_iter(),
        }
    }
}

impl EvaluatedBatchStream for InMemoryEvaluatedBatchStream {
    fn is_external(&self) -> bool {
        false
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

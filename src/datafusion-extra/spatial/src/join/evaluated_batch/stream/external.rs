use std::collections::VecDeque;
use std::iter;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::common::runtime::SpawnedTask;
use datafusion::common::{DataFusionError, Result, exec_err};
use datafusion::execution::disk_manager::RefCountedTempFile;
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream};
use datafusion::physical_plan::stream::RecordBatchReceiverStreamBuilder;
use futures::{FutureExt, StreamExt};
use pin_project_lite::pin_project;

use crate::join::evaluated_batch::EvaluatedBatch;
use crate::join::evaluated_batch::spill::{
    EvaluatedBatchSpillReader, spilled_batch_to_evaluated_batch, spilled_schema_to_evaluated_schema,
};
use crate::join::evaluated_batch::stream::EvaluatedBatchStream;

const RECORD_BATCH_CHANNEL_CAPACITY: usize = 2;

pin_project! {
    /// Streams [`EvaluatedBatch`] values read back from on-disk spill files.
    ///
    /// This stream is intended for the “spilled” path where batches have been written to disk and
    /// must be read back into memory. It wraps an [`ExternalRecordBatchStream`] and uses
    /// background tasks to prefetch/forward batches so downstream operators can process a batch
    /// while the next one is being loaded.
    pub struct ExternalEvaluatedBatchStream {
        #[pin]
        inner: RecordBatchToEvaluatedStream,
        schema: SchemaRef,
    }
}

enum State {
    AwaitingFile,
    Opening(SpawnedTask<Result<EvaluatedBatchSpillReader>>),
    Reading(SpawnedTask<(EvaluatedBatchSpillReader, Option<Result<RecordBatch>>)>),
    Finished,
}

impl ExternalEvaluatedBatchStream {
    /// Creates an external stream from a single spill file.
    pub fn try_from_spill_file(spill_file: Arc<RefCountedTempFile>) -> Result<Self> {
        let record_stream =
            ExternalRecordBatchStream::try_from_spill_files(iter::once(spill_file))?;
        let evaluated_stream =
            RecordBatchToEvaluatedStream::try_spawned_evaluated_stream(Box::pin(record_stream))?;
        let schema = evaluated_stream.schema();
        Ok(Self {
            inner: evaluated_stream,
            schema,
        })
    }

    /// Creates an external stream from multiple spill files.
    ///
    /// The stream yields the batches from each file in order. When
    /// `spill_files` is empty the stream is empty (returns `None`
    /// immediately) and no schema validation is performed.
    pub fn try_from_spill_files<I>(schema: SchemaRef, spill_files: I) -> Result<Self>
    where
        I: IntoIterator<Item = Arc<RefCountedTempFile>>,
    {
        let record_stream = ExternalRecordBatchStream::try_from_spill_files(spill_files)?;
        if !record_stream.is_empty() {
            // `ExternalRecordBatchStream` only has a meaningful schema when at least one
            // spill file is provided. In that case, validate that the
            // caller-provided evaluated schema matches what would be derived
            // from the spilled schema.
            let actual_schema = spilled_schema_to_evaluated_schema(&record_stream.schema())?;
            if schema != actual_schema {
                return exec_err!("Schema mismatch when creating ExternalEvaluatedBatchStream");
            }
        }
        let evaluated_stream =
            RecordBatchToEvaluatedStream::try_spawned_evaluated_stream(Box::pin(record_stream))?;
        Ok(Self {
            inner: evaluated_stream,
            schema,
        })
    }
}

impl EvaluatedBatchStream for ExternalEvaluatedBatchStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl futures::Stream for ExternalEvaluatedBatchStream {
    type Item = Result<EvaluatedBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.project().inner.poll_next(cx)
    }
}

pin_project! {
    /// Adapts a [`RecordBatchStream`] containing spilled batches into an [`EvaluatedBatch`] stream.
    ///
    /// Each incoming `RecordBatch` is decoded via [`spilled_batch_to_evaluated_batch`]. This type
    /// also carries the derived evaluated schema for downstream consumers.
    struct RecordBatchToEvaluatedStream {
        #[pin]
        inner: SendableRecordBatchStream,
        evaluated_schema: SchemaRef,
    }
}

impl RecordBatchToEvaluatedStream {
    fn try_new(inner: SendableRecordBatchStream) -> Result<Self> {
        let evaluated_schema = spilled_schema_to_evaluated_schema(&inner.schema())?;
        Ok(Self {
            inner,
            evaluated_schema,
        })
    }

    /// Buffers `record_stream` by forwarding it through a bounded channel.
    ///
    /// This is primarily useful for [`ExternalRecordBatchStream`], where
    /// producing the next batch may involve disk I/O and `spawn_blocking`
    /// work. By polling the source stream in a spawned task, we can overlap
    /// “load next batch” with “process current batch”, while still applying
    /// backpressure via [`RECORD_BATCH_CHANNEL_CAPACITY`].
    ///
    /// The forwarding task stops when the receiver is dropped or when the
    /// source stream yields its first error.
    fn try_spawned_evaluated_stream(record_stream: SendableRecordBatchStream) -> Result<Self> {
        let schema = record_stream.schema();
        let mut builder =
            RecordBatchReceiverStreamBuilder::new(schema, RECORD_BATCH_CHANNEL_CAPACITY);
        let tx = builder.tx();
        builder.spawn(async move {
            let mut record_stream = record_stream;
            while let Some(batch) = record_stream.next().await {
                let is_err = batch.is_err();
                if tx.send(batch).await.is_err() {
                    break;
                }
                if is_err {
                    break;
                }
            }
            Ok(())
        });

        let buffered = builder.build();
        Self::try_new(buffered)
    }

    fn schema(&self) -> SchemaRef {
        self.evaluated_schema.clone()
    }
}

impl futures::Stream for RecordBatchToEvaluatedStream {
    type Item = Result<EvaluatedBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        match this.inner.as_mut().poll_next(cx) {
            Poll::Ready(Some(Ok(batch))) => {
                Poll::Ready(Some(spilled_batch_to_evaluated_batch(batch)))
            }
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// Streams raw [`RecordBatch`] values directly from spill files.
///
/// This is the lowest-level “read from disk” stream: it opens each spill file,
/// reads the stored record batches sequentially, and yields them without
/// decoding into [`EvaluatedBatch`].
///
/// Schema handling:
/// - If at least one spill file is provided, the stream schema is taken from
///   the first file.
/// - If no files are provided, the schema is empty and the stream terminates
///   immediately.
pub struct ExternalRecordBatchStream {
    schema: SchemaRef,
    state: State,
    spill_files: VecDeque<Arc<RefCountedTempFile>>,
    is_empty: bool,
}

impl ExternalRecordBatchStream {
    /// Creates a stream over `spill_files`, yielding all batches from each file
    /// in order.
    ///
    /// This function assumes all spill files were written with a compatible
    /// schema.
    pub fn try_from_spill_files<I>(spill_files: I) -> Result<Self>
    where
        I: IntoIterator<Item = Arc<RefCountedTempFile>>,
    {
        let spill_files = spill_files.into_iter().collect::<VecDeque<_>>();
        let (schema, is_empty) = match spill_files.front() {
            Some(file) => {
                let reader = EvaluatedBatchSpillReader::try_new(file)?;
                (reader.schema(), false)
            }
            None => (Arc::new(Schema::empty()), true),
        };
        Ok(Self {
            schema,
            state: State::AwaitingFile,
            spill_files,
            is_empty,
        })
    }

    pub fn is_empty(&self) -> bool {
        self.is_empty
    }
}

impl RecordBatchStream for ExternalRecordBatchStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl futures::Stream for ExternalRecordBatchStream {
    type Item = Result<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let self_mut = self.get_mut();

        loop {
            match &mut self_mut.state {
                State::AwaitingFile => {
                    match self_mut.spill_files.pop_front() {
                        Some(spill_file) => {
                            let task = SpawnedTask::spawn_blocking(move || {
                                EvaluatedBatchSpillReader::try_new(&spill_file)
                            });
                            self_mut.state = State::Opening(task);
                        }
                        None => {
                            self_mut.state = State::Finished;
                            return Poll::Ready(None);
                        }
                    }
                }
                State::Opening(task) => {
                    match futures::ready!(task.poll_unpin(cx)) {
                        Err(e) => {
                            self_mut.state = State::Finished;
                            return Poll::Ready(Some(Err(DataFusionError::External(Box::new(e)))));
                        }
                        Ok(Err(e)) => {
                            self_mut.state = State::Finished;
                            return Poll::Ready(Some(Err(e)));
                        }
                        Ok(Ok(mut spill_reader)) => {
                            let task = SpawnedTask::spawn_blocking(move || {
                                let next_batch = spill_reader.next_raw_batch();
                                (spill_reader, next_batch)
                            });
                            self_mut.state = State::Reading(task);
                        }
                    }
                }
                State::Reading(task) => {
                    match futures::ready!(task.poll_unpin(cx)) {
                        Err(e) => {
                            self_mut.state = State::Finished;
                            return Poll::Ready(Some(Err(DataFusionError::External(Box::new(e)))));
                        }
                        Ok((_, None)) => {
                            self_mut.state = State::AwaitingFile;
                            continue;
                        }
                        Ok((_, Some(Err(e)))) => {
                            self_mut.state = State::Finished;
                            return Poll::Ready(Some(Err(e)));
                        }
                        Ok((mut spill_reader, Some(Ok(batch)))) => {
                            let task = SpawnedTask::spawn_blocking(move || {
                                let next_batch = spill_reader.next_raw_batch();
                                (spill_reader, next_batch)
                            });
                            self_mut.state = State::Reading(task);
                            return Poll::Ready(Some(Ok(batch)));
                        }
                    }
                }
                State::Finished => {
                    return Poll::Ready(None);
                }
            }
        }
    }
}

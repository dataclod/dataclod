use std::fs::File;
use std::io::BufReader;
use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::ipc::reader::StreamReader;
use datafusion::arrow::ipc::writer::{IpcWriteOptions, StreamWriter};
use datafusion::common::{DataFusionError, Result};
use datafusion::config::SpillCompression;
use datafusion::execution::disk_manager::RefCountedTempFile;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::physical_plan::metrics::SpillMetrics;

use crate::join::utils::arrow_utils::{
    compact_batch, get_record_batch_memory_size, schema_contains_view_types,
};

/// Generic Arrow IPC stream spill writer for [`RecordBatch`].
///
/// Shared between multiple components so spill metrics are updated
/// consistently.
pub struct RecordBatchSpillWriter {
    in_progress_file: RefCountedTempFile,
    writer: StreamWriter<File>,
    metrics: SpillMetrics,
    batch_in_memory_size_threshold: Option<usize>,
    gc_view_arrays: bool,
}

impl RecordBatchSpillWriter {
    pub fn try_new(
        env: Arc<RuntimeEnv>, schema: SchemaRef, request_description: &str,
        compression: SpillCompression, metrics: SpillMetrics, batch_size_threshold: Option<usize>,
    ) -> Result<Self> {
        let in_progress_file = env.disk_manager.create_tmp_file(request_description)?;
        let file = File::create(in_progress_file.path())?;

        let mut write_options = IpcWriteOptions::default();
        write_options = write_options.try_with_compression(compression.into())?;

        let writer = StreamWriter::try_new_with_options(file, schema.as_ref(), write_options)?;
        metrics.spill_file_count.add(1);

        let gc_view_arrays = schema_contains_view_types(&schema);

        Ok(Self {
            in_progress_file,
            writer,
            metrics,
            batch_in_memory_size_threshold: batch_size_threshold,
            gc_view_arrays,
        })
    }

    /// Write a record batch to the spill file.
    ///
    /// If `batch_size_threshold` is configured and the in-memory size of the
    /// batch exceeds the threshold, this will automatically split the batch
    /// into smaller slices and (optionally) compact each slice before
    /// writing.
    pub fn write_batch(&mut self, batch: RecordBatch) -> Result<()> {
        let num_rows = batch.num_rows();
        if num_rows == 0 {
            // Preserve "empty batch" semantics: callers may rely on spilling and reading
            // back a zero-row batch (e.g. as a sentinel for an empty stream).
            return self.write_one_batch(batch);
        }

        let rows_per_split = self.calculate_rows_per_split(&batch, num_rows)?;
        if rows_per_split < num_rows {
            let mut offset = 0;
            while offset < num_rows {
                let length = std::cmp::min(rows_per_split, num_rows - offset);
                let slice = batch.slice(offset, length);
                self.write_one_batch(slice)?;
                offset += length;
            }
        } else {
            self.write_one_batch(batch)?;
        }
        Ok(())
    }

    fn calculate_rows_per_split(&self, batch: &RecordBatch, num_rows: usize) -> Result<usize> {
        let Some(threshold) = self.batch_in_memory_size_threshold else {
            return Ok(num_rows);
        };
        if threshold == 0 {
            return Ok(num_rows);
        }

        let batch_size = get_record_batch_memory_size(batch)?;
        if batch_size <= threshold {
            return Ok(num_rows);
        }

        let num_splits = batch_size.div_ceil(threshold);
        let rows = num_rows.div_ceil(num_splits);
        Ok(std::cmp::max(1, rows))
    }

    fn write_one_batch(&mut self, batch: RecordBatch) -> Result<()> {
        // Writing record batches containing sparse binary view arrays may lead to
        // excessive disk usage and slow read performance later. Compact such
        // batches before writing.
        let batch = if self.gc_view_arrays {
            compact_batch(batch)?
        } else {
            batch
        };
        self.writer.write(&batch).map_err(|e| {
            DataFusionError::Execution(format!(
                "Failed to write RecordBatch to spill file {:?}: {}",
                self.in_progress_file.path(),
                e
            ))
        })?;

        self.metrics.spilled_rows.add(batch.num_rows());
        Ok(())
    }

    pub fn finish(mut self) -> Result<RefCountedTempFile> {
        self.writer.finish()?;

        let mut in_progress_file = self.in_progress_file;
        in_progress_file.update_disk_usage()?;
        let size = in_progress_file.current_disk_usage();
        self.metrics.spilled_bytes.add(size as usize);
        Ok(in_progress_file)
    }
}

/// Generic Arrow IPC stream spill reader for [`RecordBatch`].
pub struct RecordBatchSpillReader {
    stream_reader: StreamReader<BufReader<File>>,
}

impl RecordBatchSpillReader {
    pub fn try_new(temp_file: &RefCountedTempFile) -> Result<Self> {
        let file = File::open(temp_file.path())?;
        let mut stream_reader = StreamReader::try_new_buffered(file, None)?;

        // SAFETY: spill writers in this crate strictly follow Arrow IPC specifications.
        // Skip redundant validation during read to speed up.
        unsafe {
            stream_reader = stream_reader.with_skip_validation(true);
        }

        Ok(Self { stream_reader })
    }

    pub fn schema(&self) -> SchemaRef {
        self.stream_reader.schema()
    }

    pub fn next_batch(&mut self) -> Option<Result<RecordBatch>> {
        self.stream_reader
            .next()
            .map(|result| result.map_err(|e| e.into()))
    }
}

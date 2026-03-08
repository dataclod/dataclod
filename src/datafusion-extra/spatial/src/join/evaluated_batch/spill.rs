use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, Float64Array, Float64Builder, RecordBatch, StructArray,
};
use datafusion::arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef};
use datafusion::common::{DataFusionError, Result, ScalarValue, exec_err};
use datafusion::config::SpillCompression;
use datafusion::execution::disk_manager::RefCountedTempFile;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_plan::metrics::SpillMetrics;

use crate::join::evaluated_batch::EvaluatedBatch;
use crate::join::operand_evaluator::EvaluatedGeometryArray;
use crate::join::utils::spill::{RecordBatchSpillReader, RecordBatchSpillWriter};

/// Writer for spilling evaluated batches to disk
pub struct EvaluatedBatchSpillWriter {
    /// The temporary spill file being written to
    inner: RecordBatchSpillWriter,

    /// Schema of the spilled record batches. It is augmented from the schema of
    /// original record batches The `spill_schema` has 4 fields:
    /// * `data`: `StructArray` containing the original record batch columns
    /// * `geom`: geometry array in storage format
    /// * `dist`: distance field
    spill_schema: Schema,
    /// Inner fields of the "data" `StructArray` in the spilled record batches
    data_inner_fields: Fields,
}

const SPILL_FIELD_DATA_INDEX: usize = 0;
const SPILL_FIELD_GEOM_INDEX: usize = 1;
const SPILL_FIELD_DIST_INDEX: usize = 2;

impl EvaluatedBatchSpillWriter {
    /// Create a new `SpillWriter`
    pub fn try_new(
        env: Arc<RuntimeEnv>, schema: SchemaRef, request_description: &str,
        compression: SpillCompression, metrics: SpillMetrics, batch_size_threshold: Option<usize>,
    ) -> Result<Self> {
        // Construct schema of record batches to be written. The written batches are
        // augmented from the original record batches.
        let data_inner_fields = schema.fields().clone();
        let data_struct_field =
            Field::new("data", DataType::Struct(data_inner_fields.clone()), false);
        let geom_field = Field::new("geom", DataType::BinaryView, true);
        let dist_field = Field::new("dist", DataType::Float64, true);
        let spill_schema = Schema::new(vec![data_struct_field, geom_field, dist_field]);

        // Create spill file
        let inner = RecordBatchSpillWriter::try_new(
            env,
            Arc::new(spill_schema.clone()),
            request_description,
            compression,
            metrics,
            batch_size_threshold,
        )?;

        Ok(Self {
            inner,
            spill_schema,
            data_inner_fields,
        })
    }

    /// Append an `EvaluatedBatch` to the spill file
    pub fn append(&mut self, evaluated_batch: &EvaluatedBatch) -> Result<()> {
        let record_batch = self.spilled_record_batch(evaluated_batch)?;

        // Splitting/compaction and spill bytes/rows metrics are handled by
        // `RecordBatchSpillWriter`.
        self.inner.write_batch(record_batch)?;
        Ok(())
    }

    /// Finish writing and return the temporary file
    pub fn finish(self) -> Result<RefCountedTempFile> {
        self.inner.finish()
    }

    fn spilled_record_batch(&self, evaluated_batch: &EvaluatedBatch) -> Result<RecordBatch> {
        let num_rows = evaluated_batch.num_rows();

        // Store the original data batch into a StructArray
        let data_batch = &evaluated_batch.batch;
        let data_arrays = data_batch.columns().to_vec();
        let data_struct_array =
            StructArray::try_new(self.data_inner_fields.clone(), data_arrays, None)?;

        // Store dist into a Float64Array
        let mut dist_builder = Float64Builder::with_capacity(num_rows);
        let geom_array = &evaluated_batch.geom_array;
        match &geom_array.distance {
            Some(ColumnarValue::Scalar(scalar)) => {
                match scalar {
                    ScalarValue::Float64(dist_value) => {
                        for _ in 0..num_rows {
                            dist_builder.append_option(*dist_value);
                        }
                    }
                    _ => {
                        return exec_err!("Distance columnar value is not a Float64Array");
                    }
                }
            }
            Some(ColumnarValue::Array(array)) => {
                let float_array = array.as_any().downcast_ref::<Float64Array>().unwrap();
                dist_builder.append_array(float_array);
            }
            None => {
                for _ in 0..num_rows {
                    dist_builder.append_null();
                }
            }
        }
        let dist_array = dist_builder.finish();

        // Assemble the final spilled RecordBatch
        let columns: Vec<ArrayRef> = vec![
            Arc::new(data_struct_array),
            geom_array.geometry_array.clone(),
            Arc::new(dist_array),
        ];
        let spilled_record_batch =
            RecordBatch::try_new(Arc::new(self.spill_schema.clone()), columns)?;
        Ok(spilled_record_batch)
    }
}

/// Reader for reading spilled evaluated batches from disk
pub struct EvaluatedBatchSpillReader {
    inner: RecordBatchSpillReader,
}

impl EvaluatedBatchSpillReader {
    /// Create a new `SpillReader`
    pub fn try_new(temp_file: &RefCountedTempFile) -> Result<Self> {
        Ok(Self {
            inner: RecordBatchSpillReader::try_new(temp_file)?,
        })
    }

    /// Get the schema of the spilled data
    pub fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }

    /// Read the next raw `RecordBatch` from the spill file
    pub fn next_raw_batch(&mut self) -> Option<Result<RecordBatch>> {
        self.inner.next_batch()
    }
}

pub fn spilled_batch_to_evaluated_batch(record_batch: RecordBatch) -> Result<EvaluatedBatch> {
    // Extract the data struct array (column 0) and convert back to the original
    // RecordBatch
    let data_array = record_batch
        .column(SPILL_FIELD_DATA_INDEX)
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| {
            DataFusionError::Internal("Expected data column to be a StructArray".to_owned())
        })?;

    let data_schema = Arc::new(Schema::new(match data_array.data_type() {
        DataType::Struct(fields) => fields.clone(),
        _ => {
            return Err(DataFusionError::Internal(
                "Expected data column to have Struct data type".to_owned(),
            ));
        }
    }));

    let data_columns = (0..data_array.num_columns())
        .map(|i| data_array.column(i).clone())
        .collect::<Vec<_>>();

    let batch = RecordBatch::try_new(data_schema, data_columns)?;

    // Extract the geometry array (column 1)
    let geom_array = record_batch.column(SPILL_FIELD_GEOM_INDEX).clone();

    // Extract the distance array (column 3) and convert back to ColumnarValue
    let dist_array = record_batch
        .column(SPILL_FIELD_DIST_INDEX)
        .as_any()
        .downcast_ref::<Float64Array>()
        .ok_or_else(|| {
            DataFusionError::Internal("Expected dist column to be Float64Array".to_owned())
        })?;

    let distance = if !dist_array.is_empty() {
        // Check if all values are the same (scalar case)
        let first_value = if dist_array.is_null(0) {
            None
        } else {
            Some(dist_array.value(0))
        };

        let all_same = (1..dist_array.len()).all(|i| {
            let current_value = if dist_array.is_null(i) {
                None
            } else {
                Some(dist_array.value(i))
            };
            current_value == first_value
        });

        if all_same {
            Some(ColumnarValue::Scalar(ScalarValue::Float64(first_value)))
        } else {
            Some(ColumnarValue::Array(
                record_batch.column(SPILL_FIELD_DIST_INDEX).clone(),
            ))
        }
    } else {
        None
    };

    // Create EvaluatedGeometryArray
    let mut geom_array = EvaluatedGeometryArray::new(geom_array);
    geom_array.distance = distance;

    Ok(EvaluatedBatch { batch, geom_array })
}

pub fn spilled_schema_to_evaluated_schema(spilled_schema: &SchemaRef) -> Result<SchemaRef> {
    if spilled_schema.fields().is_empty() {
        return Ok(SchemaRef::new(Schema::empty()));
    }

    let data_field = spilled_schema.field(SPILL_FIELD_DATA_INDEX);
    let inner_fields = match data_field.data_type() {
        DataType::Struct(fields) => fields.clone(),
        _ => {
            return exec_err!("Invalid schema of spilled file: {:?}", spilled_schema);
        }
    };
    Ok(SchemaRef::new(Schema::new(inner_fields)))
}

pub mod first_pass_stream;
pub mod non_partitioned_stream;
pub mod partitioned_stream_provider;

use datafusion::physical_plan::metrics::{
    self, ExecutionPlanMetricsSet, MetricBuilder, SpillMetrics,
};

#[derive(Clone, Debug)]
pub struct ProbeStreamMetrics {
    pub probe_input_batches: metrics::Count,
    pub probe_input_rows: metrics::Count,
    pub repartition_time: metrics::Time,
    pub spill_metrics: SpillMetrics,
}

impl ProbeStreamMetrics {
    pub fn new(partition: usize, metrics: &ExecutionPlanMetricsSet) -> Self {
        Self {
            probe_input_batches: MetricBuilder::new(metrics)
                .counter("probe_input_batches", partition),
            probe_input_rows: MetricBuilder::new(metrics).counter("probe_input_rows", partition),
            repartition_time: MetricBuilder::new(metrics)
                .subset_time("partition_probe_time", partition),
            spill_metrics: SpillMetrics::new(metrics, partition),
        }
    }
}

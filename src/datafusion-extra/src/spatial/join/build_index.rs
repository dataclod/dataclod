use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{DataFusionError, Result};
use datafusion::execution::memory_pool::MemoryConsumer;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::JoinType;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;

use crate::spatial::join::index::build_side_collector::{
    BuildSideBatchesCollector, CollectBuildSideMetrics,
};
use crate::spatial::join::index::spatial_index::SpatialIndex;
use crate::spatial::join::index::spatial_index_builder::{
    SpatialIndexBuilder, SpatialJoinBuildMetrics,
};
use crate::spatial::join::operand_evaluator::create_operand_evaluator;
use crate::spatial::join::option::SpatialJoinOptions;
use crate::spatial::join::spatial_predicate::SpatialPredicate;

pub async fn build_index(
    context: Arc<TaskContext>, build_schema: SchemaRef,
    build_streams: Vec<SendableRecordBatchStream>, spatial_predicate: SpatialPredicate,
    join_type: JoinType, probe_threads_count: usize, metrics: ExecutionPlanMetricsSet,
) -> Result<SpatialIndex> {
    let options = SpatialJoinOptions::default();
    let memory_pool = context.memory_pool();
    let evaluator = create_operand_evaluator(&spatial_predicate);
    let collector = BuildSideBatchesCollector::new(evaluator);
    let num_partitions = build_streams.len();
    let mut collect_metrics_vec = Vec::with_capacity(num_partitions);
    let mut reservations = Vec::with_capacity(num_partitions);
    for k in 0..num_partitions {
        let consumer =
            MemoryConsumer::new(format!("SpatialJoinCollectBuildSide[{k}]")).with_can_spill(true);
        let reservation = consumer.register(memory_pool);
        reservations.push(reservation);
        collect_metrics_vec.push(CollectBuildSideMetrics::new(k, &metrics));
    }

    let build_partitions = collector
        .collect_all(build_streams, reservations, collect_metrics_vec)
        .await?;

    let contains_external_stream = build_partitions
        .iter()
        .any(|partition| partition.build_side_batch_stream.is_external());
    if !contains_external_stream {
        let mut index_builder = SpatialIndexBuilder::new(
            build_schema,
            spatial_predicate,
            options,
            join_type,
            probe_threads_count,
            memory_pool.clone(),
            SpatialJoinBuildMetrics::new(0, &metrics),
        );
        index_builder.add_partitions(build_partitions).await?;
        index_builder.finish()
    } else {
        Err(DataFusionError::ResourcesExhausted("Memory limit exceeded while collecting indexed data. External spatial index builder is not yet implemented.".to_owned()))
    }
}

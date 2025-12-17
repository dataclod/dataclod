use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{DataFusionError, Result};
use datafusion::execution::memory_pool::MemoryConsumer;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::JoinType;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;

use crate::join::index::build_side_collector::{
    BuildSideBatchesCollector, CollectBuildSideMetrics,
};
use crate::join::index::spatial_index::SpatialIndex;
use crate::join::index::spatial_index_builder::{SpatialIndexBuilder, SpatialJoinBuildMetrics};
use crate::join::operand_evaluator::create_operand_evaluator;
use crate::join::spatial_predicate::SpatialPredicate;
use crate::option::DataClodOptions;

pub async fn build_index(
    context: Arc<TaskContext>, build_schema: SchemaRef,
    build_streams: Vec<SendableRecordBatchStream>, spatial_predicate: SpatialPredicate,
    join_type: JoinType, probe_threads_count: usize, metrics: ExecutionPlanMetricsSet,
) -> Result<SpatialIndex> {
    let session_config = context.session_config();
    let dataclod_options = session_config
        .options()
        .extensions
        .get::<DataClodOptions>()
        .cloned()
        .unwrap_or_default();
    let concurrent = dataclod_options
        .spatial_join
        .concurrent_build_side_collection;
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

    let build_partitions = if concurrent {
        // Collect partitions concurrently using collect_all which spawns tasks
        collector
            .collect_all(build_streams, reservations, collect_metrics_vec)
            .await?
    } else {
        // Collect partitions sequentially (for JNI/embedded contexts)
        let mut partitions = Vec::with_capacity(num_partitions);
        for ((stream, reservation), metrics) in build_streams
            .into_iter()
            .zip(reservations)
            .zip(&collect_metrics_vec)
        {
            let partition = collector.collect(stream, reservation, metrics).await?;
            partitions.push(partition);
        }
        partitions
    };

    let contains_external_stream = build_partitions
        .iter()
        .any(|partition| partition.build_side_batch_stream.is_external());
    if !contains_external_stream {
        let mut index_builder = SpatialIndexBuilder::new(
            build_schema,
            spatial_predicate,
            dataclod_options.spatial_join,
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

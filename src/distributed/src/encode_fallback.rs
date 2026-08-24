use std::sync::Arc;

use datafusion::common::Result;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::execution::session_state::SessionState;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_distributed::{DistributedCodec, DistributedLeafExec, NetworkBoundaryExt};
use datafusion_proto::physical_plan::{AsExecutionPlan, ComposedPhysicalExtensionCodec};
use datafusion_proto::protobuf::PhysicalPlanNode;

use crate::non_distributable::is_non_distributable_plan_name;

pub(crate) fn dry_run_encode_stages(
    plan: &Arc<dyn ExecutionPlan>, session_state: &SessionState,
) -> Result<()> {
    let codec = DistributedCodec::new_combined_with_user(session_state.config());
    plan.apply(|node| {
        if let Some(boundary) = node.as_network_boundary()
            && let Some(local) = boundary.input_stage().local_plan()
        {
            reject_named_unencodable(local)?;
            encode_specialized_stage(local.clone(), &codec)?;
        }
        Ok(datafusion::common::tree_node::TreeNodeRecursion::Continue)
    })?;
    Ok(())
}

/// `SetPlan` encodes the task-specialized plan: `DistributedLeafExec` is
/// replaced by the per-task variant. Encoding the wrapper itself is not what
/// dfd sends on the wire.
fn encode_specialized_stage(
    plan: Arc<dyn ExecutionPlan>, codec: &ComposedPhysicalExtensionCodec,
) -> Result<()> {
    let specialized = plan.transform_down(|node| {
        if let Some(leaf) = node.downcast_ref::<DistributedLeafExec>() {
            for variant in leaf.variants() {
                PhysicalPlanNode::try_from_physical_plan(variant.clone(), codec)?;
            }
            Ok(Transformed::yes(leaf.original().clone()))
        } else {
            Ok(Transformed::no(node))
        }
    })?;
    PhysicalPlanNode::try_from_physical_plan(specialized.data, codec)?;
    Ok(())
}

fn reject_named_unencodable(plan: &Arc<dyn ExecutionPlan>) -> Result<()> {
    plan.apply(|node| {
        if is_non_distributable_plan_name(node.name()) {
            return Err(datafusion::common::plan_datafusion_err!(
                "plan contains non-encodable node {}",
                node.name()
            ));
        }
        Ok(datafusion::common::tree_node::TreeNodeRecursion::Continue)
    })?;
    Ok(())
}

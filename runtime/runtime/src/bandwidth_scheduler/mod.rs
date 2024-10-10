use std::collections::BTreeMap;

use near_primitives::bandwidth_scheduler::{Bandwidth, ShardLink};
use near_primitives::types::ShardId;
use near_primitives::version::ProtocolFeature;
use near_store::{
    get_bandwidth_scheduler_state, set_bandwidth_scheduler_state, StorageError, TrieUpdate,
};
use scheduler::run_bandwidth_scheduler;

use crate::ApplyState;

mod distribute_remaining;
mod scheduler;

pub struct BandwidthGrants {
    grants: BTreeMap<ShardLink, Bandwidth>,
}

impl BandwidthGrants {
    pub fn get_granted_bandwidth(&self, from: ShardId, to: ShardId) -> Bandwidth {
        self.grants.get(&ShardLink::new(from, to)).copied().unwrap_or(0)
    }
}

pub fn calculate_bandwidth_grants(
    apply_state: &ApplyState,
    state_update: &mut TrieUpdate,
) -> Result<Option<BandwidthGrants>, StorageError> {
    let _span = tracing::debug_span!(
        target: "runtime",
        "calculate_bandwidth_grants",
        height = apply_state.block_height,
        shard_id = apply_state.shard_id)
    .entered();

    if !ProtocolFeature::BandwidthScheduler.enabled(apply_state.current_protocol_version) {
        return Ok(None);
    }

    // Read the current bandwidth scheduler state (or initialize if not present)
    let mut bandwidth_scheduler_state = get_bandwidth_scheduler_state(state_update)?;

    // TODO(resharding) - set receiver shards to the shards at the next height
    let sender_shards = apply_state.congestion_info.all_shards();
    let receiver_shards = apply_state.congestion_info.all_shards();

    // Run the bandwidth scheduler algorithm
    let bandwidth_grants = run_bandwidth_scheduler(
        &sender_shards,
        &receiver_shards,
        &apply_state.bandwidth_requests,
        &apply_state.congestion_info,
        &mut bandwidth_scheduler_state,
    );

    // Save the updated bandwidth scheduler state
    set_bandwidth_scheduler_state(state_update, &bandwidth_scheduler_state);

    Ok(Some(bandwidth_grants))
}

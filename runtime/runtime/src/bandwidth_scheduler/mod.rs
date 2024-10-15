use std::collections::BTreeMap;

use near_parameters::RuntimeConfig;
use near_primitives::bandwidth_scheduler::{Bandwidth, ShardLink};
use near_primitives::congestion_info::CongestionControl;
use near_primitives::types::{ShardId, StateChangeCause};
use near_primitives::version::ProtocolFeature;
use near_store::{
    get_bandwidth_scheduler_state, set_bandwidth_scheduler_state, StorageError, TrieUpdate,
};
use scheduler::{BandwidthScheduler, ShardCongestionStatus};

use crate::ApplyState;

mod distribute_remaining;
mod max_flow;
mod request;
mod scheduler;

pub use request::make_bandwidth_request_from_receipt_sizes;

pub fn run_bandwidth_scheduler(
    apply_state: &ApplyState,
    state_update: &mut TrieUpdate,
) -> Result<Option<BandwidthSchedulerOutput>, StorageError> {
    if !ProtocolFeature::BandwidthScheduler.enabled(apply_state.current_protocol_version) {
        return Ok(None);
    }

    let _span = tracing::debug_span!(
        target: "runtime",
        "run_bandwidth_scheduler",
        height = apply_state.block_height,
        shard_id = apply_state.shard_id)
    .entered();

    // Read the current bandwidth scheduler state (or initialize if not present)
    let bandwidth_scheduler_state = get_bandwidth_scheduler_state(state_update)?;

    let mut shard_ids = apply_state.congestion_info.all_shards();
    if shard_ids.is_empty() {
        // Congestion info not initialized yet, start with a default config
        shard_ids = vec![0];
    }

    // Collect congestion control information needed by bandwidth scheduler
    let mut shards_congestion_status: BTreeMap<ShardId, ShardCongestionStatus> = BTreeMap::new();
    for (shard_id, congestion_info) in apply_state.congestion_info.iter() {
        let congestion_control = CongestionControl::new(
            apply_state.config.congestion_control_config,
            congestion_info.congestion_info,
            congestion_info.missed_chunks_count,
        );
        let status = ShardCongestionStatus {
            is_fully_congested: congestion_control.is_fully_congested(),
            allowed_sender_shard: congestion_info
                .congestion_info
                .allowed_shard()
                .try_into()
                .expect("Converting u16 to ShardId shouldn't fail"),
            was_last_chunk_missed: congestion_info.missed_chunks_count > 0,
        };
        shards_congestion_status.insert(*shard_id, status);
    }

    let rng_seed = *apply_state.random_seed.as_bytes();

    let scheduler_params =
        BandwidthSchedulerParams::calculate_from_config(shard_ids.len(), &apply_state.config);

    let bandwidth_scheduler = BandwidthScheduler::new(
        shard_ids,
        shards_congestion_status,
        bandwidth_scheduler_state,
        rng_seed,
        scheduler_params,
    );
    let (output, new_state) = bandwidth_scheduler.run(&apply_state.bandwidth_requests.requests);

    // Save the updated bandwidth scheduler state
    set_bandwidth_scheduler_state(state_update, &new_state);
    state_update.commit(StateChangeCause::UpdatedDelayedReceipts);

    Ok(Some(output))
}

pub struct BandwidthSchedulerOutput {
    pub granted_bandwidth: BTreeMap<ShardLink, Bandwidth>,
    pub params: BandwidthSchedulerParams,
}

impl BandwidthSchedulerOutput {
    pub fn get_granted_bandwidth(&self, from: ShardId, to: ShardId) -> Bandwidth {
        self.granted_bandwidth.get(&ShardLink::new(from, to)).copied().unwrap_or(0)
    }
}

#[derive(Clone, Copy, Debug)]
pub struct BandwidthSchedulerParams {
    pub base_bandwidth: Bandwidth,
    pub max_shard_bandwidth: Bandwidth,
    pub max_receipt_size: Bandwidth,
    pub max_allowance: Bandwidth,
}

impl BandwidthSchedulerParams {
    pub fn calculate_from_config(
        shards_num: usize,
        runtime_config: &RuntimeConfig,
    ) -> BandwidthSchedulerParams {
        let shards_num_u64: u64 =
            shards_num.try_into().expect("Converting usize to u64 shouldn't fail");

        // TODO(bandwidth_scheduler) - make these a runtime parameter
        let max_shard_bandwidth: Bandwidth = 4_500_000;
        let max_base_bandwidth: Bandwidth = 100_000;

        let max_receipt_size = runtime_config.wasm_config.limit_config.max_receipt_size;

        let available_bandwidth = max_shard_bandwidth - max_receipt_size;
        let mut base_bandwidth = available_bandwidth / shards_num_u64;
        if base_bandwidth > max_base_bandwidth {
            base_bandwidth = max_base_bandwidth;
        }

        let max_allowance = max_shard_bandwidth;

        BandwidthSchedulerParams {
            base_bandwidth,
            max_shard_bandwidth,
            max_receipt_size,
            max_allowance,
        }
    }
}

use near_primitives::bandwidth_scheduler::{BandwidthSchedulerState, BlockBandwidthRequests};
use near_primitives::congestion_info::BlockCongestionInfo;
use near_primitives::types::ShardId;

use super::BandwidthGrants;

pub fn run_bandwidth_scheduler(
    _sender_shards: &[ShardId],
    _receiver_shards: &[ShardId],
    _bandwidth_requests: &BlockBandwidthRequests,
    _congestion_info: &BlockCongestionInfo,
    _state: &mut BandwidthSchedulerState,
) -> BandwidthGrants {
    todo!()
}

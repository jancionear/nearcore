use std::collections::BTreeMap;

use borsh::{BorshDeserialize, BorshSerialize};
use near_primitives::bandwidth_scheduler::{
    Bandwidth, BandwidthRequest, BandwidthRequestValues, BandwidthRequests,
    BandwidthSchedulerParams, BlockBandwidthRequests,
};
use near_primitives::types::{Balance, BlockHeight, Gas, ShardId};

use crate::congestion_control::OutgoingLimit;
use crate::ApplyState;

#[derive(Debug, BorshSerialize, BorshDeserialize)]
pub struct ChunkApplyStats {
    pub height: BlockHeight,
    pub shard_id: ShardId,
    pub is_chunk_missing: bool,
    pub transactions_num: u64,
    pub incoming_receipts_num: u64,

    pub bandwidth_scheduler: BandwidthSchedulerStats,
    pub balance: BalanceStats,
    pub receipt_sink: ReceiptSinkStats,
}

impl ChunkApplyStats {
    pub fn new(apply_state: &ApplyState) -> ChunkApplyStats {
        ChunkApplyStats {
            height: apply_state.block_height,
            shard_id: apply_state.shard_id,
            is_chunk_missing: !apply_state.is_new_chunk,
            transactions_num: 0,
            incoming_receipts_num: 0,
            bandwidth_scheduler: Default::default(),
            balance: Default::default(),
            receipt_sink: Default::default(),
        }
    }

    pub fn set_new_bandwidth_requests(
        &mut self,
        requests: &BandwidthRequests,
        params: &BandwidthSchedulerParams,
    ) {
        self.bandwidth_scheduler.set_new_bandwidth_requests(self.shard_id, requests, params);
    }

    /// Dummy data for tests.
    pub fn dummy() -> ChunkApplyStats {
        ChunkApplyStats {
            height: 0,
            shard_id: ShardId::new(0),
            is_chunk_missing: false,
            transactions_num: 0,
            incoming_receipts_num: 0,
            bandwidth_scheduler: Default::default(),
            balance: Default::default(),
            receipt_sink: Default::default(),
        }
    }
}

#[derive(Debug, Default, BorshSerialize, BorshDeserialize)]
pub struct BandwidthSchedulerStats {
    pub params: Option<BandwidthSchedulerParams>,
    pub prev_bandwidth_requests: BTreeMap<(ShardId, ShardId), Vec<Bandwidth>>,
    pub prev_bandwidth_requests_num: u64,
    pub time_to_run_ms: u128,
    pub granted_bandwidth: BTreeMap<(ShardId, ShardId), Bandwidth>,
    pub new_bandwidth_requests: BTreeMap<(ShardId, ShardId), Vec<Bandwidth>>,
}

impl BandwidthSchedulerStats {
    pub fn set_prev_bandwidth_requests(
        &mut self,
        requests: &BlockBandwidthRequests,
        params: &BandwidthSchedulerParams,
    ) {
        for (from_shard, shard_requests) in &requests.shards_bandwidth_requests {
            match shard_requests {
                BandwidthRequests::V1(requests_v1) => {
                    for request in &requests_v1.requests {
                        self.prev_bandwidth_requests.insert(
                            (*from_shard, request.to_shard.into()),
                            get_requested_values(request, params),
                        );
                    }
                }
            }
        }
        self.prev_bandwidth_requests_num = self.prev_bandwidth_requests.len().try_into().unwrap();
    }

    pub fn set_new_bandwidth_requests(
        &mut self,
        from_shard: ShardId,
        requests: &BandwidthRequests,
        params: &BandwidthSchedulerParams,
    ) {
        match requests {
            BandwidthRequests::V1(requests_v1) => {
                for request in &requests_v1.requests {
                    self.new_bandwidth_requests.insert(
                        (from_shard, request.to_shard.into()),
                        get_requested_values(request, params),
                    );
                }
            }
        }
    }
}

#[derive(Debug, Default, BorshSerialize, BorshDeserialize)]
pub struct ReceiptSinkStats {
    pub outgoing_limits: BTreeMap<ShardId, OutgoingLimit>,
    pub forwarded_receipts: BTreeMap<ShardId, ReceiptsStats>,
    pub buffered_receipts: BTreeMap<ShardId, ReceiptsStats>,
    pub final_outgoing_buffers: BTreeMap<ShardId, ReceiptsStats>,
    pub is_outgoing_metadata_ready: BTreeMap<ShardId, bool>,
    pub all_outgoing_metadatas_ready: bool,
}

#[derive(Debug, Default, BorshSerialize, BorshDeserialize)]
pub struct ReceiptsStats {
    pub num: u64,
    pub total_size: u64,
    pub total_gas: u128,
}

impl ReceiptsStats {
    pub fn add_receipt(&mut self, size: u64, gas: Gas) {
        self.num += 1;
        self.total_size += size;
        let gas_u128: u128 = gas.into();
        self.total_gas += gas_u128;
    }
}

#[derive(Debug, Default, BorshSerialize, BorshDeserialize)]
pub struct BalanceStats {
    pub tx_burnt_amount: Balance,
    pub slashed_burnt_amount: Balance,
    pub other_burnt_amount: Balance,
    /// This is a negative amount. This amount was not charged from the account that issued
    /// the transaction. It's likely due to the delayed queue of the receipts.
    pub gas_deficit_amount: Balance,
}

fn get_requested_values(
    bandwidth_request: &BandwidthRequest,
    params: &BandwidthSchedulerParams,
) -> Vec<Bandwidth> {
    let values = BandwidthRequestValues::new(params);
    let mut res = Vec::new();
    for i in 0..bandwidth_request.requested_values_bitmap.len() {
        if bandwidth_request.requested_values_bitmap.get_bit(i) {
            res.push(values.values[i]);
        }
    }
    res
}

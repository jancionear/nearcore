use std::collections::BTreeMap;

use borsh::{BorshDeserialize, BorshSerialize};
use near_primitives_core::types::{Balance, BlockHeight, Gas, ShardId};

use crate::bandwidth_scheduler::{
    Bandwidth, BandwidthRequest, BandwidthRequestValues, BandwidthRequests,
    BandwidthSchedulerParams, BlockBandwidthRequests,
};

/// Information gathered during chunk application.
/// Provides insight into what happened when the chunk was applied.
/// How many transactions and receipts were processed, buffered, forwarded, etc.
/// Useful for debugging, metrics and sanity checks.
#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub enum ChunkApplyStats {
    V0(ChunkApplyStatsV0),
}

/// Information gathered during chunk application.
/// This feature is still in development. Consider V0 as unstable, fields might be added or removed
/// from it at any time. We will do proper versioning after stabilization when there will be other
/// services depending on this structure.
#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub struct ChunkApplyStatsV0 {
    pub height: BlockHeight,
    pub shard_id: ShardId,
    pub is_chunk_missing: bool,
    pub transactions_num: u64,
    pub incoming_receipts_num: u64,

    pub bandwidth_scheduler: BandwidthSchedulerStats,
    pub balance: BalanceStats,
    pub receipt_sink: ReceiptSinkStats,
}

impl ChunkApplyStatsV0 {
    pub fn new(height: BlockHeight, shard_id: ShardId) -> ChunkApplyStatsV0 {
        ChunkApplyStatsV0 {
            height: height,
            shard_id: shard_id,
            is_chunk_missing: false,
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
    pub fn dummy() -> ChunkApplyStatsV0 {
        ChunkApplyStatsV0 {
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

#[derive(Debug, Clone, Default, BorshSerialize, BorshDeserialize)]
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

#[derive(Debug, Clone, Default, BorshSerialize, BorshDeserialize)]
pub struct ReceiptSinkStats {
    pub outgoing_limits: BTreeMap<ShardId, OutgoingLimitStats>,
    pub forwarded_receipts: BTreeMap<ShardId, ReceiptsStats>,
    pub buffered_receipts: BTreeMap<ShardId, ReceiptsStats>,
    pub final_outgoing_buffers: BTreeMap<ShardId, ReceiptsStats>,
    pub is_outgoing_metadata_ready: BTreeMap<ShardId, bool>,
    pub all_outgoing_metadatas_ready: bool,
}

impl ReceiptSinkStats {
    pub fn set_outgoing_limits(&mut self, limits: impl Iterator<Item = (ShardId, (u64, Gas))>) {
        for (shard_id, (size, gas)) in limits {
            self.outgoing_limits.insert(shard_id, OutgoingLimitStats { size, gas });
        }
    }
}

#[derive(Debug, Clone, Default, BorshSerialize, BorshDeserialize)]
pub struct OutgoingLimitStats {
    pub size: u64,
    pub gas: Gas,
}

#[derive(Debug, Clone, Default, BorshSerialize, BorshDeserialize)]
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

#[derive(Debug, Clone, Default, BorshSerialize, BorshDeserialize)]
pub struct BalanceStats {
    pub tx_burnt_amount: Balance,
    pub slashed_burnt_amount: Balance,
    pub other_burnt_amount: Balance,
    /// This is a negative amount. This amount was not charged from the account that issued
    /// the transaction. It's likely due to the delayed queue of the receipts.
    pub gas_deficit_amount: Balance,
}

/// Convert a bandwidth request from the bitmap representation to a list of requested values.
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

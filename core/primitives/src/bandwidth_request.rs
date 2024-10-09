use std::collections::BTreeMap;

use borsh::{BorshDeserialize, BorshSerialize};
use near_primitives_core::types::ShardId;
use near_schema_checker_lib::ProtocolSchema;

#[derive(
    BorshSerialize,
    BorshDeserialize,
    serde::Serialize,
    serde::Deserialize,
    Debug,
    Clone,
    PartialEq,
    Eq,
    ProtocolSchema,
)]
pub enum BandwidthRequests {
    V1(BandwidthRequestsV1),
}

impl Default for BandwidthRequests {
    fn default() -> BandwidthRequests {
        BandwidthRequests::V1(BandwidthRequestsV1 { requests: Vec::new() })
    }
}

#[derive(Clone, Debug, Default)]
pub struct BlockBandwidthRequests {
    pub requests: BTreeMap<ShardId, BandwidthRequests>,
}

#[derive(
    BorshSerialize,
    BorshDeserialize,
    serde::Serialize,
    serde::Deserialize,
    Default,
    Debug,
    Clone,
    PartialEq,
    Eq,
    ProtocolSchema,
)]
pub struct BandwidthRequestsV1 {
    requests: Vec<BandwidthRequest>,
}

#[derive(
    BorshSerialize,
    BorshDeserialize,
    serde::Serialize,
    serde::Deserialize,
    Debug,
    Clone,
    PartialEq,
    Eq,
    ProtocolSchema,
)]
pub struct BandwidthRequest {
    pub to_shard: u8,
    pub requested_values_bitmap: BandwidthRequestBitmap,
}

#[derive(
    BorshSerialize,
    BorshDeserialize,
    serde::Serialize,
    serde::Deserialize,
    Debug,
    Clone,
    PartialEq,
    Eq,
    ProtocolSchema,
)]
pub struct BandwidthRequestBitmap {
    pub data: [u8; 5],
}

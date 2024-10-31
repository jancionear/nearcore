use std::collections::{BTreeMap, VecDeque};

use borsh::{BorshDeserialize, BorshSerialize};
use near_primitives::errors::StorageError;
use near_primitives::types::{ShardId, StateChangeCause};
use near_primitives::version::ProtocolFeature;
use near_schema_checker_lib::ProtocolSchema;
use near_vm_runner::logic::ProtocolVersion;

use crate::TrieUpdate;

use super::TrieAccess;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutgoingMetadatas {
    pub metadatas: BTreeMap<ShardId, OutgoingBufferMetadata>,
    pub group_size_threshold: u64,
}

impl OutgoingMetadatas {
    pub fn new(group_size_threshold: u64) -> Self {
        Self { metadatas: BTreeMap::new(), group_size_threshold }
    }

    pub fn load(
        trie: &dyn TrieAccess,
        shard_ids: impl Iterator<Item = ShardId>,
        group_size_threshold: u64,
        protocol_version: ProtocolVersion,
    ) -> Result<Self, StorageError> {
        if !ProtocolFeature::BandwidthScheduler.enabled(protocol_version) {
            return Ok(Self::new(group_size_threshold));
        }

        let mut metadatas = BTreeMap::new();
        for shard_id in shard_ids {
            if let Some(metadata) = crate::get_outgoing_buffer_metadata(trie, shard_id)? {
                metadatas.insert(shard_id, metadata);
            }
        }
        Ok(Self { metadatas, group_size_threshold })
    }

    pub fn save(&self, state_update: &mut TrieUpdate, protocol_version: ProtocolVersion) {
        if !ProtocolFeature::BandwidthScheduler.enabled(protocol_version) {
            return;
        }

        for (shard_id, metadata) in &self.metadatas {
            crate::set_outgoing_buffer_metadata(state_update, *shard_id, metadata);
        }
        state_update.commit(StateChangeCause::SaveOutgoingBufferMetadata);
    }

    pub fn on_receipt_buffered(&mut self, shard_id: ShardId, receipt_size: u64) {
        let metadata = self
            .metadatas
            .entry(shard_id)
            .or_insert_with(|| OutgoingBufferMetadata::new(self.group_size_threshold as u64));
        metadata.on_receipt_buffered(receipt_size);
    }

    pub fn on_receipt_removed(&mut self, shard_id: ShardId, receipt_size: u64) {
        let metadata = self.metadatas.get_mut(&shard_id).expect("Metadata should exist");
        metadata.on_receipt_removed(receipt_size);
    }
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub enum OutgoingBufferMetadata {
    V0(OutgoingBufferMetadataV0),
}

impl OutgoingBufferMetadata {
    pub fn new(group_size_threshold: u64) -> Self {
        OutgoingBufferMetadata::V0(OutgoingBufferMetadataV0::new(group_size_threshold))
    }

    pub fn on_receipt_buffered(&mut self, receipt_size: u64) {
        match self {
            OutgoingBufferMetadata::V0(metadata) => {
                metadata.on_receipt_pushed(receipt_size);
            }
        }
    }

    pub fn on_receipt_removed(&mut self, receipt_size: u64) {
        match self {
            OutgoingBufferMetadata::V0(metadata) => {
                metadata.on_receipt_popped(receipt_size);
            }
        }
    }

    pub fn receipt_group_sizes(&self) -> impl Iterator<Item = u64> + '_ {
        match self {
            OutgoingBufferMetadata::V0(metadata) => metadata.receipt_group_sizes(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub struct OutgoingBufferMetadataV0 {
    pub groups: VecDeque<OutgoingReceiptGroup>,
    pub group_size_threshold: u64,
}

impl OutgoingBufferMetadataV0 {
    pub fn new(group_size_threshold: u64) -> Self {
        Self { groups: VecDeque::new(), group_size_threshold }
    }

    pub fn on_receipt_pushed(&mut self, receipt_size: u64) {
        match self.groups.back_mut() {
            Some(last_group) if last_group.group_size >= self.group_size_threshold => {
                self.groups.push_back(OutgoingReceiptGroup { group_size: receipt_size });
            }
            Some(last_group) => {
                last_group.group_size += receipt_size;
            }
            None => {
                self.groups.push_back(OutgoingReceiptGroup { group_size: receipt_size });
            }
        }
    }

    pub fn on_receipt_popped(&mut self, receipt_size: u64) {
        let first_group = self.groups.front_mut().unwrap();
        first_group.group_size -= receipt_size;
        if first_group.group_size == 0 {
            self.groups.pop_front();
        }
    }

    pub fn receipt_group_sizes(&self) -> impl Iterator<Item = u64> + '_ {
        self.groups.iter().map(|group| group.group_size)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub struct OutgoingReceiptGroup {
    pub group_size: u64,
}

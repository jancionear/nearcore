use std::collections::VecDeque;

use borsh::{BorshDeserialize, BorshSerialize};
use near_schema_checker_lib::ProtocolSchema;

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

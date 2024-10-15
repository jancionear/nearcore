use std::collections::{BTreeMap, VecDeque};

use crate::{
    get, get_outgoing_buffer_metadata, get_pure, set, set_outgoing_buffer_metadata, TrieAccess,
    TrieUpdate,
};
use borsh::{BorshDeserialize, BorshSerialize};
use near_primitives::errors::{IntegerOverflowError, StorageError};
use near_primitives::receipt::{
    BufferedReceiptIndices, ReceiptOrStateStoredReceipt, TrieQueueIndices,
};
use near_primitives::trie_key::TrieKey;
use near_primitives::types::ShardId;

/// Read-only iterator over receipt queues stored in the state trie.
///
/// This iterator currently supports delayed receipts and buffered outgoing
/// receipts.
pub struct ReceiptIterator<'a> {
    indices: std::ops::Range<u64>,
    trie_queue: &'a dyn TrieQueue,
    trie: &'a dyn TrieAccess,
    side_effects: bool,
}

/// Type safe access to delayed receipts queue stored in the state. Only use one
/// at the time for the same queue!
///
/// The struct keeps a in-memory copy of the queue indics to avoid reading it
/// from the trie on every access. Modification are written back to the
/// TrieUpdate immediately on every update.
///
/// But if you load two instances of this type at the same time, modifications
/// on one won't be synced to the other!
pub struct DelayedReceiptQueue {
    indices: TrieQueueIndices,
}

/// Type safe access to outgoing receipt buffers from this shard to all other
/// shards. Only use one at the time!
///
/// Call [`ShardsOutgoingReceiptBuffer::to_shard`] to access queue operations on
/// a buffer to a specific shard.
pub struct ShardsOutgoingReceiptBuffer {
    shards_indices: BufferedReceiptIndices,
    metadatas: BTreeMap<ShardId, OutgoingBufferMetadata>,
}

/// Type safe access to buffered receipts to a specific shard.
///
/// Construct this from a parent `ShardsOutgoingReceiptBuffer` by calling
/// [`ShardsOutgoingReceiptBuffer::to_shard`]. Modification are written back to
/// the TrieUpdate immediately on every update.
///
/// Due to the shared indices, modifying two `OutgoingReceiptBuffer` instances
/// independently would lead to inconsistencies. The mutable borrow ensures at
/// compile-time this does not happen.
pub struct OutgoingReceiptBuffer<'parent> {
    shard_id: ShardId,
    parent: &'parent mut ShardsOutgoingReceiptBuffer,
}

/// Common code for persistent queues stored in the trie.
///
/// Here we use a trait to share code between different implementations of the
/// queue. Each impl defines how it loads and stores the queue indices and the
/// queue items. Based on that, a common push(), pop(), len(), and iter()
/// implementation is provided as trait default implementation.
pub trait TrieQueue {
    /// Read queue indices of the queue from the trie, depending on impl.
    fn load_indices(&self, trie: &dyn TrieAccess) -> Result<TrieQueueIndices, StorageError>;

    /// Read indices from a cached field.
    fn indices(&self) -> TrieQueueIndices;

    /// Read and write indices from a cached field.
    fn indices_mut(&mut self) -> &mut TrieQueueIndices;

    /// Write changed indices back to the trie, using the correct trie key
    /// depending on impl.
    fn write_indices(&self, state_update: &mut TrieUpdate);

    /// Construct the trie key for a queue item depending on impl.
    fn trie_key(&self, queue_index: u64) -> TrieKey;

    fn push(
        &mut self,
        state_update: &mut TrieUpdate,
        receipt: &ReceiptOrStateStoredReceipt,
    ) -> Result<(), IntegerOverflowError> {
        self.default_push_impl(state_update, receipt)
    }

    fn default_push_impl(
        &mut self,
        state_update: &mut TrieUpdate,
        receipt: &ReceiptOrStateStoredReceipt,
    ) -> Result<(), IntegerOverflowError> {
        self.debug_check_unchanged(state_update);

        let index = self.indices().next_available_index;
        let key = self.trie_key(index);
        set(state_update, key, receipt);

        self.indices_mut().next_available_index =
            index.checked_add(1).ok_or(IntegerOverflowError)?;
        self.write_indices(state_update);
        Ok(())
    }

    fn pop(
        &mut self,
        state_update: &mut TrieUpdate,
    ) -> Result<Option<ReceiptOrStateStoredReceipt>, StorageError> {
        self.default_pop_impl(state_update)
    }

    fn default_pop_impl(
        &mut self,
        state_update: &mut TrieUpdate,
    ) -> Result<Option<ReceiptOrStateStoredReceipt<'static>>, StorageError> {
        self.debug_check_unchanged(state_update);

        let indices = self.indices();
        if indices.first_index >= indices.next_available_index {
            return Ok(None);
        }
        let key = self.trie_key(indices.first_index);
        let receipt: ReceiptOrStateStoredReceipt = get(state_update, &key)?.ok_or_else(|| {
            StorageError::StorageInconsistentState(format!(
                "Receipt #{} should be in the state",
                indices.first_index
            ))
        })?;
        state_update.remove(key);
        // Math checked above, first_index < next_available_index
        self.indices_mut().first_index += 1;
        self.write_indices(state_update);
        Ok(Some(receipt))
    }

    /// Remove up to `n` values from the end of the queue and return how many
    /// were actually remove.
    ///
    /// Unlike `pop`, this method does not return the actual receipts or even
    /// check if they existed in state.
    fn pop_n(&mut self, state_update: &mut TrieUpdate, n: u64) -> Result<u64, StorageError> {
        self.debug_check_unchanged(state_update);

        let mut removed = 0;
        for _ in 0..n {
            if self.pop(state_update)?.is_some() {
                removed += 1;
            }
        }
        Ok(removed)
    }

    fn len(&self) -> u64 {
        self.indices().len()
    }

    fn iter<'a>(&'a self, trie: &'a dyn TrieAccess, side_effects: bool) -> ReceiptIterator<'a>
    where
        Self: Sized,
    {
        if side_effects {
            self.debug_check_unchanged(trie);
        }
        ReceiptIterator {
            indices: self.indices().first_index..self.indices().next_available_index,
            trie_queue: self,
            trie,
            side_effects,
        }
    }

    /// Check the queue has not been modified in the trie view.
    ///
    /// This is a semi-expensive operation. The values should be cached in
    /// memory in at least one layer. But we still want to avoid it in
    /// production.
    #[cfg(debug_assertions)]
    fn debug_check_unchanged(&self, trie: &dyn TrieAccess) {
        debug_assert_eq!(self.indices(), self.load_indices(trie).unwrap());
    }

    #[cfg(not(debug_assertions))]
    fn debug_check_unchanged(&self, _trie: &dyn TrieAccess) {
        // nop in release build
    }
}

impl DelayedReceiptQueue {
    pub fn load(trie: &dyn TrieAccess) -> Result<Self, StorageError> {
        let indices = crate::get_delayed_receipt_indices(trie)?;
        Ok(Self { indices: indices.into() })
    }
}

impl TrieQueue for DelayedReceiptQueue {
    fn load_indices(&self, trie: &dyn TrieAccess) -> Result<TrieQueueIndices, StorageError> {
        crate::get_delayed_receipt_indices(trie).map(TrieQueueIndices::from)
    }

    fn indices(&self) -> TrieQueueIndices {
        self.indices.clone()
    }

    fn indices_mut(&mut self) -> &mut TrieQueueIndices {
        &mut self.indices
    }

    fn write_indices(&self, state_update: &mut TrieUpdate) {
        set(state_update, TrieKey::DelayedReceiptIndices, &self.indices);
    }

    fn trie_key(&self, index: u64) -> TrieKey {
        TrieKey::DelayedReceipt { index }
    }
}

impl ShardsOutgoingReceiptBuffer {
    pub fn load(trie: &dyn TrieAccess) -> Result<Self, StorageError> {
        let shards_indices = crate::get_buffered_receipt_indices(trie)?;
        let mut metadatas = BTreeMap::new();
        for shard_id in shards_indices.shard_buffers.keys() {
            let metadata = get_outgoing_buffer_metadata(trie, *shard_id)?
                .unwrap_or_else(OutgoingBufferMetadata::new);
            metadatas.insert(*shard_id, metadata);
        }
        Ok(Self { shards_indices, metadatas })
    }

    pub fn to_shard(&mut self, shard_id: ShardId) -> OutgoingReceiptBuffer {
        OutgoingReceiptBuffer { shard_id, parent: self }
    }

    pub fn get_metadata(&self, shard_id: ShardId) -> Option<&OutgoingBufferMetadata> {
        self.metadatas.get(&shard_id)
    }

    /// Returns shard IDs of all shards that have a buffer stored.
    pub fn shards(&self) -> Vec<ShardId> {
        self.shards_indices.shard_buffers.keys().copied().collect()
    }

    pub fn buffer_len(&self, shard_id: ShardId) -> Option<u64> {
        self.shards_indices.shard_buffers.get(&shard_id).map(TrieQueueIndices::len)
    }

    fn write_indices(&self, state_update: &mut TrieUpdate) {
        set(state_update, TrieKey::BufferedReceiptIndices, &self.shards_indices);
    }

    pub fn save_updated_metadata(&self, state_update: &mut TrieUpdate) {
        for (shard_id, metadata) in &self.metadatas {
            set_outgoing_buffer_metadata(state_update, *shard_id, metadata);
        }
    }
}

impl TrieQueue for OutgoingReceiptBuffer<'_> {
    fn load_indices(&self, trie: &dyn TrieAccess) -> Result<TrieQueueIndices, StorageError> {
        let all_indices: BufferedReceiptIndices =
            get(trie, &TrieKey::BufferedReceiptIndices)?.unwrap_or_default();
        let indices = all_indices.shard_buffers.get(&self.shard_id).cloned().unwrap_or_default();
        Ok(indices)
    }

    fn push(
        &mut self,
        state_update: &mut TrieUpdate,
        receipt: &ReceiptOrStateStoredReceipt,
    ) -> Result<(), IntegerOverflowError> {
        let metadata =
            self.parent.metadatas.entry(self.shard_id).or_insert_with(OutgoingBufferMetadata::new);
        metadata.update_on_receipt_pushed(receipt.get_size().unwrap());
        self.default_push_impl(state_update, receipt)
    }

    fn pop(
        &mut self,
        state_update: &mut TrieUpdate,
    ) -> Result<Option<ReceiptOrStateStoredReceipt>, StorageError> {
        let sid = self.shard_id;
        let receipt_opt = self.default_pop_impl(state_update)?;
        if let Some(receipt) = &receipt_opt {
            let metadata =
                self.parent.metadatas.entry(sid).or_insert_with(OutgoingBufferMetadata::new);
            metadata.update_on_receipt_popped(receipt.get_size().unwrap());
        }
        Ok(receipt_opt)
    }

    fn indices(&self) -> TrieQueueIndices {
        self.parent.shards_indices.shard_buffers.get(&self.shard_id).cloned().unwrap_or_default()
    }

    fn indices_mut(&mut self) -> &mut TrieQueueIndices {
        self.parent.shards_indices.shard_buffers.entry(self.shard_id).or_default()
    }

    fn write_indices(&self, state_update: &mut TrieUpdate) {
        self.parent.write_indices(state_update);
    }

    fn trie_key(&self, index: u64) -> TrieKey {
        TrieKey::BufferedReceipt { index, receiving_shard: self.shard_id }
    }
}

impl<'a> Iterator for ReceiptIterator<'a> {
    type Item = Result<ReceiptOrStateStoredReceipt<'a>, StorageError>;

    fn next(&mut self) -> Option<Self::Item> {
        let index = self.indices.next()?;
        let key = self.trie_queue.trie_key(index);
        let value =
            if self.side_effects { get(self.trie, &key) } else { get_pure(self.trie, &key) };
        let result = match value {
            Err(e) => Err(e),
            Ok(None) => Err(StorageError::StorageInconsistentState(
                "Receipt referenced by index should be in the state".to_owned(),
            )),
            Ok(Some(receipt)) => Ok(receipt),
        };
        Some(result)
    }
}

impl<'a> DoubleEndedIterator for ReceiptIterator<'a> {
    fn next_back(&mut self) -> Option<Self::Item> {
        let index = self.indices.next_back()?;
        let key = self.trie_queue.trie_key(index);
        let value =
            if self.side_effects { get(self.trie, &key) } else { get_pure(self.trie, &key) };
        let result = match value {
            Err(e) => Err(e),
            Ok(None) => Err(StorageError::StorageInconsistentState(
                "Receipt referenced by index should be in the state".to_owned(),
            )),
            Ok(Some(receipt)) => Ok(receipt),
        };
        Some(result)
    }
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;

    use super::*;
    use crate::test_utils::{gen_receipts, TestTriesBuilder};
    use crate::Trie;
    use near_primitives::receipt::Receipt;
    use near_primitives::shard_layout::ShardUId;

    #[test]
    fn test_delayed_receipts_queue() {
        // empty queues
        check_delayed_receipt_queue(&[]);

        // with random receipts
        let mut rng = rand::thread_rng();
        check_delayed_receipt_queue(&gen_receipts(&mut rng, 1));
        check_delayed_receipt_queue(&gen_receipts(&mut rng, 10));
        check_delayed_receipt_queue(&gen_receipts(&mut rng, 1000));
    }

    /// Add given receipts to the delayed receipts queue, then use
    /// `ReceiptIterator` to read them back and assert it has the same receipts
    /// in the same order. Then pop from the queue and check they are the same
    /// receipts.
    #[track_caller]
    fn check_delayed_receipt_queue(input_receipts: &[Receipt]) {
        let mut trie = init_state();

        // load a queue to fill it with receipts
        {
            let mut queue = DelayedReceiptQueue::load(&trie).expect("creating queue must not fail");
            check_push_to_receipt_queue(input_receipts, &mut trie, &mut queue);
        }

        // drop queue and load another one to see if values are persisted
        {
            let mut queue = DelayedReceiptQueue::load(&trie).expect("creating queue must not fail");
            check_receipt_queue_contains_receipts(input_receipts, &mut trie, &mut queue);
        }
    }

    #[test]
    fn test_outgoing_receipt_buffer_separately() {
        // empty queues
        check_outgoing_receipt_buffer_separately(&[]);

        // with random receipts
        let mut rng = rand::thread_rng();
        check_outgoing_receipt_buffer_separately(&gen_receipts(&mut rng, 1));
        check_outgoing_receipt_buffer_separately(&gen_receipts(&mut rng, 10));
        check_outgoing_receipt_buffer_separately(&gen_receipts(&mut rng, 1000));
    }

    /// Check if inserting, reading, and popping from the outgoing buffers
    /// works, loading one buffer at the time.
    #[track_caller]
    fn check_outgoing_receipt_buffer_separately(input_receipts: &[Receipt]) {
        let mut trie = init_state();
        for id in 0..2u32 {
            // load a buffer to fill it with receipts
            {
                let mut buffers = ShardsOutgoingReceiptBuffer::load(&trie)
                    .expect("creating buffers must not fail");
                let mut buffer = buffers.to_shard(ShardId::from(id));
                check_push_to_receipt_queue(input_receipts, &mut trie, &mut buffer);
            }

            // drop queue and load another one to see if values are persisted
            {
                let mut buffers = ShardsOutgoingReceiptBuffer::load(&trie)
                    .expect("creating buffers must not fail");
                let mut buffer = buffers.to_shard(ShardId::from(id));
                check_receipt_queue_contains_receipts(input_receipts, &mut trie, &mut buffer);
            }
        }
    }

    /// Check if inserting, reading, and popping from the outgoing buffers
    /// works, loading buffers to all shards together.
    #[test]
    fn test_outgoing_receipt_buffer_combined() {
        // empty queues
        check_outgoing_receipt_buffer_combined(&[]);

        // with random receipts
        let mut rng = rand::thread_rng();
        check_outgoing_receipt_buffer_combined(&gen_receipts(&mut rng, 1));
        check_outgoing_receipt_buffer_combined(&gen_receipts(&mut rng, 10));
        check_outgoing_receipt_buffer_combined(&gen_receipts(&mut rng, 1000));
    }

    #[track_caller]
    fn check_outgoing_receipt_buffer_combined(input_receipts: &[Receipt]) {
        let mut trie = init_state();
        // load shard_buffers once and hold on to it for the entire duration
        let mut shard_buffers =
            ShardsOutgoingReceiptBuffer::load(&trie).expect("creating buffers must not fail");
        for id in 0..2u32 {
            // load a buffer to fill it with receipts
            {
                let mut buffer = shard_buffers.to_shard(ShardId::from(id));
                check_push_to_receipt_queue(input_receipts, &mut trie, &mut buffer);
            }

            // drop queue and load another one to see if values are persisted
            {
                let mut buffer = shard_buffers.to_shard(ShardId::from(id));
                check_receipt_queue_contains_receipts(input_receipts, &mut trie, &mut buffer);
            }
        }
    }

    /// Add given receipts to the  receipts queue, then use `ReceiptIterator` to
    /// read them back and assert it has the same receipts in the same order.
    #[track_caller]
    fn check_push_to_receipt_queue(
        input_receipts: &[Receipt],
        trie: &mut TrieUpdate,
        queue: &mut impl TrieQueue,
    ) {
        for receipt in input_receipts {
            let receipt = ReceiptOrStateStoredReceipt::Receipt(Cow::Borrowed(receipt));
            queue.push(trie, &receipt).expect("pushing must not fail");
        }
        let iterated_receipts: Vec<ReceiptOrStateStoredReceipt> =
            queue.iter(trie, true).collect::<Result<_, _>>().expect("iterating should not fail");
        let iterated_receipts: Vec<Receipt> =
            iterated_receipts.into_iter().map(|receipt| receipt.into_receipt()).collect();

        // check 1: receipts should be in queue and contained in the iterator
        assert_eq!(input_receipts, iterated_receipts, "receipts were not recorded in queue");
    }

    /// Assert receipts are in the queue and accessible from an iterator and
    /// from popping one by one.
    #[track_caller]
    fn check_receipt_queue_contains_receipts(
        input_receipts: &[Receipt],
        trie: &mut TrieUpdate,
        queue: &mut impl TrieQueue,
    ) {
        // check 2: assert newly loaded queue still contains the receipts
        let iterated_receipts: Vec<ReceiptOrStateStoredReceipt> =
            queue.iter(trie, true).collect::<Result<_, _>>().expect("iterating should not fail");
        let iterated_receipts: Vec<Receipt> =
            iterated_receipts.into_iter().map(|receipt| receipt.into_receipt()).collect();
        assert_eq!(input_receipts, iterated_receipts, "receipts were not persisted correctly");

        // check 3: pop receipts from queue and check if all are returned in the right order
        let mut popped = vec![];
        while let Some(receipt) = queue.pop(trie).expect("pop must not fail") {
            let receipt = receipt.into_receipt();
            popped.push(receipt);
        }
        assert_eq!(input_receipts, popped, "receipts were not popped correctly");
    }

    fn init_state() -> TrieUpdate {
        let shard_layout_version = 1;
        let tries = TestTriesBuilder::new().with_shard_layout(shard_layout_version, 2).build();
        let state_root = Trie::EMPTY_ROOT;
        let shard_uid = ShardUId { version: shard_layout_version, shard_id: 0 };
        let trie = tries.get_trie_for_shard(shard_uid, state_root);
        TrieUpdate::new(trie)
    }
}

#[derive(Clone, BorshSerialize, BorshDeserialize, Debug)]
pub enum OutgoingBufferMetadata {
    V1(OutgoingBufferMetadataV1),
}

impl OutgoingBufferMetadata {
    pub fn new() -> OutgoingBufferMetadata {
        OutgoingBufferMetadata::V1(OutgoingBufferMetadataV1::new())
    }

    pub fn update_on_receipt_pushed(&mut self, receipt_size: u64) {
        match self {
            OutgoingBufferMetadata::V1(v1) => v1.groups.on_receipt_pushed(receipt_size),
        }
    }

    pub fn update_on_receipt_popped(&mut self, receipt_size: u64) {
        match self {
            OutgoingBufferMetadata::V1(v1) => v1.groups.on_receipt_popped(receipt_size),
        }
    }

    pub fn grouped_receipts_sizes(&self) -> impl Iterator<Item = u64> + '_ {
        match self {
            OutgoingBufferMetadata::V1(v1) => v1.groups.group_sizes_iter(),
        }
    }
}

#[derive(Clone, BorshSerialize, BorshDeserialize, Debug)]
pub struct OutgoingBufferMetadataV1 {
    groups: BufferedReceiptGroups,
}

impl OutgoingBufferMetadataV1 {
    pub fn new() -> Self {
        // TODO(bandwidth_scheduler): make min_group_size configurable
        OutgoingBufferMetadataV1 { groups: BufferedReceiptGroups::new(30_000) }
    }
}

#[derive(Debug, Clone, Copy, BorshSerialize, BorshDeserialize)]
struct BufferedReceiptGroup {
    total_size: u64,
}

impl BufferedReceiptGroup {
    pub fn new() -> BufferedReceiptGroup {
        BufferedReceiptGroup { total_size: 0 }
    }
}

#[derive(Clone, BorshSerialize, BorshDeserialize, Debug)]
struct BufferedReceiptGroups {
    groups: VecDeque<BufferedReceiptGroup>,
    min_group_size: u64,
}

impl BufferedReceiptGroups {
    pub fn new(min_group_size: u64) -> BufferedReceiptGroups {
        BufferedReceiptGroups { groups: VecDeque::new(), min_group_size }
    }

    pub fn on_receipt_pushed(&mut self, receipt_size: u64) {
        let mut last_group = self.groups.pop_back().unwrap_or_else(BufferedReceiptGroup::new);
        if last_group.total_size >= self.min_group_size {
            self.groups.push_back(last_group);
            last_group = BufferedReceiptGroup::new();
        }
        last_group.total_size = last_group.total_size.checked_add(receipt_size).expect(
            "Total size of stored delayed receipts has exceeded 18 Exabytes. This shouldn't happen",
        );
        self.groups.push_back(last_group)
    }

    pub fn on_receipt_popped(&mut self, receipt_size: u64) {
        let Some(mut first_group) = self.groups.pop_front() else {
            // This could happen when popping receipts which were added before the protocol that uses group tracking was active.
            return;
        };
        first_group.total_size -= receipt_size;
        if first_group.total_size > 0 {
            self.groups.push_front(first_group);
        }
    }

    pub fn group_sizes_iter(&self) -> impl Iterator<Item = u64> + '_ {
        self.groups.iter().map(|g| g.total_size)
    }
}

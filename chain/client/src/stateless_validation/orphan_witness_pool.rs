use std::collections::{HashMap, HashSet};

use lru::LruCache;
use near_primitives::hash::CryptoHash;
use near_primitives::stateless_validation::ChunkStateWitness;
use near_primitives::types::{AccountId, BlockHeight, ShardId};

type ChunkProducerId = (AccountId, ShardId);

pub struct OrphanStateWitnessPool {
    chunk_producer_caches: LruCache<ChunkProducerId, LruCache<BlockHeight, ChunkStateWitness>>,
    chunk_producer_cache_capacity: usize,
    waiting_for_block: HashMap<CryptoHash, HashSet<(ChunkProducerId, BlockHeight)>>,
}

impl OrphanStateWitnessPool {
    pub fn new(chunk_producers_capacity: usize, chunk_producer_cache_capacity: usize) -> Self {
        OrphanStateWitnessPool {
            chunk_producer_caches: LruCache::new(chunk_producers_capacity),
            chunk_producer_cache_capacity,
            waiting_for_block: HashMap::new(),
        }
    }

    pub fn add_orphan_state_witness(
        &mut self,
        witness: ChunkStateWitness,
        chunk_producer: AccountId,
    ) {
        if self.chunk_producer_caches.cap() == 0 || self.chunk_producer_cache_capacity == 0 {
            // A cache with 0 capacity doesn't keep anything.
            return;
        }

        let chunk_header = &witness.inner.chunk_header;
        let shard_id = chunk_header.shard_id();
        let height = chunk_header.height_created();
        let prev_block_hash = chunk_header.prev_block_hash().clone();
        let chunk_producer_id = (chunk_producer, shard_id);

        match self.chunk_producer_caches.get_mut(&chunk_producer_id) {
            Some(chunk_producer_cache) => {
                let ejected = chunk_producer_cache.push(height, witness);
                if let Some((_height, ejected_witness)) = ejected {
                    self.remove_from_waiting_for_block(chunk_producer_id.clone(), ejected_witness);
                }
            }
            None => {
                let mut new_cache = LruCache::new(self.chunk_producer_cache_capacity);
                new_cache.put(height, witness);
                let ejected = self.chunk_producer_caches.push(chunk_producer_id.clone(), new_cache);
                if let Some((ejected_chunk_producer_id, ejected_cache)) = ejected {
                    for (_height, ejected_witness) in ejected_cache {
                        self.remove_from_waiting_for_block(
                            ejected_chunk_producer_id.clone(),
                            ejected_witness,
                        );
                    }
                }
            }
        }

        self.waiting_for_block
            .entry(prev_block_hash)
            .or_insert_with(|| HashSet::new())
            .insert((chunk_producer_id, height));
    }

    fn remove_from_waiting_for_block(
        &mut self,
        chunk_producer_id: ChunkProducerId,
        witness: ChunkStateWitness,
    ) {
        let block_hash = witness.inner.chunk_header.prev_block_hash();
        let height = witness.inner.chunk_header.height_created();
        let waiting_set = self
            .waiting_for_block
            .get_mut(block_hash)
            .expect("Every ejected witness must have a corresponding entry in waiting_for_block.");
        waiting_set.remove(&(chunk_producer_id, height));
        if waiting_set.is_empty() {
            self.waiting_for_block.remove(block_hash);
        }
    }

    pub fn take_state_witnesses_waiting_for_block(
        &mut self,
        prev_block: &CryptoHash,
    ) -> Vec<ChunkStateWitness> {
        let Some(waiting) = self.waiting_for_block.remove(prev_block) else {
            return Vec::new();
        };
        let mut result = Vec::new();
        for (chunk_producer_id, height) in waiting {
            let producer_cache = self.chunk_producer_caches.get_mut(&chunk_producer_id).expect(
                "Every entry in waiting_for_block must have a corresponding witness in the cache.",
            );
            let witness = producer_cache.pop(&height).expect(
                "Every entry in waiting_for_block must have a corresponding witness in the cache",
            );
            if producer_cache.is_empty() {
                self.chunk_producer_caches.pop(&chunk_producer_id);
            }
            result.push(witness);
        }
        result
    }
}

impl Default for OrphanStateWitnessPool {
    fn default() -> OrphanStateWitnessPool {
        OrphanStateWitnessPool::new(128, 4)
    }
}

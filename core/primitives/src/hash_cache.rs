use std::sync::Arc;

use crate::hash::{hash, CryptoHash};

pub struct HashCache {
    cache: dashmap::DashMap<Arc<[u8]>, CryptoHash>,
}

impl HashCache {
    pub fn new() -> Self {
        HashCache { cache: dashmap::DashMap::new() }
    }

    pub fn hash(&self, data: &[u8]) -> CryptoHash {
        if let Some(hash) = self.cache.get(data) {
            hash.clone()
        } else {
            let h = hash(data);
            self.cache.insert(Arc::from(data), h.clone());
            h
        }
    }

    pub fn clear(&self) {
        self.cache.clear();
    }
}

pub static HASH_CACHE: std::sync::LazyLock<HashCache> = std::sync::LazyLock::new(HashCache::new);

use std::collections::HashMap;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use near_chain::ChainStore;
use near_chain_configs::GenesisValidationMode;

use near_primitives::hash::CryptoHash;
use near_primitives::merkle::PartialMerkleTree;
use near_primitives::types::BlockHeight;
use near_store::{DBCol, Store};
use nearcore::config::load_config;

use nearcore::open_storage;

#[derive(Parser)]
pub(crate) struct OrdinalCommand {
    /// Get block hash with this ordinal
    #[arg(long)]
    get: Option<u64>,

    /// Delete block hash with this ordinal
    #[arg(long)]
    del: Option<u64>,

    /// Find first block that we have ordinal for
    #[arg(long)]
    find_first: bool,

    #[arg(long)]
    scan_for_corrupt: bool,
    // #[arg(long, default_value_t = 1000)]
    // scan_concurrency: usize,
}

impl OrdinalCommand {
    pub(crate) fn run(
        &self,
        home: &PathBuf,
        genesis_validation: GenesisValidationMode,
    ) -> anyhow::Result<()> {
        // Create a ChainStore and EpochManager that will be used to read blockchain data.
        let mut near_config = load_config(home, genesis_validation).unwrap();
        let node_storage = open_storage(&home, &mut near_config).unwrap();
        let store = node_storage.get_split_store().unwrap_or_else(|| node_storage.get_hot_store());
        let chain_store = Rc::new(ChainStore::new(
            store.clone(),
            false,
            near_config.genesis.config.transaction_validity_period,
        ));

        println!("get {:?}, del {:?}, find-first {}", self.get, self.del, self.find_first);

        if let Some(ordinal) = self.get {
            // Get the block hash for the given ordinal.
            let block_hash = chain_store.get_block_hash_from_ordinal(ordinal);
            println!("Block hash for ordinal {}: {:?}", ordinal, block_hash);
            return Ok(());
        }

        if let Some(ordinal) = self.del {
            chain_store.delete_ordinal(ordinal).unwrap_or_else(|e| {
                panic!("Failed to delete block hash for ordinal {}: {}", ordinal, e)
            });
        }

        if self.find_first {
            let mut jump = 2u64.pow(63);
            let mut cur: u64 = 0;

            let tip_block =
                chain_store.get_block_header(&chain_store.head().unwrap().last_block_hash).unwrap();

            while jump > 1 {
                let candidate = cur + jump;
                if candidate < tip_block.block_ordinal()
                    && chain_store.get_block_hash_from_ordinal(candidate).is_err()
                {
                    cur = candidate;
                }

                jump = jump / 2;
            }

            println!("First block ordinal we don't have: {}", cur);
            for ordinal in cur.saturating_sub(5)..cur.saturating_add(5) {
                match chain_store.get_block_hash_from_ordinal(ordinal) {
                    Ok(block_hash) => {
                        println!("Ordinal {}: {:?}", ordinal, block_hash);
                    }
                    Err(_) => {
                        println!("Ordinal {}: not found", ordinal);
                    }
                }
            }
        }

        if self.scan_for_corrupt {
            let last_block_ordinal = chain_store
                .get_block_merkle_tree(&chain_store.head().unwrap().last_block_hash)
                .unwrap()
                .size();
            let expected_count = last_block_ordinal as usize + 100_000;

            let store1 = store.clone();
            let store2 = store.clone();
            let store3 = store.clone();
            let read_ordinal_to_block_hash_thread =
                std::thread::spawn(move || read_ordinal_to_block_hash(&store1, expected_count));
            let read_height_to_block_hash =
                std::thread::spawn(move || read_height_to_block_hash(&store2, expected_count));
            let read_block_hash_to_ordinal =
                std::thread::spawn(move || read_block_hash_to_ordinal(&store3, expected_count));

            let ordinal_to_block_hash = Arc::new(read_ordinal_to_block_hash_thread.join().unwrap());
            let height_to_block_hash = Arc::new(read_height_to_block_hash.join().unwrap());
            let block_hash_to_ordinal = Arc::new(read_block_hash_to_ordinal.join().unwrap());

            let genesis_height = chain_store.genesis_height();
            let tip_height = chain_store.head().unwrap().height;
            let mut scan_timer = WorkTimer::new(
                "Scan for corrupt blocks",
                (tip_height - genesis_height + 1) as usize,
            );

            let mut total_corrupt = 0;
            for height in genesis_height..=tip_height {
                if check_is_height_corrupt(
                    height,
                    &ordinal_to_block_hash,
                    &height_to_block_hash,
                    &block_hash_to_ordinal,
                ) {
                    total_corrupt += 1;
                    println!(
                        "Corrupt block found at height {} (total corrupt: {})",
                        height, total_corrupt
                    );
                }
                scan_timer.update_total((height - genesis_height + 1) as usize);
            }
            println!("Scan finished, total corrupt blocks: {}", total_corrupt);
        }

        Ok(())
    }
}

fn read_ordinal_to_block_hash(store: &Store, expected_count: usize) -> HashMap<u64, CryptoHash> {
    let mut read_timer = WorkTimer::new("Read DBCol::BlockOrdinal", expected_count);

    let mut ordinal_to_block_hash = HashMap::with_capacity(expected_count);
    let mut iter = store.iter_ser::<CryptoHash>(DBCol::BlockOrdinal);
    while let Some(res) = iter.next() {
        let (ordinal_bytes, block_hash) = res.unwrap();
        let ordinal = u64::from_le_bytes((*ordinal_bytes).try_into().unwrap());
        ordinal_to_block_hash.insert(ordinal, block_hash);
        read_timer.update_total(ordinal_to_block_hash.len());
    }

    read_timer.finish();
    ordinal_to_block_hash
}

fn read_height_to_block_hash(store: &Store, expected_count: usize) -> HashMap<u64, CryptoHash> {
    let mut read_timer = WorkTimer::new("Read DBCol::BlockHeight", expected_count);

    let mut height_to_block_hash = HashMap::with_capacity(expected_count);
    let mut iter = store.iter_ser::<CryptoHash>(DBCol::BlockHeight);
    while let Some(res) = iter.next() {
        let (height_bytes, block_hash) = res.unwrap();
        let height = u64::from_le_bytes((*height_bytes).try_into().unwrap());
        height_to_block_hash.insert(height, block_hash);
        read_timer.update_total(height_to_block_hash.len());
    }
    read_timer.finish();
    height_to_block_hash
}

fn read_block_hash_to_ordinal(store: &Store, expected_count: usize) -> HashMap<CryptoHash, u64> {
    let mut read_timer = WorkTimer::new("Read DBCol::BlockMerkleTree", expected_count);

    let mut block_hash_to_ordinal = HashMap::with_capacity(expected_count);
    let mut iter = store.iter_ser::<PartialMerkleTree>(DBCol::BlockMerkleTree);
    while let Some(res) = iter.next() {
        let (block_hash, tree) = res.unwrap();
        let ordinal = tree.size();
        block_hash_to_ordinal.insert((*block_hash).try_into().unwrap(), ordinal);
        read_timer.update_total(block_hash_to_ordinal.len());
    }

    read_timer.finish();
    block_hash_to_ordinal
}

fn check_is_height_corrupt(
    height: BlockHeight,
    ordinal_to_block_hash: &HashMap<u64, CryptoHash>,
    height_to_block_hash: &HashMap<u64, CryptoHash>,
    block_hash_to_ordinal: &HashMap<CryptoHash, u64>,
) -> bool {
    if let Some(block_hash) = height_to_block_hash.get(&height) {
        if let Some(ordinal) = block_hash_to_ordinal.get(block_hash) {
            if let Some(expected_block_hash) = ordinal_to_block_hash.get(ordinal) {
                return expected_block_hash != block_hash;
            }
        }
    }
    false
}

struct WorkTimer {
    name: String,
    start: std::time::Instant,
    last_report_time: std::time::Instant,
    total: usize,
    expected_total: usize,
}

impl WorkTimer {
    fn new(name: &str, expected_total: usize) -> Self {
        println!("Starting read timer \"{}\"", name);
        Self {
            name: name.to_string(),
            start: std::time::Instant::now(),
            last_report_time: std::time::Instant::now(),
            total: 0,
            expected_total,
        }
    }

    fn update_total(&mut self, total: usize) {
        self.total = total;
        if self.last_report_time.elapsed() > Duration::from_secs(5) {
            println!(
                "{}: {}/{} ({:.2}%) in {:?}, ETA: {:.2?}s",
                self.name,
                self.total,
                self.expected_total,
                (self.total as f64 / self.expected_total as f64) * 100.0,
                self.start.elapsed(),
                (self.expected_total - self.total) as f64 / self.total as f64
                    * self.start.elapsed().as_secs_f64()
            );
            self.last_report_time = std::time::Instant::now();
        }
    }

    fn finish(&self) {
        println!(
            "{}: Finished reading {} entries in {:?}",
            self.name,
            self.total,
            self.start.elapsed()
        );
    }
}

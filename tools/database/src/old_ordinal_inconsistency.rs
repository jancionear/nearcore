use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use near_chain::ChainStore;
use near_chain_configs::GenesisValidationMode;
use near_primitives::hash::CryptoHash;
use near_primitives::merkle::PartialMerkleTree;
use near_primitives::types::{BlockHeight, NumBlocks};
use near_primitives::utils::index_to_bytes;
use near_store::adapter::StoreAdapter;
use near_store::{DBCol, Store};

#[derive(clap::Parser)]
#[clap(subcommand_required = true, arg_required_else_help = true)]
pub(crate) enum OrdinalInconsistencyCommand {
    Scan(ScanCommand),
    ScanAndFix(ScanAndFixCommand),
}

#[derive(clap::Args)]
pub(crate) struct ScanCommand {}

#[derive(clap::Args)]
pub(crate) struct ScanAndFixCommand {
    #[clap(long)]
    pub noconfirm: bool,
}

impl OrdinalInconsistencyCommand {
    pub(crate) fn run(
        &self,
        home: &PathBuf,
        genesis_validation: GenesisValidationMode,
    ) -> anyhow::Result<()> {
        let mut near_config = nearcore::config::load_config(home, genesis_validation)?;
        let node_storage = nearcore::open_storage(home, &mut near_config)?;
        let store = node_storage.get_split_store().unwrap_or_else(|| node_storage.get_hot_store());
        let chain_store = ChainStore::new(
            store.clone(),
            false,
            near_config.genesis.config.transaction_validity_period,
        );

        match self {
            OrdinalInconsistencyCommand::Scan(_) => {
                find_ordinal_inconsistencies(&chain_store).unwrap();
            }
            OrdinalInconsistencyCommand::ScanAndFix(scan_and_fix_cmd) => {
                let inconsistencies = find_ordinal_inconsistencies(&chain_store).unwrap();
                if !scan_and_fix_cmd.noconfirm {
                    todo!()
                }
                fix_ordinal_inconsistencies(&store, &inconsistencies).unwrap();
            }
        }
        Ok(())
    }
}

pub struct OrdinalInconsitency {
    pub block_height: BlockHeight,
    pub block_ordinal: NumBlocks,
    pub correct_block_hash: HashIndex,
    pub actual_block_hash: HashIndex,
}

pub fn find_ordinal_inconsistencies(
    chain_store: &ChainStore,
) -> anyhow::Result<Vec<OrdinalInconsitency>> {
    // First read data
    let tip = chain_store.head()?;
    let last_block_ordinal = chain_store.get_block_merkle_tree(&tip.last_block_hash)?.size();
    let expected_count: usize = (last_block_ordinal + 1).try_into().unwrap();

    let (db_update_sender, db_update_receiver) = std::sync::mpsc::channel::<DbReadUpdate>();
    let store = chain_store.store();

    let read_height_to_block_hash_thread = {
        let store = store.clone();
        let db_update_sender = db_update_sender.clone();
        std::thread::spawn(move || {
            read_height_to_block_hash(&store, expected_count, &db_update_sender)
        })
    };
    let read_block_hash_to_ordinal_thread = {
        let store = store.clone();
        let db_update_sender = db_update_sender.clone();
        std::thread::spawn(move || {
            read_block_hash_to_ordinal(&store, expected_count, &db_update_sender)
        })
    };
    let read_ordinal_to_block_hash_thread = {
        let store = store.clone();
        let db_update_sender = db_update_sender.clone();
        std::thread::spawn(move || {
            read_ordinal_to_block_hash(&store, expected_count, &db_update_sender)
        })
    };

    std::mem::drop(db_update_sender);

    let mut hash_to_index: HashMap<CryptoHash, HashIndex> = HashMap::with_capacity(expected_count);
    let mut height_to_hash: Vec<(u32, HashIndex)> = Vec::with_capacity(expected_count);
    let mut ordinal_to_hash: HashMap<u32, HashIndex> = HashMap::with_capacity(expected_count);
    let mut hash_to_ordinal: HashMap<HashIndex, u32> = HashMap::with_capacity(expected_count);

    while let Ok(db_update) = db_update_receiver.recv() {
        let next_hash_index = HashIndex(hash_to_index.len().try_into().unwrap());
        match db_update {
            DbReadUpdate::HeightToBlockHash(entries) => {
                for (height, block_hash) in entries {
                    let hash_index = hash_to_index.entry(block_hash).or_insert(next_hash_index);
                    height_to_hash.push((height.try_into().unwrap(), *hash_index));
                }
            }
            DbReadUpdate::BlockHashToOrdinal(entries) => {
                for (block_hash, ordinal) in entries {
                    let hash_index = hash_to_index.entry(block_hash).or_insert(next_hash_index);
                    hash_to_ordinal.insert(*hash_index, ordinal.try_into().unwrap());
                }
            }
            DbReadUpdate::OrdinalToBlockHash(entries) => {
                for (ordinal, block_hash) in entries {
                    let hash_index = hash_to_index.entry(block_hash).or_insert(next_hash_index);
                    ordinal_to_hash.insert(ordinal.try_into().unwrap(), *hash_index);
                }
            }
        }
    }

    read_height_to_block_hash_thread.join().unwrap();
    read_block_hash_to_ordinal_thread.join().unwrap();
    read_ordinal_to_block_hash_thread.join().unwrap();

    // Now find inconsistencies
    let hash_to_index = Arc::new(hash_to_index);
    let height_to_hash = Arc::new(height_to_hash);
    let ordinal_to_hash = Arc::new(ordinal_to_hash);
    let hash_to_ordinal = Arc::new(hash_to_ordinal);

    let num_threads = 128;
    let (update_sender, update_receiver) = std::sync::mpsc::channel::<FindInconsistenciesUpdate>();
    let mut threads = Vec::with_capacity(num_threads);
    for thread_id in 0..num_threads {
        let height_to_hash = Arc::clone(&height_to_hash);
        let ordinal_to_hash = Arc::clone(&ordinal_to_hash);
        let hash_to_ordinal = Arc::clone(&hash_to_ordinal);
        let update_sender = update_sender.clone();
        threads.push(std::thread::spawn(move || {
            find_inconsistencies_thread(
                &height_to_hash,
                &ordinal_to_hash,
                &hash_to_ordinal,
                &update_sender,
                thread_id,
                num_threads,
            )
        }));
    }
    std::mem::drop(update_sender);

    let mut found_inconsistencies = Vec::new();
    let mut processed_counter = 0;

    while let Ok(update) = update_receiver.recv() {
        match update {
            FindInconsistenciesUpdate::Inconsistency {
                block_height,
                block_ordinal,
                correct_block_hash,
                actual_block_hash,
            } => {
                todo!()
            }
            FindInconsistenciesUpdate::Processed(count) => {
                processed_counter += count;
            }
        }
    }

    todo!()
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
struct HashIndex(u32);

pub enum DbReadUpdate {
    HeightToBlockHash(Vec<(BlockHeight, CryptoHash)>),
    BlockHashToOrdinal(Vec<(CryptoHash, u64)>),
    OrdinalToBlockHash(Vec<(u64, CryptoHash)>),
}

impl DbReadUpdate {
    fn batch_size() -> usize {
        512
    }
}

fn read_height_to_block_hash(
    store: &Store,
    expected_count: usize,
    db_update_sender: &std::sync::mpsc::Sender<DbReadUpdate>,
) {
    let mut read_timer = WorkTimer::new("Read DBCol::BlockHeight", expected_count);

    let mut cur_batch = Vec::with_capacity(DbReadUpdate::batch_size());
    let mut total_read = 0;

    let mut iter = store.iter_ser::<CryptoHash>(DBCol::BlockHeight);
    while let Some(res) = iter.next() {
        let (height_bytes, block_hash) = res.unwrap();
        let height = u64::from_le_bytes((*height_bytes).try_into().unwrap());

        cur_batch.push((height, block_hash));

        if cur_batch.len() >= DbReadUpdate::batch_size() {
            db_update_sender.send(DbReadUpdate::HeightToBlockHash(cur_batch)).unwrap();
            cur_batch = Vec::with_capacity(DbReadUpdate::batch_size());
        }

        total_read += 1;
        read_timer.update_total(total_read);
    }

    db_update_sender.send(DbReadUpdate::HeightToBlockHash(cur_batch)).unwrap();
    read_timer.finish();
}

fn read_block_hash_to_ordinal(
    store: &Store,
    expected_count: usize,
    db_update_sender: &std::sync::mpsc::Sender<DbReadUpdate>,
) {
    let mut read_timer = WorkTimer::new("Read DBCol::BlockMerkleTree", expected_count);

    let mut cur_batch = Vec::with_capacity(DbReadUpdate::batch_size());
    let mut total_read = 0;

    let mut iter = store.iter_ser::<PartialMerkleTree>(DBCol::BlockMerkleTree);
    while let Some(res) = iter.next() {
        let (block_hash_bytes, tree) = res.unwrap();
        let block_hash = CryptoHash::try_from(&*block_hash_bytes).unwrap();
        let ordinal = tree.size();

        cur_batch.push((block_hash, ordinal));
        if cur_batch.len() >= DbReadUpdate::batch_size() {
            db_update_sender.send(DbReadUpdate::BlockHashToOrdinal(cur_batch)).unwrap();
            cur_batch = Vec::with_capacity(DbReadUpdate::batch_size());
        }

        total_read += 1;
        read_timer.update_total(total_read);
    }

    db_update_sender.send(DbReadUpdate::BlockHashToOrdinal(cur_batch)).unwrap();
    read_timer.finish();
}

fn read_ordinal_to_block_hash(
    store: &Store,
    expected_count: usize,
    db_update_sender: &std::sync::mpsc::Sender<DbReadUpdate>,
) {
    let mut read_timer = WorkTimer::new("Read DBCol::BlockOrdinal", expected_count);

    let mut cur_batch = Vec::with_capacity(DbReadUpdate::batch_size());
    let mut total_read = 0;

    let mut iter = store.iter_ser::<CryptoHash>(DBCol::BlockOrdinal);
    while let Some(res) = iter.next() {
        let (ordinal_bytes, block_hash) = res.unwrap();
        let ordinal = u64::from_le_bytes((*ordinal_bytes).try_into().unwrap());

        cur_batch.push((ordinal, block_hash));
        if cur_batch.len() >= 500 {
            db_update_sender.send(DbReadUpdate::OrdinalToBlockHash(cur_batch)).unwrap();
            cur_batch = Vec::with_capacity(DbReadUpdate::batch_size());
        }

        total_read += 1;
        read_timer.update_total(total_read);
    }

    db_update_sender.send(DbReadUpdate::OrdinalToBlockHash(cur_batch)).unwrap();
    read_timer.finish();
}

enum FindInconsistenciesUpdate {
    Inconsistency {
        block_height: u32,
        block_ordinal: u32,
        correct_block_hash: HashIndex,
        actual_block_hash: HashIndex,
    },
    Processed(usize),
}

pub fn find_inconsistencies_thread(
    height_to_hash: &Vec<(u32, HashIndex)>,
    ordinal_to_hash: &HashMap<u32, HashIndex>,
    hash_to_ordinal: &HashMap<HashIndex, u32>,
    update_sender: &std::sync::mpsc::Sender<FindInconsistenciesUpdate>,
    thread_id: usize,
    num_threads: usize,
) {
    let mut processed_counter = 0;

    for i in (thread_id..height_to_hash.len()).step_by(num_threads) {
        let (height, block_hash) = height_to_hash[i];

        if let Some(block_ordinal) = hash_to_ordinal.get(&block_hash) {
            if let Some(hash_at_ordinal) = ordinal_to_hash.get(&block_ordinal) {
                if *hash_at_ordinal != block_hash {
                    update_sender
                        .send(FindInconsistenciesUpdate::Inconsistency {
                            block_height: (height).into(),
                            block_ordinal: (*block_ordinal).into(),
                            correct_block_hash: block_hash,
                            actual_block_hash: *hash_at_ordinal,
                        })
                        .unwrap();
                }
            }
        }

        processed_counter += 1;
        if processed_counter == 1000 {
            update_sender.send(FindInconsistenciesUpdate::Processed(processed_counter)).unwrap();
            processed_counter = 0;
        }
    }
    update_sender.send(FindInconsistenciesUpdate::Processed(processed_counter)).unwrap();
}

pub fn fix_ordinal_inconsistencies(
    store: &Store,
    inconsistencies: &[OrdinalInconsitency],
) -> anyhow::Result<()> {
    let mut update = store.store_update();

    for inconsistency in inconsistencies {
        update.set_ser(
            DBCol::BlockOrdinal,
            &index_to_bytes(inconsistency.block_ordinal),
            &inconsistency.correct_block_hash,
        )?;
    }

    update.commit()?;
    Ok(())
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

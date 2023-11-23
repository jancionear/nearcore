use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
    sync::Arc,
};

use clap::Parser;
use near_chain::{Block, ChainStore, ChainStoreAccess, Error};
use near_epoch_manager::EpochManager;
use near_primitives::{
    epoch_manager::block_info::BlockInfo,
    hash::CryptoHash,
    shard_layout::{account_id_to_shard_id, ShardLayout},
    transaction::ExecutionOutcome,
    types::{AccountId, BlockHeight, EpochId, Gas, ShardId},
};
use near_store::{NodeStorage, ShardUId, Store};
use nearcore::open_storage;

#[derive(Parser)]
pub(crate) struct AnalyseGasUsageCommand {
    /// Analyse the last N blocks
    #[arg(long)]
    last_blocks: Option<u64>,

    /// Analyse blocks from the given block height, inclusive
    #[arg(long)]
    from_block_height: Option<BlockHeight>,

    /// Analyse blocks up to the given block height, inclusive
    #[arg(long)]
    to_block_height: Option<BlockHeight>,
}

impl AnalyseGasUsageCommand {
    pub(crate) fn run(&self, home: &PathBuf) -> anyhow::Result<()> {
        let mut near_config =
            nearcore::config::load_config(home, near_chain_configs::GenesisValidationMode::Full)
                .unwrap();
        let node_storage: NodeStorage = open_storage(&home, &mut near_config).unwrap();
        let store: Store =
            node_storage.get_split_store().unwrap_or_else(|| node_storage.get_hot_store());
        let chain_store = Arc::new(ChainStore::new(
            store.clone(),
            near_config.genesis.config.genesis_height,
            false,
        ));
        let epoch_manager =
            EpochManager::new_from_genesis_config(store, &near_config.genesis.config).unwrap();

        let blocks_iterator = self.make_block_iterator(chain_store.clone());

        analyse_gas_usage(blocks_iterator, &chain_store, &epoch_manager);
        Ok(())
    }

    fn make_block_iterator(&self, chain_store: Arc<ChainStore>) -> Box<dyn Iterator<Item = Block>> {
        if let Some(last_blocks) = self.last_blocks {
            println!("Performing analysis on the last {last_blocks} blocks");
            return Box::new(LastNBlocksIterator::new(last_blocks, chain_store));
        }

        if self.from_block_height.is_none() && self.to_block_height.is_none() {
            // The user didn't provide any arguments, default to last 1000 blocks
            println!("Defaulting to last 1000 blocks");
            return Box::new(LastNBlocksIterator::new(1000, chain_store));
        }

        Box::new(BlockHeightRangeIterator::new(
            self.from_block_height,
            self.to_block_height,
            chain_store,
        ))
    }
}

struct LastNBlocksIterator {
    chain_store: Arc<ChainStore>,
    blocks_left: u64,
    current_block_hash: Option<CryptoHash>,
}

impl LastNBlocksIterator {
    pub fn new(blocks_num: u64, chain_store: Arc<ChainStore>) -> LastNBlocksIterator {
        let current_block_hash = Some(chain_store.head().unwrap().last_block_hash);
        LastNBlocksIterator { chain_store, blocks_left: blocks_num, current_block_hash }
    }
}

impl Iterator for LastNBlocksIterator {
    type Item = Block;

    fn next(&mut self) -> Option<Block> {
        if self.blocks_left == 0 {
            return None;
        }
        self.blocks_left -= 1;

        if let Some(current_block_hash) = self.current_block_hash.take() {
            let current_block: Block = self.chain_store.get_block(&current_block_hash).unwrap();

            // Set the previous block as "current" one, as long as the current one isn't the genesis block
            if current_block.header().height() != self.chain_store.get_genesis_height() {
                self.current_block_hash = Some(current_block.header().prev_hash().clone());
            }
            return Some(current_block);
        }

        None
    }
}

struct BlockHeightRangeIterator {
    chain_store: Arc<ChainStore>,
    current_block_hash: Option<CryptoHash>,
    from_block_height: BlockHeight,
}

impl BlockHeightRangeIterator {
    pub fn new(
        from_height_opt: Option<BlockHeight>,
        to_height_opt: Option<BlockHeight>,
        chain_store: Arc<ChainStore>,
    ) -> BlockHeightRangeIterator {
        if let (Some(from), Some(to)) = (&from_height_opt, &to_height_opt) {
            if *from > *to {
                // Empty iterator
                return BlockHeightRangeIterator {
                    chain_store,
                    from_block_height: 0,
                    current_block_hash: None,
                };
            }
        }

        let min_height: BlockHeight = chain_store.get_genesis_height();
        let max_height: BlockHeight = chain_store.head().unwrap().height;

        let from_height: BlockHeight =
            from_height_opt.unwrap_or(min_height).clamp(min_height, max_height);
        let to_height: BlockHeight =
            to_height_opt.unwrap_or(max_height).clamp(min_height, max_height);

        // A block with height `to_height` might not exist.
        // Go over the range in reverse and find the highest block that exists.
        let mut current_block_hash: Option<CryptoHash> = None;
        for height in (from_height..=to_height).rev() {
            match chain_store.get_block_hash_by_height(height) {
                Ok(hash) => {
                    current_block_hash = Some(hash);
                    break;
                }
                Err(Error::DBNotFoundErr(_)) => continue,
                err => err.unwrap(),
            };
        }

        BlockHeightRangeIterator { chain_store, from_block_height: from_height, current_block_hash }
    }
}

impl Iterator for BlockHeightRangeIterator {
    type Item = Block;

    fn next(&mut self) -> Option<Block> {
        if let Some(hash) = self.current_block_hash.take() {
            let current_block = self.chain_store.get_block(&hash).unwrap();
            // Make sure that the block is within the from..=to range
            if current_block.header().height() >= self.from_block_height {
                // Set the previous block as "current" one, as long as the current one isn't the genesis block
                if current_block.header().height() != self.chain_store.get_genesis_height() {
                    self.current_block_hash = Some(current_block.header().prev_hash().clone());
                }

                return Some(current_block);
            }
        }

        None
    }
}

#[derive(Clone, Debug, Default)]
struct GasUsageInShard {
    pub used_gas_per_account: BTreeMap<AccountId, Gas>,
    pub used_gas_total: Gas,
}

#[derive(Debug, Clone)]
struct ShardSplit {
    /// Account on which the shard would be split
    pub split_account: AccountId,
    /// Gas used by accounts < split_account
    pub gas_left: Gas,
    /// Gas used by accounts >= split_account
    pub gas_right: Gas,
}

impl GasUsageInShard {
    pub fn new() -> GasUsageInShard {
        GasUsageInShard { used_gas_per_account: BTreeMap::new(), used_gas_total: 0 }
    }

    pub fn add_used_gas(&mut self, account: AccountId, used_gas: Gas) {
        let old_used_gas: &Gas = self.used_gas_per_account.get(&account).unwrap_or(&0);
        let new_used_gas: Gas = old_used_gas.checked_add(used_gas).unwrap();
        self.used_gas_per_account.insert(account, new_used_gas);

        self.used_gas_total = self.used_gas_total.checked_add(used_gas).unwrap();
    }

    pub fn merge(&mut self, other: &GasUsageInShard) {
        for (account_id, used_gas) in &other.used_gas_per_account {
            let new_gas = self
                .used_gas_per_account
                .get(account_id)
                .unwrap_or(&0)
                .checked_add(*used_gas)
                .unwrap();
            self.used_gas_per_account.insert(account_id.clone(), new_gas);
        }
        self.used_gas_total = self.used_gas_total.checked_add(other.used_gas_total).unwrap();
    }

    /// Calculate the optimal point at which this shard could be split into two halves with equal gas usage
    pub fn calculate_split(&self) -> Option<ShardSplit> {
        let mut split_account = match self.used_gas_per_account.keys().next() {
            Some(account_id) => account_id,
            None => return None,
        };

        if self.used_gas_per_account.len() < 2 {
            return None;
        }

        let mut gas_left: Gas = 0;
        let mut gas_right: Gas = self.used_gas_total;

        for (account, used_gas) in self.used_gas_per_account.iter() {
            if gas_left >= gas_right {
                break;
            }

            split_account = &account;
            gas_left = gas_left.checked_add(*used_gas).unwrap();
            gas_right = gas_right.checked_sub(*used_gas).unwrap();
        }

        Some(ShardSplit { split_account: split_account.clone(), gas_left, gas_right })
    }
}

#[derive(Clone, Debug)]
struct GasUsageStats {
    pub shards: BTreeMap<ShardUId, GasUsageInShard>,
}

impl GasUsageStats {
    pub fn new() -> GasUsageStats {
        GasUsageStats { shards: BTreeMap::new() }
    }

    pub fn add_gas_usage_in_shard(&mut self, shard_uid: ShardUId, shard_usage: GasUsageInShard) {
        match self.shards.get_mut(&shard_uid) {
            Some(existing_shard_usage) => existing_shard_usage.merge(&shard_usage),
            None => {
                let _ = self.shards.insert(shard_uid, shard_usage);
            }
        }
    }

    pub fn used_gas_total(&self) -> Gas {
        let mut result: Gas = 0;
        for shard_usage in self.shards.values() {
            result = result.checked_add(shard_usage.used_gas_total).unwrap();
        }
        result
    }

    pub fn merge(&mut self, other: GasUsageStats) {
        for (shard_uid, shard_usage) in other.shards {
            self.add_gas_usage_in_shard(shard_uid, shard_usage);
        }
    }
}

fn get_gas_usage_in_block(
    block: &Block,
    chain_store: &ChainStore,
    epoch_manager: &EpochManager,
) -> GasUsageStats {
    let block_info: Arc<BlockInfo> = epoch_manager.get_block_info(block.hash()).unwrap();
    let epoch_id: &EpochId = block_info.epoch_id();
    let shard_layout: ShardLayout = epoch_manager.get_shard_layout(epoch_id).unwrap();

    let mut result = GasUsageStats::new();

    // Go over every chunk in this block and gather data
    for chunk_header in block.chunks().iter() {
        let shard_id: ShardId = chunk_header.shard_id();
        let shard_uid: ShardUId = ShardUId::from_shard_id_and_layout(shard_id, &shard_layout);

        let mut gas_usage_in_shard = GasUsageInShard::new();

        // The outcome of each transaction and receipt executed in this chunk is saved in the database as an ExecutionOutcome.
        // Go through all ExecutionOutcomes from this chunk and record the gas usage.
        let outcome_ids: Vec<CryptoHash> =
            chain_store.get_outcomes_by_block_hash_and_shard_id(block.hash(), shard_id).unwrap();
        for outcome_id in outcome_ids {
            let outcome: ExecutionOutcome = chain_store
                .get_outcome_by_id_and_block_hash(&outcome_id, block.hash())
                .unwrap()
                .unwrap()
                .outcome;

            // Sanity check - make sure that the executor of this outcome belongs to this shard
            let account_shard = account_id_to_shard_id(&outcome.executor_id, &shard_layout);
            assert_eq!(account_shard, shard_id);

            gas_usage_in_shard.add_used_gas(outcome.executor_id, outcome.gas_burnt);
        }

        result.add_gas_usage_in_shard(shard_uid, gas_usage_in_shard);
    }

    result
}

/// A struct that can be used to find N biggest accounts by gas usage in an efficient manner.
struct BiggestAccountsFinder {
    accounts: BTreeSet<(Gas, AccountId)>,
    accounts_num: usize,
}

impl BiggestAccountsFinder {
    pub fn new(accounts_num: usize) -> BiggestAccountsFinder {
        BiggestAccountsFinder { accounts: BTreeSet::new(), accounts_num }
    }

    pub fn add_account_stats(&mut self, account: AccountId, used_gas: Gas) {
        self.accounts.insert((used_gas, account));

        // If there are more accounts than desired, remove the one with the smallest gas usage
        if self.accounts.len() > self.accounts_num {
            self.accounts.pop_first();
        }
    }

    pub fn get_biggest_accounts(&self) -> impl Iterator<Item = (AccountId, Gas)> + '_ {
        self.accounts.iter().rev().map(|(gas, account)| (account.clone(), *gas))
    }
}

fn analyse_gas_usage(
    blocks_iter: impl Iterator<Item = Block>,
    chain_store: &ChainStore,
    epoch_manager: &EpochManager,
) {
    // Gather statistics about gas usage in all of the blocks
    let mut blocks_count: usize = 0;
    let mut first_analysed_block: Option<(BlockHeight, CryptoHash)> = None;
    let mut last_analysed_block: Option<(BlockHeight, CryptoHash)> = None;

    let mut gas_usage_stats = GasUsageStats::new();

    for block in blocks_iter {
        blocks_count += 1;
        if first_analysed_block.is_none() {
            first_analysed_block = Some((block.header().height(), block.hash().clone()));
        }
        last_analysed_block = Some((block.header().height(), block.hash().clone()));

        let gas_usage_in_block: GasUsageStats =
            get_gas_usage_in_block(&block, chain_store, epoch_manager);
        gas_usage_stats.merge(gas_usage_in_block);
    }

    // Calculates how much percent of `big` is `small` and returns it as a string.
    // Example: as_percentage_of(10, 100) == "10.0%"
    let as_percentage_of = |small: Gas, big: Gas| {
        if big > 0 {
            format!("{:.1}%", small as f64 / big as f64 * 100.0)
        } else {
            format!("-")
        }
    };

    // Print out the analysis
    if blocks_count == 0 {
        println!("No blocks to analyse!");
        return;
    }
    println!("");
    println!("Analysed {} blocks between:", blocks_count);
    if let Some((block_height, block_hash)) = first_analysed_block {
        println!("Block: height = {block_height}, hash = {block_hash}");
    }
    if let Some((block_height, block_hash)) = last_analysed_block {
        println!("Block: height = {block_height}, hash = {block_hash}");
    }
    let total_gas: Gas = gas_usage_stats.used_gas_total();
    println!("");
    println!("Total gas used: {}", total_gas);
    println!("");
    for (shard_uid, shard_usage) in &gas_usage_stats.shards {
        println!("Shard: {}", shard_uid);
        println!(
            "  Gas usage: {} ({} of total)",
            shard_usage.used_gas_total,
            as_percentage_of(shard_usage.used_gas_total, total_gas)
        );
        println!("  Number of accounts: {}", shard_usage.used_gas_per_account.len());
        match shard_usage.calculate_split() {
            Some(shard_split) => {
                println!("  Optimal split:");
                println!("    split_account: {}", shard_split.split_account);
                println!(
                    "    gas(account < split_account): {} ({} of shard)",
                    shard_split.gas_left,
                    as_percentage_of(shard_split.gas_left, shard_usage.used_gas_total)
                );
                println!(
                    "    gas(account >= split_account): {} ({} of shard)",
                    shard_split.gas_right,
                    as_percentage_of(shard_split.gas_right, shard_usage.used_gas_total)
                );
            }
            None => println!("  No optimal split for this shard"),
        }
        println!("");
    }

    // Find 10 biggest accounts by gas usage
    let mut biggest_accounts_finder = BiggestAccountsFinder::new(10);
    for shard in gas_usage_stats.shards.values() {
        for (account, used_gas) in &shard.used_gas_per_account {
            biggest_accounts_finder.add_account_stats(account.clone(), *used_gas);
        }
    }
    println!("10 biggest accounts by gas usage:");
    for (i, (account, gas_usage)) in biggest_accounts_finder.get_biggest_accounts().enumerate() {
        println!("#{}: {}", i + 1, account);
        println!(
            "    Used gas: {} ({} of total)",
            gas_usage,
            as_percentage_of(gas_usage, total_gas)
        )
    }
}

#![allow(unused)]

use std::{
    collections::{BTreeMap, HashMap},
    path::PathBuf,
    sync::Arc,
};

use clap::Parser;
use near_chain::{Block, ChainStore, ChainStoreAccess, Error};
use near_epoch_manager::EpochManager;
use near_primitives::{
    account, block,
    epoch_manager::block_info::BlockInfo,
    hash::CryptoHash,
    receipt::ReceiptEnum,
    shard_layout::ShardLayout,
    sharding::{ReceiptProof, ShardChunk},
    transaction::ExecutionOutcome,
    types::{chunk_extra::ChunkExtra, AccountId, BlockHeight, EpochId, Gas, ShardId},
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

fn analyse_gas_usage(
    blocks_iter: impl Iterator<Item = Block>,
    chain_store: &ChainStore,
    epoch_manager: &EpochManager,
) {
    let mut shard_usages: BTreeMap<ShardUId, GasUsageInShard> = BTreeMap::new();
    let mut blocks_count: usize = 0;
    let mut first_analysed_block: Option<(BlockHeight, CryptoHash)> = None;
    let mut last_analysed_block: Option<(BlockHeight, CryptoHash)> = None;

    for block in blocks_iter {
        blocks_count += 1;
        if first_analysed_block.is_none() {
            first_analysed_block = Some((block.header().height(), block.hash().clone()));
        }
        last_analysed_block = Some((block.header().height(), block.hash().clone()));

        let gas_usage_in_block: GasUsageInBlock =
            analyse_gas_usage_in_block(&block, chain_store, epoch_manager);

        for (shard_uid, gas_usage) in gas_usage_in_block.shards {
            match shard_usages.get_mut(&shard_uid) {
                Some(shard_usage) => shard_usage.merge(&gas_usage),
                None => _ = shard_usages.insert(shard_uid, gas_usage),
            };
        }
    }

    if blocks_count == 0 {
        println!("No blocks to analyse!");
        return;
    }

    let mut all_shard_gas_usage: Gas = shard_usages
        .values()
        .fold(0, |sum, shard_usage| sum.checked_add(shard_usage.used_gas_total).unwrap());
    if all_shard_gas_usage == 0 {
        // Avoid dividing by 0
        all_shard_gas_usage = 1;
    }

    println!("");
    println!("Analysed {} blocks between:", blocks_count);
    if let Some((block_height, block_hash)) = first_analysed_block {
        println!("Block: height = {block_height}, hash = {block_hash}");
    }
    if let Some((block_height, block_hash)) = last_analysed_block {
        println!("Block: height = {block_height}, hash = {block_hash}");
    }
    println!("");
    for (shard_uid, shard_usage) in shard_usages {
        println!("Shard: {}", shard_uid);
        println!(
            "  Total gas usage: {} ({:.1}%)",
            shard_usage.used_gas_total,
            shard_usage.used_gas_total as f64 / all_shard_gas_usage as f64 * 100.0
        );
        println!("  Number of accounts: {}", shard_usage.used_gas_per_account.len());

        match shard_usage.calculate_split() {
            Some(shard_split) => {
                let shard_total_nonzero = std::cmp::max(1, shard_usage.used_gas_total) as f64;
                println!("  Optimal split:");
                println!("    split_account: {}", shard_split.split_account);
                println!(
                    "    gas(account < split_account): {} ({:.1}%)",
                    shard_split.gas_left,
                    shard_split.gas_left as f64 / shard_total_nonzero * 100.0
                );
                println!(
                    "    gas(account >= split_account): {} ({:.1}%)",
                    shard_split.gas_right,
                    shard_split.gas_right as f64 / shard_total_nonzero * 100.0
                );
            }
            None => println!("  No optimal split for this shard"),
        }
        println!("");
    }
}

#[derive(Clone, Debug, Default)]
struct GasUsageInBlock {
    pub shards: BTreeMap<ShardUId, GasUsageInShard>,
}

#[derive(Clone, Debug, Default)]
struct GasUsageInShard {
    pub used_gas_per_account: BTreeMap<AccountId, Gas>,
    pub used_gas_total: Gas,
}

struct ShardSplit {
    /// Account on which the shard would be split
    pub split_account: AccountId,
    /// Gas used by accounts < split_account
    pub gas_left: Gas,
    /// Gas used by accounts >= split_account
    pub gas_right: Gas,
}

impl GasUsageInShard {
    pub fn merge(&mut self, other: &GasUsageInShard) {
        self.used_gas_total = self.used_gas_total.checked_add(other.used_gas_total).unwrap();

        for (account_id, used_gas) in &other.used_gas_per_account {
            let new_gas = self
                .used_gas_per_account
                .get(account_id)
                .unwrap_or(&0)
                .checked_add(*used_gas)
                .unwrap();
            self.used_gas_per_account.insert(account_id.clone(), new_gas);
        }
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

fn analyse_gas_usage_in_block(
    block: &Block,
    chain_store: &ChainStore,
    epoch_manager: &EpochManager,
) -> GasUsageInBlock {
    //println!("Analysing block with height {}", block.header().height());

    let block_info: Arc<BlockInfo> = epoch_manager.get_block_info(block.hash()).unwrap();
    let epoch_id: &EpochId = block_info.epoch_id();
    let shard_layout: ShardLayout = epoch_manager.get_shard_layout(epoch_id).unwrap();

    let mut result = GasUsageInBlock::default();

    for chunk_header in block.chunks().iter() {
        let shard_id: ShardId = chunk_header.shard_id();
        let shard_uid: ShardUId = ShardUId::from_shard_id_and_layout(shard_id, &shard_layout);
        let chunk: Arc<ShardChunk> = chain_store.get_chunk(&chunk_header.chunk_hash()).unwrap();
        let chunk_extra: Arc<ChunkExtra> =
            chain_store.get_chunk_extra(block.hash(), &shard_uid).unwrap();

        let mut used_gas_per_account: BTreeMap<AccountId, Gas> = BTreeMap::new();

        let mut record_gas_usage = |account: &AccountId, used_gas: Gas| {
            let new_used_gas: Gas =
                used_gas_per_account.get(account).unwrap_or(&0).checked_add(used_gas).unwrap();
            used_gas_per_account.insert(account.clone(), new_used_gas);
        };

        for transaction in chunk.transactions().iter().map(|signed_tx| &signed_tx.transaction) {
            let (tx_hash, _) = transaction.get_hash_and_size();

            // Find the outcome of this transaction.
            // There might be many outcomes from different forks, choose the one that matches this block's hash.
            let execution_outcome_opt: Option<ExecutionOutcome> = chain_store
                .get_outcomes_by_id(&tx_hash)
                .unwrap()
                .into_iter()
                //.filter(|outcome| &outcome.block_hash == block.hash()) // TODO: Why aren't there ExecutionOutcomes that match this block's hash?
                .next()
                .map(|o| o.outcome_with_id.outcome);

            if let Some(execution_outcome) = execution_outcome_opt {
                record_gas_usage(&transaction.signer_id, execution_outcome.gas_burnt);
            } else {
                println!("No execution outcome for transaction {}: {:#?}", tx_hash, transaction);
            }
        }

        let incoming_receipts = chain_store.get_incoming_receipts(block.hash(), shard_id).unwrap();
        for receipt_proof in incoming_receipts.iter() {
            for receipt in receipt_proof.0.iter() {
                let receipt_hash = receipt.get_hash();
                let execution_outcome_opt: Option<ExecutionOutcome> = chain_store
                    .get_outcomes_by_id(&receipt_hash)
                    .unwrap()
                    .into_iter()
                    //.filter(|outcome| &outcome.block_hash == block.hash())
                    .next()
                    .map(|o| o.outcome_with_id.outcome);

                if let Some(execution_outcome) = execution_outcome_opt {
                    record_gas_usage(&receipt.receiver_id, execution_outcome.gas_burnt);
                } else {
                    match receipt.receipt {
                        ReceiptEnum::Data(_) => {}
                        _ => println!(
                            "No execution outcome for receipt {}: {:#?}",
                            receipt_hash, receipt
                        ),
                    }
                }
            }
        }

        // Sum gas used by all accounts
        let account_gas_sum: Gas =
            used_gas_per_account.values().fold(0, |sum, gas| sum.checked_add(*gas).unwrap());
        // TODO: Why isn't it equal??
        //assert_eq!(account_gas_sum, chunk_extra.gas_used());

        // Insert this shard's gas usage data into the result
        if result.shards.contains_key(&shard_uid) {
            panic!("Block contains the chunk with shard_uid = {} twice!", shard_uid);
        }
        result.shards.insert(
            shard_uid,
            GasUsageInShard { used_gas_per_account, used_gas_total: account_gas_sum },
        );
    }

    result
}

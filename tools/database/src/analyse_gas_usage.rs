use std::{path::PathBuf, sync::Arc};

use clap::Parser;
use near_chain::{Block, ChainStore, ChainStoreAccess, Error};
use near_primitives::{hash::CryptoHash, types::BlockHeight};
use near_store::{NodeStorage, Store};
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
        let chain_store =
            Arc::new(ChainStore::new(store, near_config.genesis.config.genesis_height, false));

        let blocks_iterator = self.make_block_iterator(chain_store.clone());

        analyse_gas_usage(blocks_iterator, &chain_store);
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

fn analyse_gas_usage(blocks_iter: impl Iterator<Item = Block>, _chain_store: &ChainStore) {
    for block in blocks_iter {
        println!("Analysing block with height {}", block.header().height());
        // TODO
    }
}

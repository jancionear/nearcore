use std::collections::BTreeMap;
use std::path::PathBuf;
use std::rc::Rc;

use bytesize::ByteSize;
use clap::Parser;
use near_chain::{Block, ChainStore, ChainStoreAccess};
use near_chain_configs::GenesisValidationMode;
use near_primitives::action::Action;
use near_primitives::hash::CryptoHash;
use near_primitives::types::AccountId;
use near_primitives::types::BlockHeight;
use nearcore::{load_config, open_storage};

use crate::block_iterators::{
    make_block_iterator_from_command_args, CommandArgs, LastNBlocksIterator,
};

#[derive(Parser)]
pub(crate) struct AnalyzeTransactionSizesCommand {
    /// Analyse the last N blocks in the blockchain
    #[arg(long)]
    last_blocks: Option<u64>,

    /// Analyse blocks from the given block height, inclusive
    #[arg(long)]
    from_block_height: Option<BlockHeight>,

    /// Analyse blocks up to the given block height, inclusive
    #[arg(long)]
    to_block_height: Option<BlockHeight>,

    /// Show top N transactions by size.
    #[arg(long, default_value_t = 50)]
    topn: usize,
}

impl AnalyzeTransactionSizesCommand {
    pub(crate) fn run(&self, home: &PathBuf) -> anyhow::Result<()> {
        // Create a ChainStore and EpochManager that will be used to read blockchain data.
        let mut near_config = load_config(home, GenesisValidationMode::Full).unwrap();
        let node_storage = open_storage(&home, &mut near_config).unwrap();
        let store = node_storage.get_split_store().unwrap_or_else(|| node_storage.get_hot_store());
        let chain_store =
            Rc::new(ChainStore::new(store, near_config.genesis.config.genesis_height, false));

        // Create an iterator over the blocks that should be analysed
        let blocks_iter_opt = make_block_iterator_from_command_args(
            CommandArgs {
                last_blocks: self.last_blocks,
                from_block_height: self.from_block_height,
                to_block_height: self.to_block_height,
            },
            chain_store.clone(),
        );

        let blocks_iter = match blocks_iter_opt {
            Some(iter) => iter,
            None => {
                println!("No arguments, defaulting to last 100 blocks");
                Box::new(LastNBlocksIterator::new(100, chain_store.clone()))
            }
        };

        analyze_transaction_sizes(blocks_iter, &chain_store);

        Ok(())
    }
}

// Things I care about
// 1) Transaction size (total)
// 2) DeployContract transactions - contract size
// 3) FunctionCall transactions - arguments size

struct TransactionInfo {
    /// An account on which behalf transaction is signed
    pub signer_id: AccountId,
    /// Receiver account for this transaction
    pub receiver_id: AccountId,
    /// The hash of the block in the blockchain on top of which the given transaction is valid
    pub tx_hash: CryptoHash,

    typ: TransactionType,
}

enum TransactionType {
    DeployContract(ByteSize),
    FunctionCall(String, ByteSize),
    Other,
}

fn analyze_transaction_sizes(blocks_iter: impl Iterator<Item = Block>, chain_store: &ChainStore) {
    let mut largest_transactions: BTreeMap<ByteSize, TransactionInfo> = BTreeMap::new();

    for (i, block) in blocks_iter.enumerate() {
        if i > 0 && i % 1000 == 0 {
            println!("Processed {} blocks...", i);
        }
        for chunk_header in block.chunks().iter() {
            let chunk = chain_store.get_chunk(&chunk_header.chunk_hash()).unwrap();
            for transaction in chunk.transactions() {
                let transaction_size = borsh::to_vec(transaction).unwrap().len();

                let transaction_type = match &transaction.transaction.actions.as_slice() {
                    &[Action::FunctionCall(fc)] => TransactionType::FunctionCall(
                        fc.method_name.clone(),
                        ByteSize::b(fc.args.len() as u64),
                    ),
                    &[Action::DeployContract(dc)] => {
                        TransactionType::DeployContract(ByteSize::b(dc.code.len() as u64))
                    }
                    _ => TransactionType::Other,
                };

                let transaction_info = TransactionInfo {
                    signer_id: transaction.transaction.signer_id.clone(),
                    receiver_id: transaction.transaction.receiver_id.clone(),
                    tx_hash: transaction.get_hash(),
                    typ: transaction_type,
                };

                largest_transactions.insert(ByteSize::b(transaction_size as u64), transaction_info);
                if largest_transactions.len() > 50 {
                    largest_transactions.pop_first();
                }
            }
        }
    }

    println!("");
    println!("Top {} transactions by size:", largest_transactions.len());
    for (size, info) in largest_transactions.iter().rev() {
        print!("{}: {}->{} ", size, info.signer_id, info.receiver_id);

        match &info.typ {
            TransactionType::DeployContract(code_size) => {
                print!("DeployContract with size: {}", code_size);
            }
            TransactionType::FunctionCall(method_name, args_size) => {
                print!("FunctionCall call {} with args size: {}", method_name, args_size);
            }
            TransactionType::Other => {
                print!("Other");
            }
        }

        println!(" {:?}", info.tx_hash);
    }
}

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::mpsc::Receiver;
use std::sync::mpsc::SyncSender;
use std::sync::Arc;
use std::sync::Mutex;

use bytesize::ByteSize;
use clap::Parser;
use near_chain::{Block, ChainStore, ChainStoreAccess};
use near_chain_configs::GenesisValidationMode;
use near_primitives::action::Action;
use near_primitives::hash::CryptoHash;
use near_primitives::types::AccountId;
use near_primitives::types::BlockHeight;
use near_store::Mode;
use near_store::NodeStorage;
use nearcore::load_config;

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
        analyze_transaction_sizes(
            home.clone(),
            CommandArgs {
                last_blocks: self.last_blocks,
                from_block_height: self.from_block_height,
                to_block_height: self.to_block_height,
            },
            self.topn,
        );

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

type Biggest = BTreeMap<ByteSize, TransactionInfo>;

struct AnalyzeJob {
    blocks: Vec<Block>,
}

struct AnalyzeJobResult {
    biggest: Biggest,
    blocks_processed: usize,
}

impl AnalyzeJob {
    fn run(&self, chain_store: &ChainStore) -> AnalyzeJobResult {
        let mut largest_transactions: Biggest = Biggest::new();

        for block in self.blocks.iter() {
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

                    largest_transactions
                        .insert(ByteSize::b(transaction_size as u64), transaction_info);
                    if largest_transactions.len() > 50 {
                        largest_transactions.pop_first();
                    }
                }
            }
        }

        AnalyzeJobResult { biggest: largest_transactions, blocks_processed: self.blocks.len() }
    }
}

fn get_chain_store(home: PathBuf) -> ChainStore {
    // Create a ChainStore and EpochManager that will be used to read blockchain data.
    let near_config = load_config(&home, GenesisValidationMode::Full).unwrap();
    let node_storage = NodeStorage::opener(
        &home,
        near_config.client_config.archive,
        &near_config.config.store,
        near_config.config.cold_store.as_ref(),
    )
    .open_in_mode(Mode::ReadOnly)
    .unwrap();
    let store = node_storage.get_split_store().unwrap_or_else(|| node_storage.get_hot_store());
    ChainStore::new(store, near_config.genesis.config.genesis_height, false)
}

fn run_job_thread(
    home: PathBuf,
    jobs_chan: Arc<Mutex<Receiver<AnalyzeJob>>>,
    result_sender: SyncSender<AnalyzeJobResult>,
) {
    let chain_store = get_chain_store(home);

    while let Ok(job) = jobs_chan.lock().unwrap().recv() {
        let result = job.run(&chain_store);
        result_sender.send(result).unwrap();
    }
}

fn generate_jobs_thread(
    home: PathBuf,
    command_args: CommandArgs,
    jobs_chan: SyncSender<AnalyzeJob>,
) {
    let chain_store = Rc::new(get_chain_store(home));

    // Create an iterator over the blocks that should be analysed
    let blocks_iter_opt = make_block_iterator_from_command_args(command_args, chain_store.clone());

    let blocks_iter = match blocks_iter_opt {
        Some(iter) => iter,
        None => {
            println!("No arguments, defaulting to last 100 blocks");
            Box::new(LastNBlocksIterator::new(100, chain_store))
        }
    };

    let mut cur_blocks: Vec<Block> = Vec::new();
    for block in blocks_iter {
        cur_blocks.push(block);
        if cur_blocks.len() == 100 {
            let job = AnalyzeJob { blocks: cur_blocks };
            cur_blocks = Vec::new();
            jobs_chan.send(job).unwrap();
        }
    }
    let job = AnalyzeJob { blocks: cur_blocks };
    jobs_chan.send(job).unwrap();
}

fn merge_biggest(a: Biggest, b: Biggest, topn: usize) -> Biggest {
    let mut result = a;
    for (size, info) in b {
        result.insert(size, info);
        if result.len() > topn {
            result.pop_first();
        }
    }
    result
}

fn analyze_transaction_sizes(home: PathBuf, command_args: CommandArgs, topn: usize) {
    let threads_num = 5;
    let (jobs_sender, jobs_receiver) = std::sync::mpsc::sync_channel(16);
    let (result_sender, result_receiver) = std::sync::mpsc::sync_channel(16);

    let jobs_chan = Arc::new(Mutex::new(jobs_receiver));
    let mut threads = Vec::new();
    for _ in 0..threads_num {
        let jobs_chan = jobs_chan.clone();
        let result_sender = result_sender.clone();
        let home = home.clone();
        threads.push(std::thread::spawn(move || run_job_thread(home, jobs_chan, result_sender)));
    }
    threads.push(std::thread::spawn(move || {
        generate_jobs_thread(home, command_args, jobs_sender);
    }));
    std::mem::drop(result_sender);

    let mut largest_transactions: Biggest = Biggest::new();
    let mut blocks_processed = 0;
    while let Ok(result) = result_receiver.recv() {
        blocks_processed += result.blocks_processed;
        largest_transactions = merge_biggest(largest_transactions, result.biggest, topn);

        if blocks_processed % 1000 == 0 {
            println!("Processed {} blocks", blocks_processed);
        }
    }

    for thread in threads {
        thread.join().unwrap();
    }

    println!("Done!");
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

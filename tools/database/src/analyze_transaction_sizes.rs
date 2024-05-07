use std::collections::BTreeMap;
use std::ops::Range;
use std::path::PathBuf;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::mpsc::SyncSender;
use std::sync::Arc;

use bytesize::ByteSize;
use clap::Parser;
use near_chain::{ChainStore, ChainStoreAccess};
use near_chain_configs::GenesisValidationMode;
use near_primitives::action::Action;
use near_primitives::hash::CryptoHash;
use near_primitives::types::AccountId;
use near_primitives::types::BlockHeight;
use near_store::Mode;
use near_store::Store;
use nearcore::load_config;
use nearcore::open_storage_in_mode;
use nearcore::NearConfig;

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

    /// Use this many threads to analyze the blocks
    #[arg(long, default_value_t = 64)]
    threads: usize,
}

impl AnalyzeTransactionSizesCommand {
    pub(crate) fn run(&self, home: &PathBuf) -> anyhow::Result<()> {
        let mut near_config = load_config(&home, GenesisValidationMode::Full).unwrap();
        let node_storage = open_storage_in_mode(&home, &mut near_config, Mode::ReadOnly).unwrap();
        let store = node_storage.get_split_store().unwrap_or_else(|| node_storage.get_hot_store());

        let height_range = if let Some(last_blocks) = self.last_blocks {
            let chain_store =
                ChainStore::new(store.clone(), near_config.genesis.config.genesis_height, false);
            let head = chain_store.head()?;
            head.height.saturating_sub(last_blocks)..head.height
        } else {
            self.from_block_height.unwrap_or(0)..self.to_block_height.unwrap_or(u64::MAX)
        };

        println!("Height range: {:?}", height_range);
        analyze_transaction_sizes(store, near_config, height_range, self.topn, self.threads);

        Ok(())
    }
}

fn analyze_transaction_sizes(
    store: Store,
    near_config: NearConfig,
    height_range: Range<BlockHeight>,
    topn: usize,
    threads: usize,
) {
    let largest_transactions = analyze_chain(
        store,
        near_config,
        height_range,
        move |height, chain_store, res| anal_block(height, chain_store, res, topn),
        move |a, b| merge_biggest(a, b, topn),
        threads,
    );

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

fn anal_block(
    height: BlockHeight,
    chain_store: &ChainStore,
    largest_transactions: &mut Biggest,
    topn: usize,
) {
    let block_res = chain_store
        .get_block_hash_by_height(height)
        .map(|block_hash| chain_store.get_block(&block_hash));
    let block = match block_res {
        Ok(Ok(block)) => block,
        _ => {
            //println!("Failed to get block at height {}", height);
            return;
        }
    };

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
            if largest_transactions.len() > topn {
                largest_transactions.pop_first();
            }
        }
    }
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

fn analyze_chain<Res, BlockFun, MergeFun>(
    store: Store,
    near_config: NearConfig,
    height_range: Range<BlockHeight>,
    analyze_block: BlockFun,
    mut merge_results: MergeFun,
    num_threads: usize,
) -> Res
where
    BlockFun: FnMut(BlockHeight, &ChainStore, &mut Res) + Clone + Send + 'static,
    MergeFun: FnMut(Res, Res) -> Res + Clone + Send + 'static,
    Res: Send + Default + 'static,
{
    let next_to_process = Arc::new(AtomicU64::new(height_range.start));
    let (update_sender, update_receiver) = std::sync::mpsc::sync_channel(num_threads * 4);
    let mut threads = Vec::new();
    for _ in 0..num_threads {
        let analyze_block = analyze_block.clone();
        let store = store.clone();
        let near_config = near_config.clone();
        let next_to_process = next_to_process.clone();
        let update_sender = update_sender.clone();
        let height_range = height_range.clone();
        threads.push(std::thread::spawn(move || {
            analyze_chain_thread(
                analyze_block,
                store,
                near_config,
                next_to_process,
                height_range,
                update_sender,
            )
        }));
    }
    std::mem::drop(update_sender);

    let mut total_processed = 0;
    let start_time = std::time::Instant::now();
    while let Ok(update) = update_receiver.recv() {
        total_processed += update;
        if total_processed % 1000 == 0 {
            let rate = total_processed as f64 / start_time.elapsed().as_secs_f64();
            let total = height_range.end - height_range.start;
            let left_to_process = total - total_processed;
            let eta = std::time::Duration::from_secs((left_to_process as f64 / rate) as u64);
            println!(
                "Processed {} blocks ({:.2} blocks/s) ({:.2}%) ETA: {:?}",
                total_processed,
                rate,
                total_processed as f64 / total as f64 * 100.0,
                eta
            );
        }
    }

    let mut res = Res::default();
    for thread in threads {
        res = merge_results(res, thread.join().unwrap());
    }

    res
}

fn analyze_chain_thread<Res, BlockFun>(
    mut analyze_block: BlockFun,
    store: Store,
    near_config: NearConfig,
    next_to_process: Arc<AtomicU64>,
    height_range: Range<BlockHeight>,
    update_sender: SyncSender<u64>,
) -> Res
where
    BlockFun: FnMut(BlockHeight, &ChainStore, &mut Res),
    Res: Default,
{
    let mut res = Res::default();

    let chain_store = ChainStore::new(store, near_config.genesis.config.genesis_height, false);

    let batch_size = 200;
    loop {
        let start = next_to_process.fetch_add(batch_size, Ordering::Relaxed);
        for height in start..(start + batch_size) {
            if height > height_range.end {
                return res;
            }

            analyze_block(height, &chain_store, &mut res);
        }

        update_sender.send(batch_size).unwrap();
    }
}

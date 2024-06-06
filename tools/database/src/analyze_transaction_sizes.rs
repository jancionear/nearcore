use std::ops::Range;
use std::path::PathBuf;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::mpsc::SyncSender;
use std::sync::Arc;

use clap::Parser;
use near_chain::{ChainStore, ChainStoreAccess};
use near_chain_configs::GenesisValidationMode;
use near_primitives::action::Action;
use near_primitives::hash::CryptoHash;
use near_primitives::receipt::ReceiptEnum;
use near_primitives::types::AccountId;
use near_primitives::types::BlockHeight;
use near_store::Mode;
use near_store::Store;
use nearcore::load_config;
use nearcore::open_storage_in_mode;
use nearcore::NearConfig;
use serde::{Deserialize, Serialize};

const SIZE_LIMIT: usize = 100_000;

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
        analyze_transaction_sizes(store, near_config, height_range, self.threads);

        Ok(())
    }
}

fn analyze_transaction_sizes(
    store: Store,
    near_config: NearConfig,
    height_range: Range<BlockHeight>,
    threads: usize,
) {
    let largest_transactions = analyze_chain(
        store,
        near_config,
        height_range,
        move |height, chain_store, res| anal_block(height, chain_store, res),
        merge_biggest,
        threads,
    );

    println!("Done!");
    println!("");
    println!("Found {} infos:", largest_transactions.len());
    println!("{}", serde_json::to_string_pretty(&largest_transactions).unwrap());
}

#[derive(Serialize, Deserialize)]
struct TransactionInfo {
    pub signer_id: AccountId,
    pub receiver_id: AccountId,
    pub tx_hash: CryptoHash,
    pub typ: TransactionType,
    pub size: usize,
}

#[derive(Serialize, Deserialize)]
enum TransactionType {
    DeployContract(usize),
    FunctionCall(String, usize),
    Other,
}

#[derive(Serialize, Deserialize)]
struct ReceiptInfo {
    pub predecessor_id: AccountId,
    pub receiver_id: AccountId,
    pub receipt_id: CryptoHash,
    pub size: usize,
    pub typ: ReceiptType,
}

#[derive(Serialize, Deserialize)]
enum ReceiptType {
    DeployContract(usize),
    FunctionCall(String, usize),
    Data,
    PromiseYield,
    PromiseResume,
    Other,
}

#[derive(Serialize, Deserialize)]
enum Info {
    Receipt(ReceiptInfo),
    Transaction(TransactionInfo),
}

type Biggest = Vec<Info>;

fn anal_block(height: BlockHeight, chain_store: &ChainStore, largest: &mut Biggest) {
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

            if transaction_size < SIZE_LIMIT {
                continue;
            }

            let transaction_type = match &transaction.transaction.actions.as_slice() {
                &[Action::FunctionCall(fc)] => {
                    TransactionType::FunctionCall(fc.method_name.clone(), fc.args.len())
                }
                &[Action::DeployContract(dc)] => TransactionType::DeployContract(dc.code.len()),
                _ => TransactionType::Other,
            };

            let transaction_info = TransactionInfo {
                signer_id: transaction.transaction.signer_id.clone(),
                receiver_id: transaction.transaction.receiver_id.clone(),
                tx_hash: transaction.get_hash(),
                typ: transaction_type,
                size: transaction_size,
            };

            largest.push(Info::Transaction(transaction_info));
        }

        for receipt in chunk.prev_outgoing_receipts() {
            let receipt_size = borsh::to_vec(receipt).unwrap().len();

            if receipt_size < SIZE_LIMIT {
                continue;
            }

            let receipt_type = match &receipt.receipt {
                ReceiptEnum::Action(action_receipt) => match &action_receipt.actions.as_slice() {
                    &[Action::FunctionCall(fc)] => {
                        ReceiptType::FunctionCall(fc.method_name.clone(), fc.args.len())
                    }
                    &[Action::DeployContract(dc)] => ReceiptType::DeployContract(dc.code.len()),
                    _ => ReceiptType::Other,
                },
                ReceiptEnum::Data(_) => ReceiptType::Data,
                ReceiptEnum::PromiseYield(_) => ReceiptType::PromiseYield,
                ReceiptEnum::PromiseResume(_) => ReceiptType::PromiseResume,
            };

            let receipt_info = ReceiptInfo {
                predecessor_id: receipt.predecessor_id.clone(),
                receiver_id: receipt.receiver_id.clone(),
                receipt_id: receipt.receipt_id,
                size: receipt_size,
                typ: receipt_type,
            };

            largest.push(Info::Receipt(receipt_info));
        }
    }
}

fn merge_biggest(mut a: Biggest, b: Biggest) -> Biggest {
    a.extend(b.into_iter());
    a
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

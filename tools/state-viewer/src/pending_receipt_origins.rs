//! Checks that no receipt which is still pending at a given block was created
//! in (or originates from a transaction included in) a protocol version older
//! than a given one.
//!
//! Motivation: `RejectUniversalAccountTransfers` (protocol 87) bans new
//! transfers to universal (`0u`) accounts, and `UniversalAccounts` (protocol 88)
//! makes such transfers more expensive. A receipt created before the ban and
//! executed after universal accounts are enabled would be charged with the old,
//! cheaper fee but refunded with the new one. This command verifies, on a node
//! running protocol 87, that there are no receipts left from before the ban.
//!
//! The pending receipts are collected from:
//! - the state of every shard at the checked block: postponed receipts, the
//!   delayed receipts queue, promise yield receipts and the outgoing buffers,
//! - the receipts that are in flight between shards: outgoing receipts of the
//!   last applied chunk of every shard which weren't included in a chunk yet,
//!   and incoming receipts which haven't been applied by the receiving shard yet.
//!
//! For every pending receipt the `ReceiptToTx` column is used to walk its
//! ancestry up to the originating transaction. The block in which a receipt was
//! created is the block in which its parent (a receipt or a transaction) was
//! executed, which is found through the parent's execution outcome.

use anyhow::bail;
use borsh::BorshDeserialize;
use near_chain::types::RuntimeAdapter;
use near_chain::{ReceiptFilter, get_incoming_receipts_for_shard};
use near_epoch_manager::{EpochManager, EpochManagerAdapter};
use near_primitives::action::Action;
use near_primitives::action::delegate::{
    VersionedDelegateActionRef, VersionedSignedDelegateActionRef,
};
use near_primitives::hash::CryptoHash;
use near_primitives::receipt::{
    Receipt, ReceiptEnum, ReceiptOrStateStoredReceipt, ReceiptOrigin, ReceiptToTxInfo,
};
use near_primitives::trie_key::col;
use near_primitives::types::{AccountId, BlockHeight, EpochId, ProtocolVersion};
use near_primitives::utils::get_outcome_id_block_hash_rev;
use near_primitives::version::ProtocolFeature;
use near_primitives_core::account::id::AccountType;
use near_store::adapter::StoreAdapter;
use near_store::adapter::chain_store::ChainStoreAdapter;
use near_store::{DBCol, ShardTries, ShardUId, Store};
use nearcore::{NearConfig, NightshadeRuntime, NightshadeRuntimeExt};
use std::collections::{BTreeMap, HashMap};
use std::fmt::{self, Display, Formatter};
use std::path::Path;
use std::process::exit;
use std::sync::Arc;

/// Maximum number of hops followed when walking from a receipt to its transaction.
const MAX_ANCESTRY_DEPTH: usize = 1000;

#[derive(clap::Parser)]
pub struct CheckPendingReceiptOriginsCmd {
    /// Height of the block at which to check the pending receipts. Defaults to
    /// the final head.
    #[clap(long)]
    block_height: Option<BlockHeight>,
    /// Pending receipts must have been created in, and originate from a
    /// transaction included in, at least this protocol version. Defaults to the
    /// version of `RejectUniversalAccountTransfers`.
    #[clap(long)]
    min_protocol_version: Option<ProtocolVersion>,
    /// Maximum number of offending receipts to print in detail.
    #[clap(long, default_value = "100")]
    max_print: usize,
    /// Print every pending receipt with its origin, not only the offending ones.
    #[clap(long)]
    print_all: bool,
}

/// What to check, see `check_pending_receipt_origins`.
pub struct CheckOptions {
    /// The block at which to check the pending receipts. Defaults to the final head.
    pub block_hash: Option<CryptoHash>,
    pub min_protocol_version: ProtocolVersion,
    /// Maximum number of offending receipts to print in detail.
    pub max_print: usize,
    /// Print every pending receipt with its origin, not only the offending ones.
    pub print_all: bool,
}

/// The result of `check_pending_receipt_origins`.
#[derive(Debug, Default)]
pub struct CheckReport {
    /// All pending receipts found, with the places they were found in.
    pub pending: BTreeMap<CryptoHash, Vec<String>>,
    /// Ids of pending receipts with actions which fail the check, by category.
    pub violations: BTreeMap<&'static str, Vec<CryptoHash>>,
    /// Ids of pending receipts without actions which fail the check, by category.
    /// They can't buy gas, so they can't generate refunds.
    pub warnings: BTreeMap<&'static str, Vec<CryptoHash>>,
}

impl CheckReport {
    pub fn is_ok(&self) -> bool {
        self.violations.is_empty()
    }
}

/// A block in which something happened, with the protocol version of its epoch.
#[derive(Clone, Copy, Debug)]
struct BlockRef {
    height: BlockHeight,
    hash: CryptoHash,
    protocol_version: ProtocolVersion,
}

impl Display for BlockRef {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "block #{} {} (protocol {})", self.height, self.hash, self.protocol_version)
    }
}

struct OriginTx {
    tx_hash: CryptoHash,
    signer_id: AccountId,
    block: Result<BlockRef, String>,
}

/// Where a receipt comes from.
struct Provenance {
    /// The block in which the receipt was created.
    created_in: Result<BlockRef, String>,
    /// The transaction that started the chain of receipts.
    origin: Result<OriginTx, String>,
    /// Number of receipts between the pending receipt and the transaction.
    depth: usize,
}

/// A receipt which hasn't been executed yet, with all places it was found in.
struct PendingReceipt {
    receipt: Receipt,
    locations: Vec<String>,
}

impl PendingReceipt {
    /// Whether the receipt carries actions, the only receipts that buy gas and
    /// can generate refunds. Data receipts and global contract distribution
    /// receipts don't.
    fn has_actions(&self) -> bool {
        matches!(
            self.receipt.receipt(),
            ReceiptEnum::Action(_)
                | ReceiptEnum::ActionV2(_)
                | ReceiptEnum::PromiseYield(_)
                | ReceiptEnum::PromiseYieldV2(_)
        )
    }

    fn kind(&self) -> &'static str {
        match self.receipt.receipt() {
            ReceiptEnum::Action(_) => "Action",
            ReceiptEnum::ActionV2(_) => "ActionV2",
            ReceiptEnum::PromiseYield(_) => "PromiseYield",
            ReceiptEnum::PromiseYieldV2(_) => "PromiseYieldV2",
            ReceiptEnum::Data(_) => "Data",
            ReceiptEnum::PromiseResume(_) => "PromiseResume",
            ReceiptEnum::GlobalContractDistribution(_) => "GlobalContractDistribution",
        }
    }

    fn actions(&self) -> Vec<Action> {
        match self.receipt.receipt() {
            ReceiptEnum::Action(r) | ReceiptEnum::PromiseYield(r) => r.actions.clone(),
            ReceiptEnum::ActionV2(r) | ReceiptEnum::PromiseYieldV2(r) => r.actions.clone(),
            ReceiptEnum::Data(_)
            | ReceiptEnum::PromiseResume(_)
            | ReceiptEnum::GlobalContractDistribution(_) => vec![],
        }
    }

    /// Descriptions of all transfers to universal accounts in this receipt,
    /// including the ones nested in delegate actions.
    fn universal_account_transfers(&self) -> Vec<String> {
        let mut result = vec![];
        let receiver = self.receipt.receiver_id();
        for action in self.actions() {
            let delegate = match &action {
                Action::Transfer(transfer) => {
                    if is_universal(receiver) {
                        result.push(format!("transfer of {} to {receiver}", transfer.deposit));
                    }
                    continue;
                }
                Action::Delegate(a) => VersionedSignedDelegateActionRef::from(a.as_ref()),
                Action::DelegateV2(a) => VersionedSignedDelegateActionRef::from(a.as_ref()),
                _ => continue,
            };
            let delegate: VersionedDelegateActionRef<'_> = delegate.delegate_action();
            let inner_receiver = delegate.receiver_id();
            if !is_universal(inner_receiver) {
                continue;
            }
            for inner in delegate.get_actions() {
                if let Action::Transfer(transfer) = inner {
                    result.push(format!(
                        "delegated transfer of {} to {inner_receiver}",
                        transfer.deposit
                    ));
                }
            }
        }
        result
    }

    fn summary(&self) -> String {
        let actions = self.actions();
        let mut out = format!(
            "{} {} {} -> {}",
            self.receipt.receipt_id(),
            self.kind(),
            self.receipt.predecessor_id(),
            self.receipt.receiver_id(),
        );
        if !actions.is_empty() {
            let names: Vec<String> = actions.iter().map(action_name).collect();
            out.push_str(&format!(" [{}]", names.join(", ")));
        }
        out
    }
}

fn is_universal(account_id: &AccountId) -> bool {
    account_id.get_account_type() == AccountType::UniversalAccount
}

fn action_name(action: &Action) -> String {
    let debug = format!("{action:?}");
    let end = debug.find(|c: char| !c.is_alphanumeric()).unwrap_or(debug.len());
    debug[..end].to_string()
}

struct Checker {
    store: Store,
    adapter: ChainStoreAdapter,
    epoch_manager: Arc<dyn EpochManagerAdapter>,
    protocol_versions: HashMap<EpochId, ProtocolVersion>,
    /// Cache of the canonical block in which a transaction or receipt was executed.
    execution_blocks: HashMap<CryptoHash, Result<BlockRef, String>>,
}

impl Checker {
    fn block_ref(&mut self, block_hash: &CryptoHash) -> Result<BlockRef, String> {
        let header = self
            .adapter
            .get_block_header(block_hash)
            .map_err(|err| format!("missing block header {block_hash}: {err}"))?;
        let epoch_id = *header.epoch_id();
        let protocol_version = match self.protocol_versions.get(&epoch_id) {
            Some(version) => *version,
            None => {
                let version = self
                    .epoch_manager
                    .get_epoch_protocol_version(&epoch_id)
                    .map_err(|err| format!("missing epoch info for {epoch_id:?}: {err}"))?;
                self.protocol_versions.insert(epoch_id, version);
                version
            }
        };
        Ok(BlockRef { height: header.height(), hash: *block_hash, protocol_version })
    }

    fn is_canonical(&self, block_ref: &BlockRef) -> bool {
        self.adapter.get_block_hash_by_height(block_ref.height).ok() == Some(block_ref.hash)
    }

    /// The canonical block in which the transaction or receipt `id` was executed.
    fn execution_block(&mut self, id: &CryptoHash) -> Result<BlockRef, String> {
        if let Some(result) = self.execution_blocks.get(id) {
            return result.clone();
        }
        let result = self.find_execution_block(id);
        self.execution_blocks.insert(*id, result.clone());
        result
    }

    fn find_execution_block(&mut self, id: &CryptoHash) -> Result<BlockRef, String> {
        // Outcomes are keyed by the executed id followed by the block hash.
        let mut block_hashes = vec![];
        for (key, _) in self.store.iter_prefix(DBCol::TransactionResultForBlock, id.as_ref()) {
            let (_, block_hash) = get_outcome_id_block_hash_rev(&key)
                .map_err(|err| format!("bad outcome key for {id}: {err}"))?;
            block_hashes.push(block_hash);
        }
        if block_hashes.is_empty() {
            return Err(format!("no execution outcome of {id} (executed before the GC tail?)"));
        }
        let mut canonical = vec![];
        for block_hash in &block_hashes {
            let block_ref = self.block_ref(block_hash)?;
            if self.is_canonical(&block_ref) {
                canonical.push(block_ref);
            }
        }
        match canonical.as_slice() {
            [block_ref] => Ok(*block_ref),
            [] => Err(format!("none of the {} outcomes of {id} is canonical", block_hashes.len())),
            _ => Err(format!("{} canonical outcomes of {id}", canonical.len())),
        }
    }

    fn provenance(&mut self, receipt_id: &CryptoHash) -> Provenance {
        let mut created_in = None;
        let mut current = *receipt_id;
        for depth in 0..MAX_ANCESTRY_DEPTH {
            let Some(ReceiptToTxInfo::V1(info)) = self.adapter.get_receipt_to_tx(&current) else {
                let err = format!("no ReceiptToTx entry for receipt {current}");
                return Provenance {
                    created_in: created_in.unwrap_or_else(|| Err(err.clone())),
                    origin: Err(err),
                    depth,
                };
            };
            // A receipt is created in the block where its parent is executed.
            let (parent_id, tx_signer) = match info.origin {
                ReceiptOrigin::FromTransaction(origin) => {
                    (origin.tx_hash, Some(origin.sender_account_id))
                }
                ReceiptOrigin::FromReceipt(origin) => (origin.parent_receipt_id, None),
            };
            let parent_executed_in = self.execution_block(&parent_id);
            let created_in = created_in.get_or_insert_with(|| parent_executed_in.clone()).clone();
            if let Some(signer_id) = tx_signer {
                let origin = OriginTx { tx_hash: parent_id, signer_id, block: parent_executed_in };
                return Provenance { created_in, origin: Ok(origin), depth };
            }
            current = parent_id;
        }
        Provenance {
            created_in: created_in.unwrap(),
            origin: Err(format!("ancestry deeper than {MAX_ANCESTRY_DEPTH}")),
            depth: MAX_ANCESTRY_DEPTH,
        }
    }
}

fn add_pending(
    pending: &mut BTreeMap<CryptoHash, PendingReceipt>,
    receipt: Receipt,
    location: String,
) {
    pending
        .entry(*receipt.receipt_id())
        .or_insert_with(|| PendingReceipt { receipt, locations: vec![] })
        .locations
        .push(location);
}

/// Reads all receipts stored in the state of a shard.
fn collect_state_receipts(
    tries: &ShardTries,
    shard_uid: ShardUId,
    state_root: CryptoHash,
    pending: &mut BTreeMap<CryptoHash, PendingReceipt>,
) -> anyhow::Result<BTreeMap<&'static str, usize>> {
    let trie = tries.get_view_trie_for_shard(shard_uid, state_root);
    let mut counts = BTreeMap::new();
    for (column, name) in [
        (col::POSTPONED_RECEIPT, "postponed"),
        (col::DELAYED_RECEIPT_OR_INDICES, "delayed"),
        (col::PROMISE_YIELD_RECEIPT, "promise_yield"),
        (col::BUFFERED_RECEIPT, "buffered"),
    ] {
        let mut count = 0;
        let mut iter = trie.disk_iter()?;
        iter.seek_prefix([column])?;
        for item in iter {
            let (key, value) = item?;
            if key.first() != Some(&column) {
                break;
            }
            // The delayed receipt queue indices share the column with the queue items.
            if column == col::DELAYED_RECEIPT_OR_INDICES && key.len() == 1 {
                continue;
            }
            let receipt = match column {
                col::POSTPONED_RECEIPT | col::PROMISE_YIELD_RECEIPT => {
                    Receipt::try_from_slice(&value)?
                }
                _ => ReceiptOrStateStoredReceipt::try_from_slice(&value)?.into_receipt(),
            };
            add_pending(pending, receipt, format!("{name} in state of {shard_uid}"));
            count += 1;
        }
        counts.insert(name, count);
    }
    Ok(counts)
}

impl CheckPendingReceiptOriginsCmd {
    pub fn run(self, home_dir: &Path, near_config: NearConfig, store: Store) {
        let report = self.run_impl(home_dir, near_config, store).unwrap_or_else(|err| {
            eprintln!("check failed with an error: {err:#}");
            exit(2);
        });
        if !report.is_ok() {
            exit(1);
        }
    }

    fn run_impl(
        &self,
        home_dir: &Path,
        near_config: NearConfig,
        store: Store,
    ) -> anyhow::Result<CheckReport> {
        if !near_config.client_config.tracked_shards_config.tracks_all_shards() {
            bail!("the node must track all shards");
        }
        let epoch_manager = EpochManager::new_arc_handle(
            store.clone(),
            &near_config.genesis.config,
            Some(home_dir),
        );
        let runtime = NightshadeRuntime::from_config(
            home_dir,
            store.clone(),
            &near_config,
            epoch_manager.clone(),
        )?;
        let block_hash = match self.block_height {
            Some(height) => Some(store.chain_store().get_block_hash_by_height(height)?),
            None => None,
        };
        let options = CheckOptions {
            block_hash,
            min_protocol_version: self
                .min_protocol_version
                .unwrap_or(ProtocolFeature::RejectUniversalAccountTransfers.protocol_version()),
            max_print: self.max_print,
            print_all: self.print_all,
        };
        check_pending_receipt_origins(&store, epoch_manager, runtime.get_tries(), &options)
    }
}

/// Finds all receipts which are pending at the given block and checks that
/// every one of them which carries actions was created in, and originates from
/// a transaction included in, at least `min_protocol_version`, and that none
/// transfers to a universal account. Prints the findings to stdout.
///
/// The store must belong to a node which tracks all shards and saves the
/// `ReceiptToTx` mapping and the execution outcomes.
pub fn check_pending_receipt_origins(
    store: &Store,
    epoch_manager: Arc<dyn EpochManagerAdapter>,
    tries: ShardTries,
    options: &CheckOptions,
) -> anyhow::Result<CheckReport> {
    let adapter = store.chain_store();
    let mut checker = Checker {
        store: store.clone(),
        adapter: adapter.clone(),
        epoch_manager: epoch_manager.clone(),
        protocol_versions: HashMap::new(),
        execution_blocks: HashMap::new(),
    };

    let block_hash = match options.block_hash {
        Some(block_hash) => block_hash,
        None => adapter.final_head()?.last_block_hash,
    };
    let block = adapter.get_block(&block_hash)?;
    let block_ref = match checker.block_ref(&block_hash) {
        Ok(block_ref) => block_ref,
        Err(err) => bail!(err),
    };
    if !checker.is_canonical(&block_ref) {
        bail!("{block_ref} is not on the canonical chain");
    }
    let tail = adapter.tail();
    let tail_ref =
        adapter.get_block_hash_by_height(tail).ok().and_then(|hash| checker.block_ref(&hash).ok());
    println!("checking pending receipts at {block_ref}");
    println!("required minimum protocol version: {}", options.min_protocol_version);
    match tail_ref {
        Some(tail_ref) => println!("oldest block in the database (GC tail): {tail_ref}"),
        None => println!("oldest block in the database (GC tail): #{tail}"),
    }
    let shard_layout = epoch_manager.get_shard_layout(block.header().epoch_id())?;

    let mut pending = BTreeMap::new();

    // Receipts stored in the state.
    println!("\nreceipts stored in the state:");
    for shard_uid in shard_layout.shard_uids() {
        let chunk_extra = adapter.chunk_store().get_chunk_extra(&block_hash, &shard_uid)?;
        let counts =
            collect_state_receipts(&tries, shard_uid, *chunk_extra.state_root(), &mut pending)?;
        println!("  {shard_uid}: {counts:?}");
    }

    // Receipts in flight between shards.
    println!("\nreceipts in flight:");
    for chunk_header in block.chunks().iter_raw() {
        let shard_id = chunk_header.shard_id();
        let last_applied_height = chunk_header.height_included();
        let last_applied_hash = adapter.get_block_hash_by_height(last_applied_height)?;

        // Generated by the last applied chunk of the shard, but not included
        // in a chunk yet. They will be in the next chunk of the shard.
        let outgoing = adapter.get_outgoing_receipts(&last_applied_hash, shard_id)?;
        for receipt in outgoing.iter() {
            add_pending(
                &mut pending,
                receipt.clone(),
                format!("outgoing from shard {shard_id} generated at #{last_applied_height}"),
            );
        }

        // Included in chunks of the blocks after the last applied chunk of the
        // shard, but not applied by it yet.
        let incoming = get_incoming_receipts_for_shard(
            &adapter,
            epoch_manager.as_ref(),
            shard_id,
            &shard_layout,
            block_hash,
            last_applied_height,
            ReceiptFilter::TargetShard,
        )?;
        let mut incoming_count = 0;
        for response in incoming {
            let height = adapter.get_block_header(&response.0)?.height();
            for proof in response.1.iter() {
                for receipt in &proof.0 {
                    incoming_count += 1;
                    add_pending(
                        &mut pending,
                        receipt.clone(),
                        format!("incoming to shard {shard_id} included at #{height}"),
                    );
                }
            }
        }
        println!(
            "  shard {shard_id}: last chunk applied at #{last_applied_height}, {} outgoing, {incoming_count} incoming",
            outgoing.len(),
        );
    }

    println!("\nfound {} distinct pending receipts, tracing their origins", pending.len());
    Ok(check_receipts(&mut checker, pending, options))
}

fn check_receipts(
    checker: &mut Checker,
    pending: BTreeMap<CryptoHash, PendingReceipt>,
    options: &CheckOptions,
) -> CheckReport {
    let min_protocol_version = options.min_protocol_version;
    let mut report = CheckReport::default();
    let mut created_versions: BTreeMap<String, usize> = BTreeMap::new();
    let mut origin_versions: BTreeMap<String, usize> = BTreeMap::new();
    let mut violation_details: BTreeMap<&'static str, Vec<String>> = BTreeMap::new();
    let mut warning_details: BTreeMap<&'static str, Vec<String>> = BTreeMap::new();

    for (receipt_id, pending_receipt) in &pending {
        let provenance = checker.provenance(receipt_id);
        let created_key = match &provenance.created_in {
            Ok(block) => format!("protocol {}", block.protocol_version),
            Err(_) => "unknown".to_string(),
        };
        let origin_key = match &provenance.origin {
            Ok(OriginTx { block: Ok(block), .. }) => format!("protocol {}", block.protocol_version),
            _ => "unknown".to_string(),
        };
        *created_versions.entry(created_key).or_default() += 1;
        *origin_versions.entry(origin_key).or_default() += 1;

        let mut problems: Vec<(&'static str, String)> = vec![];
        for transfer in pending_receipt.universal_account_transfers() {
            problems.push(("transfer to a universal account", transfer));
        }
        match &provenance.created_in {
            Ok(block) if block.protocol_version < min_protocol_version => {
                problems.push(("created in an old protocol version", block.to_string()));
            }
            Ok(_) => {}
            Err(err) => problems.push(("unknown creation block", err.clone())),
        }
        match &provenance.origin {
            Ok(OriginTx { block: Ok(block), .. })
                if block.protocol_version < min_protocol_version =>
            {
                problems.push((
                    "originates from a transaction in an old protocol version",
                    block.to_string(),
                ));
            }
            Ok(OriginTx { block: Ok(_), .. }) => {}
            Ok(OriginTx { tx_hash, block: Err(err), .. }) => {
                problems.push(("unknown origin transaction block", format!("tx {tx_hash}: {err}")))
            }
            Err(err) => problems.push(("unknown origin transaction", err.clone())),
        }

        let details = describe(pending_receipt, &provenance);
        if options.print_all {
            println!("{details}");
        }
        // Receipts without actions don't buy gas and can't produce refunds, so
        // for them problems are only reported as warnings.
        let (ids, details_by_category) = if pending_receipt.has_actions() {
            (&mut report.violations, &mut violation_details)
        } else {
            (&mut report.warnings, &mut warning_details)
        };
        for (category, reason) in problems {
            ids.entry(category).or_default().push(*receipt_id);
            details_by_category
                .entry(category)
                .or_default()
                .push(format!("{details}\n    reason: {reason}"));
        }
    }

    println!("\npending receipts by protocol version of the creation block: {created_versions:?}");
    println!("pending receipts by protocol version of the origin transaction: {origin_versions:?}");

    let print_group = |title: &str, groups: &BTreeMap<&'static str, Vec<String>>| {
        for (category, entries) in groups {
            println!("\n{title}: {category}: {} receipts", entries.len());
            for entry in entries.iter().take(options.max_print) {
                println!("{entry}");
            }
            if entries.len() > options.max_print {
                println!("  ... and {} more", entries.len() - options.max_print);
            }
        }
    };
    print_group("WARNING (receipt without actions)", &warning_details);
    print_group("VIOLATION", &violation_details);

    if report.is_ok() {
        println!(
            "\nOK: all {} pending receipts with actions were created in, and originate from transactions in, protocol version >= {min_protocol_version}, and none transfers to a universal account",
            pending.values().filter(|r| r.has_actions()).count(),
        );
    } else {
        println!("\nFAILED: found pending receipts which violate the check, see above");
    }
    report.pending =
        pending.into_iter().map(|(id, pending_receipt)| (id, pending_receipt.locations)).collect();
    report
}

fn describe(pending_receipt: &PendingReceipt, provenance: &Provenance) -> String {
    let created_in = match &provenance.created_in {
        Ok(block) => block.to_string(),
        Err(err) => format!("unknown ({err})"),
    };
    let origin = match &provenance.origin {
        Ok(OriginTx { tx_hash, signer_id, block }) => {
            let block = match block {
                Ok(block) => block.to_string(),
                Err(err) => format!("unknown block ({err})"),
            };
            format!("tx {tx_hash} signed by {signer_id} in {block}, {} hops", provenance.depth)
        }
        Err(err) => format!("unknown ({err})"),
    };
    format!(
        "  receipt {}\n    found in: {}\n    created in: {created_in}\n    origin: {origin}",
        pending_receipt.summary(),
        pending_receipt.locations.join("; "),
    )
}

#[cfg(test)]
mod tests {
    use super::PendingReceipt;
    use near_crypto::{InMemorySigner, KeyType, PublicKey};
    use near_primitives::action::delegate::{DelegateAction, SignedDelegateAction};
    use near_primitives::action::{Action, FunctionCallAction, TransferAction};
    use near_primitives::hash::CryptoHash;
    use near_primitives::receipt::{ActionReceipt, Receipt, ReceiptEnum, ReceiptV0};
    use near_primitives::types::{AccountId, Balance, Gas};
    use near_primitives_core::universal_account_id::encode_universal_account_id;

    fn pending(receiver_id: &AccountId, actions: Vec<Action>) -> PendingReceipt {
        let signer_id: AccountId = "alice.near".parse().unwrap();
        let receipt = Receipt::V0(ReceiptV0 {
            predecessor_id: signer_id.clone(),
            receiver_id: receiver_id.clone(),
            receipt_id: CryptoHash::default(),
            receipt: ReceiptEnum::Action(ActionReceipt {
                signer_id,
                signer_public_key: PublicKey::empty(KeyType::ED25519),
                gas_price: Balance::ZERO,
                output_data_receivers: vec![],
                input_data_ids: vec![],
                actions,
            }),
        });
        PendingReceipt { receipt, locations: vec![] }
    }

    fn transfer() -> Action {
        Action::Transfer(TransferAction { deposit: Balance::from_near(1) })
    }

    fn delegate(receiver_id: &AccountId, action: Action) -> Action {
        let sender_id: AccountId = "relayed.near".parse().unwrap();
        let signer = InMemorySigner::from_seed(sender_id.clone(), KeyType::ED25519, "relayed");
        let delegate_action = DelegateAction {
            sender_id,
            receiver_id: receiver_id.clone(),
            actions: vec![action.try_into().unwrap()],
            nonce: 1,
            max_block_height: 100,
            public_key: signer.public_key(),
        };
        Action::Delegate(Box::new(SignedDelegateAction::sign(&signer, delegate_action)))
    }

    #[test]
    fn finds_transfers_to_universal_accounts() {
        let universal = encode_universal_account_id(&[7; 32]);
        let named: AccountId = "bob.near".parse().unwrap();
        let call = Action::FunctionCall(Box::new(FunctionCallAction {
            method_name: "f".to_string(),
            args: vec![],
            gas: Gas::from_teragas(1),
            deposit: Balance::ZERO,
        }));

        assert_eq!(pending(&universal, vec![transfer()]).universal_account_transfers().len(), 1);
        assert_eq!(
            pending(&named, vec![delegate(&universal, transfer())])
                .universal_account_transfers()
                .len(),
            1
        );
        assert_eq!(
            Vec::<String>::new(),
            pending(&named, vec![transfer()]).universal_account_transfers()
        );
        assert_eq!(
            Vec::<String>::new(),
            pending(&universal, vec![call.clone()]).universal_account_transfers()
        );
        assert_eq!(
            Vec::<String>::new(),
            pending(&named, vec![delegate(&named, transfer())]).universal_account_transfers()
        );
        assert_eq!(
            Vec::<String>::new(),
            pending(&named, vec![delegate(&universal, call)]).universal_account_transfers()
        );
    }
}

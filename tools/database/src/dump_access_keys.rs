use clap::Parser;
use near_chain::{ChainStore, ChainStoreAccess};
use near_chain_configs::GenesisValidationMode;
use near_crypto::PublicKey;
use near_epoch_manager::{EpochManager, EpochManagerAdapter};
use near_primitives::shard_layout::ShardUId;
use near_primitives::trie_key::col;
use near_primitives::trie_key::trie_key_parsers;
use near_primitives::types::AccountId;
use near_primitives_core::trie_key::access_key_key_len;
use near_store::StateSnapshotConfig;
use near_store::adapter::StoreAdapter;
use near_store::flat::FlatStorageManager;
use near_store::{ShardTries, Trie, TrieConfig, TrieDBStorage};
use nearcore::{load_config, open_storage};
use serde::Serialize;
use std::fs::File;
use std::io::BufWriter;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::Arc;

const ENTRIES_PER_FILE: usize = 1_000_000;

#[derive(Parser)]
pub(crate) struct DumpAccessKeysCommand {
    /// Output directory for dumped access key files.
    #[arg(short, long, default_value = "dump_access_keys")]
    output: PathBuf,
    /// Only dump access keys for this shard (e.g. "s0.v3"). If omitted, all shards are dumped.
    #[arg(short, long)]
    shard_uid: Option<ShardUId>,
}

#[derive(Serialize)]
struct AccessKeyEntry {
    account_id: AccountId,
    public_key: PublicKey,
}

impl DumpAccessKeysCommand {
    pub(crate) fn run(
        &self,
        home: &PathBuf,
        genesis_validation: GenesisValidationMode,
    ) -> anyhow::Result<()> {
        let near_config = load_config(home, genesis_validation).unwrap();
        let node_storage = open_storage(&home, &near_config).unwrap();
        let store = node_storage.get_split_store().unwrap_or_else(|| node_storage.get_hot_store());
        let chain_store = Rc::new(ChainStore::new(
            store.clone(),
            false,
            near_config.genesis.config.transaction_validity_period,
        ));

        let head = chain_store.head().unwrap();
        let epoch_manager =
            EpochManager::new_arc_handle(store.clone(), &near_config.genesis.config, None);
        let shard_layout = epoch_manager.get_shard_layout(&head.epoch_id).unwrap();

        let all_shard_uids: Vec<ShardUId> = shard_layout.shard_uids().collect();

        let shard_uids_to_dump = if let Some(requested) = &self.shard_uid {
            if !all_shard_uids.contains(requested) {
                let available: Vec<String> = all_shard_uids.iter().map(|s| s.to_string()).collect();
                anyhow::bail!(
                    "shard uid {} not found. available shard uids: {}",
                    requested,
                    available.join(", ")
                );
            }
            vec![*requested]
        } else {
            all_shard_uids
        };

        let flat_storage_manager = FlatStorageManager::new(store.flat_store());
        let shard_tries = ShardTries::new(
            store.trie_store(),
            TrieConfig::default(),
            flat_storage_manager.clone(),
            StateSnapshotConfig::Disabled,
        );

        std::fs::create_dir_all(&self.output)?;

        let mut total_count = 0usize;

        for shard_uid in shard_uids_to_dump {
            println!("processing shard {} (loading memtrie...)", shard_uid);

            let chunk_extra =
                chain_store.get_chunk_extra(&head.last_block_hash, &shard_uid).unwrap();
            let state_root = *chunk_extra.state_root();

            flat_storage_manager.create_flat_storage_for_shard(shard_uid)?;
            let flat_storage = flat_storage_manager
                .get_flat_storage_for_shard(shard_uid)
                .expect("flat storage was just created");
            flat_storage.update_flat_head(&head.last_block_hash)?;

            shard_tries.load_memtrie(&shard_uid, Some(state_root), true)?;

            let trie_storage = Arc::new(TrieDBStorage::new(store.trie_store(), shard_uid));
            let trie = Trie::new(trie_storage, state_root, None);

            let memtries_handle =
                shard_tries.get_memtries(shard_uid).expect("memtrie was just loaded");
            let memtries = memtries_handle.read();
            let mut iterator = memtries.get_iter(&trie)?;
            iterator.seek_prefix(&[col::ACCESS_KEY])?;

            let mut entries: Vec<AccessKeyEntry> = Vec::new();
            let mut count = 0usize;
            let mut part = 0usize;
            for item in &mut iterator {
                let (key, _value) = item?;
                if key.is_empty() || key[0] != col::ACCESS_KEY {
                    break;
                }

                let account_id = trie_key_parsers::parse_account_id_from_access_key_key(&key)?;
                let public_key =
                    trie_key_parsers::parse_public_key_from_access_key_key(&key, &account_id)?;

                // Skip GasKeyNonce entries which share the ACCESS_KEY prefix
                // but have extra bytes after the public key.
                let expected_len = access_key_key_len(account_id.len(), public_key.len());
                if key.len() != expected_len {
                    continue;
                }

                entries.push(AccessKeyEntry { account_id, public_key });
                count += 1;
                if count % 100_000 == 0 {
                    println!("  processed {} access keys...", count);
                }
                if entries.len() >= ENTRIES_PER_FILE {
                    self.write_part(&shard_uid, part, &entries)?;
                    entries.clear();
                    part += 1;
                }
            }
            if !entries.is_empty() {
                self.write_part(&shard_uid, part, &entries)?;
                entries.clear();
                part += 1;
            }
            total_count += count;
            println!("  found {} access keys in shard {} ({} parts)", count, shard_uid, part);

            drop(iterator);
            drop(memtries);
            shard_tries.unload_memtrie(&shard_uid);
        }

        println!("done, {} access keys total", total_count);

        Ok(())
    }

    fn write_part(
        &self,
        shard_uid: &near_store::ShardUId,
        part: usize,
        entries: &[AccessKeyEntry],
    ) -> anyhow::Result<()> {
        let filename = self.output.join(format!("{}_part{}.json", shard_uid, part));
        println!("  writing {} entries to {}", entries.len(), filename.display());
        let file = File::create(&filename)?;
        let writer = BufWriter::new(file);
        serde_json::to_writer_pretty(writer, entries)?;
        Ok(())
    }
}

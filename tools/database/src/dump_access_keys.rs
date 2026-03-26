use clap::Parser;
use near_chain::{ChainStore, ChainStoreAccess};
use near_chain_configs::GenesisValidationMode;
use near_crypto::PublicKey;
use near_epoch_manager::{EpochManager, EpochManagerAdapter};
use near_primitives::trie_key::col;
use near_primitives::trie_key::trie_key_parsers;
use near_primitives::types::AccountId;
use near_primitives_core::trie_key::access_key_key_len;
use near_store::adapter::StoreAdapter;
use near_store::{Trie, TrieDBStorage};
use nearcore::{load_config, open_storage};
use serde::Serialize;
use std::fs::File;
use std::io::BufWriter;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::Arc;

#[derive(Parser)]
pub(crate) struct DumpAccessKeysCommand {
    /// Output file path.
    #[arg(short, long, default_value = "dump.json")]
    output: PathBuf,
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

        let mut entries: Vec<AccessKeyEntry> = Vec::new();

        for shard_uid in shard_layout.shard_uids() {
            println!("processing shard {}", shard_uid);

            let chunk_extra =
                chain_store.get_chunk_extra(&head.last_block_hash, &shard_uid).unwrap();
            let state_root = chunk_extra.state_root();
            let trie_storage = Arc::new(TrieDBStorage::new(store.trie_store(), shard_uid));
            let trie = Trie::new(trie_storage, *state_root, None);

            let mut iterator = trie.disk_iter()?;
            iterator.seek_prefix(&[col::ACCESS_KEY])?;

            let mut count = 0usize;
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
            }
            println!("  found {} access keys in shard {}", count, shard_uid);
        }

        println!("writing {} access keys to {}", entries.len(), self.output.display());
        let file = File::create(&self.output)?;
        let writer = BufWriter::new(file);
        serde_json::to_writer_pretty(writer, &entries)?;
        println!("done");

        Ok(())
    }
}

use std::path::PathBuf;
use std::rc::Rc;
use std::str::FromStr;

use bytesize::ByteSize;
use clap::Parser;
use near_chain::{ChainStore, ChainStoreAccess};
use near_chain_configs::GenesisValidationMode;
use near_epoch_manager::EpochManager;
use near_primitives::trie_key::col;
use near_primitives::types::AccountId;
use near_store::{ShardUId, Trie, TrieDBStorage};
use nearcore::{load_config, open_storage};

#[derive(Parser)]
pub(crate) struct AnalyzeContractSizesCommand {
    /// Show top N contracts by size.
    #[arg(short, long)]
    topn: usize,
}

//const ACCOUNT_DATA_SEPARATOR: u8 = b',';

impl AnalyzeContractSizesCommand {
    pub(crate) fn run(&self, home: &PathBuf) -> anyhow::Result<()> {
        // Create a ChainStore and EpochManager that will be used to read blockchain data.
        let mut near_config = load_config(home, GenesisValidationMode::Full).unwrap();
        let node_storage = open_storage(&home, &mut near_config).unwrap();
        let store = node_storage.get_split_store().unwrap_or_else(|| node_storage.get_hot_store());

        let chain_store = Rc::new(ChainStore::new(
            store.clone(),
            near_config.genesis.config.genesis_height,
            false,
        ));
        let head = chain_store.head().unwrap();
        let epoch_manager =
            EpochManager::new_from_genesis_config(store.clone(), &near_config.genesis.config)
                .unwrap();
        let shard_layout = epoch_manager.get_shard_layout(&head.epoch_id).unwrap();
        let last_block = chain_store.get_block(&head.last_block_hash).unwrap();
        for chunk in last_block.chunks().iter() {
            let shard_id = chunk.shard_id();
            let shard_uid = ShardUId::from_shard_id_and_layout(shard_id, &shard_layout);
            println!("Analyzing chunk with uid: {}", shard_uid);

            let state_root = chunk.prev_state_root().clone();
            let trie_storage = Rc::new(TrieDBStorage::new(store.clone(), shard_uid));
            let trie = Trie::new(trie_storage, state_root, None);

            let mut iterator = trie.iter().unwrap();
            iterator.seek_prefix(&[col::CONTRACT_CODE]).unwrap();

            for item in iterator {
                let (key, value) = item.unwrap();
                if key.is_empty() || key[0] != col::CONTRACT_CODE {
                    break;
                }
                //let separator_pos = key.iter().position(|&x| x == ACCOUNT_DATA_SEPARATOR).unwrap();
                let account_id_bytes = &key[1..];
                //let contract_code = &key[separator_pos + 1..];

                let account_id_str = std::str::from_utf8(&account_id_bytes).unwrap();
                let account_id = AccountId::from_str(account_id_str).unwrap();

                println!(
                    "account: {}, contract size: {}",
                    account_id,
                    ByteSize::b(value.len() as u64)
                );
            }
        }
        Ok(())
    }
}

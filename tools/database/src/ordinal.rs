use std::path::PathBuf;
use std::rc::Rc;

use clap::Parser;
use near_chain::ChainStore;
use near_chain_configs::GenesisValidationMode;

use nearcore::config::load_config;

use nearcore::open_storage;

#[derive(Parser)]
pub(crate) struct OrdinalCommand {
    /// Get block hash with this ordinal
    #[arg(long)]
    get: Option<u64>,

    /// Delete block hash with this ordinal
    #[arg(long)]
    del: Option<u64>,

    /// Find first block that we have ordinal for
    #[arg(long)]
    find_first: bool,
}

impl OrdinalCommand {
    pub(crate) fn run(
        &self,
        home: &PathBuf,
        genesis_validation: GenesisValidationMode,
    ) -> anyhow::Result<()> {
        // Create a ChainStore and EpochManager that will be used to read blockchain data.
        let mut near_config = load_config(home, genesis_validation).unwrap();
        let node_storage = open_storage(&home, &mut near_config).unwrap();
        let store = node_storage.get_split_store().unwrap_or_else(|| node_storage.get_hot_store());
        let chain_store = Rc::new(ChainStore::new(
            store.clone(),
            false,
            near_config.genesis.config.transaction_validity_period,
        ));

        println!("get {:?}, del {:?}, find-first {}", self.get, self.del, self.find_first);

        if let Some(ordinal) = self.get {
            // Get the block hash for the given ordinal.
            let block_hash = chain_store.get_block_hash_from_ordinal(ordinal);
            println!("Block hash for ordinal {}: {:?}", ordinal, block_hash);
            return Ok(());
        }

        if let Some(ordinal) = self.del {
            chain_store.delete_ordinal(ordinal).unwrap_or_else(|e| {
                panic!("Failed to delete block hash for ordinal {}: {}", ordinal, e)
            });
        }

        if self.find_first {
            let mut jump = 2u64.pow(63);
            let mut cur: u64 = 0;

            let tip_block =
                chain_store.get_block_header(&chain_store.head().unwrap().last_block_hash).unwrap();

            while jump > 1 {
                let candidate = cur + jump;
                if candidate < tip_block.block_ordinal()
                    && chain_store.get_block_hash_from_ordinal(candidate).is_err()
                {
                    cur = candidate;
                }

                jump = jump / 2;
            }

            println!("First block ordinal we don't have: {}", cur);
            for ordinal in cur.saturating_sub(5)..cur.saturating_add(5) {
                match chain_store.get_block_hash_from_ordinal(ordinal) {
                    Ok(block_hash) => {
                        println!("Ordinal {}: {:?}", ordinal, block_hash);
                    }
                    Err(_) => {
                        println!("Ordinal {}: not found", ordinal);
                    }
                }
            }
        }

        Ok(())
    }
}

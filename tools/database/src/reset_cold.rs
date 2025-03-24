use std::path::PathBuf;

use clap::Parser;
use near_chain::ChainStore;
use near_chain_configs::GenesisValidationMode;
use near_store::archive::cold_storage::update_cold_head;
use nearcore::{load_config, open_storage};

#[derive(Parser)]
pub(crate) struct ResetColdHeadCommand {}

impl ResetColdHeadCommand {
    pub(crate) fn run(
        &self,
        home: &PathBuf,
        genesis_validation: GenesisValidationMode,
    ) -> anyhow::Result<()> {
        let mut near_config = load_config(home, genesis_validation).unwrap();
        let node_storage = open_storage(&home, &mut near_config).unwrap();
        let chain_store = ChainStore::new(
            node_storage.get_hot_store(),
            near_config.genesis.config.genesis_height,
            false,
            near_config.genesis.config.transaction_validity_period,
        );
        let tip = chain_store.final_head().unwrap();

        update_cold_head(
            node_storage.cold_db().expect("Cold db must exist to reset cold head"),
            &node_storage.get_hot_store(),
            &tip.height,
        )
        .unwrap();

        Ok(())
    }
}

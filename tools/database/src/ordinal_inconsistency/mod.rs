mod find;
mod read_db;
mod repair;
mod timer;

use std::path::PathBuf;

pub use find::find_ordinal_inconsistencies;
use near_chain::ChainStore;
use near_chain_configs::GenesisValidationMode;
use near_primitives::hash::CryptoHash;
use near_primitives::types::{BlockHeight, NumBlocks};
pub use repair::repair_ordinal_inconsistencies;

use crate::utils::get_user_confirmation;

pub struct OrdinalInconsistency {
    pub block_height: BlockHeight,
    pub block_ordinal: NumBlocks,
    pub correct_block_hash: CryptoHash,
    pub actual_block_hash: CryptoHash,
}

#[derive(clap::Parser)]
#[clap(subcommand_required = true, arg_required_else_help = true)]
pub(crate) enum OrdinalInconsistencyCommand {
    Find(FindCommand),
    FindAndRepair(FindAndRepairCommand),
}

#[derive(clap::Args)]
pub(crate) struct FindCommand {}

#[derive(clap::Args)]
pub(crate) struct FindAndRepairCommand {
    #[clap(long)]
    pub noconfirm: bool,
}

impl OrdinalInconsistencyCommand {
    pub(crate) fn run(
        &self,
        home: &PathBuf,
        genesis_validation: GenesisValidationMode,
    ) -> anyhow::Result<()> {
        let mut near_config = nearcore::config::load_config(home, genesis_validation)?;
        let node_storage = nearcore::open_storage(home, &mut near_config)?;
        let store = node_storage.get_split_store().unwrap_or_else(|| node_storage.get_hot_store());
        let chain_store = ChainStore::new(
            store.clone(),
            false,
            near_config.genesis.config.transaction_validity_period,
        );

        match self {
            OrdinalInconsistencyCommand::Find(_) => {
                find_ordinal_inconsistencies(&chain_store).unwrap();
            }
            OrdinalInconsistencyCommand::FindAndRepair(scan_and_fix_cmd) => {
                let inconsistencies = find_ordinal_inconsistencies(&chain_store).unwrap();
                if !scan_and_fix_cmd.noconfirm {
                    if !get_user_confirmation(&format!("Contiune with repair?")) {
                        println!("Aborting...");
                        return Ok(());
                    }
                }
                repair_ordinal_inconsistencies(&store, &inconsistencies).unwrap();
            }
        }
        Ok(())
    }
}

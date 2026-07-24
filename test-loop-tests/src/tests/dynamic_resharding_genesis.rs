//! Tests for chains whose genesis protocol version already has dynamic resharding enabled.
//!
//! Such a chain has no static shard layout in its genesis epoch config, so the genesis shard layout
//! is taken from the genesis config. This is the case for every chain created after dynamic
//! resharding was enabled, for example a forknet of mainnet.

use crate::setup::builder::TestLoopBuilder;
use crate::utils::account::{create_validators_spec, validators_spec_clients};
use near_chain_configs::Genesis;
use near_chain_configs::test_genesis::{TestEpochConfigBuilder, ValidatorsSpec};
use near_o11y::testonly::init_test_logger;
use near_primitives::chains::MAINNET;
use near_primitives::epoch_manager::{
    DynamicReshardingConfig, EpochConfig, EpochConfigStore, ShardLayoutConfig,
};
use near_primitives::shard_layout::ShardLayout;
use near_primitives::types::EpochId;
use near_primitives::version::{PROD_GENESIS_PROTOCOL_VERSION, PROTOCOL_VERSION};
use near_store::genesis::initialize_sharded_genesis_state;
use near_store::get_genesis_state_roots;
use near_store::test_utils::create_test_store;
use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

const EPOCH_LENGTH: u64 = 5;

/// Builds a genesis and its epoch config, with dynamic resharding enabled from genesis. The default
/// dynamic config has thresholds high enough to never split a shard.
fn genesis_with_dynamic_resharding(
    chain_id: &str,
    shard_layout: &ShardLayout,
    validators_spec: ValidatorsSpec,
) -> (Genesis, EpochConfig) {
    let genesis = TestLoopBuilder::new_genesis_builder()
        .chain_id(chain_id.to_string())
        .protocol_version(PROTOCOL_VERSION)
        .validators_spec(validators_spec)
        .shard_layout(shard_layout.clone())
        .epoch_length(EPOCH_LENGTH)
        .build();

    let mut epoch_config = TestEpochConfigBuilder::from_genesis(&genesis).build();
    epoch_config.shard_layout_config = ShardLayoutConfig::Dynamic {
        dynamic_resharding_config: DynamicReshardingConfig::default(),
    };
    assert!(epoch_config.static_shard_layout().is_none());

    (genesis, epoch_config)
}

/// The chain starts and progresses across epoch boundaries using the shard layout from the genesis
/// config.
#[test]
fn test_chain_with_dynamic_resharding_at_genesis() {
    init_test_logger();

    let shard_layout = ShardLayout::multi_shard(3, 3);
    let validators_spec = create_validators_spec(2, 0);
    let clients = validators_spec_clients(&validators_spec);
    let (genesis, epoch_config) =
        genesis_with_dynamic_resharding("test-chain", &shard_layout, validators_spec);

    let epoch_config_store = EpochConfigStore::test(BTreeMap::from([(
        genesis.config.protocol_version,
        Arc::new(epoch_config),
    )]));

    let mut env = TestLoopBuilder::new()
        .genesis(genesis)
        .clients(clients)
        .epoch_config_store(epoch_config_store)
        .build();

    let epoch_manager = env.validator().client().epoch_manager.clone();
    assert_eq!(epoch_manager.get_shard_layout(&EpochId::default()).unwrap(), shard_layout);

    // Run over a few epoch boundaries; the layout of every epoch is the genesis one.
    for _ in 0..3 {
        env.validator_runner().run_until_new_epoch();
        let epoch_id = env.validator().head().epoch_id;
        assert_eq!(epoch_manager.get_shard_layout(&epoch_id).unwrap(), shard_layout);
    }

    // Every shard of the genesis layout keeps producing chunks.
    let mut shards_with_new_chunks = HashSet::new();
    let first_height = env.validator().head().height;
    env.validator_runner().run_for_number_of_blocks(3);
    for height in first_height..=env.validator().head().height {
        let node = env.validator();
        let Ok(block) = node.client().chain.get_block_by_height(height) else {
            continue;
        };
        assert_eq!(block.chunks().len(), shard_layout.num_shards() as usize);
        for new_chunk in block.chunks().iter_new() {
            shards_with_new_chunks.insert(new_chunk.shard_id());
        }
    }
    assert_eq!(shards_with_new_chunks, shard_layout.shard_ids().collect::<HashSet<_>>());
}

/// Genesis state is sharded according to the genesis config, the same way the node does it on
/// startup in `nearcore::start_with_config_and_synchronization_impl`. A forknet runs with
/// `chain_id = mainnet`, so the hardcoded prod genesis state roots must not be checked against it.
#[test]
fn test_genesis_state_init_with_dynamic_resharding_at_genesis() {
    init_test_logger();

    let shard_layout = ShardLayout::multi_shard(3, 3);
    let (genesis, epoch_config) =
        genesis_with_dynamic_resharding(MAINNET, &shard_layout, create_validators_spec(2, 0));
    assert_ne!(genesis.config.protocol_version, PROD_GENESIS_PROTOCOL_VERSION);

    let store = create_test_store();
    initialize_sharded_genesis_state(store.clone(), &genesis, &epoch_config, None);

    let state_roots = get_genesis_state_roots(&store).unwrap();
    assert_eq!(state_roots.len(), shard_layout.num_shards() as usize);
}

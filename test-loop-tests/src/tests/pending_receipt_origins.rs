//! The `check-pending-receipt-origins` state viewer command finds the receipts
//! left over from before a protocol upgrade.

use crate::setup::builder::TestLoopBuilder;
use crate::setup::env::TestLoopEnv;
use crate::utils::account::create_account_id;
use assert_matches::assert_matches;
use near_async::time::Duration;
use near_o11y::testonly::init_test_logger;
use near_parameters::RuntimeConfigStore;
use near_primitives::gas::Gas;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::types::{Balance, ProtocolVersion};
use near_primitives::upgrade_schedule::ProtocolUpgradeVotingSchedule;
use near_primitives::version::ProtocolFeature;
use near_primitives::views::FinalExecutionStatus;
use near_state_viewer::pending_receipt_origins::{
    CheckOptions, CheckReport, check_pending_receipt_origins,
};

fn check(env: &TestLoopEnv, min_protocol_version: ProtocolVersion) -> CheckReport {
    let node = env.rpc_node();
    let client = node.client();
    let options =
        CheckOptions { block_hash: None, min_protocol_version, max_print: 10, print_all: true };
    check_pending_receipt_origins(
        &node.store(),
        client.epoch_manager.clone(),
        client.runtime_adapter.get_tries(),
        &options,
    )
    .unwrap()
}

/// A yield created before the upgrade stays pending after it and is reported,
/// until it times out.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
#[test]
fn test_check_pending_receipt_origins_across_upgrade() {
    init_test_logger();

    let new_protocol = ProtocolFeature::RejectUniversalAccountTransfers.protocol_version();
    let old_protocol = new_protocol - 1;
    let contract = create_account_id("alice");
    let user = create_account_id("zoe");
    let epoch_length = 10;
    // The production configs time a yield out after 200 blocks, long enough to
    // outlive the upgrade.
    let runtime_config_store = RuntimeConfigStore::new(None);
    let yield_timeout = runtime_config_store
        .get_config(old_protocol)
        .wasm_config
        .limit_config
        .yield_timeout_length_in_blocks;

    let mut env = TestLoopBuilder::new()
        .enable_rpc()
        .protocol_version(old_protocol)
        .protocol_upgrade_schedule(ProtocolUpgradeVotingSchedule::new_immediate(new_protocol))
        .epoch_length(epoch_length)
        .shard_layout(ShardLayout::multi_shard_custom(vec![create_account_id("mm")], 1))
        .runtime_config_store(runtime_config_store)
        .add_user_account(&contract, Balance::from_near(1_000))
        .add_user_account(&user, Balance::from_near(1_000))
        .build();

    let deploy = env.rpc_node().tx_deploy_contract(
        &contract,
        near_test_contracts::backwards_compatible_rs_contract().to_vec(),
    );
    env.rpc_runner().run_tx(deploy, Duration::seconds(10));
    assert_eq!(env.rpc_node().protocol_version_at_head(), old_protocol);

    // The yield is called from another shard so its receipt crosses shards.
    let yield_tx = env.rpc_node().tx_call(
        &user,
        &contract,
        "call_yield_create_return_promise",
        vec![],
        Balance::ZERO,
        Gas::from_teragas(300),
    );
    let yield_tx_hash = yield_tx.get_hash();
    env.rpc_node().submit_tx(yield_tx);
    env.rpc_runner().run_for_number_of_blocks(5);
    assert_matches!(
        env.rpc_node()
            .client()
            .chain
            .get_partial_transaction_result(&yield_tx_hash)
            .unwrap()
            .status,
        FinalExecutionStatus::Started,
        "the yield should be pending"
    );
    assert_eq!(env.rpc_node().protocol_version_at_head(), old_protocol);

    // Everything pending so far comes from the old protocol.
    let report = check(&env, old_protocol);
    assert!(report.is_ok(), "{report:?}");
    let report = check(&env, new_protocol);
    assert!(!report.is_ok(), "the yield should be reported");

    // Wait until the final block is in the new protocol.
    env.rpc_runner().run_until(
        |node| {
            let client = node.client();
            let final_head = client.chain.final_head().unwrap();
            let epoch_id = client.epoch_manager.get_epoch_id(&final_head.last_block_hash).unwrap();
            client.epoch_manager.get_epoch_protocol_version(&epoch_id).unwrap() >= new_protocol
        },
        Duration::seconds(10 * epoch_length as i64),
    );
    let upgrade_height = env.rpc_node().head().height;

    let report = check(&env, new_protocol);
    let created_old = &report.violations["created in an old protocol version"];
    let originates_old =
        &report.violations["originates from a transaction in an old protocol version"];
    assert!(!created_old.is_empty(), "{report:?}");
    for receipt_id in created_old.iter().chain(originates_old) {
        let locations = &report.pending[receipt_id];
        assert!(
            locations.iter().any(|location| location.starts_with("promise_yield")),
            "only the yield should be left from the old protocol, got {receipt_id} in {locations:?}",
        );
    }

    // Once the yield times out nothing is left from the old protocol.
    env.rpc_runner().run_until_head_height(upgrade_height + yield_timeout + 5);
    let report = check(&env, new_protocol);
    assert!(report.is_ok(), "{report:?}");
}

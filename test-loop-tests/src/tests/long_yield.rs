use crate::setup::builder::TestLoopBuilder;
use crate::tests::yield_timeouts::assert_no_promise_yield_status_in_state;
use near_async::time::Duration;
use near_o11y::testonly::init_test_logger;
use near_parameters::config::TEST_CONFIG_YIELD_TIMEOUT_LENGTH;
use near_parameters::{RuntimeConfig, RuntimeConfigStore};
use near_primitives::action::{Action, FunctionCallAction};
use near_primitives::gas::Gas;
use near_primitives::test_utils::create_user_test_signer;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::{AccountId, Balance};
use near_primitives::version::{PROTOCOL_VERSION, ProtocolFeature};
use near_primitives::views::FinalExecutionStatus;

/// Test how many blocks a single transaction can delay receipt execution using
/// recursive promise_yield_create. Each yield times out after
/// `TEST_CONFIG_YIELD_TIMEOUT_LENGTH` blocks (10 in test config, 200 on mainnet),
/// and the timeout callback creates another yield, continuing until gas runs out.
///
/// The test calls `recursive_yield` with 1 PGas (1000 TGas) of attached gas
/// and measures how many iterations complete and the total block delay.
#[test]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_recursive_yield_max_delay() {
    init_test_logger();

    let test_account: AccountId = "test0".parse().unwrap();
    let test_account_signer = create_user_test_signer(&test_account).into();

    let runtime_config = RuntimeConfig::test();
    assert_eq!(
        runtime_config.wasm_config.limit_config.yield_timeout_length_in_blocks,
        TEST_CONFIG_YIELD_TIMEOUT_LENGTH
    );
    let runtime_config_store = RuntimeConfigStore::with_one_config(runtime_config);

    // The recursive yields can span many blocks (700+), exceeding the default
    // GC window (5 epochs * 100 blocks = 500). Increase GC retention to avoid
    // losing the transaction data mid-test.
    let mut env = TestLoopBuilder::new()
        .genesis_height(0)
        .add_user_account(&test_account, Balance::from_near(1_000_000))
        .runtime_config_store(runtime_config_store)
        .gc_num_epochs_to_keep(20)
        .skip_warmup()
        .build();

    assert_eq!(env.validator().head().height, 0);

    let genesis_block = env.validator().client().chain.get_block_by_height(0).unwrap();

    // Deploy the test contract.
    let deploy_contract_tx = SignedTransaction::deploy_contract(
        1,
        &test_account,
        near_test_contracts::rs_contract().into(),
        &test_account_signer,
        *genesis_block.hash(),
    );
    env.validator().submit_tx(deploy_contract_tx.clone());

    if ProtocolFeature::Spice.enabled(PROTOCOL_VERSION) {
        env.validator_runner().run_until_executed_height(2);
    } else {
        env.validator_runner().run_until_head_height(2);
    }
    assert!(matches!(
        env.validator()
            .client()
            .chain
            .get_final_transaction_result(&deploy_contract_tx.get_hash())
            .unwrap()
            .status,
        FinalExecutionStatus::SuccessValue(_),
    ));

    // Call recursive_yield with 1 PGas (1000 TGas).
    let one_pgas = Gas::from_teragas(1000);
    let yield_transaction = SignedTransaction::from_actions(
        10,
        test_account.clone(),
        test_account,
        &test_account_signer,
        vec![Action::FunctionCall(Box::new(FunctionCallAction {
            method_name: "recursive_yield".to_string(),
            args: vec![],
            gas: one_pgas,
            deposit: Balance::ZERO,
        }))],
        *genesis_block.hash(),
    );
    let yield_tx_hash = yield_transaction.get_hash();
    env.validator().submit_tx(yield_transaction);

    // Advance a few blocks so the transaction gets included and starts executing.
    if ProtocolFeature::Spice.enabled(PROTOCOL_VERSION) {
        env.validator_runner().run_until_executed_height(4);
    } else {
        env.validator_runner().run_until_head_height(4);
    }
    assert!(matches!(
        env.validator()
            .client()
            .chain
            .get_partial_transaction_result(&yield_tx_hash)
            .unwrap()
            .status,
        FinalExecutionStatus::Started,
    ));

    // Run until the transaction completes. Each yield iteration takes
    // TEST_CONFIG_YIELD_TIMEOUT_LENGTH blocks. Use a generous timeout.
    env.validator_runner().run_until(
        |node| {
            let status =
                node.client().chain.get_partial_transaction_result(&yield_tx_hash).unwrap().status;
            status != FinalExecutionStatus::Started
        },
        Duration::seconds(10_000),
    );

    // Get the full transaction result to examine receipts.
    let result =
        env.validator().client().chain.get_final_transaction_result(&yield_tx_hash).unwrap();

    // The last callback runs out of gas, so the whole chain resolves to failure.
    assert!(
        matches!(result.status, FinalExecutionStatus::Failure(_)),
        "expected failure due to gas exhaustion, got: {:?}",
        result.status,
    );

    // Count the receipts. Each yield iteration produces receipts:
    // - The function call receipt (PromiseYield)
    // - The timeout PromiseResume receipt
    // - The callback execution receipt
    // The number of recursive_yield executions is roughly receipts / 2
    // (each iteration has a yield+callback pair, plus the timeout resume receipt).
    let num_receipts = result.receipts_outcome.len();

    // Find the block heights of the first and last receipt.
    let client = env.validator().client();
    let first_receipt_block =
        client.chain.get_block(&result.transaction_outcome.block_hash).unwrap();
    let first_height = first_receipt_block.header().height();

    let last_receipt = result.receipts_outcome.last().unwrap();
    let last_receipt_block = client.chain.get_block(&last_receipt.block_hash).unwrap();
    let last_height = last_receipt_block.header().height();

    let total_block_delay = last_height - first_height;

    // Count yield iterations by looking for receipt outcomes that produced
    // outgoing receipts. Each successful recursive_yield call creates a
    // PromiseYield receipt, so it has a non-empty receipt_ids list.
    let yield_iterations = result
        .receipts_outcome
        .iter()
        .filter(|r| !r.outcome.receipt_ids.is_empty())
        .count();

    let mainnet_timeout: u64 = 200;
    let estimated_mainnet_delay = yield_iterations as u64 * mainnet_timeout;
    let estimated_mainnet_seconds = estimated_mainnet_delay as f64 * 1.3;

    eprintln!("=== Recursive Yield Delay Results ===");
    eprintln!("attached gas: 1 PGas (1000 TGas)");
    eprintln!("yield timeout (test config): {} blocks", TEST_CONFIG_YIELD_TIMEOUT_LENGTH);
    eprintln!("total receipts: {}", num_receipts);
    eprintln!("yield iterations (function executions): {}", yield_iterations);
    eprintln!(
        "block range: {} -> {} (total delay: {} blocks)",
        first_height, last_height, total_block_delay
    );
    eprintln!("--- Mainnet extrapolation (timeout = {} blocks) ---", mainnet_timeout);
    eprintln!("estimated mainnet delay: {} blocks", estimated_mainnet_delay);
    eprintln!(
        "estimated mainnet time: {:.0} seconds ({:.1} minutes, {:.2} hours)",
        estimated_mainnet_seconds,
        estimated_mainnet_seconds / 60.0,
        estimated_mainnet_seconds / 3600.0,
    );

    // Sanity checks.
    assert!(yield_iterations > 1, "should have at least 2 yield iterations");
    assert!(
        total_block_delay >= TEST_CONFIG_YIELD_TIMEOUT_LENGTH,
        "delay should be at least one timeout period"
    );

    assert_no_promise_yield_status_in_state(&env);
}

use borsh::BorshSerialize;
use evm_gas_station::instruction::TxFilter;
use evm_gas_station::state::Payer;
use evm_state::Address;
use solana_sdk::account::{AccountSharedData, WritableAccount};
use {
    bincode::serialize,
    evm_bridge::bridge::EvmBridge,
    evm_bridge::pool::{EthPool, SystemClock},
    evm_rpc::{BlockId, Hex, RPCLogFilter, RPCTransaction},
    evm_state::TransactionInReceipt,
    log::*,
    primitive_types::{H256, U256},
    reqwest::{self, header::CONTENT_TYPE},
    serde_json::{json, Value},
    solana_account_decoder::UiAccount,
    solana_client::{
        client_error::{ClientErrorKind, Result as ClientResult},
        pubsub_client::PubsubClient,
        rpc_client::RpcClient,
        rpc_config::{RpcAccountInfoConfig, RpcSendTransactionConfig, RpcSignatureSubscribeConfig},
        rpc_request::RpcError,
        rpc_response::{Response as RpcResponse, RpcSignatureResult, SlotUpdate},
        tpu_client::{TpuClient, TpuClientConfig},
    },
    solana_rpc::rpc::JsonRpcConfig,
    solana_sdk::{
        commitment_config::{CommitmentConfig, CommitmentLevel},
        fee_calculator::FeeRateGovernor,
        hash::Hash,
        pubkey::Pubkey,
        rent::Rent,
        signature::{Keypair, Signer},
        system_instruction::assign,
        system_transaction,
        transaction::Transaction,
    },
    solana_streamer::socket::SocketAddrSpace,
    solana_test_validator::{TestValidator, TestValidatorGenesis},
    solana_transaction_status::TransactionStatus,
    std::{
        collections::HashSet,
        net::UdpSocket,
        str::FromStr,
        sync::{mpsc::channel, Arc},
        thread::sleep,
        time::{Duration, Instant},
    },
    tokio::runtime::Runtime,
};

#[test]
fn test_test() {
    solana_logger::setup();

    let gas_station = Keypair::new();
    let alice = Keypair::new();
    let evm_contract1 = Address::zero();
    let evm_contract2 = Address::from([1u8; 20]);
    let payer_storage1 = Keypair::new();
    let payer_pda1 = Keypair::new();
    let payer_storage2 = Keypair::new();
    let payer_pda2 = Keypair::new();
    let payer_storage3 = Keypair::new();
    let payer_pda3 = Keypair::new();
    let mut payer_bytes = vec![];
    let payer_data = Payer {
        owner: alice.pubkey(),
        payer: payer_pda1.pubkey(),
        filters: vec![TxFilter::InputStartsWith {
            contract: evm_contract1,
            input_prefix: vec![],
        }],
    };
    BorshSerialize::serialize(&payer_data, &mut payer_bytes).unwrap();
    let acc1 = AccountSharedData::create(1, payer_bytes.clone(), gas_station.pubkey(), false, 0);
    payer_bytes.clear();
    let payer_data = Payer {
        owner: alice.pubkey(),
        payer: payer_pda2.pubkey(),
        filters: vec![TxFilter::InputStartsWith {
            contract: evm_contract1,
            input_prefix: vec![0, 1, 2, 3],
        }],
    };
    BorshSerialize::serialize(&payer_data, &mut payer_bytes).unwrap();
    let acc2 = AccountSharedData::create(1, payer_bytes.clone(), gas_station.pubkey(), false, 0);
    payer_bytes.clear();
    let payer_data = Payer {
        owner: alice.pubkey(),
        payer: payer_pda3.pubkey(),
        filters: vec![TxFilter::InputStartsWith {
            contract: evm_contract2,
            input_prefix: vec![],
        }],
    };
    BorshSerialize::serialize(&payer_data, &mut payer_bytes).unwrap();
    let acc3 = AccountSharedData::create(1, payer_bytes, gas_station.pubkey(), false, 0);
    let test_validator = TestValidatorGenesis::default()
        .add_account(payer_storage1.pubkey(), acc1)
        .add_account(payer_storage2.pubkey(), acc2)
        .add_account(payer_storage3.pubkey(), acc3)
        .add_account(
            payer_pda1.pubkey(),
            AccountSharedData::new(1, 0, &gas_station.pubkey()),
        )
        .add_account(
            payer_pda2.pubkey(),
            AccountSharedData::new(2, 0, &gas_station.pubkey()),
        )
        .add_account(
            payer_pda3.pubkey(),
            AccountSharedData::new(3, 0, &gas_station.pubkey()),
        )
        .rpc_config(JsonRpcConfig {
            max_batch_duration: Some(Duration::from_secs(0)),
            ..JsonRpcConfig::default_for_test()
        })
        .start_with_mint_address(alice.pubkey(), SocketAddrSpace::Unspecified)
        .expect("validator start failed");
    let rpc_url = test_validator.rpc_url();

    let mut bridge = EvmBridge::new_for_test(111u64, vec![], rpc_url);
    let filters = tokio_test::block_on(bridge.fetch_gas_station_filters(&gas_station.pubkey()));
    assert_eq!(filters.len(), 2);
    assert_eq!(filters.get(&evm_contract1).unwrap().len(), 2);
    assert_eq!(filters.get(&evm_contract2).unwrap().len(), 1);

    bridge.set_redirect_to_proxy_filters(gas_station.pubkey(), filters);

    let dummy_tx = evm_gas_station::evm_types::Transaction {
        nonce: 0.into(),
        gas_price: 0.into(),
        gas_limit: 0.into(),
        action: evm_gas_station::evm_types::TransactionAction::Call(evm_contract1),
        value: 0.into(),
        signature: evm_gas_station::evm_types::TransactionSignature {
            v: 0u64,
            r: H256::zero(),
            s: H256::zero(),
        },
        input: vec![0, 1, 2, 3],
    };
    let filter = tokio_test::block_on(bridge.find_gas_station_filter(&dummy_tx)).unwrap();
    assert_eq!(filter.gas_station_payer, payer_pda2.pubkey());
    assert_eq!(filter.storage_acc, payer_storage2.pubkey());
}

use super::*;
use borsh::{BorshDeserialize, BorshSerialize};
use solana_program::{program_memory::sol_memcmp, pubkey::PUBKEY_BYTES};
use solana_sdk::{
    account_info::{next_account_info, AccountInfo},
    entrypoint::ProgramResult,
    instruction::{AccountMeta, Instruction},
    msg,
    program::invoke_signed,
    program_error::ProgramError,
    program_pack::IsInitialized,
    pubkey::Pubkey,
    rent::Rent,
    system_instruction,
    sysvar::Sysvar,
};

use error::GasStationError;
use instruction::{GasStationInstruction, TxFilter};
use state::{Payer, MAX_FILTERS};

const EXECUTE_CALL_REFUND_AMOUNT: u64 = 10000;


pub fn create_evm_instruction_with_borsh(
    program_id: Pubkey,
    data: &evm_loader_instructions::EvmInstruction,
    accounts: Vec<AccountMeta>,
) -> Instruction {
    let mut res = Instruction::new_with_borsh(program_id, data, accounts);
    res.data.insert(0, evm_loader_instructions::EVM_INSTRUCTION_BORSH_PREFIX);
    res
}

pub fn process_instruction(
    program_id: &Pubkey,
    accounts: &[AccountInfo],
    instruction_data: &[u8],
) -> ProgramResult {
    let ix = BorshDeserialize::deserialize(&mut &*instruction_data)
        .map_err(|_| ProgramError::InvalidInstructionData)?;

    match ix {
        GasStationInstruction::RegisterPayer {
            owner,
            transfer_amount,
            whitelist,
        } => process_register_payer(program_id, accounts, owner, transfer_amount, whitelist),
        GasStationInstruction::ExecuteWithPayer { tx } => {
            process_execute_with_payer(program_id, accounts, tx)
        }
    }
}

fn process_register_payer(
    program_id: &Pubkey,
    accounts: &[AccountInfo],
    owner: Pubkey,
    transfer_amount: u64,
    whitelist: Vec<TxFilter>,
) -> ProgramResult {
    if whitelist.is_empty() || whitelist.len() > MAX_FILTERS {
        return Err(GasStationError::InvalidFilterAmount.into());
    }
    let account_info_iter = &mut accounts.iter();
    let creator_info = next_account_info(account_info_iter)?;
    let storage_acc_info = next_account_info(account_info_iter)?;
    let payer_acc_info = next_account_info(account_info_iter)?;
    let system_program = next_account_info(account_info_iter)?;

    let mut payer: Payer = BorshDeserialize::deserialize(&mut &**storage_acc_info.data.borrow())
        .map_err(|_e| -> ProgramError { GasStationError::InvalidAccountBorshData.into() })?;
    if payer.is_initialized() {
        return Err(GasStationError::AccountInUse.into());
    }

    let rent = Rent::get()?;
    let payer_data_len = storage_acc_info.data_len();
    if !rent.is_exempt(storage_acc_info.lamports(), payer_data_len) {
        return Err(GasStationError::NotRentExempt.into());
    }

    let (payer_acc, bump_seed) = Pubkey::find_program_address(&[owner.as_ref()], program_id);
    let rent_lamports = rent.minimum_balance(0);
    invoke_signed(
        &system_instruction::create_account(
            creator_info.key,
            &payer_acc,
            rent_lamports + transfer_amount,
            0,
            program_id,
        ),
        &[
            creator_info.clone(),
            payer_acc_info.clone(),
            system_program.clone(),
        ],
        &[&[owner.as_ref(), &[bump_seed]]],
    )?;
    msg!("PDA created: {}", payer_acc);

    payer.owner = owner;
    payer.payer = payer_acc;
    payer.filters = whitelist;
    BorshSerialize::serialize(&payer, &mut &mut storage_acc_info.data.borrow_mut()[..]).unwrap();
    Ok(())
}

fn process_execute_with_payer(
    program_id: &Pubkey,
    accounts: &[AccountInfo],
    tx: Option<evm_types::Transaction>,
) -> ProgramResult {
    let account_info_iter = &mut accounts.iter();
    let sender = next_account_info(account_info_iter)?;
    let payer_storage_info = next_account_info(account_info_iter)?;
    let payer_info = next_account_info(account_info_iter)?;
    let evm_loader = next_account_info(account_info_iter)?;
    let evm_state = next_account_info(account_info_iter)?;
    let system_program = next_account_info(account_info_iter)?;

    let big_tx_storage_info = next_account_info(account_info_iter);
    let tx_passed_directly = tx.is_some();
    if !tx_passed_directly && big_tx_storage_info.is_err() {
        return Err(GasStationError::BigTxStorageMissing.into());
    }
    if !tx_passed_directly { // Big tx not supported at the moment
        return Err(GasStationError::NotSupported.into());
    }

    if !cmp_pubkeys(program_id, payer_storage_info.owner) {
        return Err(ProgramError::IncorrectProgramId);
    }
    if !cmp_pubkeys(evm_loader.key, &solana_sdk::evm_loader::ID) {
        return Err(GasStationError::InvalidEvmLoader.into());
    }
    if !cmp_pubkeys(evm_state.key, &solana_sdk::evm_state::ID) {
        return Err(GasStationError::InvalidEvmState.into());
    }
    let mut payer_data_buf: &[u8] = &**payer_storage_info.data.borrow();
    let payer: Payer = BorshDeserialize::deserialize(&mut payer_data_buf)
        .map_err(|_e| -> ProgramError { GasStationError::InvalidAccountBorshData.into() })?;
    if !payer.is_initialized() {
        return Err(GasStationError::AccountNotInitialized.into());
    }
    if !payer_data_buf.is_empty() {
        return Err(GasStationError::InvalidAccountBorshData.into());
    }
    if payer.payer != *payer_info.key {
        return Err(GasStationError::PayerAccountMismatch.into());
    }

    let unpacked_tx = match tx {
        None => {
            let big_tx_storage_info = big_tx_storage_info.clone().unwrap();
            get_big_tx_from_storage(big_tx_storage_info)?
        }
        Some(tx) => tx,
    };
    if !payer.do_filter_match(&unpacked_tx) {
        return Err(GasStationError::PayerFilterMismatch.into());
    }

    {
        let (_payer_acc, bump_seed) =
            Pubkey::find_program_address(&[payer.owner.as_ref()], program_id);
        let signers_seeds: &[&[&[u8]]] = &[&[payer.owner.as_ref(), &[bump_seed]]];
        // pass sender acc to evm loader, execute tx restore ownership
        payer_info.assign(&solana_sdk::evm_loader::ID);

        let (ix, account_infos) = if tx_passed_directly {
            make_evm_loader_tx_execute_ix(evm_loader, evm_state, payer_info, unpacked_tx)
        } else {
            make_evm_loader_big_tx_execute_ix(
                evm_loader,
                evm_state,
                payer_info,
                big_tx_storage_info.unwrap(),
            )
        };
        invoke_signed(&ix, &account_infos, signers_seeds)?;

        let ix = make_free_ownership_ix(*payer_info.key);
        let account_infos = vec![evm_loader.clone(), evm_state.clone(), payer_info.clone()];
        invoke_signed(&ix, &account_infos, signers_seeds)?;

        let ix = system_instruction::assign(payer_info.key, &program_id);
        let account_infos = vec![system_program.clone(), payer_info.clone()];
        invoke_signed(&ix, &account_infos, signers_seeds)?;
    }

    let refund_amount = EXECUTE_CALL_REFUND_AMOUNT;
    refund_native_fee(sender, payer_info, refund_amount)?;


    let rent = Rent::get()?;
    if !rent.is_exempt(payer_info.lamports(), payer_info.data_len()) {
        return Err(GasStationError::NotRentExempt.into());
    }
    Ok(())
}

pub fn cmp_pubkeys(a: &Pubkey, b: &Pubkey) -> bool {
    sol_memcmp(a.as_ref(), b.as_ref(), PUBKEY_BYTES) == 0
}

fn get_big_tx_from_storage(storage_acc: &AccountInfo) -> Result<evm_types::Transaction, ProgramError> {
    let mut bytes: &[u8] = &storage_acc.try_borrow_data().unwrap();
    msg!("Trying to deserialize tx chunks byte = {:?}", bytes);
    BorshDeserialize::deserialize(&mut bytes)
        .map_err(|_e| GasStationError::InvalidBigTransactionData.into())
}

fn make_evm_loader_tx_execute_ix<'a>(
    evm_loader: &AccountInfo<'a>,
    evm_state: &AccountInfo<'a>,
    sender: &AccountInfo<'a>,
    tx: evm_types::Transaction,
) -> (Instruction, Vec<AccountInfo<'a>>) {
    use evm_loader_instructions::*;
    (
        create_evm_instruction_with_borsh(
            *evm_loader.key,
            &EvmInstruction::ExecuteTransaction {
                tx: ExecuteTransaction::Signed { tx: Some(tx) },
                fee_type: FeePayerType::Native,
            },
            vec![
                AccountMeta::new(*evm_state.key, false),
                AccountMeta::new(*sender.key, true),
            ],
        ),
        vec![evm_loader.clone(), evm_state.clone(), sender.clone()],
    )
}

fn make_evm_loader_big_tx_execute_ix<'a>(
    evm_loader: &AccountInfo<'a>,
    evm_state: &AccountInfo<'a>,
    sender: &AccountInfo<'a>,
    big_tx_storage: &AccountInfo<'a>,
) -> (Instruction, Vec<AccountInfo<'a>>) {
    use evm_loader_instructions::*;
    (
        create_evm_instruction_with_borsh(
            *evm_loader.key,
            &EvmInstruction::ExecuteTransaction {
                tx: ExecuteTransaction::Signed { tx: None },
                fee_type: FeePayerType::Native,
            },
            vec![
                AccountMeta::new(*evm_state.key, false),
                AccountMeta::new(*big_tx_storage.key, true),
                AccountMeta::new(*sender.key, true),
            ],
        ),
        vec![
            evm_loader.clone(),
            big_tx_storage.clone(),
            evm_state.clone(),
            sender.clone(),
        ],
    )
}

fn make_free_ownership_ix(owner: Pubkey) -> Instruction {
    use evm_loader_instructions::*;
    create_evm_instruction_with_borsh(
        solana_sdk::evm_loader::ID,
        &EvmInstruction::FreeOwnership {},
        vec![
            AccountMeta::new(solana_sdk::evm_state::ID, false),
            AccountMeta::new(owner, true),
        ],
    )
}

fn refund_native_fee(caller: &AccountInfo, payer: &AccountInfo, amount: u64) -> ProgramResult {
    **payer.try_borrow_mut_lamports()? =
        payer
            .lamports()
            .checked_sub(amount)
            .ok_or(ProgramError::from(
                GasStationError::InsufficientPayerBalance,
            ))?;
    **caller.try_borrow_mut_lamports()? = caller
        .lamports()
        .checked_add(amount)
        .ok_or(ProgramError::from(GasStationError::RefundOverflow))?;
    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;
    use solana_evm_loader_program::scope::evm;
    use solana_program::instruction::InstructionError::{Custom, IncorrectProgramId};
    use solana_program_test::{processor, ProgramTest};
    use solana_sdk::{
        account::{Account, ReadableAccount},
        signature::{Keypair, Signer},
        system_program,
        transaction::{Transaction, TransactionError::InstructionError},
        transport::TransportError::TransactionError,
    };

    const SECRET_KEY_DUMMY_ONES: [u8; 32] = [1; 32];
    const SECRET_KEY_DUMMY_TWOS: [u8; 32] = [2; 32];
    const TEST_CHAIN_ID: u64 = 0xdead;

    pub fn dummy_eth_tx(contract: evm::H160, input: Vec<u8>) -> evm_types::Transaction {
        let tx = evm::UnsignedTransaction {
            nonce: evm::U256::zero(),
            gas_price: evm::U256::zero(),
            gas_limit: evm::U256::zero(),
            action: evm::TransactionAction::Call(contract),
            value: evm::U256::zero(),
            input,
        }
        .sign(
            &evm::SecretKey::from_slice(&SECRET_KEY_DUMMY_ONES).unwrap(),
            Some(TEST_CHAIN_ID),
        );
        evm_types::Transaction {
            nonce: tx.nonce,
            gas_price: tx.gas_price,
            gas_limit: tx.gas_limit,
            action: evm_types::TransactionAction::Call(contract),
            value: tx.value,
            signature: evm_types::TransactionSignature {
                v: tx.signature.v,
                r: tx.signature.r,
                s: tx.signature.s
            },
            input: tx.input,
        }
    }

    #[tokio::test]
    async fn test_register_payer() {
        let program_id = Pubkey::new_unique();

        let mut program_test =
            ProgramTest::new("gas-station", program_id, processor!(process_instruction));

        let creator = Keypair::new();
        let storage = Keypair::new();
        program_test.add_account(
            creator.pubkey(),
            Account {
                lamports: 10000000,
                ..Account::default()
            },
        );
        program_test.add_account(
            storage.pubkey(),
            Account::new(10000000, 93, &program_id),
        );

        let (mut banks_client, _, recent_blockhash) = program_test.start().await;

        let (payer_key, _) =
            Pubkey::find_program_address(&[creator.pubkey().as_ref()], &program_id);
        let account_metas = vec![
            AccountMeta::new(creator.pubkey(), true),
            AccountMeta::new(storage.pubkey(), false),
            AccountMeta::new(payer_key, false),
            AccountMeta::new_readonly(solana_sdk::system_program::id(), false),
        ];
        let transfer_amount = 1000000;
        let ix = Instruction::new_with_borsh(
            program_id,
            &GasStationInstruction::RegisterPayer {
                owner: creator.pubkey(),
                transfer_amount,
                whitelist: vec![TxFilter::InputStartsWith {
                    contract: evm::Address::zero(),
                    input_prefix: vec![],
                }],
            },
            account_metas,
        );
        let mut transaction = Transaction::new_with_payer(&[ix], Some(&creator.pubkey()));
        transaction.sign(&[&creator], recent_blockhash);
        banks_client.process_transaction(transaction).await.unwrap();

        let account = banks_client.get_account(storage.pubkey()).await.unwrap().unwrap();
        assert_eq!(account.owner, program_id);
        assert_eq!(account.lamports, 10000000);
        assert_eq!(account.data.len(), 93);
        let mut data_slice: &[u8] = account.data();
        let payer: Payer = BorshDeserialize::deserialize(&mut data_slice).unwrap();
        assert_eq!(payer.payer, payer_key);
        assert_eq!(payer.owner, creator.pubkey());
        assert_eq!(payer.filters.len(), 1);
        assert_eq!(payer.filters[0], TxFilter::InputStartsWith {
            contract: evm::Address::zero(),
            input_prefix: vec![],
        });

        let rent = banks_client.get_rent().await.unwrap();
        let pda_account = banks_client.get_account(payer_key).await.unwrap().unwrap();
        assert_eq!(pda_account.owner, program_id);
        assert_eq!(pda_account.lamports, rent.minimum_balance(0) + transfer_amount);
    }

    #[tokio::test]
    async fn test_execute_tx() {
        let program_id = Pubkey::new_unique();
        let mut program_test =
            ProgramTest::new("gas-station", program_id, processor!(process_instruction));

        let user = Keypair::new();
        let owner = Keypair::new();
        let storage = Keypair::new();
        let (payer, _) = Pubkey::find_program_address(&[owner.pubkey().as_ref()], &program_id);
        program_test.add_account(
            user.pubkey(),
            Account::new(1000000, 0, &system_program::id()),
        );
        program_test.add_account(payer, Account::new(1000000, 0, &program_id));
        program_test.add_account(
            solana_sdk::evm_state::ID,
            solana_evm_loader_program::create_state_account(1000000).into(),
        );
        let payer_data = Payer {
            owner: owner.pubkey(),
            payer,
            filters: vec![TxFilter::InputStartsWith {
                contract: evm::Address::zero(),
                input_prefix: vec![],
            }],
        };
        let mut payer_bytes = vec![];
        BorshSerialize::serialize(&payer_data, &mut payer_bytes).unwrap();
        program_test.add_account(
            storage.pubkey(),
            Account {
                lamports: 10000000,
                owner: program_id,
                data: payer_bytes,
                ..Account::default()
            },
        );

        let (mut banks_client, _, recent_blockhash) = program_test.start().await;

        let account_metas = vec![
            AccountMeta::new(user.pubkey(), true),
            AccountMeta::new(storage.pubkey(), false),
            AccountMeta::new(payer, false),
            AccountMeta::new_readonly(solana_sdk::evm_loader::ID, false),
            AccountMeta::new(solana_sdk::evm_state::ID, false),
            AccountMeta::new_readonly(system_program::id(), false),
        ];
        let ix = Instruction::new_with_borsh(
            program_id,
            &GasStationInstruction::ExecuteWithPayer {
                tx: Some(dummy_eth_tx(evm::H160::zero(), vec![])),
            },
            account_metas,
        );
        let mut transaction = Transaction::new_with_payer(&[ix], Some(&user.pubkey()));
        transaction.sign(&[&user], recent_blockhash);
        banks_client.process_transaction(transaction).await.unwrap();
    }

    #[tokio::test]
    async fn test_execute_big_tx() {
        let program_id = Pubkey::new_unique();
        let mut program_test =
            ProgramTest::new("gas-station", program_id, processor!(process_instruction));

        let user = Keypair::new();
        let owner = Keypair::new();
        let storage = Keypair::new();
        let big_tx_storage = Keypair::new();
        let (payer, _) = Pubkey::find_program_address(&[owner.pubkey().as_ref()], &program_id);
        program_test.add_account(
            user.pubkey(),
            Account::new(1000000, 0, &system_program::id()),
        );
        program_test.add_account(payer, Account::new(1000000, 0, &program_id));
        program_test.add_account(
            solana_sdk::evm_state::ID,
            solana_evm_loader_program::create_state_account(1000000).into(),
        );
        let payer_data = Payer {
            owner: owner.pubkey(),
            payer,
            filters: vec![TxFilter::InputStartsWith {
                contract: evm::Address::zero(),
                input_prefix: vec![],
            }],
        };
        let mut payer_bytes = vec![];
        BorshSerialize::serialize(&payer_data, &mut payer_bytes).unwrap();
        program_test.add_account(
            storage.pubkey(),
            Account {
                lamports: 10000000,
                owner: program_id,
                data: payer_bytes,
                ..Account::default()
            },
        );
        let big_tx = dummy_eth_tx(evm::H160::zero(), vec![0; 1000]);
        let mut big_tx_bytes = vec![];
        BorshSerialize::serialize(&big_tx, &mut big_tx_bytes).unwrap();
        program_test.add_account(
            big_tx_storage.pubkey(),
            Account {
                lamports: 10000000,
                owner: solana_sdk::evm_loader::ID,
                data: big_tx_bytes,
                ..Account::default()
            },
        );

        let (mut banks_client, _, recent_blockhash) = program_test.start().await;
        let account_metas = vec![
            AccountMeta::new(user.pubkey(), true),
            AccountMeta::new(storage.pubkey(), false),
            AccountMeta::new(payer, false),
            AccountMeta::new_readonly(solana_sdk::evm_loader::ID, false),
            AccountMeta::new(solana_sdk::evm_state::ID, false),
            AccountMeta::new_readonly(system_program::id(), false),
            AccountMeta::new(big_tx_storage.pubkey(), true),
        ];
        let ix_no_big_tx_storage = Instruction::new_with_borsh(
            program_id,
            &GasStationInstruction::ExecuteWithPayer { tx: None },
            account_metas.split_last().unwrap().1.into(),
        );
        let ix = Instruction::new_with_borsh(
            program_id,
            &GasStationInstruction::ExecuteWithPayer { tx: None },
            account_metas,
        );
        // this will fail because neither evm tx nor big tx storage provided
        let mut transaction =
            Transaction::new_with_payer(&[ix_no_big_tx_storage], Some(&user.pubkey()));
        transaction.sign(&[&user], recent_blockhash);
        assert!(matches!(
            banks_client
                .process_transaction(transaction)
                .await
                .unwrap_err(),
            TransactionError(InstructionError(0, Custom(2)))
        ));
        let mut transaction = Transaction::new_with_payer(&[ix], Some(&user.pubkey()));
        transaction.sign(&[&user, &big_tx_storage], recent_blockhash);
        assert!(matches!(
            banks_client
                .process_transaction(transaction)
                .await
                .unwrap_err(),
            TransactionError(InstructionError(0, Custom(14))) // NotSupported
        ));
    }

    #[tokio::test]
    async fn test_invalid_storage_account_owner() {
        let program_id = Pubkey::new_unique();
        let mut program_test =
            ProgramTest::new("gas-station", program_id, processor!(process_instruction));

        let user = Keypair::new();
        let owner = Keypair::new();
        let storage = Keypair::new();
        let (payer, _) = Pubkey::find_program_address(&[owner.pubkey().as_ref()], &program_id);
        program_test.add_account(
            user.pubkey(),
            Account::new(1000000, 0, &system_program::id()),
        );
        program_test.add_account(payer, Account::new(1000000, 0, &program_id));
        program_test.add_account(
            solana_sdk::evm_state::ID,
            solana_evm_loader_program::create_state_account(1000000).into(),
        );
        let payer_data = Payer {
            owner: owner.pubkey(),
            payer,
            filters: vec![TxFilter::InputStartsWith {
                contract: evm::Address::zero(),
                input_prefix: vec![],
            }],
        };
        let mut payer_bytes = vec![];
        BorshSerialize::serialize(&payer_data, &mut payer_bytes).unwrap();
        program_test.add_account(
            storage.pubkey(),
            Account {
                lamports: 10000000,
                owner: system_program::id(),
                data: payer_bytes,
                ..Account::default()
            },
        );

        let (mut banks_client, _, recent_blockhash) = program_test.start().await;

        let account_metas = vec![
            AccountMeta::new(user.pubkey(), true),
            AccountMeta::new(storage.pubkey(), false),
            AccountMeta::new(payer, false),
            AccountMeta::new_readonly(solana_sdk::evm_loader::ID, false),
            AccountMeta::new(solana_sdk::evm_state::ID, false),
            AccountMeta::new_readonly(system_program::id(), false),
        ];
        let ix = Instruction::new_with_borsh(
            program_id,
            &GasStationInstruction::ExecuteWithPayer {
                tx: Some(dummy_eth_tx(evm::H160::zero(), vec![])),
            },
            account_metas,
        );
        let mut transaction = Transaction::new_with_payer(&[ix], Some(&user.pubkey()));
        transaction.sign(&[&user], recent_blockhash);
        assert!(matches!(
            banks_client
                .process_transaction(transaction)
                .await
                .unwrap_err(),
            TransactionError(InstructionError(0, IncorrectProgramId))
        ));
    }

    #[tokio::test]
    async fn test_invalid_storage_data() {
        let program_id = Pubkey::new_unique();
        let mut program_test =
            ProgramTest::new("gas-station", program_id, processor!(process_instruction));

        let user = Keypair::new();
        let owner = Keypair::new();
        let storage1 = Keypair::new();
        let storage2 = Keypair::new();
        let storage3 = Keypair::new();
        let (payer, _) = Pubkey::find_program_address(&[owner.pubkey().as_ref()], &program_id);
        program_test.add_account(
            user.pubkey(),
            Account::new(1000000, 0, &system_program::id()),
        );
        program_test.add_account(payer, Account::new(1000000, 0, &program_id));
        program_test.add_account(
            solana_sdk::evm_state::ID,
            solana_evm_loader_program::create_state_account(1000000).into(),
        );
        let short_payer_bytes = vec![0u8; 64];
        program_test.add_account(
            storage1.pubkey(),
            Account {
                lamports: 10000000,
                owner: program_id,
                data: short_payer_bytes.clone(), // data too short
                ..Account::default()
            },
        );
        program_test.add_account(
            storage2.pubkey(),
            Account {
                lamports: 10000000,
                owner: program_id,
                data: short_payer_bytes
                    .into_iter()
                    // this 4 bytes mean that the size of filter array is 1 but there's no data after
                    .chain([0, 0, 0, 1].into_iter())
                    .collect(),
                ..Account::default()
            },
        );
        let payer_data = Payer {
            owner: owner.pubkey(),
            payer,
            filters: vec![TxFilter::InputStartsWith {
                contract: evm::Address::zero(),
                input_prefix: vec![],
            }],
        };
        let mut valid_payer_bytes = vec![];
        BorshSerialize::serialize(&payer_data, &mut valid_payer_bytes).unwrap();
        program_test.add_account(
            storage3.pubkey(),
            Account {
                lamports: 10000000,
                owner: program_id,
                data: valid_payer_bytes
                    .into_iter()
                    // add 1 extra byte
                    .chain([0].into_iter())
                    .collect(),
                ..Account::default()
            },
        );

        let (mut banks_client, _, recent_blockhash) = program_test.start().await;

        for storage in [storage1, storage2, storage3] {
            let account_metas = vec![
                AccountMeta::new(user.pubkey(), true),
                AccountMeta::new(storage.pubkey(), false),
                AccountMeta::new(payer, false),
                AccountMeta::new_readonly(solana_sdk::evm_loader::ID, false),
                AccountMeta::new(solana_sdk::evm_state::ID, false),
                AccountMeta::new_readonly(system_program::id(), false),
            ];
            let ix = Instruction::new_with_borsh(
                program_id,
                &GasStationInstruction::ExecuteWithPayer {
                    tx: Some(dummy_eth_tx(evm::H160::zero(), vec![])),
                },
                account_metas,
            );
            let mut transaction = Transaction::new_with_payer(&[ix], Some(&user.pubkey()));
            transaction.sign(&[&user], recent_blockhash);
            assert!(matches!(
                banks_client
                    .process_transaction(transaction)
                    .await
                    .unwrap_err(),
                TransactionError(InstructionError(0, Custom(4)))
            ));
        }
    }

    #[tokio::test]
    async fn test_storage_not_initialized() {
        let program_id = Pubkey::new_unique();
        let mut program_test =
            ProgramTest::new("gas-station", program_id, processor!(process_instruction));

        let user = Keypair::new();
        let owner = Keypair::new();
        let storage = Keypair::new();
        let (payer, _) = Pubkey::find_program_address(&[owner.pubkey().as_ref()], &program_id);
        program_test.add_account(
            user.pubkey(),
            Account::new(1000000, 0, &system_program::id()),
        );
        program_test.add_account(payer, Account::new(1000000, 0, &program_id));
        program_test.add_account(
            solana_sdk::evm_state::ID,
            solana_evm_loader_program::create_state_account(1000000).into(),
        );
        let payer_bytes = vec![0u8; 93];
        program_test.add_account(
            storage.pubkey(),
            Account {
                lamports: 10000000,
                owner: program_id,
                data: payer_bytes,
                ..Account::default()
            },
        );

        let (mut banks_client, _, recent_blockhash) = program_test.start().await;

        let account_metas = vec![
            AccountMeta::new(user.pubkey(), true),
            AccountMeta::new(storage.pubkey(), false),
            AccountMeta::new(payer, false),
            AccountMeta::new_readonly(solana_sdk::evm_loader::ID, false),
            AccountMeta::new(solana_sdk::evm_state::ID, false),
            AccountMeta::new_readonly(system_program::id(), false),
        ];
        let ix = Instruction::new_with_borsh(
            program_id,
            &GasStationInstruction::ExecuteWithPayer {
                tx: Some(dummy_eth_tx(evm::H160::zero(), vec![])),
            },
            account_metas,
        );
        let mut transaction = Transaction::new_with_payer(&[ix], Some(&user.pubkey()));
        transaction.sign(&[&user], recent_blockhash);
        assert!(matches!(
            banks_client
                .process_transaction(transaction)
                .await
                .unwrap_err(),
            TransactionError(InstructionError(0, Custom(1)))
        ));
    }

    #[tokio::test]
    async fn test_payer_account_mismatch() {
        let program_id = Pubkey::new_unique();
        let mut program_test =
            ProgramTest::new("gas-station", program_id, processor!(process_instruction));

        let user = Keypair::new();
        let owner1 = Keypair::new();
        let owner2 = Keypair::new();
        let storage = Keypair::new();
        let (payer1, _) = Pubkey::find_program_address(&[owner1.pubkey().as_ref()], &program_id);
        let (payer2, _) = Pubkey::find_program_address(&[owner2.pubkey().as_ref()], &program_id);
        program_test.add_account(
            user.pubkey(),
            Account::new(1000000, 0, &system_program::id()),
        );
        program_test.add_account(payer1, Account::new(1000000, 0, &program_id));
        program_test.add_account(
            solana_sdk::evm_state::ID,
            solana_evm_loader_program::create_state_account(1000000).into(),
        );
        let payer_data = Payer {
            owner: owner1.pubkey(),
            payer: payer1,
            filters: vec![TxFilter::InputStartsWith {
                contract: evm::Address::zero(),
                input_prefix: vec![],
            }],
        };
        let mut payer_bytes = vec![];
        BorshSerialize::serialize(&payer_data, &mut payer_bytes).unwrap();
        program_test.add_account(
            storage.pubkey(),
            Account {
                lamports: 10000000,
                owner: program_id,
                data: payer_bytes,
                ..Account::default()
            },
        );

        let (mut banks_client, _, recent_blockhash) = program_test.start().await;

        let account_metas = vec![
            AccountMeta::new(user.pubkey(), true),
            AccountMeta::new(storage.pubkey(), false),
            AccountMeta::new(payer2, false),
            AccountMeta::new_readonly(solana_sdk::evm_loader::ID, false),
            AccountMeta::new(solana_sdk::evm_state::ID, false),
            AccountMeta::new_readonly(system_program::id(), false),
        ];
        let ix = Instruction::new_with_borsh(
            program_id,
            &GasStationInstruction::ExecuteWithPayer {
                tx: Some(dummy_eth_tx(evm::H160::zero(), vec![0; 4])),
            },
            account_metas,
        );
        let mut transaction = Transaction::new_with_payer(&[ix], Some(&user.pubkey()));
        transaction.sign(&[&user], recent_blockhash);
        assert!(matches!(
            banks_client
                .process_transaction(transaction)
                .await
                .unwrap_err(),
            TransactionError(InstructionError(0, Custom(10)))
        ));
    }

    #[tokio::test]
    async fn test_payer_filter_mismatch() {
        let program_id = Pubkey::new_unique();
        let mut program_test =
            ProgramTest::new("gas-station", program_id, processor!(process_instruction));

        let user = Keypair::new();
        let owner = Keypair::new();
        let storage = Keypair::new();
        let (payer, _) = Pubkey::find_program_address(&[owner.pubkey().as_ref()], &program_id);
        program_test.add_account(
            user.pubkey(),
            Account::new(1000000, 0, &system_program::id()),
        );
        program_test.add_account(payer, Account::new(1000000, 0, &program_id));
        program_test.add_account(
            solana_sdk::evm_state::ID,
            solana_evm_loader_program::create_state_account(1000000).into(),
        );
        let payer_data = Payer {
            owner: owner.pubkey(),
            payer,
            filters: vec![
                TxFilter::InputStartsWith {
                    contract: evm::Address::zero(),
                    input_prefix: vec![1; 4],
                },
                TxFilter::InputStartsWith {
                    contract: evm::Address::from([1u8; 20]),
                    input_prefix: vec![0; 4],
                },
            ],
        };
        let mut payer_bytes = vec![];
        BorshSerialize::serialize(&payer_data, &mut payer_bytes).unwrap();
        program_test.add_account(
            storage.pubkey(),
            Account {
                lamports: 10000000,
                owner: program_id,
                data: payer_bytes,
                ..Account::default()
            },
        );

        let (mut banks_client, _, recent_blockhash) = program_test.start().await;

        let account_metas = vec![
            AccountMeta::new(user.pubkey(), true),
            AccountMeta::new(storage.pubkey(), false),
            AccountMeta::new(payer, false),
            AccountMeta::new_readonly(solana_sdk::evm_loader::ID, false),
            AccountMeta::new(solana_sdk::evm_state::ID, false),
            AccountMeta::new_readonly(system_program::id(), false),
        ];
        let ix = Instruction::new_with_borsh(
            program_id,
            &GasStationInstruction::ExecuteWithPayer {
                tx: Some(dummy_eth_tx(evm::H160::zero(), vec![0; 4])),
            },
            account_metas,
        );
        let mut transaction = Transaction::new_with_payer(&[ix], Some(&user.pubkey()));
        transaction.sign(&[&user], recent_blockhash);
        assert!(matches!(
            banks_client
                .process_transaction(transaction)
                .await
                .unwrap_err(),
            TransactionError(InstructionError(0, Custom(11)))
        ));
    }

    #[tokio::test]
    async fn test_insufficient_payer_funds() {
        let program_id = Pubkey::new_unique();
        let mut program_test =
            ProgramTest::new("gas-station", program_id, processor!(process_instruction));

        let user = Keypair::new();
        let owner1 = Keypair::new();
        let owner2 = Keypair::new();
        let storage1 = Keypair::new();
        let storage2 = Keypair::new();
        let (payer1, _) = Pubkey::find_program_address(&[owner1.pubkey().as_ref()], &program_id);
        let (payer2, _) = Pubkey::find_program_address(&[owner2.pubkey().as_ref()], &program_id);
        program_test.add_account(
            user.pubkey(),
            Account::new(1000000, 0, &system_program::id()),
        );
        // Total lamports needed for successful execution: 42000 (evm call) + 10000 (native call refund)
        program_test.add_account(payer1, Account::new(51999, 0, &program_id));
        program_test.add_account(payer2, Account::new(41999, 0, &program_id));
        program_test.add_account(
            solana_sdk::evm_state::ID,
            solana_evm_loader_program::create_state_account(1000000).into(),
        );
        let mut payer_data = Payer {
            owner: owner1.pubkey(),
            payer: payer1,
            filters: vec![TxFilter::InputStartsWith {
                contract: evm::Address::zero(),
                input_prefix: vec![],
            }],
        };
        let mut payer_bytes = vec![];
        BorshSerialize::serialize(&payer_data, &mut payer_bytes).unwrap();
        program_test.add_account(
            storage1.pubkey(),
            Account {
                lamports: 10000000,
                owner: program_id,
                data: payer_bytes,
                ..Account::default()
            },
        );
        payer_data.owner = owner2.pubkey();
        payer_data.payer = payer2;
        let mut payer_bytes = vec![];
        BorshSerialize::serialize(&payer_data, &mut payer_bytes).unwrap();
        program_test.add_account(
            storage2.pubkey(),
            Account {
                lamports: 10000000,
                owner: program_id,
                data: payer_bytes,
                ..Account::default()
            },
        );

        let (mut banks_client, _, recent_blockhash) = program_test.start().await;

        let account_metas = vec![
            AccountMeta::new(user.pubkey(), true),
            AccountMeta::new(storage1.pubkey(), false),
            AccountMeta::new(payer1, false),
            AccountMeta::new_readonly(solana_sdk::evm_loader::ID, false),
            AccountMeta::new(solana_sdk::evm_state::ID, false),
            AccountMeta::new_readonly(system_program::id(), false),
        ];
        let ix = Instruction::new_with_borsh(
            program_id,
            &GasStationInstruction::ExecuteWithPayer {
                tx: Some(dummy_eth_tx(evm::H160::zero(), vec![])),
            },
            account_metas,
        );
        let mut transaction = Transaction::new_with_payer(&[ix], Some(&user.pubkey()));
        transaction.sign(&[&user], recent_blockhash);
        // This tx has funds for evm call but will fail on refund attempt
        assert!(matches!(
            banks_client
                .process_transaction(transaction)
                .await
                .unwrap_err(),
            TransactionError(InstructionError(0, Custom(3))) // GasStationError::InsufficientPayerBalance
        ));

        let account_metas = vec![
            AccountMeta::new(user.pubkey(), true),
            AccountMeta::new(storage2.pubkey(), false),
            AccountMeta::new(payer2, false),
            AccountMeta::new_readonly(solana_sdk::evm_loader::ID, false),
            AccountMeta::new(solana_sdk::evm_state::ID, false),
            AccountMeta::new_readonly(system_program::id(), false),
        ];
        let tx = evm::UnsignedTransaction {
            nonce: evm::U256::zero(),
            gas_price: evm::U256::zero(),
            gas_limit: evm::U256::zero(),
            action: evm::TransactionAction::Call(evm::H160::zero()),
            value: evm::U256::zero(),
            input: vec![],
        }
        .sign(
            &evm::SecretKey::from_slice(&SECRET_KEY_DUMMY_TWOS).unwrap(),
            Some(TEST_CHAIN_ID),
        );
        let tx = evm_types::Transaction {
            nonce: tx.nonce,
            gas_price: tx.gas_price,
            gas_limit: tx.gas_limit,
            action: evm_types::TransactionAction::Call(evm::H160::zero()),
            value: tx.value,
            signature: evm_types::TransactionSignature {
                v: tx.signature.v,
                r: tx.signature.r,
                s: tx.signature.s
            },
            input: tx.input,
        };
        let ix = Instruction::new_with_borsh(
            program_id,
            &GasStationInstruction::ExecuteWithPayer { tx: Some(tx) },
            account_metas,
        );
        let mut transaction = Transaction::new_with_payer(&[ix], Some(&user.pubkey()));
        transaction.sign(&[&user], recent_blockhash);
        // This tx will fail on evm side due to insufficient funds for evm transaction
        assert!(matches!(
            banks_client
                .process_transaction(transaction)
                .await
                .unwrap_err(),
            TransactionError(InstructionError(0, Custom(18))) // NativeAccountInsufficientFunds from evm_loader
        ));
    }

    #[tokio::test]
    async fn test_invalid_evm_accounts() {
        let program_id = Pubkey::new_unique();
        let mut program_test =
            ProgramTest::new("gas-station", program_id, processor!(process_instruction));

        let user = Keypair::new();
        let owner = Keypair::new();
        let storage = Keypair::new();
        let (payer, _) = Pubkey::find_program_address(&[owner.pubkey().as_ref()], &program_id);
        program_test.add_account(
            user.pubkey(),
            Account::new(1000000, 0, &system_program::id()),
        );
        program_test.add_account(payer, Account::new(1000000, 0, &program_id));
        program_test.add_account(
            solana_sdk::evm_state::ID,
            solana_evm_loader_program::create_state_account(1000000).into(),
        );
        let payer_data = Payer {
            owner: owner.pubkey(),
            payer,
            filters: vec![TxFilter::InputStartsWith {
                contract: evm::Address::zero(),
                input_prefix: vec![],
            }],
        };
        let mut payer_bytes = vec![];
        BorshSerialize::serialize(&payer_data, &mut payer_bytes).unwrap();
        program_test.add_account(
            storage.pubkey(),
            Account {
                lamports: 10000000,
                owner: program_id,
                data: payer_bytes,
                ..Account::default()
            },
        );

        let (mut banks_client, _, recent_blockhash) = program_test.start().await;

        let third_party_keypair = Keypair::new();
        let account_metas_invalid_evm_loader = vec![
            AccountMeta::new(user.pubkey(), true),
            AccountMeta::new(storage.pubkey(), false),
            AccountMeta::new(payer, false),
            AccountMeta::new_readonly(third_party_keypair.pubkey(), false),
            AccountMeta::new(solana_sdk::evm_state::ID, false),
            AccountMeta::new_readonly(system_program::id(), false),
        ];
        let ix = Instruction::new_with_borsh(
            program_id,
            &GasStationInstruction::ExecuteWithPayer {
                tx: Some(dummy_eth_tx(evm::H160::zero(), vec![])),
            },
            account_metas_invalid_evm_loader,
        );
        let mut transaction = Transaction::new_with_payer(&[ix], Some(&user.pubkey()));
        transaction.sign(&[&user], recent_blockhash);
        assert!(matches!(
            banks_client
                .process_transaction(transaction)
                .await
                .unwrap_err(),
            TransactionError(InstructionError(0, Custom(6))) // InvalidEvmLoader
        ));

        let account_metas_invalid_evm_state = vec![
            AccountMeta::new(user.pubkey(), true),
            AccountMeta::new(storage.pubkey(), false),
            AccountMeta::new(payer, false),
            AccountMeta::new_readonly(solana_sdk::evm_loader::ID, false),
            AccountMeta::new(third_party_keypair.pubkey(), false),
            AccountMeta::new_readonly(system_program::id(), false),
        ];
        let ix = Instruction::new_with_borsh(
            program_id,
            &GasStationInstruction::ExecuteWithPayer {
                tx: Some(dummy_eth_tx(evm::H160::zero(), vec![])),
            },
            account_metas_invalid_evm_state,
        );
        let mut transaction = Transaction::new_with_payer(&[ix], Some(&user.pubkey()));
        transaction.sign(&[&user], recent_blockhash);
        assert!(matches!(
            banks_client
                .process_transaction(transaction)
                .await
                .unwrap_err(),
            TransactionError(InstructionError(0, Custom(7))) // InvalidEvmLoader
        ));
    }

    #[tokio::test]
    async fn test_rent_exemption() {
        let program_id = Pubkey::new_unique();
        let mut program_test =
            ProgramTest::new("gas-station", program_id, processor!(process_instruction));

        let user = Keypair::new();
        let owner = Keypair::new();
        let storage = Keypair::new();
        let (payer, _) = Pubkey::find_program_address(&[owner.pubkey().as_ref()], &program_id);
        program_test.add_account(
            user.pubkey(),
            Account::new(1000000, 0, &system_program::id()),
        );
        // 890880 for rent exemption + 42000 for evm execution + 10000 refund = 942880 needed
        let payer_lamports = 942879;
        program_test.add_account(payer, Account::new(payer_lamports, 0, &program_id));
        program_test.add_account(
            solana_sdk::evm_state::ID,
            solana_evm_loader_program::create_state_account(1000000).into(),
        );
        let payer_data = Payer {
            owner: owner.pubkey(),
            payer,
            filters: vec![TxFilter::InputStartsWith {
                contract: evm::Address::zero(),
                input_prefix: vec![],
            }],
        };
        let mut payer_bytes = vec![];
        BorshSerialize::serialize(&payer_data, &mut payer_bytes).unwrap();
        program_test.add_account(
            storage.pubkey(),
            Account {
                lamports: 10000000,
                owner: program_id,
                data: payer_bytes,
                ..Account::default()
            },
        );

        let (mut banks_client, _, recent_blockhash) = program_test.start().await;

        let account_metas = vec![
            AccountMeta::new(user.pubkey(), true),
            AccountMeta::new(storage.pubkey(), false),
            AccountMeta::new(payer, false),
            AccountMeta::new_readonly(solana_sdk::evm_loader::ID, false),
            AccountMeta::new(solana_sdk::evm_state::ID, false),
            AccountMeta::new_readonly(system_program::id(), false),
        ];
        let ix = Instruction::new_with_borsh(
            program_id,
            &GasStationInstruction::ExecuteWithPayer {
                tx: Some(dummy_eth_tx(evm::H160::zero(), vec![])),
            },
            account_metas,
        );
        let mut transaction = Transaction::new_with_payer(&[ix], Some(&user.pubkey()));
        transaction.sign(&[&user], recent_blockhash);
        assert!(matches!(
            banks_client
                .process_transaction(transaction)
                .await
                .unwrap_err(),
            TransactionError(InstructionError(0, Custom(9))) // NotRentExempt
        ));
    }
}
use evm::backend::Log;
use primitive_types::{H160, H256, U256};
use serde::{Deserialize, Serialize};

use super::transactions::TransactionReceipt;
use super::version_map::Map;

/// Vivinity value of a memory backend.
#[derive(Default, Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MemoryVicinity {
    /// Gas price.
    pub gas_price: U256,
    /// Origin.
    pub origin: H160,
    /// Chain ID.
    pub chain_id: U256,
    /// Environmental block hashes.
    pub block_hashes: Vec<H256>,
    /// Environmental block number.
    pub block_number: U256,
    /// Environmental coinbase.
    pub block_coinbase: H160,
    /// Environmental block timestamp.
    pub block_timestamp: U256,
    /// Environmental block difficulty.
    pub block_difficulty: U256,
    /// Environmental block gas limit.
    pub block_gas_limit: U256,
}

#[derive(Default, Clone, Debug, Eq, PartialEq)]
pub struct AccountState {
    /// Account nonce.
    pub nonce: U256,
    /// Account balance.
    pub balance: U256,
    /// Account code.
    pub code: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct EvmState {
    pub(crate) accounts: Map<H160, AccountState>,
    // Store every account storage at single place, to use power of versioned map.
    // This allows us to save only changed data.
    pub(crate) storage: Map<(H160, H256), H256>,
    pub(crate) txs_receipts: Map<H256, TransactionReceipt>,
    pub(crate) txs_in_block: Map<u64, H256>,
    pub(crate) logs: Vec<Log>,
}

impl Default for EvmState {
    fn default() -> Self {
        Self::new_not_forget_to_deserialize_later()
    }
}

impl EvmState {
    pub fn new() -> Self {
        Self {
            accounts: Map::new(),
            storage: Map::new(),
            txs_receipts: Map::new(),
            txs_in_block: Map::new(),
            logs: Vec::new(),
        }
    }

    pub fn freeze(&mut self) {
        self.accounts.freeze();
        self.storage.freeze();
        self.txs_receipts.freeze();
    }

    pub fn try_fork(&self) -> Option<Self> {
        let accounts = self.accounts.try_fork()?;
        let storage = self.storage.try_fork()?;
        let txs_receipts = self.txs_receipts.try_fork()?;
        let txs_in_block = self.txs_in_block.try_fork()?;

        Some(Self {
            accounts,
            storage,
            txs_receipts,
            txs_in_block,
            logs: vec![],
        })
    }
}

impl EvmState {
    // TODO: Replace it by persistent storage
    pub fn new_not_forget_to_deserialize_later() -> Self {
        EvmState {
            accounts: Map::new(),
            storage: Map::new(),
            txs_receipts: Map::new(),
            txs_in_block: Map::new(),
            logs: Vec::new(),
        }
    }

    pub fn get_tx_receipt_by_hash(&self, tx_hash: H256) -> Option<TransactionReceipt> {
        self.txs_receipts.get(&tx_hash).cloned()
    }

    pub fn get_account(&self, address: H160) -> Option<AccountState> {
        self.accounts.get(&address).cloned()
    }

    pub fn basic(&self, account: H160) -> AccountState {
        self.get_account(account).unwrap_or_default()
    }

    pub fn get_storage(&self, address: H160, index: H256) -> Option<H256> {
        self.storage.get(&(address, index)).cloned()
    }

    pub fn swap_commit(&mut self, mut updated: Self) {
        // TODO: Assert that updated is newer than current state.
        std::mem::swap(self, &mut updated);
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use primitive_types::{H160, H256, U256};
    use rand::rngs::mock::StepRng;
    use rand::Rng;
    use std::collections::{BTreeMap, BTreeSet};
    const RANDOM_INCR: u64 = 734512;
    const MAX_SIZE: usize = 32; // Max size of test collections.

    const SEED: u64 = 123;

    impl EvmState {
        pub(crate) fn testing_default() -> EvmState {
            EvmState {
                accounts: Default::default(),
                storage: Default::default(),
                txs_receipts: Default::default(),
                txs_in_block: Default::default(),
                logs: Default::default(),
            }
        }
    }

    fn generate_account_by_seed(seed: u64) -> AccountState {
        let mut rng = StepRng::new(seed * RANDOM_INCR + seed, RANDOM_INCR);
        let nonce: [u8; 32] = rng.gen();
        let balance: [u8; 32] = rng.gen();

        let nonce = U256::from_little_endian(&nonce);
        let balance = U256::from_little_endian(&balance);
        let code_len: usize = rng.gen_range(0, MAX_SIZE);
        let code = (0..code_len).into_iter().map(|_| rng.gen()).collect();

        AccountState {
            nonce,
            balance,
            code,
        }
    }

    fn generate_accounts_state(seed: u64, accounts: &[H160]) -> BTreeMap<H160, AccountState> {
        let mut rng = StepRng::new(seed, RANDOM_INCR);
        let mut map = BTreeMap::new();
        for account in accounts {
            let seed = rng.gen();
            let state = generate_account_by_seed(seed);
            map.insert(*account, state);
        }
        map
    }

    fn generate_storage(seed: u64, accounts: &[H160]) -> BTreeMap<(H160, H256), H256> {
        let mut rng = StepRng::new(seed, RANDOM_INCR);

        let mut map = BTreeMap::new();

        for acc in accounts {
            let storage_len = rng.gen_range(0, MAX_SIZE);
            for _ in 0..storage_len {
                let addr: [u8; 32] = rng.gen();
                let data: [u8; 32] = rng.gen();

                let addr = H256::from_slice(&addr);
                let data = H256::from_slice(&data);
                map.insert((*acc, addr), data);
            }
        }
        map
    }

    fn generate_accounts_addresses(seed: u64, count: usize) -> Vec<H160> {
        let mut rng = StepRng::new(seed, RANDOM_INCR);
        (0..count)
            .into_iter()
            .map(|_| H256::from_slice(&rng.gen::<[u8; 32]>()).into())
            .collect()
    }

    fn to_state_diff<K: Ord, Mv>(
        inserts: BTreeMap<K, Mv>,
        removes: BTreeSet<K>,
    ) -> BTreeMap<K, Option<Mv>> {
        let len = inserts.len() + removes.len();
        let mut map = BTreeMap::new();
        for insert in inserts {
            assert!(
                map.insert(insert.0, Some(insert.1)).is_none(),
                "double insert"
            );
        }

        for insert in removes {
            assert!(
                map.insert(insert, None).is_none(),
                "delete after insert is not allowed"
            );
        }
        assert_eq!(map.len(), len, "length differ from inserts + removes len");
        map
    }

    fn save_state(
        state: &mut EvmState,
        accounts: &BTreeMap<H160, Option<AccountState>>,
        storage: &BTreeMap<(H160, H256), Option<H256>>,
    ) {
        for account in accounts {
            match &account.1 {
                Some(v) => state.accounts.insert(*account.0, v.clone()),
                None => state.accounts.remove(*account.0),
            }
        }

        for s in storage {
            match &s.1 {
                Some(v) => state.storage.insert(*s.0, *v),
                None => state.storage.remove(*s.0),
            }
        }
    }

    fn assert_state(
        state: &EvmState,
        accounts: &BTreeMap<H160, Option<AccountState>>,
        storage: &BTreeMap<(H160, H256), Option<H256>>,
    ) {
        for account in accounts {
            assert_eq!(state.accounts.get(account.0), account.1.as_ref())
        }

        for s in storage {
            assert_eq!(state.storage.get(s.0), s.1.as_ref())
        }
    }

    #[test]
    fn add_two_accounts_check_helpers() {
        let accounts = generate_accounts_addresses(SEED, 2);

        let storage = generate_storage(SEED, &accounts);
        let accounts_state = generate_accounts_state(SEED, &accounts);
        let storage_diff = to_state_diff(storage, BTreeSet::new());
        let accounts_state_diff = to_state_diff(accounts_state, BTreeSet::new());

        let mut evm_state = EvmState::testing_default();
        assert_eq!(evm_state.basic(H160::random()).balance, U256::from(0));
        save_state(&mut evm_state, &accounts_state_diff, &storage_diff);

        assert_state(&evm_state, &accounts_state_diff, &storage_diff);
    }

    #[test]
    fn fork_add_remove_accounts() {
        let accounts = generate_accounts_addresses(SEED, 10);

        let storage = generate_storage(SEED, &accounts);
        let accounts_state = generate_accounts_state(SEED, &accounts);
        let storage_diff = to_state_diff(storage, BTreeSet::new());
        let accounts_state_diff = to_state_diff(accounts_state, BTreeSet::new());

        let mut evm_state = EvmState::testing_default();
        save_state(&mut evm_state, &accounts_state_diff, &storage_diff);
        evm_state.freeze();

        assert_state(&evm_state, &accounts_state_diff, &storage_diff);

        let mut new_evm_state = evm_state.try_fork().unwrap();
        assert_state(&new_evm_state, &accounts_state_diff, &storage_diff);

        let new_accounts = generate_accounts_addresses(SEED + 1, 2);
        let new_accounts_state = generate_accounts_state(SEED + 1, &new_accounts);
        let removed_accounts: BTreeSet<_> = accounts[0..2].iter().copied().collect();
        let new_accounts_state_diff = to_state_diff(new_accounts_state, removed_accounts);

        save_state(
            &mut new_evm_state,
            &new_accounts_state_diff,
            &BTreeMap::new(),
        );

        assert_state(&new_evm_state, &new_accounts_state_diff, &BTreeMap::new());
    }
}
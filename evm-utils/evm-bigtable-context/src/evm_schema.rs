use sha3::{Digest, Keccak256};
use std::collections::HashMap;

// keys;
use crate::{
    bigtable::{BigTable, BigtableProvider},
    memory::{SerializedMap, SerializedMapProvider},
};
use evm_state::{BlockNum, H160, H256};

use std::ops::RangeInclusive;

//values
use super::*;
use evm_state::types::{Account, Code};
use log::*;
use memory::typed::{MemMap, MemMapProvider};

impl FixedSizedKey for H160 {
    const SIZE: usize = 20;

    fn write_ord_bytes(&self, buffer: &mut [u8]) {
        buffer.copy_from_slice(self.as_bytes())
    }

    fn from_buffer_ord_bytes(buffer: &[u8]) -> Self {
        H160::from_slice(&buffer)
    }
}

impl FixedSizedKey for H256 {
    const SIZE: usize = 32;

    fn write_ord_bytes(&self, buffer: &mut [u8]) {
        buffer.copy_from_slice(self.as_bytes())
    }

    fn from_buffer_ord_bytes(buffer: &[u8]) -> Self {
        H256::from_slice(&buffer)
    }
}

impl FixedSizedKey for BlockNum {
    const SIZE: usize = std::mem::size_of::<BlockNum>();

    fn write_ord_bytes(&self, buffer: &mut [u8]) {
        buffer.copy_from_slice(&self.to_be_bytes())
    }
    fn from_buffer_ord_bytes(buffer: &[u8]) -> Self {
        let mut bytes = [0; 8];
        bytes.copy_from_slice(buffer);
        BlockNum::from_be_bytes(bytes)
    }
}

impl RangeValue for BlockNum {
    fn min() -> Self {
        BlockNum::MIN
    }
    fn max() -> Self {
        BlockNum::MAX
    }
}

impl RangeValue for H160 {
    fn min() -> Self {
        H160::repeat_byte(0x00)
    }
    fn max() -> Self {
        H160::repeat_byte(0xff)
    }
}

impl RangeValue for H256 {
    fn min() -> Self {
        H256::repeat_byte(0x00)
    }
    fn max() -> Self {
        H256::repeat_byte(0xff)
    }
}

pub type HashedAddress = H256;
pub fn hash_address(address: H160) -> HashedAddress {
    H256::from_slice(Keccak256::digest(address.as_bytes()).as_slice())
}

pub fn hash_index(idx: H256) -> HashedAddress {
    H256::from_slice(Keccak256::digest(idx.as_bytes()).as_slice())
}

//
// Versioned account storage
//
// Tables:
// 1. Accounts (evm-accounts)
// H160 -> Account
//
// Primary index: H160
// Secondary index: BlockNum

//
// 2. AccountCode (evm-accounts-code)
// Immutable (H256 -> Code) or (H160 -> Code)
//
// Primary index: H160
//

// 3. AccountStorage (evm-accounts-storage)
// H160 -> (H256 -> B)
//
// Primary Index: H160
// Secondary Index: H256
// Third Index: BlockNum
//

#[derive(Debug, Clone)]
pub struct EvmSchema<AccountMap, CodeMap, StorageMap> {
    accounts: AccountMap,
    code: CodeMap,
    storage: StorageMap,
}

impl
    EvmSchema<
        MemMap<(HashedAddress, BlockNum), Account>,
        MemMap<HashedAddress, Code>,
        MemMap<(HashedAddress, H256, BlockNum), H256>,
    >
{
    pub fn new_mem_tmp(provider: &mut MemMapProvider) -> Result<Self> {
        Ok(Self {
            accounts: provider.take_map_shared(String::from("evm-accounts"))?,
            storage: provider.take_map_shared(String::from("evm-account-storage"))?,
            code: provider.take_map_shared(String::from("evm-account-code"))?,
        })
    }
}

impl
    EvmSchema<
        SerializedMap<(HashedAddress, BlockNum), Account>,
        SerializedMap<HashedAddress, Code>,
        SerializedMap<(HashedAddress, H256, BlockNum), H256>,
    >
{
    pub fn new_serialized_tmp(provider: &mut SerializedMapProvider) -> Result<Self> {
        Ok(Self {
            accounts: provider.take_map_shared(String::from("evm-accounts"))?,
            storage: provider.take_map_shared(String::from("evm-account-storage"))?,
            code: provider.take_map_shared(String::from("evm-account-code"))?,
        })
    }
}

impl
    EvmSchema<
        Arc<BigTable<(HashedAddress, BlockNum), Account>>,
        Arc<BigTable<HashedAddress, Code>>,
        Arc<BigTable<(HashedAddress, H256, BlockNum), H256>>,
    >
{
    pub fn new_bigtable(provider: &mut BigtableProvider) -> Result<Self> {
        Ok(Self {
            accounts: provider.take_map_shared(String::from("evm-accounts"))?,
            storage: provider.take_map_shared(String::from("evm-account-storage"))?,
            code: provider.take_map_shared(String::from("evm-account-code"))?,
        })
    }
}

#[doc(hidden)]
impl<AccountMap, CodeMap, StorageMap> EvmSchema<AccountMap, CodeMap, StorageMap>
// declaration
where
    // account
    AccountMap: AsyncMap<K = (HashedAddress, BlockNum)>,
    AccountMap: AsyncMap<V = Account>,
    AccountMap: AsyncMapSearch,
    // code
    CodeMap: AsyncMap<K = HashedAddress>,
    CodeMap: AsyncMap<V = Code>,
    // storage
    StorageMap: AsyncMap<K = (HashedAddress, H256, BlockNum)>,
    StorageMap: AsyncMap<V = H256>,
    StorageMap: AsyncMapSearch,
{
    pub fn find_last_account(&self, key: H160, last_block_num: BlockNum) -> Option<Account> {
        debug!(
            "Searching for account state = {:?}, since_block = {}",
            key, last_block_num
        );
        self.find_last_account_hashed(hash_address(key), last_block_num)
    }
    pub fn find_code(&self, key: H160) -> Option<Code> {
        debug!("Searching for account code = {:?}", key);
        let code = self.find_code_hashed(hash_address(key));
        // debug!("Searching for account code = {:?}, code = {:?}", key, code);
        code
    }
    pub fn find_storage(&self, key: H160, index: H256, last_block_num: BlockNum) -> Option<H256> {
        debug!(
            "Searching for storage = {:?}, {:?}, since_block = {}",
            key, index, last_block_num
        );
        self.find_storage_hashed(hash_address(key), hash_index(index), last_block_num)
    }
    pub fn push_account_change(
        &self,
        key: H160,
        block_num: BlockNum,
        state: Account,
        code: Option<Code>,
        storage_updates: HashMap<H256, H256>,
    ) {
        let mut new_storage = HashMap::new();
        for (k, v) in storage_updates {
            new_storage.insert(hash_index(k), v);
        }
        self.push_account_change_hashed(hash_address(key), block_num, state, code, new_storage)
    }

    pub fn find_last_account_hashed(
        &self,
        key: HashedAddress,
        last_block_num: BlockNum,
    ) -> Option<Account> {
        debug!(
            "Searching for account state = {:?}, since_block = {}",
            key, last_block_num
        );
        self.accounts.search_rev(
            (key,),
            last_block_num,
            None,
            |_, ((addr, block), account)| {
                debug_assert_eq!(key, addr);
                debug!(
                    "Found account at block = {}, account = {:?}",
                    block, account
                );
                ControlFlow::Break(Some(account))
            },
        )
    }
    pub fn find_code_hashed(&self, key: HashedAddress) -> Option<Code> {
        self.code.get(&key)
    }
    pub fn find_storage_hashed(
        &self,
        key: HashedAddress,
        index: H256,
        last_block_num: BlockNum,
    ) -> Option<H256> {
        debug!(
            "Searching for account storage = ({:?}, {:?}), since_block = {}",
            key, index, last_block_num
        );
        self.storage.search_rev(
            (key, index),
            last_block_num,
            None,
            |_, ((addr, index_found, block), account)| {
                debug_assert_eq!(key, addr);
                debug_assert_eq!(index, index_found);
                debug!(
                    "Found account storage at block = {}, account = {:?}",
                    block, account
                );
                ControlFlow::Break(Some(account))
            },
        )
    }
    pub fn push_account_change_hashed(
        &self,
        key: HashedAddress,
        block_num: BlockNum,
        state: Account,
        code: Option<Code>,
        storage_updates: HashMap<H256, H256>,
    ) {
        self.accounts.set((key, block_num), state);
        if let Some(code) = code {
            self.code.set(key, code);
        }
        for (storage_idx, storage_value) in storage_updates {
            self.storage
                .set((key, storage_idx, block_num), storage_value);
        }
    }
}

impl<AccountMap, CodeMap, StorageMap> EvmSchema<AccountMap, CodeMap, StorageMap>
// declaration
where
    // account
    AccountMap: AsyncMap<K = (HashedAddress, BlockNum)>,
    AccountMap: AsyncMap<V = Account>,
    AccountMap: AsyncMapSearch,
    // code
    CodeMap: AsyncMap<K = HashedAddress>,
    CodeMap: AsyncMap<V = Code>,
    // storage
    StorageMap: AsyncMap<K = (HashedAddress, H256, BlockNum)>,
    StorageMap: AsyncMap<V = H256>,
    StorageMap: AsyncMapSearch,
{
    // Return None, if results of queries is different;
    // Return true if all data in rage was found;
    // Return false if all data in rage not found;
    pub fn check_account_in_block_range(
        &self,
        key: H160,
        account: &Account,
        blocks: RangeInclusive<BlockNum>,
    ) -> Option<bool> {
        blocks.into_iter().fold(None, |init, b| {
            let new_account = self.find_last_account(key, b).unwrap_or_default();
            let new_result = new_account == *account;
            init.filter(|v| *v == new_result)
                .or_else(|| Some(new_result))
        })
    }

    pub fn check_storage_in_block_range(
        &self,
        key: H160,
        storage: &HashMap<H256, H256>,
        blocks: RangeInclusive<BlockNum>,
    ) -> Option<bool> {
        blocks.into_iter().fold(None, |mut init, b| {
            for (idx, data_before) in storage {
                let data = self.find_storage(key, *idx, b).unwrap_or_default();
                let new_result = data == *data_before;
                init = init
                    .filter(|v| *v == new_result)
                    .or_else(|| Some(new_result))
            }
            init
        })
    }
}

#[cfg(test)]
mod test {

    use crate::memory::SerializedMapProvider;

    use super::*;

    impl_trait_test_for_type! {test_h160_key => H160}
    impl_trait_test_for_type! {test_H256_key => H256}
    impl_trait_test_for_type! {test_BlockNum_key => BlockNum}
    impl_trait_test_for_type! {test_account_key => (H160,BlockNum)}
    impl_trait_test_for_type! {test_account_key_hashed => (H256,BlockNum)}
    impl_trait_test_for_type! {test_account_storage_key => (H160,H256, BlockNum)}

    fn create_storage_from_u8<I: Iterator<Item = (u8, u8)>>(i: I) -> HashMap<H256, H256> {
        i.map(|(k, v)| (H256::repeat_byte(k), H256::repeat_byte(v)))
            .collect()
    }

    #[test]
    fn insert_and_find_account() {
        let mut mem_provider = MemMapProvider::default();
        let evm_schema = EvmSchema::new_mem_tmp(&mut mem_provider).unwrap();

        let account_key = H160::repeat_byte(0x37);
        let account = Account {
            nonce: 1.into(),
            balance: 2.into(),
            storage_root: H256::repeat_byte(0x2),
            code_hash: H256::repeat_byte(0x3),
        };
        let some_code = Code::new(vec![1, 2, 3, 4]);

        let block = 0;

        let storage_updates = create_storage_from_u8((0..5).into_iter().map(|i| (i, 0xff)));
        evm_schema.push_account_change(
            account_key,
            block,
            account.clone(),
            some_code.clone().into(),
            storage_updates,
        );

        let block = 1;

        let mut account_update = account.clone();
        account_update.balance = 1231.into();
        account_update.nonce = 7.into();

        let storage_updates = create_storage_from_u8((5..7).into_iter().map(|i| (i, 0xee)));
        evm_schema.push_account_change(
            account_key,
            block,
            account_update.clone(),
            None,
            storage_updates,
        );

        assert!(evm_schema
            .check_account_in_block_range(account_key, &account_update, 1..=4)
            .unwrap());

        assert!(!evm_schema
            .check_account_in_block_range(account_key, &account_update, 0..=0)
            .unwrap());

        assert!(evm_schema
            .check_account_in_block_range(account_key, &account, 0..=0)
            .unwrap());

        assert!(!evm_schema
            .check_account_in_block_range(account_key, &account, 1..=4)
            .unwrap());

        assert_eq!(
            evm_schema.find_code(account_key).unwrap_or_default(),
            some_code
        );
    }

    #[test]
    fn insert_and_find_account_serialized() {
        let mut mem_provider = SerializedMapProvider::default();
        let evm_schema = EvmSchema::new_serialized_tmp(&mut mem_provider).unwrap();

        let account_key = H160::repeat_byte(0x37);
        let account = Account {
            nonce: 1.into(),
            balance: 2.into(),
            storage_root: H256::repeat_byte(0x2),
            code_hash: H256::repeat_byte(0x3),
        };
        let some_code = Code::new(vec![1, 2, 3, 4]);

        let block = 0;

        let storage_updates = create_storage_from_u8((0..5).into_iter().map(|i| (i, 0xff)));
        evm_schema.push_account_change(
            account_key,
            block,
            account.clone(),
            some_code.clone().into(),
            storage_updates.clone(),
        );

        let block = 1;

        let mut account_update = account.clone();
        account_update.balance = 1231.into();
        account_update.nonce = 7.into();

        let storage_updates2 = create_storage_from_u8((5..7).into_iter().map(|i| (i, 0xee)));
        evm_schema.push_account_change(
            account_key,
            block,
            account_update.clone(),
            None,
            storage_updates2.clone(),
        );

        assert!(evm_schema
            .check_account_in_block_range(account_key, &account_update, 1..=4)
            .unwrap());

        assert!(!evm_schema
            .check_account_in_block_range(account_key, &account_update, 0..=0)
            .unwrap());

        assert!(evm_schema
            .check_account_in_block_range(account_key, &account, 0..=0)
            .unwrap());

        assert!(!evm_schema
            .check_account_in_block_range(account_key, &account, 1..=4)
            .unwrap());

        assert!(evm_schema
            .check_storage_in_block_range(account_key, &storage_updates2, 1..=4)
            .unwrap());

        assert!(!evm_schema
            .check_storage_in_block_range(account_key, &storage_updates2, 0..=0)
            .unwrap());

        assert!(evm_schema
            .check_storage_in_block_range(account_key, &storage_updates, 0..=0)
            .unwrap());

        // pervious storage should be saved
        assert!(evm_schema
            .check_storage_in_block_range(account_key, &storage_updates, 1..=4)
            .unwrap());

        assert_eq!(
            evm_schema.find_code(account_key).unwrap_or_default(),
            some_code
        );
    }
}
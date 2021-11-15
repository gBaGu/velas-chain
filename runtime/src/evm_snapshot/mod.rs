use std::{borrow::Borrow, marker::PhantomData, path::Path};

use anyhow::{anyhow, ensure, Result};

use log::*;

use evm_state::storage::{self, rocksdb, Storage, SubStorage};
use evm_state::{
    types::{Account, Code},
    H256, U256,
};
use rlp::{Decodable, Rlp};
use triedb::merkle::nibble::NibbleVec;
use triedb::{
    empty_trie_hash,
    merkle::{MerkleNode, MerkleValue},
};

pub fn copy_and_purge(src: &Path, destination: &Path, root: H256) -> Result<(), anyhow::Error> {
    let storage = Storage::open_persistent(src)?;
    assert!(storage.check_root_exist(root));
    let destination = Storage::open_persistent(destination)?;

    let source = storage.clone();
    let streamer = inspectors::streamer::AccountsStreamer {
        source,
        destination,
    };
    let walker = Walker::new(storage, streamer);
    walker.traverse(Default::default(), root)
}

pub trait Inspector<T> {
    fn inspect_raw<Data: AsRef<[u8]>>(&self, _: H256, _: &Data) -> Result<bool>;
    fn inspect_typed(&self, _: Vec<u8>, _: &T) -> Result<()>;
}

pub struct Walker<DB, T, I> {
    db: DB,
    pub inspector: I,
    _data: PhantomData<T>,
}

impl<DB, T, I> Walker<DB, T, I> {
    pub fn new(db: DB, inspector: I) -> Self {
        Self {
            db,
            inspector,
            _data: PhantomData,
        }
    }
}

impl<DB, T, I> Walker<DB, T, I>
where
    DB: Borrow<rocksdb::DB> + Sync,
    T: Decodable + Sync,
    I: Inspector<T> + Sync,
{
    pub fn traverse(&self, nimble: NibbleVec, hash: H256) -> Result<()> {
        debug!("traversing {:?} ...", hash);
        if hash != empty_trie_hash() {
            let db = self.db.borrow();
            let bytes = db
                .get(hash)?
                .ok_or_else(|| anyhow!("hash {:?} not found in database"))?;
            trace!("raw bytes: {:?}", bytes);

            self.inspector.inspect_raw(hash, &bytes)?;

            let rlp = Rlp::new(bytes.as_slice());
            trace!("rlp: {:?}", rlp);
            let node = MerkleNode::decode(&rlp)?;
            debug!("node: {:?}", node);

            self.process_node(nimble, &node)?;
        } else {
            debug!("skip empty trie");
        }

        Ok(())
    }

    fn process_node(&self, mut nimble: NibbleVec, node: &MerkleNode) -> Result<()> {
        trace!("node: {:?}", node);
        match node {
            MerkleNode::Leaf(nibbles, data) => {
                nimble.extend_from_slice(&*nibbles);
                self.process_data(nimble, data)
            }
            MerkleNode::Extension(nibbles, value) => {
                nimble.extend_from_slice(&*nibbles);
                self.process_value(nimble, &value)
            }
            MerkleNode::Branch(values, mb_data) => {
                // lack of copy on result, forces setting array manually
                let mut values_result = [
                    None, None, None, None, None, None, None, None, None, None, None, None, None,
                    None, None, None,
                ];
                let result = rayon::scope(|s| {
                    for (id, (value, result)) in
                        values.into_iter().zip(&mut values_result).enumerate()
                    {
                        let mut cloned_nimble = nimble.clone();
                        cloned_nimble.push(id.into());
                        s.spawn(move |_| *result = Some(self.process_value(cloned_nimble, value)));
                    }
                    if let Some(data) = mb_data {
                        self.process_data(nimble, data)
                    } else {
                        Ok(())
                    }
                });
                for result in values_result {
                    result.unwrap()?
                }
                result
            }
        }
    }

    fn process_data(&self, nimble: NibbleVec, data: &[u8]) -> Result<()> {
        let rlp = Rlp::new(data);
        trace!("rlp: {:?}", rlp);
        let t = T::decode(&rlp)?;
        let key = triedb::merkle::nibble::into_key(&nimble);
        // TODO: debug T
        self.inspector.inspect_typed(key, &t)?;
        Ok(())
    }

    fn process_value(&self, nimble: NibbleVec, value: &MerkleValue) -> Result<()> {
        match value {
            MerkleValue::Empty => Ok(()),
            MerkleValue::Full(node) => self.process_node(nimble, &node),
            MerkleValue::Hash(hash) => self.traverse(nimble, *hash),
        }
    }
}

pub mod inspectors {
    use super::*;

    pub mod memorizer {
        use super::*;
        use dashmap::DashSet;
        use std::sync::atomic::{AtomicUsize, Ordering};

        #[derive(Default)]
        pub struct AccountsKeysCollector {
            pub trie_keys: DashSet<H256>,
            pub data_size: AtomicUsize,
            pub storage_roots: DashSet<H256>,
            pub code_hashes: DashSet<H256>,
        }

        impl Inspector<Account> for AccountsKeysCollector {
            fn inspect_raw<Data: AsRef<[u8]>>(&self, key: H256, data: &Data) -> Result<bool> {
                let is_new_key = self.trie_keys.insert(key);
                if is_new_key {
                    self.data_size
                        .fetch_add(data.as_ref().len(), Ordering::Relaxed);
                }
                Ok(is_new_key)
            }
            fn inspect_typed(&self, _: Vec<u8>, account: &Account) -> Result<()> {
                self.storage_roots.insert(account.storage_root);
                self.code_hashes.insert(account.code_hash);
                Ok(())
            }
        }

        #[derive(Default)]
        pub struct StoragesKeysCollector {
            pub trie_keys: DashSet<H256>,
            pub data_size: AtomicUsize,
        }

        impl Inspector<U256> for StoragesKeysCollector {
            fn inspect_raw<Data: AsRef<[u8]>>(&self, key: H256, data: &Data) -> Result<bool> {
                let is_new_key = self.trie_keys.insert(key);
                if is_new_key {
                    self.data_size
                        .fetch_add(data.as_ref().len(), Ordering::Relaxed);
                }
                Ok(is_new_key)
            }
            fn inspect_typed(&self, _: Vec<u8>, _: &U256) -> Result<()> {
                Ok(())
            }
        }

        impl AccountsKeysCollector {
            pub fn summarize(&self) {
                info!(
                    "Accounts state summary: \
                       trie keys: {}, \
                       data size: {}, \
                       unique storage roots: {}, \
                       unique code hashes: {}",
                    self.trie_keys.len(),
                    self.data_size.load(Ordering::Relaxed),
                    self.storage_roots.len(),
                    self.code_hashes.len(),
                );
            }
        }

        impl StoragesKeysCollector {
            pub fn summarize(&self) {
                info!(
                    "Storages state summary: \
                       trie keys: {}, \
                       data size: {}",
                    self.trie_keys.len(),
                    self.data_size.load(Ordering::Relaxed),
                );
            }
        }
    }

    pub mod streamer {
        use super::*;

        pub struct AccountsStreamer {
            pub source: Storage,
            pub destination: Storage,
        }

        impl Inspector<Account> for AccountsStreamer {
            fn inspect_raw<Data: AsRef<[u8]>>(&self, key: H256, data: &Data) -> Result<bool> {
                let destination = self.destination.db();

                if let Some(exist_data) = destination.get_pinned(key)? {
                    ensure!(
                        data.as_ref() == &*exist_data,
                        "Database existing data for key {:?} differs",
                        key
                    );
                    Ok(false)
                } else {
                    destination.put(key, data)?;
                    Ok(true)
                }
            }

            fn inspect_typed(&self, _: Vec<u8>, account: &Account) -> Result<()> {
                let source = self.source.borrow();
                let destination = self.destination.borrow();

                // - Account Storage
                let walker = Walker::new(source, StoragesKeysStreamer::new(destination));
                walker.traverse(Default::default(), account.storage_root)?;
                // walker.inspector.apply()?;

                // - Account Code
                let code_hash = account.code_hash;
                if let Some(code_data) = self.source.get::<storage::Codes>(code_hash) {
                    self.destination.set::<storage::Codes>(code_hash, code_data);
                } else {
                    assert_eq!(code_hash, Code::empty().hash());
                }

                Ok(())
            }
        }

        pub struct StoragesKeysStreamer<Destination> {
            destination: Destination,
        }

        impl<Destination> StoragesKeysStreamer<Destination> {
            fn new(destination: Destination) -> Self {
                Self { destination }
            }
        }

        impl<Destination> Inspector<U256> for StoragesKeysStreamer<Destination>
        where
            Destination: Borrow<rocksdb::DB>,
        {
            fn inspect_raw<Data: AsRef<[u8]>>(&self, key: H256, data: &Data) -> Result<bool> {
                let destination = self.destination.borrow();

                if let Some(exist_data) = destination.get_pinned(key)? {
                    if data.as_ref() != &*exist_data {
                        panic!("Database existing data for key {:?} differs", key);
                    }
                    Ok(false)
                } else {
                    destination.put(key, data)?;
                    Ok(true)
                }
            }

            fn inspect_typed(&self, _: Vec<u8>, _: &U256) -> Result<()> {
                Ok(())
            }
        }

        // impl<Destination> StoragesKeysStreamer<Destination>
        // where
        //     Destination: Borrow<rocksdb::DB>,
        // {
        //     fn apply(self) -> Result<()> {
        //         let destination = self.destination.borrow();
        //         destination.write(self.batch)?;
        //         Ok(())
        //     }
        // }
    }
}

pub mod cleaner {
    use super::{inspectors::memorizer, *};
    use std::convert::TryFrom;

    pub struct Cleaner<DB> {
        db: DB,
        accounts: memorizer::AccountsKeysCollector,
        storages: memorizer::StoragesKeysCollector,
    }

    impl<DB> Cleaner<DB> {
        pub fn new_with(
            db: DB,
            accounts: memorizer::AccountsKeysCollector,
            storages: memorizer::StoragesKeysCollector,
        ) -> Self {
            Self {
                db,
                accounts,
                storages,
            }
        }

        pub fn cleanup(self) -> Result<()>
        where
            DB: Borrow<rocksdb::DB>,
        {
            let db = self.db.borrow();

            // Cleanup unused trie keys in default column family
            {
                let mut batch = rocksdb::WriteBatch::default();

                for (key, _data) in db.iterator(rocksdb::IteratorMode::Start) {
                    let bytes = <&[u8; 32]>::try_from(key.as_ref())?;
                    let key = H256::from(bytes);
                    if self.accounts.trie_keys.contains(&key)
                        || self.accounts.storage_roots.contains(&key)
                        || self.storages.trie_keys.contains(&key)
                    {
                        continue; // skip this key
                    } else {
                        batch.delete(key);
                    }
                }

                let batch_size = batch.len();
                db.write(batch)?;
                info!("{} keys was removed", batch_size);
            }

            // Cleanup unused Account Code keys
            {
                let column_name = storage::Codes::COLUMN_NAME;
                let codes_cf = db
                    .cf_handle(column_name)
                    .ok_or_else(|| anyhow!("Codes Column Family '{}' not found", column_name))?;
                let mut batch = rocksdb::WriteBatch::default();

                for (key, _data) in db.iterator_cf(codes_cf, rocksdb::IteratorMode::Start) {
                    let code_hash = rlp::decode(&key)?; // NOTE: keep in sync with ::storage mod
                    if self.accounts.code_hashes.contains(&code_hash) {
                        continue; // skip this key
                    } else {
                        batch.delete_cf(codes_cf, key);
                    }
                }

                let batch_size = batch.len();
                db.write(batch)?;
                info!("{} code keys was removed", batch_size);
            }

            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use quickcheck::{Arbitrary, Gen};
    use quickcheck_macros::quickcheck;
    use std::convert::TryFrom;

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    struct Hash(H256);

    impl Arbitrary for Hash {
        fn arbitrary(g: &mut Gen) -> Self {
            let mut bytes = [0; 32];
            for byte in &mut bytes {
                *byte = u8::arbitrary(g);
            }
            Hash(H256::from(bytes))
        }
    }

    #[quickcheck]
    fn qc_hash_is_reversible_from_bytes(Hash(expected): Hash) {
        assert_eq!(expected, H256::from_slice(expected.as_bytes()));
        assert_eq!(expected, H256::from_slice(expected.as_ref()));
        assert_eq!(expected.as_bytes(), expected.as_ref());
        assert_eq!(
            H256::from(<&[u8; 32]>::try_from(expected.as_bytes()).unwrap()),
            expected
        );
    }
}
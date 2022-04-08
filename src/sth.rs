//! Key storage based on StoreTheHash.

use std::marker::PhantomData;

use async_trait::async_trait;
use eyre::Result;

mod buckets;
mod index;
mod recordlist;

use self::index::Index;
use crate::lsm::{KeyValueStorage, LogStorage};

pub struct StoreTheHash<V, L, const N: u8>
where
    L: LogStorage<Offset = V>,
{
    index: Index<V, L, N>,
    _v: PhantomData<V>,
}

pub struct Config<L>
where
    L: LogStorage,
{
    values: L,
}

#[async_trait(?Send)]
impl<Value, L, const N: u8> KeyValueStorage for StoreTheHash<Value, L, N>
where
    L: LogStorage<Offset = Value>,
    Value: Clone,
{
    type Config = Config<L>;
    type Value = Value;

    async fn create(config: Self::Config) -> Result<Self> {
        Ok(Self {
            index: Index::new(config.values)?,
            _v: PhantomData::default(),
        })
    }

    async fn open(config: Self::Config) -> Result<Self> {
        Ok(Self {
            index: Index::new(config.values)?,
            _v: PhantomData::default(),
        })
    }

    async fn close(self) -> Result<()> {
        Ok(())
    }

    async fn put<K: AsRef<[u8]>, V: AsRef<[u8]>>(&mut self, key: K, data: V) -> Result<()> {
        todo!()
    }

    async fn get<K: AsRef<[u8]>>(&mut self, key: K) -> Result<Option<Self::Value>> {
        todo!()
    }

    async fn delete<K: AsRef<[u8]>>(&mut self, key: K) -> Result<()> {
        todo!()
    }

    async fn has<K: AsRef<[u8]>>(&mut self, key: K) -> Result<bool> {
        todo!()
    }
}

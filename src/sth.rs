//! Key storage based on StoreTheHash.

use std::marker::PhantomData;

use async_trait::async_trait;
use eyre::Result;

mod buckets;
mod index;
mod recordlist;

use self::index::Index;
use crate::lsm::{KeyValueStorage, LogStorage};

#[derive(Debug)]
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

impl<L: LogStorage> Config<L> {
    pub fn new(values: L) -> Self {
        Config { values }
    }
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
        self.index.close().await?;
        Ok(())
    }

    async fn put<K: AsRef<[u8]>>(&mut self, key: K, value: Self::Value) -> Result<()> {
        self.index.put(key, value).await
    }

    async fn get<K: AsRef<[u8]>>(&mut self, key: K) -> Result<Option<&Self::Value>> {
        self.index.get(key)
    }

    async fn delete<K: AsRef<[u8]>>(&mut self, key: K) -> Result<()> {
        todo!()
    }

    async fn has<K: AsRef<[u8]>>(&mut self, key: K) -> Result<bool> {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use crate::vlog::{VLog, VLogConfig};

    use super::*;

    const BUCKET_BITS: u8 = 24;

    #[test]
    fn test_basics() {
        use glommio::LocalExecutorBuilder;
        LocalExecutorBuilder::default()
            .spawn(|| async move {
                let file = tempfile::NamedTempFile::new().unwrap();
                let mut vlog = VLog::create(VLogConfig::new(file.path())).await?;
                let mut sth =
                    StoreTheHash::<_, _, BUCKET_BITS>::create(Config::new(vlog.clone())).await?;
                const NUM_ENTRIES: u64 = 5;
                for i in 0..NUM_ENTRIES {
                    let key = i.to_le_bytes();
                    let offset = vlog
                        .put(&key, format!("hello world: {i}").as_bytes())
                        .await?;
                    sth.put(key, offset).await?;
                }

                // wait for all things to be actually written
                vlog.flush().await?;

                for i in 0..NUM_ENTRIES {
                    println!("{i}");
                    let offset = sth.get(i.to_le_bytes()).await?.unwrap();
                    println!("reading {i}: at {offset}");
                    let (key, value) = vlog.get(offset).await?.unwrap();
                    assert_eq!(&key[..], &i.to_le_bytes()[..]);
                    assert_eq!(
                        std::str::from_utf8(&value).unwrap(),
                        format!("hello world: {i}")
                    );
                }

                sth.close().await?;
                vlog.close().await?;

                Ok::<_, eyre::Error>(())
            })
            .unwrap()
            .join()
            .unwrap()
            .unwrap();
    }
}

use async_trait::async_trait;
use eyre::Result;
use zerocopy::AsBytes;

pub trait Lsm {
    type KeyStorage: KeyValueStorage;
    type ValueStorage: LogStorage;
    type Index: Index;
    type Filter: Filter;
    type CompactionStrategy: CompactionStrategy;
}

#[async_trait(?Send)]
pub trait KeyValueStorage: Sized {
    type Config;
    type Value;

    async fn create(config: Self::Config) -> Result<Self>;
    async fn open(config: Self::Config) -> Result<Self>;
    async fn close(self) -> Result<()>;

    async fn put<K: AsRef<[u8]>>(&mut self, key: K, data: Self::Value) -> Result<()>;
    async fn get<K: AsRef<[u8]>>(&mut self, key: K) -> Result<Option<&Self::Value>>;
    async fn delete<K: AsRef<[u8]>>(&mut self, key: K) -> Result<()>;
    async fn has<K: AsRef<[u8]>>(&mut self, key: K) -> Result<bool>;
}

#[async_trait(?Send)]
pub trait LogStorage: Sized {
    type Config;
    type Offset;
    type Key: std::ops::Deref<Target = [u8]>;
    type Value;

    async fn create(config: Self::Config) -> Result<Self>;
    async fn open(config: Self::Config) -> Result<Self>;
    async fn close(self) -> Result<()>;

    async fn put<K: AsRef<[u8]>, V: AsRef<[u8]>>(
        &mut self,
        key: K,
        data: V,
    ) -> Result<Self::Offset>;
    async fn get(&mut self, offset: &Self::Offset) -> Result<Option<(Self::Key, Self::Value)>>;
    async fn get_key(&mut self, offset: &Self::Offset) -> Result<Option<Self::Key>> {
        Ok(self.get(offset).await?.map(|(key, _)| key))
    }
    async fn get_value(&mut self, offset: &Self::Offset) -> Result<Option<Self::Value>> {
        Ok(self.get(offset).await?.map(|(_, value)| value))
    }

    async fn flush(&mut self) -> Result<()> {
        Ok(())
    }
}

#[async_trait(?Send)]
pub trait Index: Default {
    type Config;
    /// Associated data with the key, this could be the offset at which the value is stored
    /// in the `ValueStorage`.
    type KeyData: AsBytes;
    fn from_config(config: Self::Config) -> Result<Self>;

    async fn put<K: AsRef<[u8]>>(&mut self, key: K, data: Self::KeyData) -> Result<()>;
    async fn get<K: AsRef<[u8]>>(&mut self, key: K) -> Result<Option<Self::KeyData>>;
}

pub trait Filter: Default {
    type Config;
    type Item;

    fn from_config(config: Self::Config) -> Result<Self>;
    fn insert(&mut self, item: Self::Item) -> bool;
    fn contains(&self, item: &Self::Item) -> bool;
    fn clear(&mut self);
}

pub trait CompactionStrategy {}

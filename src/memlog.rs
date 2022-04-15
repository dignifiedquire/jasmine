//! In memory value log.

use std::{cell::RefCell, ops::Deref, rc::Rc};

use async_trait::async_trait;
use eyre::Result;

use crate::lsm::LogStorage;

#[derive(Debug, Clone)]
pub struct MemLog(Rc<RefCell<Inner>>);

#[derive(Debug)]
struct Inner {
    values: Vec<(Slice, Slice)>,
}

#[derive(Debug, Clone, Default)]
pub struct MemLogConfig {}

impl MemLogConfig {
    pub fn new() -> Self {
        Self::default()
    }
}

#[derive(Debug, Clone)]
#[repr(transparent)]
#[allow(clippy::redundant_allocation)]
pub struct Slice(Rc<Box<[u8]>>);

impl AsRef<[u8]> for Slice {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

impl Deref for Slice {
    type Target = [u8];
    fn deref(&self) -> &Self::Target {
        self.as_ref()
    }
}

#[async_trait(?Send)]
impl LogStorage for MemLog {
    type Config = MemLogConfig;
    type Key = Slice;
    type Value = Slice;
    type Offset = u64;

    async fn create(_config: Self::Config) -> Result<Self> {
        Ok(MemLog(Rc::new(RefCell::new(Inner {
            values: Default::default(),
        }))))
    }

    async fn open(_config: Self::Config) -> Result<Self> {
        Ok(MemLog(Rc::new(RefCell::new(Inner {
            values: Default::default(),
        }))))
    }

    async fn close(self) -> Result<()> {
        Ok(())
    }

    async fn put<K: AsRef<[u8]>, V: AsRef<[u8]>>(&self, key: K, value: V) -> Result<Self::Offset> {
        let key = key.as_ref().to_vec().into_boxed_slice();
        let value = value.as_ref().to_vec().into_boxed_slice();
        let mut inner = self.0.borrow_mut();

        let offset = inner.values.len() as u64;
        inner
            .values
            .push((Slice(Rc::new(key)), Slice(Rc::new(value))));

        Ok(offset)
    }

    async fn get(&self, offset: &Self::Offset) -> Result<Option<(Self::Key, Self::Value)>> {
        let inner = self.0.borrow();
        let res = inner
            .values
            .get(usize::try_from(*offset)?)
            .map(|(key, value)| (key.clone(), value.clone()));
        Ok(res)
    }

    async fn flush(&self) -> Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_memlog_write_read_small_values() {
        use glommio::LocalExecutorBuilder;
        LocalExecutorBuilder::default()
            .spawn(|| async move {
                let memlog = MemLog::create(MemLogConfig::new()).await?;

                let mut offsets = Vec::new();
                for i in 0u64..100 {
                    let offset = memlog
                        .put(i.to_le_bytes(), format!("hello world: {i}").as_bytes())
                        .await?;
                    offsets.push(offset);
                }

                for (i, offset) in offsets.iter().enumerate() {
                    println!("reading {i}: at {offset}");
                    let (key, value) = memlog.get(offset).await?.unwrap();
                    assert_eq!(&key[..], &i.to_le_bytes()[..]);
                    assert_eq!(
                        std::str::from_utf8(&value).unwrap(),
                        format!("hello world: {i}")
                    );
                }

                Ok::<_, eyre::Error>(())
            })
            .unwrap()
            .join()
            .unwrap()
            .unwrap();
    }

    #[test]
    fn test_basic_memlog_write_read_large_values() {
        use glommio::LocalExecutorBuilder;
        LocalExecutorBuilder::default()
            .spawn(|| async move {
                let memlog = MemLog::create(MemLogConfig::new()).await?;

                let mut offsets = Vec::new();
                for i in 0u64..100 {
                    let value = (0u64..500)
                        .flat_map(|k| (k * i).to_le_bytes())
                        .collect::<Vec<_>>();
                    let offset = memlog.put(i.to_le_bytes(), &value).await?;
                    offsets.push(offset);
                }

                for (i, offset) in offsets.iter().enumerate() {
                    let i = i as u64;
                    println!("reading {i}: at {offset}");
                    let (key, value) = memlog.get(offset).await?.unwrap();
                    assert_eq!(&key[..], &i.to_le_bytes()[..]);
                    let original_value = (0u64..500)
                        .flat_map(|k| (k * i).to_le_bytes())
                        .collect::<Vec<_>>();

                    assert_eq!(&value[..], &original_value[..]);
                }

                Ok::<_, eyre::Error>(())
            })
            .unwrap()
            .join()
            .unwrap()
            .unwrap();
    }
}

use ahash::AHashMap;

use eyre::{ensure, Result};

#[derive(Debug)]
pub struct Buckets<V, const N: u8>(AHashMap<usize, V>);

impl<V, const N: u8> Default for Buckets<V, N> {
    fn default() -> Self {
        Self(AHashMap::with_capacity(N as usize))
    }
}

impl<V: Default, const N: u8> Buckets<V, N> {
    pub fn put(&mut self, bucket: usize, value: V) -> Result<()> {
        ensure!(bucket <= Self::max_size(), "out of bounds");
        let val = self.0.entry(bucket).or_insert_with(Default::default);

        *val = value;
        Ok(())
    }

    pub fn get(&self, bucket: usize) -> Result<Option<&V>> {
        ensure!(bucket <= Self::max_size(), "out of bounds");
        let value = self.0.get(&bucket);

        Ok(value)
    }

    pub fn get_mut(&mut self, bucket: usize) -> Result<&mut V> {
        ensure!(bucket <= Self::max_size(), "out of bounds");
        let value = self.0.entry(bucket).or_insert_with(Default::default);

        Ok(value)
    }

    pub fn has(&self, bucket: usize) -> bool {
        self.0.contains_key(&bucket)
    }

    fn max_size() -> usize {
        (1 << N) - 1
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn max_size() {
        const BUCKETS_BITS: u8 = 24;

        assert_eq!(
            Buckets::<u64, BUCKETS_BITS>::max_size(),
            (1 << BUCKETS_BITS) - 1
        );
    }

    #[test]
    fn put() {
        const BUCKETS_BITS: u8 = 3;
        let mut buckets = Buckets::<u64, BUCKETS_BITS>::default();
        buckets.put(3, 54321).unwrap();
        assert!(matches!(buckets.get(3), Ok(Some(54321))));
    }

    #[test]
    fn put_error() {
        const BUCKETS_BITS: u8 = 3;
        let mut buckets = Buckets::<u64, BUCKETS_BITS>::default();
        let error = buckets.put(333, 54321);
        assert!(error.is_err());
    }

    #[test]
    fn get() {
        const BUCKETS_BITS: u8 = 3;
        let mut buckets = Buckets::<u64, BUCKETS_BITS>::default();
        let result_empty = buckets.get(3);
        assert!(matches!(result_empty, Ok(None)));

        buckets.put(3, 54321).unwrap();
        let result = buckets.get(3);
        assert!(matches!(result, Ok(Some(54321))));
    }

    #[test]
    fn get_error() {
        const BUCKETS_BITS: u8 = 3;
        let buckets = Buckets::<u64, BUCKETS_BITS>::default();
        let error = buckets.get(333);
        assert!(error.is_err());
    }
}

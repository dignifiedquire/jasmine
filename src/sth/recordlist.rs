use std::collections::BTreeMap;

#[derive(Debug)]
pub struct RecordList<V> {
    records: BTreeMap<usize, Record<V>>,
}

impl<V> Default for RecordList<V> {
    fn default() -> Self {
        Self {
            records: BTreeMap::new(),
        }
    }
}

impl<V> RecordList<V> {
    pub fn len(&self) -> usize {
        self.records.len()
    }

    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    pub fn get(&self, pos: usize) -> Option<&Record<V>> {
        todo!()
    }

    pub fn find_key_position<K: AsRef<[u8]>>(&self, key: K) -> (usize, Option<&Record<V>>) {
        todo!()
    }

    /// Add a new key value pair.
    pub fn push<K: AsRef<[u8]>>(&mut self, key: K, value: V) -> usize {
        todo!()
    }

    /// Replace an existing key value pair.
    pub fn insert<K: AsRef<[u8]>>(&mut self, key: K, value: V, pos: usize) {
        todo!()
    }

    /// Replace multiple key value pairs.
    pub fn insert_many<K: AsRef<[u8]>>(&mut self, keys: &[(K, V, usize)]) {
        todo!()
    }
}

#[derive(Debug, Default)]
pub struct Record<V> {
    pub key: Vec<u8>,
    pub value: V,
}

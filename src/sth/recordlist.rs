use std::collections::BTreeMap;

#[derive(Debug)]
pub struct RecordList<V> {
    records: BTreeMap<Vec<u8>, V>,
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

    pub fn get<K: AsRef<[u8]>>(&self, key: K) -> Option<&V> {
        self.records.get(key.as_ref())
    }

    pub fn take<K: AsRef<[u8]>>(&mut self, key: K) -> Option<(Vec<u8>, V)> {
        self.records.remove_entry(key.as_ref())
    }

    /// Returns the next and previous record for this key.
    pub fn find_key_position<K: AsRef<[u8]>>(&self, key: K) -> (Option<(&[u8], &V)>, Option<(&[u8], &V)>) {
        let mut prev_record = None;
        let key = key.as_ref();
        for (record_key, record) in &self.records {
            // Location where the key gets inserted is found
            if &record_key[..] > key {
                return (prev_record, Some((&record_key, record)));
            } else {
                prev_record = Some((&record_key, record));
            }
        }
        (prev_record, None)
    }

    /// Add a new key value pair.
    pub fn insert(&mut self, key: Vec<u8>, value: V) {
        self.records.insert(key, value);
    }
}

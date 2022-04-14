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

impl<V: Clone> RecordList<V> {
    pub fn len(&self) -> usize {
        self.records.len()
    }

    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    pub fn get<K: AsRef<[u8]>>(&self, key: K) -> Option<&V> {
        let key = key.as_ref();
        // Several prefixes can match a `key`, we are only interested in the last one that
        // matches, hence keep a match around until we can be sure it's the last one.
        let mut might_match = None;
        for (record_key, record) in &self.records {
            // The stored prefix of the key needs to match the requested key.
            if key.starts_with(record_key) {
                might_match = Some(record);
            } else if &record_key[..] > key {
                // No keys from here on can possibly match, hence stop iterating. If we had a prefix
                // match, return that, else return none.

                break;
            }
        }
        might_match
    }

    pub fn take<K: AsRef<[u8]>>(&mut self, key: K) -> Option<(Vec<u8>, V)> {
        self.records.remove_entry(key.as_ref())
    }

    /// Returns the next and previous record for this key.
    #[allow(clippy::type_complexity)]
    pub fn find_key_position<K: AsRef<[u8]>>(
        &self,
        key: K,
    ) -> (Option<(Vec<u8>, V)>, Option<(Vec<u8>, V)>) {
        let mut prev_record = None;
        let key = key.as_ref();
        for (record_key, record) in &self.records {
            // Location where the key gets inserted is found
            if &record_key[..] > key {
                return (prev_record, Some((record_key.clone(), record.clone())));
            } else {
                prev_record = Some((record_key.clone(), record.clone()));
            }
        }
        (prev_record, None)
    }

    /// Add a new key value pair.
    pub fn insert(&mut self, key: Vec<u8>, value: V) {
        self.records.insert(key, value);
    }
}

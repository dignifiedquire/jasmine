use std::{cell::RefCell, cmp};

use eyre::{ensure, Result};

use crate::lsm::LogStorage;

use super::{buckets::Buckets, recordlist::RecordList};

/// Number of bytes used for the size prefix of a record list.
pub const SIZE_PREFIX_SIZE: usize = 4;

/// Remove the prefix that is used for the bucket.
///
/// The first bits of a key are used to determine the bucket to put the key into. This function
/// removes those bytes. Only bytes that are fully covered by the bits are removed. E.g. a bit
/// value of 19 will remove only 2 bytes, whereas 24 bits removes 3 bytes.
fn strip_bucket_prefix(key: &[u8], bits: u8) -> &[u8] {
    &key[usize::from(bits / 8)..]
}

#[derive(Debug)]
pub struct Index<V, L, const N: u8>
where
    L: LogStorage<Offset = V>,
{
    buckets: RefCell<Buckets<RecordList<V>, N>>,
    values: L,
}
impl<V, L, const N: u8> Index<V, L, N>
where
    L: LogStorage<Offset = V>,
    V: Clone,
{
    pub fn new(values: L) -> Result<Self> {
        Ok(Self {
            buckets: RefCell::new(Buckets::<_, N>::default()),
            values,
        })
    }

    pub async fn put<K: AsRef<[u8]>>(&self, key: K, value: V) -> Result<()> {
        let key = key.as_ref();
        ensure!(key.len() >= 4, "key must be at least 4 bytes");

        // Determine which bucket a key falls into. Use the first few bytes of they key for it and
        // interpret them as a little-endian integer.
        let prefix_bytes: [u8; 4] = key[0..4].try_into().unwrap();
        let prefix = u32::from_le_bytes(prefix_bytes);
        let leading_bits = (1 << N) - 1;
        let bucket: u32 = prefix & leading_bits;

        // The key doesn't need the prefix that was used to find the right bucket. For simplicty
        // only full bytes are trimmed off.
        let index_key = strip_bucket_prefix(key, N);

        // No records stored in that bucket yet
        if !self.buckets.borrow().has(bucket as usize) {
            // As it's the first key a single byte is enough as it doesn't need to be distinguised
            // from other keys.
            let trimmed_index_key = &index_key[..1];

            self.buckets
                .borrow_mut()
                .get_mut(bucket as usize)?
                .insert(trimmed_index_key.to_vec(), value);
        } else {
            let (prev_record, next_record) = self
                .buckets
                .borrow()
                .get(bucket as usize)?
                .expect("checked")
                .find_key_position(index_key);

            match prev_record {
                // The previous key is fully contained in the current key. We need to read the full
                // key from the main data file in order to retrieve a key that is distinguishable
                // from the one that should get inserted.
                Some((prev_key_short, prev_record)) if index_key.starts_with(&prev_key_short) => {
                    let full_prev_key = self
                        .values
                        .get_key(&prev_record)
                        .await?
                        .ok_or_else(|| eyre::eyre!("missing full key for key: {:?}", key))?;

                    // The index key has already removed the prefix that is used to determine the
                    // bucket. Do the same for the full previous key.
                    let prev_key = strip_bucket_prefix(&full_prev_key[..], N);
                    let key_trim_pos = first_non_common_byte(index_key, prev_key);

                    // Only store the new key if it doesn't exist yet.
                    if key_trim_pos >= index_key.len() {
                        return Ok(());
                    }

                    let trimmed_prev_key = &prev_key[..=key_trim_pos];
                    let trimmed_index_key = &index_key[..=key_trim_pos];

                    let prev_key_short = prev_key_short.to_vec();

                    // Get the index file offset of the record list the key is in.
                    let mut buckets = self.buckets.borrow_mut();
                    let records = buckets.get_mut(bucket as usize)?;

                    // remove previous key
                    let (_, prev_value) = records.take(prev_key_short).expect("already checked");
                    // insert
                    records.insert(trimmed_prev_key.to_vec(), prev_value);
                    records.insert(trimmed_index_key.to_vec(), value);
                }
                // The previous key is not fully contained in the key that should get inserted.
                // Hence we only need to trim the new key to the smallest one possible that is
                // still distinguishable from the previous (in case there is one) and next key
                // (in case there is one).
                _ => {
                    let prev_record_non_common_byte_pos = match prev_record {
                        Some((record_key, _)) => first_non_common_byte(index_key, &record_key),
                        None => 0,
                    };

                    // The new record won't be the last record
                    let next_record_non_common_byte_pos =
                        if let Some((next_record_key, _)) = next_record {
                            // In order to determine the minimal key size, we need to get the next key as well.
                            first_non_common_byte(index_key, &next_record_key)
                        } else {
                            0
                        };

                    // Minimum prefix of the key that is different in at least one byte from the
                    // previous as well as the next key.
                    let min_prefix = cmp::max(
                        prev_record_non_common_byte_pos,
                        next_record_non_common_byte_pos,
                    );

                    // We cannot trim beyond the key length
                    let key_trim_pos = cmp::min(min_prefix, index_key.len());

                    let trimmed_index_key = &index_key[0..=key_trim_pos];

                    // Get the index file offset of the record list the key is in.
                    let mut buckets = self.buckets.borrow_mut();
                    let records = buckets.get_mut(bucket as usize)?;
                    records.insert(trimmed_index_key.to_vec(), value);
                }
            }
        }

        Ok(())
    }

    pub fn get<K: AsRef<[u8]>>(&self, key: K) -> Result<Option<V>> {
        let key = key.as_ref();
        ensure!(key.len() >= 4, "Key must be at least 4 bytes long");

        // Determine which bucket a key falls into. Use the first few bytes of they key for it and
        // interpret them as a little-endian integer.
        let prefix_bytes: [u8; 4] = key[0..4].try_into().unwrap();
        let prefix = u32::from_le_bytes(prefix_bytes);
        let leading_bits = (1 << N) - 1;
        let bucket: u32 = prefix & leading_bits;

        // Get the index file offset of the record list the key is in.
        let buckets = self.buckets.borrow();
        let records = buckets.get(bucket as usize)?;
        // The key doesn't need the prefix that was used to find the right bucket. For simplicty
        // only full bytes are trimmed off.
        let index_key = strip_bucket_prefix(key, N);

        Ok(records.and_then(|r| r.get(index_key)).cloned())
    }

    pub async fn close(&self) -> Result<()> {
        self.values.close().await?;

        Ok(())
    }
}

// Returns the position of the first character that both given slices have not in common.
///
/// It might return an index that is bigger than the input strings. If one is full prefix of the
/// other, the index will be `shorter_slice.len() + 1`, if both slices are equal it will be
/// `slice.len() + 1`
fn first_non_common_byte(aa: &[u8], bb: &[u8]) -> usize {
    let smaller_length = cmp::min(aa.len(), bb.len());

    let mut index = 0;
    for _ in 0..smaller_length {
        if aa[index] != bb[index] {
            break;
        }
        index += 1
    }
    index
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_first_non_common_byte() {
        assert_eq!(first_non_common_byte(&[0], &[1]), 0);
        assert_eq!(first_non_common_byte(&[0], &[0]), 1);
        assert_eq!(first_non_common_byte(&[0, 1, 2, 3], &[0]), 1);
        assert_eq!(first_non_common_byte(&[0], &[0, 1, 2, 3]), 1);
        assert_eq!(first_non_common_byte(&[0, 1, 2], &[0, 1, 2, 3]), 3);
        assert_eq!(first_non_common_byte(&[0, 1, 2, 3], &[0, 1, 2]), 3);
        assert_eq!(first_non_common_byte(&[3, 2, 1, 0], &[0, 1, 2]), 0);
        assert_eq!(first_non_common_byte(&[0, 1, 1, 0], &[0, 1, 2]), 2);
        assert_eq!(
            first_non_common_byte(&[180, 9, 113, 0], &[180, 0, 113, 0]),
            1
        );
    }
}

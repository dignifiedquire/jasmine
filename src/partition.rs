//! Partitioning of keys using Weighted Rendezvous Hashing, als known as WRH.
//!
//! Based on [New Hashing Algorithms for Data Storage](https://www.snia.org/sites/default/files/SDC15_presentations/dist_sys/Jason_Resch_New_Consistent_Hashings_Rev.pdf).

/// Represents a single bucket in the space.
#[derive(Debug, Clone, PartialEq)]
pub struct Bucket {
    /// Identifier.
    pub id: u64,
    /// The weight of this bucket, defaults to `100`.
    pub weight: f64,
    /// Hashing seed.
    pub seed: u64,
}

impl Bucket {
    pub fn new(id: u64, weight: f64, seed: u64) -> Self {
        Bucket { id, weight, seed }
    }

    /// Calculates the weighted score for this node, for the provided `key`.
    pub fn compute_score(&self, key: &[u8]) -> f64 {
        let hash = xxhash_rust::xxh3::xxh3_64_with_seed(key, self.seed);
        let hash_f = int_to_float(hash);
        let score = 1. / -hash_f.ln();
        self.weight * score
    }
}

/// Calculates the bucket to select, for the given `key`.
///
/// Returns `None` if the bucket list is empty.
pub fn choose_bucket<'a>(buckets: &'a [Bucket], key: &[u8]) -> Option<&'a Bucket> {
    let mut high_score = -1.;
    let mut champion = None;

    for bucket in buckets {
        let score = bucket.compute_score(key);
        if score > high_score {
            champion = Some(bucket);
            high_score = score;
        }
    }

    champion
}

const FIFTY_THREE_ONES: u64 = 0xFFFF_FFFF_FFFF_FFFF >> (64 - 53);
const FIFTY_THREE_ZEROS: f64 = (1u64 << 53) as f64;

/// Convert a integert to float in the range of 0 and 1 (exclusive).
fn int_to_float(value: u64) -> f64 {
    (value & FIFTY_THREE_ONES) as f64 / FIFTY_THREE_ZEROS
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basics() {
        let buckets = [
            Bucket::new(0, 100., 123),
            Bucket::new(1, 200., 567),
            Bucket::new(2, 300., 789),
        ];

        assert_eq!(choose_bucket(&buckets, &b"foo"[..]).unwrap(), &buckets[2]);
        assert_eq!(choose_bucket(&buckets, &b"bar"[..]).unwrap(), &buckets[1]);
        assert_eq!(choose_bucket(&buckets, &b"hello"[..]).unwrap(), &buckets[0]);
    }

    #[test]
    fn test_consistency() {
        let buckets = [
            Bucket::new(0, 100., 123),
            Bucket::new(1, 200., 567),
            Bucket::new(2, 300., 789),
            Bucket::new(3, 200., 789),
            Bucket::new(4, 100., 789),
        ];

        let keys = vec![&b"hello"[..], &b"world"[..], &b"foo"[..], &b"bar"[..]];
        let ids = keys
            .iter()
            .map(|key| choose_bucket(&buckets, key).unwrap().id)
            .collect::<Vec<_>>();

        for i in (1..=4).rev() {
            let new_buckets = buckets[..i].to_vec();
            let new_ids = keys
                .iter()
                .map(|key| choose_bucket(&new_buckets, key).unwrap().id)
                .collect::<Vec<_>>();
            for (id, new_id) in ids.iter().zip(new_ids.iter()) {
                if *id < i as u64 {
                    // Check that all still existing buckets have kept their original assignments
                    assert_eq!(id, new_id);
                }
            }
        }
    }

    #[test]
    fn test_int_to_float() {
        assert_eq!(int_to_float(0), 0.);
        assert_eq!(int_to_float(1), 1.1102230246251565e-16);
        assert_eq!(int_to_float(u64::MAX), 0.9999999999999999);
    }
}

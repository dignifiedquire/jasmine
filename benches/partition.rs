use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use jasmine::partition;

pub fn criterion_benchmark(c: &mut Criterion) {
    let bucket = partition::Bucket::new(0, 100., 123);

    let mut group = c.benchmark_group("compute_score");
    for key_size in [32, 64, 128].iter() {
        let key = vec![4u8; *key_size];
        group.bench_with_input(
            BenchmarkId::new("key_size", *key_size as u64),
            &key,
            |b, key| b.iter(|| bucket.compute_score(black_box(key))),
        );
    }
    group.finish();

    let mut group = c.benchmark_group("choose_bucket_24");
    let buckets = (0..24)
        .map(|i| partition::Bucket::new(i, i as f64 * 100., i * 8))
        .collect::<Vec<_>>();
    for key_size in [32, 64, 128].iter() {
        let key = vec![4u8; *key_size];
        group.bench_with_input(
            BenchmarkId::new("key_size", *key_size as u64),
            &key,
            |b, key| b.iter(|| partition::choose_bucket(&buckets, black_box(key))),
        );
    }
    group.finish();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);

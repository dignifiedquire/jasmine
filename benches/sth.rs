use std::time::Instant;

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};

use criterion::async_executor::AsyncExecutor;
use glommio::LocalExecutor;
use jasmine::lsm::{KeyValueStorage, LogStorage};
use jasmine::memlog::{MemLog, MemLogConfig};
use jasmine::sth::{Config, StoreTheHash};

#[derive(Debug, Default)]
struct GlommioExecutor {
    executor: LocalExecutor,
}

impl GlommioExecutor {
    pub fn run<T>(&self, future: impl std::future::Future<Output = T>) -> T {
        self.executor.run(future)
    }
}

impl<'a> AsyncExecutor for &'a GlommioExecutor {
    fn block_on<T>(&self, future: impl std::future::Future<Output = T>) -> T {
        self.executor.run(future)
    }
}

pub fn put_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("sth_put");
    for value_size in [64, 128, 1024, 2048].iter() {
        let value = vec![8u8; *value_size];
        let key_size = 32;

        group.throughput(criterion::Throughput::Bytes((key_size + value_size) as u64));
        group.bench_with_input(
            BenchmarkId::new("value_size", *value_size as u64),
            &value,
            |b, value| {
                let executor = GlommioExecutor::default();
                let (memlog, sth) = executor.run(async {
                    let memlog = MemLog::create(MemLogConfig::new()).await.unwrap();
                    let sth = StoreTheHash::<_, _, 24>::create(Config::new(memlog.clone()))
                        .await
                        .unwrap();
                    (memlog, sth)
                });

                let memlog_ref = &memlog;
                let sth_ref = &sth;
                b.to_async(&executor).iter_custom(|iters| async move {
                    let keys = (0..iters)
                        .map(|i| blake3::hash(&i.to_le_bytes()))
                        .collect::<Vec<_>>();
                    let start = Instant::now();
                    for key in &keys {
                        let offset = memlog_ref
                            .put(key.as_bytes(), black_box(value))
                            .await
                            .unwrap();
                        sth_ref.put(key.as_bytes(), offset).await.unwrap();
                    }
                    start.elapsed()
                });

                executor.run(async move {
                    sth.close().await.unwrap();
                    memlog.close().await.unwrap();
                });
            },
        );
    }
    group.finish();
}

pub fn get_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("sth_get");
    for value_size in [64, 128, 1024, 2048].iter() {
        let key_size = 32;

        group.throughput(criterion::Throughput::Bytes(
            (key_size + *value_size) as u64,
        ));
        group.bench_function(BenchmarkId::new("value_size", *value_size as u64), |b| {
            let executor = GlommioExecutor::default();
            let (memlog, sth) = executor.run(async {
                let memlog = MemLog::create(MemLogConfig::new()).await.unwrap();
                let sth = StoreTheHash::<_, _, 24>::create(Config::new(memlog.clone()))
                    .await
                    .unwrap();
                (memlog, sth)
            });

            let memlog_ref = &memlog;
            let keys = executor.run(async {
                let mut keys = Vec::new();
                for i in 0..1000 {
                    let value = vec![((8 * i) % 255) as u8; *value_size];
                    let key = blake3::hash(&value);
                    let offset = memlog_ref.put(key.as_bytes(), &value).await.unwrap();
                    sth.put(key.as_bytes(), offset).await.unwrap();
                    keys.push(key);
                }
                memlog.flush().await.unwrap();
                keys
            });

            let sth_ref = &sth;
            let keys_ref = &keys;
            b.to_async(&executor).iter_custom(|iters| async move {
                let l = keys_ref.len();

                let start = Instant::now();
                for i in 0..iters {
                    let key = keys_ref[(i as usize) % l];
                    let res = sth_ref.get(key.as_bytes()).await.unwrap().unwrap();
                    black_box(res);
                }
                start.elapsed()
            });
            executor.run(async move { memlog.close().await.unwrap() });
        });
    }
    group.finish();
}

criterion_group!(benches, put_benchmark, get_benchmark);
criterion_main!(benches);

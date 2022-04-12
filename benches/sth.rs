use std::time::Instant;

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};

use criterion::async_executor::AsyncExecutor;
use glommio::LocalExecutor;
use jasmine::lsm::{KeyValueStorage, LogStorage};
use jasmine::sth::{Config, StoreTheHash};
use jasmine::vlog::{VLog, VLogConfig};

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
                let file = tempfile::NamedTempFile::new().unwrap();
                let executor = GlommioExecutor::default();
                let (vlog, sth) = executor.run(async {
                    let vlog = VLog::create(VLogConfig::new(file.path())).await.unwrap();
                    let sth = StoreTheHash::<_, _, 24>::create(Config::new(vlog.clone()))
                        .await
                        .unwrap();
                    (vlog, sth)
                });
                // const NUM_ENTRIES: u64 = 5;
                // for i in 0..NUM_ENTRIES {
                //     let key = i.to_le_bytes();
                //     let offset = vlog
                //         .put(&key, format!("hello world: {i}").as_bytes())
                //         .await?;
                //     sth.put(key, offset).await?;
                // }

                let vlog_ref = &vlog;
                let sth_ref = &sth;
                b.to_async(&executor).iter_custom(|iters| async move {
                    let keys = (0..iters)
                        .map(|i| blake3::hash(&i.to_le_bytes()))
                        .collect::<Vec<_>>();
                    let start = Instant::now();
                    for key in &keys {
                        let offset = vlog_ref
                            .put(key.as_bytes(), black_box(value))
                            .await
                            .unwrap();
                        sth_ref.put(key.as_bytes(), offset).await.unwrap();
                    }
                    start.elapsed()
                });

                executor.run(async move {
                    sth.close().await.unwrap();
                    vlog.close().await.unwrap();
                });
            },
        );
    }
    group.finish();
}

criterion_group!(benches, put_benchmark);
criterion_main!(benches);

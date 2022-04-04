use std::cell::RefCell;

use criterion::{
    async_executor::AsyncExecutor, black_box, criterion_group, criterion_main, BenchmarkId,
    Criterion,
};
use glommio::LocalExecutor;
use jasmine::vlog::VLog;

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

pub fn criterion_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("vlog_put");
    let value_size = 1024;
    for key_size in [32, 64, 128].iter() {
        let key = vec![4u8; *key_size];
        let value = vec![8u8; value_size];

        group.throughput(criterion::Throughput::Bytes(
            (*key_size + value_size) as u64,
        ));
        group.bench_with_input(
            BenchmarkId::new("key_size", *key_size as u64),
            &(key, value),
            |b, (key, value)| {
                let file = tempfile::NamedTempFile::new().unwrap();
                let executor = GlommioExecutor::default();
                let vlog = executor.run(async { VLog::create(file.path()).await.unwrap() });
                let vlog = RefCell::new(vlog);
                let vlog_ref = &vlog;
                b.to_async(&executor).iter(|| async move {
                    let vlog = &mut *vlog_ref.borrow_mut();
                    vlog.put(key, black_box(value)).await.unwrap()
                });

                executor.run(async move { vlog.into_inner().close().await.unwrap() });
            },
        );
    }
    group.finish();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);

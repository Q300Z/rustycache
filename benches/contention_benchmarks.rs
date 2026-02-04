use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use rustycache::rustycache::Rustycache;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

fn bench_contention(c: &mut Criterion) {
    let cap = 10000;
    let ttl = Duration::from_secs(60);
    let interval = Duration::from_secs(60);
    let total_ops = 100_000;
    let rt = tokio::runtime::Runtime::new().unwrap();

    for thread_count in [1, 4, 8, 16] {
        let mut group = c.benchmark_group(format!("Contention_{}_threads", thread_count));
        group.throughput(Throughput::Elements(total_ops as u64));

        // put benchmark
        group.bench_function("sharded_lru_put", |b| {
            b.iter_custom(|iters| {
                let mut elapsed = Duration::ZERO;
                for _ in 0..iters {
                    let cache = Arc::new(rt.block_on(async { Rustycache::lru(16, cap, ttl, interval) }));
                    let ops_per_thread = total_ops / thread_count;
                    let mut handles = vec![];

                    let start = Instant::now();
                    for t in 0..thread_count {
                        let c_clone = Arc::clone(&cache);
                        handles.push(thread::spawn(move || {
                            for i in 0..ops_per_thread {
                                c_clone.put(format!("{}-{}", t, i), "value".to_string());
                            }
                        }));
                    }
                    for h in handles {
                        h.join().unwrap();
                    }
                    elapsed += start.elapsed();
                }
                elapsed
            });
        });

        // get benchmark (pre-filled)
        group.bench_function("sharded_lru_get", |b| {
            let cache = Arc::new(rt.block_on(async {
                let c = Rustycache::lru(16, cap, ttl, interval);
                for i in 0..cap {
                    c.put(i.to_string(), "value".to_string());
                }
                c
            }));

            b.iter_custom(|iters| {
                let mut elapsed = Duration::ZERO;
                for _ in 0..iters {
                    let ops_per_thread = total_ops / thread_count;
                    let mut handles = vec![];

                    let start = Instant::now();
                    for _ in 0..thread_count {
                        let c_clone = Arc::clone(&cache);
                        handles.push(thread::spawn(move || {
                            for i in 0..ops_per_thread {
                                c_clone.get(&(i % cap).to_string());
                            }
                        }));
                    }
                    for h in handles {
                        h.join().unwrap();
                    }
                    elapsed += start.elapsed();
                }
                elapsed
            });
        });

        group.finish();
    }
}

criterion_group!(benches, bench_contention);
criterion_main!(benches);

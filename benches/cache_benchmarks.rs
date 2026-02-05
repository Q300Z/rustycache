use criterion::{criterion_group, criterion_main, Criterion};
use rustycache::rustycache::Rustycache;
use std::hint::black_box;
use std::time::Duration;

fn bench_lru(c: &mut Criterion) {
    let cap = 10000;
    let ttl = Duration::from_secs(60);
    let interval = Duration::from_secs(60);
    let rt = tokio::runtime::Runtime::new().unwrap();

    let mut group = c.benchmark_group("Cache_LRU_Sharded");
    
    group.bench_function("put", |b| {
        let cache = rt.block_on(async { Rustycache::lru(16, cap, ttl, interval) });
        let mut i = 0;
        b.iter(|| {
            cache.put(black_box(i.to_string()), black_box(i.to_string()));
            i += 1;
        });
    });

    group.bench_function("get", |b| {
        let cache = rt.block_on(async {
            let c = Rustycache::lru(16, cap, ttl, interval);
            for i in 0..cap {
                c.put(i.to_string(), i.to_string());
            }
            c
        });
        let mut i = 0;
        b.iter(|| {
            cache.get(black_box(&(i % cap).to_string()));
            i += 1;
        });
    });

    group.finish();
}

fn bench_fifo(c: &mut Criterion) {
    let cap = 10000;
    let ttl = Duration::from_secs(60);
    let interval = Duration::from_secs(60);
    let rt = tokio::runtime::Runtime::new().unwrap();

    let mut group = c.benchmark_group("Cache_FIFO_Sharded");
    
    group.bench_function("put", |b| {
        let cache = rt.block_on(async { Rustycache::fifo(16, cap, ttl, interval) });
        let mut i = 0;
        b.iter(|| {
            cache.put(black_box(i.to_string()), black_box(i.to_string()));
            i += 1;
        });
    });

    group.bench_function("get", |b| {
        let cache = rt.block_on(async {
            let c = Rustycache::fifo(16, cap, ttl, interval);
            for i in 0..cap {
                c.put(i.to_string(), i.to_string());
            }
            c
        });
        let mut i = 0;
        b.iter(|| {
            cache.get(black_box(&(i % cap).to_string()));
            i += 1;
        });
    });

    group.finish();
}

fn bench_lfu(c: &mut Criterion) {
    let cap = 10000;
    let ttl = Duration::from_secs(60);
    let interval = Duration::from_secs(60);
    let rt = tokio::runtime::Runtime::new().unwrap();

    let mut group = c.benchmark_group("Cache_LFU_Sharded");
    
    group.bench_function("put", |b| {
        let cache = rt.block_on(async { Rustycache::lfu(16, cap, ttl, interval) });
        let mut i = 0;
        b.iter(|| {
            cache.put(black_box(i.to_string()), black_box(i.to_string()));
            i += 1;
        });
    });

    group.bench_function("get", |b| {
        let cache = rt.block_on(async {
            let c = Rustycache::lfu(16, cap, ttl, interval);
            for i in 0..cap {
                c.put(i.to_string(), i.to_string());
            }
            c
        });
        let mut i = 0;
        b.iter(|| {
            cache.get(black_box(&(i % cap).to_string()));
            i += 1;
        });
    });

    group.finish();
}

fn bench_key_sizes(c: &mut Criterion) {
    let cap = 1000;
    let ttl = Duration::from_secs(60);
    let interval = Duration::from_secs(60);
    let rt = tokio::runtime::Runtime::new().unwrap();

    let mut group = c.benchmark_group("Key_Sizes");

    for size in [16, 256, 4096] {
        let key = "a".repeat(size);
        
        group.bench_with_input(format!("put_{}_bytes", size), &key, |b, k| {
            let cache = rt.block_on(async { Rustycache::lru(1, cap, ttl, interval) });
            b.iter(|| {
                cache.put(black_box(k.clone()), black_box("value".to_string()));
            });
        });

        group.bench_with_input(format!("get_{}_bytes", size), &key, |b, k| {
            let cache = rt.block_on(async { 
                let c = Rustycache::lru(1, cap, ttl, interval);
                c.put(k.clone(), "value".to_string());
                c
            });
            b.iter(|| {
                cache.get(black_box(k));
            });
        });
    }

    group.finish();
}

criterion_group!(benches, bench_lru, bench_fifo, bench_lfu, bench_key_sizes);
criterion_main!(benches);
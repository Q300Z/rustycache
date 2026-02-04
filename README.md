# RustyCache
![Rust](https://img.shields.io/badge/Rust-lang-000000.svg?style=flat&logo=rust)
[![Crates.io](https://img.shields.io/crates/v/rustycache.svg)](https://crates.io/crates/rustycache)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

**RustyCache** is a high-performance, sharded, and thread-safe caching library for Rust. Designed for high-concurrency workloads, it features constant-time eviction algorithms and zero-cost abstractions.

## 🚀 Performance & Optimizations

RustyCache has been engineered for maximum throughput and minimum latency:

- **O(1) Eviction Algorithms**: LRU and FIFO strategies use a custom doubly linked list integrated into the hash map, ensuring all operations (`put`, `get`, `remove`) run in constant time regardless of cache size.
- **Sharded Locking**: Uses internal partitioning (sharding) to reduce lock contention. Multiple threads can access different shards simultaneously without blocking each other.
- **Static Dispatch**: Generic architecture eliminates the overhead of dynamic dispatch (`Box<dyn>`), allowing the compiler to inline code down to the storage layer.
- **Fast Hashing**: Powered by **AHash**, the fastest non-cryptographic hasher for Rust.
- **Optimized Mutexes**: Uses **Parking Lot** for faster, smaller, and more robust synchronization primitives.

## ✨ Features

- **Multiple Strategies**:
    - `LRU` (Least Recently Used)
    - `LFU` (Least Frequently Used)
    - `FIFO` (First In First Out)
- **Time-To-Live (TTL)**: Automatic entry expiration.
- **Hybrid Async/Sync**:
    - **Async Mode**: Background worker task for proactive expiration cleaning.
    - **Sync Mode**: Zero-dependency, passive expiration for low-overhead environments.
- **Thread-Safe**: Designed from the ground up for concurrent access.
- **Generic**: Works with any key `K` and value `V` that implement `Clone + Hash + Eq`.

## 📦 Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
# Default: async feature enabled (requires tokio)
rustycache = "1.0"

# Or for a pure synchronous environment (no tokio)
# rustycache = { version = "1.0", default-features = false }
```

## 🛠 Usage

### Asynchronous Mode (Default)
Ideal for applications already using `tokio`. Includes a background task that cleans expired entries.

```rust
use rustycache::rustycache::Rustycache;
use std::time::Duration;

#[tokio::main]
async fn main() {
    // 16 shards, 10k capacity, 5m TTL, 60s cleanup interval
    let cache = Rustycache::lru(16, 10000, Duration::from_secs(300), Duration::from_secs(60));

    cache.put("key".to_string(), "value".to_string());
    let val = cache.get(&"key".to_string());
}
```

### Synchronous Mode
Zero dependencies on an async runtime. Expiration is handled passively during `get` calls.

```rust
use rustycache::rustycache::Rustycache;
use std::time::Duration;

fn main() {
    // 8 shards, 1k capacity, 1m TTL
    let cache = Rustycache::lru_sync(8, 1000, Duration::from_secs(60));

    cache.put("key".to_string(), 42);
    assert_eq!(cache.get(&"key".to_string()), Some(42));
}
```

## 📊 Benchmarks

Measured on 10,000 elements with 16 shards:

| Operation | Strategy | Latency | Complexity |
| :--- | :--- | :--- | :--- |
| **Get (Hit)** | LRU | **~240 ns** | **O(1)** |
| **Get (Hit)** | FIFO | **~115 ns** | **O(1)** |
| **Get (Hit)** | LFU | **~195 ns** | O(log N) |

### Throughput (Scaling)
Thanks to sharding, RustyCache scales linearly with your CPU cores:
- **1 Thread**: ~4.0 Million ops/sec
- **8 Threads**: **~8.6 Million ops/sec** (on 8-core machine)

## 🧪 Testing

The library is strictly tested with **~98% code coverage**:

```bash
# Run all tests
cargo test

# Run tests without default features (Sync mode only)
cargo test --no-default-features
```

## 📜 License

MIT License - see [LICENSE](LICENSE) for details.
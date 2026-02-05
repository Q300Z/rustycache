# RustyCache
![Rust](https://img.shields.io/badge/Rust-lang-000000.svg?style=flat&logo=rust)
[![Crates.io](https://img.shields.io/crates/v/rustycache.svg)](https://crates.io/crates/rustycache)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

**RustyCache** is an ultra-high-performance, sharded, and thread-safe caching library for Rust. Engineered for extreme concurrency, it features index-based O(1) eviction algorithms and zero-cost abstractions.

## 🚀 Performance & Optimizations

RustyCache is optimized for modern CPU architectures and high-throughput requirements:

- **Index-based Arena (O(1))**: LRU and FIFO strategies use a `Vec`-based arena with `usize` indices for the doubly linked list. This **eliminates key cloning** during priority updates, drastically reducing CPU overhead and memory pressure.
- **Sharded Locking**: Internal partitioning (sharding) minimizes lock contention, allowing linear scaling with CPU core counts.
- **Static Dispatch**: A fully generic architecture removes dynamic dispatch (`Box<dyn>`) overhead, enabling deep compiler inlining.
- **Fast Hashing**: Powered by **AHash**, the most efficient non-cryptographic hasher available for Rust.
- **Optimized Mutexes**: Uses **Parking Lot** for fast, compact, and non-poisoning synchronization primitives.

## ✨ Features

- **Multiple Strategies**:
    - `LRU` (Least Recently Used)
    - `LFU` (Least Frequently Used)
    - `FIFO` (First In First Out)
- **Time-To-Live (TTL)**: Automatic entry expiration.
- **Hybrid Async/Sync**:
    - **Async Mode**: Background worker task for proactive expiration cleaning (requires `tokio`).
    - **Sync Mode**: Zero-dependency, passive expiration for ultra-low-overhead environments.
- **Thread-Safe**: Designed for safe concurrent access.
- **Generic**: Supports any key `K` and value `V` that implement `Clone + Hash + Eq`.

## 📦 Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
# Default: async feature enabled
rustycache = "1.0"

# For pure synchronous environments (no tokio)
# rustycache = { version = "1.0", default-features = false }
```

## 🛠 Usage

### Asynchronous Mode (Default)
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
```rust
use rustycache::rustycache::Rustycache;
use std::time::Duration;

fn main() {
    let cache = Rustycache::lru_sync(8, 1000, Duration::from_secs(60));
    cache.put("key".to_string(), 42);
    assert_eq!(cache.get(&"key".to_string()), Some(42));
}
```

## 📊 Benchmarks

*Results measured on 10,000 elements with 16 shards.*

| Operation | Strategy | Latency | Complexity |
| :--- | :--- | :--- | :--- |
| **Get (Hit)** | LRU | **~128 ns** | **O(1)** |
| **Get (Hit)** | FIFO | **~117 ns** | **O(1)** |
| **Get (Hit)** | LFU | **~205 ns** | O(log N) |

### Throughput (Scaling)
RustyCache scales exceptionally well under heavy thread contention:
- **1 Thread**: ~8.0 Million ops/sec
- **8 Threads**: **~23.0 Million ops/sec** (on 8-core machine)

*Note: For large keys (e.g., 4KB), performance is dominated by hashing (~700ns), regardless of the strategy.*

## 🧪 Testing

```bash
cargo test
cargo test --no-default-features
```

## 📜 License

MIT License - see [LICENSE](LICENSE) file for details.

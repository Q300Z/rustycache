pub mod fifo;
pub mod lfu;
pub mod lru;

use std::time::Duration;

pub trait CacheStrategy<K, V>: Send + Sync {
    fn put(&self, key: K, value: V);
    fn get(&self, key: &K) -> Option<V>;
    fn remove(&self, key: &K);
    fn contains(&self, key: &K) -> bool;
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool;
    fn clear(&self);
    #[cfg(feature = "async")]
    fn start_cleaner(&self, interval: Duration);
    #[cfg(feature = "async")]
    fn stop_cleaner(&self);
}

pub enum StrategyType {
    LRU,
    FIFO,
    LFU,
}

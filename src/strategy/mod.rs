pub mod fifo;
pub mod lfu;
pub mod lru;

use std::borrow::Borrow;
use std::hash::Hash;
use std::time::Duration;

pub trait CacheStrategy<K, V>: Send + Sync {
    fn put(&self, key: K, value: V);
    fn get<Q>(&self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized;
    fn remove<Q>(&self, key: &Q)
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized;
    fn contains<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized;
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool;
    fn clear(&self);
    #[cfg(feature = "async")]
    fn start_cleaner(&self, interval: Duration);
    #[cfg(feature = "async")]
    fn stop_cleaner(&self);
}
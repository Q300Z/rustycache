pub mod fifo;
pub mod lfu;
pub mod lru;

use std::borrow::Borrow;
use std::hash::Hash;
use std::time::Duration;

/// Common interface for all caching strategies.
///
/// This trait defines the core operations that any cache strategy must implement,
/// such as putting, getting, and removing entries.
pub trait CacheStrategy<K, V>: Send + Sync {
    /// Inserts a key-value pair into the cache.
    fn put(&self, key: K, value: V);

    /// Retrieves a value from the cache by its key.
    ///
    /// Supports [Borrow] lookups for zero-allocation searches.
    fn get<Q>(&self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized;

    /// Removes an entry from the cache.
    fn remove<Q>(&self, key: &Q)
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized;

    /// Checks if a key exists in the cache.
    fn contains<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized;

    /// Returns the number of entries in the cache.
    fn len(&self) -> usize;

    /// Returns true if the cache is empty.
    fn is_empty(&self) -> bool;

    /// Clears all entries from the cache.
    fn clear(&self);

    /// Starts the background task for cleaning expired entries.
    ///
    /// Requires the `async` feature and a running Tokio runtime.
    #[cfg(feature = "async")]
    fn start_cleaner(&self, interval: Duration);

    /// Stops the background task.
    #[cfg(feature = "async")]
    fn stop_cleaner(&self);
}

use crate::strategy::fifo::FIFOCache;
use crate::strategy::lfu::LFUCache;
use crate::strategy::lru::LRUCache;
use crate::strategy::CacheStrategy;
use ahash::RandomState;
use std::borrow::Borrow;
use std::hash::{BuildHasher, Hash, Hasher};
use std::time::Duration;

/// A high-performance, sharded, and thread-safe cache.
pub struct Rustycache<K, V, S> {
    shards: Vec<S>,
    hasher: RandomState,
    _phantom: std::marker::PhantomData<(K, V)>,
}

impl<K, V, S> Rustycache<K, V, S>
where
    K: 'static + Send + Sync + Clone + Eq + Hash,
    V: 'static + Send + Sync + Clone,
    S: CacheStrategy<K, V>,
{
    pub fn new(num_shards: usize, shard_factory: impl Fn() -> S) -> Self {
        let mut shards = Vec::with_capacity(num_shards);
        for _ in 0..num_shards {
            shards.push(shard_factory());
        }

        Rustycache {
            shards,
            hasher: RandomState::new(),
            _phantom: std::marker::PhantomData,
        }
    }

    #[inline]
    fn get_shard<Q: ?Sized>(&self, key: &Q) -> &S
    where
        K: Borrow<Q>,
        Q: Hash + Eq,
    {
        let mut s = self.hasher.build_hasher();
        key.hash(&mut s);
        let hash = s.finish();
        &self.shards[(hash as usize) % self.shards.len()]
    }

    #[inline]
    pub fn put(&self, key: K, value: V) {
        // For put, we use K directly so we can hash it
        let mut s = self.hasher.build_hasher();
        key.hash(&mut s);
        let hash = s.finish();
        self.shards[(hash as usize) % self.shards.len()].put(key, value);
    }

    #[inline]
    pub fn get<Q: ?Sized>(&self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq,
    {
        self.get_shard(key).get(key)
    }

    #[inline]
    pub fn remove<Q: ?Sized>(&self, key: &Q)
    where
        K: Borrow<Q>,
        Q: Hash + Eq,
    {
        self.get_shard(key).remove(key)
    }

    #[inline]
    pub fn contains<Q: ?Sized>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Hash + Eq,
    {
        self.get_shard(key).contains(key)
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.shards.iter().map(|s| s.len()).sum()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.shards.iter().all(|s| s.is_empty())
    }

    #[inline]
    pub fn clear(&self) {
        for shard in &self.shards {
            shard.clear();
        }
    }
}

impl<K, V> Rustycache<K, V, LRUCache<K, V>>
where
    K: 'static + Send + Sync + Clone + Eq + Hash,
    V: 'static + Send + Sync + Clone,
{
    #[cfg(feature = "async")]
    pub fn lru(num_shards: usize, capacity: usize, ttl: Duration, clean_interval: Duration) -> Self {
        Self::new(num_shards, move || {
            let shard = LRUCache::new(capacity / num_shards + 1, ttl, clean_interval);
            shard.start_cleaner(clean_interval);
            shard
        })
    }

    pub fn lru_sync(num_shards: usize, capacity: usize, ttl: Duration) -> Self {
        Self::new(num_shards, move || {
            LRUCache::new(capacity / num_shards + 1, ttl, Duration::from_secs(0))
        })
    }
}

impl<K, V> Rustycache<K, V, FIFOCache<K, V>>
where
    K: 'static + Send + Sync + Clone + Eq + Hash,
    V: 'static + Send + Sync + Clone,
{
    #[cfg(feature = "async")]
    pub fn fifo(num_shards: usize, capacity: usize, ttl: Duration, clean_interval: Duration) -> Self {
        Self::new(num_shards, move || {
            let shard = FIFOCache::new(capacity / num_shards + 1, ttl, clean_interval);
            shard.start_cleaner(clean_interval);
            shard
        })
    }

    pub fn fifo_sync(num_shards: usize, capacity: usize, ttl: Duration) -> Self {
        Self::new(num_shards, move || {
            FIFOCache::new(capacity / num_shards + 1, ttl, Duration::from_secs(0))
        })
    }
}

impl<K, V> Rustycache<K, V, LFUCache<K, V>>
where
    K: 'static + Send + Sync + Clone + Eq + Hash,
    V: 'static + Send + Sync + Clone,
{
    #[cfg(feature = "async")]
    pub fn lfu(num_shards: usize, capacity: usize, ttl: Duration, clean_interval: Duration) -> Self {
        Self::new(num_shards, move || {
            let shard = LFUCache::new(capacity / num_shards + 1, ttl, clean_interval);
            shard.start_cleaner(clean_interval);
            shard
        })
    }

    pub fn lfu_sync(num_shards: usize, capacity: usize, ttl: Duration) -> Self {
        Self::new(num_shards, move || {
            LFUCache::new(capacity / num_shards + 1, ttl, Duration::from_secs(0))
        })
    }
}
use ahash::AHashMap as HashMap;
use ahash::AHashSet as HashSet;
use parking_lot::Mutex;
use std::borrow::Borrow;
use std::collections::BTreeMap;
use std::hash::Hash;
use std::sync::Arc;
use std::time::Duration;

use crate::strategy::CacheStrategy;
use chrono::{DateTime, Utc};
#[cfg(feature = "async")]
use tokio::sync::Notify;
#[cfg(feature = "async")]
use tokio::task;
#[cfg(feature = "async")]
use tokio::time::sleep;

struct CacheEntry<K, V> {
    key: K,
    value: V,
    expires_at: DateTime<Utc>,
    frequency: usize,
}

#[derive(Clone)]
pub struct LFUCache<K, V>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    capacity: usize,
    ttl: Duration,
    map: Arc<Mutex<HashMap<K, CacheEntry<K, V>>>>,
    freq_map: Arc<Mutex<BTreeMap<usize, HashSet<K>>>>,
    #[cfg(feature = "async")]
    notify_stop: Arc<Notify>,
}

impl<K, V> LFUCache<K, V>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    pub fn new(capacity: usize, ttl: Duration, _clean_interval: Duration) -> Self {
        LFUCache {
            capacity,
            ttl,
            map: Arc::new(Mutex::new(HashMap::default())),
            freq_map: Arc::new(Mutex::new(BTreeMap::new())),
            #[cfg(feature = "async")]
            notify_stop: Arc::new(Notify::new()),
        }
    }

    fn remove_entry_internal<Q>(key: &Q, freq: usize, map: &mut HashMap<K, CacheEntry<K, V>>, freq_map: &mut BTreeMap<usize, HashSet<K>>)
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        map.remove(key);
        if let Some(set) = freq_map.get_mut(&freq) {
            set.remove(key);
            if set.is_empty() {
                freq_map.remove(&freq);
            }
        }
    }
}

impl<K, V> CacheStrategy<K, V> for LFUCache<K, V>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    #[inline]
    fn put(&self, key: K, value: V) {
        let mut map = self.map.lock();
        let mut freq_map = self.freq_map.lock();

        if let Some(entry) = map.get_mut(&key) {
            entry.value = value;
            entry.expires_at = Utc::now() + chrono::Duration::from_std(self.ttl).unwrap();
            return;
        }

        if map.len() >= self.capacity {
            let to_remove = if let Some((&min_freq, keys)) = freq_map.iter().next() {
                keys.iter().next().cloned().map(|k| (k, min_freq))
            } else {
                None
            };

            if let Some((k, freq)) = to_remove {
                Self::remove_entry_internal(&k, freq, &mut map, &mut freq_map);
            }
        }

        map.insert(key.clone(), CacheEntry {
            key: key.clone(),
            value,
            expires_at: Utc::now() + chrono::Duration::from_std(self.ttl).unwrap(),
            frequency: 1,
        });

        freq_map.entry(1).or_insert_with(HashSet::default).insert(key);
    }

    #[inline]
    fn get<Q>(&self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let mut map = self.map.lock();
        let mut freq_map = self.freq_map.lock();

        if let Some(entry) = map.get_mut(key) {
            let freq = entry.frequency;
            if entry.expires_at <= Utc::now() {
                Self::remove_entry_internal(key, freq, &mut map, &mut freq_map);
                return None;
            }

            let old_freq = freq;
            entry.frequency += 1;
            let new_freq = entry.frequency;

            if let Some(set) = freq_map.get_mut(&old_freq) {
                set.remove(key);
                if set.is_empty() {
                    freq_map.remove(&old_freq);
                }
            }

            let k_clone = entry.key.clone();
            freq_map
                .entry(new_freq)
                .or_insert_with(HashSet::default)
                .insert(k_clone);

            return Some(entry.value.clone());
        }

        None
    }

    #[inline]
    fn remove<Q>(&self, key: &Q)
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let mut map = self.map.lock();
        let mut freq_map = self.freq_map.lock();

        if let Some(entry) = map.get(key) {
            let freq = entry.frequency;
            Self::remove_entry_internal(key, freq, &mut map, &mut freq_map);
        }
    }

    #[inline]
    fn contains<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let map = self.map.lock();
        map.contains_key(key)
    }

    #[inline]
    fn len(&self) -> usize {
        let map = self.map.lock();
        map.len()
    }
    #[inline]
    fn is_empty(&self) -> bool {
        let map = self.map.lock();
        map.is_empty()
    }
    #[inline]
    fn clear(&self) {
        let mut map = self.map.lock();
        let mut freq_map = self.freq_map.lock();
        map.clear();
        freq_map.clear();
    }

    #[cfg(feature = "async")]
    fn start_cleaner(&self, clean_interval: Duration) {
        let map_clone = Arc::clone(&self.map);
        let freq_map_clone = Arc::clone(&self.freq_map);
        let notify_clone = Arc::clone(&self.notify_stop);

        task::spawn(async move {
            loop {
                tokio::select! {
                    _ = sleep(clean_interval) => {
                        let now = Utc::now();
                        let mut map = map_clone.lock();
                        let mut freq_map = freq_map_clone.lock();
                        
                        let keys_to_remove: Vec<K> = map.iter()
                            .filter_map(|(k, v)| {
                                if v.expires_at <= now {
                                    Some(k.clone())
                                } else {
                                    None
                                }
                            })
                            .collect();

                        for key in keys_to_remove {
                            if let Some(entry) = map.get(&key) {
                                let freq = entry.frequency;
                                Self::remove_entry_internal(&key, freq, &mut map, &mut freq_map);
                            }
                        }
                    }
                    _ = notify_clone.notified() => {
                        break;
                    }
                }
            }
        });
    }

    #[cfg(feature = "async")]
    fn stop_cleaner(&self) {
        self.notify_stop.notify_waiters();
    }
}

#[cfg(feature = "async")]
impl<K, V> Drop for LFUCache<K, V>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    fn drop(&mut self) {
        self.stop_cleaner();
    }
}

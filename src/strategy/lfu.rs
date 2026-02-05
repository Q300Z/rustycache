use ahash::AHashMap as HashMap;
use ahash::AHashSet as HashSet;
use parking_lot::RwLock;
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

struct LFUState<K, V> {
    map: HashMap<K, CacheEntry<K, V>>,
    freq_map: BTreeMap<usize, HashSet<K>>,
}

#[derive(Clone)]
pub struct LFUCache<K, V>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    capacity: usize,
    ttl: Duration,
    state: Arc<RwLock<LFUState<K, V>>>,
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
            state: Arc::new(RwLock::new(LFUState {
                map: HashMap::default(),
                freq_map: BTreeMap::new(),
            })),
            #[cfg(feature = "async")]
            notify_stop: Arc::new(Notify::new()),
        }
    }

    fn remove_entry_internal<Q>(key: &Q, freq: usize, state: &mut LFUState<K, V>)
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        state.map.remove(key);
        if let Some(set) = state.freq_map.get_mut(&freq) {
            set.remove(key);
            if set.is_empty() {
                state.freq_map.remove(&freq);
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
        let mut state = self.state.write();

        if let Some(entry) = state.map.get_mut(&key) {
            entry.value = value;
            entry.expires_at = Utc::now() + chrono::Duration::from_std(self.ttl).unwrap();
            return;
        }

        if state.map.len() >= self.capacity {
            let to_remove = if let Some((&min_freq, keys)) = state.freq_map.iter().next() {
                keys.iter().next().cloned().map(|k| (k, min_freq))
            } else {
                None
            };

            if let Some((k, freq)) = to_remove {
                Self::remove_entry_internal(&k, freq, &mut state);
            }
        }

        state.map.insert(
            key.clone(),
            CacheEntry {
                key: key.clone(),
                value,
                expires_at: Utc::now() + chrono::Duration::from_std(self.ttl).unwrap(),
                frequency: 1,
            },
        );

        state.freq_map.entry(1).or_default().insert(key);
    }

    #[inline]
    fn get<Q>(&self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let mut state = self.state.write();

        let (old_freq, new_freq, val, k_clone) = if let Some(entry) = state.map.get_mut(key) {
            let freq = entry.frequency;
            if entry.expires_at <= Utc::now() {
                Self::remove_entry_internal(key, freq, &mut state);
                return None;
            }

            entry.frequency += 1;
            (
                freq,
                entry.frequency,
                entry.value.clone(),
                entry.key.clone(),
            )
        } else {
            return None;
        };

        // Update freq_map
        if let Some(set) = state.freq_map.get_mut(&old_freq) {
            set.remove(key);
            if set.is_empty() {
                state.freq_map.remove(&old_freq);
            }
        }

        state.freq_map.entry(new_freq).or_default().insert(k_clone);

        Some(val)
    }

    #[inline]
    fn remove<Q>(&self, key: &Q)
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let mut state = self.state.write();

        if let Some(entry) = state.map.get(key) {
            let freq = entry.frequency;
            Self::remove_entry_internal(key, freq, &mut state);
        }
    }

    #[inline]
    fn contains<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let state = self.state.read();
        state.map.contains_key(key)
    }

    #[inline]
    fn len(&self) -> usize {
        let state = self.state.read();
        state.map.len()
    }
    #[inline]
    fn is_empty(&self) -> bool {
        let state = self.state.read();
        state.map.is_empty()
    }
    #[inline]
    fn clear(&self) {
        let mut state = self.state.write();
        state.map.clear();
        state.freq_map.clear();
    }

    #[cfg(feature = "async")]
    fn start_cleaner(&self, clean_interval: Duration) {
        let state_clone = Arc::clone(&self.state);
        let notify_clone = Arc::clone(&self.notify_stop);

        task::spawn(async move {
            loop {
                tokio::select! {
                    _ = sleep(clean_interval) => {
                        let now = Utc::now();
                        let mut state = state_clone.write();

                        let keys_to_remove: Vec<(K, usize)> = state.map.iter()
                            .filter_map(|(k, v)| {
                                if v.expires_at <= now {
                                    Some((k.clone(), v.frequency))
                                } else {
                                    None
                                }
                            })
                            .collect();

                        for (key, freq) in keys_to_remove {
                            Self::remove_entry_internal(&key, freq, &mut state);
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
use ahash::AHashMap as HashMap;
use parking_lot::Mutex;
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

struct Node<K, V> {
    value: V,
    expires_at: DateTime<Utc>,
    prev: Option<K>,
    next: Option<K>,
}

struct LRUState<K, V> {
    map: HashMap<K, Node<K, V>>,
    head: Option<K>,
    tail: Option<K>,
}

#[derive(Clone)]
pub struct LRUCache<K, V>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    capacity: usize,
    ttl: Duration,
    state: Arc<Mutex<LRUState<K, V>>>,
    #[cfg(feature = "async")]
    notify_stop: Arc<Notify>,
}

impl<K, V> LRUCache<K, V>
where
    K: Eq + Hash + Clone + Send + 'static + Sync,
    V: Clone + Send + 'static + Sync,
{
    pub fn new(capacity: usize, ttl: Duration, _clean_interval: Duration) -> Self {
        LRUCache {
            capacity,
            ttl,
            state: Arc::new(Mutex::new(LRUState {
                map: HashMap::default(),
                head: None,
                tail: None,
            })),
            #[cfg(feature = "async")]
            notify_stop: Arc::new(Notify::new()),
        }
    }

    fn detach_node(state: &mut LRUState<K, V>, key: &K) {
        let (prev, next) = {
            let node = state.map.get(key).unwrap();
            (node.prev.clone(), node.next.clone())
        };

        if let Some(ref p) = prev {
            state.map.get_mut(p).unwrap().next = next.clone();
        } else {
            state.head = next.clone();
        }

        if let Some(ref n) = next {
            state.map.get_mut(n).unwrap().prev = prev;
        } else {
            state.tail = prev;
        }
    }

    fn push_front(state: &mut LRUState<K, V>, key: K) {
        let old_head = state.head.take();
        if let Some(ref oh) = old_head {
            state.map.get_mut(oh).unwrap().prev = Some(key.clone());
        } else {
            state.tail = Some(key.clone());
        }

        let node = state.map.get_mut(&key).unwrap();
        node.prev = None;
        node.next = old_head.clone();
        state.head = Some(key);
    }
}

impl<K, V> CacheStrategy<K, V> for LRUCache<K, V>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    #[inline]
    fn put(&self, key: K, value: V) {
        let mut state = self.state.lock();
        let expires_at = Utc::now() + chrono::Duration::from_std(self.ttl).unwrap();

        if state.map.contains_key(&key) {
            Self::detach_node(&mut state, &key);
            let node = state.map.get_mut(&key).unwrap();
            node.value = value;
            node.expires_at = expires_at;
        } else {
            if state.map.len() >= self.capacity {
                if let Some(oldest_key) = state.tail.clone() {
                    Self::detach_node(&mut state, &oldest_key);
                    state.map.remove(&oldest_key);
                }
            }
            state.map.insert(
                key.clone(),
                Node {
                    value,
                    expires_at,
                    prev: None,
                    next: None,
                },
            );
        }
        Self::push_front(&mut state, key);
    }

    #[inline]
    fn get(&self, key: &K) -> Option<V> {
        let mut state = self.state.lock();
        if let Some(entry) = state.map.get(key) {
            if entry.expires_at > Utc::now() {
                let val = entry.value.clone();
                let key_clone = key.clone();
                Self::detach_node(&mut state, &key_clone);
                Self::push_front(&mut state, key_clone);
                return Some(val);
            } else {
                let key_clone = key.clone();
                Self::detach_node(&mut state, &key_clone);
                state.map.remove(&key_clone);
            }
        }
        None
    }

    #[inline]
    fn remove(&self, key: &K) {
        let mut state = self.state.lock();
        if state.map.contains_key(key) {
            Self::detach_node(&mut state, key);
            state.map.remove(key);
        }
    }

    fn contains(&self, key: &K) -> bool {
        let state = self.state.lock();
        state.map.contains_key(key)
    }

    fn len(&self) -> usize {
        let state = self.state.lock();
        state.map.len()
    }

    fn is_empty(&self) -> bool {
        let state = self.state.lock();
        state.map.is_empty()
    }

    fn clear(&self) {
        let mut state = self.state.lock();
        state.map.clear();
        state.head = None;
        state.tail = None;
    }

    #[cfg(feature = "async")]
    fn start_cleaner(&self, clean_interval: Duration) {
        let state_clone = Arc::clone(&self.state);
        let notify = Arc::clone(&self.notify_stop);

        task::spawn(async move {
            loop {
                tokio::select! {
                    _ = sleep(clean_interval) => {
                        let now = Utc::now();
                        let mut state = state_clone.lock();
                        let mut keys_to_remove = Vec::new();
                        
                        for (key, node) in state.map.iter() {
                            if node.expires_at <= now {
                                keys_to_remove.push(key.clone());
                            }
                        }

                        for key in keys_to_remove {
                            Self::detach_node(&mut state, &key);
                            state.map.remove(&key);
                        }
                    }
                    _ = notify.notified() => {
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
impl<K, V> Drop for LRUCache<K, V>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    fn drop(&mut self) {
        self.stop_cleaner();
    }
}

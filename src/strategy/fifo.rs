use ahash::AHashMap as HashMap;
use parking_lot::Mutex;
use std::borrow::Borrow;
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
    key: K,
    value: V,
    expires_at: DateTime<Utc>,
    prev: Option<usize>,
    next: Option<usize>,
}

struct FIFOState<K, V> {
    map: HashMap<K, usize>,
    nodes: Vec<Option<Node<K, V>>>,
    free_indices: Vec<usize>,
    head: Option<usize>,
    tail: Option<usize>,
}

#[derive(Clone)]
pub struct FIFOCache<K, V>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    capacity: usize,
    ttl: Duration,
    state: Arc<Mutex<FIFOState<K, V>>>,
    #[cfg(feature = "async")]
    notify_stop: Arc<Notify>,
}

impl<K, V> FIFOCache<K, V>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    pub fn new(capacity: usize, ttl: Duration, _clean_interval: Duration) -> Self {
        FIFOCache {
            capacity,
            ttl,
            state: Arc::new(Mutex::new(FIFOState {
                map: HashMap::default(),
                nodes: Vec::with_capacity(capacity),
                free_indices: Vec::new(),
                head: None,
                tail: None,
            })),
            #[cfg(feature = "async")]
            notify_stop: Arc::new(Notify::new()),
        }
    }

    fn detach_node(state: &mut FIFOState<K, V>, node_idx: usize) {
        let (prev, next) = {
            let node = state.nodes[node_idx].as_ref().unwrap();
            (node.prev, node.next)
        };

        if let Some(p) = prev {
            state.nodes[p].as_mut().unwrap().next = next;
        } else {
            state.head = next;
        }

        if let Some(n) = next {
            state.nodes[n].as_mut().unwrap().prev = prev;
        } else {
            state.tail = prev;
        }
    }

    fn push_back(state: &mut FIFOState<K, V>, node_idx: usize) {
        let old_tail = state.tail;
        if let Some(ot) = old_tail {
            state.nodes[ot].as_mut().unwrap().next = Some(node_idx);
        } else {
            state.head = Some(node_idx);
        }

        let node = state.nodes[node_idx].as_mut().unwrap();
        node.next = None;
        node.prev = old_tail;
        state.tail = Some(node_idx);
    }

    fn remove_node_internal(state: &mut FIFOState<K, V>, node_idx: usize) {
        Self::detach_node(state, node_idx);
        if let Some(node) = state.nodes[node_idx].take() {
            state.map.remove(&node.key);
            state.free_indices.push(node_idx);
        }
    }
}

impl<K, V> CacheStrategy<K, V> for FIFOCache<K, V>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    #[inline]
    fn put(&self, key: K, value: V) {
        let mut state = self.state.lock();
        if state.map.contains_key(&key) {
            return;
        }

        if state.map.len() >= self.capacity {
            if let Some(oldest_idx) = state.head {
                Self::remove_node_internal(&mut state, oldest_idx);
            }
        }

        let expires_at = Utc::now() + chrono::Duration::from_std(self.ttl).unwrap();
        
        let node_idx = if let Some(idx) = state.free_indices.pop() {
            state.nodes[idx] = Some(Node {
                key: key.clone(),
                value,
                expires_at,
                prev: None,
                next: None,
            });
            idx
        } else {
            let idx = state.nodes.len();
            state.nodes.push(Some(Node {
                key: key.clone(),
                value,
                expires_at,
                prev: None,
                next: None,
            }));
            idx
        };

        state.map.insert(key, node_idx);
        Self::push_back(&mut state, node_idx);
    }

    #[inline]
    fn get<Q>(&self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let mut state = self.state.lock();
        if let Some(&node_idx) = state.map.get(key) {
            let expired = state.nodes[node_idx].as_ref().unwrap().expires_at <= Utc::now();
            if !expired {
                return Some(state.nodes[node_idx].as_ref().unwrap().value.clone());
            } else {
                Self::remove_node_internal(&mut state, node_idx);
            }
        }
        None
    }

    #[inline]
    fn remove<Q>(&self, key: &Q)
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let mut state = self.state.lock();
        if let Some(&node_idx) = state.map.get(key) {
            Self::remove_node_internal(&mut state, node_idx);
        }
    }

    #[inline]
    fn contains<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let state = self.state.lock();
        state.map.contains_key(key)
    }

    #[inline]
    fn len(&self) -> usize {
        let state = self.state.lock();
        state.map.len()
    }

    #[inline]
    fn is_empty(&self) -> bool {
        let state = self.state.lock();
        state.map.is_empty()
    }

    #[inline]
    fn clear(&self) {
        let mut state = self.state.lock();
        state.map.clear();
        state.nodes.clear();
        state.free_indices.clear();
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
                        let mut indices_to_remove = Vec::new();
                        
                        for (_, &idx) in state.map.iter() {
                            if let Some(node) = &state.nodes[idx] {
                                if node.expires_at <= now {
                                    indices_to_remove.push(idx);
                                }
                            }
                        }

                        for idx in indices_to_remove {
                            Self::remove_node_internal(&mut state, idx);
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
impl<K, V> Drop for FIFOCache<K, V>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    fn drop(&mut self) {
        self.stop_cleaner();
    }
}
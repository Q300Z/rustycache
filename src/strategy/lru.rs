use ahash::AHashMap as HashMap;
use parking_lot::{Mutex, RwLock};
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

const BATCH_SIZE: usize = 64;

struct Node<K, V> {
    key: K,
    value: V,
    expires_at: DateTime<Utc>,
    prev: Option<u32>,
    next: Option<u32>,
}

struct LRUState<K, V> {
    map: HashMap<K, u32>,
    nodes: Vec<Option<Node<K, V>>>,
    free_indices: Vec<u32>,
    head: Option<u32>,
    tail: Option<u32>,
}

#[derive(Clone)]
pub struct LRUCache<K, V>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    capacity: usize,
    ttl: Duration,
    state: Arc<RwLock<LRUState<K, V>>>,
    access_buffer: Arc<Mutex<Vec<u32>>>,
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
            state: Arc::new(RwLock::new(LRUState {
                map: HashMap::default(),
                nodes: Vec::with_capacity(capacity),
                free_indices: Vec::new(),
                head: None,
                tail: None,
            })),
            access_buffer: Arc::new(Mutex::new(Vec::with_capacity(BATCH_SIZE))),
            #[cfg(feature = "async")]
            notify_stop: Arc::new(Notify::new()),
        }
    }

    fn detach_node(state: &mut LRUState<K, V>, node_idx: u32) {
        let (prev, next) = {
            let node = state.nodes[node_idx as usize].as_ref().unwrap();
            (node.prev, node.next)
        };

        if let Some(p) = prev {
            state.nodes[p as usize].as_mut().unwrap().next = next;
        } else {
            state.head = next;
        }

        if let Some(n) = next {
            state.nodes[n as usize].as_mut().unwrap().prev = prev;
        } else {
            state.tail = prev;
        }
    }

    fn push_front(state: &mut LRUState<K, V>, node_idx: u32) {
        let old_head = state.head;
        if let Some(oh) = old_head {
            state.nodes[oh as usize].as_mut().unwrap().prev = Some(node_idx);
        } else {
            state.tail = Some(node_idx);
        }

        let node = state.nodes[node_idx as usize].as_mut().unwrap();
        node.prev = None;
        node.next = old_head;
        state.head = Some(node_idx);
    }

    fn remove_node_internal(state: &mut LRUState<K, V>, node_idx: u32) {
        Self::detach_node(state, node_idx);
        if let Some(node) = state.nodes[node_idx as usize].take() {
            state.map.remove(&node.key);
            state.free_indices.push(node_idx);
        }
    }

    fn apply_access_batch(&self, state: &mut LRUState<K, V>) {
        let mut buffer = self.access_buffer.lock();
        for &node_idx in buffer.iter() {
            // Verify node still exists and wasn't removed/expired in the meantime
            if node_idx < state.nodes.len() as u32 && state.nodes[node_idx as usize].is_some() {
                Self::detach_node(state, node_idx);
                Self::push_front(state, node_idx);
            }
        }
        buffer.clear();
    }
}

impl<K, V> CacheStrategy<K, V> for LRUCache<K, V>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    #[inline]
    fn put(&self, key: K, value: V) {
        let mut state = self.state.write();
        
        // Always flush batch on put to maintain order before potential eviction
        self.apply_access_batch(&mut state);

        let expires_at = Utc::now() + chrono::Duration::from_std(self.ttl).unwrap();

        if let Some(&node_idx) = state.map.get(&key) {
            Self::detach_node(&mut state, node_idx);
            let node = state.nodes[node_idx as usize].as_mut().unwrap();
            node.value = value;
            node.expires_at = expires_at;
            Self::push_front(&mut state, node_idx);
        } else {
            if state.map.len() >= self.capacity {
                if let Some(oldest_idx) = state.tail {
                    Self::remove_node_internal(&mut state, oldest_idx);
                }
            }

            let node_idx = if let Some(idx) = state.free_indices.pop() {
                state.nodes[idx as usize] = Some(Node {
                    key: key.clone(),
                    value,
                    expires_at,
                    prev: None,
                    next: None,
                });
                idx
            } else {
                let idx = state.nodes.len() as u32;
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
            Self::push_front(&mut state, node_idx);
        }
    }

    #[inline]
    fn get<Q>(&self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        // Try read lock first
        {
            let state = self.state.read();
            if let Some(&node_idx) = state.map.get(key) {
                let node = state.nodes[node_idx as usize].as_ref().unwrap();
                if node.expires_at > Utc::now() {
                    let val = node.value.clone();
                    
                    // Record access in buffer
                    let mut buffer = self.access_buffer.lock();
                    buffer.push(node_idx);
                    let should_flush = buffer.len() >= BATCH_SIZE;
                    drop(buffer);

                    if should_flush {
                        drop(state); // Must release read lock before taking write lock
                        let mut state = self.state.write();
                        self.apply_access_batch(&mut state);
                    }
                    
                    return Some(val);
                }
            } else {
                return None;
            }
        }

        // Handle expiration (requires write lock)
        let mut state = self.state.write();
        if let Some(&node_idx) = state.map.get(key) {
            if state.nodes[node_idx as usize].as_ref().unwrap().expires_at <= Utc::now() {
                Self::remove_node_internal(&mut state, node_idx);
            } else {
                // Was not expired after all (race condition), just return it
                return Some(state.nodes[node_idx as usize].as_ref().unwrap().value.clone());
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
        let mut state = self.state.write();
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
        state.nodes.clear();
        state.free_indices.clear();
        state.head = None;
        state.tail = None;
        self.access_buffer.lock().clear();
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
                        let mut state = state_clone.write();
                        let mut indices_to_remove = Vec::new();
                        
                        for (_, &idx) in state.map.iter() {
                            if let Some(node) = &state.nodes[idx as usize] {
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
impl<K, V> Drop for LRUCache<K, V>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    fn drop(&mut self) {
        self.stop_cleaner();
    }
}

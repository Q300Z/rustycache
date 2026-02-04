#[cfg(test)]
mod fifo_tests {
    use rustycache::rustycache::Rustycache;
    use rustycache::strategy::fifo::FIFOCache;
    #[allow(unused_imports)]
    use rustycache::strategy::CacheStrategy;
    use std::time::Duration;
    #[cfg(feature = "async")]
    use tokio::time::sleep;

    fn create_cache(capacity: usize, ttl_secs: u64, clean_interval_secs: u64, _start_cleaner: bool) -> Rustycache<String, String, FIFOCache<String, String>> {
        let ttl = Duration::from_secs(ttl_secs);
        let interval = Duration::from_secs(clean_interval_secs);
        let s = FIFOCache::new(capacity, ttl, interval);
        #[cfg(feature = "async")]
        if _start_cleaner {
            s.start_cleaner(interval);
        }
        Rustycache::new(1, move || s.clone())
    }

    #[test]
    fn test_put_and_get_sync() {
        let cache = create_cache(2, 5, 60, false);
        cache.put("key1".to_string(), "value1".to_string());
        assert_eq!(cache.get(&"key1".to_string()), Some("value1".to_string()));
    }

    #[cfg(feature = "async")]
    #[tokio::test]
    async fn test_put_and_get_basic() {
        let cache = create_cache(2, 5, 60, true);
        cache.put("key1".to_string(), "value1".to_string());

        assert_eq!(cache.get(&"key1".to_string()), Some("value1".to_string()));
        assert!(cache.contains(&"key1".to_string()));
        assert_eq!(cache.len(), 1);
        assert!(!cache.is_empty());
    }

    #[cfg(feature = "async")]
    #[tokio::test]
    async fn test_put_does_not_update_existing_value() {
        let cache = create_cache(2, 5, 60, true);
        cache.put("key1".to_string(), "value1".to_string());
        cache.put("key1".to_string(), "value2".to_string()); // should be ignored

        assert_eq!(cache.get(&"key1".to_string()), Some("value1".to_string()));
    }

    #[cfg(feature = "async")]
    #[tokio::test]
    async fn test_fifo_eviction_order() {
        let cache = create_cache(2, 5, 60, true);
        cache.put("a".to_string(), "A".to_string());
        cache.put("b".to_string(), "B".to_string());
        cache.put("c".to_string(), "C".to_string()); // should evict "a"

        assert!(!cache.contains(&"a".to_string()));
        assert!(cache.contains(&"b".to_string()));
        assert!(cache.contains(&"c".to_string()));
        assert_eq!(cache.len(), 2);
    }

    #[cfg(feature = "async")]
    #[tokio::test]
    async fn test_expiration_removes_entry() {
        let cache = create_cache(2, 1, 60, true);
        cache.put("x".to_string(), "expire_me".to_string());

        assert_eq!(cache.get(&"x".to_string()), Some("expire_me".to_string()));

        sleep(Duration::from_secs(2)).await;

        // Now expired, get returns None and removes it internally
        assert_eq!(cache.get(&"x".to_string()), None);
        assert!(!cache.contains(&"x".to_string()));
    }

    #[cfg(feature = "async")]
    #[tokio::test]
    async fn test_remove_and_clear() {
        let cache = create_cache(3, 5, 60, true);
        cache.put("a".to_string(), "1".to_string());
        cache.put("b".to_string(), "2".to_string());
        cache.put("c".to_string(), "3".to_string());

        cache.remove(&"b".to_string());
        assert_eq!(cache.get(&"b".to_string()), None);
        assert_eq!(cache.len(), 2);

        cache.clear();
        assert_eq!(cache.len(), 0);
        assert!(cache.is_empty());
    }

    #[test]

    fn test_fifo_sync_constructor() {
        let cache = Rustycache::fifo_sync(1, 10, Duration::from_secs(60));

        cache.put("a".to_string(), "b".to_string());

        assert_eq!(cache.get(&"a".to_string()), Some("b".to_string()));
    }


    #[cfg(feature = "async")]
    #[tokio::test]

    async fn test_explicit_stop_cleaner() {
        let cache = create_cache(2, 1, 1, true);

        // Explicitly calling stop_cleaner to cover the code path

        // In our current API, Rustycache doesn't expose stop_cleaner directly,

        // but it's called on Drop. We can force a drop.

        drop(cache);
    }
}

    
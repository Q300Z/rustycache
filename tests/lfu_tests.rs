#[cfg(test)]
mod lfu_tests {
    use rustycache::rustycache::Rustycache;
    use rustycache::strategy::lfu::LFUCache;
    #[allow(unused_imports)]
    use rustycache::strategy::CacheStrategy;
    use std::time::Duration;
    #[cfg(feature = "async")]
    use tokio::time::sleep;

    fn create_cache(
        capacity: usize,
        ttl_secs: u64,
        clean_interval_secs: u64,
        _start_cleaner: bool,
    ) -> Rustycache<String, String, LFUCache<String, String>> {
        let ttl = Duration::from_secs(ttl_secs);
        let interval = Duration::from_secs(clean_interval_secs);
        let s = LFUCache::new(capacity, ttl, interval);
        #[cfg(feature = "async")]
        if _start_cleaner {
            s.start_cleaner(interval);
        }
        Rustycache::new(1, move || s.clone())
    }

    #[test]
    fn test_put_and_get_sync() {
        let cache = create_cache(10, 5, 60, false);
        cache.put("key1".to_string(), "value1".to_string());
        assert_eq!(cache.get(&"key1".to_string()), Some("value1".to_string()));
    }

    #[cfg(feature = "async")]
    #[tokio::test]
    async fn test_put_and_get_basic() {
        let cache = create_cache(10, 5, 60, true);
        cache.put("key1".to_string(), "value1".to_string());
        assert_eq!(cache.get(&"key1".to_string()), Some("value1".to_string()));
    }

    #[cfg(feature = "async")]
    #[tokio::test]
    async fn test_update_value_and_frequency() {
        let cache = create_cache(10, 5, 60, true);
        cache.put("key1".to_string(), "value1".to_string());
        cache.put("key1".to_string(), "value2".to_string());
        assert_eq!(cache.get(&"key1".to_string()), Some("value2".to_string()));
    }

    #[cfg(feature = "async")]
    #[tokio::test]
    async fn test_lfu_eviction() {
        let cache = create_cache(2, 5, 60, true);
        cache.put("a".to_string(), "A".to_string());
        cache.put("b".to_string(), "B".to_string());

        // increase frequency of "b"
        cache.get(&"b".to_string());

        // should evict "a" since it has lower frequency
        cache.put("c".to_string(), "C".to_string());

        assert!(!cache.contains(&"a".to_string()));
        assert!(cache.contains(&"b".to_string()));
        assert!(cache.contains(&"c".to_string()));
    }

    #[cfg(feature = "async")]
    #[tokio::test]
    async fn test_expiration_behavior() {
        let cache = create_cache(2, 1, 60, true);
        cache.put("x".to_string(), "expire_me".to_string());
        sleep(Duration::from_secs(2)).await;
        assert_eq!(cache.get(&"x".to_string()), None);
    }

    #[cfg(feature = "async")]
    #[tokio::test]
    async fn test_remove_and_clear() {
        let cache = create_cache(3, 5, 60, true);
        cache.put("a".to_string(), "1".to_string());
        cache.remove(&"a".to_string());
        assert!(cache.is_empty());

        cache.put("b".to_string(), "2".to_string());
        cache.clear();
        assert!(cache.is_empty());
    }

    #[cfg(feature = "async")]
    #[tokio::test]
    async fn test_cleaner_removes_expired() {
        let cache = create_cache(2, 1, 1, true);
        cache.put("k1".to_string(), "v1".to_string());
        sleep(Duration::from_secs(2)).await;
        assert_eq!(cache.len(), 0);
    }

    #[cfg(feature = "async")]
    #[tokio::test]
    async fn test_freq_map_cleanup_on_eviction() {
        let cache = create_cache(1, 10, 60, true);
        cache.put("1".to_string(), "v1".to_string());
        cache.put("2".to_string(), "v2".to_string());
        assert_eq!(cache.get(&"1".to_string()), None);
        assert_eq!(cache.get(&"2".to_string()), Some("v2".to_string()));
    }

    #[cfg(feature = "async")]
    #[tokio::test]
    async fn test_freq_map_cleanup_on_get_expiry() {
        let cache = create_cache(2, 1, 60, true);
        cache.put("1".to_string(), "v1".to_string());
        sleep(Duration::from_secs(2)).await;
        assert_eq!(cache.get(&"1".to_string()), None);
    }

    #[test]

    fn test_lfu_sync_constructor() {
        let cache = Rustycache::lfu_sync(1, 10, Duration::from_secs(60));

        cache.put("a".to_string(), "b".to_string());

        assert_eq!(cache.get(&"a".to_string()), Some("b".to_string()));
    }

    #[test]

    fn test_explicit_drop() {
        let cache = create_cache(2, 1, 1, false);

        drop(cache);
    }
}

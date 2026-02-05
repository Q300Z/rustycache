use rustycache::rustycache::Rustycache;
#[cfg(feature = "async")]
use rustycache::strategy::CacheStrategy;
#[cfg(feature = "async")]
use std::sync::Arc;
use std::time::Duration;
#[cfg(feature = "async")]
use tokio::sync::Barrier;

#[cfg(feature = "async")]
#[tokio::test]
async fn test_concurrent_put_get_lru() {
    let capacity = 100;
    let cache = Arc::new(Rustycache::lru(
        8,
        capacity,
        Duration::from_secs(10),
        Duration::from_secs(60),
    ));
    run_concurrency_test(cache).await;
}

#[cfg(feature = "async")]
#[tokio::test]
async fn test_concurrent_put_get_fifo() {
    let capacity = 100;
    let cache = Arc::new(Rustycache::fifo(
        8,
        capacity,
        Duration::from_secs(10),
        Duration::from_secs(60),
    ));
    run_concurrency_test(cache).await;
}

#[cfg(feature = "async")]
#[tokio::test]
async fn test_concurrent_put_get_lfu() {
    let capacity = 100;
    let cache = Arc::new(Rustycache::lfu(
        8,
        capacity,
        Duration::from_secs(10),
        Duration::from_secs(60),
    ));
    run_concurrency_test(cache).await;
}

#[cfg(feature = "async")]
async fn run_concurrency_test<S>(cache: Arc<Rustycache<String, String, S>>)
where
    S: CacheStrategy<String, String> + 'static,
{
    let num_threads = 10;
    let ops_per_thread = 1000;
    let barrier = Arc::new(Barrier::new(num_threads));
    let mut handles = vec![];

    for t in 0..num_threads {
        let cache_clone = Arc::clone(&cache);
        let barrier_clone = Arc::clone(&barrier);

        let handle = tokio::spawn(async move {
            barrier_clone.wait().await;
            for i in 0..ops_per_thread {
                let key = (i % 200).to_string();
                let val = format!("thread-{}-val-{}", t, i);

                if i % 2 == 0 {
                    cache_clone.put(key, val);
                } else {
                    cache_clone.get(&key);
                }
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.await.unwrap();
    }
}

#[test]
fn test_sync_basics() {
    let cache = Rustycache::lru_sync(1, 10, Duration::from_secs(60));
    cache.put("a".to_string(), "b".to_string());
    assert_eq!(cache.get(&"a".to_string()), Some("b".to_string()));
}

#[cfg(feature = "async")]
use rustycache::rustycache::Rustycache;
#[cfg(feature = "async")]
use std::time::Duration;
#[cfg(feature = "async")]
use tokio::time::sleep;

#[cfg(feature = "async")]
#[tokio::main]
async fn main() {
    println!("--- Example: Asynchronous Mode ---");

    // Create an LRU cache with:
    // - 8 shards (high concurrency)
    // - 1000 total capacity
    // - 2 seconds TTL (short for demonstration)
    // - 1 second cleaner interval
    let cache = Rustycache::lru(8, 1000, Duration::from_secs(2), Duration::from_secs(1));

    // Inserting data
    cache.put("api_result_1".to_string(), "Result A");
    println!("Inserted key 'api_result_1'");

    // Verify it exists
    if cache.contains("api_result_1") {
        println!("Key is present in cache.");
    }

    println!("Waiting for 3 seconds (longer than TTL)...");
    sleep(Duration::from_secs(3)).await;

    // The background cleaner should have removed the entry
    if cache.get("api_result_1").is_none() {
        println!("Key has been automatically removed by the background cleaner.");
    }

    println!("Cache length: {}", cache.len());
}

#[cfg(not(feature = "async"))]
fn main() {
    println!("This example requires the 'async' feature.");
}

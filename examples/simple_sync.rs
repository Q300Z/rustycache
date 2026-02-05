use rustycache::rustycache::Rustycache;
use std::time::Duration;

fn main() {
    println!("--- Example: Synchronous Mode ---");

    // Create a FIFO cache with 4 shards, total capacity of 100, and 1 minute TTL.
    // In sync mode, expiration is handled passively when calling `get`.
    let cache = Rustycache::fifo_sync(4, 100, Duration::from_secs(60));

    // Inserting data
    cache.put("session_1".to_string(), "user_data_A");
    cache.put("session_2".to_string(), "user_data_B");

    // Retrieval using &str (thanks to Borrow support, no need to create a String)
    if let Some(data) = cache.get("session_1") {
        println!("Retrieved session_1: {}", data);
    }

    // Check size
    println!("Cache length: {}", cache.len());

    // Clear the cache
    cache.clear();
    println!("Cache length after clear: {}", cache.len());
}

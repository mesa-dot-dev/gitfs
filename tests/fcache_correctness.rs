#![allow(clippy::unwrap_used, missing_docs)]

use git_fs::cache::fcache::{FileCache, InvalidRootPathError};
use git_fs::cache::traits::{AsyncReadableCache as _, AsyncWritableCache as _};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn new_creates_directory_and_marker() {
    let tmp = tempfile::tempdir().unwrap();
    let cache_path = tmp.path().join("cache");

    let _cache = FileCache::<u64>::new(&cache_path, 4096).await.unwrap();

    assert!(cache_path.join(".gitfs_cache").exists());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn new_with_existing_empty_directory() {
    let tmp = tempfile::tempdir().unwrap();
    // tmp.path() is an existing empty directory
    let _cache = FileCache::<u64>::new(tmp.path(), 4096).await.unwrap();

    assert!(tmp.path().join(".gitfs_cache").exists());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn new_rejects_file_path() {
    let tmp = tempfile::tempdir().unwrap();
    let file_path = tmp.path().join("not_a_dir");
    std::fs::write(&file_path, b"hello").unwrap();

    let result = FileCache::<u64>::new(&file_path, 4096).await;

    assert!(
        matches!(result, Err(InvalidRootPathError::NotADirectory(_))),
        "expected NotADirectory, got {:?}",
        result.as_ref().map(|_| "Ok(...)").map_err(|e| format!("{e:?}"))
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn new_rejects_non_empty_unmarked_directory() {
    let tmp = tempfile::tempdir().unwrap();
    // Place a foreign file so the directory is non-empty and has no marker
    std::fs::write(tmp.path().join("foreign.txt"), b"data").unwrap();

    let result = FileCache::<u64>::new(tmp.path(), 4096).await;

    assert!(
        matches!(result, Err(InvalidRootPathError::RootPathUnsafeCache(_))),
        "expected RootPathUnsafeCache, got {:?}",
        result.as_ref().map(|_| "Ok(...)").map_err(|e| format!("{e:?}"))
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn new_cleans_previously_used_directory() {
    let tmp = tempfile::tempdir().unwrap();
    let cache_path = tmp.path().join("cache");

    // First use: create cache and insert a value
    {
        let cache = FileCache::<u64>::new(&cache_path, 4096).await.unwrap();
        cache.insert(&1, b"value-1".to_vec()).await.unwrap();
        // FileCache::drop removes the directory
    }

    // Recreate the directory with a marker to simulate a restart
    std::fs::create_dir_all(&cache_path).unwrap();
    std::fs::write(cache_path.join(".gitfs_cache"), b"").unwrap();
    std::fs::write(cache_path.join("leftover"), b"stale").unwrap();

    // Second use: should clean old contents
    let cache = FileCache::<u64>::new(&cache_path, 4096).await.unwrap();

    // Old data should not be accessible
    assert!(cache.get(&1).await.is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn insert_and_get_returns_value() {
    let tmp = tempfile::tempdir().unwrap();
    let cache = FileCache::<u64>::new(tmp.path(), 4096).await.unwrap();

    cache.insert(&42, b"hello world".to_vec()).await.unwrap();

    let val = cache.get(&42).await;
    assert_eq!(val.as_deref(), Some(b"hello world".as_slice()));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn get_missing_key_returns_none() {
    let tmp = tempfile::tempdir().unwrap();
    let cache = FileCache::<u64>::new(tmp.path(), 4096).await.unwrap();

    assert!(cache.get(&999).await.is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn contains_reflects_presence() {
    let tmp = tempfile::tempdir().unwrap();
    let cache = FileCache::<u64>::new(tmp.path(), 4096).await.unwrap();

    assert!(!cache.contains(&1).await);

    cache.insert(&1, b"data".to_vec()).await.unwrap();

    assert!(cache.contains(&1).await);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn insert_overwrites_previous_value() {
    let tmp = tempfile::tempdir().unwrap();
    let cache = FileCache::<u64>::new(tmp.path(), 4096).await.unwrap();

    cache.insert(&1, b"first".to_vec()).await.unwrap();
    cache.insert(&1, b"second".to_vec()).await.unwrap();

    let val = cache.get(&1).await;
    assert_eq!(val.as_deref(), Some(b"second".as_slice()));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn insert_empty_value() {
    let tmp = tempfile::tempdir().unwrap();
    let cache = FileCache::<u64>::new(tmp.path(), 4096).await.unwrap();

    cache.insert(&1, Vec::new()).await.unwrap();

    let val = cache.get(&1).await;
    assert_eq!(val.as_deref(), Some(b"".as_slice()));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn eviction_removes_oldest_entries() {
    // max_size = 100 bytes. Insert 5 × 30-byte entries (150 bytes total).
    // After the last insert, eviction should have removed at least one early entry.
    let tmp = tempfile::tempdir().unwrap();
    let cache = FileCache::<u64>::new(tmp.path(), 100).await.unwrap();

    for i in 0u64..5 {
        let value = vec![b'x'; 30];
        cache.insert(&i, value).await.unwrap();
    }

    // Give the async eviction worker time to complete deletions.
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // At least one of the earliest keys should have been evicted.
    let mut present_count = 0;
    for i in 0u64..5 {
        if cache.get(&i).await.is_some() {
            present_count += 1;
        }
    }

    assert!(
        present_count < 5,
        "expected at least one eviction, but all 5 entries are still present"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn entry_larger_than_max_size_still_inserted() {
    let tmp = tempfile::tempdir().unwrap();
    let cache = FileCache::<u64>::new(tmp.path(), 10).await.unwrap();

    // Value is 50 bytes, far exceeding max_size of 10.
    let big_value = vec![b'A'; 50];
    cache.insert(&1, big_value.clone()).await.unwrap();

    let val = cache.get(&1).await;
    assert_eq!(val, Some(big_value));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn drop_removes_cache_directory() {
    let tmp = tempfile::tempdir().unwrap();
    let cache_path = tmp.path().join("cache");

    {
        let cache = FileCache::<u64>::new(&cache_path, 4096).await.unwrap();
        cache.insert(&1, b"data".to_vec()).await.unwrap();
    }
    // After drop, the cache directory should be gone.
    assert!(
        !cache_path.exists(),
        "cache directory should have been removed on drop"
    );
}

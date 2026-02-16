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

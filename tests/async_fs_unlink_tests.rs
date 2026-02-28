#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::doc_markdown,
    clippy::similar_names,
    clippy::iter_on_single_items,
    missing_docs
)]

mod common;

use std::sync::Arc;

use bytes::Bytes;

use common::async_fs_mocks::{MockFsDataProvider, MockFsState, make_dcache, make_inode};
use git_fs::cache::async_backed::FutureBackedCache;
use git_fs::fs::async_fs::AsyncFs;
use git_fs::fs::{INodeType, LoadedAddr};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unlink_returns_eisdir_for_directory() {
    let root = make_inode(1, INodeType::Directory, 0, None);
    let child_dir = make_inode(2, INodeType::Directory, 0, Some(1));

    let state = MockFsState {
        lookups: [((1, "subdir".into()), child_dir)].into_iter().collect(),
        directories: [(1, vec![("subdir".into(), child_dir)])]
            .into_iter()
            .collect(),
        ..MockFsState::default()
    };
    let provider = MockFsDataProvider::new(state);
    let table = Arc::new(FutureBackedCache::default());
    let fs = AsyncFs::new(provider, root, Arc::clone(&table), make_dcache()).await;
    let parent = LoadedAddr::new_unchecked(1);

    // Populate dcache via readdir.
    fs.readdir(parent, 0, |_, _| false).await.unwrap();

    let err = fs
        .unlink(parent, std::ffi::OsStr::new("subdir"))
        .await
        .unwrap_err();
    assert_eq!(err.raw_os_error(), Some(libc::EISDIR));

    // The directory should still be visible in readdir after the failed unlink.
    let mut entries = Vec::new();
    fs.readdir(parent, 0, |entry, _| {
        entries.push(entry.name.to_os_string());
        false
    })
    .await
    .unwrap();
    assert!(
        entries.iter().any(|n| n == "subdir"),
        "directory should still be present after EISDIR"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unlink_removes_file_from_directory_cache() {
    let root = make_inode(1, INodeType::Directory, 0, None);
    let file = make_inode(2, INodeType::File, 5, Some(1));

    let state = MockFsState {
        lookups: [((1, "test.txt".into()), file)].into_iter().collect(),
        directories: [(1, vec![("test.txt".into(), file)])].into_iter().collect(),
        file_contents: [(2, Bytes::from_static(b"hello"))].into_iter().collect(),
        ..MockFsState::default()
    };
    let provider = MockFsDataProvider::new(state);
    let table = Arc::new(FutureBackedCache::default());
    let fs = AsyncFs::new(provider, root, Arc::clone(&table), make_dcache()).await;

    // Populate the directory so the post-unlink lookup short-circuits
    // via `is_populated` instead of falling through to the provider.
    fs.readdir(LoadedAddr::new_unchecked(1), 0, |_, _| false)
        .await
        .unwrap();

    let _ = fs
        .lookup(LoadedAddr::new_unchecked(1), "test.txt".as_ref())
        .await
        .unwrap();

    fs.unlink(LoadedAddr::new_unchecked(1), "test.txt".as_ref())
        .await
        .unwrap();

    let err = fs
        .lookup(LoadedAddr::new_unchecked(1), "test.txt".as_ref())
        .await
        .unwrap_err();
    assert_eq!(err.raw_os_error(), Some(libc::ENOENT));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unlink_removes_file_from_readdir() {
    let root = make_inode(1, INodeType::Directory, 0, None);
    let file_a = make_inode(2, INodeType::File, 0, Some(1));
    let file_b = make_inode(3, INodeType::File, 0, Some(1));

    let state = MockFsState {
        lookups: [((1, "a.txt".into()), file_a), ((1, "b.txt".into()), file_b)]
            .into_iter()
            .collect(),
        directories: [(1, vec![("a.txt".into(), file_a), ("b.txt".into(), file_b)])]
            .into_iter()
            .collect(),
        ..MockFsState::default()
    };
    let provider = MockFsDataProvider::new(state);
    let table = Arc::new(FutureBackedCache::default());
    let fs = AsyncFs::new(provider, root, Arc::clone(&table), make_dcache()).await;

    let mut entries = Vec::new();
    fs.readdir(LoadedAddr::new_unchecked(1), 0, |entry, _| {
        entries.push(entry.name.to_os_string());
        false
    })
    .await
    .unwrap();
    assert_eq!(entries.len(), 2);

    fs.unlink(LoadedAddr::new_unchecked(1), "a.txt".as_ref())
        .await
        .unwrap();

    let mut entries = Vec::new();
    fs.readdir(LoadedAddr::new_unchecked(1), 0, |entry, _| {
        entries.push(entry.name.to_os_string());
        false
    })
    .await
    .unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0], "b.txt");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unlink_nonexistent_file_returns_enoent() {
    let root = make_inode(1, INodeType::Directory, 0, None);

    let state = MockFsState {
        directories: [(1, vec![])].into_iter().collect(),
        ..MockFsState::default()
    };
    let provider = MockFsDataProvider::new(state);
    let table = Arc::new(FutureBackedCache::default());
    let fs = AsyncFs::new(provider, root, Arc::clone(&table), make_dcache()).await;

    fs.readdir(LoadedAddr::new_unchecked(1), 0, |_, _| false)
        .await
        .unwrap();

    let err = fs
        .unlink(LoadedAddr::new_unchecked(1), "ghost.txt".as_ref())
        .await
        .unwrap_err();
    assert_eq!(err.raw_os_error(), Some(libc::ENOENT));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unlink_cleans_up_write_overlay() {
    let root = make_inode(1, INodeType::Directory, 0, None);
    let file = make_inode(2, INodeType::File, 0, Some(1));

    let state = MockFsState {
        lookups: [((1, "test.txt".into()), file)].into_iter().collect(),
        directories: [(1, vec![("test.txt".into(), file)])].into_iter().collect(),
        file_contents: [(2, Bytes::from_static(b""))].into_iter().collect(),
        ..MockFsState::default()
    };
    let provider = MockFsDataProvider::new(state);
    let table = Arc::new(FutureBackedCache::default());
    let fs = AsyncFs::new(provider, root, Arc::clone(&table), make_dcache()).await;

    // Populate the directory so the post-unlink lookup short-circuits
    // via `is_populated` instead of falling through to the provider.
    fs.readdir(LoadedAddr::new_unchecked(1), 0, |_, _| false)
        .await
        .unwrap();

    let _ = fs
        .lookup(LoadedAddr::new_unchecked(1), "test.txt".as_ref())
        .await
        .unwrap();

    fs.write(
        LoadedAddr::new_unchecked(2),
        0,
        Bytes::from_static(b"hello"),
    )
    .await
    .unwrap();

    fs.unlink(LoadedAddr::new_unchecked(1), "test.txt".as_ref())
        .await
        .unwrap();

    let err = fs
        .lookup(LoadedAddr::new_unchecked(1), "test.txt".as_ref())
        .await
        .unwrap_err();
    assert_eq!(err.raw_os_error(), Some(libc::ENOENT));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unlink_calls_provider() {
    let root = make_inode(1, INodeType::Directory, 0, None);
    let file = make_inode(2, INodeType::File, 0, Some(1));

    let unlink_calls = Arc::new(tokio::sync::Mutex::new(Vec::new()));
    let unlink_notify = Arc::new(tokio::sync::Notify::new());
    let state = MockFsState {
        lookups: [((1, "test.txt".into()), file)].into_iter().collect(),
        directories: [(1, vec![("test.txt".into(), file)])].into_iter().collect(),
        file_contents: [(2, Bytes::from_static(b""))].into_iter().collect(),
        unlink_calls: Arc::clone(&unlink_calls),
        unlink_notify: Arc::clone(&unlink_notify),
        ..MockFsState::default()
    };
    let provider = MockFsDataProvider::new(state);
    let table = Arc::new(FutureBackedCache::default());
    let fs = AsyncFs::new(provider, root, Arc::clone(&table), make_dcache()).await;

    let _ = fs
        .lookup(LoadedAddr::new_unchecked(1), "test.txt".as_ref())
        .await
        .unwrap();

    fs.unlink(LoadedAddr::new_unchecked(1), "test.txt".as_ref())
        .await
        .unwrap();

    tokio::time::timeout(std::time::Duration::from_secs(5), unlink_notify.notified())
        .await
        .expect("timed out waiting for provider unlink");

    let calls = unlink_calls.lock().await;
    assert_eq!(calls.len(), 1, "provider unlink should be called once");
    assert_eq!(calls[0].0, 1, "parent addr should be 1");
    assert_eq!(calls[0].1, "test.txt", "child name should be test.txt");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unlink_provider_failure_does_not_affect_local_state() {
    let root = make_inode(1, INodeType::Directory, 0, None);
    let file = make_inode(2, INodeType::File, 0, Some(1));

    let state = MockFsState {
        lookups: [((1, "test.txt".into()), file)].into_iter().collect(),
        directories: [(1, vec![("test.txt".into(), file)])].into_iter().collect(),
        file_contents: [(2, Bytes::from_static(b""))].into_iter().collect(),
        unlink_error: Some("simulated remote failure".into()),
        ..MockFsState::default()
    };
    let provider = MockFsDataProvider::new(state);
    let table = Arc::new(FutureBackedCache::default());
    let fs = AsyncFs::new(provider, root, Arc::clone(&table), make_dcache()).await;

    // Populate the directory so the post-unlink lookup short-circuits
    // via `is_populated` instead of falling through to the provider.
    fs.readdir(LoadedAddr::new_unchecked(1), 0, |_, _| false)
        .await
        .unwrap();

    let _ = fs
        .lookup(LoadedAddr::new_unchecked(1), "test.txt".as_ref())
        .await
        .unwrap();

    fs.unlink(LoadedAddr::new_unchecked(1), "test.txt".as_ref())
        .await
        .unwrap();

    let err = fs
        .lookup(LoadedAddr::new_unchecked(1), "test.txt".as_ref())
        .await
        .unwrap_err();
    assert_eq!(err.raw_os_error(), Some(libc::ENOENT));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn write_after_unlink_succeeds_for_open_file() {
    let root = make_inode(1, INodeType::Directory, 0, None);
    let file = make_inode(2, INodeType::File, 5, Some(1));

    let state = MockFsState {
        lookups: [((1, "test.txt".into()), file)].into_iter().collect(),
        directories: [(1, vec![("test.txt".into(), file)])].into_iter().collect(),
        file_contents: [(2, Bytes::from("hello"))].into_iter().collect(),
        ..MockFsState::default()
    };
    let provider = MockFsDataProvider::new(state);
    let table = Arc::new(FutureBackedCache::default());
    let fs = AsyncFs::new(provider, root, Arc::clone(&table), make_dcache()).await;
    let parent = LoadedAddr::new_unchecked(1);

    // Populate dcache.
    fs.readdir(parent, 0, |_, _| false).await.unwrap();

    // Write should succeed (file is in inode_table).
    let addr = LoadedAddr::new_unchecked(2);
    let result = fs.write(addr, 0, Bytes::from("world")).await;
    assert!(result.is_ok());

    // Unlink the file.
    fs.unlink(parent, "test.txt".as_ref()).await.unwrap();

    // Write should STILL succeed — inode must remain in table.
    let result = fs.write(addr, 0, Bytes::from("after unlink")).await;
    assert!(
        result.is_ok(),
        "write after unlink should succeed for open file: {result:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unlink_succeeds_via_lookup_fallback_when_dcache_unpopulated() {
    let root = make_inode(1, INodeType::Directory, 0, None);
    let file = make_inode(2, INodeType::File, 0, Some(1));

    let state = MockFsState {
        lookups: [((1, "test.txt".into()), file)].into_iter().collect(),
        directories: [(1, vec![("test.txt".into(), file)])].into_iter().collect(),
        ..MockFsState::default()
    };
    let provider = MockFsDataProvider::new(state);
    let table = Arc::new(FutureBackedCache::default());
    let fs = AsyncFs::new(provider, root, Arc::clone(&table), make_dcache()).await;
    let parent = LoadedAddr::new_unchecked(1);

    // Do NOT call readdir — dcache is not populated.
    // But do a lookup so the inode is in the table.
    let _ = fs
        .lookup(parent, std::ffi::OsStr::new("test.txt"))
        .await
        .unwrap();

    let result = fs.unlink(parent, std::ffi::OsStr::new("test.txt")).await;
    assert!(
        result.is_ok(),
        "unlink should succeed via lookup fallback: {result:?}"
    );
}

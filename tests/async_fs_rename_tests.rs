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
async fn rename_same_parent_updates_directory_cache() {
    let root = make_inode(1, INodeType::Directory, 0, None);
    let file = make_inode(2, INodeType::File, 5, Some(1));

    let state = MockFsState {
        lookups: [((1, "old.txt".into()), file)].into_iter().collect(),
        directories: [(1, vec![("old.txt".into(), file)])].into_iter().collect(),
        file_contents: [(2, Bytes::from_static(b"hello"))].into_iter().collect(),
        ..MockFsState::default()
    };
    let provider = MockFsDataProvider::new(state);
    let table = Arc::new(FutureBackedCache::default());
    let fs = AsyncFs::new(provider, root, Arc::clone(&table), make_dcache()).await;
    let parent = LoadedAddr::new_unchecked(1);

    fs.readdir(parent, 0, |_, _| false).await.unwrap();

    fs.rename(
        parent,
        std::ffi::OsStr::new("old.txt"),
        parent,
        std::ffi::OsStr::new("new.txt"),
    )
    .await
    .unwrap();

    let err = fs
        .lookup(parent, std::ffi::OsStr::new("old.txt"))
        .await
        .unwrap_err();
    assert_eq!(err.raw_os_error(), Some(libc::ENOENT));

    let resolved = fs
        .lookup(parent, std::ffi::OsStr::new("new.txt"))
        .await
        .unwrap();
    assert_eq!(resolved.inode.addr, 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rename_cross_parent_moves_file() {
    let root = make_inode(1, INodeType::Directory, 0, None);
    let dir_a = make_inode(2, INodeType::Directory, 0, Some(1));
    let dir_b = make_inode(3, INodeType::Directory, 0, Some(1));
    let file = make_inode(4, INodeType::File, 5, Some(2));

    let state = MockFsState {
        lookups: [
            ((1, "a".into()), dir_a),
            ((1, "b".into()), dir_b),
            ((2, "file.txt".into()), file),
        ]
        .into_iter()
        .collect(),
        directories: [
            (1, vec![("a".into(), dir_a), ("b".into(), dir_b)]),
            (2, vec![("file.txt".into(), file)]),
            (3, vec![]),
        ]
        .into_iter()
        .collect(),
        file_contents: [(4, Bytes::from_static(b"hello"))].into_iter().collect(),
        ..MockFsState::default()
    };
    let provider = MockFsDataProvider::new(state);
    let table = Arc::new(FutureBackedCache::default());
    let fs = AsyncFs::new(provider, root, Arc::clone(&table), make_dcache()).await;

    let parent_a = LoadedAddr::new_unchecked(2);
    let parent_b = LoadedAddr::new_unchecked(3);

    fs.readdir(LoadedAddr::new_unchecked(1), 0, |_, _| false)
        .await
        .unwrap();
    fs.readdir(parent_a, 0, |_, _| false).await.unwrap();
    fs.readdir(parent_b, 0, |_, _| false).await.unwrap();

    fs.rename(
        parent_a,
        std::ffi::OsStr::new("file.txt"),
        parent_b,
        std::ffi::OsStr::new("file.txt"),
    )
    .await
    .unwrap();

    let err = fs
        .lookup(parent_a, std::ffi::OsStr::new("file.txt"))
        .await
        .unwrap_err();
    assert_eq!(err.raw_os_error(), Some(libc::ENOENT));

    let resolved = fs
        .lookup(parent_b, std::ffi::OsStr::new("file.txt"))
        .await
        .unwrap();
    assert_eq!(resolved.inode.addr, 4);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rename_nonexistent_source_returns_enoent() {
    let root = make_inode(1, INodeType::Directory, 0, None);

    let state = MockFsState {
        directories: [(1, vec![])].into_iter().collect(),
        ..MockFsState::default()
    };
    let provider = MockFsDataProvider::new(state);
    let table = Arc::new(FutureBackedCache::default());
    let fs = AsyncFs::new(provider, root, Arc::clone(&table), make_dcache()).await;
    let parent = LoadedAddr::new_unchecked(1);

    fs.readdir(parent, 0, |_, _| false).await.unwrap();

    let err = fs
        .rename(
            parent,
            std::ffi::OsStr::new("ghost.txt"),
            parent,
            std::ffi::OsStr::new("new.txt"),
        )
        .await
        .unwrap_err();
    assert_eq!(err.raw_os_error(), Some(libc::ENOENT));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rename_overwrites_existing_target() {
    let root = make_inode(1, INodeType::Directory, 0, None);
    let file_a = make_inode(2, INodeType::File, 5, Some(1));
    let file_b = make_inode(3, INodeType::File, 3, Some(1));

    let state = MockFsState {
        lookups: [((1, "a.txt".into()), file_a), ((1, "b.txt".into()), file_b)]
            .into_iter()
            .collect(),
        directories: [(1, vec![("a.txt".into(), file_a), ("b.txt".into(), file_b)])]
            .into_iter()
            .collect(),
        file_contents: [
            (2, Bytes::from_static(b"hello")),
            (3, Bytes::from_static(b"bye")),
        ]
        .into_iter()
        .collect(),
        ..MockFsState::default()
    };
    let provider = MockFsDataProvider::new(state);
    let table = Arc::new(FutureBackedCache::default());
    let fs = AsyncFs::new(provider, root, Arc::clone(&table), make_dcache()).await;
    let parent = LoadedAddr::new_unchecked(1);

    fs.readdir(parent, 0, |_, _| false).await.unwrap();

    fs.rename(
        parent,
        std::ffi::OsStr::new("a.txt"),
        parent,
        std::ffi::OsStr::new("b.txt"),
    )
    .await
    .unwrap();

    let resolved = fs
        .lookup(parent, std::ffi::OsStr::new("b.txt"))
        .await
        .unwrap();
    assert_eq!(resolved.inode.addr, 2);

    let err = fs
        .lookup(parent, std::ffi::OsStr::new("a.txt"))
        .await
        .unwrap_err();
    assert_eq!(err.raw_os_error(), Some(libc::ENOENT));

    let mut entries = Vec::new();
    fs.readdir(parent, 0, |entry, _| {
        entries.push(entry.name.to_os_string());
        false
    })
    .await
    .unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0], "b.txt");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rename_preserves_write_overlay() {
    let root = make_inode(1, INodeType::Directory, 0, None);
    let file = make_inode(2, INodeType::File, 0, Some(1));

    let state = MockFsState {
        lookups: [((1, "old.txt".into()), file)].into_iter().collect(),
        directories: [(1, vec![("old.txt".into(), file)])].into_iter().collect(),
        file_contents: [(2, Bytes::from_static(b""))].into_iter().collect(),
        ..MockFsState::default()
    };
    let provider = MockFsDataProvider::new(state);
    let table = Arc::new(FutureBackedCache::default());
    let fs = AsyncFs::new(provider, root, Arc::clone(&table), make_dcache()).await;
    let parent = LoadedAddr::new_unchecked(1);

    fs.readdir(parent, 0, |_, _| false).await.unwrap();

    fs.write(
        LoadedAddr::new_unchecked(2),
        0,
        Bytes::from_static(b"written data"),
    )
    .await
    .unwrap();

    fs.rename(
        parent,
        std::ffi::OsStr::new("old.txt"),
        parent,
        std::ffi::OsStr::new("new.txt"),
    )
    .await
    .unwrap();

    let resolved = fs
        .lookup(parent, std::ffi::OsStr::new("new.txt"))
        .await
        .unwrap();
    assert_eq!(resolved.inode.addr, 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rename_calls_provider() {
    let root = make_inode(1, INodeType::Directory, 0, None);
    let file = make_inode(2, INodeType::File, 5, Some(1));

    let rename_calls = Arc::new(tokio::sync::Mutex::new(Vec::new()));
    let rename_notify = Arc::new(tokio::sync::Notify::new());
    let state = MockFsState {
        lookups: [((1, "old.txt".into()), file)].into_iter().collect(),
        directories: [(1, vec![("old.txt".into(), file)])].into_iter().collect(),
        file_contents: [(2, Bytes::from_static(b"hello"))].into_iter().collect(),
        rename_calls: Arc::clone(&rename_calls),
        rename_notify: Arc::clone(&rename_notify),
        ..MockFsState::default()
    };
    let provider = MockFsDataProvider::new(state);
    let table = Arc::new(FutureBackedCache::default());
    let fs = AsyncFs::new(provider, root, Arc::clone(&table), make_dcache()).await;
    let parent = LoadedAddr::new_unchecked(1);

    fs.readdir(parent, 0, |_, _| false).await.unwrap();

    fs.rename(
        parent,
        std::ffi::OsStr::new("old.txt"),
        parent,
        std::ffi::OsStr::new("new.txt"),
    )
    .await
    .unwrap();

    tokio::time::timeout(std::time::Duration::from_secs(5), rename_notify.notified())
        .await
        .expect("timed out waiting for provider rename");

    let calls = rename_calls.lock().await;
    assert_eq!(calls.len(), 1, "provider rename should be called once");
    assert_eq!(calls[0].0, 1, "old parent addr");
    assert_eq!(calls[0].1, "old.txt", "old name");
    assert_eq!(calls[0].2, 1, "new parent addr");
    assert_eq!(calls[0].3, "new.txt", "new name");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rename_provider_failure_does_not_affect_local_state() {
    let root = make_inode(1, INodeType::Directory, 0, None);
    let file = make_inode(2, INodeType::File, 5, Some(1));

    let state = MockFsState {
        lookups: [((1, "old.txt".into()), file)].into_iter().collect(),
        directories: [(1, vec![("old.txt".into(), file)])].into_iter().collect(),
        file_contents: [(2, Bytes::from_static(b"hello"))].into_iter().collect(),
        rename_error: Some("simulated remote failure".into()),
        ..MockFsState::default()
    };
    let provider = MockFsDataProvider::new(state);
    let table = Arc::new(FutureBackedCache::default());
    let fs = AsyncFs::new(provider, root, Arc::clone(&table), make_dcache()).await;
    let parent = LoadedAddr::new_unchecked(1);

    fs.readdir(parent, 0, |_, _| false).await.unwrap();

    fs.rename(
        parent,
        std::ffi::OsStr::new("old.txt"),
        parent,
        std::ffi::OsStr::new("new.txt"),
    )
    .await
    .unwrap();

    let err = fs
        .lookup(parent, std::ffi::OsStr::new("old.txt"))
        .await
        .unwrap_err();
    assert_eq!(err.raw_os_error(), Some(libc::ENOENT));

    let resolved = fs
        .lookup(parent, std::ffi::OsStr::new("new.txt"))
        .await
        .unwrap();
    assert_eq!(resolved.inode.addr, 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rename_updates_readdir() {
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
    let parent = LoadedAddr::new_unchecked(1);

    fs.readdir(parent, 0, |_, _| false).await.unwrap();

    fs.rename(
        parent,
        std::ffi::OsStr::new("a.txt"),
        parent,
        std::ffi::OsStr::new("c.txt"),
    )
    .await
    .unwrap();

    let mut entries = Vec::new();
    fs.readdir(parent, 0, |entry, _| {
        entries.push(entry.name.to_os_string());
        false
    })
    .await
    .unwrap();
    assert_eq!(entries.len(), 2);
    assert!(entries.iter().any(|n| n == "b.txt"));
    assert!(entries.iter().any(|n| n == "c.txt"));
    assert!(!entries.iter().any(|n| n == "a.txt"));
}

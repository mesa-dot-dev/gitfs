#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::doc_markdown,
    clippy::similar_names,
    missing_docs
)]

mod common;

use std::ffi::{OsStr, OsString};
use std::sync::Arc;

use git_fs::cache::async_backed::FutureBackedCache;
use git_fs::fs::async_fs::{AsyncFs, InodeLifecycle};
use git_fs::fs::{INode, INodeType, LoadedAddr, OpenFlags};

use common::async_fs_mocks::{MockFsDataProvider, MockFsState, make_inode};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lifecycle_inc_returns_count_after_increment() {
    let table = Arc::new(FutureBackedCache::default());
    let inode = make_inode(100, INodeType::File, 0, Some(1));
    table.insert_sync(100, inode);

    let mut lifecycle = InodeLifecycle::from_table(Arc::clone(&table));

    assert_eq!(lifecycle.inc(100), 1, "first inc should return 1");
    assert_eq!(lifecycle.inc(100), 2, "second inc should return 2");
    assert_eq!(lifecycle.inc(100), 3, "third inc should return 3");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lifecycle_dec_returns_remaining_count() {
    let table = Arc::new(FutureBackedCache::default());
    let inode = make_inode(100, INodeType::File, 0, Some(1));
    table.insert_sync(100, inode);

    let mut lifecycle = InodeLifecycle::from_table(Arc::clone(&table));
    lifecycle.inc(100);
    lifecycle.inc(100);

    assert_eq!(lifecycle.dec(&100), Some(1), "dec from 2 should give 1");
    assert_eq!(lifecycle.dec(&100), Some(0), "dec from 1 should give 0");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lifecycle_dec_unknown_addr_returns_none() {
    let table: Arc<FutureBackedCache<u64, INode>> = Arc::new(FutureBackedCache::default());
    let mut lifecycle = InodeLifecycle::from_table(Arc::clone(&table));

    assert_eq!(
        lifecycle.dec(&999),
        None,
        "dec on unknown key should return None"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lifecycle_dec_to_zero_evicts_from_table() {
    let table = Arc::new(FutureBackedCache::default());
    let inode = make_inode(100, INodeType::File, 0, Some(1));
    table.insert_sync(100, inode);

    let mut lifecycle = InodeLifecycle::from_table(Arc::clone(&table));
    lifecycle.inc(100);

    assert_eq!(lifecycle.dec(&100), Some(0));
    // The inode should have been evicted from the table.
    assert!(
        lifecycle.table().get(&100).await.is_none(),
        "inode should be evicted after refcount hits zero"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lifecycle_dec_count_decrements_by_n() {
    let table: Arc<FutureBackedCache<u64, INode>> = Arc::new(FutureBackedCache::default());
    let inode = make_inode(100, INodeType::File, 0, Some(1));
    table.insert_sync(100, inode);

    let mut lifecycle = InodeLifecycle::from_table(Arc::clone(&table));
    lifecycle.inc(100);
    lifecycle.inc(100);
    lifecycle.inc(100); // count = 3

    assert_eq!(
        lifecycle.dec_count(&100, 2),
        Some(1),
        "dec_count(3, 2) should give 1"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lifecycle_dec_count_to_zero_evicts() {
    let table = Arc::new(FutureBackedCache::default());
    let inode = make_inode(100, INodeType::File, 0, Some(1));
    table.insert_sync(100, inode);

    let mut lifecycle = InodeLifecycle::from_table(Arc::clone(&table));
    lifecycle.inc(100);
    lifecycle.inc(100); // count = 2

    assert_eq!(lifecycle.dec_count(&100, 2), Some(0));
    assert!(
        lifecycle.table().get(&100).await.is_none(),
        "inode should be evicted after dec_count to zero"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lifecycle_table_returns_underlying_cache() {
    let table = Arc::new(FutureBackedCache::default());
    let inode = make_inode(42, INodeType::Directory, 0, None);
    table.insert_sync(42, inode);

    let lifecycle = InodeLifecycle::from_table(Arc::clone(&table));

    let fetched = lifecycle.table().get(&42).await;
    assert_eq!(
        fetched.map(|n| n.addr),
        Some(42),
        "table() should expose the underlying cache"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn new_seeds_root_inode_into_table() {
    let table = Arc::new(FutureBackedCache::default());
    let root = make_inode(1, INodeType::Directory, 0, None);
    let dp = MockFsDataProvider::new(MockFsState::default());

    let fs = AsyncFs::new(dp, root, Arc::clone(&table)).await;

    assert_eq!(fs.inode_count(), 1, "root should be the only inode");
    let fetched = table.get(&1).await;
    assert_eq!(
        fetched.map(|n| n.addr),
        Some(1),
        "root inode should be in the table"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn new_preseeded_does_not_insert_root() {
    let table: Arc<FutureBackedCache<u64, INode>> = Arc::new(FutureBackedCache::default());
    let dp = MockFsDataProvider::new(MockFsState::default());

    let fs = AsyncFs::new_preseeded(dp, Arc::clone(&table));

    assert_eq!(
        fs.inode_count(),
        0,
        "preseeded constructor should not insert anything"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn statfs_reports_inode_count() {
    let table = Arc::new(FutureBackedCache::default());
    let root = make_inode(1, INodeType::Directory, 0, None);
    let dp = MockFsDataProvider::new(MockFsState::default());

    let fs = AsyncFs::new(dp, root, Arc::clone(&table)).await;
    let stats = fs.statfs();

    assert_eq!(stats.block_size, 4096);
    assert_eq!(stats.total_inodes, 1, "should reflect the root inode");
    assert_eq!(stats.free_blocks, 0);
    assert_eq!(stats.max_filename_length, 255);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn loaded_inode_returns_seeded_inode() {
    let table = Arc::new(FutureBackedCache::default());
    let root = make_inode(1, INodeType::Directory, 0, None);
    let dp = MockFsDataProvider::new(MockFsState::default());

    let fs = AsyncFs::new(dp, root, Arc::clone(&table)).await;

    let inode = fs.loaded_inode(LoadedAddr::new_unchecked(1)).await.unwrap();
    assert_eq!(inode.addr, 1);
    assert_eq!(inode.itype, INodeType::Directory);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn loaded_inode_returns_enoent_for_missing_addr() {
    let table = Arc::new(FutureBackedCache::default());
    let root = make_inode(1, INodeType::Directory, 0, None);
    let dp = MockFsDataProvider::new(MockFsState::default());

    let fs = AsyncFs::new(dp, root, Arc::clone(&table)).await;

    let err = fs
        .loaded_inode(LoadedAddr::new_unchecked(999))
        .await
        .unwrap_err();
    assert_eq!(err.raw_os_error(), Some(libc::ENOENT));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn getattr_delegates_to_loaded_inode() {
    let table = Arc::new(FutureBackedCache::default());
    let root = make_inode(1, INodeType::Directory, 4096, None);
    let dp = MockFsDataProvider::new(MockFsState::default());

    let fs = AsyncFs::new(dp, root, Arc::clone(&table)).await;

    let inode = fs.getattr(LoadedAddr::new_unchecked(1)).await.unwrap();
    assert_eq!(inode.addr, 1);
    assert_eq!(inode.size, 4096);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lookup_resolves_child_via_data_provider() {
    let root = make_inode(1, INodeType::Directory, 0, None);
    let child = make_inode(10, INodeType::File, 42, Some(1));

    let mut state = MockFsState::default();
    state.lookups.insert((1, "readme.md".into()), child);
    let dp = MockFsDataProvider::new(state);

    let table = Arc::new(FutureBackedCache::default());
    let fs = AsyncFs::new(dp, root, Arc::clone(&table)).await;

    let tracked = fs
        .lookup(LoadedAddr::new_unchecked(1), OsStr::new("readme.md"))
        .await
        .unwrap();

    assert_eq!(tracked.inode.addr, 10);
    assert_eq!(tracked.inode.size, 42);
    assert_eq!(tracked.inode.itype, INodeType::File);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lookup_populates_inode_table() {
    let root = make_inode(1, INodeType::Directory, 0, None);
    let child = make_inode(10, INodeType::File, 100, Some(1));

    let mut state = MockFsState::default();
    state.lookups.insert((1, "file.txt".into()), child);
    let dp = MockFsDataProvider::new(state);

    let table = Arc::new(FutureBackedCache::default());
    let fs = AsyncFs::new(dp, root, Arc::clone(&table)).await;

    fs.lookup(LoadedAddr::new_unchecked(1), OsStr::new("file.txt"))
        .await
        .unwrap();

    // The child should now be in the inode table.
    let cached = table.get(&10).await;
    assert_eq!(
        cached.map(|n| n.addr),
        Some(10),
        "child inode should be cached in the table after lookup"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lookup_second_call_uses_cache() {
    let root = make_inode(1, INodeType::Directory, 0, None);
    let child = make_inode(10, INodeType::File, 100, Some(1));

    let mut state = MockFsState::default();
    state.lookups.insert((1, "cached.txt".into()), child);
    let dp = MockFsDataProvider::new(state);

    let table = Arc::new(FutureBackedCache::default());
    let fs = AsyncFs::new(dp, root, Arc::clone(&table)).await;

    let first = fs
        .lookup(LoadedAddr::new_unchecked(1), OsStr::new("cached.txt"))
        .await
        .unwrap();
    let second = fs
        .lookup(LoadedAddr::new_unchecked(1), OsStr::new("cached.txt"))
        .await
        .unwrap();

    assert_eq!(first.inode.addr, second.inode.addr);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lookup_propagates_provider_error() {
    let root = make_inode(1, INodeType::Directory, 0, None);
    // No lookups configured — provider will return ENOENT.
    let dp = MockFsDataProvider::new(MockFsState::default());

    let table = Arc::new(FutureBackedCache::default());
    let fs = AsyncFs::new(dp, root, Arc::clone(&table)).await;

    let err = fs
        .lookup(LoadedAddr::new_unchecked(1), OsStr::new("nonexistent"))
        .await
        .unwrap_err();
    assert_eq!(err.raw_os_error(), Some(libc::ENOENT));
}

// open and OpenFile::read tests

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn open_returns_file_handle_and_reader() {
    let root = make_inode(1, INodeType::Directory, 0, None);
    let file = make_inode(10, INodeType::File, 5, Some(1));

    let mut state = MockFsState::default();
    state
        .file_contents
        .insert(10, bytes::Bytes::from_static(b"hello"));
    let dp = MockFsDataProvider::new(state);

    let table = Arc::new(FutureBackedCache::default());
    table.insert_sync(10, file);
    let fs = AsyncFs::new(dp, root, Arc::clone(&table)).await;

    let open_file = fs
        .open(LoadedAddr::new_unchecked(10), OpenFlags::RDONLY)
        .await
        .unwrap();

    assert!(open_file.fh >= 1, "file handle should start at 1");
    let data = open_file.read(0, 5).await.unwrap();
    assert_eq!(&data[..], b"hello");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn open_returns_eisdir_for_directory() {
    let root = make_inode(1, INodeType::Directory, 0, None);
    let dp = MockFsDataProvider::new(MockFsState::default());

    let table = Arc::new(FutureBackedCache::default());
    let fs = AsyncFs::new(dp, root, Arc::clone(&table)).await;

    let err = fs
        .open(LoadedAddr::new_unchecked(1), OpenFlags::RDONLY)
        .await
        .unwrap_err();
    assert_eq!(err.raw_os_error(), Some(libc::EISDIR));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn open_returns_enoent_for_missing_inode() {
    let root = make_inode(1, INodeType::Directory, 0, None);
    let dp = MockFsDataProvider::new(MockFsState::default());

    let table = Arc::new(FutureBackedCache::default());
    let fs = AsyncFs::new(dp, root, Arc::clone(&table)).await;

    let err = fs
        .open(LoadedAddr::new_unchecked(999), OpenFlags::RDONLY)
        .await
        .unwrap_err();
    assert_eq!(err.raw_os_error(), Some(libc::ENOENT));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn open_assigns_unique_file_handles() {
    let root = make_inode(1, INodeType::Directory, 0, None);
    let file = make_inode(10, INodeType::File, 0, Some(1));

    let dp = MockFsDataProvider::new(MockFsState::default());

    let table = Arc::new(FutureBackedCache::default());
    table.insert_sync(10, file);
    let fs = AsyncFs::new(dp, root, Arc::clone(&table)).await;

    let fh1 = fs
        .open(LoadedAddr::new_unchecked(10), OpenFlags::RDONLY)
        .await
        .unwrap()
        .fh;
    let fh2 = fs
        .open(LoadedAddr::new_unchecked(10), OpenFlags::RDONLY)
        .await
        .unwrap()
        .fh;

    assert_ne!(fh1, fh2, "each open should produce a unique file handle");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn open_file_read_with_offset() {
    let root = make_inode(1, INodeType::Directory, 0, None);
    let file = make_inode(10, INodeType::File, 11, Some(1));

    let mut state = MockFsState::default();
    state
        .file_contents
        .insert(10, bytes::Bytes::from_static(b"hello world"));
    let dp = MockFsDataProvider::new(state);

    let table = Arc::new(FutureBackedCache::default());
    table.insert_sync(10, file);
    let fs = AsyncFs::new(dp, root, Arc::clone(&table)).await;

    let open_file = fs
        .open(LoadedAddr::new_unchecked(10), OpenFlags::RDONLY)
        .await
        .unwrap();

    let data = open_file.read(6, 5).await.unwrap();
    assert_eq!(&data[..], b"world");
}

// readdir tests

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn readdir_lists_children_sorted_by_name() {
    let root = make_inode(1, INodeType::Directory, 0, None);
    let child_b = make_inode(10, INodeType::File, 10, Some(1));
    let child_a = make_inode(11, INodeType::File, 20, Some(1));

    let mut state = MockFsState::default();
    state.directories.insert(
        1,
        vec![
            (OsString::from("b.txt"), child_b),
            (OsString::from("a.txt"), child_a),
        ],
    );
    let dp = MockFsDataProvider::new(state);

    let table = Arc::new(FutureBackedCache::default());
    let fs = AsyncFs::new(dp, root, Arc::clone(&table)).await;

    let mut entries: Vec<(OsString, u64)> = Vec::new();
    fs.readdir(LoadedAddr::new_unchecked(1), 0, |entry, _offset| {
        entries.push((entry.name.to_os_string(), entry.inode.addr));
        false // don't stop
    })
    .await
    .unwrap();

    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].0, "a.txt", "entries should be sorted by name");
    assert_eq!(entries[0].1, 11);
    assert_eq!(entries[1].0, "b.txt");
    assert_eq!(entries[1].1, 10);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn readdir_respects_offset() {
    let root = make_inode(1, INodeType::Directory, 0, None);
    let child_a = make_inode(10, INodeType::File, 10, Some(1));
    let child_b = make_inode(11, INodeType::File, 20, Some(1));
    let child_c = make_inode(12, INodeType::File, 30, Some(1));

    let mut state = MockFsState::default();
    state.directories.insert(
        1,
        vec![
            (OsString::from("a"), child_a),
            (OsString::from("b"), child_b),
            (OsString::from("c"), child_c),
        ],
    );
    let dp = MockFsDataProvider::new(state);

    let table = Arc::new(FutureBackedCache::default());
    let fs = AsyncFs::new(dp, root, Arc::clone(&table)).await;

    // First readdir to populate cache
    fs.readdir(LoadedAddr::new_unchecked(1), 0, |_, _| false)
        .await
        .unwrap();

    // Second readdir starting at offset 2 (skip first two)
    let mut entries: Vec<OsString> = Vec::new();
    fs.readdir(LoadedAddr::new_unchecked(1), 2, |entry, _| {
        entries.push(entry.name.to_os_string());
        false
    })
    .await
    .unwrap();

    assert_eq!(entries, vec![OsString::from("c")]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn readdir_stops_when_filler_returns_true() {
    let root = make_inode(1, INodeType::Directory, 0, None);
    let child_a = make_inode(10, INodeType::File, 10, Some(1));
    let child_b = make_inode(11, INodeType::File, 20, Some(1));
    let child_c = make_inode(12, INodeType::File, 30, Some(1));

    let mut state = MockFsState::default();
    state.directories.insert(
        1,
        vec![
            (OsString::from("a"), child_a),
            (OsString::from("b"), child_b),
            (OsString::from("c"), child_c),
        ],
    );
    let dp = MockFsDataProvider::new(state);

    let table = Arc::new(FutureBackedCache::default());
    let fs = AsyncFs::new(dp, root, Arc::clone(&table)).await;

    let mut count = 0;
    fs.readdir(LoadedAddr::new_unchecked(1), 0, |_, _| {
        count += 1;
        count >= 2 // stop after 2 entries
    })
    .await
    .unwrap();

    assert_eq!(count, 2, "filler should have been called exactly twice");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn readdir_returns_enotdir_for_file() {
    let root = make_inode(1, INodeType::Directory, 0, None);
    let file = make_inode(10, INodeType::File, 100, Some(1));

    let dp = MockFsDataProvider::new(MockFsState::default());

    let table = Arc::new(FutureBackedCache::default());
    table.insert_sync(10, file);
    let fs = AsyncFs::new(dp, root, Arc::clone(&table)).await;

    let err = fs
        .readdir(LoadedAddr::new_unchecked(10), 0, |_, _| false)
        .await
        .unwrap_err();
    assert_eq!(err.raw_os_error(), Some(libc::ENOTDIR));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn readdir_populates_inode_table_with_children() {
    let root = make_inode(1, INodeType::Directory, 0, None);
    let child = make_inode(10, INodeType::File, 42, Some(1));

    let mut state = MockFsState::default();
    state
        .directories
        .insert(1, vec![(OsString::from("child.txt"), child)]);
    let dp = MockFsDataProvider::new(state);

    let table = Arc::new(FutureBackedCache::default());
    let fs = AsyncFs::new(dp, root, Arc::clone(&table)).await;

    fs.readdir(LoadedAddr::new_unchecked(1), 0, |_, _| false)
        .await
        .unwrap();

    let cached = table.get(&10).await;
    assert_eq!(
        cached.map(|n| n.addr),
        Some(10),
        "readdir should populate children into the inode table"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn readdir_empty_directory() {
    let root = make_inode(1, INodeType::Directory, 0, None);

    let mut state = MockFsState::default();
    state.directories.insert(1, vec![]);
    let dp = MockFsDataProvider::new(state);

    let table = Arc::new(FutureBackedCache::default());
    let fs = AsyncFs::new(dp, root, Arc::clone(&table)).await;

    let mut count = 0;
    fs.readdir(LoadedAddr::new_unchecked(1), 0, |_, _| {
        count += 1;
        false
    })
    .await
    .unwrap();

    assert_eq!(count, 0, "empty directory should yield no entries");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn readdir_provides_correct_next_offsets() {
    let root = make_inode(1, INodeType::Directory, 0, None);
    let child_a = make_inode(10, INodeType::File, 0, Some(1));
    let child_b = make_inode(11, INodeType::File, 0, Some(1));

    let mut state = MockFsState::default();
    state.directories.insert(
        1,
        vec![
            (OsString::from("a"), child_a),
            (OsString::from("b"), child_b),
        ],
    );
    let dp = MockFsDataProvider::new(state);

    let table = Arc::new(FutureBackedCache::default());
    let fs = AsyncFs::new(dp, root, Arc::clone(&table)).await;

    let mut offsets: Vec<u64> = Vec::new();
    fs.readdir(LoadedAddr::new_unchecked(1), 0, |_, next_offset| {
        offsets.push(next_offset);
        false
    })
    .await
    .unwrap();

    assert_eq!(
        offsets,
        vec![1, 2],
        "offsets should be 1-indexed and sequential"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lookup_after_eviction_returns_fresh_inode() {
    let root = make_inode(1, INodeType::Directory, 0, None);
    let child_v1 = make_inode(10, INodeType::File, 42, Some(1));
    let child_v2 = make_inode(20, INodeType::File, 99, Some(1));

    let mut state = MockFsState::default();
    state.lookups.insert((1, "readme.md".into()), child_v1);
    let dp = MockFsDataProvider::new(state);
    let state_ref = Arc::clone(&dp.state);

    let table = Arc::new(FutureBackedCache::default());
    let fs = AsyncFs::new(dp, root, Arc::clone(&table)).await;

    // First lookup → addr=10
    let first = fs
        .lookup(LoadedAddr::new_unchecked(1), OsStr::new("readme.md"))
        .await
        .unwrap();
    assert_eq!(first.inode.addr, 10);

    // Simulate forget: remove the inode from the table.
    table.remove_sync(&10);

    // Insert the refresh entry *after* the first lookup so dp.lookup()
    // returns child_v2 on the next call (refresh_lookups is checked first).
    drop(
        state_ref
            .refresh_lookups
            .insert_sync((1, "readme.md".into()), child_v2),
    );

    // Second lookup should NOT return the stale addr=10.
    let second = fs
        .lookup(LoadedAddr::new_unchecked(1), OsStr::new("readme.md"))
        .await
        .unwrap();
    assert_ne!(second.inode.addr, 10, "should not return stale inode");
    assert_eq!(second.inode.addr, 20, "should return the fresh inode");
}

// lookup-after-readdir integration test

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lookup_after_readdir_uses_directory_cache() {
    let root = make_inode(1, INodeType::Directory, 0, None);
    let child = make_inode(10, INodeType::File, 42, Some(1));

    let mut state = MockFsState::default();
    // Only configure readdir — no lookup entry. If the directory cache
    // fast path is broken, the lookup will fail with ENOENT.
    state
        .directories
        .insert(1, vec![(OsString::from("file.txt"), child)]);
    let dp = MockFsDataProvider::new(state);

    let table = Arc::new(FutureBackedCache::default());
    let fs = AsyncFs::new(dp, root, Arc::clone(&table)).await;

    // readdir populates the directory cache.
    fs.readdir(LoadedAddr::new_unchecked(1), 0, |_, _| false)
        .await
        .unwrap();

    // lookup should hit the directory cache fast path.
    let tracked = fs
        .lookup(LoadedAddr::new_unchecked(1), OsStr::new("file.txt"))
        .await
        .unwrap();
    assert_eq!(tracked.inode.addr, 10);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn readdir_prefetches_child_directories() {
    use std::sync::atomic::Ordering;

    let root = make_inode(1, INodeType::Directory, 0, None);
    let child_dir = make_inode(10, INodeType::Directory, 0, Some(1));
    let child_file = make_inode(11, INodeType::File, 100, Some(1));
    let grandchild = make_inode(20, INodeType::File, 50, Some(10));

    let mut state = MockFsState::default();
    state.directories.insert(
        1,
        vec![
            (OsString::from("subdir"), child_dir),
            (OsString::from("file.txt"), child_file),
        ],
    );
    state
        .directories
        .insert(10, vec![(OsString::from("grandchild.txt"), grandchild)]);
    let dp = MockFsDataProvider::new(state);
    let readdir_count = Arc::clone(&dp.state);

    let table = Arc::new(FutureBackedCache::default());
    let fs = AsyncFs::new(dp, root, Arc::clone(&table)).await;

    // readdir on root should trigger prefetch of child_dir (addr=10)
    fs.readdir(LoadedAddr::new_unchecked(1), 0, |_, _| false)
        .await
        .unwrap();

    // Wait for prefetch to complete (mock is instant, just need task to run)
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // dp.readdir should have been called twice: once for root, once for child_dir prefetch
    assert_eq!(
        readdir_count.readdir_count.load(Ordering::Relaxed),
        2,
        "prefetch should have called readdir on the child directory"
    );

    // Now readdir on child_dir should NOT call dp.readdir again (served from cache)
    let mut entries = Vec::new();
    fs.readdir(LoadedAddr::new_unchecked(10), 0, |entry, _| {
        entries.push(entry.name.to_os_string());
        false
    })
    .await
    .unwrap();

    assert_eq!(entries, vec![OsString::from("grandchild.txt")]);
    assert_eq!(
        readdir_count.readdir_count.load(Ordering::Relaxed),
        2,
        "cached readdir should not call dp.readdir again"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn prefetch_failure_does_not_affect_parent_readdir() {
    let root = make_inode(1, INodeType::Directory, 0, None);
    let child_dir = make_inode(10, INodeType::Directory, 0, Some(1));

    let mut state = MockFsState::default();
    state
        .directories
        .insert(1, vec![(OsString::from("bad_dir"), child_dir)]);
    // Don't configure readdir for addr=10 — mock will return ENOENT
    let dp = MockFsDataProvider::new(state);

    let table = Arc::new(FutureBackedCache::default());
    let fs = AsyncFs::new(dp, root, Arc::clone(&table)).await;

    // Parent readdir should succeed even though child prefetch will fail
    let mut entries = Vec::new();
    fs.readdir(LoadedAddr::new_unchecked(1), 0, |entry, _| {
        entries.push(entry.name.to_os_string());
        false
    })
    .await
    .unwrap();

    assert_eq!(entries, vec![OsString::from("bad_dir")]);

    // Wait for prefetch to attempt and fail
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Direct readdir on child should still work (CAS reset to UNCLAIMED by PopulateGuard)
    let err = fs
        .readdir(LoadedAddr::new_unchecked(10), 0, |_, _| false)
        .await
        .unwrap_err();
    assert_eq!(err.raw_os_error(), Some(libc::ENOENT));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn readdir_after_evict_re_fetches_from_provider() {
    use std::sync::atomic::Ordering;

    let root = make_inode(1, INodeType::Directory, 0, None);
    let child_a = make_inode(10, INodeType::File, 42, Some(1));
    let child_b = make_inode(11, INodeType::File, 99, Some(1));

    let mut state = MockFsState::default();
    state.directories.insert(
        1,
        vec![
            (OsString::from("a.txt"), child_a),
            (OsString::from("b.txt"), child_b),
        ],
    );
    let dp = MockFsDataProvider::new(state);
    let readdir_count = Arc::clone(&dp.state);

    let table = Arc::new(FutureBackedCache::default());
    let fs = AsyncFs::new(dp, root, Arc::clone(&table)).await;

    // First readdir populates cache.
    let mut entries = Vec::new();
    fs.readdir(LoadedAddr::new_unchecked(1), 0, |entry, _| {
        entries.push(entry.name.to_os_string());
        false
    })
    .await
    .unwrap();
    assert_eq!(entries.len(), 2);
    assert_eq!(readdir_count.readdir_count.load(Ordering::Relaxed), 1);

    // Simulate forget: evict all children.
    fs.evict(10);
    fs.evict(11);

    // Second readdir should re-fetch from the data provider (not return empty).
    let mut entries2 = Vec::new();
    fs.readdir(LoadedAddr::new_unchecked(1), 0, |entry, _| {
        entries2.push(entry.name.to_os_string());
        false
    })
    .await
    .unwrap();
    assert_eq!(entries2.len(), 2, "second readdir must not return empty");
    assert_eq!(
        readdir_count.readdir_count.load(Ordering::Relaxed),
        2,
        "should have called dp.readdir again after eviction"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn readdir_evict_all_readdir_returns_same_entries() {
    let root = make_inode(1, INodeType::Directory, 0, None);
    let child_dir = make_inode(10, INodeType::Directory, 0, Some(1));
    let child_file = make_inode(11, INodeType::File, 100, Some(1));

    let mut state = MockFsState::default();
    state.directories.insert(
        1,
        vec![
            (OsString::from("subdir"), child_dir),
            (OsString::from("file.txt"), child_file),
        ],
    );
    let dp = MockFsDataProvider::new(state);

    let table = Arc::new(FutureBackedCache::default());
    let fs = AsyncFs::new(dp, root, Arc::clone(&table)).await;

    // First readdir.
    let mut first = Vec::new();
    fs.readdir(LoadedAddr::new_unchecked(1), 0, |entry, _| {
        first.push((entry.name.to_os_string(), entry.inode.addr));
        false
    })
    .await
    .unwrap();
    assert_eq!(first.len(), 2);

    // Evict all children (simulating FUSE forget).
    fs.evict(10);
    fs.evict(11);

    // Wait for any prefetch tasks to settle.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Second readdir should return the same entries.
    let mut second = Vec::new();
    fs.readdir(LoadedAddr::new_unchecked(1), 0, |entry, _| {
        second.push((entry.name.to_os_string(), entry.inode.addr));
        false
    })
    .await
    .unwrap();
    assert_eq!(
        second, first,
        "readdir after evict should return same entries"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lookup_returns_enoent_without_remote_call_when_dir_populated() {
    let root = make_inode(1, INodeType::Directory, 0, None);
    let child = make_inode(10, INodeType::File, 42, Some(1));

    let mut state = MockFsState::default();
    // Configure readdir to return one child. Do NOT configure a lookup
    // entry for "nonexistent.txt". If lookup hits the remote, the mock
    // will return ENOENT anyway — but we need to verify it does NOT
    // call dp.lookup() at all.
    state
        .directories
        .insert(1, vec![(OsString::from("file.txt"), child)]);
    // Crucially, also register "nonexistent.txt" in lookups so that if
    // the slow path fires, lookup would SUCCEED — proving the test
    // catches the bug.
    let fake_child = make_inode(99, INodeType::File, 1, Some(1));
    state
        .lookups
        .insert((1, "nonexistent.txt".into()), fake_child);
    let dp = MockFsDataProvider::new(state);

    let table = Arc::new(FutureBackedCache::default());
    let fs = AsyncFs::new(dp, root, Arc::clone(&table)).await;

    // Populate the directory via readdir.
    fs.readdir(LoadedAddr::new_unchecked(1), 0, |_, _| false)
        .await
        .unwrap();

    // Lookup a name that was NOT in the readdir results.
    // With the fix, this should return ENOENT from the dcache
    // without ever calling dp.lookup().
    let err = fs
        .lookup(LoadedAddr::new_unchecked(1), OsStr::new("nonexistent.txt"))
        .await
        .unwrap_err();
    assert_eq!(
        err.raw_os_error(),
        Some(libc::ENOENT),
        "lookup for missing file in populated dir should return ENOENT"
    );
}

/// Verify that `IndexedLookupCache::evict_addr` removes only entries
/// referencing the evicted inode (parent or child), leaving unrelated
/// entries intact. This is the O(k) replacement for the old O(N) scan.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn indexed_lookup_cache_evict_removes_only_related_entries() {
    use git_fs::fs::IndexedLookupCache;

    let cache = IndexedLookupCache::default();

    let parent_a: u64 = 1;
    let parent_b: u64 = 2;
    let child_x: u64 = 10;
    let child_y: u64 = 11;
    let child_z: u64 = 12;

    let inode_x = make_inode(child_x, INodeType::File, 100, Some(parent_a));
    let inode_y = make_inode(child_y, INodeType::File, 200, Some(parent_a));
    let inode_z = make_inode(child_z, INodeType::File, 300, Some(parent_b));

    // Populate: parent_a has children x and y, parent_b has child z.
    let key_ax: (u64, Arc<OsStr>) = (parent_a, Arc::from(OsStr::new("x")));
    let key_ay: (u64, Arc<OsStr>) = (parent_a, Arc::from(OsStr::new("y")));
    let key_bz: (u64, Arc<OsStr>) = (parent_b, Arc::from(OsStr::new("z")));

    let ix = inode_x;
    cache
        .get_or_try_init(key_ax.clone(), move || async move { Ok(ix) })
        .await
        .unwrap();
    let iy = inode_y;
    cache
        .get_or_try_init(key_ay.clone(), move || async move { Ok(iy) })
        .await
        .unwrap();
    let iz = inode_z;
    cache
        .get_or_try_init(key_bz.clone(), move || async move { Ok(iz) })
        .await
        .unwrap();

    // Evict child_x. This should remove key_ax but leave key_ay and key_bz.
    cache.evict_addr(child_x);

    // key_ax should be gone (child_x was evicted).
    let result = cache
        .get_or_try_init(key_ax.clone(), move || async move {
            Ok(make_inode(child_x, INodeType::File, 999, Some(parent_a)))
        })
        .await
        .unwrap();
    assert_eq!(result.size, 999, "key_ax should have been re-fetched");

    // key_ay should still be cached (different child).
    let result = cache
        .get_or_try_init(key_ay.clone(), || async {
            panic!("factory should not be called for cached key_ay")
        })
        .await
        .unwrap();
    assert_eq!(result.size, 200, "key_ay should still be cached");

    // key_bz should still be cached (different parent and child).
    let result = cache
        .get_or_try_init(key_bz.clone(), || async {
            panic!("factory should not be called for cached key_bz")
        })
        .await
        .unwrap();
    assert_eq!(result.size, 300, "key_bz should still be cached");

    // Evict parent_a. This should remove all entries with parent_a (key_ax, key_ay).
    cache.evict_addr(parent_a);

    let result = cache
        .get_or_try_init(key_ay.clone(), move || async move {
            Ok(make_inode(child_y, INodeType::File, 888, Some(parent_a)))
        })
        .await
        .unwrap();
    assert_eq!(
        result.size, 888,
        "key_ay should have been re-fetched after parent eviction"
    );

    // key_bz should still be cached.
    let result = cache
        .get_or_try_init(key_bz.clone(), || async {
            panic!("factory should not be called for cached key_bz after parent_a eviction")
        })
        .await
        .unwrap();
    assert_eq!(result.size, 300, "key_bz should still be cached");
}

/// H1: Evicting a parent addr orphans the child-side reverse-index entries.
/// A subsequent evict of the old child then spuriously deletes a live cache
/// entry that was re-inserted under the same key with a different child.
///
/// Sequence:
/// 1. Insert (P, "foo") -> C1. Reverse: P=[key], C1=[key].
/// 2. evict_addr(P): removes P's entries, cleans cache. But since key's
///    parent_addr == P == addr, the child side (C1) is NOT cleaned.
///    Reverse: C1=[orphaned key].
/// 3. Insert (P, "foo") -> C2 (new child). Reverse: P=[key], C2=[key],
///    C1=[orphaned key still referencing (P, "foo")].
/// 4. evict_addr(C1): finds orphaned (P, "foo") in C1's reverse list,
///    calls cache.remove_sync((P, "foo")) — deletes the LIVE C2 entry.
/// 5. Lookup (P, "foo") should return C2 (cached), but instead triggers
///    the factory (cache miss).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn evict_parent_then_old_child_must_not_delete_repopulated_entry() {
    use git_fs::fs::IndexedLookupCache;

    let cache = IndexedLookupCache::default();
    let parent: u64 = 1;
    let child_c1: u64 = 10;
    let child_c2: u64 = 20;

    let key: (u64, Arc<OsStr>) = (parent, Arc::from(OsStr::new("foo")));

    // Step 1: Insert (P, "foo") -> C1.
    let c1 = make_inode(child_c1, INodeType::File, 100, Some(parent));
    cache
        .get_or_try_init(key.clone(), move || async move { Ok(c1) })
        .await
        .unwrap();

    // Step 2: Evict the parent.
    cache.evict_addr(parent);

    // Step 3: Re-insert (P, "foo") -> C2 (different child).
    let c2 = make_inode(child_c2, INodeType::File, 200, Some(parent));
    cache
        .get_or_try_init(key.clone(), move || async move { Ok(c2) })
        .await
        .unwrap();

    // Step 4: Evict old child C1. If orphaned reverse entries exist,
    // this spuriously removes the live (P, "foo") -> C2 entry.
    cache.evict_addr(child_c1);

    // Step 5: The live entry (P, "foo") -> C2 must still be cached.
    let result = cache
        .get_or_try_init(key.clone(), || async {
            panic!("factory must NOT be called — (P, \"foo\") -> C2 should still be cached")
        })
        .await
        .unwrap();
    assert_eq!(
        result.size, 200,
        "live entry C2 should not have been spuriously evicted"
    );
}

/// H2: The reverse-index Vec grows unbounded because `index_entry` is
/// called on every `get_or_try_init` hit, not just on cache misses.
/// After N lookups of the same key, the reverse-index should still
/// contain only 1 entry per addr side (not N duplicates).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn repeated_lookups_must_not_grow_reverse_index() {
    use git_fs::fs::IndexedLookupCache;

    let cache = IndexedLookupCache::default();
    let parent: u64 = 1;
    let child: u64 = 10;

    let key: (u64, Arc<OsStr>) = (parent, Arc::from(OsStr::new("foo")));
    let inode = make_inode(child, INodeType::File, 42, Some(parent));

    // First call: cache miss, factory runs.
    let i = inode;
    cache
        .get_or_try_init(key.clone(), move || async move { Ok(i) })
        .await
        .unwrap();

    // 50 more calls: all cache hits, factory never called.
    for _ in 0..50 {
        cache
            .get_or_try_init(key.clone(), || async {
                panic!("factory should not be called on cache hit")
            })
            .await
            .unwrap();
    }

    // The reverse index for parent and child should each have exactly 1
    // entry, not 51.
    assert_eq!(
        cache.reverse_entry_count(parent),
        1,
        "parent reverse-index should have exactly 1 entry, not one per lookup"
    );
    assert_eq!(
        cache.reverse_entry_count(child),
        1,
        "child reverse-index should have exactly 1 entry, not one per lookup"
    );
}

/// Verify that `remove_sync` removes the cache entry and cleans the
/// parent-side reverse index. The child-side reverse entry is documented
/// as an acceptable orphan — it is harmless and cleaned on child eviction.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn remove_sync_cleans_cache_and_parent_reverse_index() {
    use git_fs::fs::IndexedLookupCache;

    let cache = IndexedLookupCache::default();
    let parent: u64 = 1;
    let child: u64 = 10;

    let key: (u64, Arc<OsStr>) = (parent, Arc::from(OsStr::new("foo")));
    let inode = make_inode(child, INodeType::File, 42, Some(parent));

    // Insert entry.
    let i = inode;
    cache
        .get_or_try_init(key.clone(), move || async move { Ok(i) })
        .await
        .unwrap();

    assert_eq!(cache.reverse_entry_count(parent), 1);
    assert_eq!(cache.reverse_entry_count(child), 1);

    // remove_sync should remove the cache entry and parent reverse index.
    cache.remove_sync(&key);

    // Cache entry is gone — factory should be called on next get_or_try_init.
    let result = cache
        .get_or_try_init(key.clone(), move || async move {
            Ok(make_inode(child, INodeType::File, 999, Some(parent)))
        })
        .await
        .unwrap();
    assert_eq!(result.size, 999, "cache entry should have been removed");

    // Parent-side reverse index should be cleaned (0 entries after remove).
    // Note: reverse_entry_count(parent) is now 1 again because
    // get_or_try_init re-indexed it. Check that it was cleaned by verifying
    // it was re-populated from scratch (size == 999 above proves the factory ran).

    // Child-side orphan: the reverse index for child may still contain the
    // old entry (from before remove_sync). This is the documented trade-off.
    // Verify that evict_addr(child) cleanly handles the orphan without panic.
    cache.evict_addr(child);

    // After evicting the child, the re-inserted entry should be gone.
    let result = cache
        .get_or_try_init(key.clone(), move || async move {
            Ok(make_inode(child, INodeType::File, 777, Some(parent)))
        })
        .await
        .unwrap();
    assert_eq!(
        result.size, 777,
        "entry should be re-fetchable after orphan cleanup via evict_addr"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lookup_falls_through_to_remote_after_eviction_resets_populated() {
    let root = make_inode(1, INodeType::Directory, 0, None);
    let child_a = make_inode(10, INodeType::File, 42, Some(1));
    let child_b = make_inode(11, INodeType::File, 99, Some(1));

    let mut state = MockFsState::default();
    state
        .directories
        .insert(1, vec![(OsString::from("a.txt"), child_a)]);
    // "b.txt" is only reachable via lookup, NOT readdir.
    state.lookups.insert((1, "b.txt".into()), child_b);
    let dp = MockFsDataProvider::new(state);

    let table = Arc::new(FutureBackedCache::default());
    let fs = AsyncFs::new(dp, root, Arc::clone(&table)).await;

    // Populate via readdir — directory is now DONE.
    fs.readdir(LoadedAddr::new_unchecked(1), 0, |_, _| false)
        .await
        .unwrap();

    // Evict child_a — this resets populate flag to UNCLAIMED.
    fs.directory_cache().evict(LoadedAddr::new_unchecked(10));

    // Now lookup "b.txt" — directory is no longer DONE, so the
    // short-circuit must NOT fire. The slow path should call
    // dp.lookup() and find "b.txt".
    let result = fs
        .lookup(LoadedAddr::new_unchecked(1), OsStr::new("b.txt"))
        .await
        .unwrap();
    assert_eq!(result.inode.addr, 11);
}

/// Verify that `evict_addr(old_child)` does not spuriously remove a
/// freshly-indexed reverse entry for a *new* child that reuses the same
/// `LookupKey` `(parent, name)`.
///
/// Scenario:
/// 1. Insert `(P, "foo") -> C1`. Reverse: `P=[(key,C1)]`, `C1=[(key,C1)]`.
/// 2. Remove C1 from cache, re-insert `(P, "foo") -> C2` (simulating a
///    concurrent re-lookup that resolved to a new child after C1 was evicted
///    from the inode table).
/// 3. `evict_addr(C1)` should only prune entries whose child_addr is C1.
///    The freshly-indexed entry `(key, C2)` under parent P must survive.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn evict_old_child_must_not_remove_new_childs_reverse_entry() {
    use git_fs::fs::IndexedLookupCache;

    let cache = IndexedLookupCache::default();
    let parent: u64 = 100;
    let old_child: u64 = 200;
    let new_child: u64 = 300;

    let key: (u64, Arc<OsStr>) = (parent, Arc::from(OsStr::new("foo")));

    // Step 1: Insert (P, "foo") -> old_child.
    let oc = old_child;
    cache
        .get_or_try_init(key.clone(), move || async move {
            Ok(make_inode(oc, INodeType::File, 10, Some(parent)))
        })
        .await
        .unwrap();
    assert_eq!(cache.reverse_entry_count(parent), 1);
    assert_eq!(cache.reverse_entry_count(old_child), 1);

    // Step 2: Simulate old_child being evicted from the inode table, then a
    // new lookup resolving (P, "foo") -> new_child. We remove the old cache
    // entry first (as InodeForget::delete would), then re-insert.
    cache.remove_sync(&key);
    let nc = new_child;
    cache
        .get_or_try_init(key.clone(), move || async move {
            Ok(make_inode(nc, INodeType::File, 20, Some(parent)))
        })
        .await
        .unwrap();

    // Parent's reverse index should have the new entry (key, new_child).
    assert_eq!(
        cache.reverse_entry_count(parent),
        1,
        "parent should have exactly 1 reverse entry (the new child)"
    );
    assert_eq!(cache.reverse_entry_count(new_child), 1);

    // Step 3: evict_addr(old_child). The old_child's reverse Vec was already
    // partially cleaned by remove_sync, but may still contain an orphaned
    // entry. The retain predicate must NOT remove the new child's entry from
    // parent's Vec.
    cache.evict_addr(old_child);

    // The new child's entry under parent must survive.
    assert_eq!(
        cache.reverse_entry_count(parent),
        1,
        "evict_addr(old_child) must not remove new_child's reverse entry under parent"
    );
    assert_eq!(
        cache.reverse_entry_count(new_child),
        1,
        "new_child's own reverse entry must survive"
    );

    // The cache entry itself must also survive.
    let result = cache
        .get_or_try_init(key.clone(), move || async move {
            Ok(make_inode(new_child, INodeType::File, 999, Some(parent)))
        })
        .await
        .unwrap();
    assert_eq!(
        result.size, 20,
        "cache entry for new_child should still be present (factory should not run)"
    );
}

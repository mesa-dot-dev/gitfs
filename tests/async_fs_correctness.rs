#![allow(clippy::unwrap_used, clippy::expect_used, missing_docs)]

mod common;

use std::ffi::{OsStr, OsString};
use std::sync::Arc;

use git_fs::cache::async_backed::FutureBackedCache;
use git_fs::fs::async_fs::{AsyncFs, InodeLifecycle};
use git_fs::fs::{INode, INodeType, LoadedAddr, OpenFlags};

use common::async_fs_mocks::{MockFsDataProvider, MockFsState, make_inode};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lifecycle_inc_returns_count_after_increment() {
    let table = FutureBackedCache::default();
    let inode = make_inode(100, INodeType::File, 0, Some(1));
    table.insert_sync(100, inode);

    let mut lifecycle = InodeLifecycle::from_table(table);

    assert_eq!(lifecycle.inc(100), 1, "first inc should return 1");
    assert_eq!(lifecycle.inc(100), 2, "second inc should return 2");
    assert_eq!(lifecycle.inc(100), 3, "third inc should return 3");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lifecycle_dec_returns_remaining_count() {
    let table = FutureBackedCache::default();
    let inode = make_inode(100, INodeType::File, 0, Some(1));
    table.insert_sync(100, inode);

    let mut lifecycle = InodeLifecycle::from_table(table);
    lifecycle.inc(100);
    lifecycle.inc(100);

    assert_eq!(lifecycle.dec(&100), Some(1), "dec from 2 should give 1");
    assert_eq!(lifecycle.dec(&100), Some(0), "dec from 1 should give 0");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lifecycle_dec_unknown_addr_returns_none() {
    let table: FutureBackedCache<u64, INode> = FutureBackedCache::default();
    let mut lifecycle = InodeLifecycle::from_table(table);

    assert_eq!(
        lifecycle.dec(&999),
        None,
        "dec on unknown key should return None"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lifecycle_dec_to_zero_evicts_from_table() {
    let table = FutureBackedCache::default();
    let inode = make_inode(100, INodeType::File, 0, Some(1));
    table.insert_sync(100, inode);

    let mut lifecycle = InodeLifecycle::from_table(table);
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
    let table: FutureBackedCache<u64, INode> = FutureBackedCache::default();
    let inode = make_inode(100, INodeType::File, 0, Some(1));
    table.insert_sync(100, inode);

    let mut lifecycle = InodeLifecycle::from_table(table);
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
    let table = FutureBackedCache::default();
    let inode = make_inode(100, INodeType::File, 0, Some(1));
    table.insert_sync(100, inode);

    let mut lifecycle = InodeLifecycle::from_table(table);
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
    let table = FutureBackedCache::default();
    let inode = make_inode(42, INodeType::Directory, 0, None);
    table.insert_sync(42, inode);

    let lifecycle = InodeLifecycle::from_table(table);

    let fetched = lifecycle.table().get(&42).await;
    assert_eq!(
        fetched.map(|n| n.addr),
        Some(42),
        "table() should expose the underlying cache"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn new_seeds_root_inode_into_table() {
    let table = FutureBackedCache::default();
    let root = make_inode(1, INodeType::Directory, 0, None);
    let dp = MockFsDataProvider::new(MockFsState::default());

    let fs = AsyncFs::new(dp, root, &table).await;

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
    let table: FutureBackedCache<u64, INode> = FutureBackedCache::default();
    let dp = MockFsDataProvider::new(MockFsState::default());

    let fs = AsyncFs::new_preseeded(dp, &table);

    assert_eq!(
        fs.inode_count(),
        0,
        "preseeded constructor should not insert anything"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn statfs_reports_inode_count() {
    let table = FutureBackedCache::default();
    let root = make_inode(1, INodeType::Directory, 0, None);
    let dp = MockFsDataProvider::new(MockFsState::default());

    let fs = AsyncFs::new(dp, root, &table).await;
    let stats = fs.statfs();

    assert_eq!(stats.block_size, 4096);
    assert_eq!(stats.total_inodes, 1, "should reflect the root inode");
    assert_eq!(stats.free_blocks, 0);
    assert_eq!(stats.max_filename_length, 255);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn loaded_inode_returns_seeded_inode() {
    let table = FutureBackedCache::default();
    let root = make_inode(1, INodeType::Directory, 0, None);
    let dp = MockFsDataProvider::new(MockFsState::default());

    let fs = AsyncFs::new(dp, root, &table).await;

    let inode = fs.loaded_inode(LoadedAddr(1)).await.unwrap();
    assert_eq!(inode.addr, 1);
    assert_eq!(inode.itype, INodeType::Directory);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn loaded_inode_returns_enoent_for_missing_addr() {
    let table = FutureBackedCache::default();
    let root = make_inode(1, INodeType::Directory, 0, None);
    let dp = MockFsDataProvider::new(MockFsState::default());

    let fs = AsyncFs::new(dp, root, &table).await;

    let err = fs.loaded_inode(LoadedAddr(999)).await.unwrap_err();
    assert_eq!(err.raw_os_error(), Some(libc::ENOENT));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn getattr_delegates_to_loaded_inode() {
    let table = FutureBackedCache::default();
    let root = make_inode(1, INodeType::Directory, 4096, None);
    let dp = MockFsDataProvider::new(MockFsState::default());

    let fs = AsyncFs::new(dp, root, &table).await;

    let inode = fs.getattr(LoadedAddr(1)).await.unwrap();
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

    let table = FutureBackedCache::default();
    let fs = AsyncFs::new(dp, root, &table).await;

    let tracked = fs
        .lookup(LoadedAddr(1), OsStr::new("readme.md"))
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

    let table = FutureBackedCache::default();
    let fs = AsyncFs::new(dp, root, &table).await;

    fs.lookup(LoadedAddr(1), OsStr::new("file.txt"))
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

    let table = FutureBackedCache::default();
    let fs = AsyncFs::new(dp, root, &table).await;

    let first = fs
        .lookup(LoadedAddr(1), OsStr::new("cached.txt"))
        .await
        .unwrap();
    let second = fs
        .lookup(LoadedAddr(1), OsStr::new("cached.txt"))
        .await
        .unwrap();

    assert_eq!(first.inode.addr, second.inode.addr);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lookup_propagates_provider_error() {
    let root = make_inode(1, INodeType::Directory, 0, None);
    // No lookups configured — provider will return ENOENT.
    let dp = MockFsDataProvider::new(MockFsState::default());

    let table = FutureBackedCache::default();
    let fs = AsyncFs::new(dp, root, &table).await;

    let err = fs
        .lookup(LoadedAddr(1), OsStr::new("nonexistent"))
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

    let table = FutureBackedCache::default();
    table.insert_sync(10, file);
    let fs = AsyncFs::new(dp, root, &table).await;

    let open_file = fs.open(LoadedAddr(10), OpenFlags::RDONLY).await.unwrap();

    assert!(open_file.fh >= 1, "file handle should start at 1");
    let data = open_file.read(0, 5).await.unwrap();
    assert_eq!(&data[..], b"hello");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn open_returns_eisdir_for_directory() {
    let root = make_inode(1, INodeType::Directory, 0, None);
    let dp = MockFsDataProvider::new(MockFsState::default());

    let table = FutureBackedCache::default();
    let fs = AsyncFs::new(dp, root, &table).await;

    let err = fs.open(LoadedAddr(1), OpenFlags::RDONLY).await.unwrap_err();
    assert_eq!(err.raw_os_error(), Some(libc::EISDIR));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn open_returns_enoent_for_missing_inode() {
    let root = make_inode(1, INodeType::Directory, 0, None);
    let dp = MockFsDataProvider::new(MockFsState::default());

    let table = FutureBackedCache::default();
    let fs = AsyncFs::new(dp, root, &table).await;

    let err = fs
        .open(LoadedAddr(999), OpenFlags::RDONLY)
        .await
        .unwrap_err();
    assert_eq!(err.raw_os_error(), Some(libc::ENOENT));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn open_assigns_unique_file_handles() {
    let root = make_inode(1, INodeType::Directory, 0, None);
    let file = make_inode(10, INodeType::File, 0, Some(1));

    let dp = MockFsDataProvider::new(MockFsState::default());

    let table = FutureBackedCache::default();
    table.insert_sync(10, file);
    let fs = AsyncFs::new(dp, root, &table).await;

    let fh1 = fs.open(LoadedAddr(10), OpenFlags::RDONLY).await.unwrap().fh;
    let fh2 = fs.open(LoadedAddr(10), OpenFlags::RDONLY).await.unwrap().fh;

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

    let table = FutureBackedCache::default();
    table.insert_sync(10, file);
    let fs = AsyncFs::new(dp, root, &table).await;

    let open_file = fs.open(LoadedAddr(10), OpenFlags::RDONLY).await.unwrap();

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

    let table = FutureBackedCache::default();
    let fs = AsyncFs::new(dp, root, &table).await;

    let mut entries: Vec<(OsString, u64)> = Vec::new();
    fs.readdir(LoadedAddr(1), 0, |entry, _offset| {
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

    let table = FutureBackedCache::default();
    let fs = AsyncFs::new(dp, root, &table).await;

    // First readdir to populate cache
    fs.readdir(LoadedAddr(1), 0, |_, _| false).await.unwrap();

    // Second readdir starting at offset 2 (skip first two)
    let mut entries: Vec<OsString> = Vec::new();
    fs.readdir(LoadedAddr(1), 2, |entry, _| {
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

    let table = FutureBackedCache::default();
    let fs = AsyncFs::new(dp, root, &table).await;

    let mut count = 0;
    fs.readdir(LoadedAddr(1), 0, |_, _| {
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

    let table = FutureBackedCache::default();
    table.insert_sync(10, file);
    let fs = AsyncFs::new(dp, root, &table).await;

    let err = fs
        .readdir(LoadedAddr(10), 0, |_, _| false)
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

    let table = FutureBackedCache::default();
    let fs = AsyncFs::new(dp, root, &table).await;

    fs.readdir(LoadedAddr(1), 0, |_, _| false).await.unwrap();

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

    let table = FutureBackedCache::default();
    let fs = AsyncFs::new(dp, root, &table).await;

    let mut count = 0;
    fs.readdir(LoadedAddr(1), 0, |_, _| {
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

    let table = FutureBackedCache::default();
    let fs = AsyncFs::new(dp, root, &table).await;

    let mut offsets: Vec<u64> = Vec::new();
    fs.readdir(LoadedAddr(1), 0, |_, next_offset| {
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

    let table = FutureBackedCache::default();
    let fs = AsyncFs::new(dp, root, &table).await;

    // First lookup → addr=10
    let first = fs
        .lookup(LoadedAddr(1), OsStr::new("readme.md"))
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
        .lookup(LoadedAddr(1), OsStr::new("readme.md"))
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

    let table = FutureBackedCache::default();
    let fs = AsyncFs::new(dp, root, &table).await;

    // readdir populates the directory cache.
    fs.readdir(LoadedAddr(1), 0, |_, _| false).await.unwrap();

    // lookup should hit the directory cache fast path.
    let tracked = fs
        .lookup(LoadedAddr(1), OsStr::new("file.txt"))
        .await
        .unwrap();
    assert_eq!(tracked.inode.addr, 10);
}

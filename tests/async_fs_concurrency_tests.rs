#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::doc_markdown,
    clippy::similar_names,
    clippy::iter_on_single_items,
    let_underscore_drop,
    missing_docs
)]

mod common;

use std::sync::Arc;

use bytes::Bytes;
use git_fs::cache::async_backed::FutureBackedCache;
use git_fs::drop_ward::StatelessDrop as _;
use git_fs::fs::async_fs::{AsyncFs, ForgetContext};
use git_fs::fs::{INodeType, InodeForget, LoadedAddr, OpenFlags};

use common::async_fs_mocks::{MockFsDataProvider, MockFsState, make_dcache, make_inode};

/// Helper: build an `AsyncFs` with a single file `test.txt` (addr=2) under root (addr=1).
async fn setup_single_file() -> (
    Arc<AsyncFs<MockFsDataProvider>>,
    Arc<FutureBackedCache<u64, git_fs::fs::INode>>,
    Arc<MockFsState>,
) {
    let root = make_inode(1, INodeType::Directory, 0, None);
    let file = make_inode(2, INodeType::File, 0, Some(1));

    let write_calls = Arc::new(tokio::sync::Mutex::new(Vec::new()));
    let write_notify = Arc::new(tokio::sync::Notify::new());
    let state = MockFsState {
        lookups: [((1, "test.txt".into()), file)].into_iter().collect(),
        directories: [(1, vec![("test.txt".into(), file)])].into_iter().collect(),
        file_contents: [(2, Bytes::from_static(b""))].into_iter().collect(),
        write_calls: Arc::clone(&write_calls),
        write_notify: Arc::clone(&write_notify),
        next_addr: std::sync::atomic::AtomicU64::new(100),
        ..MockFsState::default()
    };
    let provider = MockFsDataProvider::new(state);
    let state_ref = Arc::clone(&provider.state);
    let table = Arc::new(FutureBackedCache::default());
    let fs = AsyncFs::new(provider, root, Arc::clone(&table), make_dcache()).await;
    let fs = Arc::new(fs);

    // Load the file inode into the table via lookup.
    let _ = fs
        .lookup(LoadedAddr::new_unchecked(1), "test.txt".as_ref())
        .await
        .unwrap();

    (fs, table, state_ref)
}

/// Multiple tasks writing concurrently to the same file must all succeed
/// and the final overlay state must be consistent (matching the last write's
/// content, not a torn/corrupted mix).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_writes_to_same_file_are_serialized() {
    let (fs, _table, state) = setup_single_file().await;
    let addr = LoadedAddr::new_unchecked(2);

    let num_writers = 8;
    let mut handles = Vec::new();

    for i in 0..num_writers {
        let fs = Arc::clone(&fs);
        handles.push(tokio::spawn(async move {
            // Each writer writes a distinctive payload at offset 0.
            let payload = format!("writer-{i:02}");
            fs.write(addr, 0, Bytes::from(payload))
                .await
                .expect("concurrent write should succeed");
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    // Read back — content must be exactly one writer's payload (not corrupted).
    let open = fs.open(addr, OpenFlags::RDONLY).await.unwrap();
    let data = open.read(0, 1024).await.unwrap();
    let content = std::str::from_utf8(&data).expect("should be valid utf8");
    assert!(
        content.starts_with("writer-"),
        "overlay content should be a complete writer payload, got: {content:?}"
    );
    assert_eq!(
        content.len(),
        "writer-00".len(),
        "overlay content should not be corrupted or concatenated"
    );

    // Provider should have received exactly num_writers calls, each with
    // full file content (not partial).
    let calls = state.write_calls.lock().await;
    assert_eq!(
        calls.len(),
        num_writers,
        "provider should receive exactly one write per FUSE write"
    );
    for (i, call) in calls.iter().enumerate() {
        let payload = std::str::from_utf8(&call.2).expect("should be valid utf8");
        assert!(
            payload.starts_with("writer-"),
            "call {i}: provider should receive a complete payload, got: {payload:?}"
        );
    }
}

/// Concurrent write and setattr(truncate=0) to the same file must not
/// corrupt the overlay. After both operations complete, the overlay must
/// contain either the written data (write happened last) or be empty
/// (truncate happened last) — never a partial or corrupt state.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_write_and_setattr_truncate() {
    let (fs, _table, state) = setup_single_file().await;
    let addr = LoadedAddr::new_unchecked(2);

    // Run many rounds to increase the chance of hitting the interleaving.
    for _round in 0..20 {
        // Reset overlay to known state.
        fs.write(addr, 0, Bytes::from_static(b"initial"))
            .await
            .unwrap();

        let fs_w = Arc::clone(&fs);
        let fs_t = Arc::clone(&fs);

        let write_handle = tokio::spawn(async move {
            fs_w.write(addr, 0, Bytes::from_static(b"WRITTEN"))
                .await
                .unwrap();
        });

        let truncate_handle = tokio::spawn(async move {
            fs_t.setattr(addr, Some(0), None, None).await.unwrap();
        });

        write_handle.await.unwrap();
        truncate_handle.await.unwrap();

        // Read back — must be consistent.
        let open = fs.open(addr, OpenFlags::RDONLY).await.unwrap();
        let data = open.read(0, 1024).await.unwrap();
        assert!(
            data.is_empty() || &data[..] == b"WRITTEN",
            "overlay must be either empty (truncate won) or 'WRITTEN' (write won), got: {:?}",
            std::str::from_utf8(&data)
        );

        // Inode size must be consistent with overlay content.
        let inode = fs.getattr(addr).await.unwrap();
        assert_eq!(
            inode.size,
            data.len() as u64,
            "inode size must match overlay content length"
        );
    }

    // Provider write ordering: each write call should contain full file
    // content (never partial). We just verify no panics occurred above and
    // that all calls have offset 0.
    let calls = state.write_calls.lock().await;
    for call in calls.iter() {
        assert_eq!(call.1, 0, "provider writes should always use offset 0");
    }
}

/// Concurrent create and unlink of the same filename must not panic and
/// should leave the directory in a consistent state.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_create_and_unlink_same_name() {
    let root = make_inode(1, INodeType::Directory, 0, None);

    let state = MockFsState {
        directories: [(1, vec![])].into_iter().collect(),
        next_addr: std::sync::atomic::AtomicU64::new(100),
        ..MockFsState::default()
    };
    let provider = MockFsDataProvider::new(state);
    let table = Arc::new(FutureBackedCache::default());
    let fs = Arc::new(AsyncFs::new(provider, root, Arc::clone(&table), make_dcache()).await);

    // Populate the directory cache.
    fs.readdir(LoadedAddr::new_unchecked(1), 0, |_, _| false)
        .await
        .unwrap();

    // Create the file first so unlink has something to work with.
    drop(
        fs.create(LoadedAddr::new_unchecked(1), "race.txt".as_ref(), 0o644)
            .await
            .unwrap(),
    );

    for _round in 0..20 {
        // Create the file — ignore failures (EEXIST or similar), we just
        // want to exercise the concurrent path.
        drop(
            fs.create(LoadedAddr::new_unchecked(1), "race.txt".as_ref(), 0o644)
                .await,
        );

        let fs_u = Arc::clone(&fs);
        let fs_c = Arc::clone(&fs);

        let unlink_handle = tokio::spawn(async move {
            drop(
                fs_u.unlink(LoadedAddr::new_unchecked(1), "race.txt".as_ref())
                    .await,
            );
        });

        let create_handle = tokio::spawn(async move {
            drop(
                fs_c.create(LoadedAddr::new_unchecked(1), "race.txt".as_ref(), 0o644)
                    .await,
            );
        });

        unlink_handle.await.unwrap();
        create_handle.await.unwrap();
    }

    // Verify the directory is still consistent — readdir should not panic.
    // The file may or may not exist depending on race outcome — either is fine.
    // The key assertion is that we didn't panic or corrupt the dcache.
    fs.readdir(LoadedAddr::new_unchecked(1), 0, |_, _| false)
        .await
        .unwrap();
}

/// Concurrent unlink and lookup on the same file must not panic. The lookup
/// should return either the inode (unlink hasn't happened yet) or ENOENT
/// (unlink won the race).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_unlink_and_lookup() {
    let root = make_inode(1, INodeType::Directory, 0, None);
    let file = make_inode(2, INodeType::File, 5, Some(1));

    let state = MockFsState {
        lookups: [((1, "target.txt".into()), file)].into_iter().collect(),
        directories: [(1, vec![("target.txt".into(), file)])]
            .into_iter()
            .collect(),
        file_contents: [(2, Bytes::from_static(b"hello"))].into_iter().collect(),
        next_addr: std::sync::atomic::AtomicU64::new(100),
        ..MockFsState::default()
    };
    let provider = MockFsDataProvider::new(state);
    let table = Arc::new(FutureBackedCache::default());
    let fs = Arc::new(AsyncFs::new(provider, root, Arc::clone(&table), make_dcache()).await);

    // Populate directory cache.
    fs.readdir(LoadedAddr::new_unchecked(1), 0, |_, _| false)
        .await
        .unwrap();
    // Load the inode.
    let _ = fs
        .lookup(LoadedAddr::new_unchecked(1), "target.txt".as_ref())
        .await
        .unwrap();

    let fs_u = Arc::clone(&fs);
    let fs_l = Arc::clone(&fs);

    let unlink_handle = tokio::spawn(async move {
        drop(
            fs_u.unlink(LoadedAddr::new_unchecked(1), "target.txt".as_ref())
                .await,
        );
    });

    let lookup_handle = tokio::spawn(async move {
        let result = fs_l
            .lookup(LoadedAddr::new_unchecked(1), "target.txt".as_ref())
            .await;
        // Either Ok (found before unlink) or ENOENT (found after unlink) is valid.
        if let Err(ref e) = result {
            assert_eq!(
                e.raw_os_error(),
                Some(libc::ENOENT),
                "lookup error should be ENOENT, got: {e}"
            );
        }
    });

    unlink_handle.await.unwrap();
    lookup_handle.await.unwrap();

    // Directory should be consistent after the race.
    let mut entries = Vec::new();
    fs.readdir(LoadedAddr::new_unchecked(1), 0, |entry, _| {
        entries.push(entry.name.to_os_string());
        false
    })
    .await
    .unwrap();
    // File should be gone (unlink always fires).
    assert!(
        !entries.iter().any(|n| n == "target.txt"),
        "file should be removed after unlink"
    );
}

/// Full lifecycle: create → write → unlink → forget. Verifies that the
/// inode, overlay, and unlinked set are properly cleaned up after forget
/// on an unlinked inode.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn create_write_unlink_forget_lifecycle() {
    let root = make_inode(1, INodeType::Directory, 0, None);

    let state = MockFsState {
        directories: [(1, vec![])].into_iter().collect(),
        next_addr: std::sync::atomic::AtomicU64::new(100),
        ..MockFsState::default()
    };
    let provider = MockFsDataProvider::new(state);
    let table = Arc::new(FutureBackedCache::default());
    let fs = AsyncFs::new(provider.clone(), root, Arc::clone(&table), make_dcache()).await;

    // Populate directory cache.
    fs.readdir(LoadedAddr::new_unchecked(1), 0, |_, _| false)
        .await
        .unwrap();

    // Create.
    let (child, _open) = fs
        .create(
            LoadedAddr::new_unchecked(1),
            "lifecycle.txt".as_ref(),
            0o644,
        )
        .await
        .unwrap();
    let child_addr = child.addr;

    // Write.
    fs.write(
        LoadedAddr::new_unchecked(child_addr),
        0,
        Bytes::from_static(b"lifecycle data"),
    )
    .await
    .unwrap();

    // Verify inode is in table and overlay.
    assert!(table.get(&child_addr).await.is_some());
    assert!(fs.write_overlay().get(&child_addr).await.is_some());

    // Unlink.
    fs.unlink(LoadedAddr::new_unchecked(1), "lifecycle.txt".as_ref())
        .await
        .unwrap();

    // After unlink: inode and overlay should still exist (POSIX: open handles valid).
    assert!(
        table.get(&child_addr).await.is_some(),
        "inode should survive unlink"
    );
    assert!(
        fs.write_overlay().get(&child_addr).await.is_some(),
        "overlay should survive unlink"
    );
    assert!(
        fs.unlinked_inodes().contains_sync(&child_addr),
        "inode should be in unlinked set"
    );

    // Forget — simulates FUSE refcount dropping to zero.
    let ctx = ForgetContext {
        inode_table: Arc::clone(&table),
        dcache: fs.directory_cache(),
        lookup_cache: fs.lookup_cache(),
        provider: provider.clone(),
        write_overlay: fs.write_overlay(),
        unlinked_inodes: fs.unlinked_inodes(),
    };
    InodeForget::delete(&ctx, &child_addr);

    // After forget on unlinked inode: full cleanup.
    assert!(
        table.get(&child_addr).await.is_none(),
        "inode should be evicted after forget on unlinked inode"
    );
    assert!(
        fs.write_overlay().get(&child_addr).await.is_none(),
        "overlay should be cleaned up after forget on unlinked inode"
    );
    assert!(
        !fs.unlinked_inodes().contains_sync(&child_addr),
        "unlinked set should be cleaned up after forget"
    );
}

/// Multiple concurrent writes to different files should not interfere with
/// each other. The per-inode lock only serializes writes to the same inode,
/// so writes to different inodes should proceed in parallel.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_writes_to_different_files() {
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
        file_contents: [(2, Bytes::new()), (3, Bytes::new())].into_iter().collect(),
        ..MockFsState::default()
    };
    let provider = MockFsDataProvider::new(state);
    let table = Arc::new(FutureBackedCache::default());
    let fs = Arc::new(AsyncFs::new(provider, root, Arc::clone(&table), make_dcache()).await);

    let _ = fs
        .lookup(LoadedAddr::new_unchecked(1), "a.txt".as_ref())
        .await
        .unwrap();
    let _ = fs
        .lookup(LoadedAddr::new_unchecked(1), "b.txt".as_ref())
        .await
        .unwrap();

    let fs_a = Arc::clone(&fs);
    let fs_b = Arc::clone(&fs);

    let handle_a = tokio::spawn(async move {
        for i in 0..10 {
            let payload = format!("a-{i}");
            fs_a.write(LoadedAddr::new_unchecked(2), 0, Bytes::from(payload))
                .await
                .unwrap();
        }
    });

    let handle_b = tokio::spawn(async move {
        for i in 0..10 {
            let payload = format!("b-{i}");
            fs_b.write(LoadedAddr::new_unchecked(3), 0, Bytes::from(payload))
                .await
                .unwrap();
        }
    });

    handle_a.await.unwrap();
    handle_b.await.unwrap();

    // Each file should have its own writer's content, not the other's.
    let open_a = fs
        .open(LoadedAddr::new_unchecked(2), OpenFlags::RDONLY)
        .await
        .unwrap();
    let data_a = open_a.read(0, 1024).await.unwrap();
    assert!(
        std::str::from_utf8(&data_a).unwrap().starts_with("a-"),
        "file a should contain writer a's data"
    );

    let open_b = fs
        .open(LoadedAddr::new_unchecked(3), OpenFlags::RDONLY)
        .await
        .unwrap();
    let data_b = open_b.read(0, 1024).await.unwrap();
    assert!(
        std::str::from_utf8(&data_b).unwrap().starts_with("b-"),
        "file b should contain writer b's data"
    );
}

/// Provider write ordering: sequential writes to the same file under the
/// per-inode lock should produce monotonically growing provider write calls
/// (each containing the full accumulated content).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn provider_writes_are_ordered_under_inode_lock() {
    let (fs, _table, state) = setup_single_file().await;
    let addr = LoadedAddr::new_unchecked(2);

    // Write three chunks sequentially — each should produce a provider call
    // with the full accumulated content.
    fs.write(addr, 0, Bytes::from_static(b"A")).await.unwrap();
    fs.write(addr, 1, Bytes::from_static(b"B")).await.unwrap();
    fs.write(addr, 2, Bytes::from_static(b"C")).await.unwrap();

    let calls = state.write_calls.lock().await;
    assert_eq!(calls.len(), 3);

    // Each call should contain the full file content at that point in time.
    assert_eq!(&calls[0].2[..], b"A");
    assert_eq!(&calls[1].2[..], b"AB");
    assert_eq!(&calls[2].2[..], b"ABC");
}

/// setattr(truncate) followed by write under concurrent pressure should
/// maintain MPSC ordering: the truncation's provider write arrives before
/// the subsequent write's provider write.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn setattr_then_write_maintains_provider_ordering() {
    let (fs, _table, state) = setup_single_file().await;
    let addr = LoadedAddr::new_unchecked(2);

    // Seed with initial content.
    fs.write(addr, 0, Bytes::from_static(b"hello world"))
        .await
        .unwrap();

    // Truncate to 0.
    fs.setattr(addr, Some(0), None, None).await.unwrap();

    // Write new content.
    fs.write(addr, 0, Bytes::from_static(b"fresh"))
        .await
        .unwrap();

    let calls = state.write_calls.lock().await;
    // We should have: write("hello world"), then the setattr truncation may
    // or may not produce a provider write (it skips when content is empty),
    // then write("fresh").
    let last = calls.last().unwrap();
    assert_eq!(
        &last.2[..],
        b"fresh",
        "last provider write should be the fresh content"
    );
}

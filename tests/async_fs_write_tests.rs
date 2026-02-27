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
use git_fs::cache::async_backed::FutureBackedCache;
use git_fs::drop_ward::StatelessDrop as _;
use git_fs::fs::async_fs::{AsyncFs, ForgetContext, FsDataProvider as _};
use git_fs::fs::{INodeType, InodeForget, LoadedAddr, OpenFlags};

use common::async_fs_mocks::{MockFsDataProvider, MockFsState, make_inode};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn create_method_exists_on_provider() {
    let parent = make_inode(1, INodeType::Directory, 0, None);

    let state = MockFsState {
        next_addr: std::sync::atomic::AtomicU64::new(100),
        ..MockFsState::default()
    };
    let provider = MockFsDataProvider::new(state);

    let result = provider.create(parent, "newfile.txt".as_ref(), 0o644).await;
    assert!(result.is_ok());
    let inode = result.unwrap();
    assert_eq!(inode.addr, 100);
    assert_eq!(inode.itype, INodeType::File);
    assert_eq!(inode.size, 0);
    assert_eq!(inode.parent, Some(1));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn write_method_exists_on_provider() {
    let _root = make_inode(1, INodeType::Directory, 0, None);
    let file = make_inode(2, INodeType::File, 0, Some(1));

    let state = MockFsState {
        file_contents: [(2, Bytes::from_static(b""))].into_iter().collect(),
        ..MockFsState::default()
    };
    let provider = MockFsDataProvider::new(state);

    let result = provider.write(file, 0, Bytes::from_static(b"hello")).await;
    assert!(result.is_ok());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn async_fs_write_stores_data_in_overlay() {
    let root = make_inode(1, INodeType::Directory, 0, None);
    let file = make_inode(2, INodeType::File, 0, Some(1));

    let state = MockFsState {
        lookups: [((1, "test.txt".into()), file)].into_iter().collect(),
        directories: [(1, vec![("test.txt".into(), file)])].into_iter().collect(),
        file_contents: [(2, Bytes::from_static(b"original"))].into_iter().collect(),
        ..MockFsState::default()
    };
    let provider = MockFsDataProvider::new(state);
    let table = Arc::new(FutureBackedCache::default());
    let fs = AsyncFs::new(provider, root, Arc::clone(&table)).await;

    // Load the file inode into the table via lookup.
    let _ = fs
        .lookup(LoadedAddr::new_unchecked(1), "test.txt".as_ref())
        .await
        .unwrap();

    // Write to the overlay
    let written = fs
        .write(
            LoadedAddr::new_unchecked(2),
            0,
            Bytes::from_static(b"new content"),
        )
        .await
        .unwrap();
    assert_eq!(written, 11);

    // Read back — should return the overlay data, not "original"
    let open = fs
        .open(LoadedAddr::new_unchecked(2), OpenFlags::RDONLY)
        .await
        .unwrap();
    let data = open.read(0, 1024).await.unwrap();
    assert_eq!(&data[..], b"new content");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn write_at_offset_merges_correctly() {
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
    let fs = AsyncFs::new(provider, root, Arc::clone(&table)).await;

    let _ = fs
        .lookup(LoadedAddr::new_unchecked(1), "test.txt".as_ref())
        .await
        .unwrap();

    // Write "hello" at offset 0
    fs.write(
        LoadedAddr::new_unchecked(2),
        0,
        Bytes::from_static(b"hello"),
    )
    .await
    .unwrap();

    // Write " world" at offset 5
    fs.write(
        LoadedAddr::new_unchecked(2),
        5,
        Bytes::from_static(b" world"),
    )
    .await
    .unwrap();

    // Read back
    let open = fs
        .open(LoadedAddr::new_unchecked(2), OpenFlags::RDONLY)
        .await
        .unwrap();
    let data = open.read(0, 1024).await.unwrap();
    assert_eq!(&data[..], b"hello world");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn write_updates_inode_size() {
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
    let fs = AsyncFs::new(provider, root, Arc::clone(&table)).await;

    let _ = fs
        .lookup(LoadedAddr::new_unchecked(1), "test.txt".as_ref())
        .await
        .unwrap();

    fs.write(
        LoadedAddr::new_unchecked(2),
        0,
        Bytes::from_static(b"12345"),
    )
    .await
    .unwrap();

    let inode = fs.getattr(LoadedAddr::new_unchecked(2)).await.unwrap();
    assert_eq!(inode.size, 5);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn write_to_directory_returns_eisdir() {
    let root = make_inode(1, INodeType::Directory, 0, None);

    let state = MockFsState::default();
    let provider = MockFsDataProvider::new(state);
    let table = Arc::new(FutureBackedCache::default());
    let fs = AsyncFs::new(provider, root, Arc::clone(&table)).await;

    let result = fs
        .write(LoadedAddr::new_unchecked(1), 0, Bytes::from_static(b"data"))
        .await;
    assert!(result.is_err());
    assert_eq!(result.unwrap_err().raw_os_error(), Some(libc::EISDIR));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn written_inode_survives_forget() {
    let root = make_inode(1, INodeType::Directory, 0, None);
    let file = make_inode(2, INodeType::File, 10, Some(1));

    let state = MockFsState {
        lookups: [((1, "test.txt".into()), file)].into_iter().collect(),
        directories: [(1, vec![("test.txt".into(), file)])].into_iter().collect(),
        file_contents: [(2, Bytes::from_static(b"original"))].into_iter().collect(),
        ..MockFsState::default()
    };
    let provider = MockFsDataProvider::new(state);
    let table = Arc::new(FutureBackedCache::default());
    let fs = AsyncFs::new(provider.clone(), root, Arc::clone(&table)).await;

    // Look up the file to load it into the table.
    let _ = fs
        .lookup(LoadedAddr::new_unchecked(1), "test.txt".as_ref())
        .await
        .unwrap();

    // Write to the file — this puts it in the overlay.
    fs.write(
        LoadedAddr::new_unchecked(2),
        0,
        Bytes::from_static(b"written"),
    )
    .await
    .unwrap();

    // Simulate forget by calling InodeForget::delete directly.
    let ctx = ForgetContext {
        inode_table: Arc::clone(&table),
        dcache: fs.directory_cache(),
        lookup_cache: fs.lookup_cache(),
        provider: provider.clone(),
        write_overlay: fs.write_overlay(),
    };
    InodeForget::delete(&ctx, &2);

    // The inode should still be in the table because it was written to.
    assert!(
        table.get(&2).await.is_some(),
        "written inode must survive forget"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unwritten_inode_is_evicted_on_forget() {
    let root = make_inode(1, INodeType::Directory, 0, None);
    let file = make_inode(2, INodeType::File, 10, Some(1));

    let state = MockFsState {
        lookups: [((1, "test.txt".into()), file)].into_iter().collect(),
        directories: [(1, vec![("test.txt".into(), file)])].into_iter().collect(),
        file_contents: [(2, Bytes::from_static(b"original"))].into_iter().collect(),
        ..MockFsState::default()
    };
    let provider = MockFsDataProvider::new(state);
    let table = Arc::new(FutureBackedCache::default());
    let fs = AsyncFs::new(provider.clone(), root, Arc::clone(&table)).await;

    // Look up the file to load it into the table.
    let _ = fs
        .lookup(LoadedAddr::new_unchecked(1), "test.txt".as_ref())
        .await
        .unwrap();
    assert!(table.get(&2).await.is_some());

    // Forget without writing — should be evicted.
    let ctx = ForgetContext {
        inode_table: Arc::clone(&table),
        dcache: fs.directory_cache(),
        lookup_cache: fs.lookup_cache(),
        provider: provider.clone(),
        write_overlay: fs.write_overlay(),
    };
    InodeForget::delete(&ctx, &2);

    assert!(
        table.get(&2).await.is_none(),
        "unwritten inode should be evicted"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn write_then_read_round_trip() {
    let root = make_inode(1, INodeType::Directory, 0, None);
    let file = make_inode(2, INodeType::File, 0, Some(1));

    let state = MockFsState {
        lookups: [((1, "data.bin".into()), file)].into_iter().collect(),
        directories: [(1, vec![("data.bin".into(), file)])].into_iter().collect(),
        file_contents: [(2, Bytes::new())].into_iter().collect(),
        ..MockFsState::default()
    };
    let provider = MockFsDataProvider::new(state);
    let table = Arc::new(FutureBackedCache::default());
    let fs = AsyncFs::new(provider, root, Arc::clone(&table)).await;

    let _ = fs
        .lookup(LoadedAddr::new_unchecked(1), "data.bin".as_ref())
        .await
        .unwrap();

    // Write 3 chunks at different offsets.
    fs.write(LoadedAddr::new_unchecked(2), 0, Bytes::from_static(b"AAA"))
        .await
        .unwrap();
    fs.write(LoadedAddr::new_unchecked(2), 3, Bytes::from_static(b"BBB"))
        .await
        .unwrap();
    fs.write(LoadedAddr::new_unchecked(2), 6, Bytes::from_static(b"CCC"))
        .await
        .unwrap();

    // Verify full content.
    let open = fs
        .open(LoadedAddr::new_unchecked(2), OpenFlags::RDONLY)
        .await
        .unwrap();
    let data = open.read(0, 1024).await.unwrap();
    assert_eq!(&data[..], b"AAABBBCCC");

    // Verify inode size.
    let inode = fs.getattr(LoadedAddr::new_unchecked(2)).await.unwrap();
    assert_eq!(inode.size, 9);

    // Verify partial read.
    let partial = open.read(3, 3).await.unwrap();
    assert_eq!(&partial[..], b"BBB");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn write_with_gap_fills_zeros() {
    let root = make_inode(1, INodeType::Directory, 0, None);
    let file = make_inode(2, INodeType::File, 0, Some(1));

    let state = MockFsState {
        lookups: [((1, "gap.bin".into()), file)].into_iter().collect(),
        directories: [(1, vec![("gap.bin".into(), file)])].into_iter().collect(),
        file_contents: [(2, Bytes::new())].into_iter().collect(),
        ..MockFsState::default()
    };
    let provider = MockFsDataProvider::new(state);
    let table = Arc::new(FutureBackedCache::default());
    let fs = AsyncFs::new(provider, root, Arc::clone(&table)).await;

    let _ = fs
        .lookup(LoadedAddr::new_unchecked(1), "gap.bin".as_ref())
        .await
        .unwrap();

    // Write at offset 5 with no prior data — should zero-fill [0..5).
    fs.write(LoadedAddr::new_unchecked(2), 5, Bytes::from_static(b"XY"))
        .await
        .unwrap();

    let open = fs
        .open(LoadedAddr::new_unchecked(2), OpenFlags::RDONLY)
        .await
        .unwrap();
    let data = open.read(0, 1024).await.unwrap();
    assert_eq!(&data[..], b"\0\0\0\0\0XY");
    assert_eq!(data.len(), 7);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn create_file_returns_new_inode() {
    let root = make_inode(1, INodeType::Directory, 0, None);

    let state = MockFsState {
        directories: [(1, vec![])].into_iter().collect(),
        next_addr: std::sync::atomic::AtomicU64::new(100),
        ..MockFsState::default()
    };
    let provider = MockFsDataProvider::new(state);
    let table = Arc::new(FutureBackedCache::default());
    let fs = AsyncFs::new(provider, root, Arc::clone(&table)).await;

    let (inode, open_file) = fs
        .create(LoadedAddr::new_unchecked(1), "newfile.txt".as_ref(), 0o644)
        .await
        .unwrap();

    assert_eq!(inode.itype, INodeType::File);
    assert_eq!(inode.size, 0);
    assert!(open_file.fh > 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn create_file_appears_in_readdir() {
    let root = make_inode(1, INodeType::Directory, 0, None);

    let state = MockFsState {
        directories: [(1, vec![])].into_iter().collect(),
        next_addr: std::sync::atomic::AtomicU64::new(100),
        ..MockFsState::default()
    };
    let provider = MockFsDataProvider::new(state);
    let table = Arc::new(FutureBackedCache::default());
    let fs = AsyncFs::new(provider, root, Arc::clone(&table)).await;

    // Populate the directory cache first so readdir sees the empty state.
    let mut entries_before = vec![];
    fs.readdir(LoadedAddr::new_unchecked(1), 0, |entry, _offset| {
        entries_before.push(entry.name.to_os_string());
        false
    })
    .await
    .unwrap();
    assert!(entries_before.is_empty());

    // Create a file.
    let _result = fs
        .create(LoadedAddr::new_unchecked(1), "created.txt".as_ref(), 0o644)
        .await
        .unwrap();

    // readdir should now include the new file.
    let mut entries_after = vec![];
    fs.readdir(LoadedAddr::new_unchecked(1), 0, |entry, _offset| {
        entries_after.push(entry.name.to_os_string());
        false
    })
    .await
    .unwrap();
    assert!(
        entries_after.contains(&std::ffi::OsString::from("created.txt")),
        "readdir should include the newly created file"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn create_file_then_write_and_read() {
    let root = make_inode(1, INodeType::Directory, 0, None);

    let state = MockFsState {
        directories: [(1, vec![])].into_iter().collect(),
        next_addr: std::sync::atomic::AtomicU64::new(100),
        ..MockFsState::default()
    };
    let provider = MockFsDataProvider::new(state);
    let table = Arc::new(FutureBackedCache::default());
    let fs = AsyncFs::new(provider, root, Arc::clone(&table)).await;

    let (child, open_file) = fs
        .create(LoadedAddr::new_unchecked(1), "data.txt".as_ref(), 0o644)
        .await
        .unwrap();

    // Write data through the filesystem layer.
    let child_addr = LoadedAddr::new_unchecked(child.addr);
    fs.write(child_addr, 0, Bytes::from_static(b"hello create"))
        .await
        .unwrap();

    // Read back through the open file handle.
    let data = open_file.read(0, 1024).await.unwrap();
    assert_eq!(&data[..], b"hello create");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn create_in_non_directory_returns_enotdir() {
    let root = make_inode(1, INodeType::Directory, 0, None);
    let file = make_inode(2, INodeType::File, 10, Some(1));

    let state = MockFsState {
        lookups: [((1, "file.txt".into()), file)].into_iter().collect(),
        directories: [(1, vec![("file.txt".into(), file)])].into_iter().collect(),
        file_contents: [(2, Bytes::from_static(b"content"))].into_iter().collect(),
        next_addr: std::sync::atomic::AtomicU64::new(100),
        ..MockFsState::default()
    };
    let provider = MockFsDataProvider::new(state);
    let table = Arc::new(FutureBackedCache::default());
    let fs = AsyncFs::new(provider, root, Arc::clone(&table)).await;

    // Load the file inode.
    let _ = fs
        .lookup(LoadedAddr::new_unchecked(1), "file.txt".as_ref())
        .await
        .unwrap();

    // Try to create inside a file — should fail with ENOTDIR.
    let result = fs
        .create(
            LoadedAddr::new_unchecked(2),
            "impossible.txt".as_ref(),
            0o644,
        )
        .await;
    assert!(result.is_err());
    assert_eq!(result.unwrap_err().raw_os_error(), Some(libc::ENOTDIR));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn setattr_updates_size_truncate() {
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
    let fs = AsyncFs::new(provider, root, Arc::clone(&table)).await;

    // Lookup the file to load it into the inode table.
    let _ = fs
        .lookup(LoadedAddr::new_unchecked(1), "test.txt".as_ref())
        .await
        .unwrap();

    // Write "hello world" to the file.
    fs.write(
        LoadedAddr::new_unchecked(2),
        0,
        Bytes::from_static(b"hello world"),
    )
    .await
    .unwrap();

    // Truncate to 5 bytes via setattr.
    let inode = fs
        .setattr(LoadedAddr::new_unchecked(2), Some(5), None, None)
        .await
        .unwrap();
    assert_eq!(inode.size, 5);

    // Read back — should return "hello".
    let open = fs
        .open(LoadedAddr::new_unchecked(2), OpenFlags::RDONLY)
        .await
        .unwrap();
    let data = open.read(0, 1024).await.unwrap();
    assert_eq!(&data[..], b"hello");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn setattr_truncate_to_zero() {
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
    let fs = AsyncFs::new(provider, root, Arc::clone(&table)).await;

    // Lookup the file to load it into the inode table.
    let _ = fs
        .lookup(LoadedAddr::new_unchecked(1), "test.txt".as_ref())
        .await
        .unwrap();

    // Write "content" to the file.
    fs.write(
        LoadedAddr::new_unchecked(2),
        0,
        Bytes::from_static(b"content"),
    )
    .await
    .unwrap();

    // Truncate to 0 bytes via setattr.
    let inode = fs
        .setattr(LoadedAddr::new_unchecked(2), Some(0), None, None)
        .await
        .unwrap();
    assert_eq!(inode.size, 0);

    // Read back — should return empty.
    let open = fs
        .open(LoadedAddr::new_unchecked(2), OpenFlags::RDONLY)
        .await
        .unwrap();
    let data = open.read(0, 1024).await.unwrap();
    assert!(data.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn evict_skips_written_inode() {
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
    let fs = AsyncFs::new(provider, root, Arc::clone(&table)).await;

    let _ = fs
        .lookup(LoadedAddr::new_unchecked(1), "test.txt".as_ref())
        .await
        .unwrap();

    fs.write(
        LoadedAddr::new_unchecked(2),
        0,
        Bytes::from_static(b"written data"),
    )
    .await
    .unwrap();

    // Evict the inode (simulates CompositeFs::forget path).
    fs.evict(2);

    assert!(
        table.get(&2).await.is_some(),
        "written inode must survive evict"
    );

    let inode = fs.getattr(LoadedAddr::new_unchecked(2)).await.unwrap();
    assert_eq!(inode.size, 12);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn setattr_honors_explicit_mtime() {
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
    let fs = AsyncFs::new(provider, root, Arc::clone(&table)).await;

    let _ = fs
        .lookup(LoadedAddr::new_unchecked(1), "test.txt".as_ref())
        .await
        .unwrap();

    let epoch_plus_1000 = std::time::UNIX_EPOCH + std::time::Duration::from_secs(1000);
    let inode = fs
        .setattr(
            LoadedAddr::new_unchecked(2),
            None,
            None,
            Some(epoch_plus_1000),
        )
        .await
        .unwrap();

    assert_eq!(
        inode.last_modified_at, epoch_plus_1000,
        "setattr must honor the explicit mtime, not overwrite with now()"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn setattr_uses_atime_as_mtime_fallback() {
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
    let fs = AsyncFs::new(provider, root, Arc::clone(&table)).await;

    let _ = fs
        .lookup(LoadedAddr::new_unchecked(1), "test.txt".as_ref())
        .await
        .unwrap();

    let epoch_plus_2000 = std::time::UNIX_EPOCH + std::time::Duration::from_secs(2000);
    let inode = fs
        .setattr(
            LoadedAddr::new_unchecked(2),
            None,
            Some(epoch_plus_2000),
            None,
        )
        .await
        .unwrap();

    assert_eq!(
        inode.last_modified_at, epoch_plus_2000,
        "setattr must use atime as mtime when mtime is not provided"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn evict_removes_unwritten_inode() {
    let root = make_inode(1, INodeType::Directory, 0, None);
    let file = make_inode(2, INodeType::File, 10, Some(1));

    let state = MockFsState {
        lookups: [((1, "test.txt".into()), file)].into_iter().collect(),
        directories: [(1, vec![("test.txt".into(), file)])].into_iter().collect(),
        file_contents: [(2, Bytes::from_static(b"original"))].into_iter().collect(),
        ..MockFsState::default()
    };
    let provider = MockFsDataProvider::new(state);
    let table = Arc::new(FutureBackedCache::default());
    let fs = AsyncFs::new(provider, root, Arc::clone(&table)).await;

    let _ = fs
        .lookup(LoadedAddr::new_unchecked(1), "test.txt".as_ref())
        .await
        .unwrap();
    assert!(table.get(&2).await.is_some());

    fs.evict(2);

    assert!(
        table.get(&2).await.is_none(),
        "unwritten inode should be evicted"
    );
}

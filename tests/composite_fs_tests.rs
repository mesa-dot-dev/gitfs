#![allow(clippy::unwrap_used, clippy::expect_used, missing_docs)]

mod common;

use std::collections::HashMap;
use std::ffi::{OsStr, OsString};
use std::sync::Arc;

use bytes::Bytes;

use git_fs::cache::async_backed::FutureBackedCache;
use git_fs::fs::async_fs::{AsyncFs, FsDataProvider as _};
use git_fs::fs::composite::CompositeFs;
use git_fs::fs::{INode, INodeType, LoadedAddr, OpenFlags};

use common::async_fs_mocks::{MockFsDataProvider, MockFsState, make_inode};
use common::composite_mocks::MockRoot;

/// Build a child data provider with a root directory and a set of children.
///
/// Each child is `(name, addr, itype, size)`. Files get auto-generated content
/// of the form `"content of {name}"`.
fn make_child_provider(
    root_addr: u64,
    children: &[(&str, u64, INodeType, u64)],
) -> (MockFsDataProvider, INode) {
    let root = make_inode(root_addr, INodeType::Directory, 0, None);
    let mut state = MockFsState::default();
    let mut dir_entries = Vec::new();
    for (name, addr, itype, size) in children {
        let child = make_inode(*addr, *itype, *size, Some(root_addr));
        state
            .lookups
            .insert((root_addr, OsString::from(name)), child);
        dir_entries.push((OsString::from(name), child));
        if *itype == INodeType::File {
            state
                .file_contents
                .insert(*addr, Bytes::from(format!("content of {name}")));
        }
    }
    state.directories.insert(root_addr, dir_entries);
    (MockFsDataProvider::new(state), root)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn composite_root_lookup_resolves_child() {
    let (provider, root_ino) = make_child_provider(100, &[("file.txt", 101, INodeType::File, 42)]);

    let mut children = HashMap::new();
    children.insert(OsString::from("repo-a"), (provider, root_ino));

    let mock_root = MockRoot::new(children);
    let composite = CompositeFs::new(mock_root, (1000, 1000));
    let root_inode = composite.make_root_inode();

    let table = Arc::new(FutureBackedCache::default());
    table.insert_sync(1, root_inode);
    let afs = AsyncFs::new_preseeded(composite, Arc::clone(&table));

    let tracked = afs
        .lookup(LoadedAddr::new_unchecked(1), OsStr::new("repo-a"))
        .await
        .unwrap();

    assert_eq!(
        tracked.inode.itype,
        INodeType::Directory,
        "child should appear as a directory at composite level"
    );
    assert_ne!(
        tracked.inode.addr, 1,
        "child should have a composite-level address different from root"
    );
    assert_eq!(
        tracked.inode.parent,
        Some(1),
        "child directory should have the composite root as parent"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn composite_root_readdir_lists_children() {
    let (prov_a, root_a) = make_child_provider(100, &[]);
    let (prov_b, root_b) = make_child_provider(200, &[]);

    let mut children = HashMap::new();
    children.insert(OsString::from("alpha"), (prov_a, root_a));
    children.insert(OsString::from("beta"), (prov_b, root_b));

    let mock_root = MockRoot::new(children);
    let composite = CompositeFs::new(mock_root, (1000, 1000));
    let root_inode = composite.make_root_inode();

    let table = Arc::new(FutureBackedCache::default());
    table.insert_sync(1, root_inode);
    let afs = AsyncFs::new_preseeded(composite, Arc::clone(&table));

    let mut entries = Vec::new();
    afs.readdir(LoadedAddr::new_unchecked(1), 0, |de, _offset| {
        entries.push(de.name.to_os_string());
        false
    })
    .await
    .unwrap();

    entries.sort();
    assert_eq!(entries.len(), 2, "should list both children");
    assert_eq!(entries[0], "alpha");
    assert_eq!(entries[1], "beta");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn composite_delegated_lookup_reaches_child() {
    let (provider, root_ino) = make_child_provider(
        100,
        &[
            ("readme.md", 101, INodeType::File, 256),
            ("src", 102, INodeType::Directory, 0),
        ],
    );

    let mut children = HashMap::new();
    children.insert(OsString::from("my-repo"), (provider, root_ino));

    let mock_root = MockRoot::new(children);
    let composite = CompositeFs::new(mock_root, (1000, 1000));
    let root_inode = composite.make_root_inode();

    let table = Arc::new(FutureBackedCache::default());
    table.insert_sync(1, root_inode);
    let afs = AsyncFs::new_preseeded(composite, Arc::clone(&table));

    // First, lookup the child at root level.
    let child_dir = afs
        .lookup(LoadedAddr::new_unchecked(1), OsStr::new("my-repo"))
        .await
        .unwrap();
    let child_addr = child_dir.inode.addr;

    // Then, lookup a file inside the child.
    let file = afs
        .lookup(
            LoadedAddr::new_unchecked(child_addr),
            OsStr::new("readme.md"),
        )
        .await
        .unwrap();

    assert_eq!(file.inode.itype, INodeType::File);
    assert_eq!(file.inode.size, 256);

    // Also lookup a subdirectory inside the child.
    let subdir = afs
        .lookup(LoadedAddr::new_unchecked(child_addr), OsStr::new("src"))
        .await
        .unwrap();

    assert_eq!(subdir.inode.itype, INodeType::Directory);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn composite_open_and_read_through_child() {
    let (provider, root_ino) = make_child_provider(100, &[("hello.txt", 101, INodeType::File, 20)]);

    let mut children = HashMap::new();
    children.insert(OsString::from("repo"), (provider, root_ino));

    let mock_root = MockRoot::new(children);
    let composite = CompositeFs::new(mock_root, (1000, 1000));
    let root_inode = composite.make_root_inode();

    let table = Arc::new(FutureBackedCache::default());
    table.insert_sync(1, root_inode);
    let afs = AsyncFs::new_preseeded(composite, Arc::clone(&table));

    // Navigate to the file.
    let child_dir = afs
        .lookup(LoadedAddr::new_unchecked(1), OsStr::new("repo"))
        .await
        .unwrap();
    let file_tracked = afs
        .lookup(
            LoadedAddr::new_unchecked(child_dir.inode.addr),
            OsStr::new("hello.txt"),
        )
        .await
        .unwrap();
    let file_addr = file_tracked.inode.addr;

    // Open and read.
    let open_file = afs
        .open(LoadedAddr::new_unchecked(file_addr), OpenFlags::empty())
        .await
        .unwrap();
    let data = open_file.read(0, 1024).await.unwrap();

    assert_eq!(
        data,
        Bytes::from("content of hello.txt"),
        "should read the file content through the composite layer"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn composite_lookup_unknown_child_returns_enoent() {
    let (provider, root_ino) = make_child_provider(100, &[]);

    let mut children = HashMap::new();
    children.insert(OsString::from("existing"), (provider, root_ino));

    let mock_root = MockRoot::new(children);
    let composite = CompositeFs::new(mock_root, (1000, 1000));
    let root_inode = composite.make_root_inode();

    let table = Arc::new(FutureBackedCache::default());
    table.insert_sync(1, root_inode);
    let afs = AsyncFs::new_preseeded(composite, Arc::clone(&table));

    let err = afs
        .lookup(LoadedAddr::new_unchecked(1), OsStr::new("nonexistent"))
        .await
        .unwrap_err();

    assert_eq!(
        err.raw_os_error(),
        Some(libc::ENOENT),
        "looking up a nonexistent child at root should return ENOENT"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn composite_readdir_delegated_lists_child_contents() {
    let (provider, root_ino) = make_child_provider(
        100,
        &[
            ("a.rs", 101, INodeType::File, 10),
            ("b.rs", 102, INodeType::File, 20),
            ("lib", 103, INodeType::Directory, 0),
        ],
    );

    let mut children = HashMap::new();
    children.insert(OsString::from("repo"), (provider, root_ino));

    let mock_root = MockRoot::new(children);
    let composite = CompositeFs::new(mock_root, (1000, 1000));
    let root_inode = composite.make_root_inode();

    let table = Arc::new(FutureBackedCache::default());
    table.insert_sync(1, root_inode);
    let afs = AsyncFs::new_preseeded(composite, Arc::clone(&table));

    // Navigate into the child.
    let child_dir = afs
        .lookup(LoadedAddr::new_unchecked(1), OsStr::new("repo"))
        .await
        .unwrap();

    // Readdir inside the child.
    let mut entries = Vec::new();
    afs.readdir(
        LoadedAddr::new_unchecked(child_dir.inode.addr),
        0,
        |de, _offset| {
            entries.push((de.name.to_os_string(), de.inode.itype));
            false
        },
    )
    .await
    .unwrap();

    entries.sort_by(|(a, _), (b, _)| a.cmp(b));
    assert_eq!(entries.len(), 3);
    assert_eq!(entries[0], (OsString::from("a.rs"), INodeType::File));
    assert_eq!(entries[1], (OsString::from("b.rs"), INodeType::File));
    assert_eq!(entries[2], (OsString::from("lib"), INodeType::Directory));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn composite_repeated_lookup_returns_same_addr() {
    let (provider, root_ino) = make_child_provider(100, &[]);

    let mut children = HashMap::new();
    children.insert(OsString::from("repo"), (provider, root_ino));

    let mock_root = MockRoot::new(children);
    let composite = CompositeFs::new(mock_root, (1000, 1000));
    let root_inode = composite.make_root_inode();

    let table = Arc::new(FutureBackedCache::default());
    table.insert_sync(1, root_inode);
    let afs = AsyncFs::new_preseeded(composite, Arc::clone(&table));

    let first = afs
        .lookup(LoadedAddr::new_unchecked(1), OsStr::new("repo"))
        .await
        .unwrap();
    let second = afs
        .lookup(LoadedAddr::new_unchecked(1), OsStr::new("repo"))
        .await
        .unwrap();

    assert_eq!(
        first.inode.addr, second.inode.addr,
        "repeated lookups for the same child should return the same composite address"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn composite_forget_propagates_to_child_inode_table() {
    let (provider, root_ino) = make_child_provider(100, &[("file.txt", 101, INodeType::File, 42)]);
    let mock_state = Arc::clone(&provider.state);

    let mut children = HashMap::new();
    children.insert(OsString::from("repo"), (provider, root_ino));

    let mock_root = MockRoot::new(children);
    let composite = CompositeFs::new(mock_root, (1000, 1000));
    let root_inode = composite.make_root_inode();

    let table = Arc::new(FutureBackedCache::default());
    table.insert_sync(1, root_inode);
    let afs = AsyncFs::new_preseeded(composite.clone(), Arc::clone(&table));

    // Navigate to the file.
    let child_dir = afs
        .lookup(LoadedAddr::new_unchecked(1), OsStr::new("repo"))
        .await
        .unwrap();
    let child_addr = child_dir.inode.addr;

    let file = afs
        .lookup(
            LoadedAddr::new_unchecked(child_addr),
            OsStr::new("file.txt"),
        )
        .await
        .unwrap();
    let file_addr = file.inode.addr;

    // Forget the file — this should propagate to the child.
    composite.forget(file_addr);

    // The child's data provider should have received the forget call
    // for the inner address (101).
    assert!(
        mock_state.forgotten_addrs.contains_sync(&101),
        "forget should propagate to child data provider"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn composite_forget_cleans_up_slot_and_name_mapping() {
    // Setup: one child "repo" with a file.
    let (provider, root_ino) = make_child_provider(100, &[("file.txt", 101, INodeType::File, 42)]);

    let mut children = HashMap::new();
    children.insert(OsString::from("repo"), (provider, root_ino));

    let mock_root = MockRoot::new(children);
    let composite = CompositeFs::new(mock_root, (1000, 1000));
    let root_inode = composite.make_root_inode();

    let table = Arc::new(FutureBackedCache::default());
    table.insert_sync(1, root_inode);
    let afs = AsyncFs::new_preseeded(composite.clone(), Arc::clone(&table));

    // Look up the child and a file inside it.
    let child_dir = afs
        .lookup(LoadedAddr::new_unchecked(1), OsStr::new("repo"))
        .await
        .unwrap();
    let child_addr = child_dir.inode.addr;

    let file = afs
        .lookup(
            LoadedAddr::new_unchecked(child_addr),
            OsStr::new("file.txt"),
        )
        .await
        .unwrap();
    let file_addr = file.inode.addr;

    // Forget the file, then the child directory.
    composite.forget(file_addr);
    composite.forget(child_addr);

    // Re-lookup the child — should succeed with a fresh slot.
    let re_resolved = afs
        .lookup(LoadedAddr::new_unchecked(1), OsStr::new("repo"))
        .await
        .unwrap();

    assert_eq!(re_resolved.inode.itype, INodeType::Directory);
    // The new address may differ from the original (fresh slot allocated).
}

#![allow(clippy::unwrap_used, missing_docs)]

use std::ffi::{OsStr, OsString};

use git_fs::fs::LoadedAddr;
use git_fs::fs::dcache::{DCache, PopulateStatus};

#[tokio::test]
async fn lookup_returns_none_for_missing_entry() {
    let cache = DCache::new();
    assert!(
        cache
            .lookup(LoadedAddr::new_unchecked(1), OsStr::new("foo"))
            .is_none()
    );
}

#[tokio::test]
async fn insert_then_lookup() {
    let cache = DCache::new();
    cache.insert(
        LoadedAddr::new_unchecked(1),
        OsString::from("foo"),
        LoadedAddr::new_unchecked(10),
        false,
    );
    let dv = cache.lookup(LoadedAddr::new_unchecked(1), OsStr::new("foo"));
    assert!(dv.is_some(), "entry should be present after insert");
    let dv = dv.expect("checked above");
    assert_eq!(dv.ino, LoadedAddr::new_unchecked(10));
    assert!(!dv.is_dir);
}

#[tokio::test]
async fn readdir_returns_only_children_of_parent() {
    let cache = DCache::new();
    cache.insert(
        LoadedAddr::new_unchecked(1),
        OsString::from("a"),
        LoadedAddr::new_unchecked(10),
        false,
    );
    cache.insert(
        LoadedAddr::new_unchecked(1),
        OsString::from("b"),
        LoadedAddr::new_unchecked(11),
        true,
    );
    cache.insert(
        LoadedAddr::new_unchecked(2),
        OsString::from("c"),
        LoadedAddr::new_unchecked(12),
        false,
    );
    let mut children = Vec::new();
    cache.readdir(LoadedAddr::new_unchecked(1), |name, dvalue| {
        children.push((name.to_os_string(), dvalue.clone()));
    });
    assert_eq!(children.len(), 2);
    let names: Vec<_> = children.iter().map(|(n, _)| n.clone()).collect();
    assert!(names.contains(&OsString::from("a")));
    assert!(names.contains(&OsString::from("b")));
}

#[tokio::test]
async fn readdir_empty_parent_returns_empty() {
    let cache = DCache::new();
    let mut children = Vec::new();
    cache.readdir(LoadedAddr::new_unchecked(1), |name, dvalue| {
        children.push((name.to_os_string(), dvalue.clone()));
    });
    assert!(children.is_empty());
}

#[tokio::test]
async fn try_claim_populate_unclaimed_returns_claimed() {
    let cache = DCache::new();
    assert!(matches!(
        cache.try_claim_populate(LoadedAddr::new_unchecked(1)),
        PopulateStatus::Claimed
    ));
}

#[tokio::test]
async fn finish_populate_then_claim_returns_done() {
    let cache = DCache::new();
    assert!(matches!(
        cache.try_claim_populate(LoadedAddr::new_unchecked(1)),
        PopulateStatus::Claimed
    ));
    cache.finish_populate(LoadedAddr::new_unchecked(1));
    assert!(matches!(
        cache.try_claim_populate(LoadedAddr::new_unchecked(1)),
        PopulateStatus::Done
    ));
}

#[tokio::test]
async fn double_claim_returns_in_progress() {
    let cache = DCache::new();
    assert!(matches!(
        cache.try_claim_populate(LoadedAddr::new_unchecked(1)),
        PopulateStatus::Claimed
    ));
    assert!(matches!(
        cache.try_claim_populate(LoadedAddr::new_unchecked(1)),
        PopulateStatus::InProgress
    ));
}

#[tokio::test]
async fn abort_populate_allows_reclaim() {
    let cache = DCache::new();
    assert!(matches!(
        cache.try_claim_populate(LoadedAddr::new_unchecked(1)),
        PopulateStatus::Claimed
    ));
    cache.abort_populate(LoadedAddr::new_unchecked(1));
    assert!(matches!(
        cache.try_claim_populate(LoadedAddr::new_unchecked(1)),
        PopulateStatus::Claimed
    ));
}

#[tokio::test]
async fn insert_does_not_mark_populated() {
    let cache = DCache::new();
    cache.insert(
        LoadedAddr::new_unchecked(1),
        OsString::from("foo"),
        LoadedAddr::new_unchecked(10),
        false,
    );
    assert!(
        matches!(
            cache.try_claim_populate(LoadedAddr::new_unchecked(1)),
            PopulateStatus::Claimed
        ),
        "insert alone should not mark a directory as populated"
    );
}

#[tokio::test]
async fn upsert_overwrites_existing_entry() {
    let cache = DCache::new();
    cache.insert(
        LoadedAddr::new_unchecked(1),
        OsString::from("foo"),
        LoadedAddr::new_unchecked(10),
        false,
    );
    cache.insert(
        LoadedAddr::new_unchecked(1),
        OsString::from("foo"),
        LoadedAddr::new_unchecked(20),
        true,
    );
    let dv = cache.lookup(LoadedAddr::new_unchecked(1), OsStr::new("foo"));
    assert!(dv.is_some(), "entry should still be present after upsert");
    let dv = dv.expect("checked above");
    assert_eq!(dv.ino, LoadedAddr::new_unchecked(20));
    assert!(dv.is_dir);
}

#[tokio::test]
async fn readdir_returns_entries_in_sorted_order() {
    let cache = DCache::new();
    for name in ["zebra", "apple", "mango"] {
        cache.insert(
            LoadedAddr::new_unchecked(1),
            OsString::from(name),
            LoadedAddr::new_unchecked(10),
            false,
        );
    }
    let mut names = Vec::new();
    cache.readdir(LoadedAddr::new_unchecked(1), |name, _| {
        names.push(name.to_str().unwrap().to_owned());
    });
    assert_eq!(names, ["apple", "mango", "zebra"]);
}

#[tokio::test]
async fn child_dir_addrs_returns_only_directories() {
    let cache = DCache::new();
    let parent = LoadedAddr::new_unchecked(1);
    cache.insert(
        parent,
        OsString::from("file.txt"),
        LoadedAddr::new_unchecked(10),
        false,
    );
    cache.insert(
        parent,
        OsString::from("subdir"),
        LoadedAddr::new_unchecked(11),
        true,
    );
    cache.insert(
        parent,
        OsString::from("another_file"),
        LoadedAddr::new_unchecked(12),
        false,
    );
    cache.insert(
        parent,
        OsString::from("another_dir"),
        LoadedAddr::new_unchecked(13),
        true,
    );

    let dirs = cache.child_dir_addrs(parent);
    assert_eq!(dirs.len(), 2);
    assert!(dirs.contains(&LoadedAddr::new_unchecked(11)));
    assert!(dirs.contains(&LoadedAddr::new_unchecked(13)));
}

#[tokio::test]
async fn child_dir_addrs_returns_empty_for_unknown_parent() {
    let cache = DCache::new();
    let dirs = cache.child_dir_addrs(LoadedAddr::new_unchecked(999));
    assert!(dirs.is_empty());
}

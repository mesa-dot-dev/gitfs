#![allow(clippy::unwrap_used, clippy::expect_used, missing_docs)]

use std::path::PathBuf;

use git_fs::fs::LoadedAddr;
use git_fs::fs::dcache::DCache;

fn addr(n: u64) -> LoadedAddr {
    LoadedAddr::new_unchecked(n)
}

#[test]
fn root_gitignore_ignores_matching_child() {
    let cache = DCache::new(PathBuf::from("/"));
    cache.insert(addr(1), "debug.log".into(), addr(2), false);
    cache.set_ignore_rules(addr(1), ".gitignore", "*.log");
    assert!(cache.is_ignored(addr(2)));
}

#[test]
fn root_gitignore_does_not_ignore_non_matching_child() {
    let cache = DCache::new(PathBuf::from("/"));
    cache.insert(addr(1), "readme.md".into(), addr(2), false);
    cache.set_ignore_rules(addr(1), ".gitignore", "*.log");
    assert!(!cache.is_ignored(addr(2)));
}

#[test]
fn subdirectory_gitignore_scoped_to_subtree() {
    let cache = DCache::new(PathBuf::from("/"));
    // Root (1) -> sub (2, dir) -> file.txt (3)
    // Root (1) -> file.txt (4)
    cache.insert(addr(1), "sub".into(), addr(2), true);
    cache.insert(addr(2), "file.txt".into(), addr(3), false);
    cache.insert(addr(1), "file.txt".into(), addr(4), false);

    // .gitignore in /sub/ ignores "file.txt"
    cache.set_ignore_rules(addr(2), ".gitignore", "file.txt");

    // /sub/file.txt should be ignored
    assert!(cache.is_ignored(addr(3)), "/sub/file.txt should be ignored");
    // /file.txt should NOT be ignored (rule scoped to /sub/)
    assert!(
        !cache.is_ignored(addr(4)),
        "/file.txt should NOT be ignored"
    );
}

#[test]
fn deeper_negation_takes_precedence() {
    let cache = DCache::new(PathBuf::from("/"));
    // Root (1) -> sub (2, dir) -> important.log (3)
    cache.insert(addr(1), "sub".into(), addr(2), true);
    cache.insert(addr(2), "important.log".into(), addr(3), false);

    // Root ignores *.log
    cache.set_ignore_rules(addr(1), ".gitignore", "*.log");
    // Subdirectory un-ignores important.log
    cache.set_ignore_rules(addr(2), ".gitignore", "!important.log");

    // Deeper negation should win
    assert!(!cache.is_ignored(addr(3)));
}

#[test]
fn clear_ignore_rules_removes_matcher() {
    let cache = DCache::new(PathBuf::from("/"));
    cache.insert(addr(1), "debug.log".into(), addr(2), false);
    cache.set_ignore_rules(addr(1), ".gitignore", "*.log");
    assert!(cache.is_ignored(addr(2)));

    cache.clear_ignore_rules(addr(1), ".gitignore");
    assert!(!cache.is_ignored(addr(2)));
}

#[test]
fn is_ignored_returns_false_for_unknown_inode() {
    let cache = DCache::new(PathBuf::from("/"));
    assert!(!cache.is_ignored(addr(999)));
}

#[test]
fn is_name_ignored_checks_without_insertion() {
    let cache = DCache::new(PathBuf::from("/"));
    // Set up root with ignore rules but don't insert a child.
    // We need the root dir_state to exist, so insert a dummy child first.
    cache.insert(addr(1), "dummy".into(), addr(2), false);
    cache.set_ignore_rules(addr(1), ".gitignore", "*.log");

    assert!(cache.is_name_ignored(addr(1), "debug.log".as_ref(), false));
    assert!(!cache.is_name_ignored(addr(1), "readme.md".as_ref(), false));
}

#[test]
fn is_name_ignored_respects_subdirectory_scope() {
    let cache = DCache::new(PathBuf::from("/"));
    cache.insert(addr(1), "sub".into(), addr(2), true);
    cache.set_ignore_rules(addr(2), ".gitignore", "secret.txt");

    // Checking from /sub/ should match
    assert!(cache.is_name_ignored(addr(2), "secret.txt".as_ref(), false));
    // Checking from root should NOT match (rule scoped to /sub/)
    assert!(!cache.is_name_ignored(addr(1), "secret.txt".as_ref(), false));
}

#[test]
fn directory_only_rule_matches_directory_not_file() {
    let cache = DCache::new(PathBuf::from("/"));
    cache.insert(addr(1), "build".into(), addr(2), true); // directory
    cache.insert(addr(1), "build.rs".into(), addr(3), false); // file
    cache.set_ignore_rules(addr(1), ".gitignore", "build/");

    assert!(
        cache.is_ignored(addr(2)),
        "directory 'build' should be ignored by 'build/' rule"
    );
    assert!(
        !cache.is_ignored(addr(3)),
        "file 'build.rs' should NOT be ignored by 'build/' rule"
    );
}

#[test]
fn parent_of_returns_parent() {
    let cache = DCache::new(PathBuf::from("/"));
    cache.insert(addr(1), "child".into(), addr(2), false);
    assert_eq!(cache.parent_of(addr(2)), Some(addr(1)));
    assert_eq!(cache.parent_of(addr(999)), None);
}

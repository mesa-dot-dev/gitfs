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
        PopulateStatus::Claimed(_)
    ));
}

#[tokio::test]
async fn finish_populate_then_claim_returns_done() {
    let cache = DCache::new();
    let PopulateStatus::Claimed(claim_gen) = cache.try_claim_populate(LoadedAddr::new_unchecked(1))
    else {
        panic!("expected Claimed")
    };
    cache.finish_populate(LoadedAddr::new_unchecked(1), claim_gen);
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
        PopulateStatus::Claimed(_)
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
        PopulateStatus::Claimed(_)
    ));
    cache.abort_populate(LoadedAddr::new_unchecked(1));
    assert!(matches!(
        cache.try_claim_populate(LoadedAddr::new_unchecked(1)),
        PopulateStatus::Claimed(_)
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
            PopulateStatus::Claimed(_)
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
    for (i, name) in ["zebra", "apple", "mango"].iter().enumerate() {
        cache.insert(
            LoadedAddr::new_unchecked(1),
            OsString::from(*name),
            LoadedAddr::new_unchecked(10 + i as u64),
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

#[tokio::test]
async fn remove_child_returns_removed_entry() {
    let cache = DCache::new();
    let parent = LoadedAddr::new_unchecked(1);
    cache.insert(
        parent,
        OsString::from("foo"),
        LoadedAddr::new_unchecked(10),
        false,
    );
    let removed = cache.remove_child(parent, OsStr::new("foo"));
    assert!(removed.is_some(), "should return the removed entry");
    let dv = removed.unwrap();
    assert_eq!(dv.ino, LoadedAddr::new_unchecked(10));
    assert!(!dv.is_dir);
    assert!(
        cache.lookup(parent, OsStr::new("foo")).is_none(),
        "entry should no longer be present after removal"
    );
}

#[tokio::test]
async fn remove_child_returns_none_for_missing_entry() {
    let cache = DCache::new();
    let parent = LoadedAddr::new_unchecked(1);
    assert!(cache.remove_child(parent, OsStr::new("nope")).is_none());
}

#[tokio::test]
async fn remove_child_does_not_affect_siblings() {
    let cache = DCache::new();
    let parent = LoadedAddr::new_unchecked(1);
    cache.insert(
        parent,
        OsString::from("a"),
        LoadedAddr::new_unchecked(10),
        false,
    );
    cache.insert(
        parent,
        OsString::from("b"),
        LoadedAddr::new_unchecked(11),
        true,
    );
    cache.remove_child(parent, OsStr::new("a"));
    assert!(
        cache.lookup(parent, OsStr::new("b")).is_some(),
        "sibling should survive removal of another child"
    );
}

#[tokio::test]
async fn remove_parent_resets_populate_status() {
    let cache = DCache::new();
    let parent = LoadedAddr::new_unchecked(1);
    cache.insert(
        parent,
        OsString::from("x"),
        LoadedAddr::new_unchecked(10),
        false,
    );
    let PopulateStatus::Claimed(claim_gen) = cache.try_claim_populate(parent) else {
        panic!("expected Claimed")
    };
    cache.finish_populate(parent, claim_gen);
    assert!(matches!(
        cache.try_claim_populate(parent),
        PopulateStatus::Done
    ));

    assert!(
        cache.remove_parent(parent),
        "should return true for existing parent"
    );

    // After removal, the parent is gone, so populate returns Claimed again.
    assert!(matches!(
        cache.try_claim_populate(parent),
        PopulateStatus::Claimed(_)
    ));
    // Children should also be gone.
    assert!(cache.lookup(parent, OsStr::new("x")).is_none());
}

#[tokio::test]
async fn remove_parent_returns_false_for_unknown() {
    let cache = DCache::new();
    assert!(
        !cache.remove_parent(LoadedAddr::new_unchecked(999)),
        "should return false for unknown parent"
    );
}

#[tokio::test]
async fn evict_removes_child_and_resets_populate_status() {
    let cache = DCache::new();
    let parent = LoadedAddr::new_unchecked(1);
    let child = LoadedAddr::new_unchecked(10);
    cache.insert(parent, OsString::from("foo"), child, false);
    let PopulateStatus::Claimed(claim_gen) = cache.try_claim_populate(parent) else {
        panic!("expected Claimed")
    };
    cache.finish_populate(parent, claim_gen);
    assert!(matches!(
        cache.try_claim_populate(parent),
        PopulateStatus::Done
    ));

    cache.evict(child);

    // Child should be gone.
    assert!(cache.lookup(parent, OsStr::new("foo")).is_none());
    // Populate status should be reset so next readdir re-fetches.
    assert!(matches!(
        cache.try_claim_populate(parent),
        PopulateStatus::Claimed(_)
    ));
}

#[tokio::test]
async fn evict_unknown_child_is_noop() {
    let cache = DCache::new();
    // Should not panic or corrupt state.
    cache.evict(LoadedAddr::new_unchecked(999));
}

#[tokio::test]
async fn evict_does_not_affect_siblings() {
    let cache = DCache::new();
    let parent = LoadedAddr::new_unchecked(1);
    cache.insert(
        parent,
        OsString::from("a"),
        LoadedAddr::new_unchecked(10),
        false,
    );
    cache.insert(
        parent,
        OsString::from("b"),
        LoadedAddr::new_unchecked(11),
        true,
    );
    let PopulateStatus::Claimed(claim_gen) = cache.try_claim_populate(parent) else {
        panic!("expected Claimed")
    };
    cache.finish_populate(parent, claim_gen);

    cache.evict(LoadedAddr::new_unchecked(10));

    // Sibling should survive.
    assert!(cache.lookup(parent, OsStr::new("b")).is_some());
    // But populate status should be reset.
    assert!(matches!(
        cache.try_claim_populate(parent),
        PopulateStatus::Claimed(_)
    ));
}

#[tokio::test]
async fn evict_child_from_multiple_parents_removes_from_correct_parent() {
    let cache = DCache::new();
    let parent_a = LoadedAddr::new_unchecked(1);
    let parent_b = LoadedAddr::new_unchecked(2);
    let child = LoadedAddr::new_unchecked(10);
    // Same child addr under two parents — last insert wins in reverse index
    // because upsert_sync overwrites.
    cache.insert(parent_a, OsString::from("x"), child, false);
    cache.insert(parent_b, OsString::from("y"), child, false);

    cache.evict(child);

    // The parent_b entry should be removed (last insert wins in reverse index).
    assert!(cache.lookup(parent_b, OsStr::new("y")).is_none());
}

#[tokio::test]
async fn evict_during_populate_invalidates_generation() {
    let cache = DCache::new();
    let parent = LoadedAddr::new_unchecked(1);
    let child = LoadedAddr::new_unchecked(10);
    cache.insert(parent, OsString::from("foo"), child, false);

    let PopulateStatus::Claimed(claim_gen) = cache.try_claim_populate(parent) else {
        panic!("expected Claimed")
    };

    // Evict while populate is in progress.
    cache.evict(child);

    // Finish populate with the stale generation.
    cache.finish_populate(parent, claim_gen);

    // The finish_populate should have detected the generation mismatch
    // and reset to UNCLAIMED instead of DONE.
    assert!(
        matches!(cache.try_claim_populate(parent), PopulateStatus::Claimed(_)),
        "should be re-claimable after evict invalidated the generation"
    );
}

#[tokio::test]
async fn evict_then_reinsert_same_child_leaves_consistent_state() {
    let cache = DCache::new();
    let parent_a = LoadedAddr::new_unchecked(1);
    let parent_b = LoadedAddr::new_unchecked(2);
    let child = LoadedAddr::new_unchecked(10);

    // Insert child under parent_a.
    cache.insert(parent_a, OsString::from("foo"), child, false);
    assert!(cache.lookup(parent_a, OsStr::new("foo")).is_some());

    // Evict the child.
    cache.evict(child);
    assert!(cache.lookup(parent_a, OsStr::new("foo")).is_none());

    // Re-insert the same child under a different parent.
    cache.insert(parent_b, OsString::from("bar"), child, true);
    assert!(cache.lookup(parent_b, OsStr::new("bar")).is_some());

    // A second evict should remove from parent_b, not parent_a.
    cache.evict(child);
    assert!(
        cache.lookup(parent_b, OsStr::new("bar")).is_none(),
        "evict after re-insert should remove from the new parent"
    );
}

#[tokio::test]
async fn evict_with_concurrent_reparent_does_not_corrupt() {
    // Simulates the interleaving where insert re-parents a child between
    // evict's parent lookup and write-lock acquisition.
    let cache = DCache::new();
    let parent_a = LoadedAddr::new_unchecked(1);
    let parent_b = LoadedAddr::new_unchecked(2);
    let child = LoadedAddr::new_unchecked(10);

    // Insert child under parent_a.
    cache.insert(parent_a, OsString::from("foo"), child, false);

    // Simulate: evict reads parent_ino = parent_a from reverse index,
    // then insert re-parents child to parent_b before evict acquires
    // the write lock. We can't truly interleave threads here, but we
    // can verify the post-condition: after insert moves the child to
    // parent_b and evict runs, the child should still be in parent_b.
    cache.insert(parent_b, OsString::from("bar"), child, false);

    // Now evict — should detect that child_to_parent no longer points
    // to parent_a and leave parent_b's entry intact.
    cache.evict(child);

    // parent_b's "bar" entry should have been evicted (child_to_parent
    // now points to parent_b, so evict targets the correct parent).
    assert!(
        cache.lookup(parent_b, OsStr::new("bar")).is_none(),
        "evict should target the current parent, not a stale one"
    );
    // parent_a's "foo" entry should have been cleaned up by the insert
    // that re-parented the child to parent_b.
    assert!(
        cache.lookup(parent_a, OsStr::new("foo")).is_none(),
        "insert should clean up stale entry in old parent when re-parenting"
    );
}

#[tokio::test]
async fn insert_reparent_removes_stale_entry_from_old_parent() {
    let cache = DCache::new();
    let parent_a = LoadedAddr::new_unchecked(1);
    let parent_b = LoadedAddr::new_unchecked(2);
    let child = LoadedAddr::new_unchecked(10);

    cache.insert(parent_a, OsString::from("foo"), child, false);
    assert!(cache.lookup(parent_a, OsStr::new("foo")).is_some());

    // Re-parent: insert same child under parent_b.
    cache.insert(parent_b, OsString::from("bar"), child, false);

    // Old parent should no longer have the stale entry.
    assert!(
        cache.lookup(parent_a, OsStr::new("foo")).is_none(),
        "stale entry in old parent should be cleaned up on re-parent"
    );
    // New parent should have the entry.
    assert!(cache.lookup(parent_b, OsStr::new("bar")).is_some());
}

#[tokio::test]
async fn insert_reparent_resets_old_parent_populate_status() {
    let cache = DCache::new();
    let parent_a = LoadedAddr::new_unchecked(1);
    let parent_b = LoadedAddr::new_unchecked(2);
    let child = LoadedAddr::new_unchecked(10);

    cache.insert(parent_a, OsString::from("foo"), child, false);
    let PopulateStatus::Claimed(claim_gen) = cache.try_claim_populate(parent_a) else {
        panic!("expected Claimed");
    };
    cache.finish_populate(parent_a, claim_gen);
    assert!(matches!(
        cache.try_claim_populate(parent_a),
        PopulateStatus::Done
    ));

    // Re-parent: insert same child under parent_b.
    cache.insert(parent_b, OsString::from("bar"), child, false);

    // Old parent's populate status should be reset to allow re-fetch.
    assert!(
        matches!(
            cache.try_claim_populate(parent_a),
            PopulateStatus::Claimed(_)
        ),
        "old parent should be re-claimable after child was re-parented away"
    );
}

#[tokio::test]
async fn insert_reparent_does_not_remove_reused_name_in_old_parent() {
    let cache = DCache::new();
    let parent_a = LoadedAddr::new_unchecked(1);
    let parent_b = LoadedAddr::new_unchecked(2);
    let child_1 = LoadedAddr::new_unchecked(10);
    let child_2 = LoadedAddr::new_unchecked(20);

    // Insert child_1 under parent_a as "foo".
    cache.insert(parent_a, OsString::from("foo"), child_1, false);

    // Replace "foo" in parent_a with a different child (child_2).
    cache.insert(parent_a, OsString::from("foo"), child_2, false);

    // Now re-parent child_1 to parent_b. The old name "foo" is still in
    // child_to_name for child_1, but parent_a's "foo" now points to child_2.
    cache.insert(parent_b, OsString::from("bar"), child_1, false);

    // parent_a's "foo" should still point to child_2, not be removed.
    let dv = cache.lookup(parent_a, OsStr::new("foo"));
    assert!(
        dv.is_some(),
        "should not remove entry belonging to different child"
    );
    assert_eq!(dv.unwrap().ino, child_2);
}

#[tokio::test]
async fn insert_reparent_same_parent_removes_old_name() {
    let cache = DCache::new();
    let parent = LoadedAddr::new_unchecked(1);
    let child = LoadedAddr::new_unchecked(10);

    cache.insert(parent, OsString::from("foo"), child, false);

    // Re-insert under the same parent with a different name.
    cache.insert(parent, OsString::from("bar"), child, false);

    // "bar" should exist (the new entry).
    assert!(cache.lookup(parent, OsStr::new("bar")).is_some());
    // "foo" must be gone — otherwise readdir would return two entries
    // pointing to the same child inode.
    assert!(cache.lookup(parent, OsStr::new("foo")).is_none());
}

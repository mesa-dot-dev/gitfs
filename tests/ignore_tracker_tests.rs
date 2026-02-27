#![allow(clippy::unwrap_used, clippy::expect_used, missing_docs)]

use std::io::Write as _;
use std::path::PathBuf;
use std::sync::Arc;

use git_fs::ignore_tracker::IgnoreTracker;

#[test]
fn new_tracker_ignores_nothing() {
    let root = PathBuf::from("/tmp/fake-repo");
    let tracker = IgnoreTracker::new(root);

    assert!(
        !tracker.is_abspath_ignored(&PathBuf::from("/tmp/fake-repo/src/main.rs")),
        "fresh tracker should not ignore any path"
    );
    assert!(
        !tracker.is_abspath_ignored(&PathBuf::from("/tmp/fake-repo/.env")),
        "fresh tracker should not ignore any path"
    );
}

#[test]
fn observe_ignorefile_ignores_matching_paths() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_path_buf();

    let gitignore_path = root.join(".gitignore");
    {
        let mut f = std::fs::File::create(&gitignore_path).unwrap();
        writeln!(f, "*.log").unwrap();
        writeln!(f, "build/").unwrap();
        writeln!(f, "!important.log").unwrap();
    }

    // Create the build directory so `path.is_dir()` returns true when checked.
    std::fs::create_dir_all(root.join("build")).unwrap();

    let tracker = IgnoreTracker::new(root.clone());
    tracker.observe_ignorefile(&gitignore_path).unwrap();

    assert!(tracker.is_abspath_ignored(&root.join("debug.log")));
    assert!(tracker.is_abspath_ignored(&root.join("subdir/error.log")));
    assert!(tracker.is_abspath_ignored(&root.join("build")));
    assert!(!tracker.is_abspath_ignored(&root.join("important.log")));
    assert!(!tracker.is_abspath_ignored(&root.join("src/main.rs")));
}

#[test]
fn multiple_ignore_files_accumulate_rules() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_path_buf();

    let root_ignore = root.join(".gitignore");
    {
        let mut f = std::fs::File::create(&root_ignore).unwrap();
        writeln!(f, "*.log").unwrap();
    }

    let sub_dir = root.join("subdir");
    std::fs::create_dir_all(&sub_dir).unwrap();
    let sub_ignore = sub_dir.join(".gitignore");
    {
        let mut f = std::fs::File::create(&sub_ignore).unwrap();
        writeln!(f, "*.tmp").unwrap();
    }

    let tracker = IgnoreTracker::new(root.clone());
    tracker.observe_ignorefile(&root_ignore).unwrap();
    tracker.observe_ignorefile(&sub_ignore).unwrap();

    assert!(tracker.is_abspath_ignored(&root.join("debug.log")));
    assert!(tracker.is_abspath_ignored(&root.join("subdir/scratch.tmp")));
    assert!(!tracker.is_abspath_ignored(&root.join("src/lib.rs")));
}

#[test]
fn concurrent_reads_and_writes() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_path_buf();

    let gitignore_path = root.join(".gitignore");
    {
        let mut f = std::fs::File::create(&gitignore_path).unwrap();
        writeln!(f, "*.log").unwrap();
    }

    let tracker = Arc::new(IgnoreTracker::new(root.clone()));

    let mut handles = Vec::new();
    for _ in 0..4 {
        let t = Arc::clone(&tracker);
        let r = root.clone();
        handles.push(std::thread::spawn(move || {
            for _ in 0..1000 {
                let _ = t.is_abspath_ignored(&r.join("file.log"));
                let _ = t.is_abspath_ignored(&r.join("file.rs"));
            }
        }));
    }

    {
        let t = Arc::clone(&tracker);
        let gp = gitignore_path.clone();
        handles.push(std::thread::spawn(move || {
            for _ in 0..100 {
                t.observe_ignorefile(&gp).unwrap();
            }
        }));
    }

    for h in handles {
        h.join().expect("thread panicked");
    }

    tracker.observe_ignorefile(&gitignore_path).unwrap();
    assert!(tracker.is_abspath_ignored(&root.join("test.log")));
    assert!(!tracker.is_abspath_ignored(&root.join("test.rs")));
}

#[test]
fn observe_nonexistent_file_returns_parse_error() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_path_buf();
    let tracker = IgnoreTracker::new(root.clone());

    let missing = root.join("nonexistent/.gitignore");
    let result = tracker.observe_ignorefile(&missing);

    assert!(
        result.is_err(),
        "observing a missing ignore file should return a parse error"
    );
    let err = result.unwrap_err();
    assert!(
        matches!(err, git_fs::ignore_tracker::ObserveError::Parse { .. }),
        "expected Parse variant, got: {err:?}"
    );
    // Tracker should still have no rules after the failed observe.
    assert!(!tracker.is_abspath_ignored(&root.join("anything.txt")));
}

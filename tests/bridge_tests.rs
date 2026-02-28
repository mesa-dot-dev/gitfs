#![allow(clippy::unwrap_used, missing_docs)]

use mesafs::fs::bridge::ConcurrentBridge;

#[test]
fn insert_then_forward_returns_inner() {
    let bridge = ConcurrentBridge::new();
    bridge.insert(10, 100);
    assert_eq!(bridge.forward(10), Some(100));
}

#[test]
fn insert_then_backward_returns_outer() {
    let bridge = ConcurrentBridge::new();
    bridge.insert(10, 100);
    assert_eq!(bridge.backward(100), Some(10));
}

#[test]
fn forward_missing_returns_none() {
    let bridge = ConcurrentBridge::new();
    assert_eq!(bridge.forward(42), None);
}

#[test]
fn backward_or_insert_existing_returns_cached() {
    let bridge = ConcurrentBridge::new();
    bridge.insert(10, 100);
    let outer = bridge.backward_or_insert(100, 999);
    assert_eq!(outer, 10, "should return existing outer addr");
}

#[test]
fn backward_or_insert_new_allocates() {
    let bridge = ConcurrentBridge::new();
    let outer = bridge.backward_or_insert(200, 50);
    assert_eq!(outer, 50, "should use fallback address");
    assert_eq!(bridge.forward(50), Some(200));
    assert_eq!(bridge.backward(200), Some(50));
}

#[test]
fn remove_by_outer_clears_both_directions() {
    let bridge = ConcurrentBridge::new();
    bridge.insert(10, 100);
    let (removed_inner, empty) = bridge.remove_by_outer(10);
    assert_eq!(
        removed_inner,
        Some(100),
        "should return the removed inner addr"
    );
    assert!(
        empty,
        "bridge should be empty after removing the only entry"
    );
    assert_eq!(bridge.forward(10), None);
    assert_eq!(bridge.backward(100), None);
}

#[test]
fn remove_by_outer_missing_key_returns_none() {
    let bridge = ConcurrentBridge::new();
    bridge.insert(10, 100);
    let (removed_inner, empty) = bridge.remove_by_outer(42);
    assert_eq!(removed_inner, None, "should return None for missing key");
    assert!(
        !empty,
        "bridge should not be empty when other entries exist"
    );
}

#[test]
fn remove_by_outer_not_empty_when_others_remain() {
    let bridge = ConcurrentBridge::new();
    bridge.insert(10, 100);
    bridge.insert(20, 200);
    let (removed_inner, empty) = bridge.remove_by_outer(10);
    assert_eq!(removed_inner, Some(100));
    assert!(
        !empty,
        "bridge should not be empty when other entries remain"
    );
    assert_eq!(bridge.forward(20), Some(200), "other entry should survive");
}

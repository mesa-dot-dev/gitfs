#![allow(clippy::unwrap_used, missing_docs)]

use git_fs::fs::bridge::ConcurrentBridge;

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
    let outer = bridge.backward_or_insert(100, || 999);
    assert_eq!(outer, 10, "should return existing outer addr");
}

#[test]
fn backward_or_insert_new_allocates() {
    let bridge = ConcurrentBridge::new();
    let outer = bridge.backward_or_insert(200, || 50);
    assert_eq!(outer, 50, "should use allocator");
    assert_eq!(bridge.forward(50), Some(200));
    assert_eq!(bridge.backward(200), Some(50));
}

#[test]
fn remove_by_outer_clears_both_directions() {
    let bridge = ConcurrentBridge::new();
    bridge.insert(10, 100);
    bridge.remove_by_outer(10);
    assert_eq!(bridge.forward(10), None);
    assert_eq!(bridge.backward(100), None);
}

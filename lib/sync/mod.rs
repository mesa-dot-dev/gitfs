//! Synchronization primitives.
//!
//! Shims between loom and std synchronization primitives.
pub mod atomic;

#[cfg(loom)]
pub use loom::sync::{Arc, Condvar, Mutex, Once, RwLock, RwLockReadGuard, RwLockWriteGuard};

#[cfg(loom)]
pub struct OnceLock<T> {
    once: Once<T>,
}

#[cfg(loom)]
unsafe impl<T: Send + Sync> Send for OnceLock<T> {}

#[cfg(loom)]
unsafe impl<T: Send + Sync> Sync for OnceLock<T> {}

#[cfg(loom)]
impl<T> OnceLock<T> {
    pub fn new() -> Self {
        Self {
            once: Once::new(),
            value: UnsafeCell::new(None),
        }
    }

    pub fn get(&self) -> Option<&T> {
        if self.once.is_completed() {
            // Safety: once is completed, so value is initialized
            // and will never be written to again
            unsafe { (*self.value.with(|ptr| ptr)).as_ref() }
        } else {
            None
        }
    }

    pub fn get_or_init(&self, f: impl FnOnce() -> T) -> &T {
        self.once.call_once(|| unsafe {
            self.value.with_mut(|ptr| {
                *ptr = Some(f());
            });
        });
        // Safety: call_once guarantees initialization is complete
        unsafe { (*self.value.with(|ptr| ptr)).as_ref().unwrap() }
    }

    pub fn set(&self, value: T) -> Result<(), T> {
        let mut value = Some(value);
        self.once.call_once(|| {
            let val = value.take().unwrap();
            unsafe {
                self.value.with_mut(|ptr| {
                    *ptr = Some(val);
                });
            }
        });
        match value {
            None => Ok(()),    // we consumed it — success
            Some(v) => Err(v), // already initialized
        }
    }
}

#[cfg(not(loom))]
pub use std::sync::{Arc, Condvar, Mutex, OnceLock, RwLock, RwLockReadGuard, RwLockWriteGuard};

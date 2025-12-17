use std::sync::atomic::{AtomicPtr, Ordering};

use datafusion::common::Result;
use datafusion::common::utils::proxy::VecAllocExt;

/// A thread-safe array of values with lock-free concurrent access.
///
/// This array allows multiple threads to concurrently:
/// - Read existing values
/// - Set new values if slots are empty
/// - Create and cache values on-demand
///
/// Uses atomic pointers for lock-free operations with proper memory ordering.
/// Null pointers indicate empty slots.
pub struct InitOnceArray<T: Send + Sync> {
    ptrs: Vec<AtomicPtr<T>>,
}

impl<T: Send + Sync> InitOnceArray<T> {
    /// Create a new array with the specified size.
    /// All slots start as empty (null pointers).
    pub fn new(size: usize) -> Self {
        let ptrs = (0..size)
            .map(|_| AtomicPtr::new(std::ptr::null_mut()))
            .collect();
        Self { ptrs }
    }

    /// Atomically set a value at the given index if the slot is currently
    /// empty.
    ///
    /// Returns tuple containing:
    /// - reference to the value that is now at that index:
    ///   - If the slot was empty, returns the newly set value
    ///   - If the slot was occupied, returns the existing value
    /// - boolean indicating whether the slot was empty
    ///
    /// # Panics
    /// Panics if index is out of bounds (internal API, bounds checking is
    /// caller's responsibility).
    pub fn set_if_null(&self, index: usize, value: Box<T>) -> (&T, bool) {
        let new_ptr = Box::into_raw(value);
        let result = self.ptrs[index].compare_exchange_weak(
            std::ptr::null_mut(),
            new_ptr,
            Ordering::AcqRel,
            Ordering::Acquire,
        );

        match result {
            Ok(_) => (unsafe { &*new_ptr }, true), // Successfully set - return reference to the
            // new value
            Err(old_ptr) => {
                // Drop the new value since it's not set. The slot at index is already occupied.
                let _ = unsafe { Box::from_raw(new_ptr) };
                (unsafe { &*old_ptr }, false)
            }
        }
    }

    /// Get the value at the given index, if present.
    ///
    /// Returns None if the slot is empty or index is out of bounds.
    ///
    /// # Panics
    /// Panics if index is out of bounds (internal API, bounds checking is
    /// caller's responsibility).
    pub fn get(&self, index: usize) -> Option<&T> {
        let ptr = self.ptrs[index].load(Ordering::Acquire);
        if ptr.is_null() {
            None
        } else {
            Some(unsafe { &*ptr })
        }
    }

    /// Get the value at the given index, or create and set it if not present.
    ///
    /// This is the primary method for lazy initialization of values.
    /// Multiple threads can safely call this method concurrently.
    ///
    /// Returns tuple containing:
    /// - reference to the value that is now at that index:
    ///   - If the slot was empty, returns the newly set value
    ///   - If the slot was occupied, returns the existing value
    /// - boolean indicating whether the returned object is newly created
    ///
    /// # Panics
    /// Panics if index is out of bounds (internal API, bounds checking is
    /// caller's responsibility).
    pub fn get_or_create(&self, index: usize, f: impl FnOnce() -> Result<T>) -> Result<(&T, bool)> {
        if let Some(value) = self.get(index) {
            return Ok((value, false));
        }

        let new_value = f()?;
        Ok(self.set_if_null(index, Box::new(new_value)))
    }

    /// Get the allocated size of the array. This does not include the size of
    /// the objects pointed by the elements in the array.
    pub fn allocated_size(&self) -> usize {
        self.ptrs.allocated_size()
    }
}

impl<T: Send + Sync> Drop for InitOnceArray<T> {
    fn drop(&mut self) {
        for ptr in &self.ptrs {
            let ptr = ptr.load(Ordering::Acquire);
            if !ptr.is_null() {
                unsafe {
                    let _ = Box::from_raw(ptr);
                }
            }
        }
    }
}

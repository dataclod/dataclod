use std::sync::atomic::{AtomicUsize, Ordering};

use datafusion::common::Result;
use datafusion::execution::memory_pool::MemoryReservation;
use parking_lot::Mutex;

/// A wrapper around `MemoryReservation` that reserves memory ahead of time to
/// reduce the contention caused by concurrent access to the reservation. It
/// will reserve more memory than requested to skip growing the reservation for
/// the next few reservation growth requests, until the actually reserved size
/// is smaller than the requested size.
pub(crate) struct ConcurrentReservation {
    reservation: Mutex<MemoryReservation>,
    /// The size of reservation. This should be equal to `reservation.size()`.
    /// This is used to minimize contention and avoid growing the underlying
    /// reservation in the fast path.
    reserved_size: AtomicUsize,
    prealloc_size: usize,
}

impl ConcurrentReservation {
    pub fn new(prealloc_size: usize, reservation: MemoryReservation) -> Self {
        let actual_size = reservation.size();

        Self {
            reservation: Mutex::new(reservation),
            reserved_size: AtomicUsize::new(actual_size),
            prealloc_size,
        }
    }

    /// Resize the reservation to the given size. If the new size is smaller or
    /// equal to the current reserved size, do nothing. Otherwise grow the
    /// reservation to be `prealloc_size` larger than the new size. This is
    /// for reducing the frequency of growing the underlying reservation.
    pub fn resize(&self, new_size: usize) -> Result<()> {
        // Fast path: the reserved size is already large enough, no need to lock and
        // grow the reservation
        if new_size <= self.reserved_size.load(Ordering::Relaxed) {
            return Ok(());
        }

        // Slow path: lock the mutex for possible reservation growth
        let mut reservation = self.reservation.lock();
        let current_size = reservation.size();

        // Double-check under the lock in case another thread already grew it
        if new_size <= current_size {
            return Ok(());
        }

        // Grow the reservation to the target size
        let growth_needed = new_size + self.prealloc_size - current_size;
        reservation.try_grow(growth_needed)?;

        // Update our atomic to reflect the new size
        let final_size = reservation.size();
        self.reserved_size.store(final_size, Ordering::Relaxed);

        Ok(())
    }
}

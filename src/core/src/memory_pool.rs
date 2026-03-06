use datafusion::common::{DataFusionError, Result, resources_datafusion_err};
use datafusion::execution::memory_pool::{
    MemoryConsumer, MemoryLimit, MemoryPool, MemoryReservation,
};
use parking_lot::Mutex;

pub const DEFAULT_UNSPILLABLE_RESERVE_RATIO: f64 = 0.2;

/// A [`MemoryPool`] implementation similar to `DataFusion`'s
/// [`datafusion::execution::memory_pool::FairSpillPool`], but with the
/// following changes:
///
/// Spillable and non-spillable operators use logically separate portions of the
/// memory pool, controlled by `unspillable_reserve_ratio`, instead of sharing a
/// single pool as in `DataFusion`'s default `FairSpillPool`, which can lead to
/// the following issue: spillable consumers could potentially exhaust all
/// available memory, preventing unspillable operations from acquiring necessary
/// resources. This behavior is tracked in `DataFusion` issue <https://github.com/apache/datafusion/issues/17334>.
///
/// By reserving a configurable fraction of the total memory pool specifically
/// for unspillable allocations (defined by `unspillable_reserve_ratio`), this
/// pool ensures that critical non-spillable operations can proceed even under
/// heavy memory pressure from spillable operators.
#[derive(Debug)]
pub struct QueryFairSpillPool {
    /// The total memory limit
    pool_size: usize,
    /// The fraction of memory reserved for unspillable consumers (0.0 - 1.0)
    unspillable_reserve_ratio: f64,

    state: Mutex<FairSpillPoolState>,
}

#[derive(Debug)]
struct FairSpillPoolState {
    /// The number of consumers that can spill
    num_spill: usize,

    /// The total amount of memory reserved that can be spilled
    spillable: usize,

    /// The total amount of memory reserved by consumers that cannot spill
    unspillable: usize,
}

impl QueryFairSpillPool {
    /// Allocate up to `limit` bytes
    pub fn new(pool_size: usize, unspillable_reserve_ratio: f64) -> Self {
        Self {
            pool_size,
            unspillable_reserve_ratio,
            state: Mutex::new(FairSpillPoolState {
                num_spill: 0,
                spillable: 0,
                unspillable: 0,
            }),
        }
    }
}

impl MemoryPool for QueryFairSpillPool {
    fn register(&self, consumer: &MemoryConsumer) {
        if consumer.can_spill() {
            self.state.lock().num_spill += 1;
        }
    }

    fn unregister(&self, consumer: &MemoryConsumer) {
        if consumer.can_spill() {
            let mut state = self.state.lock();
            state.num_spill = state.num_spill.checked_sub(1).unwrap();
        }
    }

    fn grow(&self, reservation: &MemoryReservation, additional: usize) {
        let mut state = self.state.lock();
        match reservation.consumer().can_spill() {
            true => state.spillable += additional,
            false => state.unspillable += additional,
        }
    }

    fn shrink(&self, reservation: &MemoryReservation, shrink: usize) {
        let mut state = self.state.lock();
        match reservation.consumer().can_spill() {
            true => state.spillable -= shrink,
            false => state.unspillable -= shrink,
        }
    }

    fn try_grow(&self, reservation: &MemoryReservation, additional: usize) -> Result<()> {
        let mut state = self.state.lock();

        // Calculate the amount of memory reserved for unspillable consumers
        let reserved_for_unspillable =
            (self.pool_size as f64 * self.unspillable_reserve_ratio) as usize;

        // The effective unspillable usage is the max of actual usage and the reserved
        // amount
        let effective_unspillable = state.unspillable.max(reserved_for_unspillable);

        // The total amount of memory available to spilling consumers
        let spill_available = self.pool_size.saturating_sub(effective_unspillable);

        match reservation.consumer().can_spill() {
            true => {
                // No spiller may use more than their fraction of the memory available
                let available = spill_available
                    .checked_div(state.num_spill)
                    .unwrap_or(spill_available);

                if reservation.size() + additional > available {
                    return Err(insufficient_capacity_err(
                        reservation,
                        additional,
                        available,
                        effective_unspillable,
                        spill_available,
                    ));
                }
                state.spillable += additional;
            }
            false => {
                let available = self
                    .pool_size
                    .saturating_sub(state.unspillable + state.spillable);

                if available < additional {
                    return Err(insufficient_capacity_err(
                        reservation,
                        additional,
                        available,
                        effective_unspillable,
                        spill_available,
                    ));
                }
                state.unspillable += additional;
            }
        }
        Ok(())
    }

    fn reserved(&self) -> usize {
        let state = self.state.lock();
        state.spillable + state.unspillable
    }

    fn memory_limit(&self) -> MemoryLimit {
        MemoryLimit::Finite(self.pool_size)
    }
}

fn insufficient_capacity_err(
    reservation: &MemoryReservation, additional: usize, available: usize, unspillable: usize,
    spill_available: usize,
) -> DataFusionError {
    resources_datafusion_err!(
        "Failed to allocate additional {} bytes for {} with {} bytes already allocated - maximum available is {} bytes. \
        Current unspillable memory usage: {} bytes, spillable memory available: {} bytes",
        additional,
        reservation.consumer().name(),
        reservation.size(),
        available,
        unspillable,
        spill_available
    )
}

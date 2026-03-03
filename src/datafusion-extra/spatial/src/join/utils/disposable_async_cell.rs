use std::fmt;

use parking_lot::Mutex;
use tokio::sync::Notify;

/// Error returned when writing to a [`DisposableAsyncCell`] fails.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CellSetError {
    /// The cell has already been disposed, so new values are rejected.
    Disposed,

    /// The cell already has a value.
    AlreadySet,
}

/// An asynchronous cell that can be set at most once before either being
/// disposed or read by any number of waiters.
///
/// This is used as a lightweight one-shot coordination primitive in the spatial
/// join implementation. For example, `PartitionedIndexProvider` keeps one
/// `DisposableAsyncCell` per regular partition to publish either a successfully
/// built `SpatialIndex` (or the build error) exactly once. Concurrent
/// `SpatialJoinStream`s racing to probe the same partition can then await the
/// same shared result instead of building duplicate indexes.
///
/// When an index is no longer needed (e.g. the last stream finishes a
/// partition), the cell can be disposed to free resources.
///
/// Awaiters calling [`DisposableAsyncCell::get`] will park until a value is set
/// or the cell is disposed. Once disposed, `get` returns `None` and `set`
/// returns [`CellSetError::Disposed`].
pub struct DisposableAsyncCell<T> {
    state: Mutex<CellState<T>>,
    notify: Notify,
}

impl<T> fmt::Debug for DisposableAsyncCell<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DisposableAsyncCell")
    }
}

impl<T> Default for DisposableAsyncCell<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> DisposableAsyncCell<T> {
    /// Creates a new empty cell with no stored value.
    pub fn new() -> Self {
        Self {
            state: Mutex::new(CellState::Empty),
            notify: Notify::new(),
        }
    }

    /// Marks the cell as disposed and wakes every waiter.
    pub fn dispose(&self) {
        {
            let mut state = self.state.lock();
            *state = CellState::Disposed;
        }
        self.notify.notify_waiters();
    }

    /// Check whether the cell has a value or not.
    pub fn is_set(&self) -> bool {
        let state = self.state.lock();
        matches!(*state, CellState::Value(_))
    }

    /// Check whether the cell is empty (not set or disposed)
    pub fn is_empty(&self) -> bool {
        let state = self.state.lock();
        matches!(*state, CellState::Empty)
    }
}

impl<T: Clone> DisposableAsyncCell<T> {
    /// Waits until a value is set or the cell is disposed.
    /// Returns `None` if the cell is disposed without a value.
    pub async fn get(&self) -> Option<T> {
        loop {
            let notified = self.notify.notified();
            {
                let state = self.state.lock();
                match &*state {
                    CellState::Value(val) => return Some(val.clone()),
                    CellState::Disposed => return None,
                    CellState::Empty => {}
                }
            }
            notified.await;
        }
    }

    /// Stores the provided value if the cell is still empty.
    /// Fails if a value already exists or the cell has been disposed.
    pub fn set(&self, value: T) -> std::result::Result<(), CellSetError> {
        {
            let mut state = self.state.lock();
            match &mut *state {
                CellState::Empty => *state = CellState::Value(value),
                CellState::Disposed => return Err(CellSetError::Disposed),
                CellState::Value(_) => return Err(CellSetError::AlreadySet),
            }
        }

        self.notify.notify_waiters();
        Ok(())
    }
}

enum CellState<T> {
    Empty,
    Value(T),
    Disposed,
}

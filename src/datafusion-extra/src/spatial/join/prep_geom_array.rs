use std::sync::atomic::{AtomicPtr, Ordering};

use datafusion::common::{DataFusionError, Result};
use geos::{Geom, PreparedGeometry};
use parking_lot::Mutex;

/// A wrapper around a GEOS Geometry and its corresponding PreparedGeometry.
///
/// This struct solves the self-referential lifetime problem by using unsafe
/// transmutation to extend the PreparedGeometry lifetime to 'static. This is
/// safe because:
/// 1. The PreparedGeometry is created from self.geometry, which lives as long
///    as self
/// 2. The PreparedGeometry is stored in self and will be dropped before
///    self.geometry
/// 3. We only return references, never move the PreparedGeometry out
///
/// The PreparedGeometry is protected by a Mutex because it has internal mutable
/// state that is not thread-safe.
pub struct OwnedPreparedGeometry {
    geometry: geos::Geometry,
    /// PreparedGeometry references the original geometry `geometry` it is
    /// created from. The GEOS objects are allocated on the heap so moving
    /// `OwnedPreparedGeometry` does not move the underlying GEOS object, so
    /// we don't need to worry about pinning.
    ///
    /// `PreparedGeometry` is not thread-safe, because it has some lazily
    /// initialized internal states, so we need to use a `Mutex` to protect
    /// it.
    prepared_geometry: Mutex<PreparedGeometry<'static>>,
}

impl OwnedPreparedGeometry {
    /// Create a new OwnedPreparedGeometry from a GEOS Geometry.
    pub fn try_new(geometry: geos::Geometry) -> Result<Self> {
        let prepared = geometry.to_prepared_geom().map_err(|e| {
            DataFusionError::Execution(format!("Failed to create prepared geometry: {e}"))
        })?;

        // SAFETY: We're extending the lifetime of PreparedGeometry to 'static.
        // This is safe because:
        // 1. The PreparedGeometry is created from self.geometry, which lives as long as
        //    self
        // 2. The PreparedGeometry is stored in self.prepared_geometry, which also lives
        //    as long as self
        // 3. We only return references to the PreparedGeometry, never move it out
        // 4. The PreparedGeometry will be dropped when self is dropped, before
        //    self.geometry
        let prepared_static: PreparedGeometry<'static> = unsafe { std::mem::transmute(prepared) };

        Ok(Self {
            geometry,
            prepared_geometry: Mutex::new(prepared_static),
        })
    }

    /// Create a new OwnedPreparedGeometry from WKB bytes.
    pub fn try_from_wkb(wkb: &[u8]) -> Result<Self> {
        let geometry = geos::Geometry::new_from_wkb(wkb).map_err(|e| {
            DataFusionError::Execution(format!("Failed to create geometry from WKB: {e}"))
        })?;
        Self::try_new(geometry)
    }

    /// Get access to the prepared geometry via a Mutex.
    ///
    /// The returned reference has a lifetime tied to &self, which ensures
    /// memory safety. The 'static lifetime on PreparedGeometry indicates it
    /// doesn't borrow from external data.
    pub fn prepared(&self) -> &Mutex<PreparedGeometry<'static>> {
        &self.prepared_geometry
    }

    /// Get the original geometry (for testing purposes).
    pub fn geometry(&self) -> &geos::Geometry {
        &self.geometry
    }
}

/// A thread-safe array of prepared geometries with lock-free concurrent access.
///
/// This array allows multiple threads to concurrently:
/// - Read existing prepared geometries
/// - Set new prepared geometries if slots are empty
/// - Create and cache prepared geometries on-demand
///
/// Uses atomic pointers for lock-free operations with proper memory ordering.
/// Null pointers indicate empty slots.
pub(crate) struct PreparedGeometryArray {
    geometries: Vec<AtomicPtr<OwnedPreparedGeometry>>,
}

impl PreparedGeometryArray {
    /// Create a new array with the specified size.
    /// All slots start as empty (null pointers).
    pub fn new(size: usize) -> Self {
        let geometries = (0..size)
            .map(|_| AtomicPtr::new(std::ptr::null_mut()))
            .collect();
        Self { geometries }
    }

    /// Atomically set a geometry at the given index if the slot is currently
    /// empty.
    ///
    /// Returns a reference to the geometry that is now at that index:
    /// - If the slot was empty, returns the newly set geometry
    /// - If the slot was occupied, returns the existing geometry and drops the
    ///   new one
    ///
    /// # Panics
    /// Panics if index is out of bounds (internal API, bounds checking is
    /// caller's responsibility).
    pub fn set_if_null(
        &self, index: usize, geometry: Box<OwnedPreparedGeometry>,
    ) -> &OwnedPreparedGeometry {
        let new_ptr = Box::into_raw(geometry);
        let result = self.geometries[index].compare_exchange_weak(
            std::ptr::null_mut(),
            new_ptr,
            Ordering::AcqRel,
            Ordering::Acquire,
        );

        match result {
            Ok(_) => unsafe { &*new_ptr }, // Successfully set - return reference to the new
            // geometry
            Err(old_ptr) => {
                // Drop the new geometry since it's not set. The slot at index is already
                // occupied.
                let _ = unsafe { Box::from_raw(new_ptr) };
                unsafe { &*old_ptr }
            }
        }
    }

    /// Get the geometry at the given index, if present.
    ///
    /// Returns None if the slot is empty or index is out of bounds.
    ///
    /// # Panics
    /// Panics if index is out of bounds (internal API, bounds checking is
    /// caller's responsibility).
    pub fn get(&self, index: usize) -> Option<&OwnedPreparedGeometry> {
        let ptr = self.geometries[index].load(Ordering::Acquire);
        if ptr.is_null() {
            None
        } else {
            Some(unsafe { &*ptr })
        }
    }

    /// Get the geometry at the given index, or create and set it if not
    /// present.
    ///
    /// This is the primary method for lazy initialization of prepared
    /// geometries. Multiple threads can safely call this method
    /// concurrently.
    ///
    /// # Panics
    /// Panics if index is out of bounds (internal API, bounds checking is
    /// caller's responsibility).
    pub fn get_or_create(
        &self, index: usize, f: impl FnOnce() -> Result<OwnedPreparedGeometry>,
    ) -> Result<&OwnedPreparedGeometry> {
        if let Some(prep_geom) = self.get(index) {
            return Ok(prep_geom);
        }

        let geometry = f()?;
        Ok(self.set_if_null(index, Box::new(geometry)))
    }

    /// Get the size of the array.
    #[allow(dead_code)]
    pub fn len(&self) -> usize {
        self.geometries.len()
    }

    /// Check if the array is empty.
    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.geometries.is_empty()
    }
}

impl Drop for PreparedGeometryArray {
    fn drop(&mut self) {
        for geometry in &self.geometries {
            let ptr = geometry.load(Ordering::Relaxed);
            if !ptr.is_null() {
                unsafe {
                    let _ = Box::from_raw(ptr);
                }
            }
        }
    }
}

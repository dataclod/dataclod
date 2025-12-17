use anyhow::{Error, Result};
use serde::{Deserialize, Serialize};
use serde_with::{DisplayFromStr, serde_as};

/// Generic 1D intervals with wraparound support
///
/// This trait specifies common behaviour implemented by the [Interval] and
/// [`WraparoundInterval`]. In general, one should use [Interval] unless
/// specifically working with wraparound, as the contingency of wraparound
/// incurs overhead (particularly in a loop). This trait is mostly used to
/// simplify testing and unify documentation for the two concrete
/// implementations.
pub trait IntervalTrait: std::fmt::Debug + PartialEq {
    /// Create an interval from lo and hi values
    fn new(lo: f64, hi: f64) -> Self;

    /// Create an empty interval that intersects nothing (except the full
    /// interval)
    fn empty() -> Self;

    /// Create the full interval (that intersects everything, including the
    /// empty interval)
    fn full() -> Self;

    /// Lower bound
    ///
    /// If `is_wraparound()` returns false, this is also the minimum value. When
    /// empty, this value is Infinity; when full, this value is -Infinity.
    fn lo(&self) -> f64;

    /// Upper bound
    ///
    /// If `is_wraparound()` returns false, this is also the maximum value. When
    /// empty, this value is -Infinity; when full, this value is Infinity.
    fn hi(&self) -> f64;

    /// Check for wraparound
    ///
    /// If `is_wraparound()` returns false, this interval represents the values
    /// that are between lo and hi. If `is_wraparound()` returns true, this
    /// interval represents the values that are *not* between lo and hi.
    ///
    /// It is recommended to work directly with an [Interval] where this is
    /// guaranteed to return false unless wraparound support is specifically
    /// required.
    fn is_wraparound(&self) -> bool;

    /// Check for potential intersection with an interval
    ///
    /// Note that intervals always contain their endpoints (for both the
    /// wraparound and non-wraparound case).
    ///
    /// This method accepts Self for performance reasons to prevent unnecessary
    /// checking of `is_wraparound()` when not required for an
    /// implementation.
    fn intersects_interval(&self, other: &Self) -> bool;

    /// The width of the interval
    ///
    /// For the non-wraparound case, this is the distance between lo and hi. For
    /// the wraparound case, this is infinity.
    fn width(&self) -> f64;

    /// The midpoint of the interval
    ///
    /// For the non-wraparound case, this is the point exactly between lo and
    /// hi. For the wraparound case, this is arbitrarily chosen as infinity
    /// (to preserve the property that intervals intersect their midpoint).
    fn mid(&self) -> f64;

    /// True if this interval is empty (i.e. intersects no values)
    fn is_empty(&self) -> bool;

    /// Compute a new interval that is the union of both
    ///
    /// When accumulating intervals in a loop, use
    /// [`Interval::update_interval`].
    fn merge_interval(&self, other: &Self) -> Self;
}

/// 1D Interval that never wraps around
///
/// Represents a minimum and maximum value without wraparound logic (see
/// [`WraparoundInterval`] for a wraparound implementation).
#[serde_as]
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct Interval {
    /// Lower bound
    ///
    /// This is serialized and deserialized using its string representation to
    /// preserve NaN and Infinity values when using `serde_json`.
    #[serde_as(as = "DisplayFromStr")]
    lo: f64,

    /// Upper bound
    ///
    /// This is serialized and deserialized using its string representation to
    /// preserve NaN and Infinity values when using `serde_json`.
    #[serde_as(as = "DisplayFromStr")]
    hi: f64,
}

impl Interval {
    /// Expand this interval to the union of self and other in place
    ///
    /// Note that NaN values are ignored when updating bounds.
    pub fn update_interval(&mut self, other: &Self) {
        self.lo = self.lo.min(other.lo);
        self.hi = self.hi.max(other.hi);
    }

    /// Expand this interval to the union of self and other in place
    ///
    /// Note that NaN values are ignored when updating bounds.
    pub fn update_value(&mut self, other: f64) {
        self.lo = self.lo.min(other);
        self.hi = self.hi.max(other);
    }
}

impl From<(f64, f64)> for Interval {
    fn from(value: (f64, f64)) -> Self {
        Interval::new(value.0, value.1)
    }
}

impl From<(i32, i32)> for Interval {
    fn from(value: (i32, i32)) -> Self {
        Interval::new(value.0 as f64, value.1 as f64)
    }
}

impl TryFrom<WraparoundInterval> for Interval {
    type Error = Error;

    fn try_from(value: WraparoundInterval) -> Result<Self, Self::Error> {
        if value.is_wraparound() {
            Err(anyhow::anyhow!(
                "Can't convert wraparound interval {value:?} to Interval"
            ))
        } else {
            Ok(Interval::new(value.lo(), value.hi()))
        }
    }
}

impl IntervalTrait for Interval {
    fn new(lo: f64, hi: f64) -> Self {
        Self { lo, hi }
    }

    fn empty() -> Self {
        Self {
            lo: f64::INFINITY,
            hi: -f64::INFINITY,
        }
    }

    fn full() -> Self {
        Self {
            lo: -f64::INFINITY,
            hi: f64::INFINITY,
        }
    }

    fn lo(&self) -> f64 {
        self.lo
    }

    fn hi(&self) -> f64 {
        self.hi
    }

    fn is_wraparound(&self) -> bool {
        false
    }

    fn intersects_interval(&self, other: &Self) -> bool {
        self.lo <= other.hi && other.lo <= self.hi
    }

    fn width(&self) -> f64 {
        self.hi - self.lo
    }

    fn mid(&self) -> f64 {
        self.lo + self.width() / 2.0
    }

    fn is_empty(&self) -> bool {
        self.width() == -f64::INFINITY
    }

    fn merge_interval(&self, other: &Self) -> Self {
        let mut out = *self;
        out.update_interval(other);
        out
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct WraparoundInterval {
    inner: Interval,
}

impl WraparoundInterval {
    fn split(&self) -> (Interval, Interval) {
        if self.is_wraparound() {
            (
                Interval {
                    lo: -f64::INFINITY,
                    hi: self.inner.hi,
                },
                Interval {
                    lo: self.inner.lo,
                    hi: f64::INFINITY,
                },
            )
        } else {
            (self.inner, Interval::empty())
        }
    }
}

impl From<(f64, f64)> for WraparoundInterval {
    fn from(value: (f64, f64)) -> Self {
        WraparoundInterval::new(value.0, value.1)
    }
}

impl From<(i32, i32)> for WraparoundInterval {
    fn from(value: (i32, i32)) -> Self {
        WraparoundInterval::new(value.0 as f64, value.1 as f64)
    }
}

impl From<Interval> for WraparoundInterval {
    fn from(value: Interval) -> Self {
        WraparoundInterval::new(value.lo(), value.hi())
    }
}

impl IntervalTrait for WraparoundInterval {
    fn new(lo: f64, hi: f64) -> Self {
        Self {
            inner: Interval::new(lo, hi),
        }
    }

    fn empty() -> Self {
        Self {
            inner: Interval::empty(),
        }
    }

    fn full() -> Self {
        Self {
            inner: Interval::full(),
        }
    }

    fn lo(&self) -> f64 {
        self.inner.lo
    }

    fn hi(&self) -> f64 {
        self.inner.hi
    }

    fn is_wraparound(&self) -> bool {
        !self.is_empty() && self.inner.width() < 0.0
    }

    fn intersects_interval(&self, other: &Self) -> bool {
        let (left, right) = self.split();
        let (other_left, other_right) = other.split();
        left.intersects_interval(&other_left)
            || left.intersects_interval(&other_right)
            || right.intersects_interval(&other_left)
            || right.intersects_interval(&other_right)
    }

    fn width(&self) -> f64 {
        if self.is_wraparound() {
            f64::INFINITY
        } else {
            self.inner.width()
        }
    }

    fn mid(&self) -> f64 {
        if self.is_wraparound() {
            f64::INFINITY
        } else {
            self.inner.mid()
        }
    }

    fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    fn merge_interval(&self, other: &Self) -> Self {
        if self.is_empty() {
            return *other;
        }

        if other.is_empty() {
            return *self;
        }

        let (wraparound, not_wraparound) = match (self.is_wraparound(), other.is_wraparound()) {
            // Handle wraparound/not wraparound below
            (true, false) => (self, other),
            (false, true) => (other, self),
            // Both are wraparound: Merge the two left intervals, then merge the two right intervals
            // and check if we need the full interval
            (true, true) => {
                let (left, right) = self.split();
                let (other_left, other_right) = other.split();

                let new_left = left.merge_interval(&other_left);
                let new_right = right.merge_interval(&other_right);

                // If the left and right intervals intersect each other, we need the full
                // interval
                if new_left.intersects_interval(&new_right) {
                    return WraparoundInterval::full();
                } else {
                    return WraparoundInterval::new(new_right.lo(), new_left.hi());
                }
            }
            // Neither are wraparound: just merge the inner intervals
            (false, false) => {
                return Self {
                    inner: self.inner.merge_interval(&other.inner),
                };
            }
        };

        let (left, right) = wraparound.split();
        let distance_not_wraparound_left = (not_wraparound.mid() - left.hi()).abs();
        let distance_not_wraparound_right = (not_wraparound.mid() - right.lo()).abs();
        let (new_left, new_right) = if distance_not_wraparound_left < distance_not_wraparound_right
        {
            (left.merge_interval(&not_wraparound.inner), right)
        } else {
            (left, right.merge_interval(&not_wraparound.inner))
        };

        // If the left and right intervals intersect each other, we need the full
        // interval
        if new_left.intersects_interval(&new_right) {
            WraparoundInterval::full()
        } else {
            WraparoundInterval::new(new_right.lo(), new_left.hi())
        }
    }
}

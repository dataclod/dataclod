use datafusion::common::{DataFusionError, Result};
use fastrand::Rng;

use crate::geometry::bounding_box::BoundingBox;

/// Multi-stage bounding box sampling algorithm for spatial join partitioning.
///
/// # Purpose
///
/// This sampler is designed to collect a representative sample of bounding
/// boxes from a large spatial dataset in a single pass, without knowing the
/// total count beforehand. The samples are used to build high-quality spatial
/// partitioning grids for out-of-core spatial joins, where datasets may be too
/// large to fit entirely in memory.
///
/// # Goals
///
/// 1. **Uniform Sampling**: Collect samples that faithfully represent the
///    spatial distribution of the entire dataset, ensuring good partitioning
///    quality.
///
/// 2. **Memory Bounded**: Never exceed a maximum sample count to avoid running
///    out of memory, even for very large datasets.
///
/// 3. **Minimum Sample Guarantee**: Collect at least a minimum number of
///    samples to ensure partitioning quality, even for small datasets.
///
/// 4. **Single Pass**: Collect samples in one pass without requiring prior
///    knowledge of the dataset size, which is critical for avoiding expensive
///    double-scans in out-of-core scenarios.
///
/// 5. **Target Sampling Rate**: Maintain samples at a target sampling rate
///    before hitting the maximum sample count, balancing between sample quality
///    and memory usage.
///
/// # Algorithm Stages
///
/// The algorithm uses a combination of reservoir sampling and Bernoulli
/// sampling across 4 stages:
///
/// 1. **Stage 1 - Filling the small reservoir** (k < `min_samples)`: Simply
///    collect all bounding boxes to ensure we have enough samples for small
///    datasets.
///
/// 2. **Stage 2 - Small reservoir sampling** (`min_samples` ≤ k < `min_samples`
///    / `target_sampling_rate)`: Use reservoir sampling to maintain exactly
///    `min_samples` samples while keeping the sampling rate above the target.
///
/// 3. **Stage 3 - Bernoulli sampling** (k ≥ `min_samples` /
///    `target_sampling_rate` && |samples| < `max_samples)`: The reservoir can
///    no longer guarantee the target sampling rate. Use Bernoulli sampling at
///    the target rate to grow the sample set.
///
/// 4. **Stage 4 - Large reservoir sampling** (|samples| = `max_samples)`: We've
///    hit the memory limit. Use reservoir sampling to maintain exactly
///    `max_samples`, preventing memory overflow.
///
/// This multi-stage approach ensures uniform sampling across all stages while
/// satisfying all the goals above.
#[derive(Debug)]
pub struct BoundingBoxSampler {
    /// Minimum number of samples to collect
    min_samples: usize,

    /// Maximum number of samples to collect
    max_samples: usize,

    /// Target sampling rate (minimum sampling rate before hitting
    /// `max_samples`)
    target_sampling_rate: f64,

    /// The threshold count for switching from stage 2 to stage 3
    /// (`min_samples` / `target_sampling_rate`)
    reservoir_sampling_max_count: usize,

    /// The collected samples
    samples: Vec<BoundingBox>,

    /// The number of bounding boxes seen so far
    population_count: usize,

    /// Random number generator (fast, non-cryptographic)
    rng: Rng,
}

/// Samples collected by [`BoundingBoxSampler`].
#[derive(Debug, PartialEq, Clone, Default)]
pub struct BoundingBoxSamples {
    /// The collected samples
    samples: Vec<BoundingBox>,

    /// The size of population where the samples were collected from
    population_count: usize,
}

impl BoundingBoxSampler {
    /// Create a new [`BoundingBoxSampler`]
    ///
    /// # Arguments
    /// * `min_samples` - Minimum number of samples to collect (corresponds to
    ///   minNumSamples in Java)
    /// * `max_samples` - Maximum number of samples to collect (corresponds to
    ///   maxNumSamples in Java)
    /// * `target_sampling_rate` - Target sampling rate (corresponds to
    ///   minSamplingRate in Java)
    /// * `seed` - Seed for the random number generator to ensure reproducible
    ///   sampling
    ///
    /// # Errors
    /// Returns an error if:
    /// - `min_samples` is 0
    /// - `max_samples` is less than `min_samples`
    /// - `target_sampling_rate` is not in the range (0, 1]
    pub fn try_new(
        min_samples: usize, max_samples: usize, target_sampling_rate: f64, seed: u64,
    ) -> Result<Self> {
        if min_samples == 0 {
            return Err(DataFusionError::Plan(
                "min_samples must be positive".to_owned(),
            ));
        }
        if max_samples < min_samples {
            return Err(DataFusionError::Plan(
                "max_samples must be >= min_samples".to_owned(),
            ));
        }
        if target_sampling_rate <= 0.0 || target_sampling_rate > 1.0 {
            return Err(DataFusionError::Plan(
                "target_sampling_rate must be in (0, 1]".to_owned(),
            ));
        }

        let reservoir_sampling_max_count = (min_samples as f64 / target_sampling_rate) as usize;

        Ok(Self {
            min_samples,
            max_samples,
            target_sampling_rate,
            reservoir_sampling_max_count,
            samples: Vec::with_capacity(min_samples),
            population_count: 0,
            rng: Rng::with_seed(seed),
        })
    }

    /// Add a bounding box and update the samples using the multi-stage sampling
    /// algorithm
    pub fn add_bbox(&mut self, bbox: &BoundingBox) {
        self.population_count += 1;

        if self.samples.len() < self.min_samples {
            // Stage 1: Filling the small reservoir
            self.samples.push(bbox.clone());
        } else if self.population_count <= self.reservoir_sampling_max_count {
            // Stage 2: Small reservoir sampling
            // Use reservoir sampling to keep min_samples samples
            let index = self.rng.usize(..self.population_count);
            if index < self.min_samples {
                self.samples[index] = bbox.clone();
            }
        } else if self.samples.len() < self.max_samples {
            // Stage 3: Bernoulli sampling
            // The reservoir cannot guarantee the minimum sampling rate
            if self.rng.f64() < self.target_sampling_rate {
                self.samples.push(bbox.clone());
            }
        } else {
            // Stage 4: Large reservoir sampling
            // We've reached the maximum number of samples
            let index = self.rng.usize(..self.population_count);
            if index < self.max_samples {
                self.samples[index] = bbox.clone();
            }
        }
    }

    /// Estimate the maximum amount memory used by this sampler
    pub fn estimate_maximum_memory_usage(&self) -> usize {
        self.max_samples * size_of::<BoundingBox>()
    }

    /// Consume the sampler and return the collected samples
    pub fn into_samples(self) -> BoundingBoxSamples {
        BoundingBoxSamples {
            samples: self.samples,
            population_count: self.population_count,
        }
    }
}

impl BoundingBoxSamples {
    /// Create an empty bounding box samples
    pub fn empty() -> Self {
        Self {
            samples: Vec::new(),
            population_count: 0,
        }
    }

    /// Move samples out and consume this value
    pub fn take_samples(self) -> Vec<BoundingBox> {
        self.samples
    }

    /// Size of the population
    pub fn population_count(&self) -> usize {
        self.population_count
    }

    /// Actual sampling rate
    pub fn sampling_rate(&self) -> f64 {
        if self.population_count() == 0 {
            0.0
        } else {
            self.samples.len() as f64 / self.population_count() as f64
        }
    }

    /// Combine 2 samples into one. The combined samples remain uniformly
    /// sampled from the combined population.
    pub fn combine(self, other: BoundingBoxSamples, rng: &mut Rng) -> BoundingBoxSamples {
        if self.population_count() == 0 {
            return other;
        }
        if other.population_count() == 0 {
            return self;
        }

        let self_sampling_rate = self.sampling_rate();
        let other_sampling_rate = other.sampling_rate();
        if self_sampling_rate > other_sampling_rate {
            return other.combine(self, rng);
        }
        if other_sampling_rate <= 0.0 {
            return BoundingBoxSamples {
                samples: Vec::new(),
                population_count: self.population_count() + other.population_count(),
            };
        }

        // self has smaller sampling rate. We need to subsample other to
        // make both sides having the same sampling rate
        let subsampling_rate = self_sampling_rate / other_sampling_rate;
        let mut samples = self.samples;
        for bbox in other.samples {
            if rng.f64() < subsampling_rate {
                samples.push(bbox);
            }
        }
        BoundingBoxSamples {
            samples,
            population_count: self.population_count + other.population_count,
        }
    }
}

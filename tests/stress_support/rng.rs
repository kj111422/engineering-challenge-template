//! Deterministic SplitMix64 pseudo-random number generator.
//!
//! The stress harness generates every workload from one explicit, versioned
//! algorithm so that a run is reproducible from its seed alone — across
//! machines and Rust releases. `rand` is available in the dependency tree but
//! is unsuitable here: its algorithms and output order are not a stable
//! contract we want to version as `prng_algorithm_version`.
//!
//! Reference: Steele, Lea & Flood, "Fast splittable pseudorandom number
//! generators" (OOPSLA 2014), public-domain SplitMix64 definition:
//!
//! ```text
//! state = state.wrapping_add(0x9E3779B97F4A7C15)
//! z = state
//! z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9)
//! z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB)
//! output = z ^ (z >> 31)
//! ```
//!
//! `below(limit)` maps to `[0, limit)` with multiply-high reduction, never
//! modulo bias. All tests in this module pin the algorithm's exact output
//! sequence; changing any constant is a new `prng_algorithm_version`.

/// Algorithm version of the generator. Bump when the algorithm changes.
pub const PRNG_ALGORITHM_VERSION: u32 = 1;

/// One deterministic counter-based SplitMix64 stream.
#[derive(Debug, Clone)]
pub struct SplitMix64 {
    state: u64,
}

impl SplitMix64 {
    #[must_use]
    pub fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    #[must_use]
    pub fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Uniform value in `[0, limit)`. `limit == 0` yields `0`.
    #[must_use]
    pub fn below(&mut self, limit: u64) -> u64 {
        if limit == 0 {
            return 0;
        }
        ((u128::from(self.next_u64()) * u128::from(limit)) >> 64) as u64
    }

    /// Uniform value in `[0, limit)` for `usize` limits.
    #[must_use]
    pub fn below_usize(&mut self, limit: usize) -> usize {
        usize::try_from(self.below(limit as u64)).unwrap_or(usize::MAX)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn splitmix64_output_sequence_is_pinned() {
        let mut rng = SplitMix64::new(0);
        let output: Vec<u64> = (0..4).map(|_| rng.next_u64()).collect();
        assert_eq!(
            output,
            vec![
                0xE220_A839_7B1D_CDAF,
                0x6E78_9E6A_A1B9_65F4,
                0x06C4_5D18_8009_454F,
                0xF88B_B8A8_724C_81EC,
            ]
        );
    }

    #[test]
    fn seeded_sequences_are_reproducible() {
        let mut a = SplitMix64::new(0xDEAD_BEEF);
        let mut b = SplitMix64::new(0xDEAD_BEEF);
        for _ in 0..64 {
            assert_eq!(a.next_u64(), b.next_u64());
        }
    }

    #[test]
    fn below_respects_bounds_and_is_stable() {
        let mut rng = SplitMix64::new(42);
        for _ in 0..1_000 {
            assert!(rng.below(17) < 17);
        }
        let mut first = SplitMix64::new(42);
        let mut second = SplitMix64::new(42);
        for _ in 0..100 {
            assert_eq!(first.below(1_000_000), second.below(1_000_000));
        }
    }

    #[test]
    fn algorithm_version_constant_is_stable() {
        assert_eq!(PRNG_ALGORITHM_VERSION, 1);
    }
}

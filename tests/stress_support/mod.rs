//! Shared support for the deterministic stress laboratory.
//!
//! Everything under `stress_support` exists only for the stress suite: it is
//! not compiled into the collector binary and contributes nothing to
//! production behavior.

pub mod host;
pub mod manifest;
pub mod mock;
pub mod rng;
pub mod runner;
pub mod universe;

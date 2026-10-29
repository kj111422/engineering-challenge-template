# Code provenance

This repository does not claim that a static scan can prove authorship or
exclude all possible similarity to other software. Instead, it records the
origins that a reviewer can inspect and the evidence needed for an honest
submission.

| Path group | Origin and review expectation |
|---|---|
| `sbe_schemas/` | Supplied challenge schema; retain it unchanged unless the challenge requires an update. |
| `src/util/build.rs`, generated SBE bridge | Generated from the supplied schema through the configured build dependency; review generated output as generated material, not original protocol design. |
| `src/util/{aligned_sbe_reader,aligned_sbe_writer}.rs` | Challenge-compatible helper implementation; changes require template-reader compatibility tests. |
| `src/` excluding generated/schema helpers | Project implementation. Nontrivial changes require an invariant, a focused test, and a Git commit explaining the decision. |
| Cargo dependencies | Third-party packages governed by their published licenses; `Cargo.lock` pins the reviewed dependency graph. |

Before external submission, review every copied or adapted snippet against its
source and license, preserve required notices, and disclose automation or AI
assistance where the challenge or employer requires it. Do not rewrite code to
evade similarity detectors: provenance, attribution, and explainable behavior
are the meaningful controls.

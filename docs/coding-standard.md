# Rust engineering standard

**Version:** 1.0
**Status:** normative for this repository.
**Authority:** this document, `Cargo.toml` `[lints]`, `.rustfmt.toml`, `clippy.toml`.
**Enforcement:** `cargo fmt --all -- --check`, `cargo clippy --all-targets --locked -- -D warnings`, `cargo test --release`, review.

## 1. Scope and normative language

This standard governs hand-written Rust in `src/`, `tests/`, `benches/` and `build.rs`. It governs generated code only at the boundary described in §5.

`MUST`, `MUST NOT`, `SHOULD` and `MAY` carry their RFC 2119 meaning. A rule is binding unless the change records an exception under §12. A rule that no tool checks is a review obligation, and the review gate names it.

The repository has one formatting authority and one lint authority. Reviewers do not debate either.

## 2. Tooling and formatting

| ID | Rule |
| --- | --- |
| T1 | Layout is owned by `rustfmt`. `.rustfmt.toml` pins line endings and the edition style; every other option stays at the stable default. Manual alignment and hand-wrapped lines are defects. |
| T2 | `cargo fmt --all -- --check` is a merge gate. A formatting diff is reviewed with `git diff -w`. |
| T3 | Lint severity lives in `Cargo.toml` `[lints]`. Lint thresholds and test allowances live in `clippy.toml`. CI passes `-D warnings`; per-invocation lint flags are not used. |
| T4 | The minimum supported Rust version is declared in `rust-version`, `clippy.toml` `msrv` and `rust-toolchain.toml`. A change to one is a change to all three. |
| T5 | Imports are vertical, one item per line, with braces once a group has more than one item. A trailing `//` holds the layout when `rustfmt` would otherwise collapse the group. This is a review rule; `rustfmt` does not express it. |
| T6 | Formatting is performed by `rustfmt`. A script or a line-oriented rewrite MUST NOT perform formatting. |

## 3. Structure and control flow

| ID | Rule |
| --- | --- |
| S1 | A function performs one named operation. A decoder decodes, a validator validates, a writer persists. |
| S2 | Use early return and `?` instead of nesting. Nesting deeper than three levels requires a named helper. |
| S3 | A function holds at most seven live local bindings. Beyond that, extract a struct that names the state or split the operation. |
| S4 | A function above 100 lines is either a table-driven dispatch or a candidate for splitting. Length alone is not a defect; mixed responsibilities are. |
| S5 | Do not add a trait, generic parameter or lifetime with one implementation unless it is a test seam or a public extension point. |
| S6 | Model domain states with exhaustive enums. A `kind: String` that selects behavior is a defect. |
| S7 | A module owns a capability. `util` is not a destination for code that has no owner. |
| S8 | A change MUST NOT hide discovery, reconciliation, decoding, reconstruction and persistence in one function. |
| S9 | A loop carries one exit condition per named state. A `break` that means "failed", "timed out" and "done" is three states expressed as one. |

## 4. Types, errors and arithmetic

| ID | Rule |
| --- | --- |
| E1 | `Result` for a fallible operation, `Option` for absence. A sentinel integer or empty string is not an error channel. |
| E2 | An error carries the operation, the identifier and the path that failed. An error that only says "invalid" is not actionable. |
| E3 | `unwrap`, `expect`, `panic!`, `todo!`, `unimplemented!`, `dbg!` and `print_stdout!` are prohibited in library and binary code. The lint set enforces this. |
| E4 | `expect` is permitted only where a preceding invariant makes failure impossible, and the message states that invariant. |
| E5 | Counters, prices, sizes and offsets use checked arithmetic. `saturating_*` is permitted only where saturation is the documented semantic; otherwise `checked_*` plus an error. |
| E6 | Fallible conversion uses `TryFrom`; infallible conversion uses `From`. Numeric narrowing uses `try_from`, not `as`. |
| E7 | Identifiers and units are newtypes. A `String` that carries an instrument or a currency is a defect. |
| E8 | Decimal conversion is exact or it is an error. Rounding a value that the consumer treats as exact is a defect. |
| E9 | A public API change carries a compatibility note. `avoid-breaking-exported-api` remains enabled. |
| E10 | Absence has one representation. `None`, `null` and a missing key are not three spellings of the same state in one document. |

## 5. Unsafe code

| ID | Rule |
| --- | --- |
| U1 | `unsafe_code = "deny"` for hand-written code. |
| U2 | Generated code is isolated in a module boundary that names its generator and carries a scoped exemption. |
| U3 | If hand-written unsafe is admitted, every block carries `// SAFETY:` with the invariant, the preconditions and the aliasing argument. `undocumented_unsafe_blocks` becomes `deny` in the same change. |
| U4 | `mem::transmute`, uninitialized memory and pointer arithmetic outside an audited module are prohibited. |
| U5 | A safe wrapper exposes an API that cannot be misused to violate the safety argument. |

## 6. Comments and documentation

| ID | Rule |
| --- | --- |
| D1 | `///` documents intent for readers; `//` records implementation constraints. Both apply to private items that encode a non-obvious invariant. |
| D2 | A comment explains why, or records a constraint the code cannot express. It does not narrate the next statement. |
| D3 | Comment at the head of the item. An inline comment inside a body marks a body that should be split or renamed. |
| D4 | Comments are Markdown, sentence case and end with a period. Tags are `// SAFETY:`, `// TODO(owner):` and `// FIXME(owner):`. |
| D5 | A public fallible function documents `# Errors`; a panicking function documents `# Panics`; an unsafe function documents `# Safety`. |
| D6 | A doc example compiles under `cargo test --doc`. |
| D7 | Commented-out code is deleted. Git retains the history. |
| D8 | A comment that restates the signature is noise and is removed. |

## 7. Naming and terminology

| ID | Rule |
| --- | --- |
| N1 | Public names are descriptive; local names are short and scoped to a bounded body. |
| N2 | No type prefixes and no Hungarian notation. The compiler knows the type. |
| N3 | Use `primary`/`replica` and `allowlist`/`denylist`. Master/slave and blacklist/whitelist are not introduced. |
| N4 | A venue-native concept keeps the venue's term, adapted to Rust case. The item name does not repeat the module path. |
| N5 | A boolean reads as a predicate: `is_`, `has_`, `can_`, `should_`. |
| N6 | A unit that the type does not encode appears in the name (`_ms`, `_us`, `_bytes`) or in a unit newtype. |
| N7 | A module name states what the module owns, not how it is implemented. |

## 8. Lints and suppressions

| ID | Rule |
| --- | --- |
| L1 | A suppression uses `#[expect(lint, reason = "...")]`. `#[allow]` is prohibited in hand-written code. `clippy::allow_attributes` and `clippy::allow_attributes_without_reason` are `deny`. |
| L2 | A reason states the invariant or the boundary that justifies the suppression. "Temporary", "legacy" and "clippy" are not reasons. |
| L3 | An unfulfilled expectation is a build failure. A suppression that stops firing is removed, not relaxed. |
| L4 | A blanket suppression is permitted at a generated-code boundary. It names the generator, is module-scoped and is documented in §5. |
| L5 | A test crate may relax the production set. The relaxation carries a reason and does not extend to `src/`. |
| L6 | `dead_code` is not suppressed to hide an unfinished feature. The code is deleted or carries an `expect` that names the consuming milestone. |
| L7 | A new lint is enabled at `deny` only after the existing tree is clean. Enabling a lint and the cleanup land in one commit. |

## 9. Performance and measurement

| ID | Rule |
| --- | --- |
| P1 | An optimization requires a measurement: command, environment, before and after. |
| P2 | `#[inline]` requires a profile. The compiler already inlines within a crate. |
| P3 | Allocation, copy and lock contention on a hot path require a measurement. |
| P4 | Queues are bounded and backpressure is explicit. An unbounded channel requires a written justification. |
| P5 | No busy-wait. Use the runtime timer or a condition variable. |
| P6 | A benchmark states workload, duration, warm-up and variance. A single sample is not a benchmark. |
| P7 | A harness that becomes the bottleneck classifies the run as harness-limited. A harness-limited run is not collector-capacity evidence. |

## 10. Tests and evidence

| ID | Rule |
| --- | --- |
| V1 | A defect fix lands with a regression test that fails on the parent commit. |
| V2 | Tests are deterministic: seeded RNG, controlled time, loopback only, no wall-clock threshold. |
| V3 | Every fail-closed branch has a failure-path test. |
| V4 | An artifact generator is `#[ignore]`d and named `reference_*`. CI runs the comparison, not the generator. |
| V5 | A success verdict requires all of: required metrics present, stage accounting reconciled, manifest fields complete, publication verified, evidence hashes recorded. |
| V6 | A mock establishes harness behavior, not venue behavior. The manifest records the evidence class. |
| V7 | A claim in a commit, pull request or document carries the command and output that support it. |
| V8 | An unavailable value is `null` plus a reason. It is not `0`, `false` or an empty string. |

## 11. Review and merge gates

| ID | Rule |
| --- | --- |
| G1 | `cargo fmt --all -- --check`, `cargo clippy --all-targets --locked -- -D warnings` and `cargo test --release` pass. |
| G2 | A new public item has documentation and, where fallible, an `# Errors` section. |
| G3 | A new dependency carries a license, MSRV, maintenance and binary-size justification. |
| G4 | A style change and a behavior change use separate commits. |
| G5 | A changed file is brought to this standard; an untouched file is not reformatted. |
| G6 | A generated artifact records the source commit and the binary hash. Regeneration runs with `STRESS_REQUIRE_CLEAN=1`. |
| G7 | A finding is closed by evidence in the tree, not by a prose assertion in the pull request. |

## 12. Exceptions and migration

| ID | Rule |
| --- | --- |
| X1 | An exception is a scoped `expect`, a lint configuration entry or a written waiver in the pull request. It names the rule ID, the owner and the condition that removes it. |
| X2 | A permanent exception requires a tracking issue. |
| X3 | An exception that outlives its condition is a defect. |
| X4 | A repository-wide rewrite requires a semantic-preservation argument and a tool that parses Rust. Preference is not such an argument. |
| X5 | A rewrite that changes every file invalidates artifact provenance. That cost is weighed before the rewrite, not after. |

## 13. Upstream references

The Rust API guidelines, the Rustonomicon, the Clippy lint catalogue and the rustfmt configuration reference answer questions this document does not cover.

The Linux kernel's Rust guidance and its `.rustfmt.toml` are prior art for two decisions in this standard: formatting is tool-owned, and a suppression that can go stale is written as an `expect`. They are upstream references, not this repository's policy. Where they differ from `rustfmt` defaults, `rustfmt` is authoritative.

## 14. Adoption record

Applied in the commit that introduced this standard:

- `clippy::allow_attributes` and `clippy::allow_attributes_without_reason` are denied. Every hand-written suppression in `src/` is an `expect` with a reason.
- The migration removed two suppressions that no longer fired: the crate-level `print_stdout` allowance in `src/util/build.rs` and the `too_many_arguments` allowance on `session::handle_control`.
- The build-generated SBE codec carries one documented, module-scoped exemption because generated code cannot carry reasons.
- Test crates state why they relax the production lint set.
- `scripts/annotate_test_lint_reasons.py` is the enumerated, compiler-verified transform used for the test-crate headers. It matches two exact shapes and refuses any other input.

Deferred, with a stated condition:

- Vertical import layout across the tree. This is a `rustfmt` operation, but it touches most files, so it lands in a dedicated commit whose `git diff -w` is empty apart from import blocks.
- `clippy::undocumented_unsafe_blocks`. No hand-written unsafe remains to document; enabling it now would flag only the generated codec. It becomes relevant if `unsafe_code = "deny"` is relaxed.

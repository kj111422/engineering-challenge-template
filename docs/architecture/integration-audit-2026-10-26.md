# Architecture integration audit — 2026-10-26
## Scope and disposition

Audited input: `engineering-challenge-2026-10-26.zip`, SHA-256:

`1a8d6e3d7ba53e3369d3cbac854405d96451dfd4a00a1b274e39b9c17c51188d`

The ZIP contains no `.git` directory. Findings are tied to this exact source snapshot, not to a remote branch tip or a commit. **Disposition (at time of audit): integration blocked; do not describe the archive as a verified build or a production-ready runtime.** This review environment has Python 3 and Node.js, but no `cargo` or `rustc`; no Rust build/test result is claimed.

> **Note (2026-10-27):** The current codebase in this repository has since integrated the three draft modules (`capacity`, `catalog`, `persistence`). They are now declared in `src/lib.rs`, wired into the coordinator, and all tests pass (185 passing, 3 ignored). The architecture gate (`scripts/check_architecture.py --check`) passes. The findings below describe the state of the audited ZIP snapshot only and do not reflect the current repository state.

## Findings

### P0 — Runtime module graph and implementation are inconsistent

- `src/lib.rs:1-21` declares the library modules but does not declare `capacity`, `catalog`, or `persistence`.
- `src/metrics.rs:23-28` imports `persistence::PersistenceStatus` through `crate`, so the active metrics module refers to a module absent from the library module graph.
- `src/coordinator.rs` still calls `MetricsRegistry::new(exchange.name())`, while `src/metrics.rs` defines `MetricsRegistry::new(exchange, policy)` and requires `ReadinessPolicy`.
- `src/coordinator.rs::update_metrics` calls `registry.update(core.stats(), writer.stats(), core.statuses())`, while `src/metrics.rs::MetricsRegistry::update` also requires a fourth `RuntimeStatus` argument.

These conflicts are visible directly in source. A compiler run was not available to produce a compiler diagnostic, but the mismatched module/API declarations mean the source snapshot is not a coherent compiled architecture.

### P1 — Capacity draft is not connected to startup and lacks required settings

`src/capacity.rs` defines `plan` and estimates connection count, initial subscription size, book memory, and snapshot output rate. It is not reachable from either application crate root and `src/coordinator.rs` does not call it. Further, the capacity source reads these fields from `CollectorSettings`, but they do not appear in the struct in `src/config.rs`:

- `max_total_connections`
- `max_initial_subscription_bytes`
- `max_estimated_book_memory_mb`
- `max_snapshot_bytes_per_sec`

The active coordinator path uses its own `derive_shard_count` and enforces shard/per-socket limits only. The broader capacity ceilings must not be represented as active preflight gates.

### P1 — Catalog draft depends on missing modules/helpers

`src/catalog.rs` is not reachable from a crate root. It imports `crate::capacity::count_selected_markets` and `util::sha256_hex`; the `capacity` module is not declared, and no `sha256_hex` definition exists under `src/util/`. Merely adding `pub mod catalog;` is not a complete integration fix.

### P1 — Background persistence draft is not used by the coordinator

`src/persistence.rs` is not reachable from a crate root. The live path in `src/coordinator.rs` writes metadata using `MarketMetadata::write` directly on initial discovery, accepted discovery, and shutdown. The latest-wins writer's source tests are in the unrooted file and are not compiled by ordinary Cargo targets. Until integrated, the architecture must not claim non-blocking metadata/catalog persistence or checkpoint-based bootstrap.

### P1 — The generated test inventory was stale and overcounted executable coverage

After regeneration, the static inventory records 159 test-attributed function declarations. Of these, 15 occur in the three unrooted source files (`capacity.rs`, `catalog.rs`, `persistence.rs`) and are marked `NOT COMPILED`. The remaining 144 declarations are reachable through a crate root or an integration-test file, but this does not prove that they compile or pass. The README and dated verification record must be reconciled only after the exact source tree builds and its tests are run.

### P1 — Output lifecycle remains unsafe for unattended existing paths

`src/util/aligned_sbe_writer.rs` uses `File::create`, which truncates an existing output file. `validate_output_paths` in `src/config.rs` prevents path aliasing and directory destinations; it does not reject an existing capture path. No segment rotation/retention implementation is wired into `src/writer.rs`. Rotation-related metric labels are not evidence of an implemented rotation lifecycle.

### P1 — Readiness implementation and producer are not aligned

`src/metrics.rs` defines `/livez`, `/readyz`, coverage/catalog/persistence fields, and a `RuntimeStatus` input. The coordinator does not pass that input, and currently uses the old `MetricsRegistry` method signatures. Therefore the complete readiness path—from observed books, catalog freshness, persistence health, and writer health through the coordinator to HTTP status—is not wired as a consistent runtime contract in this snapshot.

## Verification performed

| Check | Result | Interpretation |
|---|---|---|
| ZIP integrity (`unzip -t`) | Passed | The uploaded archive is readable. |
| Source root traversal | 38 of 41 Rust files reachable; 3 unrooted | The unrooted files are `capacity.rs`, `catalog.rs`, `persistence.rs`. The build script is counted as a separate crate root. |
| Architecture Mermaid structural checker | 9 diagrams; no structural errors | This validates node/edge structure, not full Mermaid grammar. |
| Architecture checker unit suite | 7 passed | Includes regression tests for declaring modules inactive rather than counting their tests as compiled. |
| `scripts/check_architecture.py --check` | **Fails** on integration mismatches above | It must not produce a green architecture gate for this snapshot. |
| `cargo fmt`, Clippy, Rust tests, release build | Not run | `cargo` and `rustc` are unavailable in this review environment. |
| Pinned Mermaid parser action | Not run | Requires GitHub Actions; a local structural check is not a substitute. |

## Required integration order

1. **Reconcile crate roots and APIs.** Decide whether to complete or remove the three draft modules. If completing them, expose the modules in the appropriate crate root and resolve the `MetricsRegistry` constructor/update API mismatch first.
2. **Complete capacity dependencies.** Add/validate the intended capacity settings, connect `capacity::plan` after discovery but before output files or source workers are created, and make the plan part of startup logs/reports. Test every ceiling at below/equal/above boundaries through an active Cargo target.
3. **Complete checkpoint dependencies.** Provide a stable, tested checksum helper; wire `Catalog` load/save into a documented cold/warm/stale bootstrap policy; reject exchange mismatch, checksum mismatch, unsupported schema, and invalid age before subscribing.
4. **Connect the persistence worker.** Publish immutable accepted catalog snapshots after complete discovery rounds, route metadata/catalog writes through `PersistenceHandle`, surface errors and timings into `RuntimeStatus`, and wait for final persistence during bounded shutdown.
5. **Close the readiness loop.** Give the coordinator a single typed runtime-status builder that reports book coverage/freshness, last accepted discovery time, catalog age/state, persistence status, writer errors, and fatal state; test HTTP code/body for healthy, starting, degraded, and failed states.
6. **Make output paths safe.** Reject existing captures by default or require an explicit overwrite option. If rotation is added, implement segments, manifest/finalization, restart behavior, disk-space/error handling, and reader/verify support before emitting rotation metrics.
7. **Rebuild evidence on the integrated tree.** Run `cargo fmt --all -- --check`, `cargo clippy --all-targets --locked -- -D warnings`, `cargo test --all-targets --locked`, `cargo test --doc --locked`, `cargo build --release --locked`, then the opt-in live Polymarket test separately. Update the requirement matrix and dated verification record from those actual results.
8. **Regenerate architecture inventories and validate Mermaid.** Run `python3 scripts/check_architecture.py --update` and inspect changes; then require `python3 scripts/check_architecture.py --check` and the pinned Mermaid parser workflow to pass. Do not weaken the gate to make the current source snapshot appear healthy.

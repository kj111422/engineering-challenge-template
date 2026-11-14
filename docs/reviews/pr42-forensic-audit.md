# PR #42 — forensic audit (M1.1)

| Field | Value |
| --- | --- |
| Repository | `siebengestirn/engineering-challenge-template` |
| PR | #42 `feat/full-universe-stress-lab` (open, draft) |
| Audited commit | `c0eaea24718930510eb3341a5558c0b8f9bd3c60` |
| Base | `b66fdbda73e528b6560bde1926838cbfa7e9b1c3` |
| Worktree state at audit | clean, `HEAD == origin/feat/full-universe-stress-lab` |
| Files read | `tests/stress_support/{runner,manifest,mock,universe,rng,host}.rs`, `tests/full_universe_stress.rs`, `src/{writer,metrics}.rs`, `src/collector/state.rs` (delta path), `docs/stress-lab-design.md`, `measurements/stress/*` |
| Evidence class of this audit | static read of the exact commit + the committed reference manifests |

## Executed evidence (not taken from the PR description)

```
$ cargo test --release
```

| Target | Passed | Ignored |
| --- | --- | --- |
| unittests `src/lib.rs` | 323 | 0 |
| unittests `src/main.rs` | 1 | 0 |
| `coverage_certificate` | 7 | 0 |
| `e2e_mock_exchange` | 12 | 1 |
| `egress_lab` | 19 | 0 |
| `full_universe_stress` | 22 | 5 |
| `inspect_capture` | 9 | 0 |
| `kalshi_lifecycle_plane` | 4 | 0 |
| `live_kalshi` | 0 | 1 |
| `live_polymarket` | 0 | 2 |
| `numeric_model` | 10 | 0 |
| `replay_fixture` | 2 | 0 |
| `session_recovery` | 4 | 0 |
| doc-tests | 1 | 0 |
| **Total** | **414** | **9** |

The reported "413 tests" is accurate (413 + 1 doc test). The suite passes on this machine (aarch64-apple-darwin, Apple M1, release profile); the entire `full_universe_stress` target — 22 tests including the 4 000-symbol smoke — completes in ~10 s. **None of the findings below is a failing test.** They are properties the suite does not currently test.

The five ignored tests are the reference-artifact generators, so CI never regenerates or compares a manifest — which is why P-09 and P-10 survive a green build.

## Verdict

**Do not merge as-is, and do not treat `run_status: success` as a certificate yet.**

The increment is a genuine improvement: the acceptance verdict now lives inside `run_cell`, the anchored set is snapshot-only, the mock is bounded, provenance is recorded, and the index fails closed on malformed JSON. Those are real.

But the central claim — *a success manifest proves every event was accounted for* — does not hold at this commit. Five independent mechanisms can produce a `success` artifact that is false, and the manifest omits fields that its own design document marks as required. The gap is not one bug; it is that acceptance covers *what the harness could observe* and not *what the harness promised to observe*.

The findings below are ordered by whether they can manufacture a false success (S1), whether they leave a required claim unproven (S2), whether they are defects without false success (S3), or conformance/hygiene (S4).

## Summary

| ID | Sev | One-line |
| --- | --- | --- |
| P-01 | S1 | Index failure leaves an on-disk `success` manifest with no index entry |
| P-02 | S1 | `read_index` treats *every* I/O error as an empty index, then overwrites it |
| P-03 | S1 | Drain barrier is fail-open: missing counters read as `0`, comparisons are `>=` |
| P-04 | S1 | Ledger cannot see over-application; `saturating_sub` + no `expected_applied` check |
| P-05 | S1 | Manifest completeness is not part of acceptance; `success` ships with required fields absent |
| P-06 | S2 | Ledger collapses six stages into two residuals; design §4 counters not implemented |
| P-07 | S2 | Journal record count is recorded but never reconciled |
| P-08 | S2 | No receive/decode evidence; `sink.send()` completion is the only transmit proof |
| P-09 | S2 | Reference artifacts were produced at `5c9e45a`, not the PR head, with the clean gate off |
| P-10 | S2 | `reference_success_manifest_is_reproducible` compares nothing |
| P-11 | S3 | Index upsert is an unlocked read-modify-write: concurrent cells lose entries |
| P-12 | S3 | Predictable shared temp filename for manifests |
| P-13 | S3 | Rename without fsync of file or directory: crash can leave no artifact |
| P-14 | S3 | Mock silently drops frames on a closed connection, uncounted |
| P-15 | S3 | Injected-drop expectation assumes every drop lands on every replica |
| P-16 | S3 | `queue_high_water` maps "absent" to `0` via `#[serde(default)]` |
| P-17 | S3 | Metric scraper folds labelled families by last sample and skips unparseable lines |
| P-18 | S4 | CI smoke covers one workload; design §3 requires four |
| P-19 | S4 | No SIGINT child-process shutdown test; the fixture is in-process |
| P-20 | S4 | No disk ceiling; `STRESS_DISK_CEILING` exists only in the design doc |
| P-21 | S4 | Mock saturation is classified `failed`, not `mock_throughput_limited` |
| P-22 | S4 | RSS is whole-test-process, not collector-resident |
| P-23 | S4 | `schema_version` duplicated as a literal in two modules |

---

## S1 — can manufacture a false success

### P-01 — A failed index update leaves a `success` manifest on disk

- **Location:** `tests/stress_support/runner.rs:673-703`
- **What it claims:** "an unverifiable artifact is never presented as success" (`runner.rs:630-634`).
- **Defect:** `write_atomic(&out_dir.join(<cell>.json))` runs at line 674, *before* `upsert_cell` at line 693. If the upsert fails, the function returns `manifest_index_error` in memory, but the file already written says `"run_status": "success"`. The artifact survives the failure it was supposed to be gated by.
- **Failure scenario:** two cells run concurrently; cell B's upsert fails (P-11). Cell A's manifest file is on disk, absent from the index, and self-declares success. A later reader that globs `measurements/stress/*.json` instead of reading the index sees a passing cell that was never indexed.
- **Required fix:** stage the manifest in memory, upsert the index first, and publish the body only after the index entry is durable — or write the body to a quarantine name (`unindexed-<run_id>.json`) on index failure and mark it `run_status: failed`.
- **Regression test:** inject an index write failure (read-only directory after validation, or a sink that fails on rename) and assert (a) no file whose body says `success` exists, (b) the failure is recorded somewhere.

### P-02 — `read_index` swallows every I/O error as "empty"

- **Location:** `tests/stress_support/manifest.rs:52-74`, arm at line 72
- **What it claims:** "A missing or empty file starts fresh; a malformed or non-object document is an error" (lines 50-51).
- **Defect:** the final match arm is `_ => Ok(Map::new())`. That arm catches `Ok(empty)` **and every `Err(_)`**, including `PermissionDenied`, `EISDIR`, and I/O errors. A transient permission change silently converts "cannot read the index" into "index is empty", and `upsert_cell` then writes a fresh index containing only the current cell — destroying the record of every other cell.
- **Failure scenario:** `chmod 000 measurements/stress/index.json` (or a full disk on read, or a directory in place of the file). Validation passes, the cell is published as success, and the index now contains one entry. All prior evidence disappears. This is the fail-open path the malformed-JSON test (`tests/full_universe_stress.rs:392`) does *not* cover.
- **Required fix:** match `ErrorKind::NotFound` (and empty content) explicitly; propagate every other error.
- **Regression test:** make the index unreadable (mode 000 on Unix) and assert the cell fails closed with `manifest_index_error` and the file bytes are untouched — the same assertions as the malformed-JSON fixture.

### P-03 — The drain barrier is fail-open on missing metrics

- **Location:** `tests/stress_support/runner.rs:580-616` (`capture_baseline`), `829-870` (barrier)
- **Defect:** `capture_baseline` reads the applied and writer-message counters with `.unwrap_or(0.0)` (lines 588, 601-602, 606). The barrier then requires `deltas >= base.deltas_emitted + expected` and `messages >= base.writer_messages + expected` (lines 852-853). Depth and errors default to `INFINITY` (correctly fail-closed), but the two counters that carry the proof default to zero.
- **Failure scenario:** the applied counter is renamed, removed, or fails one scrape at baseline time. Baseline becomes `0`, the absolute counter is already at, say, 8 200 from the snapshot phase, and `8 200 >= 0 + 200` passes instantly — the barrier certifies before a single delta is applied. The same shape applies to a partial scrape (P-17).
- **Required fix:** a fixed set of required metrics read as `Option<f64>`; absence is `event_accounting_unavailable`, never zero. Distinguish counter *reset* from counter *absence* explicitly.
- **Related misclassification:** `drain_incomplete` is used both for deadline expiry (line 868) and for "baseline unavailable" (line 828), and `drain_incomplete` is in `TIMEOUT_CLASSES` (`runner.rs:168`). A metrics-availability failure is therefore reported as `run_status: incomplete`, which implies "retryable", not "the evidence path is broken".
- **Regression test:** remove one required metric from `/metrics` (or point the runner at a stub registry missing it) and assert the cell is `failed`/`incomplete`, never `success`.

### P-04 — The ledger cannot detect over-application or duplicate application

- **Location:** `tests/stress_support/runner.rs:1008-1013`, `EventLedger::reconciled` at `211-213`
- **Defect:** `unexplained_drops = generated.saturating_sub(applied)` and `persistence_gap = applied.saturating_sub(sbe)`. Saturating arithmetic clamps every negative difference to zero, and `reconciled()` never compares `applied` with `expected_applied`. A run that applies *more* than it generated is indistinguishable from a clean one.
- **Evidence in the committed artifact:** `measurements/stress/seed-0007-4000-uniform.json` records `mock_transport.deltas_sent: 400`, `applied_events: 200`, and no counter for the 200 replica duplicates. The balance holds only because the active-source rule happens to apply exactly one copy. If duplicate suppression regressed, `applied` would be 400 and `unexplained_drops` would still be `0`.
- **Failure scenario:** a dedup regression at 2 replicas. Every delta is applied twice; the ledger reports `unexplained_drops: 0`, `persistence_gap: 0`, `reconciled: true`.
- **Required fix:** make every derived gap two-sided (`applied - expected` checked in both directions), and count suppressed replica duplicates as a first-class counter (design §4 `suppressed_replica_duplicates`).
- **Regression test:** a fixture that publishes each delta to both replicas with dedup disabled must fail with `duplicate_application`, not pass with `reconciled: true`.

### P-05 — `success` does not imply the manifest is complete

- **Location:** `tests/stress_support/runner.rs:1229-1440` vs `docs/stress-lab-design.md:170-208`
- **Defect:** §8 lists required fields. Compared with the emitted document (`seed-0007-4000-uniform.json`, 25 top-level keys), these are **absent entirely**: `configuration_sha256`, `collector_binary_sha256`, `collector { desired_topology, actual_topology, capacity_plan }`, `disk { ... }`, `incidents { ... }`, `coverage.per_epoch`. `grep` confirms the names appear only in the design document, never in code. `performance` is present but every field is `unavailable`.
- **Failure scenario:** a reviewer reads `run_status: success` and concludes the run satisfied the manifest contract. It did not: there is no configuration identity, no collector binary identity, no disk accounting, and no incident block. The success predicate and the manifest contract are two different documents that were never reconciled.
- **Note on `incidents`:** the core already counts continuity gaps (`discontinuity_incidents`, `unquantifiable_gaps` — `src/collector/state.rs:1852-1855`, `src/metrics.rs:99-102`), but neither is exported as a metric, so the harness cannot scrape them. Populating §8 `incidents` is not a harness-only change; it needs a production metric first.
- **Required fix:** either implement the §8 fields or amend §8 to the implemented subset *and* make the acceptance predicate assert the presence of every field the design still requires. Acceptance must fail on a missing required field, not emit `unavailable`.
- **Regression test:** validate every emitted manifest against a JSON schema derived from §8 inside the runner; a missing required field fails the cell.

---

## S2 — required claims that remain unproven

### P-06 — Six-stage accounting collapsed into two residuals

- **Location:** `tests/stress_support/runner.rs:186-214`
- **Defect:** design §4 requires `generated → transmitted → received → decoded → applied → deduped → written → persisted → journaled`, with six mutually exclusive discrepancy classes. `EventLedger` has `generated`, `applied`, `sbe_deltas`, `journaled_records`, two injected counters, and two residuals. `received_frames` and `decoded_frames` do not exist anywhere in the tree.
- **Consequence, proven by a committed artifact:** `measurements/stress/failed-seed-000e-200-coverage_set_mismatch.json` records `queue_saturated: 1386`, `enqueued: 17`, `applied: 0`, and `unexplained_drops: 500`. All 500 were never transmitted — a *transport* failure — yet the ledger files them as unexplained application drops. A residual cannot name its stage, so it cannot be reviewed.
- **Required fix:** stage counters plus a single classification enum per lost logical event (`availability_gap | overflow | replica_transport_duplicate | persistence_failure | order_violation | unexplained_drops`), as §4 already specifies.
- **Regression test:** the existing saturation fixture must produce `availability_gap: 500, unexplained_drops: 0`, which is the opposite of what it produces today.

### P-07 — The journal stage is recorded but never reconciled

- **Location:** `tests/stress_support/runner.rs:971-988`, `1001-1014`
- **Defect:** `journaled_records` is captured and written to the manifest, and `journal_complete` is a boolean from the verifier, but nothing compares the record count with anything. In the reference manifest `journaled_frames: 8417` equals `mock_transport.socket_written: 8417` exactly; the equality is a coincidence of the data, not a check.
- **Failure scenario:** a journal that truncates its final frames but still verifies as "complete" (verifier semantics are not cross-checked against transmit counts) passes.
- **Required fix:** reconcile `journaled_frames` against `socket_written` (raw frames, both replicas, acks included) with an explicit tolerance of zero, and record the delta as `journal_gap`.
- **Regression test:** drop the last journal frame during finalization and assert `journal_gap: 1` fails the cell.

### P-08 — Transmit proof stops at the socket

- **Location:** `tests/stress_support/mock.rs:301-312` (`complete_send`), consumed at `runner.rs:1113`
- **Defect:** `sent` increments when `sink.send()` resolves. That is proof the mock handed bytes to the kernel, not proof the collector read a frame. There is no receive counter in the collector's `/metrics` and no per-frame identity to correlate transmit with application.
- **Consequence:** a frame lost after `send()` (kernel buffer, collector ingress, decoder, or the active-source gate) is only detectable in aggregate via `applied < generated`, which cannot say *where* it was lost and — because standby copies are discarded by design — cannot detect loss of a standby copy at all.
- **Required fix:** add a collector-side ingress counter (frames read) and a decoder counter, or inject a monotonic frame id the collector can echo into a counter. Design §4 asks for exactly this.
- **Regression test:** a mock that reports success on `send()` but discards the frame must fail with `ingress_gap`, not pass.

### P-09 — Reference artifacts are not bound to the submitted binary or commit

- **Location:** `measurements/stress/seed-0007-4000-uniform.json` (`provenance`), `runner.rs:1143-1146`, `1287-1296`
- **Defect:** the manifest records `source_commit: 5c9e45a…`, `clean_run_required: false`, `untracked_file_count: 3`, while the PR head is `c0eaea2`. `stress_harness_binary_sha256` hashes the test binary, but nothing ties that hash to a commit, so a reader cannot verify the binary came from the recorded source.
- **Mitigating fact (verified):** `git diff --stat 5c9e45a..c0eaea2` touches only `measurements/` and one doc — no `src/` and no `tests/`. The code is identical. The provenance is therefore *probably* sound but unprovable from the artifact, which is the property that matters.
- **Secondary:** the "clean worktree" claim was made with `STRESS_REQUIRE_CLEAN` unset and three untracked files present. The tracked tree was clean; the gate was not exercised.
- **Required fix:** regenerate at the final commit with `STRESS_REQUIRE_CLEAN=1`; embed the source commit into the binary at build time (e.g. `option_env!("GIT_COMMIT")` via build script) and record both the embedded commit and the binary hash, then assert they match the worktree.
- **Regression test:** a build with a dirty tree or a mismatched embedded commit must be rejected before any cell runs.

### P-10 — The reproducibility test compares nothing

- **Location:** `tests/full_universe_stress.rs:631-666`
- **Defect:** `reference_success_manifest_is_reproducible` is `#[ignore]`d, requires `STRESS_MANIFEST_OUT`, regenerates a manifest, and asserts only `outcome.acceptance.valid`. No field is compared with the committed artifact. The test name asserts a property the test does not test, and CI never runs it.
- **Required fix:** either compare a declared deterministic projection (scenario, set hashes, ledger counts, acceptance reasons — excluding host, run id, timings) against the committed file, or rename the test to `reference_success_manifest_generator` and add a separate comparison gate.
- **Regression test:** mutate one deterministic field in a checked-in manifest and assert the comparison fails.

---

## S3 — defects that do not by themselves produce a false success

### P-11 — Index upsert is an unlocked read-modify-write

- **Location:** `tests/stress_support/manifest.rs:86-105`
- **Defect:** read whole document → mutate map → write whole document. No file lock, no compare-and-swap, no optimistic retry. The `pid` in the temp name prevents temp-file collisions between processes, not lost updates.
- **Failure scenario:** two cells finishing within the read-write window. The second write wins; the first cell's entry vanishes. Combined with P-01, its manifest file still says `success`.
- **Required fix:** an advisory lock (`flock`/`fcntl`) around the read-modify-write, or a write-once per-cell file plus a derived index, or a directory-per-cell layout.
- **Regression test:** two processes upserting distinct cells in a loop; assert the final index contains every cell (the test the review asked for in M1.2).

### P-12 — Predictable temp filename

- **Location:** `tests/stress_support/manifest.rs:33-38`
- **Defect:** `parent/.{file_name}.tmp` is derived only from the destination. Two writers of the same cell share one temp path; their bytes interleave and one rename publishes a mixed document.
- **Required fix:** `tempfile::NamedTempFile` in the destination directory (already a dev-dependency) or `{file_name}.{pid}.{nonce}.tmp`.
- **Regression test:** N threads writing the same manifest concurrently; every published body must parse and equal one writer's document.

### P-13 — Rename without durability

- **Location:** `tests/stress_support/manifest.rs:45-46`, `102-104`
- **Defect:** `fs::write` then `fs::rename`, with no `sync_all` on the file and no directory fsync. On a crash or power loss the rename may be visible with zero-length content, or absent while the caller believes it succeeded. The module docstring claims atomicity; atomicity of *visibility* is not durability of *content*.
- **Required fix:** write → `sync_all` → rename → fsync the parent directory. Document the reduced guarantee where that is too costly.
- **Regression test:** hard to assert portably; at minimum assert the code path calls `sync_all` on the temp file before rename (unit test with a fault-injecting sink), and document the directory-fsync decision.

### P-14 — Closed-connection drops are uncounted

- **Location:** `tests/stress_support/mock.rs:340`
- **Defect:** `Err(TrySendError::Closed(_)) => {}`. A closed outbound channel drops the frame with no counter, no log. The `Full` arm is counted; `Closed` is not. The module claims "a full queue is an accounted drop, never a silent one"; a closed queue is exactly a silent one.
- **Failure scenario:** the collector disconnects mid-run; every frame for that connection disappears, `in_flight` stays zero, and the only symptom is a lower `applied` count — which the ledger then blames on the decoder.
- **Required fix:** a `dropped_closed` counter, included in the transport stage and in the failure classification.
- **Regression test:** close the client mid-publish and assert `dropped_closed > 0` and the run fails with an availability class, not `unexplained_drops`.

### P-15 — Injected-drop arithmetic assumes replica symmetry

- **Location:** `tests/stress_support/runner.rs:551-562`
- **Defect:** `expected_applied = generated - injected_frames / replicas`. The division is only correct when the drop counter consumes frames for every replica of the same logical event. `publish_delta` iterates connections in map order (`mock.rs:402-420`), so the fixture's expectation depends on connection ordering and on `count` being a multiple of the replica count.
- **Failure scenario:** `count = 9` with 2 replicas drops 4 events fully and half of a fifth; the expectation `9/2 = 4` is wrong by one, and the fixture either fails for the wrong reason or hides a real loss.
- **Required fix:** count dropped *logical events* (the mock knows the event, not just the frame), or assert explicitly that every drop hit every replica and fail the fixture otherwise.
- **Regression test:** `count = 1..=16` against 1, 2 and 3 replicas; the observed loss must equal the derived expectation in every case.

### P-16 — `queue_high_water` treats absence as zero

- **Location:** `src/metrics.rs:153-161`
- **Defect:** the new field is `#[serde(default)] u64`, documented as "defaults to zero instead of failing to parse". For a field whose entire purpose is to prove a saturation bound was reached, "absent" and "never grew" must not be the same value.
- **Failure scenario:** a report written before the counter existed, or one produced by a build that dropped the metric, reads as `queue_high_water: 0` — evidence of *no* pressure rather than *no* measurement.
- **Required fix:** `Option<u64>`; `None` in the manifest as `{value: null, reason}` (the harness already has `unavailable()` for this).
- **Regression test:** deserialize a report without the field and assert `None`, not `Some(0)`.

### P-17 — The metric scraper is not label-correct

- **Location:** `tests/stress_support/runner.rs:424-442`
- **Defect:** labelled samples are folded to the family name with `name.split('{').next()`, and `HashMap::insert` keeps the **last** sample. For a counter family with per-source labels the reported value is one arbitrary series, not the sum. Lines that do not parse as `f64` are skipped silently. There is no requirement that a metric was present at all.
- **Failure scenario:** a future `collector_deltas_emitted_total{shard="0..n"}` becomes labelled (a natural change at 110 shards); the barrier silently compares against one shard's count and either fails every run or passes a wrong one.
- **Required fix:** sum counter families, reject multiple samples for gauge families used as evidence, and return `None` when a required metric is absent.
- **Regression test:** scrape a fixture registry with a three-label counter and assert the sum; scrape one missing metric and assert the barrier fails.

---

## S4 — conformance and hygiene

| ID | Location | Gap |
| --- | --- | --- |
| P-18 | `tests/full_universe_stress.rs:89-163` | CI smoke runs the uniform workload only; design §3 requires uniform + zipf + lifecycle-burst + disconnect |
| P-19 | `runner.rs:58-73`, `825-828` | `ShutdownBeforeDrain` skips the drain wait in-process; §7 requires a real signal to a separate collector process with exit-code and finalization checks |
| P-20 | design §10 | No disk ceiling; `STRESS_DISK_CEILING` appears only in the design document |
| P-21 | `runner.rs:1070-1072` | Saturation is always `mock_queue_saturation` (a failure); §8 requires `mock_throughput_limited` when the harness, not the collector, is the limiter. The reference success cell reached `mock_queue_high_water: 4008 / 4096` — 97.8 % — so this distinction is already load-bearing |
| P-22 | `runner.rs:448-458` | `harness_process_rss_sample_bytes` is the whole test process (`ps -p self`), i.e. collector + mock + SBE scanner. The name is honest; the usefulness for capacity planning is limited |
| P-23 | `manifest.rs:15` vs `runner.rs:1238` | `STRESS_MANIFEST_SCHEMA_VERSION` exists but the document writes a literal `1` |

---

## Conformance matrix against `docs/stress-lab-design.md`

The design document is committed in this same PR. These are therefore self-imposed requirements, not external opinions.

| Design section | Requirement | State at `c0eaea2` |
| --- | --- | --- |
| §3 | CI smoke = uniform + zipf + lifecycle-burst + disconnect | one workload (P-18) |
| §4 | Nine stage counters, six discrepancy classes | two residuals (P-06) |
| §4 | Replica transport duplicates counted separately | not counted (P-04) |
| §4 | Availability gap distinct from unexplained drop | conflated (P-06) |
| §5 | Exact three-way set equality + decoy disjointness | implemented, verified |
| §6 | Percentiles over 1 s bins, latency histograms | not implemented (`unavailable`) |
| §7 | SIGINT to a child process | not implemented (P-19) |
| §7 | Backpressure visible, never silent | mostly; `Closed` path silent (P-14) |
| §8 | Required manifest fields | 6 field groups absent (P-05) |
| §8 | `mock_throughput_limited` classification | missing (P-21) |
| §8 | Manifest on every exit path | contradicted by fail-closed validation; no quarantined artifact |
| §10 | Disk ceiling | not implemented (P-20) |
| §12.3 | Every event accounted for or classified | accounting incomplete (P-06) |
| §12.10 | Index stays consistent | no concurrency safety (P-11) |

## What this increment genuinely establishes

To be precise about the boundary — these are supported by the code and by the committed artifacts:

- Acceptance is computed inside `run_cell`, not by the caller.
- The anchored set is derived from snapshots only; an update-only symbol cannot be anchored.
- Coverage equality is a real three-way set comparison over canonical SHA-256, and the expected set is independently regenerated in the CI test (`full_universe_stress.rs:151-162`).
- The mock is bounded and reports enqueue / socket-write / in-flight separately; saturation is observed.
- Fail-closed behaviour on a **malformed** index is real and tested.
- Provenance capture (commit, tracked cleanliness, diff hash) exists and is recorded.
- The 4 000-symbol cell did anchor 4 000 / 4 000 and apply 200 / 200 deltas with a valid SBE capture and a finalized journal.

What it does not establish: that a `success` manifest is a complete certificate; that any event can be traced to a stage; that the index is safe under concurrency or crash; that the reference artifacts came from the submitted commit; or anything about capacity.

## Required sequence to close

1. **P-01, P-02, P-03, P-05** — these are the false-success paths. Fix first; each has a cheap, deterministic regression test. No new evidence infrastructure is needed.
2. **P-11, P-12, P-13, P-14** — publication transaction and transport accounting (M1.2). Concurrent-process and crash-injection tests.
3. **P-06, P-07, P-08, P-17** — the stage ledger (M1.3). This is the largest piece; design §4 already specifies the target, so no new design work is required.
4. **P-09, P-10** — regenerate reference artifacts at the final commit with the gate on, and make the comparison a real gate (M1.4).
5. **P-15, P-16, P-18–P-23** — can land alongside, in any order.

Do not widen the capacity matrix until step 1 and step 2 are closed. A capacity number produced by a fail-open barrier is not a measurement.

## Method limitation

This audit read the exact commit and cross-checked it against the committed artifacts. It did not execute fault injection against the harness: the P-01/P-02/P-03 scenarios are derived from control flow, not observed. Each finding therefore names the regression test that would convert it from a code-read to a demonstrated failure. That conversion is the first task of M1.2, and it is the reason the fixes should land with their tests rather than after them.

No claim here is a claim that the current tests fail. All 413 pass, as reported.

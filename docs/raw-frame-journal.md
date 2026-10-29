# Raw-frame journal

The collector can retain every received public data frame in a versioned,
append-only journal next to the normalized SBE capture. The journal is a
forensic record: it keeps the exact payload, receive time, source epoch, and
decode outcome, including frames the decoder rejected. The supplied FIX SBE
schema and output stream are unchanged.

The journal contains public market-data payloads only. It does not store
credentials, orders, strategy state, or authenticated account data, and no
order-submission path exists in the collector.

## Enabling

The journal is off unless a directory is configured. Set it in the TOML file:

```toml
[journal]
directory = "captures/journal"
queue_capacity = 4096
enqueue_timeout_ms = 250
flush_interval_ms = 1000
sync_interval_ms = 5000
segment_max_bytes = 268435456
segment_max_secs = 300
sync_mode = "interval"   # or "always"
```

or use the environment or command line:

```bash
COLLECTOR_JOURNAL_DIR=captures/journal cargo run --release -- collect
cargo run --release -- --journal-dir captures/journal collect
```

`sync_mode = "always"` flushes and fsyncs each record. The default `interval`
mode flushes on `flush_interval_ms`, syncs on `sync_interval_ms`, and performs a
final flush and fsync at shutdown. `segment_max_bytes = 0` and
`segment_max_secs = 0` disable the corresponding rotation trigger.

## Directory layout

Each process run creates an exclusive subdirectory under the configured root:

```text
captures/journal/
  18f0c2a91d4-2f1c/
    manifest.json
    00000000.pmj
    00000001.pmj
```

An active segment is named `NNNNNNNN.pmj.partial`. Finalization writes the
footer, flushes and fsyncs the file, renames it to `NNNNNNNN.pmj`, fsyncs the
directory, and only then records it in the manifest. A process that stops
without finalization leaves the `.partial` file in place; it is never renamed
or described as complete.

## Record format

Each frame becomes one record:

| Field | Meaning |
|---|---|
| `schema_version` | Journal schema version (`1`). |
| `source` | Source identity such as `s0r0`, with `shard` and `replica` fields. |
| `connection_epoch` | Increments on every successful websocket connection for that source. |
| `receive_seq` | Per-source frame counter, reset to zero for each new epoch and advanced for every payload read, including payloads that fail decoding. |
| `received_at_us` | Unix-epoch microseconds captured immediately after the payload is read and before decoding. |
| `clock_source` | Fixed label `unix_epoch_microseconds`. |
| `frame_kind` | `text` or `binary`, preserved from the transport. |
| `payload_len` | Exact payload length. |
| `decode` | `{"status":"decoded","events":N}`, `{"status":"ignored","reason":"..."}`, or `{"status":"rejected","category":"..."}`. |
| payload | Exact bytes as received. No re-encoding, truncation, or normalization. |

Rejected frames keep their payload and a stable category such as
`invalid_json` or `invalid_payload`. Error text and payload previews are not
written into the record header, so a journal does not accumulate unbounded
diagnostic strings.

A record is framed as:

```text
"PMR1" | u32 record_len | u32 header_len | u32 payload_len
       | JSON header | payload | 64-byte lower-case SHA-256 hex
```

`record_len` covers the two length fields, header, payload, and checksum. The
checksum covers every preceding byte of the framed record. Readers enforce
`header_len`, `payload_len`, and `record_len` consistency and reject records
larger than 64 MiB.

## Segment format

A segment starts with a header and ends with a footer. Both are length-prefixed
JSON blocks followed by a 64-byte SHA-256 hex checksum of the block:

- Header magic `PMJSEG01`: schema version, run id, venue, source commit,
  capture-configuration digest, segment index, and creation time.
- Footer magic `PMJEND01`: finalized flag, record count, record-region byte
  count, segment checksum, first and last receive times, and per-source
  `first_epoch`/`first_seq`/`last_epoch`/`last_seq`/`records` ranges.

The segment checksum covers every complete framed record, including each
record's own checksum. Footer and manifest counts refer to framed record bytes,
not header or footer metadata.

## Manifest

`manifest.json` is rewritten atomically after each segment finalization and at
run close. It records the run id, venue, source commit, capture-configuration
digest, creation time, state, durability policy, optional failure text,
finalization time, and the finalized segment list with counts, checksums,
receive-time bounds, and source ranges.

States are `open`, `finalized`, `failed`, and `incomplete`. Only `finalized`
means every accepted record was written, flushed, synced, and recorded in a
renamed segment before the manifest claimed completion.

## Ordering and time

One writer thread consumes a bounded queue, so the journal order is the order in
which source workers enqueued records. For one source, records strictly advance
`(connection_epoch, receive_seq)`. Across sources, file order is queue arrival
order, not a global sort by receive timestamp; receive times can regress between
sources. A reader that needs a timestamp-ordered view must sort by receive time
and accept the resulting loss of observed arrival order.

The receive fence advances before decoding, so a rejected frame cannot reuse the
previous sequence number or hide itself from recovery ordering.

## Durability accounting

`JournalStatus` counters distinguish:

- `queued`: records accepted into the writer queue.
- `written`: records handed to the segment buffer.
- `flushed`: records whose bytes left the process buffer.
- `synced`: records covered by `sync_all`.
- `finalized`: records inside renamed segments recorded in the manifest.
- `segments`: finalized segments.
- `queue_depth`: current backlog.
- `bytes`: record-region bytes in active and finalized segments.

Only `finalized` is evidence of a complete run. The run report includes the
final `JournalStatus`, including the run directory and any failure text.

## Failure behavior

The journal fails the run instead of dropping a frame:

- A full queue that stays full for `enqueue_timeout_ms` marks the run failed and
  returns an error to the source worker.
- A write, flush, sync, rotation, or finalization error marks the manifest
  `failed`, records the error, stops the writer, and rejects later records.
- Finalization uses the same send timeout as the SBE writer. A timeout marks the
  journal failed and is returned as a run error.
- An abrupt stop or explicit abort leaves the active `.partial` segment and an
  open manifest. Verification reports the run incomplete.
- Records accepted into the queue are either written to a finalized segment or
  the run ends with a failure. There is no silent drop path.

Disk-full, permission, and fsync failures surface through the same paths. The
collector does not claim durability for a record that only reached the process
buffer.

## Verification

Verify a run directory against its manifest:

```bash
cargo run --release -- journal verify captures/journal/18f0c2a91d4-2f1c
```

The command prints a JSON summary and exits non-zero unless `complete` is true.
Verification checks the manifest schema and state, failure text, every listed
segment's existence and finalized state, header provenance, header and footer
checksums, record counts, byte counts, receive-time bounds, and source ranges. It
re-parses every record checksum, requires per-source order within and across
segments, rejects bytes after a footer, rejects unlisted `.pmj` files, and
rejects any `.pmj.partial` file.

`journal::read_records` performs the same completeness check and then returns
records in file order with their exact payloads.

## Current limits

- No compression, retention policy, disk watermark, or cross-run index is
  implemented. Operators must size the directory and remove old runs.
- `read_records` loads the full run into memory.
- One writer thread serves the process; the queue bound converts overload into
  an explicit failure rather than unbounded memory growth.
- Payloads are not encrypted. Directory permissions are the access boundary.
- The journal complements, rather than replaces, the SBE capture. Existing
  consumers can continue to read the unchanged SBE output.

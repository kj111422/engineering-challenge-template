//! Defines the [`AlignedSbeReader`], a fast reader for SBE messages written by
//! the [`AlignedSbeWriter`](crate::util::aligned_sbe_writer::AlignedSbeWriter).

use std::{fs::File, io, marker::PhantomData, os::unix::fs::FileExt, path::Path};

use anyhow::Context;
use crossbeam::channel::{Receiver, Sender, bounded};

/// Every SBE schema read by the [`AlignedSbeReader`] implements this trait.
pub trait AlignedSbeSchema: 'static {
    const SCHEMA_ID: u16;
    const SCHEMA_VERSION: u16;

    type Message<'buf>
    where
        Self: 'buf;

    fn try_decode<'buf>(buffer: &'buf [u8], offset: usize) -> anyhow::Result<Self::Message<'buf>>;

    fn encoded_length(message: &Self::Message<'_>) -> anyhow::Result<usize>;

    fn expected_block_length(message: &Self::Message<'_>) -> anyhow::Result<usize>;
}

const DEFAULT_CHUNK_SIZE: usize = 64 << 20; // 64MiB

// Every schema in `dtos` uses the standard eight-byte SBE message header.
const HEADER_LENGTH: usize = 8;

/// A chunk is always addressable as `usize`; clamp a larger file remainder to
/// the configured chunk instead of relying on a conversion invariant.
fn chunk_take(remaining: u64, chunk: usize) -> usize {
    usize::try_from(remaining).map_or(chunk, |value| value.min(chunk))
}

fn widen(length: usize) -> u64 {
    // Rust targets guarantee `usize` is no wider than `u64`.
    length as u64
}

/// See [`AlignedSbeReader::create_prefetch_task`] for more details.
pub type PrefetchTask = Box<dyn FnOnce() + Send + 'static>;

/// The [`AlignedSbeReader`] solves the problem of partial messages in SBE flat
/// files.
///
/// The design targets aeron-spec SBE reads from disk at very high rates -- 100M
/// messages/sec on a single reader thread, with another prefetch thread/task in
/// the background -- through a small API: reads are prefetched and the file is
/// read in big chunks ([`DEFAULT_CHUNK_SIZE`]). Reading in chunks alone allows a
/// message to tear across a chunk boundary, which forces the prefetch logic and
/// the reader to coordinate finishing the truncated message on the next chunk.
///
/// The aligned format removes that coordination: **Write the encoded SBE
/// messages all-or-nothing, aligned to the chunk size.** If the next message
/// would tear across two chunks, the rest of the current chunk is zero-padded
/// and the message is written in full at the start of the next chunk.
///
/// The reader sees the zeros, treats the chunk as finished, and skips to the
/// next one.
pub struct AlignedSbeReader<T: AlignedSbeSchema> {
    file: Option<File>,
    file_len: u64,
    chunk_size: usize,
    buffer: Vec<u8>,

    file_offset: u64,
    buffer_pos: usize,
    eof: bool,
    ready_chunks: Option<Receiver<std::io::Result<Option<Vec<u8>>>>>,
    recycled_chunks: Option<Sender<Vec<u8>>>,
    _schema: PhantomData<T>,
}

impl<T: AlignedSbeSchema> AlignedSbeReader<T> {
    /// Create a new [`AlignedSbeReader`] from the file at `path`. Chunk size is
    /// set as a constant in this file, [`DEFAULT_CHUNK_SIZE`].
    pub fn open(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        Self::open_with_chunk_size(path, DEFAULT_CHUNK_SIZE)
    }

    /// Detect the power-of-two chunk size used by an aligned writer and open
    /// the file with that alignment.
    ///
    /// Detection validates every candidate boundary by streaming the file; it
    /// never loads the whole capture into memory. The largest candidate is
    /// [`MAX_AUTO_CHUNK_SIZE`]; a capture written with a larger chunk must be
    /// opened with an explicit `--read-chunk-size`.
    pub fn open_auto(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let path = path.as_ref();
        let chunk_size = detect_chunk_size_file::<T>(path)?;
        Self::open_with_chunk_size(path, chunk_size)
    }

    /// Create a new [`AlignedSbeReader`] from the file at `path`, with
    /// `chunk_size` for chunk size.
    ///
    /// The final chunk may be partial: this is the state of a capture while a
    /// writer is running, or after a crash. Complete chunks are decoded
    /// normally; a partial tail is decoded up to its last complete message.
    /// After a graceful close the file is padded to whole chunks and the
    /// template's default reader can read it.
    pub fn open_with_chunk_size(path: impl AsRef<Path>, chunk_size: usize) -> anyhow::Result<Self> {
        let file = File::open(path)?;
        let file_len = file.metadata()?.len();
        anyhow::ensure!(chunk_size >= 1024, "chunk size must be at least 1024");

        Ok(Self {
            file: Some(file),
            file_len,
            chunk_size,
            buffer: vec![0; chunk_size],
            file_offset: 0,
            buffer_pos: 0,
            eof: false,
            ready_chunks: None,
            recycled_chunks: None,
            _schema: PhantomData,
        })
    }

    /// Chunk size in bytes used for aligned reads.
    #[must_use]
    pub fn chunk_size(&self) -> usize {
        self.chunk_size
    }

    /// Create a task to be passed up to the owner of this reader, so it can run
    /// chunk prefetching on a separate thread (optionally). When this method is
    /// run, this consumes certain fields of `self` so they may only be used by
    /// the prefetch task.
    pub fn create_prefetch_task(&mut self) -> anyhow::Result<PrefetchTask> {
        anyhow::ensure!(
            self.file_offset == 0 && self.buffer_pos == 0,
            "prefetch must start before reading"
        );

        let file = self.file.take().context("prefetch already started")?;
        let file_len = self.file_len;
        let chunk_size = self.chunk_size;
        let spare_buffers = [std::mem::take(&mut self.buffer), vec![0; self.chunk_size]];
        let (ready_chunks_tx, ready_chunks) = bounded(1);
        let (recycled_chunks, recycled_chunks_rx) = bounded(2);
        self.ready_chunks = Some(ready_chunks);
        self.recycled_chunks = Some(recycled_chunks);

        Ok(Box::new(move || {
            let mut spare_buffers = spare_buffers.into_iter();
            let mut file_offset = 0u64;

            loop {
                let remaining = file_len.saturating_sub(file_offset);
                if remaining == 0 {
                    let _ = ready_chunks_tx.send(Ok(None));
                    return;
                }
                let take = chunk_take(remaining, chunk_size);

                let mut buffer = match spare_buffers.next() {
                    Some(buffer) => buffer,
                    None => match recycled_chunks_rx.recv() {
                        Ok(buffer) => buffer,
                        Err(_) => return,
                    },
                };
                // Recycle short buffers produced by an earlier partial tail.
                buffer.resize(take, 0);
                if let Err(error) = ChunkSource::read_exact_at(&file, file_offset, &mut buffer) {
                    let _ = ready_chunks_tx.send(Err(error));
                    return;
                }
                if ready_chunks_tx.send(Ok(Some(buffer))).is_err() {
                    return;
                }
                file_offset = file_offset.saturating_add(widen(take));
            }
        }))
    }

    /// Signal to the prefetch task that it should stop running.
    pub fn signal_stop_prefetch(&mut self) {
        self.ready_chunks = None;
        self.recycled_chunks = None;
    }

    /// Returns whether the next chunk could be loaded or not.
    fn load_chunk(&mut self) -> anyhow::Result<bool> {
        if self.eof {
            return Ok(false);
        }

        if let Some(ready_chunks) = &self.ready_chunks {
            if !self.buffer.is_empty() {
                let buffer = std::mem::take(&mut self.buffer);
                if let Some(recycled_chunks) = &self.recycled_chunks {
                    let _ = recycled_chunks.send(buffer);
                }
            }

            return match ready_chunks
                .recv()
                .context("SBE prefetch thread stopped")??
            {
                Some(buffer) => {
                    self.buffer = buffer;
                    self.buffer_pos = 0;
                    Ok(true)
                }
                None => {
                    self.eof = true;
                    Ok(false)
                }
            };
        }

        let remaining = self.file_len.saturating_sub(self.file_offset);
        if remaining == 0 {
            self.eof = true;
            return Ok(false);
        }
        let take = chunk_take(remaining, self.chunk_size);
        self.buffer.resize(take, 0);
        ChunkSource::read_exact_at(
            self.file.as_ref().context("file moved to prefetch task")?,
            self.file_offset,
            &mut self.buffer,
        )?;

        self.file_offset = self.file_offset.saturating_add(widen(take));
        self.buffer_pos = 0;
        if self.file_offset >= self.file_len {
            self.eof = true;
        }

        Ok(true)
    }

    /// Decode the next message from the chunk buffer.
    ///
    /// Returns `Ok(None)` at the end of the file and `Err(...)` on a file read
    /// or message decode failure.
    pub fn read_msg(&mut self) -> anyhow::Result<Option<T::Message<'_>>> {
        loop {
            let header_end = self
                .buffer_pos
                .checked_add(HEADER_LENGTH)
                .context("pos overflow")?;

            if header_end <= self.buffer.len()
                && self.buffer[self.buffer_pos..header_end] != [0; HEADER_LENGTH]
            {
                break;
            }

            // A chunk sentinel is a run of zero bytes through the end of the
            // chunk. Anything non-zero after the first zero header is malformed
            // and must not be silently skipped while advancing to the next
            // chunk.
            anyhow::ensure!(
                self.buffer[self.buffer_pos..].iter().all(|byte| *byte == 0),
                "non-zero bytes after the aligned SBE chunk sentinel"
            );

            if !self.load_chunk()? {
                return Ok(None);
            }
        }

        // All generated schemas share this header layout:
        // blockLength, templateId, schemaId, version -- all little-endian u16.
        let header_end = self.buffer_pos + HEADER_LENGTH;
        let header = &self.buffer[self.buffer_pos..header_end];
        let block_length = usize::from(u16::from_le_bytes([header[0], header[1]]));
        let schema_id = u16::from_le_bytes([header[4], header[5]]);
        let schema_version = u16::from_le_bytes([header[6], header[7]]);

        anyhow::ensure!(schema_id == T::SCHEMA_ID);
        anyhow::ensure!(schema_version == T::SCHEMA_VERSION);

        let decoded = T::try_decode(&self.buffer, self.buffer_pos)?;

        anyhow::ensure!(block_length == T::expected_block_length(&decoded)?);

        let message_len = T::encoded_length(&decoded)?;
        let end = self
            .buffer_pos
            .checked_add(message_len)
            .context("overflow")?;
        anyhow::ensure!(
            end <= self.buffer.len(),
            "SBE message crosses the aligned chunk boundary"
        );

        self.buffer_pos = end;

        Ok(Some(decoded))
    }
}

/// Largest chunk size considered by [`AlignedSbeReader::open_auto`].
///
/// Detection streams one candidate chunk at a time, so this bound is also the
/// peak memory used by detection. The default writer chunk is 64 MiB; captures
/// written with a larger chunk remain readable with an explicit
/// `--read-chunk-size`.
pub const MAX_AUTO_CHUNK_SIZE: usize = 256 << 20;

/// Read-only byte source used by bounded-memory chunk detection.
trait ChunkSource {
    fn len(&self) -> io::Result<u64>;
    fn read_exact_at(&self, offset: u64, buffer: &mut [u8]) -> io::Result<()>;
}

impl ChunkSource for File {
    fn len(&self) -> io::Result<u64> {
        self.metadata().map(|metadata| metadata.len())
    }

    fn read_exact_at(&self, offset: u64, buffer: &mut [u8]) -> io::Result<()> {
        FileExt::read_exact_at(self, buffer, offset)
    }
}

/// Scan power-of-two chunk sizes and return the one whose complete chunks are
/// valid SBE streams with a valid (possibly partial) final chunk.
fn detect_chunk_size_file<T: AlignedSbeSchema>(path: &Path) -> anyhow::Result<usize> {
    let file = File::open(path)
        .with_context(|| format!("opening `{}` for chunk detection", path.display()))?;
    detect_chunk_size_source::<T, _>(&file)
}

fn detect_chunk_size_source<T: AlignedSbeSchema, S: ChunkSource>(
    source: &S,
) -> anyhow::Result<usize> {
    let len = source.len().context("reading SBE source length")?;
    if len == 0 {
        return Ok(DEFAULT_CHUNK_SIZE);
    }

    let len_usize = usize::try_from(len).unwrap_or(usize::MAX);
    // Include one candidate at or above the file length so a capture whose
    // final chunk has not been padded yet is still decodable. The cap keeps
    // detection memory bounded even for multi-gigabyte captures.
    let limit = len_usize
        .checked_next_power_of_two()
        .unwrap_or(usize::MAX)
        .clamp(1024, MAX_AUTO_CHUNK_SIZE);
    let mut best: Option<(usize, usize)> = None; // (chunk size, message count)
    let mut chunk = 1024usize;
    let mut buffer = Vec::new();
    while chunk <= limit {
        match validate_aligned_source::<T, _>(source, chunk, &mut buffer) {
            Ok(count) if count > 0 && best.is_none_or(|(_, best_count)| count > best_count) => {
                best = Some((chunk, count));
            }
            _ => {}
        }
        // Every candidate at or above the file length parses the file as one
        // partial chunk, so trying a larger one cannot add information.
        if u64::try_from(chunk).unwrap_or(u64::MAX) >= len {
            break;
        }
        chunk = chunk.saturating_mul(2);
    }

    if let Some((chunk, _)) = best {
        return Ok(chunk);
    }

    anyhow::bail!(
        "could not auto-detect chunk size for {} bytes of SBE data within {} MiB; \
         use --read-chunk-size for a non-default chunk size",
        len,
        MAX_AUTO_CHUNK_SIZE >> 20
    )
}

fn validate_aligned_source<T: AlignedSbeSchema, S: ChunkSource>(
    source: &S,
    chunk: usize,
    buffer: &mut Vec<u8>,
) -> anyhow::Result<usize> {
    let len = source.len().context("reading SBE source length")?;
    let mut total = 0usize;
    let mut start = 0u64;
    while start < len {
        let take = chunk_take(len - start, chunk);
        buffer.resize(take, 0);
        source
            .read_exact_at(start, buffer)
            .context("reading aligned SBE chunk")?;
        let mut pos = 0usize;
        while pos < take {
            let remaining = take - pos;
            if remaining < HEADER_LENGTH {
                // Fewer than a header: a partial tail, not an error.
                break;
            }
            let header = &buffer[pos..pos + HEADER_LENGTH];
            if header == [0; HEADER_LENGTH] {
                // A sentinel must be followed only by zero padding through the
                // end of this chunk. In a partial final chunk the sentinel is
                // the last data byte.
                anyhow::ensure!(
                    buffer[pos..].iter().all(|byte| *byte == 0),
                    "non-zero bytes after the chunk sentinel"
                );
                break;
            }
            let block_length = usize::from(u16::from_le_bytes([header[0], header[1]]));
            let schema_id = u16::from_le_bytes([header[4], header[5]]);
            let schema_version = u16::from_le_bytes([header[6], header[7]]);
            anyhow::ensure!(schema_id == T::SCHEMA_ID, "schema id mismatch");
            anyhow::ensure!(
                schema_version == T::SCHEMA_VERSION,
                "schema version mismatch"
            );

            let (message_len, expected_block_length) = {
                let decoded = T::try_decode(buffer, pos)?;
                (
                    T::encoded_length(&decoded)?,
                    T::expected_block_length(&decoded)?,
                )
            };
            anyhow::ensure!(
                block_length == expected_block_length,
                "block length mismatch"
            );
            anyhow::ensure!(message_len >= HEADER_LENGTH, "invalid message length");
            anyhow::ensure!(message_len <= remaining, "message crosses chunk boundary");
            pos += message_len;
            total += 1;
        }
        start = start.saturating_add(widen(take));
    }
    Ok(total)
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use super::*;
    use crate::util::OrderbookSchema;

    #[test]
    fn rejects_non_zero_bytes_after_a_chunk_sentinel() {
        let path = std::env::temp_dir().join(format!(
            "engineering-challenge-sentinel-{}-{}.sbe",
            std::process::id(),
            crate::time::now_us().unwrap_or(0)
        ));
        let mut bytes = vec![0u8; 1024];
        bytes[100] = 1;
        {
            let mut file = File::create(&path).unwrap();
            file.write_all(&bytes).unwrap();
            file.sync_all().unwrap();
        }
        let mut reader =
            AlignedSbeReader::<OrderbookSchema>::open_with_chunk_size(&path, 1024).unwrap();
        let error = match reader.read_msg() {
            Ok(_) => panic!("expected a sentinel validation error"),
            Err(error) => error.to_string(),
        };
        assert!(error.contains("non-zero bytes"), "{error}");
        let _ = std::fs::remove_file(path);
    }
}

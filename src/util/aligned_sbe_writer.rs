//! [`AlignedSbeWriter`] writes SBE messages in fixed-size, zero-padded chunks.
//!
//! The file format is the template's: messages never cross a chunk boundary;
//! a reader can therefore decode every chunk independently and stop at the
//! all-zero sentinel. The default chunk size is the template's 64 MiB so an
//! `AlignedSbeReader::open()` from the unmodified template decodes a closed
//! capture.
//!
//! Flushing is incremental: [`Self::flush`] writes only the encoded bytes, not
//! the padding. Padding is emitted when a chunk rotates and by
//! [`Self::finish`] (or [`Self::sync_all`]) at shutdown. This keeps
//! event-to-visible-output latency low without writing a whole chunk of zeroes
//! on every flush. The same aligned format is used for stdout, so redirecting
//! stdout to a file produces a capture the template reader can open.

use std::{
    fs::{File, OpenOptions},
    io::{BufWriter, Write},
    path::Path,
};

use sha2::{Digest, Sha256};

use super::dtos::SbeEncode;

// Every schema in `dtos` uses the standard eight-byte SBE message header.
const HEADER_LENGTH: usize = 8;

/// Writes SBE messages to an aligned chunked output.
pub struct AlignedSbeWriter<W: Write + Send = File> {
    writer: W,
    buf: Vec<u8>,
    chunk: usize,
    /// Bytes of `buf` already handed to `writer`, so a flush never repeats
    /// data.
    committed: usize,
    bytes_written: u64,
    /// Optional SHA-256 over every byte handed to the sink, including chunk
    /// padding. Enabled for rotated segments so the manifest can detect
    /// truncation or modification after finalization.
    checksum: Option<Sha256>,
}

impl AlignedSbeWriter<BufWriter<File>> {
    /// Create an aligned [`AlignedSbeWriter`] at a new path.
    ///
    /// Creation is exclusive: an existing file is an error. Callers that
    /// replace an artifact with [`Self::create`].
    pub fn create_new(path: impl AsRef<Path>, chunk: usize) -> anyhow::Result<Self> {
        validate_chunk(chunk)?;
        let file = OpenOptions::new().write(true).create_new(true).open(path)?;
        Ok(Self::from_file(file, chunk))
    }

    /// Create an aligned [`AlignedSbeWriter`] writing to `path` with `chunk`
    /// chunk size, clearing the file at `path` if it exists.
    ///
    /// This is the explicit-overwrite constructor. Production startup rejects
    /// existing artifacts unless the operator enables `output.overwrite`.
    pub fn create(path: impl AsRef<Path>, chunk: usize) -> anyhow::Result<Self> {
        validate_chunk(chunk)?;
        let file = File::create(path)?;
        Ok(Self::from_file(file, chunk))
    }

    fn from_file(file: File, chunk: usize) -> Self {
        // The chunk buffer is the alignment unit; the OS write buffer only
        // needs to absorb syscall overhead. Do not double the 64 MiB default.
        let write_buffer = chunk.min(1 << 20);
        Self::new(BufWriter::with_capacity(write_buffer, file), chunk)
    }

    /// Finish the final chunk, flush, and persist the file.
    ///
    /// `fsync` is only valid for regular files. Pipes, FIFOs, character
    /// devices such as `/dev/stdout`, and sockets return `EINVAL` on Linux, so
    /// those sinks are flushed but not synced.
    pub fn sync_all(&mut self) -> anyhow::Result<()> {
        self.finish()?;
        let file = self.writer.get_ref();
        if file.metadata()?.file_type().is_file() {
            file.sync_all()?;
        }
        Ok(())
    }

    /// Flush visible bytes and persist them without closing the chunk.
    ///
    /// Periodic durability uses this instead of [`Self::sync_all`]: padding
    /// the active chunk on every sync would grow the capture by up to one
    /// chunk per interval. Readers can decode the committed messages; the
    /// remainder of the chunk stays unwritten until it fills or the file is
    /// finalized.
    pub fn sync_data(&mut self) -> anyhow::Result<()> {
        self.write_uncommitted()?;
        self.writer.flush()?;
        let file = self.writer.get_ref();
        if file.metadata()?.file_type().is_file() {
            file.sync_data()?;
        }
        Ok(())
    }
}

impl<W: Write + Send> AlignedSbeWriter<W> {
    /// Create an aligned writer over an arbitrary byte sink, such as an
    /// in-memory buffer. Chunks are padded exactly like file output.
    pub fn from_writer(writer: W, chunk: usize) -> anyhow::Result<Self> {
        validate_chunk(chunk)?;
        Ok(Self::new(writer, chunk))
    }

    fn new(writer: W, chunk: usize) -> Self {
        Self {
            writer,
            buf: Vec::with_capacity(chunk),
            chunk,
            committed: 0,
            bytes_written: 0,
            checksum: None,
        }
    }

    /// Start a SHA-256 over all bytes written from this point.
    ///
    /// Must be called before the first message so the digest covers the whole
    /// stream, including padding.
    pub fn enable_checksum(&mut self) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.bytes_written == 0 && self.buf.is_empty(),
            "checksum must be enabled before the first message"
        );
        self.checksum = Some(Sha256::new());
        Ok(())
    }

    /// Hex SHA-256 of every byte written so far, when checksumming is enabled.
    #[must_use]
    pub fn checksum_hex(&self) -> Option<String> {
        self.checksum.as_ref().map(|hasher| {
            let digest = hasher.clone().finalize();
            let mut text = String::with_capacity(digest.len() * 2);
            for byte in digest {
                use std::fmt::Write as _;
                let _ = write!(text, "{byte:02x}");
            }
            text
        })
    }

    /// Total bytes handed to the underlying writer, including zero padding.
    #[must_use]
    pub fn bytes_written(&self) -> u64 {
        self.bytes_written
    }

    /// Configured chunk size in bytes.
    #[must_use]
    pub fn chunk_size(&self) -> usize {
        self.chunk
    }

    /// Encodes a complete message. In aligned mode a message that would cross
    /// the current chunk boundary closes the chunk first.
    pub fn write_msg<T: SbeEncode + ?Sized>(&mut self, message: &T) -> anyhow::Result<()> {
        let len = message.encoded_length_with_header()?;
        let payload_limit = self.chunk - HEADER_LENGTH;
        anyhow::ensure!(len <= payload_limit, "SBE message does not fit in a chunk");
        if len > payload_limit - self.buf.len() {
            self.close_chunk()?;
        }

        let start = self.buf.len();
        self.buf.resize(start + len, 0);
        let result = message.encode(&mut self.buf[start..]).and_then(|written| {
            anyhow::ensure!(
                written == len,
                "SBE encoded length mismatch: expected {len} bytes, wrote {written}"
            );
            Ok(())
        });
        if result.is_err() {
            self.buf.truncate(start);
        }
        result
    }

    /// Make every encoded byte visible to readers without padding the current
    /// chunk. A reader can decode the complete messages written so far; a
    /// graceful close later pads the final chunk to the configured alignment.
    pub fn flush(&mut self) -> anyhow::Result<()> {
        self.write_uncommitted()?;
        self.writer.flush()?;
        Ok(())
    }

    /// Pad and write the final chunk, then flush. Idempotent.
    pub fn finish(&mut self) -> anyhow::Result<()> {
        self.close_chunk()?;
        self.writer.flush()?;
        Ok(())
    }

    fn write_uncommitted(&mut self) -> anyhow::Result<()> {
        if self.committed < self.buf.len() {
            let bytes = &self.buf[self.committed..];
            self.writer.write_all(bytes)?;
            if let Some(checksum) = &mut self.checksum {
                checksum.update(bytes);
            }
            self.bytes_written += bytes.len() as u64;
            self.committed = self.buf.len();
        }
        Ok(())
    }

    /// Rotate to the next chunk. Only meaningful for aligned output.
    ///
    /// An empty buffer is left alone: a capture with no messages stays empty,
    /// and a file that ends exactly on a chunk boundary does not gain a
    /// spurious empty padded chunk.
    fn close_chunk(&mut self) -> anyhow::Result<()> {
        if self.buf.is_empty() {
            self.committed = 0;
            return Ok(());
        }
        if self.buf.len() < self.chunk {
            self.buf.resize(self.chunk, 0);
        }
        self.write_uncommitted()?;
        self.buf.clear();
        self.committed = 0;
        Ok(())
    }
}

fn validate_chunk(chunk: usize) -> anyhow::Result<()> {
    anyhow::ensure!(
        chunk > HEADER_LENGTH,
        "chunk must leave room for SBE data and an SBE header"
    );
    anyhow::ensure!(
        chunk.is_power_of_two(),
        "chunk size must be a power of two so the reader can auto-detect it"
    );
    anyhow::ensure!(chunk >= 1024, "chunk size must be at least 1024 bytes");
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;

    struct TestMessage;

    impl SbeEncode for TestMessage {
        fn encoded_length_with_header(&self) -> anyhow::Result<usize> {
            Ok(16)
        }

        fn encode(&self, buf: &mut [u8]) -> anyhow::Result<usize> {
            buf[..16].fill(1);
            Ok(16)
        }
    }

    #[test]
    fn aligned_writer_pads_only_on_finish() {
        let mut writer = AlignedSbeWriter::from_writer(Cursor::new(Vec::new()), 4096).unwrap();
        writer.flush().unwrap();
        assert_eq!(writer.bytes_written(), 0);
        writer.finish().unwrap();
        // No messages: an empty aligned capture is an empty file.
        assert_eq!(writer.bytes_written(), 0);
    }

    #[test]
    fn flush_is_incremental_and_finish_pads() {
        let mut writer = AlignedSbeWriter::from_writer(Cursor::new(Vec::new()), 1024).unwrap();
        writer.write_msg(&TestMessage).unwrap();
        writer.flush().unwrap();
        assert_eq!(writer.bytes_written(), 16);
        writer.finish().unwrap();
        assert_eq!(writer.bytes_written(), 1024);
        // Finishing again must not add a spurious empty chunk.
        writer.finish().unwrap();
        assert_eq!(writer.bytes_written(), 1024);
    }
}

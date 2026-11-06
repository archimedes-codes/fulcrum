//! The JSONL frame codec both platforms share (T9 Task 3).
//!
//! One frame per `\n`-terminated JSON line — the SAME framing as
//! `fulcrum_core::worker::protocol`, on purpose (see the module docs on
//! [`crate::transport`]). Nothing here is platform-specific: `unix.rs` and
//! `windows.rs` hand me a stream that is `AsyncRead + AsyncWrite`, and this file
//! turns it into [`Connection`]. That is why the reader-task rule below is
//! written once instead of twice.

use serde::Serialize;
use tokio::io::{AsyncBufReadExt as _, AsyncRead, AsyncWrite, AsyncWriteExt as _, BufReader};
use tokio::sync::mpsc;

use crate::transport::{Endpoint, TransportError};

/// The duplex both legs hand in, as ONE trait.
///
/// A `dyn` object may name only one non-auto trait, so the pair becomes a
/// private supertrait with a blanket impl.
trait Duplex: AsyncRead + AsyncWrite {}
impl<T: AsyncRead + AsyncWrite + ?Sized> Duplex for T {}

/// The erased duplex both legs hand in.
///
/// Erased rather than a type parameter because `Listener::accept` returns a
/// plain `Connection`: the caller must not learn which platform it is on, and a
/// generic parameter here would leak `UnixStream` / `NamedPipeServer` back into
/// the platform-neutral signature.
type DuplexBox = Box<dyn Duplex + Unpin + Send>;

/// One framed connection to the daemon (or to a client, from the listener's
/// side).
///
/// Bidirectional, and the two directions are deliberately DECOUPLED: the read
/// half lives on its own task, the write half is driven in place. See
/// [`Connection::new`] for why the reader cannot be a future inside this struct.
///
/// Dropping it closes the socket/pipe and stops the reader task.
pub struct Connection {
    /// The write half, plus the bytes of the current frame the OS has not taken
    /// yet (see [`Connection::send_frame`]).
    writer: tokio::io::WriteHalf<DuplexBox>,
    /// The tail of a frame whose `write` was only partially accepted. Kept here
    /// so an abandoned `send_frame` cannot desync the stream.
    pending: Vec<u8>,
    /// Decoded inbound lines, in order. `None` (i.e. the channel closing) is
    /// EOF.
    lines: mpsc::UnboundedReceiver<Result<serde_json::Value, TransportError>>,
    /// The reader task. Held so `Drop` can abort it — otherwise a dropped
    /// `Connection` would leave a task parked on a dead socket forever.
    reader: tokio::task::JoinHandle<()>,
}

/// Manual, not `derive`: the write half wraps a `dyn` object, which has no
/// `Debug` to forward to. Same shape tokio's own `split::WriteHalf` prints.
impl std::fmt::Debug for Connection {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        fmt.debug_struct("Connection")
            .field("reader", &self.reader)
            .finish_non_exhaustive()
    }
}

impl Connection {
    /// Wrap a connected stream: split it, and put the read half on its own
    /// task.
    pub(crate) fn new<S>(stream: S) -> Self
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let stream: DuplexBox = Box::new(stream);
        // `split` gives two handles over one stream, so the reader task and the
        // caller's `send_frame` never fight over a single `&mut`.
        let (read_half, write_half) = tokio::io::split(stream);
        let (tx, lines) = mpsc::unbounded_channel();

        // A DEDICATED TASK, not a `read_line` future this struct owns.
        //
        // `AsyncBufReadExt::read_line` appends into the caller's `String` as
        // bytes arrive and is NOT cancellation-safe: a `select!`/`timeout` that
        // abandons a partially-filled `read_line` discards the bytes that call
        // already consumed from the socket, and the next read starts mid-frame.
        // On a socket that is a silently corrupted frame, not a retryable error.
        // A task that is never `select`ed over cannot be abandoned mid-read, so
        // the line buffer can never be seen in a half-filled state. This is the
        // same reason `client.rs`'s `spawn_pipe_reader` exists for the worker's
        // stdout/stderr pipes — a socket reader carries the identical hazard.
        let reader = tokio::spawn(async move {
            let mut reader = BufReader::new(read_half);
            let mut line = String::new();
            loop {
                line.clear();
                match reader.read_line(&mut line).await {
                    // EOF — the peer is gone. A final line with NO trailing
                    // newline still gets forwarded (a peer killed mid-write is
                    // exactly the frame worth keeping), matching
                    // `spawn_pipe_reader`.
                    Ok(0) => {
                        let tail = line.trim_end();
                        if !tail.is_empty() {
                            // Going away anyway, so the send result is moot.
                            let _ = tx.send(decode(tail));
                        }
                        break;
                    }
                    Ok(_) => {
                        // `trim_end` drops the `\n` (and a `\r\n`'s `\r`). A
                        // blank line carries no frame, so it is skipped rather
                        // than surfaced as a decode error.
                        let line = line.trim_end();
                        if !line.is_empty() && tx.send(decode(line)).is_err() {
                            break; // the Connection is gone.
                        }
                    }
                    // A read error is indistinguishable from a closed peer here;
                    // ending the channel ends the stream for the consumer.
                    Err(_) => break,
                }
            }
            // Dropping `tx` here is what makes `Connection::recv` report EOF.
        });

        Self {
            writer: write_half,
            pending: Vec::new(),
            lines,
            reader,
        }
    }

    /// Serialize `frame` as one JSON object and write it as one `\n`-terminated
    /// line.
    ///
    /// Cancellation-safe in the sense that matters for a framing layer: the
    /// bytes of a frame already handed to `send_frame` are buffered in
    /// `self.pending` and drained on the next call, so abandoning this future
    /// mid-write cannot leave the stream holding half a frame (which every
    /// later frame would then be glued onto). It is still NOT a good idea to
    /// `select!` over a send — the frame you started may still go out.
    pub async fn send_frame<T>(&mut self, frame: &T) -> Result<(), TransportError>
    where
        T: Serialize + ?Sized,
    {
        // `to_string` then one `\n` — the worker protocol's exact framing
        // (`stamp_outbound` formats `"{}\n"`), so both ends of the daemon speak
        // one dialect.
        let mut line =
            serde_json::to_string(frame).map_err(|e| TransportError::Encode(e.to_string()))?;
        line.push('\n');
        self.pending.extend_from_slice(line.as_bytes());

        while !self.pending.is_empty() {
            match self.writer.write(&self.pending).await {
                // A zero-length write means the sink is done (see
                // `AsyncWriteExt::write`); the frame is unrecoverable.
                Ok(0) => {
                    self.pending.clear();
                    return Err(TransportError::Io(std::io::Error::new(
                        std::io::ErrorKind::WriteZero,
                        "the peer stopped accepting bytes",
                    )));
                }
                Ok(written) => {
                    let _ = self.pending.drain(..written);
                }
                Err(e) => return Err(e.into()),
            }
        }
        // The write half is unbuffered, so every byte is already handed to the
        // OS; `flush` is what the trait requires and a no-op on a socket.
        self.writer.flush().await?;
        Ok(())
    }

    /// The next inbound frame.
    ///
    /// `None` is EOF — the peer closed. A line that is not valid JSON arrives as
    /// `Some(Err(Decode(..)))` and the stream CONTINUES: one malformed line is a
    /// peer bug, not a reason to lose the connection. That mirrors
    /// `protocol::decode_inbound`, which is permissive for the same reason.
    pub async fn recv(&mut self) -> Option<Result<serde_json::Value, TransportError>> {
        self.lines.recv().await
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        // Stop the reader task. Without this the task keeps the read half alive
        // and the socket is never fully closed by a dropped connection.
        self.reader.abort();
    }
}

/// Decode one inbound line. Permissive by design: the daemon's clients may be
/// older or newer than it is, and a bad line is reported per-line rather than
/// killing the connection (see [`Connection::recv`]).
fn decode(line: &str) -> Result<serde_json::Value, TransportError> {
    serde_json::from_str(line).map_err(|e| TransportError::Decode(e.to_string()))
}

/// The error every platform leg raises when handed the other platform's
/// address.
///
/// Lives here rather than being written twice: the shape is identical, only the
/// two `'static` strings swap.
pub(crate) fn wrong_platform(
    family: &'static str,
    platform: &'static str,
    endpoint: Endpoint,
) -> TransportError {
    TransportError::WrongPlatform {
        family,
        platform,
        endpoint,
    }
}

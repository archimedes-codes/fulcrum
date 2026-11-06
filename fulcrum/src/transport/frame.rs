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

/// How many decoded inbound lines one connection's reader task may hold before
/// it stops draining the socket (finding 7).
///
/// # Why this channel is bounded at all
///
/// The reader task used to hand every line to an `mpsc::unbounded_channel`, and
/// `Connection::recv` was the only consumer. A peer that writes frames and never
/// waits for replies therefore had an infinitely deep mailbox: the reader drained
/// the socket as fast as the kernel would deliver bytes, `recv` lagged by whatever
/// the peer chose to send, and the daemon's heap grew by one `serde_json::Value`
/// per line with nothing to stop it.
///
/// Measured on Linux against the daemon, one connection, 10 s of `hello` frames
/// from a client that read NO bytes at all: **1 168 009 requests absorbed, daemon
/// RSS 15 MB → 895 MB.** The socket buffers are ~200 KB; the extra ~880 MB was
/// this channel. That is a local denial-of-service available to any process that
/// can reach the socket, and `serve` has no authentication before T13.
///
/// # Why backpressure is the right response, not dropping
///
/// The reader `await`s room instead of discarding, so the pressure propagates to
/// the only party that can actually resolve it: the socket fills, the peer's own
/// `write` blocks, and the attacker is throttled by their own kernel rather than
/// by the daemon paying for their backlog. Dropping inbound frames would be worse
/// than the disease here — a request that vanishes is a client bug the daemon
/// caused, whereas a client that blocks on `write` is seeing a limit, and every
/// real client reads its replies so never reaches it.
///
/// # Why 1024
///
/// The same budget as `serve::CLIENT_QUEUE_CAPACITY`, for the same reason: at a
/// few hundred bytes per decoded `Value` this is well under a megabyte per
/// connection, and it is comfortably above what any honest client needs in flight
/// (the daemon answers a `hello` in one step and a `start` after one `attach`).
/// It is deliberately NOT tied to `FAN_OUT_CAPACITY`: that is the daemon's
/// outbound event ring and has no bearing on how many REQUESTS one peer may have
/// outstanding.
const MAX_QUEUED_INBOUND_FRAMES: usize = 1024;

/// The longest inbound LINE (in bytes, terminator included) this transport will
/// buffer from one peer (finding 7).
///
/// `pub` because it is a contract a client can act on: a caller that must send
/// something near the limit needs to know it is being refused for length and not
/// for syntax.
///
/// # Why a cap on the LINE, when the queue is already bounded
///
/// They stop two different attacks, and neither subsumes the other. The queue
/// bound limits how many COMPLETED frames may sit undelivered. This limits how
/// large ONE frame may be while it is still being assembled — and a line
/// assembler is a buffer too. `AsyncBufReadExt::read_line` appends into its
/// caller's `String` and keeps going until it sees a `\n`, so a peer that sends
/// ten megabytes with no terminator holds ten megabytes in the reader task with
/// no newline in sight and nothing in the daemon able to object. One connection,
/// one allocation, no ceiling: `read_line` will happily grow until the process
/// cannot.
///
/// # Why 1 MiB
///
/// The largest legitimate inbound frame is a `start`, which carries a
/// `StartEnv` — cwd, capabilities, and a full `ModelCatalog`. A catalog of a few
/// hundred models is tens of kilobytes, so 1 MiB is roughly an order of magnitude
/// above the biggest thing a real client can send today, with room for the
/// protocol to grow (T10's `attach` will carry more). Below that the daemon
/// refuses; above it a single hostile connection costs 1 MiB of buffer instead of
/// an amount chosen by the attacker.
///
/// # Why exceeding it closes the connection rather than skipping the line
///
/// The alternative — drop the bytes, resync on the next `\n` — is what "just
/// ignore it" sounds like, and it is the worse failure. A frame the daemon
/// refused is a frame the client believes it sent; the client's next frame then
/// arrives on a stream the daemon has already re-interpreted, and the two sides
/// disagree about where frames begin. That is the desync this file's whole design
/// (one frame per line, a reader that is never abandoned mid-line) exists to
/// avoid, so the cap ends the connection with a named error instead of
/// inventing a resync the protocol has no framing to support.
pub const MAX_INBOUND_LINE_BYTES: usize = 1024 * 1024;

/// One framed connection to the daemon (or to a client, from the listener's
/// side).
///
/// Bidirectional, and the two directions are deliberately DECOUPLED: the read
/// half lives on its own task, the write half is a [`ConnectionWriter`] — this
/// type's dormant writer slot until [`Connection::split_writer`] hands it out.
/// See [`Connection::new`] for why the reader cannot be a future inside this
/// struct.
///
/// Dropping it closes the socket/pipe and stops the reader task. Note that
/// dropping ONE half of a split pair does not: `tokio::io::split`'s halves share
/// an `Arc` over the stream, so the socket's full close rides the drop of the
/// LAST handle — the reader here plus the peer's `ConnectionWriter`.
pub struct Connection {
    /// The write half, held ONLY so [`Connection::split_writer`] can hand it out
    /// once. `None` after `split_writer`, and `Connection` has NO write method.
    ///
    /// `Option` rather than a plain field because `split_writer` consumes `self`
    /// and must still be able to take the writer out: taking it from a `Some` and
    /// leaving a `None` behind is what makes a second `split_writer` panic instead
    /// of handing out a second handle to `pending`.
    ///
    /// It holds the whole [`ConnectionWriter`] and not a bare `WriteHalf` so that
    /// `pending` moves out with it in one piece — there is no code path in this
    /// type that can reach `pending` at all, which is the point: an unsplit
    /// `Connection` cannot write, and a split one cannot write twice. (While the
    /// split landed in two commits there was a transient `Connection::send_frame`
    /// delegator that borrowed this slot; T4b deleted it, because a `Connection`
    /// that can write alongside a live `ConnectionWriter` is exactly the two-handles
    /// hazard this type exists to prevent.)
    writer: Option<ConnectionWriter>,
    /// Decoded inbound lines, in order. `None` (i.e. the channel closing) is
    /// EOF.
    ///
    /// Bounded — see [`MAX_QUEUED_INBOUND_FRAMES`]. When it is full the reader
    /// task waits, which is the backpressure: the socket fills and the peer's own
    /// `write` blocks.
    lines: mpsc::Receiver<Result<serde_json::Value, TransportError>>,
    /// The reader task. Held so `Drop` can abort it — otherwise a dropped
    /// `Connection` would leave a task parked on a dead socket forever.
    reader: tokio::task::JoinHandle<()>,
}

/// The WRITE half of a framed connection, with the unwritten tail of a
/// partially-accepted frame. Owns `pending` EXCLUSIVELY — it is the only
/// type whose methods can reach the bytes of a half-sent frame. See
/// [`Connection::split_writer`] for why the two writers may never be
/// shared.
///
/// The exclusivity is the reason this type exists. A JSONL stream cannot
/// resynchronise once two writers interleave a half-written line, so the tail of
/// a frame the OS only partly took must have exactly one owner; making that a
/// field of a type nobody can clone, share, or reach twice is what turns "two
/// tasks must not both send" from a comment into a structure.
pub struct ConnectionWriter {
    /// The write half of the shared duplex (see [`Connection::new`]'s split).
    writer: tokio::io::WriteHalf<DuplexBox>,
    /// The tail of a frame whose `write` was only partially accepted. Kept here
    /// so an abandoned `send_frame` cannot desync the stream.
    pending: Vec<u8>,
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
        // writer never fight over a single `&mut`. The write half is NOT spawned
        // anywhere and is NOT written from the reader side: it is kept in this
        // struct's slot, ready for [`Connection::split_writer`] to hand out once.
        let (read_half, write_half) = tokio::io::split(stream);
        // BOUNDED, and the reader waits for room — see [`MAX_QUEUED_INBOUND_FRAMES`]
        // for the measurement that made this a correctness issue rather than
        // hygiene.
        let (tx, lines) = mpsc::channel(MAX_QUEUED_INBOUND_FRAMES);

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
            loop {
                match read_capped_line(&mut reader).await {
                    LineRead::Complete(bytes) => {
                        // UTF-8 is checked here rather than by `read_line`
                        // (which validated it as it appended). A line that is not
                        // UTF-8 ENDS the stream, exactly as it did under
                        // `read_line`'s `Err(_) => break`: the permissive rule is
                        // about a line that is not JSON, not about a byte stream
                        // that is not text.
                        let Ok(line) = String::from_utf8(bytes) else {
                            break;
                        };
                        // `trim_end` drops the `\n` (and a `\r\n`'s `\r`). A
                        // blank line carries no frame, so it is skipped rather
                        // than surfaced as a decode error.
                        let line = line.trim_end();
                        if line.is_empty() {
                            continue;
                        }
                        // Awaits room. This task is never `select`ed over — the
                        // whole reason it is a task (see below) — so being parked
                        // here is safe: nothing can abandon it mid-line, and the
                        // line it already decoded is held in `line`, not lost.
                        // `Err` means the `Connection` is gone, which is the
                        // reader's own EOF condition.
                        if tx.send(decode(line)).await.is_err() {
                            break;
                        }
                    }
                    // EOF — the peer is gone. A final line with NO trailing
                    // newline still gets forwarded (a peer killed mid-write is
                    // exactly the frame worth keeping), matching
                    // `spawn_pipe_reader`.
                    LineRead::Eof(bytes) => {
                        if let Ok(tail) = String::from_utf8(bytes) {
                            let tail = tail.trim_end();
                            if !tail.is_empty() {
                                // Going away anyway, so the send result is moot —
                                // and `send` cannot block for long here, because
                                // the receiver is alive only while the
                                // `Connection` is.
                                let _ = tx.send(decode(tail)).await;
                            }
                        }
                        break;
                    }
                    // The cap fired: one error item naming the limit, then the
                    // stream ends. See [`MAX_INBOUND_LINE_BYTES`] for why the
                    // connection is closed rather than resynced.
                    LineRead::TooLong { limit } => {
                        let _ = tx.send(Err(TransportError::LineTooLong { limit })).await;
                        break;
                    }
                    // A read error is indistinguishable from a closed peer here;
                    // ending the channel ends the stream for the consumer.
                    LineRead::Failed => break,
                }
            }
            // Dropping `tx` here is what makes `Connection::recv` report EOF.
        });

        Self {
            // The write half waits here, whole, for `split_writer`. Keeping the
            // `ConnectionWriter` (and not just the `WriteHalf`) is what lets
            // `pending` leave with it in one piece: nothing in this type reads
            // either field.
            writer: Some(ConnectionWriter {
                writer: write_half,
                pending: Vec::new(),
            }),
            lines,
            reader,
        }
    }

    /// Return the connection's write half (and its exclusive `pending`),
    /// leaving this `Connection` owning only the reader.
    ///
    /// May be called at most ONCE. The returned [`ConnectionWriter`] is the ONLY
    /// way to send frames on this connection, full stop: `Connection` has no write
    /// method, so a second call is not a lesser evil but the only remaining mistake,
    /// and it panics (this is the same trust `tokio::io::split` gives).
    ///
    /// # Why both halves come back
    ///
    /// The caller keeps the reader (its `recv` is the connection's inbound) and
    /// hands the writer wherever the writes belong — for the daemon, to a writer
    /// task. Returning `(Self, ConnectionWriter)` rather than taking `&mut` is
    /// what makes the exclusivity mechanical: the original `Connection` cannot be
    /// used to ask twice, and the half that comes back has no writer slot left to
    /// take.
    ///
    /// # Panics
    ///
    /// If the writer has already been taken. That panic IS the two-writer hazard
    /// surfacing at the mistake rather than as a corrupted frame later; pinned by
    /// `a_second_split_writer_panics` in `tests/transport.rs`.
    pub fn split_writer(mut self) -> (Self, ConnectionWriter) {
        let writer = self.writer.take().expect("the writer was already taken");
        (self, writer)
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

impl ConnectionWriter {
    /// Serialize `frame` as one JSON object and write it as one `\n`-terminated
    /// line.
    ///
    /// This is the ONLY method in the transport that can reach `pending`, and
    /// taking `&mut self` is what makes it the only WRITER: two tasks cannot both
    /// be inside it on one handle, and there is no second handle
    /// ([`Connection::split_writer`] hands out the only one).
    ///
    /// Cancellation-safe in the sense that matters for a framing layer: the
    /// bytes of a frame already handed to `send_frame` are buffered in
    /// `pending` and drained on the next call, so abandoning this future
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

    /// Ask the OS to close the write direction of the whole duplex, even while
    /// the read half is still alive. `tokio::io::split`'s `WriteHalf` holds a
    /// SHARED `Arc<Inner<T>>` and has NO `Drop` impl, so dropping it does NOT
    /// close a socket while any other half's `Arc` is alive (verified in tokio
    /// 1.54 `src/io/split.rs`); a writer parked inside `send_frame`'s write loop
    /// would stall there forever. `poll_shutdown` on the shared stream is what
    /// makes FUTURE `write`s on this direction fail and tells the peer that this
    /// direction is over (EOF) — it does NOT reliably un-park a `write` already
    /// blocked inside the OS (shutdown(2) semantics: the wake latency of a parked
    /// send is kernel-dependent), so this method is the direction-closer + the
    /// peer-EOF signal, NOT a guaranteed parked-write breaker. For the daemon's
    /// connections the writer's parked `write` is broken by the PEER'S CLOSE or —
    /// on the reader-first path — by the socket's full close when the last `Arc`
    /// (the reader's `Connection` and the writer task's future) drops.
    ///
    /// MUST be called by the exiting writer AFTER `send_frame` returns (never
    /// from inside a `select!`). It is a best-effort close: an `Err` is ignored,
    /// the callers exit on their own schedules regardless.
    ///
    /// This is the ONLY place the transport reaches `poll_shutdown` on the shared
    /// duplex.
    ///
    /// # Why this takes `&mut self` and not `self`
    ///
    /// The plan (A1b) spells it `shutdown(mut self)`, which makes the direction
    /// close unobservable from the handle that asked for it — and the acceptance
    /// criterion for this split is a test named
    /// `a_send_against_a_shut_down_connection_errors_that_the_writer_survives`,
    /// whose first claim is that a send AFTER the shutdown errors. A consuming
    /// receiver cannot express that claim at all (there is no writer left to send
    /// on), so the signature is the one that keeps the plan's own test writable.
    /// It costs nothing at the call site: the daemon's writer calls
    /// `writer.shutdown().await` and the handle drops with the task either way.
    /// It does mean a caller MAY try to write again — and that write fails, which
    /// is the behaviour the test pins.
    pub async fn shutdown(&mut self) {
        let _ = self.writer.shutdown().await;
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        // Stop the reader task. Without this the task keeps the read half alive
        // and the socket is never fully closed by a dropped connection.
        self.reader.abort();
    }
}

/// One line read from a peer, and why it stopped.
///
/// `Debug` for the tests' failure messages.
#[derive(Debug)]
enum LineRead {
    /// A full line, terminator included (the caller `trim_end`s it).
    Complete(Vec<u8>),
    /// The peer closed. Carries whatever had accumulated, which may be a line
    /// with no terminator — still worth forwarding, see the reader task.
    Eof(Vec<u8>),
    /// [`MAX_INBOUND_LINE_BYTES`] was reached with no terminator in sight.
    TooLong { limit: usize },
    /// The stream errored.
    Failed,
}

/// Read one `\n`-terminated line, refusing anything longer than
/// [`MAX_INBOUND_LINE_BYTES`].
///
/// # Why not `AsyncBufReadExt::read_line`
///
/// Because `read_line` has no ceiling. It appends into its `String` until it sees
/// a newline, so a peer that sends bytes and never sends a `\n` makes it allocate
/// for as long as it can send — the second unbounded buffer finding 7 is about,
/// and one the queue bound does not touch: the bytes are in the line buffer, not
/// in the queue. (`read_until` is no better; it has the same shape over a `Vec`.)
///
/// # Why `fill_buf` / `consume` rather than a bounded read
///
/// It is the only pair that lets this look at bytes WITHOUT taking them. A
/// chunk-oriented loop over `read(&mut [u8; N])` would work, but it re-reads
/// nothing the peer has not already sent and it defeats the `BufReader`'s own
/// buffering; `fill_buf` says "here is what is buffered, take as much as you
/// want", so the newline scan and the length check run over kernel bytes with no
/// copying at all until a decision is made. The copy into `buf` happens once per
/// byte, and only for bytes that are actually being kept.
///
/// # Cancellation
///
/// NOT cancellation-safe, exactly like `read_line` — a future abandoned after it
/// consumed some bytes leaves the stream positioned mid-line. That is safe here
/// for the same reason the dedicated task makes `read_line` safe: this is awaited
/// only inside that task, which is never `select`ed over, so it is never
/// abandoned. Anyone tempted to wrap the reader in a `timeout` should read the
/// comment at the spawn site first.
async fn read_capped_line<R>(reader: &mut R) -> LineRead
where
    R: tokio::io::AsyncBufRead + Unpin,
{
    read_capped_line_under(reader, MAX_INBOUND_LINE_BYTES).await
}

/// [`read_capped_line`] with the limit as an argument.
///
/// The parameter exists so the boundary is testable: at the real 1 MiB a test
/// would have to write a megabyte to reach the edge and another to cross it, and
/// an off-by-one at the limit is exactly the bug worth pinning. The production
/// caller passes the constant and nothing else.
async fn read_capped_line_under<R>(reader: &mut R, limit: usize) -> LineRead
where
    R: tokio::io::AsyncBufRead + Unpin,
{
    let mut buf: Vec<u8> = Vec::new();
    loop {
        let chunk = match reader.fill_buf().await {
            // EOF. Whatever accumulated is the unterminated tail.
            Ok([]) => return LineRead::Eof(buf),
            Ok(chunk) => chunk,
            Err(_) => return LineRead::Failed,
        };

        // The terminator wins over the cap, but the cap is still checked HERE: a
        // line of exactly `MAX_INBOUND_LINE_BYTES` INCLUDING its `\n` is legal and
        // one byte more is not. Testing `buf.len() + at + 1` rather than only
        // `buf.len()` is what makes the documented number the real number —
        // otherwise the last chunk's worth of bytes (up to `BufReader`'s 8 KiB)
        // could ride in past it, and a bound the implementation quietly overshoots
        // is a bound no client can rely on.
        if let Some(at) = chunk.iter().position(|byte| *byte == b'\n') {
            if buf.len() + at + 1 > limit {
                return LineRead::TooLong { limit };
            }
            buf.extend_from_slice(&chunk[..=at]);
            reader.consume(at + 1);
            return LineRead::Complete(buf);
        }

        // No terminator in what is buffered. Keep what fits, and refuse the
        // instant it stops fitting — `buf` never exceeds the cap, so the
        // attacker's cost is the cap and not their patience.
        let room = limit.saturating_sub(buf.len());
        if room == 0 {
            return LineRead::TooLong { limit };
        }
        let take = chunk.len().min(room);
        buf.extend_from_slice(&chunk[..take]);
        reader.consume(take);
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

#[cfg(test)]
mod tests {
    use super::*;

    /// A `LineRead` rendered for a panic message: the SHAPE and a length, never
    /// the bytes. `#[derive(Debug)]` prints the whole payload, and the payload
    /// this file cares about is up to a megabyte — a test failure whose diagnosis
    /// is buried under a megabyte of `a`s is not a diagnosis.
    fn describe(read: &LineRead) -> String {
        match read {
            LineRead::Complete(bytes) => format!("Complete({} bytes)", bytes.len()),
            LineRead::Eof(bytes) => format!("Eof({} bytes)", bytes.len()),
            LineRead::TooLong { limit } => format!("TooLong {{ limit: {limit} }}"),
            LineRead::Failed => "Failed".to_string(),
        }
    }

    /// The reader, over an in-memory stream. A `BufReader` is the SAME
    /// `AsyncBufRead` the socket half runs on, so the chunking the boundary
    /// depends on is `BufReader`'s real 8 KiB `fill_buf` and not a simulation.
    fn reader_over(bytes: Vec<u8>) -> tokio::io::BufReader<std::io::Cursor<Vec<u8>>> {
        tokio::io::BufReader::new(std::io::Cursor::new(bytes))
    }

    /// A line of exactly the limit (terminator included) is COMPLETE, not too
    /// long — the limit is a limit, not an off-by-one.
    #[tokio::test]
    async fn a_line_of_exactly_the_limit_is_accepted() {
        let mut line = vec![b'x'; 63];
        line.push(b'\n');
        assert_eq!(line.len(), 64);
        let mut reader = reader_over(line);
        match read_capped_line_under(&mut reader, 64).await {
            LineRead::Complete(ref got) => assert_eq!(got.len(), 64),
            other => panic!(
                "exactly-at-limit must be a complete line, got {}",
                describe(&other)
            ),
        }
    }

    /// One byte over the limit is refused — and the refusal is the dedicated
    /// variant, not a decode error (the operator must be able to tell "your frame
    /// is too big" from "your frame is not JSON").
    #[tokio::test]
    async fn one_byte_over_the_limit_is_refused() {
        let mut line = vec![b'x'; 64];
        line.push(b'\n');
        let mut reader = reader_over(line);
        match read_capped_line_under(&mut reader, 64).await {
            LineRead::TooLong { limit } => assert_eq!(limit, 64),
            other => panic!(
                "one over the limit must be refused, got {}",
                describe(&other)
            ),
        }
    }

    /// The cap holds against a peer that never sends a terminator. This is the
    /// attack `read_line` had no answer for: bytes arrive, no newline, and the
    /// buffer would otherwise grow for as long as the peer can send.
    #[tokio::test]
    async fn an_unterminated_flood_is_refused_at_the_cap_and_no_further() {
        // 8 KiB of bytes and no `\n` anywhere — four times the cap.
        static FLOOD: &[u8] = &[b'a'; 8192];
        let mut reader = reader_over(FLOOD.to_vec());
        match read_capped_line_under(&mut reader, 256).await {
            LineRead::TooLong { limit } => assert_eq!(limit, 256),
            other => panic!(
                "an unterminated flood must hit the cap, got {}",
                describe(&other)
            ),
        }
    }

    /// EOF with an unterminated tail still forwards the tail — the rule the
    /// transport has always had (a peer killed mid-write is the frame worth
    /// keeping), which the cap must not silently change.
    #[tokio::test]
    async fn an_unterminated_tail_at_eof_is_still_delivered() {
        static TAIL: &[u8] = br#"{"half":"written"}"#;
        let mut reader = reader_over(TAIL.to_vec());
        match read_capped_line_under(&mut reader, 1024).await {
            LineRead::Eof(ref got) => assert_eq!(got, TAIL),
            other => panic!("EOF must report the tail, got {}", describe(&other)),
        }
    }

    /// A clean EOF reports an empty tail (nothing to forward).
    #[tokio::test]
    async fn a_clean_eof_reports_nothing() {
        let mut reader = reader_over(Vec::new());
        match read_capped_line_under(&mut reader, 1024).await {
            LineRead::Eof(ref got) => assert!(got.is_empty()),
            other => panic!("empty stream must be EOF, got {}", describe(&other)),
        }
    }

    /// The cap counts BYTES, not characters, because the limit is what the buffer
    /// costs — and a multi-byte character is the case where the two differ.
    #[tokio::test]
    async fn the_cap_counts_bytes_not_characters() {
        // Six 3-byte characters (`€` is U+20AC, 3 bytes in UTF-8) = 18 bytes,
        // plus the terminator. The count is asserted rather than trusted, because
        // the whole point of the test is that the number is BYTES: a source file
        // that normalised these to a different encoding would change the length
        // and silently change what the two arms below assert.
        let line = "\u{20AC}\u{20AC}\u{20AC}\u{20AC}\u{20AC}\u{20AC}\n".as_bytes();
        assert_eq!(line.len(), 19, "six 3-byte characters plus `\\n`");
        let mut reader = reader_over(line.to_vec());
        match read_capped_line_under(&mut reader, 19).await {
            LineRead::Complete(ref got) => assert_eq!(got.len(), 19),
            other => panic!(
                "19 bytes at a 19-byte cap must pass, got {}",
                describe(&other)
            ),
        }
        let mut reader = reader_over(line.to_vec());
        match read_capped_line_under(&mut reader, 18).await {
            LineRead::TooLong { .. } => {}
            other => panic!(
                "19 bytes at an 18-byte cap must be refused, got {}",
                describe(&other)
            ),
        }
    }

    /// The `gap`-shaped notice and the length error are the two things a client
    /// can be told that are not a frame it asked for, so both are named here.
    #[test]
    fn the_line_too_long_error_names_the_limit() {
        let err = TransportError::LineTooLong { limit: 1024 * 1024 };
        let message = err.to_string();
        assert!(
            message.contains(&format!("{}", 1024 * 1024)),
            "the message must carry the number the peer has to get under: {message}"
        );
    }
}

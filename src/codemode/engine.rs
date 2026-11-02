//! The codemode sandbox engine (ADR 0089): an embedded QuickJS runtime that
//! lives inside the Worker.
//!
//! One [`CodemodeEngine`] is one `AsyncRuntime` + one `AsyncContext`, built with
//! the turn's [`CancellationToken`] and an [`EngineLimits`] budget. Cancellation
//! is two-part by design (spec §7.1):
//!
//! 1. **the interrupt handler** — QuickJS calls it while bytecode executes, and
//!    it raises an *uncatchable* exception. This is the only mechanism that can
//!    stop a synchronously spinning script.
//! 2. **the [`race_turn`] select!** — races the script future against the turn
//!    token and the wall clock, so a script parked on a host `await` (running no
//!    bytecode, so part 1 is silent) is still stoppable.
//!
//! The engine owns the ENGINE and nothing else: the `tools.*` bridge, the
//! result shaping and the per-call lifetime are task C2's job.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use tokio_util::sync::CancellationToken;

/// The codemode sandbox memory cap — spec §7.1's 256 MiB. C2's `EngineLimits`
/// should set `memory_bytes` to this (the engine itself does not default
/// limits; it applies whatever it is given). Tests use a smaller cap for
/// speed; this constant is the production figure.
pub const CODEMODE_MEMORY_LIMIT: usize = 256 * 1024 * 1024;

/// The engine's resource limits, applied at construction.
/// `0` means unlimited for a byte limit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EngineLimits {
    /// QuickJS-managed memory cap (bytes) — `AsyncRuntime::set_memory_limit`.
    /// rquickjs documents this as a no-op under the `rust-alloc`/`allocator`
    /// features (see the note on the dependency in `Cargo.toml`, where the
    /// measured behaviour on 0.14.0 is recorded too). That it is installed at
    /// all is pinned by
    /// `memory_cap_is_installed_not_merely_requested`, and that it enforces by
    /// `a_tiny_cap_surfaces_as_a_script_end_not_a_hang`.
    pub memory_bytes: usize,
    /// JS stack cap (bytes) — `AsyncRuntime::set_max_stack_size`.
    /// The crate default is 256 * 1024.
    pub stack_bytes: usize,
    /// The wall-clock bound for one script run.
    pub wall_clock: Duration,
}

/// How one script run ended, from the engine's point of view. Carrying the
/// script's VALUE is the bridge's job — rquickjs values cannot leave
/// `async_with` (the `for<'js>` bound), so the engine reports completion only.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScriptEnd {
    /// The script ran to completion without an engine exception.
    Done,
    /// The script threw (the JS exception message).
    Failed(String),
    /// The turn's `CancellationToken` fired while the script (or a host
    /// await inside it) was running.
    Cancelled,
    /// `EngineLimits::wall_clock` elapsed before the script finished.
    TimedOut,
}

/// The engine error type — `thiserror`-derived, matching the tree's
/// `agent/errors.rs` convention of small owned enums.
#[derive(Debug, thiserror::Error)]
pub enum EngineError {
    /// `AsyncRuntime::new` or `AsyncContext::full` failed.
    #[error("quickjs engine construction failed: {0}")]
    Construction(#[from] rquickjs::Error),
    /// The turn token was already cancelled when `new` was called.
    #[error("the turn is already cancelled")]
    AlreadyCancelled,
    /// Evaluated with an empty/whitespace-only `code` string.
    #[error("script is empty")]
    EmptyScript,
}

/// The sentinel `WallClock::deadline_ns` value for "no deadline installed".
const NO_DEADLINE: u64 = u64::MAX;

/// The per-engine wall-clock deadline, shared between `eval` (which arms it)
/// and the interrupt handler (which reads it).
///
/// WHY THIS EXISTS: a synchronous `Ctx::eval` blocks the task that runs it, so
/// `race_turn`'s `sleep` arm can never be polled while a hot loop spins — the
/// only thing that can end a spinning script is the interrupt handler. The
/// handler therefore watches BOTH the turn token and this deadline, which makes
/// `wall_clock` enforce itself inside bytecode (`while (true) {}`) as well as
/// across a host `await`.
struct WallClock {
    /// The instant this clock was created; deadlines are nanos after it.
    base: Instant,
    /// Nanos after `base` at which the running script is interrupted, or
    /// [`NO_DEADLINE`]. Written by whoever owns the runtime lock (the one
    /// in-flight `eval`), read from the interrupt handler on the same thread.
    deadline_ns: AtomicU64,
}

impl WallClock {
    fn new() -> Self {
        Self {
            base: Instant::now(),
            deadline_ns: AtomicU64::new(NO_DEADLINE),
        }
    }

    /// Start a deadline `wall_clock` from now. Called at the top of the eval
    /// closure, so a future cancelled before it ever polls arms nothing.
    /// The stored value is ABSOLUTE (nanos after `base`), not relative — a
    /// second `eval` on the same engine must not inherit the first's expiry.
    fn arm(&self, wall_clock: Duration) {
        let at = self.base.elapsed().as_nanos() as u64 + wall_clock.as_nanos() as u64;
        self.deadline_ns.store(at, Ordering::Release);
    }

    /// Clear the deadline (only the owner of the runtime lock calls this, and
    /// a sync eval always reaches it — the closure cannot be dropped mid-eval).
    fn disarm(&self) {
        self.deadline_ns.store(NO_DEADLINE, Ordering::Release);
    }

    /// True when an installed deadline has passed. Reads one atomic and, when
    /// armed, one monotonic clock — the handler runs every 10 000 bytecodes
    /// (`JS_INTERRUPT_COUNTER_INIT`), so this is not a hot-path concern.
    fn expired(&self) -> bool {
        let at = self.deadline_ns.load(Ordering::Acquire);
        at != NO_DEADLINE && self.base.elapsed().as_nanos() as u64 >= at
    }
}

/// The engine handle: one `AsyncRuntime` + one `AsyncContext`, with the
/// memory/stack limits applied and the interrupt handler installed
/// (part 1 of the two-part cancellation).
pub struct CodemodeEngine {
    runtime: rquickjs::AsyncRuntime,
    context: rquickjs::AsyncContext,
    /// The TURN token (not the session token) — C2 passes `AgentLoop::turn_cancel`.
    turn: CancellationToken,
    /// Copied out of `limits.wall_clock` at construction.
    wall_clock: Duration,
    /// The same clock the interrupt handler watches (see [`WallClock`]).
    clock: std::sync::Arc<WallClock>,
}

impl CodemodeEngine {
    /// Build the runtime, apply `limits`, and install the interrupt handler:
    /// `set_interrupt_handler(Some(Box::new(move || cancel.is_cancelled())))`.
    pub async fn new(cancel: CancellationToken, limits: EngineLimits) -> Result<Self, EngineError> {
        if cancel.is_cancelled() {
            return Err(EngineError::AlreadyCancelled);
        }

        let runtime = rquickjs::AsyncRuntime::new()?;
        // Apply the byte limits to the FRESH runtime, before the context is
        // born, so the context's whole life sits inside the cap. `0` means
        // unlimited — QuickJS's own convention for both setters.
        if limits.memory_bytes > 0 {
            runtime.set_memory_limit(limits.memory_bytes).await;
        }
        if limits.stack_bytes > 0 {
            runtime.set_max_stack_size(limits.stack_bytes).await;
        }
        let context = rquickjs::AsyncContext::full(&runtime).await?;

        // Part 1 of the two-part cancellation. The handler is called while
        // bytecode executes and returns `true` to raise an UNcatchable
        // exception; a parked script runs no bytecode, which is exactly why
        // part 2 (`race_turn`) exists.
        let handler_token = cancel.clone();
        let clock = std::sync::Arc::new(WallClock::new());
        let handler_clock = std::sync::Arc::clone(&clock);
        runtime
            .set_interrupt_handler(Some(Box::new(move || {
                handler_token.is_cancelled() || handler_clock.expired()
            })))
            .await;

        Ok(Self {
            runtime,
            context,
            turn: cancel,
            wall_clock: limits.wall_clock,
            clock,
        })
    }

    pub fn runtime(&self) -> &rquickjs::AsyncRuntime {
        &self.runtime
    }

    pub fn context(&self) -> &rquickjs::AsyncContext {
        &self.context
    }

    /// Passthrough to `AsyncContext::with` — the bridge's entry point
    /// for installing `tools.*` and for reading/converting results to owned
    /// Rust values (no `Value<'js>` may escape the closure).
    pub async fn with_context<F, R>(&self, f: F) -> R
    where
        F: for<'js> FnOnce(rquickjs::Ctx<'js>) -> R + rquickjs::markers::ParallelSend,
        R: rquickjs::markers::ParallelSend,
    {
        self.context.with(f).await
    }

    /// Run `code` with the two-part cancellation: (1) the interrupt handler
    /// installed in `new` fires while JS executes; (2) the script future —
    /// `self.with_context(|ctx| ...)` — is raced with `turn.cancelled()` and
    /// the `wall_clock` `sleep` in a `tokio::select!`, so a script parked on a
    /// host await is still stoppable.
    pub async fn eval(&self, code: &str) -> ScriptEnd {
        if code.trim().is_empty() {
            return ScriptEnd::Failed(EngineError::EmptyScript.to_string());
        }

        let script = self.with_context(|ctx| {
            // The wall clock is armed INSIDE the closure so it measures the
            // script, not the queueing before it.
            self.clock.arm(self.wall_clock);
            let result: rquickjs::Result<()> = ctx.eval::<(), _>(code);
            let end = match result {
                Ok(()) => ScriptEnd::Done,
                Err(err) => {
                    // `rquickjs::Error` is `#[non_exhaustive]` and has no
                    // `PartialEq`, so the exception test is a `matches!`.
                    if matches!(err, rquickjs::Error::Exception) {
                        self.exception_verdict(ctx)
                    } else {
                        ScriptEnd::Failed(err.to_string())
                    }
                }
            };
            self.clock.disarm();
            end
        });

        match race_turn(script, &self.turn, self.wall_clock).await {
            // The closure's own verdict (Done | Failed | Cancelled | TimedOut).
            Ok(end) => end,
            Err(ScriptEnd::Cancelled) => ScriptEnd::Cancelled,
            Err(ScriptEnd::TimedOut) => ScriptEnd::TimedOut,
            Err(ScriptEnd::Done | ScriptEnd::Failed(_)) => {
                unreachable!("race_turn returns Done/Failed only on the Ok arm")
            }
        }
    }

    /// The exception-path rule: the interrupt handler raises an UNcatchable
    /// exception, so an interrupted script arrives HERE as `Error::Exception`.
    /// The turn token is checked FIRST and the wall clock second, so a
    /// cancelled or timed-out hot loop reports `Cancelled` / `TimedOut` and NOT
    /// `Failed` — which is what makes those two outcomes deterministic instead
    /// of a coin-flip against `race_turn`'s already-ready select! arms.
    fn exception_verdict(&self, ctx: rquickjs::Ctx<'_>) -> ScriptEnd {
        if self.turn.is_cancelled() {
            // An interrupt-exception because the token fired — NOT a throw.
            return ScriptEnd::Cancelled;
        }
        if self.clock.expired() {
            // The same mechanism, armed by the wall clock instead of the token.
            return ScriptEnd::TimedOut;
        }
        // A genuine script throw. `Ctx::catch` takes the pending exception out;
        // `Exception::from_object` takes the `Object` BY VALUE, hence `.cloned()`.
        let message = ctx
            .catch()
            .as_object()
            .cloned()
            .and_then(rquickjs::Exception::from_object)
            .and_then(|exception| exception.message())
            // A thrown NON-Error value (a string, a number, …) has no message.
            .unwrap_or_else(|| "unknown exception".to_string());
        ScriptEnd::Failed(message)
    }
}

/// The standalone cancellation wrapper (the same part-2 race as
/// `CodemodeEngine::eval`, exposed so the bridge — or any nested call — can
/// race an arbitrary future against the turn token and a wall-clock bound).
pub async fn race_turn<T>(
    fut: impl std::future::Future<Output = T>,
    turn: &CancellationToken,
    wall_clock: Duration,
) -> std::result::Result<T, ScriptEnd> {
    tokio::select! {
        result = fut => Ok(result),
        _ = turn.cancelled() => Err(ScriptEnd::Cancelled),
        _ = tokio::time::sleep(wall_clock) => Err(ScriptEnd::TimedOut),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// THE SPIKE ARTIFACT — the permanent guard that the engine stays
    /// `Send + Sync`, which the Worker's `tokio::spawn(loop_.run())` requires.
    /// Without rquickjs's `parallel` feature this fails to COMPILE: that
    /// failure was the spike evidence that `parallel` is required (ADR 0089).
    ///
    /// A plain helper, NOT a `#[test]` itself — rustc rejects a test function
    /// with non-lifetime generics ("functions used as tests can not have any
    /// non-lifetime generic parameters"); `engine_is_send_sync` below is the
    /// test that instantiates it, exactly as the plan's text describes.
    fn assert_send_sync<T: Send + Sync>() {}

    #[test]
    fn engine_is_send_sync() {
        assert_send_sync::<CodemodeEngine>();
    }

    /// Build an engine with a test-sized cap. Returns the engine and the TURN
    /// token it was given (the engine holds its own clone).
    async fn test_engine(
        memory_bytes: usize,
        wall_clock: Duration,
    ) -> (CodemodeEngine, CancellationToken) {
        let turn = CancellationToken::new();
        let limits = EngineLimits {
            memory_bytes,
            stack_bytes: 256 * 1024,
            wall_clock,
        };
        let engine = CodemodeEngine::new(turn.clone(), limits)
            .await
            .expect("engine construction");
        (engine, turn)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn eval_returns_done_for_a_completing_script() {
        let (engine, _turn) = test_engine(CODEMODE_MEMORY_LIMIT, Duration::from_secs(5)).await;
        assert_eq!(engine.eval("1 + 1").await, ScriptEnd::Done);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn eval_reports_a_throwing_script_as_failed() {
        let (engine, _turn) = test_engine(CODEMODE_MEMORY_LIMIT, Duration::from_secs(5)).await;
        let end = engine.eval("throw new Error('boom')").await;
        match end {
            ScriptEnd::Failed(message) => assert!(
                message.contains("boom"),
                "the JS exception message must reach the caller, got: {message}"
            ),
            other => panic!("expected Failed, got {other:?}"),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn eval_rejects_an_empty_script() {
        let (engine, _turn) = test_engine(CODEMODE_MEMORY_LIMIT, Duration::from_secs(5)).await;
        let end = engine.eval("   ").await;
        assert!(
            matches!(end, ScriptEnd::Failed(ref m) if m == "script is empty"),
            "whitespace-only code is the EmptyScript route, got {end:?}"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn new_rejects_an_already_cancelled_turn() {
        let turn = CancellationToken::new();
        turn.cancel();
        let limits = EngineLimits {
            memory_bytes: 0,
            stack_bytes: 0,
            wall_clock: Duration::from_secs(1),
        };
        // Destructured rather than `expect_err`, which would demand a `Debug`
        // impl on the frozen `CodemodeEngine` (the contract gives it none).
        let error = match CodemodeEngine::new(turn, limits).await {
            Ok(_) => panic!("a dead turn must not build an engine"),
            Err(error) => error,
        };
        assert!(matches!(error, EngineError::AlreadyCancelled), "{error:?}");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn eval_timeouts_on_wall_clock() {
        // A hot loop blocks the task that runs it, so `race_turn`'s sleep arm
        // alone could NEVER win — the interrupt handler must watch the wall
        // clock too (see `WallClock`). Both routes report TimedOut, so the
        // outcome does not depend on select! arm order.
        let (engine, _turn) = test_engine(CODEMODE_MEMORY_LIMIT, Duration::from_millis(50)).await;
        let end = tokio::time::timeout(Duration::from_secs(5), engine.eval("while (true) {}"))
            .await
            .expect("the wall clock must end the hot loop, not hang");
        assert_eq!(end, ScriptEnd::TimedOut);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn interrupt_handler_stops_a_busy_loop() {
        // Part 1 alone: the token fires while bytecode runs. Deterministic
        // because of the exception-path rule — the closure checks the token
        // BEFORE falling back to Failed, so both the closure's return and
        // `race_turn`'s token arm yield Cancelled.
        let (engine, turn) = test_engine(CODEMODE_MEMORY_LIMIT, Duration::from_secs(30)).await;
        let canceller = turn.clone();
        let waiter = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            canceller.cancel();
        });
        let end = tokio::time::timeout(Duration::from_secs(5), engine.eval("while (true) {}"))
            .await
            .expect("the interrupt handler must stop the loop, not hang");
        waiter.await.expect("canceller task");
        assert_eq!(end, ScriptEnd::Cancelled);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn eval_cancels_on_turn_token() {
        // THE PARKED-CANCEL TEST — route (a). This does NOT run through
        // `engine.eval`: production `eval` is a SYNC `with` closure and can
        // never park (`EvalOptions { promise: false }` is `Ctx::eval`'s
        // default). It exercises the exact parked recipe a host-awaiting
        // bridge call uses, through the frozen `context()` accessor and the
        // frozen standalone `race_turn` — the same race `eval` runs.
        let (engine, turn) = test_engine(CODEMODE_MEMORY_LIMIT, Duration::from_secs(30)).await;

        let parked = engine.context().async_with(async |ctx| {
            // `function::Async` turns a Rust future into a JS-callable that
            // returns a Promise (`Promise::wrap_future`). ZERO JS args, because
            // JS-visible arguments must be `FromJs`-convertible — a token could
            // not be passed, so the never-resolving park is what is captured.
            ctx.globals().set(
                "blockOnCancel",
                rquickjs::Function::new(
                    ctx.clone(),
                    rquickjs::function::Async(|| async move {
                        // NEVER resolves: the only thing that can end this is
                        // part 2 dropping the future. (Awaiting the token HERE
                        // would resolve in the same instant as the select!'s
                        // token arm and race it — the coin-flip this avoids.)
                        std::future::pending::<()>().await;
                        rquickjs::Result::Ok(())
                    }),
                ),
            )?;
            // LOAD-BEARING top-level `await`: `eval_promise` is the
            // `EvalOptions { promise: true }` eval, so a top-level await keeps
            // the eval promise pending. Defining `async () => { await … }`
            // without INVOKING it builds a function object whose body never
            // runs, the promise settles in microseconds, and this whole test
            // silently proves nothing.
            ctx.eval_promise("await blockOnCancel();")?
                .into_future::<()>()
                .await?;
            rquickjs::Result::Ok(()) // NEVER reached — the park never resolves
        });

        let mut fut = Box::pin(race_turn(parked, &turn, Duration::from_secs(10)));

        // The guard that makes a false pass impossible: if the eval future were
        // NOT genuinely parked, it would resolve here and this would be Ok.
        assert!(
            tokio::time::timeout(Duration::from_millis(500), &mut fut)
                .await
                .is_err(),
            "the parked script must STILL be pending before the token fires — \
             a resolved future means the script never parked"
        );

        turn.cancel();
        assert!(
            matches!(fut.await, Err(ScriptEnd::Cancelled)),
            "only the token arm can win a park that never resolves"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn memory_cap_rejects_a_blowup() {
        // `set_memory_limit` counts QuickJS-managed memory, NOT process RSS —
        // so the assertion is on `ScriptEnd`, never on RSS. Measured here the
        // message is `"unknown exception"`: QuickJS throws `JS_NULL` on OOM to
        // avoid recursing inside the allocator (quickjs.c "out of memory: throw
        // JS_NULL to avoid recursing"), which is not an Error instance, so
        // `Exception::from_object` yields `None` and the documented fallback
        // string is what reaches us. The CONTRACT this test holds is that the
        // cap turns the blowup into a `Failed`, not a hang and not a `Done`.
        let (engine, _turn) = test_engine(64 * 1024 * 1024, Duration::from_secs(5)).await;
        let end = engine.eval(BLOWUP).await;
        assert!(
            matches!(end, ScriptEnd::Failed(_)),
            "the memory cap must surface as a throw, got {end:?}"
        );
    }

    /// The cap the pin test below asks for. Deliberately small and explicit —
    /// 8 MiB against the 256 MiB production figure and the 64 MiB the behaviour
    /// tests use — so the assertion reads as a statement about the MECHANISM
    /// ("the number I asked for is the number the runtime reports") rather than
    /// about any particular budget. It also keeps the negative control fast.
    const PIN_MEMORY_LIMIT: usize = 8 * 1024 * 1024;

    /// The blowup script, shared by the two cap tests. 1e7 iterations of a
    /// 100-char string is ~1 GB of JS strings: far over every cap in this file,
    /// so the ONLY thing that can end it is a limit that is actually installed.
    const BLOWUP: &str = "const a = []; for (let i = 0; i < 1e7; i++) { a.push('x'.repeat(100)); }";

    /// FINDING 1's pin: the memory cap is INSTALLED, not merely requested.
    ///
    /// WHY THIS TEST EXISTS. `AsyncRuntime::set_memory_limit`'s own doc comment
    /// (rquickjs-core-0.14.0/src/runtime/async.rs:204-210) says it "is a Noop
    /// when a custom allocator is being used, as is the case for the
    /// 'rust-alloc' or 'allocator' features". Those features are transitive, so
    /// any dependency that turns one on turns the 256 MiB cap into a comment —
    /// and `memory_cap_rejects_a_blowup` cannot see it, because it measures
    /// BEHAVIOUR at a cap that would then simply be absent: an uncapped heap
    /// does not throw, it grows until the OOM killer takes the process.
    ///
    /// WHAT THIS ACTUALLY OBSERVES, and what it does NOT — stated plainly
    /// because a guard that overstates itself is worse than no guard:
    ///
    ///   `AsyncRuntime::memory_usage` (async.rs:248) fills a
    ///   `rquickjs::runtime::MemoryUsage` (= `rquickjs_sys::JSMemoryUsage`) via
    ///   `JS_ComputeMemoryUsage`, which copies `malloc_limit` straight out of
    ///   `rt->malloc_state.malloc_limit` (quickjs.c:7583) — the SAME field
    ///   `JS_SetMemoryLimit` writes (quickjs.c:2443). So this reads the limit
    ///   back out of the C runtime through a path the setter does not touch.
    ///
    ///   It therefore detects the cap never being installed: never calling the
    ///   setter, calling it with the wrong number, a future `EngineLimits` that
    ///   drops `memory_bytes`, or a rquickjs that makes the setter conditional
    ///   on the allocator and skips the write. In each case the reported limit
    ///   is 0 (= QuickJS's "unlimited", quickjs.c:2321) or a different number,
    ///   and this assertion goes RED. The control below PROVES that: a runtime
    ///   nobody set a limit on reports 0, not a plausible-looking default — so
    ///   the equality is discriminating and not tautological.
    ///
    ///   It does NOT detect a limit that is RECORDED but not ENFORCED — which
    ///   is the literal shape of the upstream "no-op" warning. Measured on this
    ///   tree, that configuration does not occur: the cap check lives in
    ///   `js_malloc_rt` (quickjs.c:2017), ABOVE the `rt->mf.js_malloc` allocator
    ///   dispatch, so it runs whichever allocator is installed, and a
    ///   `--features rquickjs/rust-alloc` build of this exact dependency set
    ///   still refused an 8 MiB blowup (`malloc_size` plateaued at 8 387 864
    ///   bytes against the 8 388 608 requested). The enforcement half is
    ///   `a_tiny_cap_surfaces_as_a_script_end_not_a_hang`; the two together
    ///   cover install + enforce, and neither alone does.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn memory_cap_is_installed_not_merely_requested() {
        let (engine, _turn) = test_engine(PIN_MEMORY_LIMIT, Duration::from_secs(5)).await;
        let reported = engine.runtime().memory_usage().await.malloc_limit;
        assert_eq!(
            usize::try_from(reported).expect("malloc_limit is a byte count, never negative"),
            PIN_MEMORY_LIMIT,
            "the cap CodemodeEngine::new asked for is not the cap the QuickJS \
             runtime reports — set_memory_limit is a no-op in this build, so the \
             memory limit is NOT enforcing anything (requested {PIN_MEMORY_LIMIT}, \
             runtime reports {reported})"
        );

        // THE ANTI-TAUTOLOGY CONTROL, inside the test that depends on it. If
        // `malloc_limit` reported something non-zero for an untouched runtime,
        // the assertion above could pass on a build where the setter does
        // nothing, and this whole pin would be a decoration. 0 is QuickJS's
        // encoding of "unlimited" — exactly what a silent no-op leaves behind.
        let untouched = rquickjs::AsyncRuntime::new()
            .expect("fresh runtime for the control")
            .memory_usage()
            .await
            .malloc_limit;
        assert_eq!(
            untouched, 0,
            "a runtime with NO limit installed must report 0, or the assertion \
             above proves nothing (it reported {untouched})"
        );
    }

    /// FINDING 1's negative control: a cap small enough to be crossed for
    /// certain, asserted to end the run as a `ScriptEnd`.
    ///
    /// THE LOAD-BEARING DETAIL is the two durations. `wall_clock` is 30 s and
    /// the outer guard is 10 s, so the wall clock CANNOT be what ends this run:
    /// the only mechanism that can is the memory cap. Swap the two (a 5 s wall
    /// clock, say) and this passes with no cap installed at all — which is the
    /// vacuous-green shape `memory_cap_rejects_a_blowup` alone cannot rule out.
    ///
    /// Asserting `ScriptEnd` rather than "the process survived" is the cheap
    /// version of the reviewer's hang concern, and measured under the mutation
    /// above it fails as `Done` — with no cap installed the 1 GB allocation
    /// simply COMPLETES and the script reports success. That is the worse
    /// failure and the one this catches: the sandbox answering a 1 GB blowup
    /// with `Done` instead of refusing it. A hang or an OOM-kill would be the
    /// other route and both are covered too, since `expect` on the timeout is a
    /// failure and not a pass.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_tiny_cap_surfaces_as_a_script_end_not_a_hang() {
        let (engine, _turn) = test_engine(PIN_MEMORY_LIMIT, Duration::from_secs(30)).await;
        let end = tokio::time::timeout(Duration::from_secs(10), engine.eval(BLOWUP))
            .await
            .expect("the 8 MiB cap must end the blowup well inside its 30 s wall clock");
        assert!(
            matches!(end, ScriptEnd::Failed(_)),
            "an allocation over the installed cap must surface as a throw, got {end:?}"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn with_context_reads_and_writes_globals() {
        let (engine, _turn) = test_engine(CODEMODE_MEMORY_LIMIT, Duration::from_secs(5)).await;
        let value = engine
            .with_context(|ctx| {
                ctx.globals().set("x", 42)?;
                let read: i32 = ctx.globals().get("x")?;
                rquickjs::Result::Ok(read)
            })
            .await;
        assert_eq!(value.expect("global round-trip"), 42);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn race_turn_wraps_an_arbitrary_future() {
        let turn = CancellationToken::new();
        let ok = race_turn(async { "ok" }, &turn, Duration::from_secs(1))
            .await
            .expect("a ready future wins its own race");
        assert_eq!(ok, "ok");

        let dead = CancellationToken::new();
        dead.cancel();
        let cancelled = race_turn(std::future::pending::<()>(), &dead, Duration::from_secs(10))
            .await
            .expect_err("a cancelled token ends the race");
        assert_eq!(cancelled, ScriptEnd::Cancelled);

        let slow = race_turn(
            std::future::pending::<()>(),
            &turn,
            Duration::from_millis(10),
        )
        .await
        .expect_err("the wall clock ends the race");
        assert_eq!(slow, ScriptEnd::TimedOut);
    }
}

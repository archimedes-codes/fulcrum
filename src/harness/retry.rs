//! The provider retry policy (native-agent-harness Task 6): on
//! `ProviderError::Retryable`, exponential backoff (1s, 2s, 4s, … up to
//! 5 attempts) re-calling the model; `Fatal` / `Auth` are returned
//! verbatim. Emits `auto_retry_start` / `auto_retry_end` (the loop
//! passes the event emitter in — `auto_retry_end { success: true }` is
//! the loop's, via [`RetryPolicy::note_success`]; the exhaustion
//! `auto_retry_end { success: false }` is emitted here).
//!
//! The attempt budget is shared per TURN (the `attempt` counter
//! persists across `call_with_retry` invocations): a mid-stream
//! `ProviderEvent::Error(Retryable)` re-enters the budget via
//! [`RetryPolicy::next_retry_delay`], so a turn can never make more
//! than `max_attempts` model calls.

use std::future::Future;
use std::time::Duration;

use tokio_util::sync::CancellationToken;

use crate::agent::harness::provider::ProviderError;
use crate::agent::rpc::RpcEvent;

/// The default attempt budget (the plan's N=5).
const DEFAULT_MAX_ATTEMPTS: u64 = 5;

/// The provider retry policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetryPolicy {
    max_attempts: u64,
    base_delay: Duration,
    /// The current attempt number within the turn (the first attempt is
    /// `1`; `0` before the first call).
    attempt: u64,
    /// The `auto_retry_end { success: true }` was already reported for
    /// the current retry (cleared by the next `next_retry_delay`, so a
    /// LATER retry in the same session reports again).
    reported: bool,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self::new()
    }
}

impl RetryPolicy {
    /// 5 attempts, a 1 s base delay (1s, 2s, 4s, 8s).
    pub fn new() -> Self {
        Self {
            max_attempts: DEFAULT_MAX_ATTEMPTS,
            base_delay: Duration::from_secs(1),
            attempt: 0,
            reported: false,
        }
    }

    /// A custom budget + base delay (tests use a millisecond base).
    pub fn new_with(max_attempts: u64, base_delay: Duration) -> Self {
        Self {
            max_attempts,
            base_delay,
            attempt: 0,
            reported: false,
        }
    }

    pub fn max_attempts(&self) -> u64 {
        self.max_attempts
    }

    /// The current attempt number (for the `auto_retry_start` event).
    pub fn attempt(&self) -> u64 {
        self.attempt
    }

    /// The delay for the NEXT retry (recording that attempt), or
    /// `None` when the budget is exhausted (the next attempt number
    /// must be ≤ `max_attempts`). The first retry is the base delay
    /// (1s), then doubles (1s, 2s, 4s, …).
    pub fn next_retry_delay(&mut self) -> Option<Duration> {
        if self.attempt + 1 > self.max_attempts {
            return None;
        }
        let delay =
            self.base_delay * 2u32.saturating_pow(self.attempt.saturating_sub(1).min(30) as u32);
        self.attempt += 1;
        self.reported = false;
        Some(delay)
    }

    /// Record a successful call: `Some(attempt)` ONCE per retry — when
    /// the turn used a retry and the `auto_retry_end { success: true }`
    /// was not reported yet (the caller emits it); a second successful
    /// call (no new retry) reports nothing. A later retry (a new
    /// `next_retry_delay`) arms the report again.
    pub fn note_success(&mut self) -> Option<u64> {
        self.attempt = self.attempt.max(1);
        if self.attempt > 1 && !self.reported {
            self.reported = true;
            Some(self.attempt)
        } else {
            None
        }
    }

    /// Attempt the call with retries: on `Retryable`, an exponential
    /// backoff (recording the attempt + emitting `auto_retry_start`),
    /// re-calling `f`; on `Fatal` / `Auth`, return the error verbatim
    /// (a non-retryable error is never retried).
    ///
    /// The backoff `sleep` RACES `cancel` (finding 8 / the backoff nit):
    /// a cancelled turn must not wait out the delay (up to 8 s) — the
    /// cancel surfaces a non-retryable `Fatal` "cancelled" error (the
    /// loop settles the turn on it).
    ///
    /// (An `impl Future` return — a `for<'a>` HRTB over the return type is
    /// not expressible for a boxed-future `Output` (E0582). `Send` bounds
    /// so the loop's spawned task can await it.)
    pub fn call_with_retry<'a, F, T, Fut>(
        &'a mut self,
        mut f: F,
        cancel: &'a CancellationToken,
        emit: &'a mut (dyn FnMut(RpcEvent) + Send),
    ) -> impl Future<Output = Result<T, ProviderError>> + 'a + Send
    where
        F: FnMut() -> Fut + 'a + Send,
        Fut: Future<Output = Result<T, ProviderError>> + 'a + Send,
        // `T` is held across the `sleep` await (a failed attempt's result
        // outlives the retry delay).
        T: Send,
    {
        let self_ = self;
        async move {
            self_.attempt = self_.attempt.max(1);
            loop {
                let result = f().await;
                match result {
                    Ok(t) => return Ok(t),
                    Err(e @ ProviderError::Retryable(_)) => {
                        let Some(delay) = self_.next_retry_delay() else {
                            emit(RpcEvent::auto_retry_end {
                                success: false,
                                attempt: self_.attempt,
                                final_error: Some(e.to_string()),
                            });
                            return Err(e);
                        };
                        emit(RpcEvent::auto_retry_start {
                            attempt: self_.attempt,
                            max_attempts: self_.max_attempts,
                            delay_ms: delay.as_millis() as u64,
                            error_message: e.to_string(),
                        });
                        // The backoff RACES the cancel (finding 8 / the
                        // backoff nit — a cancelled turn must not wait
                        // out the delay): a non-retryable `Fatal`
                        // "cancelled" surfaces (the loop settles the
                        // turn on it — it does NOT re-call `f`).
                        tokio::select! {
                            _ = cancel.cancelled() => {
                                emit(RpcEvent::auto_retry_end {
                                    success: false,
                                    attempt: self_.attempt,
                                    final_error: Some(
                                        "the turn was cancelled during the retry backoff".to_string(),
                                    ),
                                });
                                return Err(ProviderError::Fatal(
                                    "the turn was cancelled during the retry backoff".into(),
                                ));
                            }
                            _ = tokio::time::sleep(delay) => {}
                        }
                    }
                    Err(e) => return Err(e),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::future::BoxFuture;

    /// A canned-response closure factory (fails `n - 1` times with
    /// `Retryable`, then succeeds with `n`).
    fn flaky_failing(
        n: u32,
    ) -> (
        impl FnMut() -> BoxFuture<'static, Result<u32, ProviderError>>,
        std::sync::Arc<std::sync::atomic::AtomicU32>,
    ) {
        let calls = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0));
        let calls_inner = calls.clone();
        (
            move || {
                let c = calls_inner.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
                Box::pin(async move {
                    if c < n {
                        Err(ProviderError::Retryable(format!("transient {c}")))
                    } else {
                        Ok(c)
                    }
                })
            },
            calls,
        )
    }

    #[tokio::test]
    async fn a_cancel_during_the_backoff_returns_fast() {
        // The backoff nit: a 30 s delay + a cancel 50 ms in → the call
        // returns a non-retryable `Fatal` "cancelled" FAST (it does NOT
        // wait out the delay, and it does NOT re-call `f`).
        let mut policy = RetryPolicy::new_with(5, Duration::from_secs(30));
        let calls = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0));
        let calls_inner = calls.clone();
        let f = move || {
            let c = calls_inner.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
            Box::pin(async move {
                let _ = c;
                Err(ProviderError::Retryable("transient".to_string()))
            })
        };
        let cancel = CancellationToken::new();
        // The task gets a CLONE (a `CancellationToken` is cheap to clone —
        // the outer `cancel` must outlive the task to fire it).
        let task_cancel = cancel.clone();
        let task = tokio::spawn(async move {
            let r: Result<u32, ProviderError> =
                policy.call_with_retry(f, &task_cancel, &mut |_| {}).await;
            (r, policy)
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        cancel.cancel();
        let (result, _policy) = tokio::time::timeout(Duration::from_secs(3), task)
            .await
            .expect("the cancel cut the backoff short")
            .unwrap();
        assert!(
            matches!(result, Err(ProviderError::Fatal(_))),
            "the cancel surfaces a non-retryable error, got {result:?}"
        );
        assert_eq!(
            calls.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "the backoff sleep was cut short — no second attempt"
        );
    }

    #[tokio::test]
    async fn retryable_is_retried_until_success() {
        let mut policy = RetryPolicy::new_with(5, Duration::from_millis(1));
        let (f, calls) = flaky_failing(3);
        let mut events: Vec<RpcEvent> = Vec::new();
        let result = policy
            .call_with_retry(f, &CancellationToken::new(), &mut |ev| events.push(ev))
            .await;
        assert_eq!(result, Ok(3), "the third attempt succeeds");
        assert_eq!(
            calls.load(std::sync::atomic::Ordering::SeqCst),
            3,
            "two failures + one success"
        );
        // Two `auto_retry_start`s (attempts 2 and 3); `auto_retry_end
        // { success: true }` is the LOOP's (via `note_success`).
        let starts = events
            .iter()
            .filter(|e| matches!(e, RpcEvent::auto_retry_start { .. }))
            .count();
        assert_eq!(starts, 2, "one auto_retry_start per retry");
        // The delays are exponential (1ms, 2ms).
        let delays: Vec<u64> = events
            .iter()
            .filter_map(|e| match e {
                RpcEvent::auto_retry_start { delay_ms, .. } => Some(*delay_ms),
                _ => None,
            })
            .collect();
        assert_eq!(delays, vec![1, 2]);
        // `note_success` reports the retried turn.
        assert_eq!(policy.note_success(), Some(3));
    }

    #[tokio::test]
    async fn retryable_budget_exhaustion_surfaces_the_error() {
        let mut policy = RetryPolicy::new_with(3, Duration::from_millis(1));
        let (f, calls) = flaky_failing(u32::MAX); // never succeeds
        let mut events: Vec<RpcEvent> = Vec::new();
        let result = policy
            .call_with_retry(f, &CancellationToken::new(), &mut |ev| events.push(ev))
            .await;
        assert!(
            matches!(result, Err(ProviderError::Retryable(_))),
            "the last retryable error surfaces"
        );
        assert_eq!(
            calls.load(std::sync::atomic::Ordering::SeqCst),
            3,
            "exactly max_attempts calls (5 is the default budget)"
        );
        // The exhaustion `auto_retry_end { success: false }` is emitted.
        assert!(events
            .iter()
            .any(|e| matches!(e, RpcEvent::auto_retry_end { success: false, .. })));
    }

    #[tokio::test]
    async fn fatal_is_never_retried() {
        let mut policy = RetryPolicy::new_with(5, Duration::from_millis(1));
        let calls = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0));
        let calls_inner = calls.clone();
        let f = move || {
            let c = calls_inner.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
            Box::pin(async move {
                if c == 1 {
                    Err(ProviderError::Fatal("400 bad request".into()))
                } else {
                    Ok(c)
                }
            })
        };
        let mut events: Vec<RpcEvent> = Vec::new();
        let result = policy
            .call_with_retry(f, &CancellationToken::new(), &mut |ev| events.push(ev))
            .await;
        assert!(
            matches!(result, Err(ProviderError::Fatal(_))),
            "a Fatal error is returned verbatim"
        );
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert!(events.is_empty(), "no retry events for a Fatal error");
    }

    #[tokio::test]
    async fn auth_is_never_retried() {
        let mut policy = RetryPolicy::new_with(5, Duration::from_millis(1));
        let calls = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0));
        let calls_inner = calls.clone();
        let f = move || {
            let c = calls_inner.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
            Box::pin(async move {
                if c == 1 {
                    Err(ProviderError::Auth("401".into()))
                } else {
                    Ok(c)
                }
            })
        };
        let result = policy
            .call_with_retry(f, &CancellationToken::new(), &mut |_| {})
            .await;
        assert!(matches!(result, Err(ProviderError::Auth(_))));
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    #[test]
    fn note_success_reports_only_a_retried_turn() {
        let mut policy = RetryPolicy::new_with(5, Duration::from_millis(1));
        assert_eq!(policy.note_success(), None, "no retry yet");
        assert!(policy.next_retry_delay().is_some()); // attempt 2
        assert_eq!(policy.note_success(), Some(2));
        // A second successful call (no new retry) reports nothing.
        assert_eq!(policy.note_success(), None);
    }

    #[test]
    fn the_default_budget_is_five_attempts() {
        let policy = RetryPolicy::new();
        assert_eq!(policy.max_attempts(), 5);
    }
}

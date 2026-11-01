//! Regression tests for issue #120: provider rebuild when the model's
//! composed key changes in `AgentLoop::set_model`.

use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;
use wiremock::matchers::method;
use wiremock::{Mock, MockServer, ResponseTemplate};

use super::support::{build_loop_with_db, build_loop_with_headers_and_db, fake_model, text_prompt};
use crate::agent::harness::provider::build_provider;
use crate::agent::harness::retry::RetryPolicy;

const OK_SSE: &str = "data: {\"choices\":[{\"delta\":{\"content\":\"ok\"}}]}\n\n\
     data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n\
     data: [DONE]\n\n";

async fn mount_chat_completions(server: &MockServer) {
    Mock::given(method("POST"))
        .respond_with(
            ResponseTemplate::new(200).set_body_raw(OK_SSE.to_string(), "text/event-stream"),
        )
        .mount(server)
        .await;
}

#[tokio::test]
async fn set_model_rebuilds_provider_when_composed_key_changes() {
    let server_a = MockServer::start().await;
    let server_b = MockServer::start().await;
    mount_chat_completions(&server_a).await;
    mount_chat_completions(&server_b).await;

    let mut model_a = fake_model("m-a");
    model_a.provider = "prov-a".to_string();
    model_a.base_url = server_a.uri();
    model_a.api = Some("litellm".to_string());

    let mut model_b = fake_model("m-b");
    model_b.provider = "prov-b".to_string();
    model_b.base_url = server_b.uri();
    model_b.api = Some("litellm".to_string());

    let models = vec![model_a.clone(), model_b.clone()];
    let provider = build_provider(&model_a);

    let (events_tx, _events_rx) = mpsc::unbounded_channel();
    let turn_cancel = Arc::new(StdMutex::new(CancellationToken::new()));
    let (settle_tx, _settle_rx) = watch::channel(0u64);
    let retry = RetryPolicy::new_with(0, Duration::from_millis(1));

    let (_space, mut loop_, _db) = build_loop_with_db(
        provider,
        events_tx,
        turn_cancel,
        settle_tx,
        retry,
        models,
        None,
    );

    // Switch model to model B whose composed key differs.
    loop_.set_model(model_b);

    // Drive one turn.
    loop_.handle_prompt(&text_prompt("hello")).await;

    // Server B should receive exactly one request containing model B id.
    let b_requests = server_b.received_requests().await.expect("b requests");
    assert_eq!(
        b_requests.len(),
        1,
        "server B should receive exactly one request"
    );
    let b_body = String::from_utf8_lossy(&b_requests[0].body);
    assert!(
        b_body.contains("m-b"),
        "server B request body should contain model B id, got: {b_body}"
    );

    // Server A should receive zero requests.
    let a_requests = server_a.received_requests().await.expect("a requests");
    assert_eq!(a_requests.len(), 0, "server A should receive zero requests");
}

/// Preserves the existing provider when the model's composed key does not change.
///
/// References ADR 0024's amendment and accepted consequence #4 in
/// `docs/features/model-selection.md` (a same-key baseUrl edit preserves the
/// existing provider without rebuilding).
#[tokio::test]
async fn set_model_preserves_provider_when_composed_key_is_identical() {
    let server_a = MockServer::start().await;
    let server_b = MockServer::start().await;
    mount_chat_completions(&server_a).await;
    mount_chat_completions(&server_b).await;

    let mut model_a = fake_model("m-a");
    model_a.provider = "prov-a".to_string();
    model_a.base_url = server_a.uri();
    model_a.api = Some("litellm".to_string());

    let models = vec![model_a.clone()];
    let provider = build_provider(&model_a);

    let (events_tx, _events_rx) = mpsc::unbounded_channel();
    let turn_cancel = Arc::new(StdMutex::new(CancellationToken::new()));
    let (settle_tx, _settle_rx) = watch::channel(0u64);
    let retry = RetryPolicy::new_with(0, Duration::from_millis(1));

    let (_space, mut loop_, _db) = build_loop_with_db(
        provider,
        events_tx,
        turn_cancel,
        settle_tx,
        retry,
        models,
        None,
    );

    // Build model A' as a clone with the same provider and id, but with base_url pointing at server B.
    let mut model_a_prime = model_a.clone();
    model_a_prime.base_url = server_b.uri();

    // Switch model with identical composed key.
    loop_.set_model(model_a_prime);

    // Drive one turn.
    loop_.handle_prompt(&text_prompt("hello")).await;

    // Server A should receive the request because the provider was not rebuilt.
    let a_requests = server_a.received_requests().await.expect("a requests");
    assert_eq!(
        a_requests.len(),
        1,
        "server A should receive the request because provider was retained"
    );

    // Server B should receive zero requests.
    let b_requests = server_b.received_requests().await.expect("b requests");
    assert_eq!(b_requests.len(), 0, "server B should receive zero requests");
}

/// (issue #129, R1) The MID-SESSION SWITCH regression: a provider that carries
/// its custom headers at session start must STILL carry them after
/// `set_model` rebuilds it. The silent regression this closes is a switch that
/// re-builds the provider through the header-less `build_provider` — the first
/// prompt passes a gated gateway and the second 401s.
///
/// The switch keeps the SAME provider id (so the `settings.json` row the
/// re-lookup finds is the same row) but a DIFFERENT model `id` — the composed
/// `ModelKey` is `(provider, id)` only, so the key changes and the rebuild
/// fires. Both wires are asserted: the FIRST turn proves the initial provider
/// was built with headers (the header-less `build_loop_with_db` shape could
/// never show this), the SECOND proves `set_model` re-resolved them rather
/// than dropping them.
/// `#[allow(clippy::await_holding_lock)]` is INTENTIONAL: the header value is
/// read from the process environment by `set_model`'s RE-LOOKUP — i.e. DURING
/// the driven turns, not only at setup — so the `env_lock` + the restore guard
/// must stay held across the `.await`s to serialize against a sibling test that
/// could flip the same variable mid-expansion (the `commands/settings.rs` /
/// `tools_enabled.rs` precedent).
#[allow(clippy::await_holding_lock)]
#[tokio::test]
async fn set_model_reapplies_custom_headers_after_a_switch() {
    let server_a = MockServer::start().await;
    let server_b = MockServer::start().await;
    mount_chat_completions(&server_a).await;
    mount_chat_completions(&server_b).await;

    // The header value the settings row's `$`-template expands to. The
    // env-mutating rule: hold the `env_lock` for the whole set→read→restore
    // span — the expansion happens inside `set_model`'s re-lookup, i.e.
    // DURING the test body, not only at setup.
    let _lock = crate::test_support::env_lock();
    let original = std::env::var_os("T2B_SWITCH_HEADER");
    struct Restore(Option<std::ffi::OsString>);
    impl Drop for Restore {
        fn drop(&mut self) {
            match self.0.take() {
                // SAFETY: the `ENV_LOCK` is still held (the guard declared
                // above outlives this one in reverse declaration order).
                Some(v) => unsafe { std::env::set_var("T2B_SWITCH_HEADER", v) },
                None => unsafe { std::env::remove_var("T2B_SWITCH_HEADER") },
            }
        }
    }
    // SAFETY: the `ENV_LOCK` is held for the whole span.
    unsafe { std::env::set_var("T2B_SWITCH_HEADER", "switched") };
    let _restore = Restore(original);

    // The SAME provider id on both models (the row the re-lookup finds); the
    // model `id` differs, which is what changes the composed key.
    let mut model_a = fake_model("m-a");
    model_a.provider = "prov-headers".to_string();
    model_a.base_url = server_a.uri();
    model_a.api = Some("litellm".to_string());

    let mut model_b = fake_model("m-b");
    model_b.provider = "prov-headers".to_string();
    model_b.base_url = server_b.uri();
    model_b.api = Some("litellm".to_string());

    let models = vec![model_a.clone(), model_b.clone()];

    // The settings row carries the RAW template; the production expansion
    // (`provider_headers_for` → `expand_map`) turns it into the wire value —
    // so the asserted value is the EXPANDED one, computed by the same engine.
    let mut raw_headers = std::collections::HashMap::new();
    raw_headers.insert("x-env-test".to_string(), "$T2B_SWITCH_HEADER".to_string());
    let expanded = crate::agent::provider_auth::headers::expand_map(&raw_headers);
    assert_eq!(
        expanded.get("x-env-test").map(String::as_str),
        Some("switched"),
        "the fixture's template expands against the process env"
    );

    // The INITIAL provider is built the production way — through the header-
    // carrying construction entry.
    let provider =
        crate::agent::harness::provider::build_provider_with_headers(&model_a, Some(&expanded));

    let (events_tx, _events_rx) = mpsc::unbounded_channel();
    let turn_cancel = Arc::new(StdMutex::new(CancellationToken::new()));
    let (settle_tx, _settle_rx) = watch::channel(0u64);
    let retry = RetryPolicy::new_with(0, Duration::from_millis(1));
    let sink = Arc::new(super::support::TestSink {
        updates: Arc::new(StdMutex::new(Vec::new())),
    });

    let (_config_home, _space, mut loop_, _db) = build_loop_with_headers_and_db(
        provider,
        events_tx,
        turn_cancel,
        settle_tx,
        retry,
        models,
        "prov-headers",
        raw_headers,
        sink,
    );

    // (1) The FIRST wire (the initial provider, built WITH headers).
    loop_.handle_prompt(&text_prompt("hello")).await;
    let a_requests = server_a.received_requests().await.expect("a requests");
    assert_eq!(a_requests.len(), 1, "server A receives the first turn");
    assert_eq!(
        a_requests[0]
            .headers
            .get("x-env-test")
            .and_then(|v| v.to_str().ok()),
        Some("switched"),
        "the FIRST turn carries the expanded custom header — the initial \
         provider was built through `build_provider_with_headers`"
    );

    // (2) The switch: same provider id, different model id → the composed key
    // changes → the provider is REBUILT.
    loop_.set_model(model_b);
    loop_.handle_prompt(&text_prompt("hello again")).await;

    let b_requests = server_b.received_requests().await.expect("b requests");
    assert_eq!(
        b_requests.len(),
        1,
        "server B receives the post-switch turn"
    );
    assert_eq!(
        b_requests[0]
            .headers
            .get("x-env-test")
            .and_then(|v| v.to_str().ok()),
        Some("switched"),
        "the SECOND turn STILL carries the custom header — `set_model` \
         re-resolved it from `settings.json` instead of dropping it (R1)"
    );

    // Server A saw only the pre-switch turn (the switch really rebuilt).
    let a_requests = server_a.received_requests().await.expect("a requests");
    assert_eq!(
        a_requests.len(),
        1,
        "the post-switch turn went to server B, not the retained provider"
    );
}

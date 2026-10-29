//! Regression tests for issue #120: provider rebuild when the model's
//! composed key changes in `AgentLoop::set_model`.

use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;
use wiremock::matchers::method;
use wiremock::{Mock, MockServer, ResponseTemplate};

use super::support::{build_loop_with_db, fake_model, text_prompt};
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

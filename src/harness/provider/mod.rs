//! The model providers (ADR 0024: one per wire shape) + the shared
//! transport. `OpenAiCompatibleProvider` (in [`openai`]) speaks the
//! OpenAI-compatible chat-completions API: a `POST {base_url}/chat/completions`
//! with `stream: true`, parsing the SSE response (content / reasoning /
//! tool-call deltas, `finish_reason`, `usage`) into a `ProviderEvent`
//! stream.
//!
//! The [`Provider`] trait (in [`types`]) is the seam the `AgentLoop`
//! (Task 6) calls — it is provider-agnostic in shape (a `complete()`
//! returning a boxed, `'static`, `Send` stream) so a second provider
//! (Anthropic) can be added later without touching the loop.

pub mod anthropic;
pub mod openai;
pub mod responses;
#[cfg(test)]
pub(crate) mod test_util;
pub mod transport;

// ── T5/S4 — `types.rs` and `overflow.rs` moved to `fulcrum-harness` ──────────────
//
// Both moved because `StartEnv.transcript` is an `Option<Vec<ChatMessage>>` and
// `StoreFrame`'s sibling wire types embed it, so E1 could not be severed without
// it; `provider_types.rs` in turn calls `provider_overflow::is_context_overflow_text`
// in production (`parse_error_chunk`, the ONE classifier of the three wires), so
// the two moved together. `Provider`/`ModelRequest`/`ProviderError` came as part of
// the same file — moving half a module is not a thing.
//
// ALIASED rather than re-exported by name, deliberately: `transport.rs`,
// `openai.rs`, `anthropic.rs`, `responses.rs` and `test_util.rs` all STAY desktop
// through S5 and every one of them imports `super::types::…` (and `transport.rs`
// imports `super::overflow::…`). The alias keeps all five compiling with ZERO
// edits — the S1 lesson (re-export the MODULE, not just named symbols: deleting
// one module-level line there cost 36 errors) applied to a split module.
pub use crate::{provider_overflow as overflow, provider_types as types};

pub use anthropic::{normalize_anthropic_base_url, normalize_wire_base_url, AnthropicProvider};
pub use openai::OpenAiCompatibleProvider;
pub use responses::OpenAiResponsesProvider;
pub use types::{
    ArgumentDefect, ChatMessage, ChatRole, FinishReason, MessageContent, ModelOptions,
    ModelRequest, Provider, ProviderError, ProviderEvent, ToolCall, ToolCallDelta, ToolSpec, Usage,
};
// (T5/S4, Step A.3) The two `pub(crate)` free functions and the two `pub(super)`
// wire shapes in the moved file are now `pub`, because a package boundary erases
// both visibilities and the STAYING `openai.rs`/`anthropic.rs`/`responses.rs` name
// all four. Re-exported through the alias so their import sites did not change.
pub use types::{cost_usd_from_usage, parse_error_chunk, WireToolCall};

/// The ONE place the construction-time wire line is produced, for BOTH sinks.
///
/// Both builders call this with the `wire` they just resolved, so the two
/// builders cannot drift and the two SINKS cannot disagree — the format string
/// exists once. An edit that lands in one builder or one sink and misses the
/// other reintroduces, one level up, the exact "the log and the dispatch
/// disagree" defect the resolved-once local was introduced to close.
///
/// # Why two sinks
///
/// * `debuglog::log` is out. It is the GATED full protocol transcript
///   (`ARCHIMEDES_DEBUG=1` only) and it returns at its first statement without
///   that var, so it is a no-op for the whole population this line is for.
/// * `diagnostics::record` is always on (no env var, `<datadir>/archimedes/
///   diagnostics.log`). It covers the case `eprintln!` cannot: from the naming
///   pass this code runs in the SUPERVISOR, whose stderr is inherited and never
///   persisted — and on a Windows release build the GUI subsystem discards it
///   outright. **That is the *desktop's* Supervisor.** The standalone daemon
///   (`fulcrum serve`) arms neither channel — nothing in `fulcrum/src/` calls
///   `diagnostics::init` or `debuglog::init_from_env` — so in a `fulcrum serve`
///   deployment this line reaches neither file, only whatever the daemon does
///   with the process's stderr.
/// * The `eprintln!` half is what covers the WORKER: the Worker's stderr is
///   drained by a dedicated task in `worker/client.rs`
///   (`debuglog::log_truncated("worker-stderr:", &line)`), so it is the only
///   Worker-side sink that survives **with `ARCHIMEDES_DEBUG=1`** — that drain
///   feeds `debuglog`, which returns immediately without the var. In a DEFAULT
///   launch, therefore, NEITHER half reaches a file from inside a Worker: the
///   `record` half is a no-op because the Worker never arms diagnostics (ADR
///   0110, "What this does not fix"), and the `eprintln!` half is dropped by the
///   gate. DO NOT delete the `eprintln!` as redundant with the `record` line on
///   the strength of the sentence above — with the var set it is the Worker's
///   only reader, and deleting it makes the Worker-side wire unobservable again.
///
/// # What may appear in the `record` payload
///
/// `diagnostics` states the rule for a call site as "Not a transcript, not a
/// prompt, not a model answer, **not a token, not a bearer**, not a file body".
/// A `base_url` is configuration, but it is NOT safe to emit raw: it is free-text
/// user configuration, typed in Settings and committed with no validation that
/// touches userinfo or query, and `Url::parse(…).to_string()` preserves both
/// verbatim. `https://user:pass@gw.example/v1` is a WORKING config (reqwest's
/// `extract_authority` turns the userinfo into a Basic auth header) and so is
/// `https://acct.example/v1?sig=…` (Azure-OpenAI's SAS shape) — and this file is
/// world-readable at default umask with no opt-in. So the payload carries
/// [`reduced_base_url`], never `m.base_url`. That is the reviewed decision the
/// module doc asks to see recorded at the call site.
fn log_wire(wire: crate::harness::WireApi, m: &crate::harness::catalog::Model) {
    // `m.id` is THIRD-PARTY text: `catalog.rs`'s `expand_v1_models` takes the id
    // straight from the provider's `/models` JSON, filtered only for emptiness, so
    // an id of `"x\n<forged line>"` would forge a line in a file a supporter
    // reads. `m.id` and the reduced base are the only two interpolated fields, and
    // `wire.as_str()` is one of four static strings, so these two sanitises close
    // the injection surface for the whole line.
    let model_id = one_field_per_line(&m.id);
    let base = reduced_base_url(&m.base_url);
    eprintln!(
        "harness: provider wire={} model={} base={}",
        wire.as_str(),
        model_id,
        base
    );
    crate::diagnostics::record(&format!(
        "provider wire={} model={} base={}",
        wire.as_str(),
        model_id,
        base
    ));
}

/// Replace any newline / carriage return with a space, so a field that came from
/// outside this crate cannot forge a line in the newline-delimited diagnostics
/// file. Both are replaced (not stripped) so the field stays delimited from the
/// one that follows it.
fn one_field_per_line(s: &str) -> String {
    s.chars()
        .map(|c| match c {
            '\n' | '\r' => ' ',
            other => other,
        })
        .collect()
}

/// The credential-free form of a `base_url`, for the log line ONLY.
///
/// `scheme://host[:port]/path` and nothing else: userinfo cleared (username AND
/// password — clearing only the username leaves the password, and `Url` then
/// renders `://:password@host`), query dropped (`?sig=`, `?api-version=`, the
/// Azure SAS shape) and fragment dropped.
///
/// The **path is kept on purpose**: it is what distinguishes two routes on one
/// provider id, which is the fact the line was added to record. This touches no
/// request bytes — the provider still gets `m.base_url` verbatim; only the log
/// line is reduced.
///
/// A parse failure yields something INERT, never the raw string: the bare host
/// if one can be cut out, else a fixed placeholder. A string that will not parse
/// has no scheme to anchor a userinfo strip to, so echoing it is what would put
/// the credential in the file.
fn reduced_base_url(base_url: &str) -> String {
    let reduced = url::Url::parse(base_url.trim())
        .ok()
        .map(|mut url| {
            // `unset` rather than `""`: an empty username is still a username,
            // and `Url` renders `https://:8080/v1` for it — a placeholder that
            // is not the credential, but it is a lie about the shape.
            if url.password().is_some() {
                let _ = url.set_password(None);
            }
            if !url.username().is_empty() {
                let _ = url.set_username("");
            }
            url.set_query(None);
            url.set_fragment(None);
            // A non-special scheme (`unknown://u:p@host`) renders `://@host` for
            // an EMPTY username where a special scheme renders `://host`, so the
            // leftover `@` is removed by hand rather than left in the line. The
            // removal is anchored at the authority, NOT a whole-string `replace`
            // — a path may legitimately contain `://@` and must survive.
            let rendered = url.as_str();
            let prefix = format!("{}://@", url.scheme());
            match rendered.strip_prefix(prefix.as_str()) {
                Some(rest) => format!("{}://{}", url.scheme(), rest),
                None => rendered.to_string(),
            }
        })
        .or_else(|| {
            // Unparseable: a bare host is still extractable for the common
            // `host:port/path` and `//host/path` shapes, and a host carries no
            // credential. Anything that does not LOOK like a host is replaced by
            // the placeholder rather than echoed — for a string with no scheme
            // there is no way to tell "the host" from "the secret", and the whole
            // point of the fallback is to never echo the raw value.
            let trimmed = base_url.trim();
            // Drop a leading scheme first: `https://[bad` must yield something
            // from the AUTHORITY, not the word `https`.
            let after_scheme = match trimmed.split_once("://") {
                Some((_, rest)) => rest,
                None => trimmed,
            };
            let host = after_scheme
                .strip_prefix("//")
                .unwrap_or(after_scheme)
                .split(['/', '?', '#'])
                .next()
                .unwrap_or("");
            // A `@` in what is left means the piece before the first `/` was
            // `user:pass@host` — take only the host half of it.
            let host = host.rsplit('@').next().unwrap_or("");
            // A port is kept only when the host half is a bare hostname / IPv4,
            // so an IPv6 literal (`[::1]`) cannot be mangled into a fake
            // `host:port` by the split.
            let host = if host.starts_with('[') {
                host
            } else {
                host.split(':').next().unwrap_or("")
            };
            // The host-likeness gate. Space, `@`, `%`, a quote — any of these and
            // what we cut out is not a hostname, it is free text that may itself
            // be the credential.
            let host_like = !host.is_empty()
                && host.chars().all(|c| {
                    c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | '_' | ':' | '[' | ']')
                });
            host_like.then(|| host.to_string())
        })
        .unwrap_or_else(|| "<unparseable>".to_string());
    one_field_per_line(&reduced)
}

/// Build the `Provider` for a `Model` (the `api` discriminator — ADR
/// 0024; the `SessionManager`'s default factory + the tests).
///
/// The `None`-headers shorthand (issue #129): no custom headers on the wire.
/// [`build_provider_with_headers`] is the header-carrying form the two
/// production construction sites use.
///
/// WHY `eprintln!` AND `diagnostics::record`, not one of them — the reasoning
/// lives on [`log_wire`], the ONE place both lines are produced.
pub fn build_provider(m: &crate::harness::catalog::Model) -> Box<dyn Provider> {
    // The wire is resolved ONCE into a local and the `match` below runs on the
    // local, so the line and the dispatch cannot disagree.
    let wire = crate::harness::WireApi::parse(m.api.as_deref().unwrap_or("openai-completions"))
        .unwrap_or_default();
    log_wire(wire, m);
    match wire {
        crate::harness::WireApi::AnthropicMessages => Box::new(AnthropicProvider {
            // T3b (#131): the Messages wire wants the ROOT — the client appends
            // its own `/v1/messages`, so a `{root}/v1` base (the Settings'
            // convention, and what the LiteLLM discovery rows carry) would
            // otherwise produce `{root}/v1/v1/messages`. The pass is idempotent
            // (`n(n(u)) == n(u)`), so re-running it is a no-op.
            base_url: normalize_wire_base_url(
                &m.base_url,
                crate::harness::WireApi::AnthropicMessages,
            ),
            api_key: m.api_key.clone(),
            custom_headers: None,
        }),
        // Completions / Responses keep the configured base VERBATIM
        // (byte-identical to today — see the note above
        // `build_provider_with_headers` for why the append rule is not applied
        // here, and `normalize_wire_base_url` for the rule itself).
        crate::harness::WireApi::OpenAiResponses => Box::new(OpenAiResponsesProvider {
            base_url: m.base_url.clone(),
            api_key: m.api_key.clone(),
            custom_headers: None,
        }),
        // `openai-completions` / `litellm` / `None` / unknown — the
        // current default (an unknown API is unselectable in practice,
        // but a hand-built `Model` with a weird `api` still gets a
        // working client). `litellm` is a DISCOVERY mode, not a wire —
        // its completion wire IS `openai-completions` (ADR 0026).
        _ => Box::new(OpenAiCompatibleProvider::new(
            m.base_url.clone(),
            m.api_key.clone(),
        )),
    }
}

/// Expand + validate ONE provider's custom headers from the PACKAGED provider
/// rows (R1, issue #129 — the completion-wire path; Fulcrum T3 repoints it at
/// the envelope). The rows are `StartEnv.providers` — the desktop's
/// `providers` vec as the Supervisor packaged it — and the row is the one whose
/// `id == model.provider`. No row / a row with no `headers` → `None` (no custom
/// headers — the behaviour byte-for-byte), which is ALSO what an old envelope
/// (`#[serde(default)]` → the empty vec) produces, so the additive path cannot
/// change anyone's headers.
///
/// This replaced `provider_headers_for(config_dir, model)`, which read
/// `settings.json` through the loop's `config_dir`. Removing that read is the
/// point: BOTH header sites (`worker/core.rs`'s loop construction and
/// `AgentLoop::set_model`) are worker-reachable, so packaging four fields and
/// leaving this one would still leave the Worker opening `settings.json`.
///
/// The expansion engine is the SAME `provider_auth::headers::expand_map` the
/// discovery spine (T2a) calls — a header value is expanded once and both wires
/// see the identical result — and it still runs HERE, against the Worker's own
/// process env (the env the Worker inherited from the Supervisor at spawn, so
/// the same `$VAR` resolves to the same value as it did when the lookup read
/// the file in-process). The insecure-HTTP gate is NOT re-checked here: it
/// belongs to the discovery spine (T2a's `validate_base_url`), and this lookup
/// is synchronous by construction (`AgentLoop::set_model` is a sync `&mut self`
/// method).
// (Step A.3) RAISED from `pub(crate)`: the STAYING desktop `worker/core.rs`
// calls it (the loop-construction header site), and a package boundary erases
// `pub(crate)`. T8/S8.2 then moved `worker/core.rs` INTO this crate, so the
// caller is no longer across the boundary and the raise is no longer strictly
// required — it is KEPT (widening is not the thing to undo under a move, and
// S8.5's extraction audit is where a visibility tighten belongs if it is wanted).
pub fn headers_for_providers(
    providers: &[crate::config_types::ProviderConfig],
    model: &crate::harness::catalog::Model,
) -> Option<std::collections::BTreeMap<String, String>> {
    providers
        .iter()
        .find(|p| p.id == model.provider)
        .and_then(|p| p.headers.as_ref())
        .map(crate::provider_headers::expand_map)
}

/// Build the `Provider` for a `Model` WITH the provider's custom headers
/// (R1, issue #129): the production construction entry —
/// `build_provider_with_headers(&model, headers_for_providers(&rows, model).as_ref())`.
/// `custom_headers` is the EXPANDED + VALIDATED map; it rides every completion
/// request through the ONE seam, [`transport::build_client`]'s
/// `default_headers`. `None` is byte-identical to [`build_provider`] (which
/// stays the `None` shorthand for the naming path and the tests).
pub fn build_provider_with_headers(
    m: &crate::harness::catalog::Model,
    custom_headers: Option<&std::collections::BTreeMap<String, String>>,
) -> Box<dyn Provider> {
    // THE production construction entry (`worker/core.rs`'s loop construction and
    // `AgentLoop::set_model`), so this is the site whose wire a user's session
    // actually ran on. The same resolved-once local and the same [`log_wire`] as
    // [`build_provider`] — one call, so the two builders and the two sinks cannot
    // drift apart. Reached in the Worker, where the `record` half is a no-op
    // (never armed) and the `eprintln!` half survives only through the stderr
    // drain, and in the Supervisor, where the `record` half is the only one with
    // a reader.
    let wire = crate::harness::WireApi::parse(m.api.as_deref().unwrap_or("openai-completions"))
        .unwrap_or_default();
    log_wire(wire, m);
    match wire {
        crate::harness::WireApi::AnthropicMessages => Box::new(AnthropicProvider {
            // T3b (#131): the SAME root-for-Messages pass as `build_provider`
            // (the idempotent strip; the client re-appends `/v1/messages` at
            // its own adapter boundary, so a `{root}/v1` row and a root row
            // both land on `{root}/v1/messages`).
            base_url: normalize_wire_base_url(
                &m.base_url,
                crate::harness::WireApi::AnthropicMessages,
            ),
            api_key: m.api_key.clone(),
            custom_headers: custom_headers.cloned(),
        }),
        // Completions / Responses keep the configured base VERBATIM — see the
        // note on `build_provider` (the append half of
        // `normalize_wire_base_url` is deliberately NOT applied here).
        crate::harness::WireApi::OpenAiResponses => Box::new(OpenAiResponsesProvider {
            base_url: m.base_url.clone(),
            api_key: m.api_key.clone(),
            custom_headers: custom_headers.cloned(),
        }),
        // `openai-completions` / `litellm` / `None` / unknown — the
        // current default (an unknown API is unselectable in practice,
        // but a hand-built `Model` with a weird `api` still gets a
        // working client). `litellm` is a DISCOVERY mode, not a wire —
        // its completion wire IS `openai-completions` (ADR 0026).
        // The CONSTRUCTOR (not a struct literal) — `OpenAiCompatibleProvider`
        // holds the PRIVATE `accepts_stream_options` latch a literal cannot
        // name; `with_custom_headers` is its header-carrying twin.
        _ => Box::new(OpenAiCompatibleProvider::with_custom_headers(
            m.base_url.clone(),
            m.api_key.clone(),
            custom_headers.cloned(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::test_util::chat_message;
    use super::*;
    use futures_util::StreamExt;

    // ── `build_provider` (ADR 0024: the `api` discriminator) ────────

    /// A `Model` test-literal helper (everything else pinned; `api` is
    /// the parameter — the `catalog.rs` tests' `model` helper pattern,
    /// extended with the `api` parameter).
    fn model(base_url: &str, api: Option<&str>) -> crate::harness::catalog::Model {
        crate::harness::catalog::Model {
            id: "m/1".to_string(),
            provider: "p".to_string(),
            base_url: base_url.to_string(),
            api_key: "k".to_string(),
            context_window: 128000,
            cost_per_mtok_in: 0.0,
            cost_per_mtok_out: 0.0,
            supports_tools: true,
            supports_thinking: false,
            thinking_levels: Vec::new(),
            api: api.map(str::to_string),
        }
    }

    /// A minimal `ModelRequest` (a single user message; `max_tokens`
    /// present for the Anthropic wire's required field).
    fn test_request() -> ModelRequest {
        ModelRequest {
            model: "m/1".to_string(),
            messages: vec![chat_message(
                ChatRole::User,
                MessageContent::Text("hi".to_string()),
                None,
                None,
            )],
            tools: vec![],
            options: ModelOptions {
                temperature: None,
                max_tokens: Some(64),
                reasoning_effort: None,
                stream: true,
            },
            session_id: None,
        }
    }

    /// A recorded mock HTTP server: LOOP-accepts (one request per
    /// connection — the `build_provider` dispatch test performs FOUR
    /// dispatches, so a single-accept server would hang the 2nd/3rd/4th),
    /// reads each request's headers until `\r\n\r\n` (+ the
    /// `Content-Length` body), records the `PATH` line + the
    /// `authorization` / `x-api-key` header presence, and replies the
    /// wire-appropriate minimal 200 SSE body (a `data: {"choices":[…]}`
    /// for OpenAI, a `message_start` + `message_delta` pair for
    /// Anthropic, a `response.completed` for Responses).
    async fn recorded_dispatch_server(
        listener: tokio::net::TcpListener,
    ) -> (
        tokio::task::JoinHandle<()>,
        tokio::sync::mpsc::Receiver<(String, bool, bool)>,
    ) {
        let (tx, rx) = tokio::sync::mpsc::channel::<(String, bool, bool)>(8);
        let server = tokio::spawn(async move {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    break;
                };
                let mut buf = [0u8; 65536];
                let mut data = Vec::new();
                // Read the headers (until `\r\n\r\n`).
                while !data.windows(4).any(|w| w == b"\r\n\r\n") {
                    let Ok(n) = stream.read(&mut buf).await else {
                        return;
                    };
                    if n == 0 {
                        return;
                    }
                    data.extend_from_slice(&buf[..n]);
                }
                let header_end = data
                    .windows(4)
                    .position(|w| w == b"\r\n\r\n")
                    .expect("the header terminator")
                    + 4;
                let headers = String::from_utf8_lossy(&data[..header_end]).to_ascii_lowercase();
                let path = headers.lines().next().unwrap_or_default().to_string();
                let has_auth = headers.lines().any(|l| l.starts_with("authorization:"));
                let has_x_api_key = headers.lines().any(|l| l.starts_with("x-api-key:"));
                // Read the body (`Content-Length` bytes — a clean close
                // needs the request fully consumed; the client may send
                // the body in the same or a later segment).
                let content_length = headers
                    .lines()
                    .find(|l| l.starts_with("content-length:"))
                    .and_then(|l| l.split(':').nth(1).unwrap().trim().parse().ok())
                    .unwrap_or(0) as usize;
                let mut have = data.len();
                while have < header_end + content_length {
                    let Ok(n) = stream.read(&mut buf).await else {
                        return;
                    };
                    if n == 0 {
                        break;
                    }
                    data.extend_from_slice(&buf[..n]);
                    have = data.len();
                }
                // The wire-appropriate minimal 200 SSE body (the `PATH`
                // decides the wire — one server, four dispatches).
                let body = if path.contains("/messages") {
                    // Anthropic: a `message_start` + `message_delta` pair.
                    "event: message_start\r\n\
                     data: {\"type\":\"message_start\",\"message\":{\"usage\":{\"input_tokens\":1}}}\r\n\
                     \r\n\
                     event: message_delta\r\n\
                     data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":1}}\r\n\
                     \r\n"
                } else if path.contains("/responses") {
                    // Responses: a `response.completed`.
                    "event: response.completed\r\n\
                     data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"usage\":{\"input_tokens\":1,\"output_tokens\":1}}}\r\n\
                     \r\n"
                } else {
                    // OpenAI: a `data: {"choices":[…]}` + `[DONE]`.
                    "data: {\"choices\":[{\"delta\":{\"content\":\"x\"},\"finish_reason\":\"stop\"}]}\r\n\
                     \r\n\
                     data: [DONE]\r\n\
                     \r\n"
                };
                let resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                if stream.write_all(resp.as_bytes()).await.is_err() {
                    break;
                }
                if tx.send((path, has_auth, has_x_api_key)).await.is_err() {
                    break;
                }
                // DROP the stream (the client sees EOF → the stream ends).
            }
        });
        (server, rx)
    }

    /// (ADR 0024) `build_provider` dispatches on `Model.api`: an
    /// `anthropic-messages` model runs on `AnthropicProvider` (`POST
    /// /messages`, an `x-api-key` header, NO `authorization` header), an
    /// `openai-responses` model on `OpenAiResponsesProvider` (`POST
    /// /responses`, an `authorization` header), and a `None`-api model
    /// on `OpenAiCompatibleProvider` (`POST /chat/completions`, an
    /// `authorization` header — the current default), and a `litellm`
    /// model on `OpenAiCompatibleProvider` too (`POST /chat/completions`,
    /// an `authorization` header — ADR 0026: `litellm` is a DISCOVERY
    /// mode, its wire is `openai-completions`). Each stream is collected
    /// to completion (the recorded mock server replies the
    /// wire-appropriate minimal 200 SSE body; the loop-accept server
    /// handles the FOUR dispatches — one request per connection).
    #[tokio::test]
    async fn build_provider_dispatches_on_the_api() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("the listener binds");
        let addr = listener.local_addr().expect("the listener has an address");
        let base_url = format!("http://{addr}/v1");
        let (server, mut rx) = recorded_dispatch_server(listener).await;
        let req = test_request();

        // (1) `anthropic-messages` → `POST /messages` + `x-api-key` +
        // NO `authorization`.
        let provider = build_provider(&model(&base_url, Some("anthropic-messages")));
        let events: Vec<ProviderEvent> = provider
            .complete(&req)
            .await
            .expect("the request sends")
            .collect()
            .await;
        assert!(
            events
                .iter()
                .any(|e| matches!(e, ProviderEvent::Done(FinishReason::Stop))),
            "the anthropic stream completes: {events:?}"
        );
        let (path, has_auth, has_x_api_key) =
            rx.recv().await.expect("the server recorded the request");
        // (the method line is lowercase on the wire — `post`)
        assert!(
            path.starts_with("post ") && path.contains("/messages"),
            "got {path:?}"
        );
        assert!(
            has_x_api_key,
            "the anthropic wire sends `x-api-key` ({path:?})"
        );
        assert!(
            !has_auth,
            "the anthropic wire sends NO `authorization` ({path:?})"
        );

        // (2) `openai-responses` → `POST /responses` + `authorization`.
        let provider = build_provider(&model(&base_url, Some("openai-responses")));
        let events: Vec<ProviderEvent> = provider
            .complete(&req)
            .await
            .expect("the request sends")
            .collect()
            .await;
        assert!(
            events
                .iter()
                .any(|e| matches!(e, ProviderEvent::Done(FinishReason::Stop))),
            "the responses stream completes: {events:?}"
        );
        let (path, has_auth, has_x_api_key) =
            rx.recv().await.expect("the server recorded the request");
        assert!(
            path.starts_with("post ") && path.contains("/responses"),
            "got {path:?}"
        );
        assert!(
            has_auth,
            "the responses wire sends `authorization` ({path:?})"
        );
        assert!(
            !has_x_api_key,
            "the responses wire sends NO `x-api-key` ({path:?})"
        );

        // (3) `None` (the default) → `POST /chat/completions` +
        // `authorization`.
        let provider = build_provider(&model(&base_url, None));
        let events: Vec<ProviderEvent> = provider
            .complete(&req)
            .await
            .expect("the request sends")
            .collect()
            .await;
        assert!(
            events
                .iter()
                .any(|e| matches!(e, ProviderEvent::Done(FinishReason::Stop))),
            "the openai-compatible stream completes: {events:?}"
        );
        let (path, has_auth, _has_x_api_key) =
            rx.recv().await.expect("the server recorded the request");
        assert!(
            path.starts_with("post ") && path.contains("/chat/completions"),
            "got {path:?}"
        );
        assert!(
            has_auth,
            "the openai-compatible wire sends `authorization` ({path:?})"
        );

        // (4) (ADR 0026) `litellm` → the OpenAI arm: `POST /chat/completions` +
        // `authorization` (it is a discovery mode, NOT a fourth wire).
        let provider = build_provider(&model(&base_url, Some("litellm")));
        let events: Vec<ProviderEvent> = provider
            .complete(&req)
            .await
            .expect("the request sends")
            .collect()
            .await;
        assert!(
            events
                .iter()
                .any(|e| matches!(e, ProviderEvent::Done(FinishReason::Stop))),
            "the litellm (openai-compatible) stream completes: {events:?}"
        );
        let (path, has_auth, has_x_api_key) =
            rx.recv().await.expect("the server recorded the request");
        assert!(
            path.starts_with("post ") && path.contains("/chat/completions"),
            "got {path:?}"
        );
        assert!(
            has_auth,
            "the litellm api uses the openai-compatible wire: `authorization` ({path:?})"
        );
        assert!(
            !has_x_api_key,
            "the litellm api uses the openai-compatible wire: NO `x-api-key` ({path:?})"
        );

        // The loop-accept server never exits on its own — abort it (an
        // `await` would hang on the `accept()` loop).
        server.abort();
    }

    // ── the construction-time wire line ──────────────────────────────

    /// Restores the global diagnostics target to UNARMED on scope exit, **including
    /// through a panicking assertion** — which the plain "call `init(None)` last"
    /// shape does not.
    ///
    /// Why that matters here rather than being tidiness: a failing assertion
    /// unwinds past a trailing `init(None)`, leaving the process-global `DIR` aimed
    /// at this test's scratch dir, which `ScratchDir::Drop` then removes. Every
    /// later test that reaches `record` calls `append_to`, whose FIRST act is
    /// `create_dir_all(dir)` — silently resurrecting the deleted directory and
    /// writing into it for the rest of the run, never cleaned. A red test would
    /// therefore leave a stray `diagnostics.log` in the temp dir behind and pollute
    /// whatever sibling armed nothing.
    ///
    /// The desktop solved exactly this shape (`agent/session/naming.rs`'s
    /// `DisableDiagnosticsOnExit`) and this mirrors it. The declaration order is
    /// load-bearing: the guard is declared AFTER the scratch dir so it drops FIRST
    /// (locals drop in reverse), clearing the global before the dir it names is
    /// removed.
    struct DisableDiagnosticsOnExit;
    impl Drop for DisableDiagnosticsOnExit {
        fn drop(&mut self) {
            crate::diagnostics::init(None);
        }
    }

    /// Truncate the armed diagnostics file so the NEXT assertion is proven by its
    /// own line.
    ///
    /// Without this the headline loop was partly vacuous: iterations 2 and 3
    /// expect the same string (`openai-completions`) and the file was never
    /// truncated between them, so iteration 3 — `api: None`, the double-fallback
    /// case this whole commit exists to make diagnosable — passed off iteration 2's
    /// line.
    ///
    /// A missing file is fine (`create`d by the first `record`); a sibling test
    /// that reaches `build_provider` without `env_lock()` can still append here,
    /// which is why callers assert on the CONTENT of a specific line rather than on
    /// a line count — and why a sibling's line cannot satisfy an expectation: each
    /// expectation names this test's own `base_url`, which no other test uses.
    fn reset_diagnostics_file(dir: &std::path::Path) {
        let path = dir.join(crate::diagnostics::FILE_NAME);
        match std::fs::write(&path, "") {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => panic!("the scratch diagnostics file can be truncated: {e}"),
        }
    }

    /// The wire `build_provider` resolves is the wire the construction line
    /// RECORDS, and the rule it records is the DOUBLE fallback
    /// (`WireApi::parse(m.api.as_deref().unwrap_or("openai-completions")).unwrap_or_default()`):
    /// all four known `api` strings resolve to their own spelling, and an unknown
    /// string AND `None` both fold onto `openai-completions` — which is exactly why
    /// the wire was undecidable after the fact (a configured `openai-completions`
    /// and an unparseable `api` are the same provider, the same request bytes, and
    /// until this line, the same silence).
    ///
    /// The assertion is on the **recorded line's** `wire=` value, not on a
    /// re-execution of the parse. An earlier version of this test called
    /// `build_provider`, discarded the result, and then compared
    /// `WireApi::parse(…).unwrap_or_default().as_str()` against the expected
    /// spelling — the test re-deriving the value and comparing it to itself, which
    /// `wire.rs::parse_round_trips_all_four` already pins and which no change to
    /// `build_provider` could ever redden. Reading the emitted line instead makes
    /// the assertion non-vacuous AND gives the credential-reduction fix its
    /// assertion surface, since the same line carries `base=`.
    ///
    /// Going through `build_provider` (rather than calling `log_wire` directly) is
    /// what keeps the dispatch wired to the line: a builder that panicked on one of
    /// these inputs reddens this, and a builder whose `match` arm disagreed with its
    /// log line reddens it too. None of the tests above can pin that — they assert
    /// recorded HTTP traffic, and traffic cannot tell `openai-completions` from
    /// `litellm` (ADR 0026: same wire) nor either from an unknown string; there is
    /// no `Provider::as_any` to ask.
    ///
    /// What this CANNOT reach: the `eprintln!` half. A lib unit test runs in
    /// cargo's test harness, whose stderr goes to the harness and is not
    /// capturable from inside the process without replacing the global sink —
    /// and the only production reader of a Worker's stderr is the drain task in
    /// `worker/client.rs`, which needs a real spawned Worker process. That drain
    /// is what makes the `eprintln!` half worth writing, so the honest statement
    /// is that the stderr line is pinned here only by sharing the `wire=` value
    /// this test records, not by an assertion on its bytes.
    ///
    /// **The `_guard` at the top IS the lock, and it must not be taken twice.**
    /// `test_support::env_lock()` is a non-reentrant `std::sync::Mutex`, so calling
    /// it a second time inside one test **self-deadlocks** — the test hangs with no
    /// message and no panic, and the suite stalls until something external kills it.
    /// This has bitten before in this crate. A helper this test calls must never
    /// take `env_lock()` itself; it inherits the caller's hold.
    #[test]
    fn the_chosen_wire_is_logged_at_construction() {
        // Taken FIRST, before any `build_provider` call: every one of them
        // reaches `record`, so an unarmed-phase call racing a sibling test's armed
        // scratch dir would pollute THAT test. The lock the module doc prescribes
        // covers the whole test, not just the asserted phase.
        let _guard = crate::test_support::env_lock();

        // A port nothing listens on: construction opens no connection, so this
        // never needs a server (and never could race one). NO other test uses
        // port 9, so a sibling's line can never satisfy an expectation here.
        let base_url = "http://127.0.0.1:9/v1";

        // Declared BEFORE the guard so the guard drops first (locals drop in
        // reverse): the global target is cleared before the dir it names is gone.
        let dir = crate::test_support::ScratchDir::in_env_temp("provider-wire-test-", "dir");
        let _disable = DisableDiagnosticsOnExit;
        crate::diagnostics::init(Some(dir.to_path_buf()));

        // The four known spellings, each proven by ITS OWN recorded line: the file
        // is truncated at the top of every iteration, so an expectation can only be
        // met by the line this iteration wrote.
        for api in [
            "openai-completions",
            "anthropic-messages",
            "openai-responses",
            "litellm",
        ] {
            reset_diagnostics_file(dir.path());
            let m = model(base_url, Some(api));
            let _provider = build_provider(&m);
            let content = std::fs::read_to_string(dir.join(crate::diagnostics::FILE_NAME))
                .expect("arming the channel makes the construction line land with no env var");
            let want = format!("provider wire={api} model=m/1 base={base_url}");
            assert!(
                content.lines().any(|line| line.ends_with(&want)),
                "`api = {api:?}` records its OWN spelling as the whole payload of one \
                 line; got:\n{content}"
            );
        }

        // The double fallback: an unknown string, `""` and `None` all fold onto
        // `openai-completions`. `None` is the case the line exists for — it is the
        // one input whose wire was previously indistinguishable from a configured
        // one in EVERY observable — and because the file is truncated each time
        // round, it is now proven by its own line rather than by the iteration
        // above it.
        for api in [Some("google-generative-ai"), Some(""), None] {
            reset_diagnostics_file(dir.path());
            let m = model(base_url, api);
            let _provider = build_provider(&m);
            let content = std::fs::read_to_string(dir.join(crate::diagnostics::FILE_NAME))
                .expect("arming the channel makes the construction line land with no env var");
            let want = format!("provider wire=openai-completions model=m/1 base={base_url}");
            assert!(
                content.lines().any(|line| line.ends_with(&want)),
                "the double fallback folds `{api:?}` onto the default wire — the line \
                 therefore records `wire=openai-completions` for it, indistinguishably \
                 from an explicit one (the limit the commit body states); got:\n{content}"
            );
        }

        // The teardown is the `_disable` guard, not a call here: an assertion
        // failure must not be able to leave the global aimed at a dir `Drop` removes.
    }

    /// `base_url` is free-text user configuration and it can BEAR A CREDENTIAL,
    /// so the construction line must carry a REDUCED form of it, never the raw
    /// string.
    ///
    /// Two shapes, both of which WORK (which is why a user keeps them, and why
    /// "it would be a broken config anyway" is not a defence):
    ///
    /// * `https://user:sekrit-token@gateway.example/v1/v1` — userinfo survives
    ///   `Url::parse(…).to_string()` verbatim, and reqwest's `extract_authority`
    ///   turns it into a working `Authorization: Basic …` header.
    /// * `https://acct.example/v1?sig=sekrit` — the query survives too, because
    ///   `normalize_wire_base_url` does PATH-only surgery (`set_path`) and never
    ///   touches the query. This is the Azure-OpenAI / SAS shape (`?api-version=`,
    ///   `?sig=`).
    ///
    /// And for every non-anthropic arm the builder passes `m.base_url.clone()`
    /// verbatim to the provider, so nothing anywhere reduces it.
    ///
    /// Why that is a defect rather than a detail: `diagnostics.log` is ALWAYS-ON
    /// with no opt-in, created by a plain `OpenOptions::create().append()` — so
    /// default umask, world-readable on a typical Linux desktop. `diagnostics`'
    /// module doc states the rule for a call site as "Not a transcript, not a
    /// prompt, not a model answer, **not a token, not a bearer**", and a URL whose
    /// userinfo is a Basic-auth password IS a bearer.
    ///
    /// The reduction keeps the PATH, because the path is what distinguishes two
    /// routes on one provider id — the fact the line was added to record.
    #[test]
    fn the_construction_line_never_carries_a_credential_bearing_base_url() {
        // The process-global diagnostics target: the contract the module documents
        // is `init` under this lock, and the guard keeps a failing assertion from
        // leaving the target aimed at a dir `Drop` then removes.
        let _guard = crate::test_support::env_lock();
        let dir = crate::test_support::ScratchDir::in_env_temp("provider-redact-test-", "dir");
        let _disable = DisableDiagnosticsOnExit;
        crate::diagnostics::init(Some(dir.to_path_buf()));

        for (base_url, kept) in [
            (
                "https://user:sekrit-token@gateway.example/v1/v1",
                "gateway.example/v1/v1",
            ),
            ("https://acct.example/v1?sig=sekrit", "acct.example/v1"),
        ] {
            // Both builders, so a reduction that lands in only ONE of them reddens
            // this rather than passing off the other's line. The file is truncated
            // per iteration so each line is proven by itself, and each iteration
            // uses a base URL no sibling test could be writing.
            for api in [Some("openai-completions"), Some("anthropic-messages")] {
                reset_diagnostics_file(dir.path());
                let m = model(base_url, api);
                let _provider = build_provider(&m);
                let _provider = build_provider_with_headers(&m, None);

                let content = std::fs::read_to_string(dir.join(crate::diagnostics::FILE_NAME))
                    .expect("the construction line lands in the always-on file with no env var");
                for secret in ["sekrit", "token", "user:", "sig="] {
                    assert!(
                        !content.contains(secret),
                        "`{secret}` from a credential-bearing `base_url` reached the \
                         always-on diagnostics log ({base_url} / {api:?}):\n{content}"
                    );
                }
                // The reduction is a REDUCTION, not a deletion: the wire, the host
                // and the path all survive — the path is what distinguishes two
                // routes on one provider id, the fact the line was added for.
                assert!(
                    content.contains(&format!(
                        "provider wire={} model=m/1 base=https://{kept}",
                        api.unwrap_or("openai-completions")
                    )),
                    "host, path and wire survive the reduction ({base_url} / {api:?}):\n{content}"
                );
            }
        }
    }

    /// An unparseable `base_url` yields something INERT, never the raw string —
    /// a bare string can itself be the credential (there is no scheme to parse a
    /// userinfo out of, and no parse means no reduction), and a `base_url` with
    /// an embedded newline forges a second line in the file.
    #[test]
    fn an_unparseable_base_url_falls_back_to_something_inert() {
        let _guard = crate::test_support::env_lock();
        let dir = crate::test_support::ScratchDir::in_env_temp("provider-inert-test-", "dir");
        let _disable = DisableDiagnosticsOnExit;
        crate::diagnostics::init(Some(dir.to_path_buf()));

        // `not a url:sekrit` parses as no scheme at all; `https://[bad` is a
        // malformed authority.
        build_provider(&model("not a url:sekrit", Some("openai-completions")));
        build_provider(&model("https://[bad", Some("openai-completions")));

        let content = std::fs::read_to_string(dir.join(crate::diagnostics::FILE_NAME))
            .expect("the line lands");
        assert!(
            !content.contains("sekrit"),
            "an unparseable base is not echoed verbatim:\n{content}"
        );
        // NOT a line count: a sibling test that reaches `build_provider` without
        // `env_lock()` (and ~17 of them do — see
        // `diagnostics::tests::a_line_is_timestamped_and_terminated` for why that
        // is now an accepted property rather than a bug) can legitimately write
        // into the dir this test armed. What IS pinned is that every line in here
        // is one this module emitted — a payload that does not begin with the
        // construction prefix is a forged line.
        for line in content.lines() {
            let payload = line.split_once(' ').map(|(_, rest)| rest).unwrap_or("");
            assert!(
                payload.starts_with("provider wire="),
                "every line is a construction line, not a forgery: {line:?}"
            );
        }
        assert!(
            content.contains("provider wire=openai-completions"),
            "both constructions still record:\n{content}"
        );
    }

    /// A provider's `/models` JSON can carry a newline in a model id, and the id
    /// is interpolated into the always-on line — so it must not be able to FORGE a
    /// second line.
    ///
    /// `catalog.rs`'s `expand_v1_models` takes `m.id` straight from the provider's
    /// response, filtered only for emptiness, so `{"id":"x\n<forged line>"}` is a
    /// value a hostile or merely buggy provider can deliver. The file it lands in
    /// is the one a supporter is asked to read, so a forged line there is a
    /// credibility problem, not a cosmetic one.
    #[test]
    fn a_newline_in_a_model_id_cannot_forge_a_line() {
        let _guard = crate::test_support::env_lock();
        let dir = crate::test_support::ScratchDir::in_env_temp("provider-inject-test-", "dir");
        let _disable = DisableDiagnosticsOnExit;
        crate::diagnostics::init(Some(dir.to_path_buf()));

        let mut m = model("http://127.0.0.1:9/v1", Some("openai-completions"));
        // The id a hostile `/models` row delivers, plus the `\r` form (which a
        // terminal would render as a rewrite, and which `lines()` would split on).
        m.id = "x\ndiagnostics: THE USER'S SESSION WAS RECORDED\rmore".to_string();
        build_provider(&m);

        let content = std::fs::read_to_string(dir.join(crate::diagnostics::FILE_NAME))
            .expect("the line lands");
        // Filtered on THIS test's unique model id, not on the shared `provider
        // wire=` prefix: a sibling test that reaches `build_provider` without
        // `env_lock()` can append its own `provider wire=…` line to the dir this
        // test armed, and a prefix filter would count that one too (the same
        // vacuity/flake class `a_line_is_timestamped_and_terminated` documents).
        let ours: Vec<&str> = content
            .lines()
            .filter(|line| line.contains("model=x diagnostics:"))
            .collect();
        assert_eq!(
            ours.len(),
            1,
            "the injected id produced no second line:\n{content}"
        );
        // The forged sentence must never OWN a line. Stated as "every line that
        // mentions it is one of ours" rather than "the file has N lines", so a
        // sibling's line cannot satisfy or break it either way.
        for line in content.lines() {
            if line.contains("THE USER'S SESSION WAS RECORDED") {
                let payload = line.split_once(' ').map(|(_, rest)| rest).unwrap_or("");
                assert!(
                    payload.starts_with("provider wire="),
                    "the forged text sits on a construction line, not on a line of \
                     its own: {line:?}"
                );
            }
        }
        assert!(
            content.contains("THE USER'S SESSION WAS RECORDED"),
            "the id is still legible (sanitised, not dropped):\n{content}"
        );
        assert!(
            ours[0].contains("more base="),
            "the id is still legible, with both newline forms replaced by spaces, \
             and the fields after it stayed on the SAME line:\n{}",
            ours[0]
        );
    }

    // ── the per-wire base-URL pass (T3b / #131) ───────────────────────

    /// A `Model` whose `api` carries a wire, for the recorded-path assertions.
    async fn record_paths(base_url: &str, api: &str) -> (String, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("the listener binds");
        let addr = listener.local_addr().expect("the listener has an address");
        let (server, mut rx) = recorded_dispatch_server(listener).await;
        let base = base_url.replace("{addr}", &addr.to_string());
        let provider = build_provider(&model(&base, Some(api)));
        let events: Vec<ProviderEvent> = provider
            .complete(&test_request())
            .await
            .expect("the request sends")
            .collect()
            .await;
        assert!(
            events
                .iter()
                .any(|e| matches!(e, ProviderEvent::Done(FinishReason::Stop))),
            "the {api} stream completes: {events:?}"
        );
        let (path, _auth, _key) = rx.recv().await.expect("the server recorded the request");
        (path, server)
    }

    /// The Messages rule at the `build_provider` call site: the base is
    /// normalized to the ROOT, so the request lands on `POST /v1/messages` —
    /// NOT `/messages` (a root-only base) and NOT `/v1/v1/messages` (the
    /// double-prefix the un-normalized `{root}/v1` base produced before T3b).
    #[tokio::test]
    async fn build_provider_posts_messages_at_root_normalized_v1() {
        // A ROOT base (no `/v1`): the pass leaves it, the client appends its
        // own `/v1/messages`.
        let (path, server) = record_paths("http://{addr}", "anthropic-messages").await;
        assert!(
            path.starts_with("post ") && path.contains(" /v1/messages"),
            "a root base posts /v1/messages: got {path:?}"
        );
        server.abort();

        // A `{root}/v1` base (the Settings' convention): the STRIP is the new
        // behavior — without it the client's own `/v1` append doubles it.
        let (path, server) = record_paths("http://{addr}/v1", "anthropic-messages").await;
        assert!(
            path.contains(" /v1/messages") && !path.contains("/v1/v1/"),
            "a `/v1` base is stripped to the root, never double-prefixed: got {path:?}"
        );
        server.abort();
    }

    /// Completions / Responses keep the configured base VERBATIM (the
    /// no-regression pin — every existing suite mounts the path its own base
    /// produces, and a root base stays a root base).
    #[tokio::test]
    async fn build_provider_keeps_the_configured_base_for_completions_and_responses() {
        // A root base → the client's own path, no appended `/v1` (byte-identical
        // to the pre-T3b behavior the unowned suites pin).
        let (path, server) = record_paths("http://{addr}", "openai-completions").await;
        assert!(
            path.contains(" /chat/completions") && !path.contains("/v1/chat"),
            "a root base posts /chat/completions verbatim: got {path:?}"
        );
        server.abort();
        let (path, server) = record_paths("http://{addr}", "openai-responses").await;
        assert!(
            path.contains(" /responses") && !path.contains("/v1/responses"),
            "a root base posts /responses verbatim: got {path:?}"
        );
        server.abort();

        // A `/v1` base → `/v1/…` (the configured shape, untouched).
        let (path, server) = record_paths("http://{addr}/v1", "openai-completions").await;
        assert!(
            path.contains(" /v1/chat/completions"),
            "a `/v1` base posts /v1/chat/completions: got {path:?}"
        );
        server.abort();
        let (path, server) = record_paths("http://{addr}/v1", "openai-responses").await;
        assert!(
            path.contains(" /v1/responses"),
            "a `/v1` base posts /v1/responses: got {path:?}"
        );
        server.abort();
    }

    /// The pass is idempotent THROUGH `build_provider`: feeding a base the
    /// helper itself would produce records the SAME path as feeding the raw
    /// base (the call-site half of the `n(n(u)) == n(u)` contract).
    #[tokio::test]
    async fn build_provider_base_pass_is_idempotent_on_v1_bases() {
        // ONE server, two bases (so the recorded paths are literally comparable
        // — the same authority in both).
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("the listener binds");
        let addr = listener.local_addr().expect("the listener has an address");
        let (server, mut rx) = recorded_dispatch_server(listener).await;

        /// Send through `build_provider` and return the RECORDED request line
        /// (one call site, so every recorded path is comparable).
        async fn record(
            addr: std::net::SocketAddr,
            api: &str,
            suffix: &str,
            rx: &mut tokio::sync::mpsc::Receiver<(String, bool, bool)>,
        ) -> String {
            let base = format!("http://{addr}{suffix}");
            let provider = build_provider(&model(&base, Some(api)));
            let events: Vec<ProviderEvent> = provider
                .complete(&test_request())
                .await
                .expect("the request sends")
                .collect()
                .await;
            assert!(
                events
                    .iter()
                    .any(|e| matches!(e, ProviderEvent::Done(FinishReason::Stop))),
                "the {api} stream completes: {events:?}"
            );
            rx.recv().await.expect("the server recorded the request").0
        }

        // Messages: `{root}/v1` (what the helper strips FROM) and the stripped
        // root (what it produces) record the IDENTICAL path.
        let from_v1 = record(addr, "anthropic-messages", "/v1", &mut rx).await;
        let from_root = record(addr, "anthropic-messages", "", &mut rx).await;
        assert_eq!(
            from_v1, from_root,
            "the Messages pass is idempotent through build_provider \
             (`{{root}}/v1` and the root both land on /v1/messages)"
        );
        assert!(
            from_v1.contains(" /v1/messages"),
            "and the shared path is /v1/messages: {from_v1:?}"
        );

        // Completions / Responses: the pass is the identity, so an already-
        // `/v1` base records exactly the path its base names (no double-append).
        let comp = record(addr, "openai-completions", "/v1", &mut rx).await;
        assert!(
            comp.contains(" /v1/chat/completions") && !comp.contains("/v1/v1/"),
            "a `/v1` completions base is never double-appended: {comp:?}"
        );
        let resp = record(addr, "openai-responses", "/v1", &mut rx).await;
        assert!(
            resp.contains(" /v1/responses") && !resp.contains("/v1/v1/"),
            "a `/v1` responses base is never double-appended: {resp:?}"
        );

        server.abort();
    }
}

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

/// Replace every control character and every Unicode line separator with a space,
/// so a field that came from outside this crate cannot forge a line in the
/// newline-delimited diagnostics file. Both are replaced (not stripped) so the
/// field stays delimited from the one that follows it.
///
/// `pub(crate)` rather than private: the three provider parsers each put a
/// provider-supplied tool-call `id` into a `diagnostics::record` payload (see
/// `openai::tool_call_dropped_with_no_name`), and a `\n` in a provider-supplied
/// field forges a line in a file a supporter reads. `reduced_base_url` below is
/// the second consumer, and the existing tests below pin both.
///
/// # Why `is_control()` and not just `\n` / `\r`
///
/// The two were enough while this sanitizer guarded only `m.id`, whose third-party
/// `/models` text reaches the line through one path. It is NOT enough now:
/// [`reduced_base_url`] runs this on the value [`extract_host`] produced, and
/// `extract_host` emits a **path verbatim** — the anchored arm keeps it on purpose
/// (§ "The path is kept"). A pasted `base_url` whose path carries `\u{2028}`,
/// `\u{2029}` or any other control byte therefore reaches the line with it intact.
///
/// The severity is genuinely low and this does not claim otherwise: `\u{2028}` and
/// friends are NOT `\n` or `\r`, so they cannot forge a line in a file delimited by
/// `\n`, and `is_control()`'s own additions (`\t`, NUL, `\x0b`, the C1 range)
/// likewise contain no line terminator. What they CAN do is break the reader —
/// `diagnostics` renders into a log a supporter reads, where NUL truncates the line
/// in most viewers and a line separator re-wraps it in anything Unicode-aware. This
/// closes that corridor because the corridor is new, not because it was ever a
/// forgery.
pub(crate) fn one_field_per_line(s: &str) -> String {
    s.chars()
        .map(|c| {
            if c.is_control() || c == '\u{2028}' || c == '\u{2029}' {
                ' '
            } else {
                c
            }
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
/// # The fallback REFUSES. That is the whole design.
///
/// A value is rendered only when one of two things is true of it:
///
/// 1. it parses as a URL whose scheme is on [`LOGGABLE_SCHEMES`] **and** which
///    has a real host — the reduction above is then applied by `Url` itself; or
/// 2. [`extract_host`] recognises one of its two anchored shapes, whose host is
///    a form that cannot BE a secret ([`host_cannot_be_a_secret`]) or which the
///    author explicitly anchored with `//`.
///
/// Everything else is `<unparseable>`. The previous shape of this function cut a
/// "host" out of the string by `split`/`rsplit` surgery and gated the result on
/// host-*likeness*:
///
/// ```text
/// host.chars().all(|c| c.is_ascii_alphanumeric() || matches!(c, '.'|'-'|'_'|':'|'['|']'))
/// ```
///
/// That charset is the alphabet of a modern API key, so the gate passed on the
/// credential instead of on a host. Measured against that version, all four of
/// `sk-proj-abc123XYZdef456ghi789`, `AKIAIOSFODNN7EXAMPLE`, a `ghp_…` PAT and a
/// JWT came back **byte-identical** to the input — and a real key is NOT
/// URL-shaped, so the fallback is the LIKELY path for the credential case, not
/// the rare one. A charset gate cannot be repaired by adding another character
/// class (that is how it broke), and blacklisting `sk-` / `AKIA` prefixes is a
/// losing game against the next vendor's key format. Hence refuse-by-default:
/// `base=<unparseable>` costs a supporter one ambiguous line, a key in the
/// always-on, world-readable, no-opt-in `diagnostics.log` costs a rotation.
///
/// # The two claims this function used to make that it did not keep
///
/// Both are now **true because the behaviour was changed to match them**, not
/// because the wording was softened:
///
/// * It said `host:port/path` and `//host/path` were extractable "through the
///   fallback". They were not reachable there: `localhost:8080/v1` and
///   `gw.example:443/v1` **parse successfully** as scheme `localhost` /
///   `gw.example` with NO host, so they took the success arm, where `has_host()`
///   is false and both userinfo strips are no-ops, and they were emitted whole.
///   Inert content, but an arm the comment described that no input could reach.
///   Such a value now reaches [`extract_host`] through the new
///   "parsed, but not loggable" route, so the shapes the comment named are
///   genuinely handled there — by an explicit, separately-tested extractor. What
///   it then does with them is the second claim's business: `localhost:8080/v1`
///   comes out byte-identical to what the old success arm emitted (so the
///   comment's promise holds), and `gw.example:443/v1` is now REFUSED, because a
///   scheme-less dotted host is the shape a JWT has.
/// * It said a non-special scheme (`unknown://u:p@host`) renders `://@host` for
///   an empty username, justifying a hand-anchored removal of the leftover `@`.
///   True of `unknown:`, which the allowlist now REFUSES; for every scheme that
///   survives, `Url` drops the empty userinfo itself (measured:
///   `http://@h/v1`, `http://:@h/v1` and `ws://:@h/v1` all render without the
///   `@`). The removal was therefore dead code for the reachable set and is gone.
fn reduced_base_url(base_url: &str) -> String {
    let reduced = match url::Url::parse(base_url.trim()) {
        // The reduction arm — `Url` does the userinfo/query/fragment surgery.
        // Gated on the scheme AND on a real host, because a parse SUCCEEDING is
        // not evidence of anything: `Url::parse` accepts any
        // `[a-zA-Z][a-zA-Z0-9+.-]*` prefix as a scheme, so `sk-proj-abc123:1234`
        // parses "successfully" with the key as its scheme and no host at all.
        Ok(mut url) if LOGGABLE_SCHEMES.contains(&url.scheme()) && url.has_host() => {
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
            Some(url.as_str().to_string())
        }
        // Either no parse at all, or a parse whose scheme is not one we will name
        // in a log (which includes the key-as-scheme case above).
        _ => extract_host(base_url),
    }
    .unwrap_or_else(|| "<unparseable>".to_string());
    one_field_per_line(&reduced)
}

/// The schemes this reducer will render WITH their scheme.
///
/// An allowlist, and it is load-bearing rather than tidy: `Url::parse` accepts
/// any `[a-zA-Z][a-zA-Z0-9+.-]*` prefix as a scheme, so a denylist would have to
/// name every scheme that can carry a secret — starting with the secret itself.
/// These four are the host-bearing web schemes a `base_url` is ever configured
/// with. `file:`, `data:` and `unix:` are deliberately out: their "authority" is
/// a PATH, and a path is somebody's secret at least as often as a host is (the
/// version this replaces emitted `file:///etc/passwd` and
/// `unix:///tmp/gw.sock` whole).
const LOGGABLE_SCHEMES: [&str; 4] = ["http", "https", "ws", "wss"];

/// The ONLY two shapes the fallback will extract a host from, and the only
/// thing standing between a pasted credential and the log line.
///
/// * **Anchored** — `scheme://authority…` or `//authority…`. The `//` is the
///   author's own declaration that an authority follows, which is exactly how
///   `Url` would read it, and a pasted key does not begin with it. The userinfo
///   is dropped whole (at the LAST `@`, so a percent-encoded one inside the
///   password cannot strand half of it) and the path is kept, query and fragment
///   dropped — the same reduction the `Url` arm applies. The port is optional
///   here, and a validated domain ([`is_domain`]) is accepted, because the `//`
///   already says "what follows is an authority".
/// * **Unanchored** — `host:port[/path]`, port PRESENT and valid, and host a
///   **literal** ([`host_cannot_be_a_secret`]) — an IPv4, a bracketed IPv6, or
///   `localhost`. No dotted domain is accepted here, and that is the load-bearing
///   decision, not an oversight:
///
/// A JWT is `base64.base64.base64` — two or more dot-separated labels of
/// alphanumerics, i.e. a syntactically valid domain with no marker to key on.
/// `gw.example:443/v1` and
/// `eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NSJ9.dXNlcl9zZWNyZXRfdmFsdWU:443` are
/// the SAME SHAPE, and no character rule separates them; prefix blacklists
/// (`sk-`, `AKIA`, `ghp_`, `eyJ…`) are a losing game against the next vendor's
/// key format, which is why the reviewer's suggested blacklist is worse than
/// refusing. So the unanchored shape keeps only the hosts a credential
/// structurally cannot be.
///
/// What that gives up: a scheme-less dotted host (`gw.example:443/v1`) now reads
/// `base=<unparseable>`. That costs nothing real — a base URL with no scheme is
/// BROKEN provider config (the HTTP client rejects a relative URL), so no working
/// deployment is made less legible, while every scheme-bearing config
/// (`https://gw.example/v1`) still renders in full through the `Url` arm, where
/// `Url` has parsed a real host and there is nothing left to guess.
///
/// THE ONE RESIDUAL, stated rather than glossed: an ANCHORED authority whose host
/// validates as a domain is emitted, so `//sk-proj-abc123.example/v1` yields
/// `sk-proj-abc123.example/v1`. Getting there takes a paste that begins with `//`
/// AND is dot-separated AND ends in a purely alphabetic label — a JWT fails the
/// last two today on its base64URL `_`, and would fail the first anyway. Closing
/// it completely means refusing domains everywhere, which is the `//host/path`
/// shape the reducer is specified to keep; narrowing it further means matching
/// secret prefixes, the game this function exists to stop playing.
fn extract_host(base_url: &str) -> Option<String> {
    let trimmed = base_url.trim();
    let after = if let Some((_, rest)) = trimmed.split_once("://") {
        rest
    } else if let Some(rest) = trimmed.strip_prefix("//") {
        rest
    } else {
        return extract_unanchored_host(trimmed);
    };
    // The authority is everything up to the first path/query/fragment delimiter.
    let authority = after.split(['/', '?', '#']).next().unwrap_or("");
    // The LAST `@`, so a percent-encoded `@` inside a password cannot strand the
    // front half of the userinfo.
    let host_and_port = authority.rsplit('@').next().unwrap_or("");
    // The third element says the host is a LITERAL; when it is false the host is
    // only good enough for an anchored authority, and only if it validates as a
    // domain.
    let (host, port, is_literal) = loggable_host_and_port(host_and_port)?;
    if !is_literal && !is_domain(&host) {
        return None;
    }
    Some(format!(
        "{host}{}{}",
        port.map(|p| format!(":{p}")).unwrap_or_default(),
        path_of(after)
    ))
}

/// The unanchored shape: `host:port[/path]`, port REQUIRED, host a literal.
///
/// Requiring the port is the second half of the anchor. Without it `sk-abc` and
/// `localhost` on their own would be emitted; with it, a value has to look like
/// something a person typed a listening socket for.
fn extract_unanchored_host(trimmed: &str) -> Option<String> {
    let head = trimmed.split(['/', '?', '#']).next().unwrap_or("");
    // A port must be PRESENT — that is what distinguishes the shape from a bare
    // pasted secret. Checked on the PARSED port rather than on "the string
    // contains a `:`", because a bracketed IPv6 literal is full of colons and
    // carries no port at all (`[::1]`), and that one is refused.
    let (host, port, is_literal) = loggable_host_and_port(head)?;
    port?;
    // Literal-only, unlike the anchored arm: a dotted label sequence is not
    // enough here (see [`extract_host`] — that is the shape a JWT has).
    is_literal.then_some(host).map(|host| {
        format!(
            "{host}{}{}",
            port.map(|p| format!(":{p}")).unwrap_or_default(),
            path_of(trimmed)
        )
    })
}

/// The path of an authority-then-path string: from the first `/`, stopping at
/// `?` / `#`. Empty when there is no path (the line then reads `host:port`,
/// which is the whole of what such a config says).
fn path_of(after_authority: &str) -> &str {
    let until_query = after_authority.split(['?', '#']).next().unwrap_or("");
    match until_query.find('/') {
        Some(i) => &until_query[i..],
        None => "",
    }
}

/// Split `host[:port]` (or `[ipv6]:port`) and accept it only when BOTH halves are
/// well-formed, lowercasing the host the way `Url` would.
///
/// The returned triple is `(host, port, is_literal)`. `is_literal` is the answer
/// to [`host_cannot_be_a_secret`] and it travels with the host rather than being
/// folded into `None`, because a non-literal host is NOT a failure: an anchored
/// authority may legitimately name a domain. The caller decides.
///
/// The IPv6 case is matched on brackets FIRST so a bare `::1` can never be
/// split into the fake `host:port` that the deleted `split(':').next()` produced
/// — that is the other way the old code mangled a real value.
fn loggable_host_and_port(authority: &str) -> Option<(String, Option<u16>, bool)> {
    let (host, port_text) = if let Some(rest) = authority.strip_prefix('[') {
        let end = rest.find(']')?;
        // +2: `end` indexes the `]` inside `rest`, which is one behind `authority`.
        let host = &authority[..end + 2];
        let after = &rest[end + 1..];
        match after {
            "" => (host, None),
            other => (host, Some(other.strip_prefix(':')?)),
        }
    } else {
        match authority.split_once(':') {
            Some((host, port)) => (host, Some(port)),
            None => (authority, None),
        }
    };
    let port = match port_text {
        None => None,
        Some(p) => {
            if p.is_empty() || p.len() > 5 || !p.bytes().all(|b| b.is_ascii_digit()) {
                return None;
            }
            let value: u32 = p.parse().ok()?;
            if value == 0 || value > 65535 {
                return None;
            }
            Some(value as u16)
        }
    };
    let host = host.to_ascii_lowercase();
    Some((host.clone(), port, host_cannot_be_a_secret(&host)))
}

/// A host that is STRUCTURALLY incapable of being a credential: an IPv4 literal,
/// a bracketed IPv6 literal, or `localhost`.
///
/// These are the only three host forms accepted outside a parsed URL. A key is
/// alphanumeric entropy; none of these can be, and none of them has a dot
/// followed by another label, which is the shape a JWT shares with a domain.
fn host_cannot_be_a_secret(host: &str) -> bool {
    if let Some(inner) = host.strip_prefix('[').and_then(|h| h.strip_suffix(']')) {
        // An IPv6 literal: hex and `:` only, so a zone id (`%eth0`) and anything
        // that is not an address are both refused.
        return !inner.is_empty() && inner.bytes().all(|b| b.is_ascii_hexdigit() || b == b':');
    }
    host.eq_ignore_ascii_case("localhost") || is_ipv4(host)
}

/// Four dot-separated decimal octets, each 1-3 digits and <= 255.
fn is_ipv4(host: &str) -> bool {
    let mut labels = host.split('.');
    let (a, b, c, d) = (labels.next(), labels.next(), labels.next(), labels.next());
    // Exactly four: a fifth label makes `labels.next()` `Some` and fails here.
    if labels.next().is_some() {
        return false;
    }
    [a, b, c, d].into_iter().all(|label| {
        label.is_some_and(|l| {
            !l.is_empty()
                && l.len() <= 3
                && l.bytes().all(|b| b.is_ascii_digit())
                && l.parse::<u16>().is_ok_and(|v| v <= 255)
        })
    })
}

/// A multi-label domain: at least two labels (a bare `sk-proj-abc` is ONE and is
/// therefore refused), each 1-63 chars of alphanumerics with INNER hyphens only,
/// and a final label of pure letters (a trailing digit means a malformed IP).
///
/// Only ever consulted on an ANCHORED authority — see [`extract_host`], whose
/// asymmetry explains why this is not trusted on the unanchored one.
fn is_domain(host: &str) -> bool {
    if host.is_empty() || host.len() > 253 || !host.contains('.') {
        return false;
    }
    let mut labels = host.split('.').peekable();
    while let Some(label) = labels.next() {
        let last = labels.peek().is_none();
        if label.is_empty()
            || label.len() > 63
            || !label
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || c == b'-')
            || label.starts_with('-')
            || label.ends_with('-')
        {
            return false;
        }
        if last && !label.bytes().all(|c| c.is_ascii_alphabetic()) {
            return false;
        }
    }
    true
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
    ///
    /// This test used to pass FOR THE WRONG REASON: its only two inputs were
    /// `not a url:sekrit` (rejected because of the SPACE) and `https://[bad`
    /// (a malformed authority), and neither is remotely credential-shaped. The
    /// four real key formats — which are exactly what a user pastes into the wrong
    /// Settings row — all sailed through the old host-likeness gate and were
    /// echoed byte-identical. They are in here now, so the test fails on the bug
    /// it claims to catch. `a_pasted_api_key_never_survives_the_reducer` is the
    /// per-shape unit form of the same property.
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
        // The four credential shapes, at the same sink.
        for secret in [
            "sk-proj-abc123XYZdef456ghi789",
            "AKIAIOSFODNN7EXAMPLE",
            "ghp_16C7e42KpqpX6q9F1kQ3vZ8rLmYbNc0pQrSt",
            "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NSJ9.dXNlcl9zZWNyZXRfdmFsdWU",
        ] {
            build_provider(&model(secret, Some("openai-completions")));
        }

        let content = std::fs::read_to_string(dir.join(crate::diagnostics::FILE_NAME))
            .expect("the line lands");
        assert!(
            !content.contains("sekrit"),
            "an unparseable base is not echoed verbatim:\n{content}"
        );
        // No credential-shaped run survives ANYWHERE in the file — the assertion
        // the four shapes above need, and one that echoing cannot satisfy.
        for secret in [
            "sk-proj-abc123XYZdef456ghi789",
            "AKIAIOSFODNN7EXAMPLE",
            "ghp_16C7e42KpqpX6q9F1kQ3vZ8rLmYbNc0pQrSt",
            "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NSJ9.dXNlcl9zZWNyZXRfdmFsdWU",
        ] {
            assert!(
                !content.contains(&secret[..16]),
                "`{secret}` was echoed into the always-on file:\n{content}"
            );
        }
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

    /// Does a run of 12+ alphanumeric characters appear anywhere in `s`?
    ///
    /// The shape of an assertion the REDUCTION has to satisfy rather than the
    /// ECHO: a real host (`gateway.example`, `127.0.0.1`, `[::1]`) and the
    /// `<unparseable>` placeholder both have no such run, while every one of the
    /// credential shapes below carries one or more runs far longer than 12. A test
    /// that only asserted `!= input` would be satisfied by a reducer that appended
    /// a marker to the raw value; this one is not.
    fn has_long_alnum_run(s: &str) -> bool {
        s.split(|c: char| !c.is_ascii_alphanumeric())
            .any(|run| run.len() >= 12)
    }

    /// The four credential shapes a user actually pastes into the wrong Settings
    /// row, each REDUCED TO NOTHING.
    ///
    /// These are the shapes `reduced_base_url`'s fallback used to ECHO VERBATIM.
    /// Its host-likeness gate allowed `[A-Za-z0-9._-:[]]`, which is precisely the
    /// alphabet of a modern API key, so the gate passed on the credential rather
    /// than on a host — and a real key is NOT URL-shaped, so the fallback is the
    /// LIKELY path for exactly the credential case, not the rare one.
    ///
    /// The charset gate could not be repaired by adding a character class (that is
    /// how it broke), and blacklisting `sk-` / `AKIA` prefixes is a losing game
    /// against the next vendor's format. The reducer now REFUSES BY DEFAULT: a
    /// value is emitted only when it parses as a URL with an explicitly allowed
    /// host-bearing scheme and a real host, or when the ONE extractor recognises
    /// an anchored `host[:port][/path]` / `//host/path` shape. `base=<unparseable>`
    /// costs a supporter one line of ambiguity; a key in the always-on,
    /// world-readable `diagnostics.log` costs a credential rotation.
    #[test]
    fn a_pasted_api_key_never_survives_the_reducer() {
        // The four shapes the reviewer reproduced, plus the two variants that
        // reach the OTHER arm (see the `scheme` cases below): a key carrying a
        // `:` parses as a URL whose SCHEME is the key.
        let secrets = [
            // OpenAI's project-scoped key: `[A-Za-z0-9-]` only, no delimiter.
            "sk-proj-abc123XYZdef456ghi789",
            // An AWS access key id: alphanumeric only.
            "AKIAIOSFODNN7EXAMPLE",
            // A GitHub PAT: the `_` is the only character outside a hostname's
            // alphabet, so a charset gate alone would be doing all the work.
            "ghp_16C7e42KpqpX6q9F1kQ3vZ8rLmYbNc0pQrSt",
            // A JWT: base64URL segments joined by `.` — DOTS, i.e. the shape a
            // "does it look like a domain" heuristic mistakes for a host.
            "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NSJ9.dXNlcl9zZWNyZXRfdmFsdWU",
            // The same key with a port glued on: `Url::parse` reads the key as
            // the SCHEME and the whole thing still parses successfully, so this
            // one reached the SUCCESS arm and was echoed there too (lowercased).
            "sk-proj-abc123:1234",
            // The same JWT with a port: same scheme-masquerade, success arm.
            "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NSJ9.dXNlcl9zZWNyZXRfdmFsdWU:443",
            // A bare key with a path glued on (a paste that caught the row's
            // trailing `/v1`).
            "sk-proj-abc123XYZdef456ghi789/v1",
        ];
        for secret in secrets {
            let reduced = reduced_base_url(secret);
            assert_eq!(
                reduced, "<unparseable>",
                "`{secret}` is not a URL the log may render — it must be refused, \
                 not reduced"
            );
            assert!(
                !has_long_alnum_run(&reduced),
                "`{secret}` left a credential-shaped run in `{reduced}`"
            );
            assert!(
                !reduced.contains(&secret[..12]),
                "the head of `{secret}` survives in `{reduced}`"
            );
        }
    }

    /// The same property at the REAL SINK, not on the helper: the always-on
    /// `diagnostics.log`, with no env var and no opt-in.
    ///
    /// Each iteration gives the `Model` a UNIQUE id, so the asserted line is
    /// unambiguously this test's — a sibling `build_provider` call that takes no
    /// `env_lock()` cannot satisfy it (the ~17 unlocked call sites the module doc
    /// names) and cannot forge it either.
    #[test]
    fn a_pasted_api_key_never_reaches_the_always_on_log() {
        let _guard = crate::test_support::env_lock();
        let dir = crate::test_support::ScratchDir::in_env_temp("provider-keyleak-test-", "dir");
        let _disable = DisableDiagnosticsOnExit;
        crate::diagnostics::init(Some(dir.to_path_buf()));

        for (n, secret) in [
            "sk-proj-abc123XYZdef456ghi789",
            "AKIAIOSFODNN7EXAMPLE",
            "ghp_16C7e42KpqpX6q9F1kQ3vZ8rLmYbNc0pQrSt",
            "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NSJ9.dXNlcl9zZWNyZXRfdmFsdWU",
        ]
        .into_iter()
        .enumerate()
        {
            reset_diagnostics_file(dir.path());
            let tag = format!("keyleak-{n}");
            let mut m = model(secret, Some("openai-completions"));
            m.id = tag.clone();
            build_provider(&m);

            let content = std::fs::read_to_string(dir.join(crate::diagnostics::FILE_NAME))
                .expect("the construction line lands with no env var");
            let ours: Vec<&str> = content
                .lines()
                .filter(|line| line.contains(&format!("model={tag}")))
                .collect();
            assert_eq!(
                ours.len(),
                1,
                "this iteration's own line is in the file exactly once:\n{content}"
            );
            assert!(
                ours[0].ends_with("base=<unparseable>"),
                "the line records `base=<unparseable>`, not a reduction of the \
                 pasted key: {}\n{content}",
                ours[0]
            );
            assert!(
                !ours[0].contains(&secret[..12]),
                "the pasted key `{secret}` leaked into the always-on file: {}",
                ours[0]
            );
        }
    }

    /// The extractor is not merely a refusal: the shapes it exists for still
    /// come through, WITH their port and their path.
    ///
    /// This is the refusal's counterpart — the positive half. Without it the
    /// whole fix could be satisfied by deleting the fallback and returning
    /// `<unparseable>` unconditionally, which would be safe and would silently
    /// take away the line's only value for the configs people actually run
    /// (`localhost:8080/v1`, `127.0.0.1:11434`).
    ///
    /// Note WHICH arm each of these used to reach. `localhost:8080/v1` PARSES
    /// SUCCESSFULLY — as scheme `localhost` with NO host — so it never went
    /// through the fallback the comment claimed handled it; it was emitted whole
    /// by the success arm's `.map`, where `has_host()` is false and both userinfo
    /// strips are no-ops. It now reaches the extractor on purpose, so the comment
    /// is true because the BEHAVIOUR was changed to match it, not because the
    /// claim was reworded.
    #[test]
    fn the_extractor_emits_the_anchored_host_shapes_with_port_and_path() {
        for (base_url, want) in [
            // `host:port/path` — parses as a bogus scheme today (success arm).
            ("localhost:8080/v1", "localhost:8080/v1"),
            ("Localhost:8080/v1", "localhost:8080/v1"),
            ("localhost:8080", "localhost:8080"),
            // The query and the fragment go, exactly as in the success arm.
            ("localhost:8080/v1?a=b#c", "localhost:8080/v1"),
            // `//host/path` — a parse failure today.
            ("//gw.example/v1", "gw.example/v1"),
            ("//gw.example:8443/v1", "gw.example:8443/v1"),
            ("//gw.example", "gw.example"),
            // A scheme-relative URL with userinfo: the userinfo is the credential
            // and it is dropped, never reduced.
            ("//user:sekrit@gw.example/v1", "gw.example/v1"),
            // An IPv4 literal, with its port. The port was DROPPED before (the
            // string surgery split on `:` and kept the left half); keeping it is
            // what makes `127.0.0.1:11434` and `127.0.0.1:8080` distinguishable
            // in the file.
            ("127.0.0.1:11434", "127.0.0.1:11434"),
            ("127.0.0.1:11434/v1", "127.0.0.1:11434/v1"),
            ("10.0.0.5:11434/v1", "10.0.0.5:11434/v1"),
            // An IPv6 literal — brackets and all, never mangled into a fake
            // `host:port` by a `:` split (`loggable_host_and_port` matches the
            // brackets before it looks for a port, which is the thing the deleted
            // `split(':').next()` could not do).
            ("[::1]:8080/v1", "[::1]:8080/v1"),
            ("[::1]:8080", "[::1]:8080"),
        ] {
            assert_eq!(
                reduced_base_url(base_url),
                want,
                "`{base_url}` is a host the log may name"
            );
        }
    }

    /// A dotted host with NO scheme and NO `//` anchor is refused, even though it
    /// looks like a hostname.
    ///
    /// This is the deliberate cost of the design, stated where someone will
    /// argue with it. `gw.example:443/v1` and
    /// `eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NSJ9.dXNlcl9zZWNyZXRfdmFsdWU:443`
    /// are the SAME SHAPE — a JWT is dot-separated alphanumerics, which is a
    /// valid DNS label sequence — so accepting the first on shape alone means
    /// accepting the second. The anchor (`//`, or a real scheme that `Url`
    /// parsed) is the only thing that tells them apart, and a pasted key has
    /// neither. The loss is nil: a scheme-less base URL is BROKEN provider
    /// config, and the working form (`https://gw.example/v1`) renders in full.
    #[test]
    fn an_unanchored_dotted_host_is_refused_because_a_jwt_has_that_shape() {
        for base_url in [
            "gw.example:443/v1",
            "api.openai.com:443/v1",
            // The JWT with a port glued on, i.e. the same shape as the line
            // above, asserted side by side so the equivalence is on the record.
            "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NSJ9.dXNlcl9zZWNyZXRfdmFsdWU:443",
        ] {
            assert_eq!(
                reduced_base_url(base_url),
                "<unparseable>",
                "`{base_url}` is indistinguishable from a JWT carrying a port"
            );
        }
    }

    /// The refusal is structural, so the shapes that are NOT hosts are refused —
    /// including the ones that used to be emitted whole because they parsed.
    ///
    /// The scheme set is an ALLOWLIST, and that is load-bearing rather than
    /// tidiness: `Url::parse` accepts any `[a-zA-Z][a-zA-Z0-9+.-]*` prefix as a
    /// scheme, so a secret with a `:` in it parses as a URL whose scheme is the
    /// secret. `file:` / `data:` / `unix:` are out for the same reason in reverse:
    /// their authority is a PATH, and a path is somebody's secret at least as
    /// often as a host is.
    #[test]
    fn only_an_allowed_scheme_with_a_real_host_is_rendered() {
        for (base_url, want) in [
            // The allowed schemes, reduced as before: userinfo (both halves),
            // query and fragment gone, path kept.
            (
                "https://user:sekrit-token@gateway.example/v1/v1",
                "https://gateway.example/v1/v1",
            ),
            (
                "https://acct.example/v1?sig=sekrit",
                "https://acct.example/v1",
            ),
            (
                "https://user:pass@host.example/v1?sig=x#frag",
                "https://host.example/v1",
            ),
            // A PERCENT-ENCODED userinfo is still a userinfo: `Url` decodes
            // `%40` when it exposes the username, so the strip removes it whole
            // rather than leaving `%40h@` behind.
            (
                "https://u:p%40h@host.example/v1#frag",
                "https://host.example/v1",
            ),
            // A username with no password, and the reverse, both clear to nothing.
            ("http://user@host.example/v1", "http://host.example/v1"),
            ("http://:pw@host.example/v1", "http://host.example/v1"),
            // A scheme-relative parse of an allowed scheme keeps working.
            ("http://127.0.0.1:9/v1", "http://127.0.0.1:9/v1"),
            // The two other host-bearing web schemes.
            ("ws://user:pw@gw.example/v1", "ws://gw.example/v1"),
            ("wss://gw.example/v1", "wss://gw.example/v1"),
            // REFUSED: a non-allowed scheme. `file:`/`unix:`/`data:` were echoed
            // whole before, which is a filesystem path in the log — `unix://`
            // carried a socket path and `data:` carried whatever followed it.
            ("file:///etc/passwd", "<unparseable>"),
            ("unix:///var/run/secrets/gw.sock", "<unparseable>"),
            ("data:text/plain,hello", "<unparseable>"),
            // A non-allowed scheme whose authority IS a host: the host survives
            // (via the extractor) and the userinfo does not.
            ("unknown://u:sekrit@host.example/v1", "host.example/v1"),
            // REFUSED: the empty host and the empty value. `https://:8080/v1` has
            // no host at all — `:8080` is not a host that got a port.
            ("http://", "<unparseable>"),
            ("https://:8080/v1", "<unparseable>"),
            ("", "<unparseable>"),
            ("   ", "<unparseable>"),
            // REFUSED: a bare label with no anchor. This is the pasted-key shape
            // (`sk-proj-abc`), and it is also `localhost` on its own — an
            // acceptable price, because a base URL worth logging carries a port,
            // a path or a scheme.
            ("sk-abc", "<unparseable>"),
            ("localhost", "<unparseable>"),
            ("gw.example", "<unparseable>"),
            // REFUSED: a single label is not a domain, so `key:port` does not
            // become `host:port`. This is the `sk-proj-abc123:1234` case again,
            // asserted through the extractor rather than the parse.
            ("myserver:8080/v1", "<unparseable>"),
            // REFUSED: a malformed port (not digits, out of range) and a
            // malformed host (an empty label, a leading hyphen).
            ("gw.example:http/v1", "<unparseable>"),
            ("gw.example:99999/v1", "<unparseable>"),
            ("gw.example.", "<unparseable>"),
            ("-bad.example:80", "<unparseable>"),
            // REFUSED: the free-text case the original test used, and the
            // malformed authority.
            ("not a url:sekrit", "<unparseable>"),
            ("https://[bad", "<unparseable>"),
        ] {
            assert_eq!(
                reduced_base_url(base_url),
                want,
                "`{base_url}` reduces to exactly the credential-free form, and \
                 nothing else"
            );
            assert!(
                !reduced_base_url(base_url).contains("sekrit")
                    && !reduced_base_url(base_url).contains("pw@"),
                "`{base_url}` left a credential in the line"
            );
        }
    }

    /// Pins the ONE residual the reducer's docblock states, so it stays a
    /// documented limit and not a surprise.
    ///
    /// An anchored authority whose host validates as a domain is emitted — the
    /// `//host/path` shape the reducer is specified to keep. That means a paste
    /// that begins with `//` AND is dot-separated AND ends in an alphabetic label
    /// comes through. The test asserts the boundary on BOTH sides: the JWT form
    /// (the realistic credential) is refused even anchored, because base64URL
    /// segments carry `_` and `-` and their final label is not purely alphabetic.
    #[test]
    fn the_anchored_domain_residual_is_the_documented_one() {
        // The residual, pinned as-is: an anchored, well-formed DOMAIN name is
        // treated as a host. Widening this set is the thing to argue with.
        assert_eq!(reduced_base_url("//gw.example/v1"), "gw.example/v1");
        assert_eq!(
            reduced_base_url("//sk-proj-abc123.example/v1"),
            "sk-proj-abc123.example/v1",
            "the stated residual"
        );
        // And the credential shapes stay refused EVEN anchored — this is the half
        // that makes the residual narrow rather than a hole.
        for anchored in [
            "//eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NSJ9.dXNlcl9zZWNyZXRfdmFsdWU/v1",
            "//AKIAIOSFODNN7EXAMPLE",
            "//ghp_16C7e42KpqpX6q9F1kQ3vZ8rLmYbNc0pQrSt",
            "//sk-proj-abc123XYZdef456ghi789",
        ] {
            assert_eq!(
                reduced_base_url(anchored),
                "<unparseable>",
                "`{anchored}` is refused even with the `//` anchor"
            );
        }
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

    /// The sanitizer covers every control character and both Unicode line
    /// separators, not just `\n` / `\r` — because the value it now sanitises is
    /// not the one it was written for.
    ///
    /// `reduced_base_url` runs this on whatever [`extract_host`] produced, and the
    /// anchored arm keeps the PATH VERBATIM (deliberately — it is the fact the log
    /// line exists to record). So a path carrying `\u{2028}`, `\u{2029}`, NUL,
    /// `\x0b` or a C1 byte reaches the diagnostics line, which is a corridor the
    /// original `\n | \r` match could not reach because the fallback was inert
    /// then.
    ///
    /// What is asserted is the honest claim: none of these bytes survives. What is
    /// NOT claimed is that any of them could FORGE a line — none of them is `\n`
    /// or `\r`, so none is a line terminator here. The bug this closes is a log a
    /// supporter cannot read (NUL truncates in most viewers, U+2028 re-wraps in
    /// anything Unicode-aware), not an injection.
    #[test]
    fn control_characters_in_a_reduced_base_url_never_reach_the_line() {
        // `grpc:` is not on `LOGGABLE_SCHEMES`, so this reaches `extract_host`
        // rather than the `Url` reduction arm — the only route that emits a path
        // verbatim. The host is a domain and the `//` anchor is present, which is
        // the shape `extract_host` renders.
        for (name, byte) in [
            ("U+2028 LINE SEPARATOR", '\u{2028}'),
            ("U+2029 PARAGRAPH SEPARATOR", '\u{2029}'),
            ("NUL", '\0'),
            ("VT", '\u{000b}'),
            ("FF", '\u{000c}'),
            ("ESC", '\u{001b}'),
            ("C1", '\u{0085}'),
            ("TAB", '\t'),
        ] {
            let raw = format!("grpc://gw.example/v1{name}/tail", name = byte);
            let reduced = reduced_base_url(&raw);
            assert!(
                reduced
                    .chars()
                    .all(|c| !c.is_control() && c != '\u{2028}' && c != '\u{2029}'),
                "{name} survived the sanitizer: {reduced:?}"
            );
            // The reduction still happened (the test is not passing because the
            // whole value was refused): the host and both path halves are there.
            assert!(
                reduced.starts_with("gw.example/v1"),
                "{name}: the anchored reduction stopped being rendered, so this \
                 assertion is passing on `<unparseable>`: {reduced:?}"
            );
            assert!(
                reduced.contains("/tail"),
                "{name}: the path is still emitted verbatim apart from the byte \
                 under test: {reduced:?}"
            );
        }
        // And `\n` / `\r` — the ORIGINAL pair — still work, so the extension did
        // not replace them.
        assert_eq!(
            reduced_base_url("grpc://gw.example/a\nb\rc"),
            "gw.example/a b c",
            "the two forms the sanitizer was written for still become spaces"
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

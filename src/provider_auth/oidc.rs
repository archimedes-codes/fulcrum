//! Direct OIDC authorization-code + PKCE login against an external identity
//! provider (issue #127) — the `oidc` flow (Okta / Keycloak / Entra ID, whose
//! validated `id_token` becomes the Bearer token sent to the LiteLLM proxy).
//!
//! Discovery at `GET {issuer}/.well-known/openid-configuration`, a sequential
//! `redirect_ports` loopback binding via T5's [`CallbackServer::bind_with`]
//! (ephemeral fallback), the code exchange at the IdP `token_endpoint`, and
//! claims validation of the `id_token` WITHOUT local signature verification (the
//! LiteLLM proxy validates against the IdP's JWKS — the spec's deliberate scope
//! boundary; adding a JWT crate to SIGN tokens nobody here verifies is the wrong
//! economy, so this module decodes with `base64` + `serde_json` only).
//! The validated `id_token` is injected as the Bearer token to the proxy through
//! `resolve_bearer`'s arm 2 — substituted into `Model.api_key` (interface §1: NO
//! new `Model` field, nothing downstream changes).
//!
//! **HEADER HYGIENE (an acceptance criterion, pinned by
//! `gateway_headers_never_reach_the_idp`):** every IdP-bound request (discovery,
//! token exchange, refresh) goes out on a FRESH, headerless [`reqwest::Client`]
//! built by [`auth_client`] in THIS module. There is no header injection point
//! here at all: no `default_headers`, no `header(` call — provider/gateway
//! headers attach only in `agent::mcp::http::HttpClient` and the resolve spine.
//! The client also refuses redirects (`Policy::none()`): a `307/308` must never
//! replay the code + `code_verifier` to wherever `Location` points (the same
//! discipline `litellm_oauth` states for the native-client contract).
//!
//! **RULING R4 (interface §10):** the ONE constant-time compare in this crate is
//! T5's `pub(crate) fn constant_time_eq` in `agent::mcp::callback`, IMPORTED
//! below. This module defines NO second implementation — `mcp/callback.rs` is
//! read-only for this task, and the structural lint
//! `the_constant_time_compare_is_imported_not_copy_pasted` in
//! `tests/provider_oidc.rs` reddens if a copy-pasted definition ever appears
//! here instead of the import.

use std::time::Duration;

use base64::Engine as _;
use rand::Rng as _;
use sha2::Digest as _;
use tokio_util::sync::CancellationToken;

use crate::agent::mcp::callback::constant_time_eq;
use crate::agent::mcp::callback::CallbackServer;
use crate::agent::provider_auth::store::{self, OidcCredential};
use crate::config::OidcConfig;

/// The callback wait: 10 MINUTES — the same caller-side duration T5's native
/// client passes to [`CallbackServer::wait`] (a parameter, not a constant in
/// `callback.rs`). Long enough that a user fumbling an SSO login is not cut off;
/// the command's caller can cancel at any time through `cancel`.
const CALLBACK_TIMEOUT: Duration = Duration::from_secs(600);

/// The grace absorbed by the `exp` check (30 s): an IdP and a desktop whose
/// clocks disagree by a few seconds must not produce a spurious login failure.
/// It is a TOLERANCE, not a licence to serve a long-expired token — a minute of
/// skew passes, an hour does not.
const EXP_GRACE_SECS: i64 = 30;

/// The path the loopback callback server binds, and therefore the path a
/// corporate IdP's redirect-URI allowlist entry must carry
/// (`http://127.0.0.1:<port>/callback`). The authorize request and the IdP's
/// redirect both use this EXACT string (redirect URIs match literally).
const CALLBACK_PATH: &str = "/callback";

/// The two discovery fields this flow reads (OIDC Discovery / RFC 8414 §3).
/// Deliberately NOT `mcp::oauth::ServerMetadata` (a different contract, and this
/// flow does no dynamic registration so `registration_endpoint` is meaningless
/// here). Everything else in the document is discarded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OidcMetadata {
    pub authorization_endpoint: String,
    pub token_endpoint: String,
}

/// The identity claims validated out of an `id_token` (NO signature
/// verification — see the module doc).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidatedClaims {
    /// The account subject — THE refresh-consistency key (the account anchor).
    pub sub: String,
    pub iss: String,
    pub aud: Vec<String>,
    pub azp: Option<String>,
    /// Epoch SECONDS (the JWT `exp` unit — NOT this store's milliseconds).
    pub exp: i64,
    /// The nonce claim, when the IdP echoed one.
    pub nonce: Option<String>,
}

/// One completed OIDC login: the artifacts the caller persists as a
/// [`OidcCredential`] through the contract store.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OidcSession {
    /// The VALIDATED `id_token` — this is what becomes the proxy Bearer.
    pub id_token: String,
    /// The access token (kept for diagnostics; NEVER the proxy bearer — the
    /// contract's bearer is the `id_token`).
    pub access_token: String,
    pub refresh_token: Option<String>,
    /// Epoch MILLISECONDS (the contract store's unit — `expires_in` arrives in
    /// seconds and is converted here, once).
    pub expires_at: Option<i64>,
    /// The validated `sub` (the account identity).
    pub sub: String,
    /// The IdP `token_endpoint`, stored so a refresh needs no re-discovery.
    pub token_endpoint: String,
    /// The scope actually requested.
    pub scope: String,
}

// ── the PKCE / encoding helpers ───────────────────────────────────────────
//
// The same three primitives `mcp::oauth` and `provider_auth::litellm_oauth`
// carry: those copies are module-private in files this task does not own, so the
// ~20 lines are copied rather than a shared file widened (T5's recorded decision
// #1 — the algorithms are frozen, RFC 7636 / RFC 3986, and each copy names its
// siblings so a reader can compare them).

/// Base64url WITHOUT padding (RFC 4648 §5) — the PKCE / nonce / `state` /
/// JWT-payload encoding.
fn base64url(input: &[u8]) -> String {
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;
    URL_SAFE_NO_PAD.encode(input)
}

/// A PKCE pair: a random 32-byte `verifier` (base64url, 43 chars — RFC 7636 §4.1
/// allows 43..=128) + its S256 `challenge`
/// (`base64url(SHA-256(verifier_ascii))`).
///
/// The `verifier` NEVER leaves this process except in the ONE token-endpoint
/// form field: it is held in the orchestrator's stack frame and is NEVER written
/// to the store (only the exchanged token pair is persisted, via the existing
/// atomic 0600 `store::store_credential`).
fn pkce() -> (String, String) {
    let mut verifier_bytes = [0u8; 32];
    rand::rng().fill(&mut verifier_bytes);
    let verifier = base64url(&verifier_bytes);
    let digest = sha2::Sha256::digest(verifier.as_bytes());
    let challenge = base64url(&digest);
    (verifier, challenge)
}

/// A random URL-safe value (the `state` CSRF guard and the `nonce`): base64url
/// of 16 random bytes (43 chars).
fn random_token() -> String {
    let mut bytes = [0u8; 16];
    rand::rng().fill(&mut bytes);
    base64url(&bytes)
}

/// Percent-encode a query/form value (RFC 3986 unreserved set only) — the same
/// minimal encoder the sibling flows use.
fn url_encode(s: &str) -> String {
    let mut out = String::new();
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

/// Split `id_token` on `.`, base64url-decode segment 2 (the payload), JSON-parse
/// the claims object. `Err` on a malformed token shape.
///
/// NO signature verification, deliberately: the LiteLLM proxy validates the
/// signature against the IdP's JWKS (the spec's split of responsibilities). This
/// decode exists so the CLAIMS the flow depends on (`iss`/`aud`/`azp`/`sub`/
/// `exp`/`nonce`) are actually checked instead of trusted blindly — an
/// unverified token whose `iss` is wrong is still a token we must not carry to
/// the proxy.
fn decode_claims_unverified(id_token: &str) -> Result<serde_json::Value, String> {
    let segments: Vec<&str> = id_token.split('.').collect();
    if segments.len() != 3 {
        return Err(format!(
            "the id_token is not a three-segment JWT (got {} segments)",
            segments.len()
        ));
    }
    // Tolerate a padded payload segment (some IdPs emit standard base64url WITH
    // `=` padding; `URL_SAFE_NO_PAD` refuses it).
    let payload = segments[1].trim_end_matches('=');
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;
    let bytes = URL_SAFE_NO_PAD
        .decode(payload)
        .map_err(|e| format!("the id_token payload is not base64url: {e}"))?;
    let claims: serde_json::Value = serde_json::from_slice(&bytes)
        .map_err(|e| format!("the id_token payload is not a JSON claims object: {e}"))?;
    if !claims.is_object() {
        return Err("the id_token payload is not a JSON object".to_string());
    }
    Ok(claims)
}

/// Build a JWT-shaped `id_token` from claims (a TEST helper — the tests mint
/// their own unverifiable tokens, which is the whole point of the no-signature
/// design). `pub` so the integration test crate can use it; production never
/// calls it.
pub fn build_test_id_token(claims: &serde_json::Value) -> String {
    let header = base64url(br#"{"alg":"none","typ":"JWT"}"#);
    let payload = base64url(&serde_json::to_vec(claims).expect("claims serialize"));
    format!("{header}.{payload}.signature")
}

// ── the claims validation (the acceptance-criterion core) ─────────────────

/// Validate the claims of a decoded `id_token` (JWKS verification is
/// deliberately OUT of scope — the proxy validates the signature). EVERY check
/// fails the login on mismatch; there is no "warn and continue" path:
///
/// - `iss` == the configured issuer (compared with any trailing `/` trimmed from
///   BOTH sides — the two spellings of one issuer are the same issuer, and the
///   discovery URL is built the same trimmed way);
/// - `aud` CONTAINS `client_id` (both the string and the array form — OIDC
///   allows either);
/// - `azp` == `client_id` WHEN PRESENT — an absent `azp` PASSES (the spec's
///   "matches the `client_id` IF present" rule); a present `azp` that differs
///   rejects;
/// - `sub` non-empty and not merely whitespace;
/// - `exp` (epoch SECONDS) is in the future, within [`EXP_GRACE_SECS`] of skew;
/// - `nonce` == the nonce generated for THIS login, compared in CONSTANT TIME
///   ([`constant_time_eq`], the R4 import). A missing nonce claim is a mismatch:
///   we sent a nonce, so an IdP that dropped it cannot prove the token is ours.
///
/// `expected_nonce` is `Some` for the LOGIN (the code exchange), where we minted a
/// nonce and the IdP must echo it. It is `None` for a REFRESH: no authorization
/// request happened, so there is nothing to echo (the OIDC core sends the nonce at
/// the authorize endpoint only), and the identity question at that stage is
/// answered by the strictly stronger `sub` guard in [`refresh`] — which is why
/// `refresh` never passes `Some` here.
///
/// `sub` is returned even for a token with no refresh token — it is the account
/// anchor every later refresh is checked against.
fn validate_claims(
    claims: &serde_json::Value,
    config: &OidcConfig,
    expected_nonce: Option<&str>,
    now: i64,
) -> Result<ValidatedClaims, String> {
    // `iss` — the known value (not a secret), so an ordinary equality is fine;
    // the trailing-slash tolerance keeps `https://idp` and `https://idp/` the
    // same issuer (both are valid spellings of one discovery document's home).
    let iss = claims
        .get("iss")
        .and_then(serde_json::Value::as_str)
        .ok_or("the id_token carries no `iss` claim")?;
    let want_iss = config.issuer.trim_end_matches('/');
    if !constant_time_eq(iss.as_bytes(), want_iss.as_bytes())
        && !constant_time_eq(iss.trim_end_matches('/').as_bytes(), want_iss.as_bytes())
    {
        return Err(format!(
            "the id_token issuer `{iss}` is not the configured issuer `{}`",
            config.issuer
        ));
    }

    // `aud` — a string OR an array (both are legal); the client id must be ONE
    // of them.
    let aud: Vec<String> = match claims.get("aud") {
        Some(serde_json::Value::String(one)) => vec![one.clone()],
        Some(serde_json::Value::Array(many)) => many
            .iter()
            .filter_map(|v| v.as_str().map(str::to_string))
            .collect(),
        _ => return Err("the id_token carries no `aud` claim".to_string()),
    };
    if !aud
        .iter()
        .any(|one| constant_time_eq(one.as_bytes(), config.client_id.as_bytes()))
    {
        return Err(format!(
            "the id_token audience {:?} does not include the client id `{}`",
            aud, config.client_id
        ));
    }

    // `azp` — checked only when PRESENT (the spec's conditional).
    if let Some(azp) = claims.get("azp").and_then(serde_json::Value::as_str) {
        if !constant_time_eq(azp.as_bytes(), config.client_id.as_bytes()) {
            return Err(format!(
                "the id_token `azp` claim `{azp}` is not the client id `{}`",
                config.client_id
            ));
        }
    }

    // `sub` — the account anchor. An empty or whitespace subject cannot anchor
    // anything, so it is rejected HERE rather than stored and discovered later.
    let sub = claims
        .get("sub")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default();
    if sub.trim().is_empty() {
        return Err("the id_token `sub` claim is empty (no account to bind)".to_string());
    }

    // `exp` — epoch SECONDS, in the future within the skew grace. A missing `exp`
    // is rejected: OIDC requires it, and an unbounded token is not something to
    // keep handing to a proxy.
    let exp = claims
        .get("exp")
        .and_then(serde_json::Value::as_i64)
        .ok_or("the id_token carries no `exp` claim")?;
    if exp.saturating_add(EXP_GRACE_SECS) <= now {
        return Err(format!(
            "the id_token expired at {exp} (now {now}) — the login did not complete in time"
        ));
    }

    // `nonce` — the replay guard, in constant time (the R4 import).
    let nonce = claims.get("nonce").and_then(serde_json::Value::as_str);
    if let Some(expected_nonce) = expected_nonce {
        // The LOGIN path: we minted a nonce, so an IdP that dropped it or echoed
        // somebody else's cannot prove this token answers OUR request.
        let Some(nonce) = nonce else {
            return Err(
                "the id_token carries no `nonce` claim (the login nonce cannot be \
                        verified)"
                    .to_string(),
            );
        };
        if !constant_time_eq(nonce.as_bytes(), expected_nonce.as_bytes()) {
            return Err(
                "the id_token `nonce` does not match the nonce this login sent".to_string(),
            );
        }
    }

    Ok(ValidatedClaims {
        sub: sub.to_string(),
        iss: iss.to_string(),
        aud,
        azp: claims
            .get("azp")
            .and_then(serde_json::Value::as_str)
            .map(str::to_string),
        exp,
        // `None` on the refresh path, where the IdP echoes no nonce (the claim is
        // carried out for the record when it IS present).
        nonce: nonce.map(str::to_string),
    })
}

/// The scope this login asks for: the configured scope with `openid` appended
/// when absent (OIDC refuses a code without it), and `openid` alone when the row
/// configured nothing.
fn effective_scope(config: &OidcConfig) -> String {
    let Some(scope) = config
        .scope
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
    else {
        return "openid".to_string();
    };
    let has_openid = scope.split_whitespace().any(|word| word == "openid");
    if has_openid {
        scope.to_string()
    } else {
        format!("{scope} openid")
    }
}

/// The authorization URL: `response_type=code`, the pre-registered public
/// `client_id`, the exact `redirect_uri`, the PKCE `S256` challenge + method,
/// `state` (the CSRF guard), `nonce` (the replay guard) and the scope. Every
/// value percent-encoded.
#[allow(clippy::too_many_arguments)]
fn authorize_url(
    authorization_endpoint: &str,
    client_id: &str,
    redirect_uri: &str,
    state: &str,
    nonce: &str,
    challenge: &str,
    scope: &str,
) -> String {
    format!(
        "{authorization_endpoint}?response_type=code&client_id={}&redirect_uri={}\
         &scope={}&state={}&nonce={}&code_challenge={}&code_challenge_method=S256",
        url_encode(client_id),
        url_encode(redirect_uri),
        url_encode(scope),
        url_encode(state),
        url_encode(nonce),
        url_encode(challenge),
    )
}

/// A fresh, headerless, redirect-refusing client for IdP traffic: a 30 s timeout
/// (reqwest has NO default) and `Policy::none()` so a redirect can never replay
/// the code + verifier somewhere else. THE only client this module builds — see
/// the module doc's header-hygiene rule; the structural census test in
/// `tests/provider_oidc.rs` reddens if a header call is ever added here.
fn auth_client() -> Result<reqwest::Client, String> {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .map_err(|e| e.to_string())
}

/// `GET {issuer}/.well-known/openid-configuration` and read the two endpoints
/// this flow needs. A trailing slash on the configured issuer is trimmed before
/// the well-known path is appended (a double slash is a different URL to every
/// real server). `Err` on a non-2xx / non-JSON / missing endpoint — the caller
/// surfaces the text (an interface §1 degrade: the user is told, never blanked).
pub async fn discover(issuer: &str, client: &reqwest::Client) -> Result<OidcMetadata, String> {
    let issuer = issuer.trim_end_matches('/');
    let url = format!("{issuer}/.well-known/openid-configuration");
    let resp = client
        .get(&url)
        .send()
        .await
        .map_err(|e| format!("the OIDC discovery request failed: {e}"))?;
    let status = resp.status();
    if !status.is_success() {
        return Err(format!("the OIDC discovery request failed: HTTP {status}"));
    }
    let doc: serde_json::Value = resp
        .json()
        .await
        .map_err(|e| format!("the OIDC discovery document was not JSON: {e}"))?;
    let read = |key: &str| -> Result<String, String> {
        doc.get(key)
            .and_then(serde_json::Value::as_str)
            .map(str::to_string)
            .ok_or_else(|| format!("the OIDC discovery document carries no `{key}`"))
    };
    Ok(OidcMetadata {
        authorization_endpoint: read("authorization_endpoint")?,
        token_endpoint: read("token_endpoint")?,
    })
}

/// Bind ONE callback server on `127.0.0.1:<port>` (`0` = ephemeral) with the
/// redirect URI `http://127.0.0.1:<port>/callback` — T5's shared
/// [`CallbackServer::bind_with`] (interface §10, "also settled": T6 uses T5's
/// server, it does not write a second one). A bind failure is an `Err` meaning
/// "this port is taken" — the cascade in [`bind_callback`] treats it that way.
async fn bind_callback(port: u16) -> Result<CallbackServer, String> {
    CallbackServer::bind_with(port, CALLBACK_PATH)
        .await
        .map_err(|e| format!("could not bind the OIDC callback listener: {e}"))
}

/// The `redirect_ports` cascade: try each configured port IN ORDER (a corporate
/// IdP with a fixed redirect-URI allowlist only accepts those), take the FIRST
/// bind that succeeds, and fall back to ONE ephemeral bind when the list is
/// empty or every entry failed. A final failure is an `Err` — the orchestrator
/// turns it into the manual-URL path rather than hanging (ADR 0018).
///
/// The bind IS the probe: a port already in use (or privileged, under the
/// non-root CI user) fails here, which is exactly the signal to move on.
async fn bind_callback_cascade(redirect_ports: &[u16]) -> Result<CallbackServer, String> {
    let mut last = String::from("no redirect ports were configured");
    for port in redirect_ports {
        // Port 0 in a hand-written config means "ephemeral", which the fallback
        // below does anyway — asking the OS for it twice buys nothing.
        if *port == 0 {
            last = "port 0 (ephemeral) is the fallback, not a configured port".to_string();
            continue;
        }
        match bind_callback(*port).await {
            Ok(server) => return Ok(server),
            Err(e) => last = e,
        }
    }
    bind_callback(0).await.map_err(|e| {
        format!(
            "every configured redirect port failed to bind ({last}); the \
                             ephemeral fallback failed too: {e}"
        )
    })
}

/// Exchange the authorization `code` at the IdP `token_endpoint` (public client:
/// form-encoded with `client_id`, NO client secret — the same shape as
/// `litellm_oauth::exchange_code`'s no-secret branch) with the PKCE
/// `code_verifier`, then CAPTURE the `id_token` (the MCP `parse_token_response`
/// drops it; this flow is its first consumer) and run [`validate_claims`] over
/// it against `expected_nonce`.
///
/// `expires_at` becomes MILLISECONDS (the store's unit); `expires_in` arrives in
/// SECONDS.
#[allow(clippy::too_many_arguments)]
async fn exchange_code(
    metadata: &OidcMetadata,
    config: &OidcConfig,
    code: &str,
    verifier: &str,
    redirect_uri: &str,
    nonce: &str,
    scope: &str,
    client: &reqwest::Client,
) -> Result<OidcSession, String> {
    let form: Vec<(&str, String)> = vec![
        ("grant_type", "authorization_code".to_string()),
        ("code", code.to_string()),
        ("redirect_uri", redirect_uri.to_string()),
        ("client_id", config.client_id.clone()),
        ("code_verifier", verifier.to_string()),
        // RFC 6749 §4.1.3 / the OIDC core's public-client rule: a public client
        // that authenticated at the authorize endpoint WITHOUT a secret repeats
        // its `client_id` in the token request body. (The sibling
        // `litellm_oauth::exchange_code` sends the same shape.)
        ("scope", scope.to_string()),
        // The nonce is re-sent at the token endpoint (the OIDC core's
        // public-client recommendation) so an IdP that re-derives the `id_token`
        // nonce server-side still echoes the value we validate below.
        ("nonce", nonce.to_string()),
    ];
    let resp = client
        .post(&metadata.token_endpoint)
        .form(&form)
        .send()
        .await
        .map_err(|e| format!("the OIDC token request failed: {e}"))?;
    let status = resp.status();
    if !status.is_success() {
        let text = resp.text().await.unwrap_or_default();
        return Err(format!(
            "the OIDC token request failed: HTTP {status} {text}"
        ));
    }
    let body: serde_json::Value = resp
        .json()
        .await
        .map_err(|e| format!("the OIDC token response was not JSON: {e}"))?;
    let id_token = body
        .get("id_token")
        .and_then(serde_json::Value::as_str)
        .ok_or(
            "the OIDC token response carried no `id_token` (the validated identity is \
                what this flow sends to the proxy)",
        )?
        .to_string();
    let claims = decode_claims_unverified(&id_token)?;
    let validated = validate_claims(
        &claims,
        config,
        Some(nonce),
        crate::agent::mcp::oauth::now_epoch_secs(),
    )?;
    let expires_at = body
        .get("expires_in")
        .and_then(serde_json::Value::as_i64)
        .map(|secs| store::now_epoch_millis().saturating_add(secs.saturating_mul(1000)));
    Ok(OidcSession {
        id_token,
        access_token: body
            .get("access_token")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
            .to_string(),
        refresh_token: body
            .get("refresh_token")
            .and_then(serde_json::Value::as_str)
            .map(str::to_string),
        expires_at,
        sub: validated.sub,
        token_endpoint: metadata.token_endpoint.clone(),
        scope: scope.to_string(),
    })
}

/// Refresh one stored OIDC credential: `POST` the stored `refresh_token` (+ the
/// `client_id`, public client) to the STORED `token_endpoint` on a fresh
/// headerless client, decode + validate the NEW `id_token`'s claims, and — THE
/// GUARD — reject the whole refresh when the new `sub` differs from the stored
/// one.
///
/// **`sub` consistency (the sharpest acceptance criterion):** an IdP whose
/// refreshed token names a DIFFERENT account means the session behind the
/// browser tab changed identities. Serving that token would silently swap the
/// user's account on the proxy. So a mismatch is an `Err` that escapes BEFORE any
/// write: the caller (the Supervisor loop / the resolve arm) keeps the stored,
/// still-valid credential and the user re-logs in when it finally expires. The
/// compare rides the R4 import (one implementation, audited once).
///
/// A rotated `refresh_token` REPLACES the stored one; an absent one KEEPS it
/// (never clear what we hold). An absent `expires_in` keeps the stored expiry.
pub async fn refresh(
    cred: &OidcCredential,
    // The contract's dispatcher passes its client; the header-hygiene rule means
    // the POST goes out on this module's own fresh client instead (the same
    // choice `litellm_oauth::refresh_pkce` records). The parameter stays because
    // the dispatcher's signature is the interface's, not ours.
    _client: &reqwest::Client,
) -> Result<OidcCredential, String> {
    let token_endpoint = cred.token_endpoint.as_deref().ok_or(
        "an oidc credential with no stored token endpoint cannot be refreshed \
         (re-log in to re-discover it)",
    )?;
    let refresh_token = cred
        .refresh_token
        .as_deref()
        .ok_or("an oidc credential with no stored refresh token (re-log in)")?;
    let config = OidcConfig {
        issuer: cred.issuer.clone(),
        // The `client_id` is not stored on the credential, so the aud/azp check
        // at refresh time uses the `aud` the stored token itself carried —
        // see `OidcCredential::client_id`, which IS stored for exactly this.
        client_id: cred.client_id.clone().ok_or(
            "an oidc credential with no stored client id (re-log in so the audience can be \
             verified)",
        )?,
        scope: cred.scope.clone(),
        redirect_ports: Vec::new(),
    };
    let client = auth_client()?;
    let form: Vec<(&str, String)> = vec![
        ("grant_type", "refresh_token".to_string()),
        ("refresh_token", refresh_token.to_string()),
        ("client_id", config.client_id.clone()),
    ];
    let resp = client
        .post(token_endpoint)
        .form(&form)
        .send()
        .await
        .map_err(|e| format!("the OIDC refresh request failed: {e}"))?;
    let status = resp.status();
    if !status.is_success() {
        // A rejected grant (400 `invalid_grant`) and a transient 5xx share ONE
        // policy at this level: fail the attempt, keep the stored credential
        // (T1's rule — a failed refresh never discards a still-valid token).
        return Err(format!("the OIDC token endpoint answered {status}"));
    }
    let body: serde_json::Value = resp
        .json()
        .await
        .map_err(|e| format!("the OIDC token endpoint response was not JSON: {e}"))?;
    let Some(id_token) = body.get("id_token").and_then(serde_json::Value::as_str) else {
        // A refresh that returns no identity proves nothing about the account, so
        // it is NOT a refresh we can serve: the guard needs a `sub` to check.
        return Err(
            "the OIDC refresh response carried no `id_token` (the account identity \
                    cannot be verified)"
                .to_string(),
        );
    };
    let claims = decode_claims_unverified(id_token)?;
    // The nonce was sent at AUTHORIZATION time, not at refresh time — an IdP
    // does not echo it here — so the refresh validates everything EXCEPT the
    // nonce. The `sub` guard below is what covers the identity question at this
    // stage (and it is strictly stronger than a nonce for a refresh).
    let validated = validate_claims(
        &claims,
        &config,
        // `None`: a refresh performs no authorization, so there is no nonce to
        // echo — the `sub` guard below is the identity check at this stage.
        None,
        crate::agent::mcp::oauth::now_epoch_secs(),
    )
    .map_err(|e| format!("the refreshed id_token failed validation: {e}"))?;
    // THE GUARD. Equal-length compare, constant time, before anything is written.
    if !constant_time_eq(validated.sub.as_bytes(), cred.sub.as_bytes()) {
        return Err(format!(
            "the refreshed id_token belongs to a DIFFERENT account (`{}` than the stored `{}`) — \
             refusing to swap accounts; the stored credential is kept and a re-login is required",
            validated.sub, cred.sub
        ));
    }
    let expires_at = match body.get("expires_in").and_then(serde_json::Value::as_i64) {
        Some(secs) => Some(store::now_epoch_millis().saturating_add(secs.saturating_mul(1000))),
        None => cred.expires_at,
    };
    Ok(OidcCredential {
        id_token: id_token.to_string(),
        access_token: body
            .get("access_token")
            .and_then(serde_json::Value::as_str)
            .map(str::to_string)
            .or_else(|| cred.access_token.clone()),
        refresh_token: body
            .get("refresh_token")
            .and_then(serde_json::Value::as_str)
            .map(str::to_string)
            // A server that does not rotate must not cost us the token we hold.
            .or_else(|| cred.refresh_token.clone()),
        expires_at,
        sub: validated.sub,
        ..cred.clone()
    })
}

/// The login orchestrator (the command and — via the resolve arm's absence — the
/// only entry to this flow):
///
/// 1. [`discover`] on a fresh headerless client.
/// 2. Nonce + `state` (random) + the PKCE verifier/challenge.
/// 3. The [`bind_callback_cascade`] port loop.
/// 4. Build the authorize URL and hand it to `open_browser` (the ADR 0051 seam —
///    production passes `crate::open::open(url, None)`, tests pass a closure that
///    drives the callback: **no test ever launches a browser**), then wait on the
///    callback for `state` (constant-time inside `CallbackServer::wait`).
/// 5. [`exchange_code`] → the validated [`OidcSession`].
///
/// A bind failure or a failed callback surfaces the authorize URL in the `Err`
/// text so the user can complete the login by hand (ADR 0018 — never a hang,
/// never a dead end). The caller persists the session through the store.
pub async fn authenticate_oidc(
    config: &OidcConfig,
    cancel: &CancellationToken,
    open_browser: &(dyn Fn(&str) + Send + Sync),
) -> Result<OidcSession, String> {
    let client = auth_client()?;
    let metadata = discover(&config.issuer, &client).await?;
    let (verifier, challenge) = pkce();
    let state = random_token();
    let nonce = random_token();
    let scope = effective_scope(config);
    let server = bind_callback_cascade(&config.redirect_ports).await?;
    let redirect_uri = server.redirect_uri().to_string();
    let url = authorize_url(
        &metadata.authorization_endpoint,
        &config.client_id,
        &redirect_uri,
        &state,
        &nonce,
        &challenge,
        &scope,
    );
    // Fire-and-forget (the `mcp::oauth` / `litellm_oauth` rule): a failure to
    // launch the browser is not fatal — the URL is reported so a human can open
    // it, and the SPAWNED browser leg must never be followed from here (the flow
    // is about to wait for the very callback that leg triggers; joining it would
    // deadlock the login against its own browser).
    open_browser(&url);
    let callback = server
        .wait(&state, CALLBACK_TIMEOUT, cancel)
        .await
        .map_err(|e| format!("{e} (authorize manually at: {url})"))?;
    exchange_code(
        &metadata,
        config,
        &callback.code,
        &verifier,
        &redirect_uri,
        &nonce,
        &scope,
        &client,
    )
    .await
}

/// Turn one [`OidcSession`] into the credential the store holds (`flow:
/// "oidc"` — the refresh dispatcher's arm key). One place, so the command and
/// any future caller write the SAME record.
pub fn credential_from_session(session: &OidcSession, config: &OidcConfig) -> OidcCredential {
    OidcCredential {
        flow: "oidc".to_string(),
        id_token: session.id_token.clone(),
        access_token: (!session.access_token.is_empty()).then(|| session.access_token.clone()),
        refresh_token: session.refresh_token.clone(),
        expires_at: session.expires_at,
        sub: session.sub.clone(),
        issuer: config.issuer.clone(),
        client_id: Some(config.client_id.clone()),
        token_endpoint: Some(session.token_endpoint.clone()),
        scope: Some(session.scope.clone()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> OidcConfig {
        OidcConfig {
            issuer: "https://idp.example".to_string(),
            client_id: "archimedes".to_string(),
            scope: None,
            redirect_ports: vec![],
        }
    }

    /// Claims a compliant IdP would answer (`now` in seconds; the nonce is the
    /// one the login generated).
    fn claims(now: i64, nonce: &str) -> serde_json::Value {
        serde_json::json!({
            "iss": "https://idp.example",
            "aud": ["archimedes"],
            "sub": "user-42",
            "exp": now + 3600,
            "nonce": nonce,
        })
    }

    #[test]
    fn base64url_is_url_safe_and_unpadded() {
        // 32 random bytes → 43 chars, and NO padding or the base64 alphabet's
        // `+` / `/` (both would break a PKCE verifier on the wire).
        let encoded = base64url(&[0xfb, 0xff, 0xfe, 0xff, 0xbf, 0xfe]);
        assert!(!encoded.contains('='), "{encoded}");
        assert!(!encoded.contains('+'), "{encoded}");
        assert!(!encoded.contains('/'), "{encoded}");
        assert_eq!(base64url(b""), "");
        assert_eq!(base64url(&[0u8; 32]).len(), 43);
    }

    #[test]
    fn pkce_challenge_is_the_s256_of_the_verifier() {
        // RFC 7636 §4.2, pinned against an independently-computed digest of the
        // SAME verifier rather than against this module's own helper twice.
        let (verifier, challenge) = pkce();
        let expected = {
            use base64::engine::general_purpose::URL_SAFE_NO_PAD;
            URL_SAFE_NO_PAD.encode(sha2::Sha256::digest(verifier.as_bytes()))
        };
        assert_eq!(challenge, expected, "the challenge is S256(verifier)");
        assert_eq!(verifier.len(), 43, "a 32-byte verifier is 43 chars");
        assert_ne!(verifier, pkce().0, "the verifier is random per call");
    }

    #[test]
    fn random_tokens_are_random_and_url_safe() {
        // 16 random bytes → 22 base64url chars, unpadded (the `state` and the
        // `nonce` — CSRF/replay guards, not PKCE verifiers, so 16 bytes is the
        // sibling flows' own size).
        let a = random_token();
        assert_eq!(a.len(), 22);
        assert!(!a.contains('='));
        assert!(!a.contains('+'));
        assert!(!a.contains('/'));
        assert_ne!(a, random_token());
    }

    #[test]
    fn decode_claims_unverified_reads_the_payload_segment() {
        // A hand-built three-segment token (the header and the signature are
        // never read — that is the design).
        let token = build_test_id_token(&serde_json::json!({"sub": "abc", "iss": "https://x"}));
        let claims = decode_claims_unverified(&token).expect("the payload decodes");
        assert_eq!(claims["sub"], "abc");
        assert_eq!(claims["iss"], "https://x");
        // A padded payload segment still decodes (some IdPs pad).
        let segments: Vec<&str> = token.split('.').collect();
        let padded = format!("{}.{}=", segments[0], segments[1]);
        let padded = format!("{padded}.sig");
        assert_eq!(
            decode_claims_unverified(&padded).expect("a padded payload decodes")["sub"],
            "abc"
        );
        // And the malformed shapes are refused, not guessed at.
        assert!(decode_claims_unverified("not.a.jwt.at.all").is_err());
        assert!(decode_claims_unverified("onlytwosegments.here").is_err());
        assert!(decode_claims_unverified("hdr.!!!notbase64!!!.sig").is_err());
        // A payload that decodes but is not an object (a bare scalar).
        use base64::engine::general_purpose::URL_SAFE_NO_PAD;
        let scalar = format!(
            "h.{}.s",
            URL_SAFE_NO_PAD.encode(serde_json::to_vec(&42).unwrap())
        );
        assert!(decode_claims_unverified(&scalar).is_err());
    }

    #[test]
    fn validate_claims_accepts_a_compliant_id_token() {
        let now = 1_700_000_000;
        let nonce = "nonce-value";
        let validated = validate_claims(&claims(now, nonce), &config(), Some(nonce), now)
            .expect("a valid token");
        assert_eq!(validated.sub, "user-42");
        assert_eq!(validated.iss, "https://idp.example");
        assert_eq!(validated.aud, vec!["archimedes".to_string()]);
        assert_eq!(validated.azp, None, "an absent azp stays absent");
        assert_eq!(validated.exp, now + 3600);
        assert_eq!(validated.nonce.as_deref(), Some(nonce));
    }

    #[test]
    fn validate_claims_accepts_an_aud_string_and_a_present_azp() {
        // OIDC allows `aud` as a bare string when there is one audience…
        let now = 1_700_000_000;
        let nonce = "n";
        let mut one = claims(now, nonce);
        one["aud"] = serde_json::json!("archimedes");
        assert!(validate_claims(&one, &config(), Some(nonce), now).is_ok());
        // …and `azp` may be present, in which case it must MATCH the client id.
        let mut with_azp = claims(now, nonce);
        with_azp["azp"] = serde_json::json!("archimedes");
        let validated =
            validate_claims(&with_azp, &config(), Some(nonce), now).expect("a matching azp passes");
        assert_eq!(validated.azp.as_deref(), Some("archimedes"));
        // A MULTI-audience array that includes us is fine (a token minted for
        // several clients).
        let mut many = claims(now, nonce);
        many["aud"] = serde_json::json!(["other-client", "archimedes"]);
        assert!(validate_claims(&many, &config(), Some(nonce), now).is_ok());
    }

    #[test]
    fn validate_claims_rejects_a_wrong_issuer() {
        let now = 1_700_000_000;
        let nonce = "n";
        let mut bad = claims(now, nonce);
        bad["iss"] = serde_json::json!("https://attacker.example");
        let err = validate_claims(&bad, &config(), Some(nonce), now).unwrap_err();
        assert!(err.contains("issuer"), "the error names it: {err}");
        // A trailing slash on the CONFIGURED issuer is the same issuer (the
        // discovery URL is built the same trimmed way).
        let slashed = OidcConfig {
            issuer: "https://idp.example/".to_string(),
            ..config()
        };
        assert!(validate_claims(&claims(now, nonce), &slashed, Some(nonce), now).is_ok());
        // An absent `iss` is not a token we can attribute.
        let mut missing = claims(now, nonce);
        missing.as_object_mut().unwrap().remove("iss");
        assert!(validate_claims(&missing, &config(), Some(nonce), now).is_err());
    }

    #[test]
    fn validate_claims_rejects_an_audience_without_the_client_id() {
        let now = 1_700_000_000;
        let nonce = "n";
        for shape in [
            serde_json::json!("someone-else"),
            serde_json::json!(["a", "b"]),
        ] {
            let mut bad = claims(now, nonce);
            bad["aud"] = shape.clone();
            let err = validate_claims(&bad, &config(), Some(nonce), now).unwrap_err();
            assert!(
                err.contains("audience"),
                "the {shape} case names the audience: {err}"
            );
        }
        let mut missing = claims(now, nonce);
        missing.as_object_mut().unwrap().remove("aud");
        assert!(validate_claims(&missing, &config(), Some(nonce), now).is_err());
    }

    #[test]
    fn validate_claims_rejects_a_present_azp_that_differs() {
        let now = 1_700_000_000;
        let nonce = "n";
        let mut bad = claims(now, nonce);
        bad["azp"] = serde_json::json!("some-other-client");
        let err = validate_claims(&bad, &config(), Some(nonce), now).unwrap_err();
        assert!(err.contains("azp"), "the error names azp: {err}");
    }

    #[test]
    fn validate_claims_rejects_an_empty_sub() {
        let now = 1_700_000_000;
        let nonce = "n";
        for empty in ["", "   ", "\t"] {
            let mut bad = claims(now, nonce);
            bad["sub"] = serde_json::json!(empty);
            let err = validate_claims(&bad, &config(), Some(nonce), now).unwrap_err();
            assert!(err.contains("sub"), "the {empty:?} case names sub: {err}");
        }
        // And a missing `sub` entirely.
        let mut missing = claims(now, nonce);
        missing.as_object_mut().unwrap().remove("sub");
        assert!(validate_claims(&missing, &config(), Some(nonce), now).is_err());
    }

    #[test]
    fn validate_claims_rejects_an_expired_token_but_absorbs_clock_skew() {
        let now = 1_700_000_000;
        let nonce = "n";
        // Expired by an hour: rejected.
        let mut stale = claims(now, nonce);
        stale["exp"] = serde_json::json!(now - 3600);
        let err = validate_claims(&stale, &config(), Some(nonce), now).unwrap_err();
        assert!(err.contains("expired"), "the error says expired: {err}");
        // Expired by 10 s: inside the 30 s skew grace, accepted.
        let mut skewed = claims(now, nonce);
        skewed["exp"] = serde_json::json!(now - 10);
        assert!(validate_claims(&skewed, &config(), Some(nonce), now).is_ok());
        // Expired by a minute: outside the grace, rejected.
        let mut outside = claims(now, nonce);
        outside["exp"] = serde_json::json!(now - 60);
        assert!(validate_claims(&outside, &config(), Some(nonce), now).is_err());
        // No `exp` at all: refused (an unbounded token is not something to keep).
        let mut missing = claims(now, nonce);
        missing.as_object_mut().unwrap().remove("exp");
        assert!(validate_claims(&missing, &config(), Some(nonce), now).is_err());
    }

    #[test]
    fn validate_claims_rejects_a_wrong_or_absent_nonce() {
        let now = 1_700_000_000;
        let mut bad = claims(now, "nonce-we-sent");
        bad["nonce"] = serde_json::json!("nonce-from-somewhere-else");
        let err = validate_claims(&bad, &config(), Some("nonce-we-sent"), now).unwrap_err();
        assert!(err.contains("nonce"), "the error names the nonce: {err}");
        // A missing nonce claim is a mismatch: we SENT one, so an IdP that
        // dropped it cannot prove this token answers our request.
        let mut missing = claims(now, "nonce-we-sent");
        missing.as_object_mut().unwrap().remove("nonce");
        assert!(validate_claims(&missing, &config(), Some("nonce-we-sent"), now).is_err());
    }

    #[test]
    fn effective_scope_always_asks_for_openid() {
        let base = config();
        assert_eq!(effective_scope(&base), "openid", "nothing configured");
        let with = OidcConfig {
            scope: Some("email profile".to_string()),
            ..base.clone()
        };
        assert_eq!(effective_scope(&with), "email profile openid");
        let already = OidcConfig {
            scope: Some("openid email".to_string()),
            ..base.clone()
        };
        assert_eq!(effective_scope(&already), "openid email", "not doubled");
        let blank = OidcConfig {
            scope: Some("   ".to_string()),
            ..base
        };
        assert_eq!(effective_scope(&blank), "openid");
    }

    #[test]
    fn authorize_url_carries_the_pkce_and_state_parameters() {
        let url = authorize_url(
            "https://idp.example/authorize",
            "client id",
            "http://127.0.0.1:9/callback",
            "st te",
            "nonce",
            "chal",
            "openid email",
        );
        assert!(
            url.starts_with("https://idp.example/authorize?response_type=code&"),
            "{url}"
        );
        // Every value is percent-encoded (a space in the client id or the scope
        // must not split the query).
        assert!(url.contains("client_id=client%20id"), "{url}");
        assert!(url.contains("scope=openid%20email"), "{url}");
        assert!(url.contains("state=st%20te"), "{url}");
        assert!(url.contains("nonce=nonce"), "{url}");
        assert!(url.contains("code_challenge=chal"), "{url}");
        assert!(url.contains("code_challenge_method=S256"), "{url}");
        assert!(
            url.contains("redirect_uri=http%3A%2F%2F127.0.0.1%3A9%2Fcallback"),
            "{url}"
        );
    }

    #[tokio::test]
    async fn bind_callback_binds_the_named_path_into_the_redirect_uri() {
        // T5's `bind_with` is the intended path (ruling R4 / interface §10): the
        // redirect URI is exactly `http://127.0.0.1:<port>/callback`.
        let server = bind_callback(0).await.expect("an ephemeral bind");
        let uri = server.redirect_uri().to_string();
        assert!(uri.starts_with("http://127.0.0.1:"), "{uri}");
        assert!(uri.ends_with("/callback"), "{uri}");
        let port: u16 = uri
            .trim_start_matches("http://127.0.0.1:")
            .trim_end_matches("/callback")
            .parse()
            .expect("the port parses out of the redirect URI");
        assert!(port > 0);
        drop(server);
    }

    #[tokio::test]
    async fn the_cascade_takes_the_first_free_port_and_falls_back_to_ephemeral() {
        // A LISTENING port is a bind failure, so holding one open is the
        // deterministic "this port is taken" probe (no root, no privileged-port
        // assumption — the CI-leg-portable shape the plan's UNVERIFIED note asks
        // for).
        let taken = std::net::TcpListener::bind("127.0.0.1:0").expect("a listener binds");
        let taken_port = taken.local_addr().expect("an address").port();
        let server = bind_callback_cascade(&[taken_port])
            .await
            .expect("the ephemeral fallback binds after the taken port fails");
        let uri = server.redirect_uri().to_string();
        let port: u16 = uri
            .trim_start_matches("http://127.0.0.1:")
            .trim_end_matches("/callback")
            .parse()
            .expect("a port");
        assert_ne!(port, taken_port, "the taken port was skipped: {uri}");
        drop(server);
        // An EMPTY list is one ephemeral bind.
        let ephemeral = bind_callback_cascade(&[])
            .await
            .expect("an empty list binds ephemerally");
        assert!(ephemeral.redirect_uri().ends_with("/callback"));
        drop(ephemeral);
        // A configured list that includes a FREE port takes it, in order: the
        // first entry is a taken port (skipped), the second is a free one.
        let free_probe = std::net::TcpListener::bind("127.0.0.1:0").expect("binds");
        let free_port = free_probe.local_addr().expect("addr").port();
        drop(free_probe);
        let chosen = bind_callback_cascade(&[taken_port, free_port])
            .await
            .expect("the second port binds");
        assert_eq!(
            chosen.redirect_uri(),
            format!("http://127.0.0.1:{free_port}/callback"),
            "the cascade stops at the FIRST bindable port"
        );
        drop(chosen);
        drop(taken);
    }

    #[tokio::test]
    async fn discover_reads_the_two_endpoints_on_a_headerless_client() {
        let server = wiremock::MockServer::start().await;
        wiremock::Mock::given(wiremock::matchers::path(
            "/.well-known/openid-configuration",
        ))
        .respond_with(
            wiremock::ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "issuer": server.uri(),
                "authorization_endpoint": format!("{}/authorize", server.uri()),
                "token_endpoint": format!("{}/token", server.uri()),
                "jwks_uri": format!("{}/jwks", server.uri()),
                "id_token_signing_alg_values_supported": ["RS256"]
            })),
        )
        .mount(&server)
        .await;
        // A trailing slash on the issuer must not produce a double slash.
        let meta = discover(&format!("{}/", server.uri()), &auth_client().unwrap())
            .await
            .expect("the document is read");
        assert_eq!(
            meta.authorization_endpoint,
            format!("{}/authorize", server.uri())
        );
        assert_eq!(meta.token_endpoint, format!("{}/token", server.uri()));
    }

    #[tokio::test]
    async fn discover_fails_loudly_on_a_non_2xx_or_a_missing_endpoint() {
        // A 404 is an `Err` (unlike the LiteLLM native-client discovery, which
        // degrades on 404): an OIDC row that cannot reach its IdP has no other
        // tier to fall to, and the text is what the UI shows.
        let server = wiremock::MockServer::start().await;
        wiremock::Mock::given(wiremock::matchers::path(
            "/.well-known/openid-configuration",
        ))
        .respond_with(wiremock::ResponseTemplate::new(404))
        .mount(&server)
        .await;
        let err = discover(&server.uri(), &auth_client().unwrap())
            .await
            .unwrap_err();
        assert!(err.contains("404"), "the status is carried: {err}");
        // A document missing the endpoint the flow cannot run on.
        let lean = wiremock::MockServer::start().await;
        wiremock::Mock::given(wiremock::matchers::path(
            "/.well-known/openid-configuration",
        ))
        .respond_with(wiremock::ResponseTemplate::new(200).set_body_json(
            serde_json::json!({"issuer": lean.uri(), "authorization_endpoint": "https://x/auth"}),
        ))
        .mount(&lean)
        .await;
        let err = discover(&lean.uri(), &auth_client().unwrap())
            .await
            .unwrap_err();
        assert!(
            err.contains("token_endpoint"),
            "the missing field is named: {err}"
        );
    }

    #[tokio::test]
    async fn exchange_code_captures_and_validates_the_id_token() {
        let now = crate::agent::mcp::oauth::now_epoch_secs();
        let nonce = "the-nonce-we-sent";
        let server = wiremock::MockServer::start().await;
        let id_token = build_test_id_token(&claims_of(server.uri(), now, nonce));
        let recorded = Arc::new(Mutex::new(String::new()));
        let body = recorded.clone();
        let token_for_mock = id_token.clone();
        wiremock::Mock::given(wiremock::matchers::method("POST"))
            .and(wiremock::matchers::path("/token"))
            .respond_with(move |req: &wiremock::Request| {
                let text = String::from_utf8_lossy(&req.body).to_string();
                *body.lock().unwrap() = text;
                wiremock::ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "access_token": "access-1",
                    "id_token": token_for_mock,
                    "refresh_token": "refresh-1",
                    "expires_in": 3600
                }))
            })
            .mount(&server)
            .await;
        let metadata = OidcMetadata {
            authorization_endpoint: format!("{}/authorize", server.uri()),
            token_endpoint: format!("{}/token", server.uri()),
        };
        // The claims' `iss` is the mock server's URI, so the config must name the
        // same issuer (the issuer check is exactly the equality under test).
        let config = OidcConfig {
            issuer: server.uri(),
            ..config()
        };
        let session = exchange_code(
            &metadata,
            &config,
            "the-code",
            "the-verifier",
            "http://127.0.0.1:9/callback",
            nonce,
            "openid",
            &auth_client().unwrap(),
        )
        .await
        .expect("the exchange validates");
        assert_eq!(session.id_token, id_token, "the id_token is CAPTURED");
        assert_eq!(session.access_token, "access-1");
        assert_eq!(session.refresh_token.as_deref(), Some("refresh-1"));
        assert_eq!(session.sub, "user-42", "the validated sub is carried out");
        assert_eq!(session.token_endpoint, format!("{}/token", server.uri()));
        let millis = session.expires_at.expect("expires_at is set");
        assert!(
            millis > store::now_epoch_millis() + 3_500_000,
            "expires_in SECONDS became MILLISECONDS: {millis}"
        );
        let form = recorded.lock().unwrap().clone();
        for pair in [
            "grant_type=authorization_code",
            "code=the-code",
            "code_verifier=the-verifier",
            "client_id=archimedes",
            "redirect_uri=http%3A%2F%2F127.0.0.1%3A9%2Fcallback",
            "scope=openid",
        ] {
            assert!(form.contains(pair), "the form carries `{pair}`: {form}");
        }
        assert!(
            !form.contains("client_secret"),
            "a public client sends no secret: {form}"
        );
    }

    #[tokio::test]
    async fn exchange_code_refuses_a_token_that_fails_claims_validation() {
        // The claims check is INSIDE the exchange, so a bad `iss` cannot escape
        // into a session (and thence into the store or the proxy).
        let now = crate::agent::mcp::oauth::now_epoch_secs();
        let server = wiremock::MockServer::start().await;
        let mut bad = claims_of(server.uri(), now, "nonce");
        bad["iss"] = serde_json::json!("https://attacker.example");
        let id_token = build_test_id_token(&bad);
        wiremock::Mock::given(wiremock::matchers::path("/token"))
            .respond_with(
                wiremock::ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({"id_token": id_token, "access_token": "a"})),
            )
            .mount(&server)
            .await;
        let metadata = OidcMetadata {
            authorization_endpoint: format!("{}/authorize", server.uri()),
            token_endpoint: format!("{}/token", server.uri()),
        };
        let err = exchange_code(
            &metadata,
            &config(),
            "c",
            "v",
            "http://127.0.0.1:9/callback",
            "nonce",
            "openid",
            &auth_client().unwrap(),
        )
        .await
        .unwrap_err();
        assert!(
            err.contains("issuer"),
            "the rejection names the claim: {err}"
        );
    }

    #[tokio::test]
    async fn exchange_code_refuses_a_response_with_no_id_token() {
        let server = wiremock::MockServer::start().await;
        wiremock::Mock::given(wiremock::matchers::path("/token"))
            .respond_with(
                wiremock::ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({"access_token": "just-an-access-token"})),
            )
            .mount(&server)
            .await;
        let metadata = OidcMetadata {
            authorization_endpoint: format!("{}/authorize", server.uri()),
            token_endpoint: format!("{}/token", server.uri()),
        };
        let err = exchange_code(
            &metadata,
            &config(),
            "c",
            "v",
            "http://127.0.0.1:9/callback",
            "n",
            "openid",
            &auth_client().unwrap(),
        )
        .await
        .unwrap_err();
        assert!(err.contains("id_token"), "the error says why: {err}");
    }

    /// Claims whose `iss` is the mock server's URI (what [`config`] points at in
    /// the network tests).
    fn claims_of(issuer: String, now: i64, nonce: &str) -> serde_json::Value {
        serde_json::json!({
            "iss": issuer,
            "aud": ["archimedes"],
            "sub": "user-42",
            "exp": now + 3600,
            "nonce": nonce,
        })
    }

    use std::sync::{Arc, Mutex};
}

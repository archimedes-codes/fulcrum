//! LiteLLM's native CLI-auth contract (PKCE DCR, issue #126): discovery at
//! `GET {base}/.well-known/litellm-cli-auth` (contract_version 1, S256,
//! same-origin pinning), RFC 7591 public-client registration, the browser
//! authorization leg (PKCE S256 + RFC 8707 `resource`), and the code
//! exchange at `token_endpoint` — plus the fallback-chain orchestrator that
//! degrades to `cli_sso` (device flow) when discovery or DCR is unavailable.
//!
//! Header hygiene: every request this module makes uses a FRESH, headerless
//! `reqwest::Client` built with `redirect(Policy::none())` — provider/gateway
//! headers never attach to an auth-server request, and a 307/308 must not
//! replay the code + verifier to wherever `Location` points (the official
//! native-client contract states both; the reference CLI passes
//! `allow_redirects=False`).
//!
//! The four PKCE/DCR reuse decisions (plan §8), recorded here because they are
//! load-bearing:
//! 1. `base64url` / `pkce` / `url_encode` are REIMPLEMENTED here (copied, ~20
//!    lines) because `mcp::oauth`'s are module-private and that file is not in
//!    this task's owned set. Consequence owned: the two copies can drift; both
//!    are frozen algorithms (RFC 7636 / RFC 3986) and each carries a
//!    keep-in-sync comment naming the sibling.
//! 2. The DCR body is DUPLICATED (`register_public_client`) rather than
//!    parameterizing `mcp::oauth::register_client`. The ONE behavioral delta is
//!    `token_endpoint_auth_method: "none"` (a PUBLIC client) where MCP hardcodes
//!    `client_secret_basic` — that constant is load-bearing for MCP and stays
//!    untouched.
//! 3. `mcp::oauth::refresh`'s public-client guard (a public client never
//!    auto-refreshes) is NOT generalized — its OPPOSITE is correct here (ADR
//!    0015 decision record: see `refresh.rs`'s `litellm_cli_pkce` arm).
//! 4. The loopback callback is the SHARED `agent::mcp::callback` server, fixed
//!    in place (constant-time `state`, additive `bind_with`).

use std::path::Path;
use std::time::Duration;

use base64::Engine as _;
use rand::Rng as _;
use serde::{Deserialize, Serialize};
use sha2::Digest as _;
use tokio_util::sync::CancellationToken;

use super::cli_sso;
use super::store::{self, OAuthCredential, ProviderCredential};
use crate::agent::mcp::callback::CallbackServer;

/// The interactive login's wire steps (T8 renders each variant; issue #126).
///
/// Tagged `status` on the wire (camelCase), Serialize-only. Declared HERE (not
/// in `commands/settings.rs`) so the flow that PRODUCES it does not depend on
/// the IPC layer — `commands::settings` RE-EXPORTS this same type as its wire
/// surface, so T8 mirrors ONE type, never a mirror-struct (the deviation is
/// recorded in the commit message).
#[derive(Debug, Clone, PartialEq, Serialize)]
// `rename_all` on an ENUM container renames the VARIANTS only — the struct
// variants' FIELDS need `rename_all_fields` (serde 1.0.100+) or they ship
// snake_case to the renderer. Both are here because T8's contract is camelCase
// for the tag (`devicePending`) AND every field (`userCode`); pinned by
// `provider_login_step_serializes_camelcase_with_a_status_tag`.
#[serde(
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    tag = "status"
)]
pub enum ProviderLoginStep {
    /// A credential is stored for the provider (PKCE success, device-flow
    /// success, or a paste).
    Authenticated,
    /// The legacy device flow: show `userCode` and open `verificationUrl`
    /// (the `{base}/sso/key/generate?source=litellm-cli&key={loginId}` link);
    /// the user completes SSO in the browser, then T8 polls
    /// `provider_login_sso_poll` every 2 s (the cadence lives in
    /// `cli_sso::SSO_POLL_INTERVAL`; T8 drives the timer — plan §4f).
    DevicePending {
        login_id: String,
        poll_secret: String,
        user_code: String,
        verification_url: String,
    },
    /// The device flow needs a team choice: T8 renders `teams` (the verified
    /// `team_details` / `teams` arrays from the poll response) and calls
    /// `provider_login_sso_choose_team`.
    ChooseTeam {
        login_id: String,
        poll_secret: String,
        teams: Vec<TeamDto>,
    },
}

/// One team row for the `ChooseTeam` step (camelCase on the wire).
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TeamDto {
    pub team_id: String,
    pub team_alias: String,
}

/// The `/.well-known/litellm-cli-auth` document (a NEW struct — deliberately
/// NOT `mcp::oauth::ServerMetadata`, which reads a different contract:
/// `/.well-known/oauth-authorization-server`).
///
/// Tolerant of absent OPTIONAL members (`#[serde(default)]`) but strict on the
/// ones the flow needs (`contract_version`, `issuer`, the two endpoints).
///
/// WIRE NAMES ARE SNAKE_CASE (the RFC 8414 / native-client key names the proxy
/// serves: `contract_version`, `authorization_endpoint`,
/// `code_challenge_methods_supported`, …). The plan's §4a sketch paired
/// `rename_all = "camelCase"` with a snake_case key list — the two contradict
/// each other, and camelCase would not parse a real discovery document
/// (deviation recorded in the commit message; the test
/// `the_discovery_document_parses_from_the_official_snake_case_keys` pins the
/// primary-sourced names).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CliAuthMetadata {
    pub contract_version: u32,
    pub issuer: String,
    pub authorization_endpoint: String,
    pub token_endpoint: String,
    #[serde(default)]
    pub registration_endpoint: Option<String>,
    #[serde(default)]
    pub revocation_endpoint: Option<String>,
    /// RFC 8707 — without it the same authorize takes the MCP path and the
    /// minted token is rejected on `/v1/*` (the native-client contract).
    #[serde(default)]
    pub resource: Option<String>,
    #[serde(default)]
    pub response_types_supported: Vec<String>,
    #[serde(default)]
    pub grant_types_supported: Vec<String>,
    #[serde(default)]
    pub code_challenge_methods_supported: Vec<String>,
    #[serde(default)]
    pub token_endpoint_auth_methods_supported: Vec<String>,
    #[serde(default)]
    pub revocation_endpoint_auth_methods_supported: Vec<String>,
}

/// The RFC 8414 §3.3 canonical form of a URL (the identity rule LiteLLM's own
/// CLI applies — `pkce_login.py`'s `_canonical_url`): lowercase scheme +
/// lowercase host, the DEFAULT port for the scheme dropped, and the path with
/// its trailing slash dropped. `None` when the input does not parse or carries
/// no host (so a caller can distinguish "unparseable" from "different").
fn canonical_url(u: &str) -> Option<String> {
    let parsed = url::Url::parse(u.trim()).ok()?;
    let scheme = parsed.scheme().to_ascii_lowercase();
    let host = parsed.host_str()?;
    let default_port = match scheme.as_str() {
        "https" => Some(443),
        "http" => Some(80),
        _ => None,
    };
    // A port spelled out as the scheme default is the SAME origin as no port.
    let port = parsed
        .port()
        .filter(|p| Some(*p) != default_port)
        .map(|p| format!(":{p}"))
        .unwrap_or_default();
    // The trailing slash is dropped (RFC 8414 §3.3 — `…/llm/` and `…/llm` are
    // the same issuer identity).
    let path = parsed.path().trim_end_matches('/');
    Some(format!("{scheme}://{host}{port}{path}"))
}

/// Do two URLs sit on the same ORIGIN (scheme + host + port; the PATH is
/// ignored — an endpoint may live under a path prefix on the same server)?
fn same_origin(a: &str, b: &str) -> bool {
    let (Some(x), Some(y)) = (canonical_url(a), canonical_url(b)) else {
        return false;
    };
    // Compare the origin by truncating the canonical form at the first `/`
    // after the `://` marker (the path boundary).
    fn origin_of(canonical: &str) -> &str {
        let scheme_end = canonical.find("://").map(|i| i + 3).unwrap_or(0);
        match canonical[scheme_end..].find('/') {
            Some(rel) => &canonical[..scheme_end + rel],
            None => canonical,
        }
    }
    origin_of(&x) == origin_of(&y)
}

/// Validate the discovery document per the native-client contract:
/// `contract_version == 1`; `code_challenge_methods_supported` contains
/// `"S256"`; `issuer` matches the address the document was FETCHED from under
/// the RFC 8414 §3.3 identity rule ([`canonical_url`] — scheme, host, port AND
/// path, case- and trailing-slash-insensitive, so a path-prefixed proxy still
/// passes); every endpoint AND `resource` sit on `issuer`'s ORIGIN. `Err`
/// names the FIRST violated check.
///
/// A PURE function (no IO) so the pinning rules are unit-tested row by row.
pub fn validate_metadata(meta: &CliAuthMetadata, fetched_base: &str) -> Result<(), String> {
    if meta.contract_version != 1 {
        return Err(format!(
            "unsupported LiteLLM CLI-auth contract_version {} (this build speaks 1)",
            meta.contract_version
        ));
    }
    if !meta
        .code_challenge_methods_supported
        .iter()
        .any(|m| m == "S256")
    {
        return Err(
            "the discovery document does not advertise the S256 PKCE challenge method".to_string(),
        );
    }
    let issuer = canonical_url(&meta.issuer).ok_or_else(|| {
        format!(
            "the discovery document's issuer is not a valid URL: {:?}",
            meta.issuer
        )
    })?;
    let fetched = canonical_url(fetched_base)
        .ok_or_else(|| format!("the provider base URL is not a valid URL: {fetched_base:?}"))?;
    if issuer != fetched {
        return Err(format!(
            "the discovery document's issuer {:?} does not match the address it was fetched \
             from {fetched_base:?} (RFC 8414 §3.3 identity check)",
            meta.issuer
        ));
    }
    // Every endpoint (and the RFC 8707 resource) must sit on the issuer's
    // origin — a document that points the token exchange at ANOTHER server
    // would ship the code + verifier there.
    let check = |name: &str, url: &str| -> Result<(), String> {
        if same_origin(url, &meta.issuer) {
            Ok(())
        } else {
            Err(format!(
                "the discovery document's `{name}` endpoint {url:?} is not on the issuer's \
                 origin ({:?})",
                meta.issuer
            ))
        }
    };
    check("authorization", &meta.authorization_endpoint)?;
    check("token", &meta.token_endpoint)?;
    if let Some(reg) = &meta.registration_endpoint {
        check("registration", reg)?;
    }
    if let Some(rev) = &meta.revocation_endpoint {
        check("revocation", rev)?;
    }
    if let Some(resource) = &meta.resource {
        if !same_origin(resource, &meta.issuer) {
            return Err(format!(
                "the discovery document's RFC 8707 resource {resource:?} is not on the issuer's \
                 origin ({:?})",
                meta.issuer
            ));
        }
    }
    Ok(())
}

// ── the PKCE / encoding helpers ───────────────────────────────────────────
//
// `//! mirror of `mcp::oauth`'s `base64url` / `pkce` / `url_encode` — keep in
// sync.` Those three are module-private in `agent/mcp/oauth.rs`, which is NOT
// in this task's owned set, so the ~20 lines are copied rather than the MCP
// file widened (plan §8 decision 1; the grounding's "copying is lower-risk"
// verdict). Both are frozen algorithms (RFC 7636 / RFC 3986).

/// Base64url WITHOUT padding (RFC 4648 §5 — the PKCE encoding).
fn base64url(input: &[u8]) -> String {
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;
    URL_SAFE_NO_PAD.encode(input)
}

/// A PKCE pair: a random 32-byte `verifier` (base64url) + its S256 `challenge`
/// (`base64url(SHA-256(verifier_ascii))`).
fn pkce() -> (String, String) {
    let mut verifier_bytes = [0u8; 32];
    rand::rng().fill(&mut verifier_bytes);
    let verifier = base64url(&verifier_bytes);
    let digest = sha2::Sha256::digest(verifier.as_bytes());
    let challenge = base64url(&digest);
    (verifier, challenge)
}

/// A random URL-safe `state` (the CSRF guard), base64url of 16 random bytes.
fn random_state() -> String {
    let mut bytes = [0u8; 16];
    rand::rng().fill(&mut bytes);
    base64url(&bytes)
}

/// Percent-encode a query value (RFC 3986 unreserved set only).
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

/// The registered client (a PUBLIC client — `client_secret` is always `None`).
/// A LOCAL copy of the MCP `Credentials` shape on purpose: importing MCP's type
/// would couple the two flows' storage shapes for a struct this flow never
/// fills the secret of.
#[derive(Debug, Clone, PartialEq)]
struct Credentials {
    client_id: String,
    #[allow(dead_code)] // a public client has no secret; the field documents the delta
    client_secret: Option<String>,
}

/// The RFC 7591 registration body for a PUBLIC client (plan §8 decision 2:
/// DUPLICATED from `mcp::oauth::register_client`, NOT parameterized — that file
/// is not owned and its `client_secret_basic` constant is load-bearing for MCP).
/// The ONE delta is `token_endpoint_auth_method: "none"`.
fn dcr_body(redirect_uri: &str) -> serde_json::Value {
    serde_json::json!({
        "client_name": "archimedes",
        "redirect_uris": [redirect_uri],
        "grant_types": ["authorization_code", "refresh_token"],
        "response_types": ["code"],
        // THE delta from MCP's body: a public client has no secret to
        // authenticate with (RFC 8252 §8.4 — a native app cannot hold one).
        "token_endpoint_auth_method": "none",
    })
}

/// Register a PUBLIC client at the discovery document's
/// `registration_endpoint` (RFC 7591). A 404/405 here means the proxy serves
/// discovery without registration — the caller degrades to the device flow.
async fn register_public_client(
    metadata: &CliAuthMetadata,
    redirect_uri: &str,
    client: &reqwest::Client,
) -> Result<Credentials, String> {
    let reg_endpoint = metadata
        .registration_endpoint
        .as_deref()
        .ok_or("the discovery document advertises no registration endpoint (DCR unsupported)")?;
    let resp = client
        .post(reg_endpoint)
        .json(&dcr_body(redirect_uri))
        .send()
        .await
        .map_err(|e| format!("the registration request failed: {e}"))?;
    let status = resp.status();
    if !status.is_success() {
        // The status CODE is what the orchestrator branches on (404/405 →
        // device flow), so it leads the message.
        return Err(format!("the DCR request failed: HTTP {status}"));
    }
    let v: serde_json::Value = resp
        .json()
        .await
        .map_err(|e| format!("the registration response was not JSON: {e}"))?;
    let client_id = v
        .get("client_id")
        .and_then(serde_json::Value::as_str)
        .ok_or("the registration response carried no `client_id`")?;
    Ok(Credentials {
        client_id: client_id.to_string(),
        // A PUBLIC client by construction: any `client_secret` the server
        // bothers to hand back is DISCARDED, because sending one would contradict
        // the `token_endpoint_auth_method: "none"` we registered with.
        client_secret: None,
    })
}

/// Build the authorization URL exactly as LiteLLM's own CLI does:
/// `response_type`, `client_id`, `redirect_uri`, `state`, `code_challenge`,
/// `code_challenge_method=S256`, and the RFC 8707 `resource` (without
/// `resource` the same authorize takes the MCP path and the minted token is
/// rejected on `/v1/*`). Every value is percent-encoded.
fn authorize_url(
    authorization_endpoint: &str,
    client_id: &str,
    redirect_uri: &str,
    state: &str,
    challenge: &str,
    resource: Option<&str>,
) -> String {
    let mut url = format!(
        "{authorization_endpoint}?response_type=code&client_id={}&redirect_uri={}\
         &state={}&code_challenge={}&code_challenge_method=S256",
        url_encode(client_id),
        url_encode(redirect_uri),
        url_encode(state),
        url_encode(challenge),
    );
    if let Some(resource) = resource {
        url.push_str(&format!("&resource={}", url_encode(resource)));
    }
    url
}

/// The callback wait: 10 MINUTES (the grounding §2.6 finding — a caller-side
/// parameter, so NO constant in `callback.rs` changes).
const CALLBACK_TIMEOUT: Duration = Duration::from_secs(600);

/// A fresh, headerless, redirect-refusing client for auth-server traffic
/// (plan §4a: a 307/308 must never replay the code + verifier to wherever
/// `Location` points, and gateway headers must never attach here).
fn auth_client() -> Result<reqwest::Client, String> {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .map_err(|e| e.to_string())
}

/// `GET {base}/.well-known/litellm-cli-auth` + [`validate_metadata`]. Returns
/// `Ok(None)` for the one case the orchestrator degrades on — a 404 (the proxy
/// predates the contract) — and `Err` for every OTHER failure (bad JSON,
/// `contract_version != 1`, missing S256, an off-origin endpoint): a REAL proxy
/// on the new contract never serves those, so a misconfigured proxy is surfaced,
/// never silently downgraded to the legacy tier.
async fn discover(base: &str, client: &reqwest::Client) -> Result<Option<CliAuthMetadata>, String> {
    let url = format!("{base}/.well-known/litellm-cli-auth");
    let resp = client
        .get(&url)
        .send()
        .await
        .map_err(|e| format!("the discovery request failed: {e}"))?;
    if resp.status().as_u16() == 404 {
        return Ok(None);
    }
    let status = resp.status();
    if !status.is_success() {
        return Err(format!("the discovery request failed: HTTP {status}"));
    }
    let meta: CliAuthMetadata = resp
        .json()
        .await
        .map_err(|e| format!("the discovery document was not valid CLI-auth JSON: {e}"))?;
    validate_metadata(&meta, base).map(|()| Some(meta))
}

/// Exchange the authorization `code` at `token_endpoint` (the official
/// native-client form: form-encoded, `client_id` in the form, NO client secret,
/// `resource` present). Returns the pieces the T1 store needs, with
/// `expires_at` in MILLISECONDS (the store's unit — never seconds; `expires_in`
/// arrives in seconds).
async fn exchange_code(
    meta: &CliAuthMetadata,
    creds: &Credentials,
    code: &str,
    verifier: &str,
    redirect_uri: &str,
    client: &reqwest::Client,
    base_url: &str,
) -> Result<OAuthCredential, String> {
    let mut form: Vec<(&str, String)> = vec![
        ("grant_type", "authorization_code".to_string()),
        ("code", code.to_string()),
        ("redirect_uri", redirect_uri.to_string()),
        ("client_id", creds.client_id.clone()),
        ("code_verifier", verifier.to_string()),
    ];
    if let Some(resource) = &meta.resource {
        form.push(("resource", resource.clone()));
    }
    let resp = client
        .post(&meta.token_endpoint)
        .form(&form)
        .send()
        .await
        .map_err(|e| format!("the token request failed: {e}"))?;
    let status = resp.status();
    if !status.is_success() {
        let text = resp.text().await.unwrap_or_default();
        return Err(format!("the token request failed: HTTP {status} {text}"));
    }
    let v: serde_json::Value = resp
        .json()
        .await
        .map_err(|e| format!("the token response was not JSON: {e}"))?;
    let access_token = v
        .get("access_token")
        .and_then(serde_json::Value::as_str)
        .ok_or("the token response carried no `access_token`")?
        .to_string();
    // `expires_in` is SECONDS on the wire; the store's unit is MILLISECONDS
    // (interface §3). The contract's default lifetime is 86400 s.
    let expires_in = v
        .get("expires_in")
        .and_then(serde_json::Value::as_i64)
        .unwrap_or(86_400);
    Ok(OAuthCredential {
        flow: "litellm_cli_pkce".to_string(),
        access_token,
        // Rotated on EVERY exchange/refresh — the new pair is persisted before
        // use, because a used refresh token is refused with
        // `400 {"error":"invalid_grant"}`.
        refresh_token: v
            .get("refresh_token")
            .and_then(serde_json::Value::as_str)
            .map(str::to_string),
        expires_at: Some(store::now_epoch_millis().saturating_add(expires_in.saturating_mul(1000))),
        base_url: base_url.to_string(),
        client_id: Some(creds.client_id.clone()),
        token_endpoint: Some(meta.token_endpoint.clone()),
        resource: meta.resource.clone(),
    })
}

/// Refresh a `litellm_cli_pkce` credential (the native-client contract's step 7,
/// verified §11f): `POST {cred.token_endpoint}` with the form
/// `grant_type=refresh_token`, `refresh_token`, `client_id`, `resource` — NO
/// client secret (ADR 0015's MCP guard is deliberately NOT applied here; the
/// opposite is correct for this public client, see `refresh.rs`'s arm).
///
/// The response's `refresh_token` REPLACES the stored one (LiteLLM rotates on
/// every refresh and refuses a used token). `expires_in` (seconds) becomes
/// `expires_at` in MILLISECONDS.
///
/// Builds its OWN redirect-none client: the arm receives the Supervisor's shared
/// client (the interface's signature), but a token request must never follow a
/// redirect, so the shared one is not used for the POST.
pub async fn refresh_pkce(
    cred: &OAuthCredential,
    _client: &reqwest::Client,
) -> Result<OAuthCredential, String> {
    let token_endpoint = cred
        .token_endpoint
        .as_deref()
        .ok_or("a litellm_cli_pkce credential with no stored token endpoint")?;
    let refresh_token = cred
        .refresh_token
        .as_deref()
        .ok_or("a litellm_cli_pkce credential with no stored refresh token")?;
    let client_id = cred
        .client_id
        .as_deref()
        .ok_or("a litellm_cli_pkce credential with no stored client id")?;
    let client = auth_client()?;
    let mut form: Vec<(&str, String)> = vec![
        ("grant_type", "refresh_token".to_string()),
        ("refresh_token", refresh_token.to_string()),
        ("client_id", client_id.to_string()),
    ];
    if let Some(resource) = &cred.resource {
        form.push(("resource", resource.clone()));
    }
    let resp = client
        .post(token_endpoint)
        .form(&form)
        .send()
        .await
        .map_err(|e| format!("the token endpoint request failed: {e}"))?;
    let status = resp.status();
    if !status.is_success() {
        // A rejected grant (400 `invalid_grant` — the token was already used or
        // revoked) and a transient 5xx share ONE policy at this level: fail the
        // attempt, keep the stored credential (T1's rule — a failed refresh never
        // discards a still-valid access token).
        return Err(format!("the token endpoint answered {status}"));
    }
    let v: serde_json::Value = resp
        .json()
        .await
        .map_err(|e| format!("the token endpoint response was not JSON: {e}"))?;
    let access_token = v
        .get("access_token")
        .and_then(serde_json::Value::as_str)
        .ok_or("the token response carried no `access_token`")?
        .to_string();
    let expires_at = match v.get("expires_in").and_then(serde_json::Value::as_i64) {
        Some(secs) => Some(store::now_epoch_millis().saturating_add(secs.saturating_mul(1000))),
        // An absent `expires_in` KEEPS the stored expiry rather than inventing
        // one (T1's rule: `None` means the server never advertised a lifetime).
        None => cred.expires_at,
    };
    let refresh_token = v
        .get("refresh_token")
        .and_then(serde_json::Value::as_str)
        .map(str::to_string)
        // A server that does not rotate must not cost us the token we hold.
        .or_else(|| cred.refresh_token.clone());
    Ok(OAuthCredential {
        access_token,
        refresh_token,
        expires_at,
        ..cred.clone()
    })
}

/// The T5 login entry: PKCE DCR first; on a discovery 404 (or a device-flow
/// start 404/405 handled by the caller's paste path) or a DCR 404/405 →
/// `cli_sso::start_device_flow`. An INVALID discovery document is an `Err`,
/// never a silent downgrade (plan §4a step 1).
///
/// - `store_path` is T1's `store::auth_file_path()` in production (ruling R3:
///   `dirs::data_dir()/archimedes/provider-auth.json`, BESIDE `mcp-auth.json` —
///   never `config_dir`), and a scratch path in tests (the injection shape the
///   `mcp::manager` tests use for `mcp-auth.json`).
/// - `open_browser` is the ADR 0051 seam (production = `crate::open::open(url,
///   None)`; tests = a closure that `reqwest::get`s the authorize URL so the mock
///   proxy's 302 lands on the bound callback — MCP's proven pattern, so NO test
///   ever launches a real browser).
///
/// `code_verifier` NEVER leaves the process: it is held in this stack frame,
/// sent only to `token_endpoint` over the mock/real HTTPS client, and never
/// written to the store (only the exchanged token pair is persisted, via the
/// existing atomic 0600 `store::store_credential`).
pub async fn login(
    provider_id: &str,
    base_url: &str,
    store_path: &Path,
    open_browser: &(dyn Fn(&str) + Send + Sync),
    cancel: &CancellationToken,
) -> Result<ProviderLoginStep, String> {
    // The fetched base is the user's `base_url` with any trailing slash trimmed
    // (the discovery path is appended verbatim), and the RFC 8707 / store
    // `base_url` is the same trimmed value.
    let base = base_url.trim_end_matches('/');
    let client = auth_client()?;
    let meta = match discover(base, &client).await? {
        Some(meta) => meta,
        // Discovery 404 → the legacy CLI SSO device flow (the tier the issue
        // names). A device-flow start that 404s/405s is the caller's cue to fall
        // to the pasted-token command (T8 surfaces that choice).
        None => return start_device_step(base, &client).await,
    };
    let server = CallbackServer::bind_with(0, "/callback")
        .await
        .map_err(|e| format!("could not bind the OAuth callback listener: {e}"))?;
    let redirect_uri = server.redirect_uri().to_string();
    let creds = match register_public_client(&meta, &redirect_uri, &client).await {
        Ok(creds) => creds,
        // A proxy that serves discovery WITHOUT registration (the DCR endpoint
        // 404s/405s, or is simply absent) → the device flow, per the dispatch's
        // "degrades correctly when DCR 404s". Any other registration failure is
        // surfaced — a 500 from the registration endpoint is a real problem, not
        // a missing feature.
        Err(e) => {
            if registration_unavailable(&e) {
                return start_device_step(base, &client).await;
            }
            return Err(e);
        }
    };
    let (verifier, challenge) = pkce();
    let state = random_state();
    let url = authorize_url(
        &meta.authorization_endpoint,
        &creds.client_id,
        &redirect_uri,
        &state,
        &challenge,
        meta.resource.as_deref(),
    );
    // Fire-and-forget: a failure to launch the browser is NOT fatal — the URL is
    // reported so the user can open it by hand (MCP's manual-URL precedent).
    open_browser(&url);
    let callback = server
        .wait(&state, CALLBACK_TIMEOUT, cancel)
        .await
        .map_err(|e| format!("{e} (authorization URL: {url})"))?;
    let cred = exchange_code(
        &meta,
        &creds,
        &callback.code,
        &verifier,
        &redirect_uri,
        &client,
        base,
    )
    .await?;
    store::store_credential(store_path, provider_id, ProviderCredential::OAuth(cred))?;
    Ok(ProviderLoginStep::Authenticated)
}

/// Did the registration failure mean "this proxy has no DCR" (fall back), as
/// opposed to "the proxy's DCR endpoint is broken" (surface it)?
fn registration_unavailable(err: &str) -> bool {
    // The two messages this module's own registration path produces for an
    // absent/unsupported endpoint, plus the 404/405 status codes.
    err.contains("no registration endpoint") || err.contains("HTTP 404") || err.contains("HTTP 405")
}

/// The device-flow leg of the fallback chain: start it and hand back the step
/// T8 renders (the user code + the verification link).
async fn start_device_step(
    base: &str,
    client: &reqwest::Client,
) -> Result<ProviderLoginStep, String> {
    let session = cli_sso::start_device_flow(base, client).await?;
    // The verification link T8 opens. A newer proxy that opts into
    // `allow_cli_sso_verification_uri_complete` hands back a PREFILLED link, and
    // the plan's §11 note is that the Rust side simply carries it through the
    // same `verificationUrl` field (the UI-level choice of which link to prefer
    // is T8's; carrying the prefilled one is the strictly more useful default).
    // Otherwise the proxy's own older link
    // `{base}/sso/key/generate?source=litellm-cli&key={login_id}`.
    let verification_url = session
        .verification_uri_complete
        .clone()
        .unwrap_or_else(|| cli_sso::verification_url(base, &session.login_id));
    Ok(ProviderLoginStep::DevicePending {
        login_id: session.login_id,
        poll_secret: session.poll_secret,
        user_code: session.user_code,
        verification_url,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The reference discovery document (the official native-client contract,
    /// docs.litellm.ai/docs/proxy/cli_sso).
    fn reference_meta() -> CliAuthMetadata {
        CliAuthMetadata {
            contract_version: 1,
            issuer: "https://gw.example".to_string(),
            authorization_endpoint: "https://gw.example/authorize".to_string(),
            token_endpoint: "https://gw.example/token".to_string(),
            registration_endpoint: Some("https://gw.example/register".to_string()),
            revocation_endpoint: Some("https://gw.example/revoke".to_string()),
            resource: Some("https://gw.example".to_string()),
            response_types_supported: vec!["code".to_string()],
            grant_types_supported: vec![
                "authorization_code".to_string(),
                "refresh_token".to_string(),
            ],
            code_challenge_methods_supported: vec!["S256".to_string()],
            token_endpoint_auth_methods_supported: vec!["none".to_string()],
            revocation_endpoint_auth_methods_supported: vec!["none".to_string()],
        }
    }

    #[test]
    fn validate_metadata_accepts_the_reference_document() {
        let meta = reference_meta();
        validate_metadata(&meta, "https://gw.example").expect("the reference document is accepted");
        // A trailing slash on the fetched base is the SAME identity (RFC 8414 §3.3).
        validate_metadata(&meta, "https://gw.example/").expect("a trailing slash is the same URL");
    }

    #[test]
    fn the_discovery_document_parses_from_the_official_snake_case_keys() {
        // The primary-sourced key names (docs.litellm.ai/docs/proxy/cli_sso's
        // native-client contract + RFC 8414): SNAKE_CASE. A camelCase rename here
        // would parse a mock we wrote ourselves and 400 against a real proxy —
        // so the fixture is the documented document, verbatim.
        let doc = serde_json::json!({
            "contract_version": 1,
            "issuer": "https://gw.example",
            "authorization_endpoint": "https://gw.example/authorize",
            "token_endpoint": "https://gw.example/token",
            "registration_endpoint": "https://gw.example/register",
            "revocation_endpoint": "https://gw.example/revoke",
            "resource": "https://gw.example",
            "response_types_supported": ["code"],
            "grant_types_supported": ["authorization_code", "refresh_token"],
            "code_challenge_methods_supported": ["S256"],
            "token_endpoint_auth_methods_supported": ["none"],
            "revocation_endpoint_auth_methods_supported": ["none"]
        });
        let meta: CliAuthMetadata =
            serde_json::from_value(doc.clone()).expect("the documented document parses");
        assert_eq!(meta, reference_meta(), "every documented key landed");
        validate_metadata(&meta, "https://gw.example").expect("and it validates");
        // An OPTIONAL member may be absent entirely (an older proxy without
        // revocation): the flow still parses and validates.
        let mut lean = doc.clone();
        let obj = lean.as_object_mut().expect("an object");
        for key in [
            "revocation_endpoint",
            "resource",
            "revocation_endpoint_auth_methods_supported",
        ] {
            obj.remove(key);
        }
        let lean: CliAuthMetadata =
            serde_json::from_value(lean).expect("the absent optionals default");
        assert_eq!(lean.resource, None);
        assert!(lean.revocation_endpoint.is_none());
        validate_metadata(&lean, "https://gw.example").expect("a lean document still validates");
        // …and the strict members are NOT defaulted: a document with no
        // `token_endpoint` is a contract violation, not a missing optional.
        let mut broken = doc;
        broken
            .as_object_mut()
            .expect("an object")
            .remove("token_endpoint");
        assert!(
            serde_json::from_value::<CliAuthMetadata>(broken).is_err(),
            "a document the flow cannot run on fails to parse"
        );
    }

    #[test]
    fn validate_metadata_accepts_a_path_prefixed_issuer() {
        // The RFC 8414 §3.3 identity rule is scheme+host+port AND PATH (not bare
        // origin) — LiteLLM's own `pkce_login.py::_canonical_url` compares that
        // way, so a proxy mounted under a prefix (`https://gw/llm`) passes when
        // the document was fetched from that same prefix.
        let mut meta = reference_meta();
        meta.issuer = "https://gw.example/llm".to_string();
        for endpoint in [&mut meta.authorization_endpoint, &mut meta.token_endpoint] {
            *endpoint = endpoint.replace("https://gw.example", "https://gw.example/llm");
        }
        meta.registration_endpoint = Some("https://gw.example/llm/register".to_string());
        meta.revocation_endpoint = Some("https://gw.example/llm/revoke".to_string());
        validate_metadata(&meta, "https://gw.example/llm/").expect("a prefixed issuer is valid");
        // …and fetching from a DIFFERENT path is the mismatch this check exists
        // to catch.
        assert!(validate_metadata(&meta, "https://gw.example").is_err());
    }

    #[test]
    fn validate_metadata_rejects_contract_version_2() {
        let mut meta = reference_meta();
        meta.contract_version = 2;
        let err = validate_metadata(&meta, "https://gw.example").unwrap_err();
        assert!(
            err.contains("contract_version"),
            "the error names the violated check: {err}"
        );
    }

    #[test]
    fn validate_metadata_rejects_missing_s256() {
        let mut meta = reference_meta();
        meta.code_challenge_methods_supported = vec!["plain".to_string()];
        let err = validate_metadata(&meta, "https://gw.example").unwrap_err();
        assert!(err.contains("S256"), "the error names S256: {err}");
    }

    #[test]
    fn validate_metadata_rejects_an_endpoint_on_another_origin() {
        // The check that stops a hostile document from pointing the code +
        // verifier at someone else's token endpoint.
        for field in ["authorization", "token"] {
            let mut meta = reference_meta();
            match field {
                "authorization" => {
                    meta.authorization_endpoint = "https://evil.example/authorize".to_string()
                }
                _ => meta.token_endpoint = "https://evil.example/token".to_string(),
            }
            let err = validate_metadata(&meta, "https://gw.example").unwrap_err();
            assert!(
                err.contains(field) && err.contains("origin"),
                "the {field} case names the field and the origin rule: {err}"
            );
        }
        // …and the RFC 8707 resource gets the same treatment.
        let mut meta = reference_meta();
        meta.resource = Some("https://evil.example".to_string());
        let err = validate_metadata(&meta, "https://gw.example").unwrap_err();
        assert!(
            err.contains("resource"),
            "the resource case names it: {err}"
        );
    }

    #[test]
    fn validate_metadata_rejects_an_issuer_that_is_not_the_fetched_base() {
        // The RFC 8414 §3.3 identity check: a document served BY
        // `https://gw.example` that claims a different issuer is rejected (the
        // case- and default-port-insensitive spellings are the SAME identity).
        let mut meta = reference_meta();
        meta.issuer = "https://other.example".to_string();
        assert!(validate_metadata(&meta, "https://gw.example").is_err());
        meta.issuer = "HTTPS://GW.example".to_string();
        validate_metadata(&meta, "https://gw.example").expect("case-insensitive host");
        meta.issuer = "https://gw.example:443".to_string();
        validate_metadata(&meta, "https://gw.example").expect("the default port is spelled out");
    }

    #[test]
    fn canonical_url_and_same_origin_agree_on_the_documented_table() {
        // The exact predicate pair the validation is built on.
        assert_eq!(
            canonical_url("HTTPS://GW.example:443/llm/").as_deref(),
            Some("https://gw.example/llm")
        );
        assert_eq!(
            canonical_url("https://gw.example/llm").as_deref(),
            Some("https://gw.example/llm")
        );
        assert_eq!(canonical_url("not a url"), None);
        // Same origin (a path prefix is fine — the same server).
        assert!(same_origin(
            "https://gw.example/token",
            "https://gw.example/llm"
        ));
        // Different port = different origin, even on the same host.
        assert!(!same_origin(
            "https://gw.example/token",
            "https://gw.example:8443"
        ));
        // A non-default port spelled out on both sides IS the same origin.
        assert!(same_origin(
            "https://gw.example:8443/token",
            "https://gw.example:8443"
        ));
        // Different scheme = different origin.
        assert!(!same_origin(
            "http://gw.example/token",
            "https://gw.example"
        ));
    }

    #[test]
    fn pkce_challenge_is_the_s256_of_the_verifier() {
        // RFC 7636 §4.2: challenge = BASE64URL(SHA256(ASCII(verifier))). Pinned
        // against an independently-computed digest of the SAME verifier, not
        // against the module's own helper twice.
        let (verifier, challenge) = pkce();
        let expected = {
            use base64::engine::general_purpose::URL_SAFE_NO_PAD;
            URL_SAFE_NO_PAD.encode(sha2::Sha256::digest(verifier.as_bytes()))
        };
        assert_eq!(challenge, expected, "the challenge is S256(verifier)");
        assert!(
            !challenge.contains('=') && !challenge.contains('+') && !challenge.contains('/'),
            "the encoding is base64URL without padding: {challenge}"
        );
        // The verifier is 32 random bytes → 43 base64url chars (RFC 7636 §4.1
        // allows 43..=128), and two calls differ.
        assert_eq!(verifier.len(), 43, "a 32-byte verifier encodes to 43 chars");
        assert_ne!(verifier, pkce().0, "the verifier is random per call");
    }

    #[test]
    fn register_public_client_body_uses_token_endpoint_auth_method_none() {
        // The ONE delta from MCP's body (which hardcodes `client_secret_basic`
        // for a reason — MCP is a CONFIDENTIAL client). A public client must
        // register with `"none"` or the token endpoint will demand a secret this
        // flow never holds.
        let body = dcr_body("http://127.0.0.1:34123/callback");
        assert_eq!(body["token_endpoint_auth_method"], "none");
        assert_eq!(body["client_name"], "archimedes");
        assert_eq!(body["redirect_uris"][0], "http://127.0.0.1:34123/callback");
        assert_eq!(body["grant_types"][0], "authorization_code");
        assert_eq!(body["grant_types"][1], "refresh_token");
        assert_eq!(body["response_types"][0], "code");
        // No secret is ever REGISTERED for (a public client has none).
        assert!(
            body.get("client_secret").is_none(),
            "the registration body carries no secret"
        );
    }

    #[test]
    fn authorize_url_carries_the_resource_and_the_s256_challenge() {
        // The unit-level pin of the same property the §12 integration test
        // asserts on the wire: without `resource`, LiteLLM's authorize takes the
        // MCP path and the minted token is rejected on `/v1/*`.
        let url = authorize_url(
            "https://gw.example/authorize",
            "client/id",
            "http://127.0.0.1:34123/callback",
            "the state",
            "the+challenge=",
            Some("https://gw.example"),
        );
        assert!(url.starts_with("https://gw.example/authorize?"));
        for expected in [
            "response_type=code",
            "client_id=client%2Fid",
            "redirect_uri=http%3A%2F%2F127.0.0.1%3A34123%2Fcallback",
            "state=the%20state",
            "code_challenge=the%2Bchallenge%3D",
            "code_challenge_method=S256",
            "resource=https%3A%2F%2Fgw.example",
        ] {
            assert!(url.contains(expected), "the URL carries {expected}: {url}");
        }
        // A document without a `resource` omits the parameter entirely (an empty
        // `resource=` would be a malformed request, not an absent one).
        let no_resource = authorize_url(
            "https://gw.example/authorize",
            "c",
            "http://127.0.0.1:1/callback",
            "s",
            "ch",
            None,
        );
        assert!(
            !no_resource.contains("resource"),
            "an absent resource is absent, not empty: {no_resource}"
        );
    }

    #[test]
    fn the_registration_unavailable_predicate_reads_only_the_missing_feature_codes() {
        // 404/405 and "no registration endpoint" degrade; a 500 does NOT (a
        // broken DCR endpoint is a real failure the user must see).
        assert!(registration_unavailable(
            "the DCR request failed: HTTP 404 Not Found"
        ));
        assert!(registration_unavailable(
            "the DCR request failed: HTTP 405 Method Not Allowed"
        ));
        assert!(registration_unavailable(
            "the discovery document advertises no registration endpoint (DCR unsupported)"
        ));
        assert!(
            !registration_unavailable("the DCR request failed: HTTP 500 Internal Server Error"),
            "a server error is surfaced, never silently downgraded"
        );
        assert!(
            !registration_unavailable("the registration response carried no `client_id`"),
            "a malformed response is surfaced too"
        );
    }

    #[tokio::test]
    async fn refresh_pkce_requires_the_stored_triple_and_never_invents_one() {
        // The arm's guard rails: a credential missing any of the three pieces it
        // needs is an `Err` HERE, and `refresh.rs`'s arm turns that into the
        // documented `Ok(clone)` no-op (nothing to refresh is never an error at
        // the dispatcher). No network is touched — the guards run before any
        // request is built (the success path is the wiremock test
        // `refresh_pkce_exchanges_the_refresh_token_and_rotates_it`).
        let base = OAuthCredential {
            flow: "litellm_cli_pkce".to_string(),
            access_token: "a".to_string(),
            refresh_token: Some("r".to_string()),
            expires_at: None,
            base_url: "https://gw.example".to_string(),
            client_id: Some("c".to_string()),
            token_endpoint: Some("https://gw.example/token".to_string()),
            resource: None,
        };
        let client = reqwest::Client::new();
        for (name, needle, cred) in [
            (
                "token_endpoint",
                "token endpoint",
                OAuthCredential {
                    token_endpoint: None,
                    ..base.clone()
                },
            ),
            (
                "refresh_token",
                "refresh token",
                OAuthCredential {
                    refresh_token: None,
                    ..base.clone()
                },
            ),
            (
                "client_id",
                "client id",
                OAuthCredential {
                    client_id: None,
                    ..base.clone()
                },
            ),
        ] {
            let err = refresh_pkce(&cred, &client).await.unwrap_err();
            assert!(
                err.contains(needle),
                "the missing {name} is named in the error: {err}"
            );
        }
    }
}

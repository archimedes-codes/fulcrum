//! The LEGACY LiteLLM CLI SSO device flow + pasted-token fallback (issue
//! #126): `POST {base}/sso/cli/start` → `login_id`/`poll_secret`/`user_code`;
//! polling `GET {base}/sso/cli/poll/{login_id}` with the
//! `x-litellm-cli-poll-secret` header; team selection as a SECOND poll with a
//! `team_id` query parameter (NOT a separate POST — verified against the proxy
//! route and the official `lite` client, §11a–c); and the pasted SSO token with
//! optional virtual-key minting via `POST {base}/key/generate`.
//!
//! Every wire shape below was verified against LiteLLM's own source
//! (`litellm/proxy/management_endpoints/ui_sso.py` for the routes; the official
//! CLI client `litellm/proxy/client/cli/commands/auth.py` and `pkce_login.py`
//! for the consumption side; `litellm/proxy/_types.py` +
//! `key_management_endpoints.py` for `/key/generate`) — cited inline per shape.
//! The legacy endpoints gate behind `EXPERIMENTAL_UI_LOGIN=True`, but the shapes
//! are STABLE and primary-sourced, NOT guessed (interface ruling R6). Where a
//! shape genuinely cannot be confirmed from Rust, the test that pins it says it
//! pins OUR assumption, not the proxy's contract.

use std::path::Path;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use super::store::{self, OAuthCredential, ProviderCredential};

/// The documented poll cadence (the official `lite` client's
/// `_poll_for_ready_data` uses `poll_interval = 2`, verified §11c).
///
/// WHO DRIVES IT: the command surface exposes ONE tick (`poll_once`) and T8
/// drives the timer — a blocking Rust loop cannot hold the user code on screen
/// and the polling at once (plan §4f). This constant is the contract's cadence,
/// pinned here so the number is not invented by the UI.
pub const SSO_POLL_INTERVAL: Duration = Duration::from_secs(2);

/// The header the poll endpoint reads (`x_litellm_cli_poll_secret` in the proxy
/// route, verified §11b). A wrong/missing secret is a `403`.
pub const POLL_SECRET_HEADER: &str = "x-litellm-cli-poll-secret";

/// `litellm.constants.LITELLM_CLI_SOURCE_IDENTIFIER` — the `source` the
/// verification link carries.
pub const CLI_SOURCE: &str = "litellm-cli";

/// The client the command layer builds for the device-flow / paste legs: a
/// FRESH, headerless client that REFUSES redirects — the same hygiene rule
/// `litellm_oauth`'s auth client holds (a bearer must never be replayed to
/// wherever a `Location` points, and no gateway/provider header ever attaches to
/// an auth-server request). `pub` so `commands::settings` builds exactly THIS
/// client rather than a lookalike with different redirect behaviour.
pub fn poll_client() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .redirect(reqwest::redirect::Policy::none())
        .build()
        // The only way this fails is an exhausted TLS backend, which every other
        // client construction in this crate already treats as impossible (T1's
        // `refresh` and `mcp::oauth::exchange_code` both `expect` the same call).
        .expect("the reqwest client builds")
}

/// The proxy's `CLI_SSO_SESSION_TTL_SECONDS` — the default poll deadline when
/// the start response omits `expires_in` (verified §11a).
pub const SSO_SESSION_TTL_SECONDS: u64 = 600;

/// `LITELLM_CLI_JWT_EXPIRATION_HOURS` — applied ONLY when the ready JWT carries
/// no parseable `exp` claim (verified §11b: the ready body has NO lifetime
/// field, so the client derives it from the JWT).
pub const LEGACY_TOKEN_DEFAULT_TTL_HOURS: u64 = 24;

/// The proxy's own verification link for a device session (the user opens this
/// to complete SSO). `verification_uri_complete` (present only when the proxy
/// sets `general_settings.allow_cli_sso_verification_uri_complete`, §11a) is
/// carried through on the [`DeviceSession`] for T8 to prefer; this builder is
/// the older-proxy fallback the issue text names.
pub fn verification_url(base_url: &str, login_id: &str) -> String {
    format!(
        "{}/sso/key/generate?source={}&key={}",
        base_url.trim_end_matches('/'),
        CLI_SOURCE,
        login_id
    )
}

/// `POST {base}/sso/cli/start` → the device handle.
///
/// The request body is never read by the proxy (the route takes only `Request`,
/// verified §11a) — the reference clients POST with no body at all, so neither
/// do we.
///
/// WIRE NAMES ARE SNAKE_CASE, deliberately: the proxy is Python
/// (`ui_sso.py::cli_start_key_generate` returns a plain dict keyed
/// `login_id`/`poll_secret`/`user_code`/`expires_in`/`verification_uri_complete`,
/// §11a) and FastAPI does not camelCase a bare dict. The plan's §4b sketch said
/// `rename_all = "camelCase"`, which would have failed to parse the REAL
/// response — deviation recorded in the commit message. The camelCase rule
/// applies to OUR OWN IPC DTOs (`ProviderLoginStep`/`TeamDto`), never to a
/// struct that mirrors someone else's payload.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DeviceSession {
    /// `"cli-" + token_urlsafe(24)` on the proxy.
    pub login_id: String,
    /// `token_urlsafe(32)` — the bearer for the poll endpoint (it dies with the
    /// session; the UI holding it is the same trust level as an MCP server entry
    /// holding its headers).
    pub poll_secret: String,
    /// The 4+4 grid code the user types/prefills (`ABCDEFGHJKLMNPQRSTUVWXYZ23456789`).
    pub user_code: String,
    /// The poll deadline in SECONDS (600 on the proxy); `None` → assume
    /// [`SSO_SESSION_TTL_SECONDS`].
    #[serde(default)]
    pub expires_in: Option<u64>,
    /// Present ONLY on a proxy with `allow_cli_sso_verification_uri_complete`.
    #[serde(default)]
    pub verification_uri_complete: Option<String>,
}

/// Start the legacy device flow. A 404/405 here means the proxy has no CLI SSO
/// at all (or `EXPERIMENTAL_UI_LOGIN` is off) — the message carries the status
/// so the caller/T8 can fall to the pasted-token path.
pub async fn start_device_flow(
    base_url: &str,
    client: &reqwest::Client,
) -> Result<DeviceSession, String> {
    let base = base_url.trim_end_matches('/');
    let resp = client
        .post(format!("{base}/sso/cli/start"))
        .send()
        .await
        .map_err(|e| format!("the CLI SSO start request failed: {e}"))?;
    let status = resp.status();
    if !status.is_success() {
        return Err(format!("the CLI SSO start request failed: HTTP {status}"));
    }
    let session: DeviceSession = resp
        .json()
        .await
        .map_err(|e| format!("the CLI SSO start response was not valid JSON: {e}"))?;
    Ok(session)
}

/// One team row from the poll response's `team_details` (preferred) or `teams`
/// (a bare id list on older proxies — the alias is then the id, which the UI
/// renders as-is).
#[derive(Debug, Clone, PartialEq)]
pub struct Team {
    pub team_id: String,
    pub team_alias: String,
}

/// The outcome of ONE poll tick.
#[derive(Debug, Clone, PartialEq)]
pub enum PollOutcome {
    /// `{"status":"pending"}` — the browser leg is not done; wait
    /// [`SSO_POLL_INTERVAL`] and tick again.
    Pending,
    /// `{"status":"ready","requires_team_selection":true,…}` — SSO is done but
    /// the user has several teams, and the response carries NO `key` (verified
    /// §11b). The caller re-polls with the chosen `team_id`.
    NeedsTeam(Vec<Team>),
    /// The final credential. `expires_at_ms` is derived (see
    /// [`expires_at_from_jwt`]) — the ready body itself carries NO lifetime
    /// field, so this is OUR derivation, pinned by the named test.
    Ready {
        token: String,
        user_id: Option<String>,
        team_id: Option<String>,
        expires_at_ms: Option<i64>,
    },
}

/// One poll tick: `GET {base}/sso/cli/poll/{login_id}` with the
/// [`POLL_SECRET_HEADER`] header. `team_id: Option<&str>` — a SECOND poll
/// carries `?team_id=<chosen>` (a URL-encoded query param on the SAME endpoint);
/// that IS the team-selection round-trip (verified §11b/c — there is no separate
/// team-choice POST for a desktop client).
///
/// Status semantics (verified against `ui_sso.py::cli_poll_key`): `403` = wrong
/// poll secret, `400` = the session is gone/expired (the flow must restart).
pub async fn poll_once(
    base_url: &str,
    session: &DeviceSession,
    team_id: Option<&str>,
    client: &reqwest::Client,
) -> Result<PollOutcome, String> {
    let base = base_url.trim_end_matches('/');
    let mut url = format!("{base}/sso/cli/poll/{}", url_encode(&session.login_id));
    if let Some(team_id) = team_id {
        url.push_str(&format!("?team_id={}", url_encode(team_id)));
    }
    let resp = client
        .get(&url)
        .header(POLL_SECRET_HEADER, &session.poll_secret)
        .send()
        .await
        .map_err(|e| format!("the CLI SSO poll request failed: {e}"))?;
    let status = resp.status();
    if status.as_u16() == 400 {
        // The session is gone (expired or never existed): the ONLY recovery is a
        // fresh start, and the message says so — this is TERMINAL, never a tick
        // the caller should retry (the bounded-polling rule).
        return Err(
            "the CLI SSO session has expired or does not exist (HTTP 400) — restart the login"
                .to_string(),
        );
    }
    if status.as_u16() == 403 {
        return Err(
            "the CLI SSO poll secret was rejected (HTTP 403) — restart the login".to_string(),
        );
    }
    if !status.is_success() {
        return Err(format!("the CLI SSO poll request failed: HTTP {status}"));
    }
    let body: serde_json::Value = resp
        .json()
        .await
        .map_err(|e| format!("the CLI SSO poll response was not JSON: {e}"))?;
    parse_poll_body(&body)
}

/// Interpret a poll body (a PURE function so every verified shape is unit-tested
/// without a server). Lenient by design: unknown fields (`attribution_metadata`,
/// a future claim map) are dropped, not fatal.
fn parse_poll_body(body: &serde_json::Value) -> Result<PollOutcome, String> {
    match body.get("status").and_then(serde_json::Value::as_str) {
        Some("pending") => Ok(PollOutcome::Pending),
        Some("ready") => {
            // The TWO verified ready shapes (§11b): the team-pick ASK carries
            // `requires_team_selection: true` and NO `key`; the final credential
            // carries `key`.
            let requires_team = body
                .get("requires_team_selection")
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(false);
            let key = body.get("key").and_then(serde_json::Value::as_str);
            if requires_team || key.is_none() {
                return Ok(PollOutcome::NeedsTeam(parse_teams(body)));
            }
            let token = key.expect("checked above").to_string();
            // The lifetime is derived BEFORE the token moves into the outcome.
            let expires_at_ms = expires_at_from_jwt(&token, store::now_epoch_millis());
            Ok(PollOutcome::Ready {
                token,
                user_id: field_str(body, "user_id"),
                team_id: field_str(body, "team_id"),
                expires_at_ms,
            })
        }
        // An unrecognised status is a CONTRACT change, not a tick: surfacing it
        // beats treating a proxy upgrade as "still pending" forever (the bounded
        // polling rule — a poll never loops on an answer it cannot understand).
        other => Err(format!(
            "the CLI SSO poll returned an unrecognised status {other:?} (this build speaks \
             `pending`/`ready`)"
        )),
    }
}

/// The team list, preferring `team_details` (`[{team_id, team_alias}]`) and
/// falling back to `teams` (a bare id array on older proxies).
fn parse_teams(body: &serde_json::Value) -> Vec<Team> {
    if let Some(details) = body
        .get("team_details")
        .and_then(serde_json::Value::as_array)
    {
        let teams: Vec<Team> = details
            .iter()
            .filter_map(|t| {
                let team_id = t.get("team_id").and_then(serde_json::Value::as_str)?;
                Some(Team {
                    team_id: team_id.to_string(),
                    team_alias: t
                        .get("team_alias")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or(team_id)
                        .to_string(),
                })
            })
            .collect();
        if !teams.is_empty() {
            return teams;
        }
    }
    body.get("teams")
        .and_then(serde_json::Value::as_array)
        .map(|teams| {
            teams
                .iter()
                .filter_map(|t| match t {
                    serde_json::Value::String(id) => Some(Team {
                        team_id: id.clone(),
                        team_alias: id.clone(),
                    }),
                    other => {
                        let team_id = other.get("team_id").and_then(serde_json::Value::as_str)?;
                        Some(Team {
                            team_id: team_id.to_string(),
                            team_alias: other
                                .get("team_alias")
                                .and_then(serde_json::Value::as_str)
                                .unwrap_or(team_id)
                                .to_string(),
                        })
                    }
                })
                .collect()
        })
        .unwrap_or_default()
}

fn field_str(body: &serde_json::Value, key: &str) -> Option<String> {
    body.get(key)
        .and_then(serde_json::Value::as_str)
        .map(str::to_string)
}

/// The device-flow token's lifetime, derived (the verified order, §11b):
/// 1. the JWT's `exp` claim, when the token parses as an unsigned-decoded JWT
///    (the client MUST NOT verify the signature — it is HS256 on the proxy's
///    clock, and pi decodes blind for exactly this reason);
/// 2. else [`LEGACY_TOKEN_DEFAULT_TTL_HOURS`] (24 h) from `now_ms` —
///    `LITELLM_CLI_JWT_EXPIRATION_HOURS`' documented default.
///
/// This is OUR assumption about the JWT (the one item §11 keeps honest); the
/// named test `a_device_flow_expiry_is_derived_from_the_jwt_exp_claim_or_the_24h_fallback`
/// pins it, not the proxy's contract.
pub fn expires_at_from_jwt(token: &str, now_ms: i64) -> Option<i64> {
    if let Some(exp_secs) = jwt_exp_claim(token) {
        // `exp` is epoch SECONDS; the store's unit is MILLISECONDS.
        return Some(exp_secs.saturating_mul(1000));
    }
    Some(now_ms.saturating_add((LEGACY_TOKEN_DEFAULT_TTL_HOURS as i64).saturating_mul(3_600_000)))
}

/// Decode the `exp` claim from a JWT WITHOUT verifying its signature (three
/// base64url segments; the payload is the middle one). `None` when the token is
/// not a JWT or carries no numeric `exp` — the caller then applies the default.
fn jwt_exp_claim(token: &str) -> Option<i64> {
    let payload = token.split('.').nth(1)?;
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;
    use base64::Engine as _;
    // Tolerate a padded segment by stripping the padding before decoding.
    let payload = payload.trim_end_matches('=');
    let bytes = URL_SAFE_NO_PAD.decode(payload).ok()?;
    let claims: serde_json::Value = serde_json::from_slice(&bytes).ok()?;
    claims.get("exp").and_then(serde_json::Value::as_i64)
}

/// The interactive team choice: ONE re-poll with `?team_id=<chosen>` (the
/// verified wire — §11b/c: the choice travels as a query parameter on the poll
/// endpoint, nothing else).
pub async fn choose_team(
    base_url: &str,
    session: &DeviceSession,
    team_id: &str,
    client: &reqwest::Client,
) -> Result<PollOutcome, String> {
    poll_once(base_url, session, Some(team_id), client).await
}

/// Persist a ready device-flow credential (`flow = "litellm_cli_sso"`).
pub fn store_device_token(
    store_path: &Path,
    provider_id: &str,
    base_url: &str,
    outcome: &PollOutcome,
) -> Result<OAuthCredential, String> {
    let PollOutcome::Ready {
        token,
        expires_at_ms,
        ..
    } = outcome
    else {
        return Err("the device flow is not ready — nothing to store".to_string());
    };
    let cred = OAuthCredential {
        flow: "litellm_cli_sso".to_string(),
        access_token: token.clone(),
        // The device-flow JWT has NO refresh token (verified §11b: the ready
        // body carries only `key`/`user_id`/`team_id`) — `refresh.rs`'s arm is a
        // deliberate no-op because of it.
        refresh_token: None,
        expires_at: *expires_at_ms,
        base_url: base_url.trim_end_matches('/').to_string(),
        client_id: None,
        token_endpoint: None,
        resource: None,
    };
    store::store_credential(
        store_path,
        provider_id,
        ProviderCredential::OAuth(cred.clone()),
    )?;
    Ok(cred)
}

/// Pasted-token storage: the token is stored as-is with `flow = "pasted_token"`.
/// When `mint` is true, FIRST `POST {base}/key/generate` with
/// `Authorization: Bearer <token>` and an EMPTY `{}` body (the verified mint
/// wire, §11d — the endpoint is `user_api_key_auth`-gated, so the pasted SSO
/// token IS the bearer; without it the mint 401s) and use the response's `key`
/// (and `expires` when present).
///
/// ANY mint failure (404/401/network/unparseable) DEGRADES to the pasted token:
/// a virtual-key generation failure must never fail the paste (pi's "using SSO
/// token directly" fallback).
pub async fn store_pasted(
    base_url: &str,
    provider_id: &str,
    token: &str,
    mint: bool,
    store_path: &Path,
    client: &reqwest::Client,
) -> Result<OAuthCredential, String> {
    let base = base_url.trim_end_matches('/');
    let mut access_token = token.to_string();
    let mut expires_at = None;
    if mint {
        match mint_virtual_key(base, token, client).await {
            Ok(minted) => {
                access_token = minted.0;
                expires_at = minted.1;
            }
            Err(e) => {
                // The degradation is LOUD in the log but silent on the wire —
                // the paste succeeds with the token the user handed us.
                crate::agent::debuglog::log(&format!(
                    "provider-auth: the virtual-key mint failed ({e}) — storing the pasted token \
                     directly"
                ));
            }
        }
    }
    let cred = OAuthCredential {
        flow: "pasted_token".to_string(),
        access_token,
        // A pasted token / a minted virtual key is a STATIC bearer: no refresh
        // token exists (`GenerateKeyResponse` carries none, §11d) — `refresh.rs`'s
        // arm is a deliberate, tested no-op.
        refresh_token: None,
        expires_at,
        base_url: base.to_string(),
        client_id: None,
        token_endpoint: None,
        resource: None,
    };
    store::store_credential(
        store_path,
        provider_id,
        ProviderCredential::OAuth(cred.clone()),
    )?;
    Ok(cred)
}

/// `POST {base}/key/generate` with the pasted token as the bearer and an EMPTY
/// `{}` body → `(key, expires_at_ms)`. A key with no `expires` is long-lived →
/// `None` (never an invented lifetime — T1's `None` = never expires rule).
async fn mint_virtual_key(
    base: &str,
    token: &str,
    client: &reqwest::Client,
) -> Result<(String, Option<i64>), String> {
    let resp = client
        .post(format!("{base}/key/generate"))
        .bearer_auth(token)
        .json(&serde_json::json!({}))
        .send()
        .await
        .map_err(|e| format!("the key/generate request failed: {e}"))?;
    let status = resp.status();
    if !status.is_success() {
        return Err(format!("the key/generate request failed: HTTP {status}"));
    }
    let body: serde_json::Value = resp
        .json()
        .await
        .map_err(|e| format!("the key/generate response was not JSON: {e}"))?;
    let key = body
        .get("key")
        .and_then(serde_json::Value::as_str)
        .ok_or("the key/generate response carried no `key`")?
        .to_string();
    // `expires` is an ISO-8601 datetime on the wire (`GenerateKeyResponse`); a
    // mint that did not advertise one is long-lived → `None`. Parsing is
    // best-effort: an unparseable stamp is "no known lifetime", NOT a failed
    // mint (the token we already hold still works).
    let expires_at = body
        .get("expires")
        .and_then(serde_json::Value::as_str)
        .and_then(parse_iso8601_millis);
    Ok((key, expires_at))
}

/// A minimal ISO-8601 → epoch-millis parser for the mint's `expires` field
/// (`2030-01-02T03:04:05(.123)(Z|±hh:mm)`). No new dependency (`chrono` is not a
/// dependency of this crate). Days-from-civil per Howard Hinnant's algorithm.
fn parse_iso8601_millis(s: &str) -> Option<i64> {
    let s = s.trim();
    let (rest, offset_secs) = if let Some(rest) = s.strip_suffix('Z') {
        (rest, 0)
    } else if let Some(idx) = s.rfind(['+', '-']).filter(|i| *i >= 10) {
        // `rfind` over the WHOLE string, filtered to a position past the date
        // (`YYYY-MM-DD` is 10 chars) so a negative-year or short input cannot
        // panic the slice.
        let (rest, off) = s.split_at(idx);
        let off = off.trim_start_matches(['Z']);
        let mut sign = 1;
        let mut digits = off;
        if let Some(d) = digits.strip_prefix('-') {
            sign = -1;
            digits = d;
        } else if let Some(d) = digits.strip_prefix('+') {
            digits = d;
        }
        // `±hh:mm` or `±hh`
        let (hh, mm) = match digits.split_once(':') {
            Some((h, m)) => (h.parse::<i64>().ok()?, m.parse::<i64>().ok()?),
            None if digits.len() == 2 => (digits.parse::<i64>().ok()?, 0),
            None => return None,
        };
        (rest, sign * (hh * 3600 + mm * 60))
    } else {
        // No zone designator: the proxy emits UTC (its datetimes are tz-aware),
        // so treat a naive stamp as UTC.
        (s, 0)
    };
    // `rest` is `YYYY-MM-DDTHH:MM:SS(.frac)`.
    let (date, time) = rest.split_once(['T', 't'])?;
    let mut date_parts = date.split('-');
    let year: i64 = date_parts.next()?.parse().ok()?;
    let month: i64 = date_parts.next()?.parse().ok()?;
    let day: i64 = date_parts.next()?.parse().ok()?;
    let time = time.split('.').next()?;
    let mut time_parts = time.split(':');
    let hour: i64 = time_parts.next()?.parse().ok()?;
    let min: i64 = time_parts.next()?.parse().ok()?;
    let sec: i64 = time_parts.next().unwrap_or("0").parse().ok()?;
    let days = days_from_civil(year, month, day);
    let secs = days.saturating_mul(86_400) + hour * 3600 + min * 60 + sec - offset_secs;
    Some(secs.saturating_mul(1000))
}

/// Days since 1970-01-01 for a proleptic-Gregorian date (Howard Hinnant's
/// `days_from_civil`, the public-domain version).
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400; // [0, 399]
    let mp = (m + 9) % 12; // Mar = 0
    let doy = (153 * mp + 2) / 5 + d - 1; // [0, 365]
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy; // [0, 146096]
    era * 146_097 + doe - 719_468
}

/// Percent-encode a path/query segment (RFC 3986 unreserved set only). A local
/// copy for the same reason `litellm_oauth` carries one (plan §8 decision 1):
/// `mcp::oauth`'s is module-private and that file is not in this task's set.
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

#[cfg(test)]
mod tests {
    use super::*;

    /// Base64url WITHOUT padding (the JWT segment encoding) — the test-side
    /// encoder (the production path only ever DEcodes).
    fn b64url(input: &[u8]) -> String {
        use base64::engine::general_purpose::URL_SAFE_NO_PAD;
        use base64::Engine as _;
        URL_SAFE_NO_PAD.encode(input)
    }

    fn session() -> DeviceSession {
        DeviceSession {
            login_id: "cli-abc123".to_string(),
            poll_secret: "poll-secret".to_string(),
            user_code: "ABCD-EFGH".to_string(),
            expires_in: Some(600),
            verification_uri_complete: None,
        }
    }

    #[test]
    fn the_device_session_round_trips_the_proxy_wire_shape() {
        // The start response's field names are the PROXY's snake_case (the
        // Python dict keys, §11a — see the struct doc: the plan's camelCase
        // sketch was wrong and would not parse a real response). Pinned so a
        // rename in this struct cannot silently break the real payload.
        let s = session();
        let json = serde_json::to_value(&s).expect("serializes");
        for key in [
            "login_id",
            "poll_secret",
            "user_code",
            "expires_in",
            "verification_uri_complete",
        ] {
            assert!(
                json.get(key).is_some(),
                "the start payload carries {key}: {json}"
            );
        }
        let back: DeviceSession = serde_json::from_value(json).expect("deserializes");
        assert_eq!(back, s, "the shape round-trips");
        // Lenient in the direction the older proxy needs: no `expires_in` /
        // `verification_uri_complete` at all still parses (both are optional).
        let lean: DeviceSession = serde_json::from_value(serde_json::json!({
            "login_id": "cli-x",
            "poll_secret": "p",
            "user_code": "ABCD-EFGH"
        }))
        .expect("a minimal start payload parses");
        assert_eq!(lean.login_id, "cli-x");
        assert_eq!(
            lean.expires_in, None,
            "absent → the caller's default applies"
        );
    }

    #[test]
    fn verification_url_carries_the_cli_source_and_the_login_id() {
        // The proxy's own link shape (§11a). The trailing slash on the base is
        // trimmed so the URL never doubles it.
        assert_eq!(
            verification_url("https://gw.example/", "cli-abc"),
            "https://gw.example/sso/key/generate?source=litellm-cli&key=cli-abc"
        );
    }

    #[test]
    fn the_poll_body_shapes_parse_as_verified() {
        // The pending tick.
        assert_eq!(
            parse_poll_body(&serde_json::json!({"status": "pending"})).unwrap(),
            PollOutcome::Pending
        );
        // The team-pick ASK: `requires_team_selection` + NO `key` (§11b).
        let ask = serde_json::json!({
            "status": "ready",
            "requires_team_selection": true,
            "user_id": "u-1",
            "teams": ["team-a", "team-b"],
            "team_details": [
                {"team_id": "team-a", "team_alias": "Alpha"},
                {"team_id": "team-b", "team_alias": "Beta"}
            ]
        });
        match parse_poll_body(&ask).unwrap() {
            PollOutcome::NeedsTeam(teams) => {
                assert_eq!(teams.len(), 2, "both teams survive");
                assert_eq!(teams[0].team_alias, "Alpha", "team_details wins over teams");
            }
            other => panic!("the team-pick ask must be NeedsTeam, got {other:?}"),
        }
        // The final credential: `key` + `user_id` + `team_id`, and an UNKNOWN
        // extra field (`attribution_metadata`) is dropped, not fatal.
        let done = serde_json::json!({
            "status": "ready",
            "key": "jwt.token.here",
            "user_id": "u-1",
            "team_id": "team-a",
            "teams": ["team-a"],
            "attribution_metadata": {"sub": "someone"}
        });
        match parse_poll_body(&done).unwrap() {
            PollOutcome::Ready {
                token,
                user_id,
                team_id,
                ..
            } => {
                assert_eq!(token, "jwt.token.here");
                assert_eq!(user_id.as_deref(), Some("u-1"));
                assert_eq!(team_id.as_deref(), Some("team-a"));
            }
            other => panic!("the final shape must be Ready, got {other:?}"),
        }
    }

    #[test]
    fn a_ready_body_with_no_key_asks_for_a_team_even_without_the_flag() {
        // Lenient in the ONE direction the contract allows: a `ready` with no
        // `key` cannot be stored, so it is treated as the team ask (older proxies
        // omit `requires_team_selection` when the user has exactly one team and
        // the proxy still expects a `?team_id=` re-poll).
        let ask = serde_json::json!({"status": "ready", "teams": ["only-team"]});
        match parse_poll_body(&ask).unwrap() {
            PollOutcome::NeedsTeam(teams) => {
                assert_eq!(teams.len(), 1);
                // The bare `teams` fallback uses the id as the alias.
                assert_eq!(teams[0].team_alias, "only-team");
            }
            other => panic!("expected NeedsTeam, got {other:?}"),
        }
    }

    #[test]
    fn an_unrecognised_poll_status_is_an_error_never_an_endless_pending() {
        // The bounded-polling rule: a poll must not loop forever on an answer it
        // cannot understand.
        let err = parse_poll_body(&serde_json::json!({"status": "denied"})).unwrap_err();
        assert!(
            err.contains("denied"),
            "the unknown status is named so a denied device code is terminal: {err}"
        );
        let err = parse_poll_body(&serde_json::json!({})).unwrap_err();
        assert!(
            err.contains("status"),
            "the error names the missing field: {err}"
        );
    }

    #[test]
    fn a_device_flow_expiry_is_derived_from_the_jwt_exp_claim_or_the_24h_fallback() {
        // PINS OUR ASSUMPTION (§11, kept honest): the ready body carries no
        // lifetime, so we decode the JWT's `exp` WITHOUT verifying the signature
        // (pi does the same) and fall back to
        // `LITELLM_CLI_JWT_EXPIRATION_HOURS`/24h. This test pins OUR choice, not
        // the proxy's contract.
        let now_ms = 1_700_000_000_000i64;
        // A payload {"exp": 1800000000} base64url-encoded, with a fake signature.
        let payload = b64url(br#"{"sub":"u","exp":1800000000}"#);
        let jwt = format!("eyJhbGciOiJIUzI1NiJ9.{payload}.sig");
        assert_eq!(
            expires_at_from_jwt(&jwt, now_ms),
            Some(1_800_000_000_000),
            "the exp claim (epoch SECONDS) becomes millis"
        );
        // No parseable exp → now + 24 h.
        let fallback = expires_at_from_jwt("not-a-jwt", now_ms).unwrap();
        assert_eq!(
            fallback,
            now_ms + (LEGACY_TOKEN_DEFAULT_TTL_HOURS as i64) * 3_600_000,
            "the documented default lifetime is applied from issue time"
        );
        // A JWT whose payload has NO exp also falls back (the claim is optional).
        let no_exp = b64url(br#"{"sub":"u"}"#);
        assert_eq!(
            expires_at_from_jwt(&format!("a.{no_exp}.b"), now_ms),
            Some(fallback)
        );
    }

    #[test]
    fn the_documented_constants_match_the_primary_sources() {
        // The named-constant discipline (R6): these double as the diff point if
        // a proxy diff ever lands.
        assert_eq!(SSO_POLL_INTERVAL, Duration::from_secs(2));
        assert_eq!(POLL_SECRET_HEADER, "x-litellm-cli-poll-secret");
        assert_eq!(CLI_SOURCE, "litellm-cli");
        assert_eq!(SSO_SESSION_TTL_SECONDS, 600);
        assert_eq!(LEGACY_TOKEN_DEFAULT_TTL_HOURS, 24);
    }

    #[test]
    fn the_mint_expiry_stamp_parses_to_epoch_millis() {
        // `GenerateKeyResponse.expires` is an ISO-8601 datetime (§11d). Pinned
        // against hand-computed epochs, not against a library we do not depend on.
        assert_eq!(
            parse_iso8601_millis("2030-01-01T00:00:00Z"),
            Some(1_893_456_000_000)
        );
        // With fractional seconds and an explicit offset.
        assert_eq!(
            parse_iso8601_millis("2030-01-01T05:30:00.500+05:30"),
            Some(1_893_456_000_000),
            "the offset is normalized to UTC"
        );
        assert_eq!(
            parse_iso8601_millis("1970-01-01T00:00:00Z"),
            Some(0),
            "the epoch itself"
        );
        // A leap day (2024-02-29) — the civil-date algorithm's edge case.
        assert_eq!(
            parse_iso8601_millis("2024-03-01T00:00:00Z"),
            Some(1_709_251_200_000),
            "the day after 2024-02-29"
        );
        assert_eq!(parse_iso8601_millis("tomorrow"), None);
        assert_eq!(days_from_civil(1970, 1, 1), 0);
        assert_eq!(days_from_civil(1970, 1, 2), 1);
        assert_eq!(days_from_civil(1969, 12, 31), -1);
    }

    #[test]
    fn url_encode_escapes_everything_outside_the_unreserved_set() {
        assert_eq!(url_encode("cli-abc123"), "cli-abc123");
        assert_eq!(url_encode("a b/c?d=e"), "a%20b%2Fc%3Fd%3De");
    }
}

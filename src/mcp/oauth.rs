//! OAuth 2.1 for MCP HTTP servers (ADR 0018) — the `authenticate` flow:
//! discovery (RFC 8414 / RFC 9728), DCR, `authorization_code` + PKCE (a
//! local callback, `callback.rs`), `client_credentials`, refresh (the ADR
//! 0015 guard), and the `~/.local/share/archimedes/mcp-auth.json` storage
//! (0600).
//!
//! The `authenticate` orchestrator takes an `open_browser` seam (a
//! `&dyn Fn(&str)`) so the tests can SIMULATE the browser (a reqwest
//! request to the authorization URL — which the mock server 302-redirects
//! to the callback) instead of launching a real browser.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use rand::Rng;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::Digest;
use tokio_util::sync::CancellationToken;

use super::callback::CallbackServer;
use super::types::HttpDef;

/// OAuth server metadata (RFC 8414 — the `/.well-known/…` JSON).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ServerMetadata {
    pub authorization_endpoint: String,
    pub token_endpoint: String,
    pub registration_endpoint: Option<String>,
}

/// Client credentials (from DCR or a pre-registered config `clientId` /
/// `clientSecret`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Credentials {
    pub client_id: String,
    pub client_secret: Option<String>,
}

/// A stored OAuth token (one entry in `mcp-auth.json`, keyed by the server
/// name).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StoredAuth {
    pub client_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_secret: Option<String>,
    pub token: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub refresh_token: Option<String>,
    /// Unix epoch seconds (the token's expiry; `None` = unknown / no expiry).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<i64>,
    pub auth_server_url: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scope: Option<String>,
}

/// The `mcp-auth.json` path (`~/.local/data_dir/archimedes/mcp-auth.json` —
/// the `dirs` data dir the rest of the desktop uses, joined with a bare
/// `archimedes` (NOT the bundle identifier)).
pub fn auth_file_path() -> PathBuf {
    dirs::data_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join("archimedes")
        .join("mcp-auth.json")
}

/// A base64url encode (the `base64` crate's URL-safe engine, padding
/// stripped — the PKCE `verifier` / `challenge` encoding).
fn base64url(input: &[u8]) -> String {
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;
    use base64::Engine;
    URL_SAFE_NO_PAD.encode(input)
}

/// A PKCE pair: a random 32-byte `verifier` (base64url) + its S256
/// `challenge` (`base64url(SHA-256(verifier_ascii))`).
fn pkce() -> (String, String) {
    let mut verifier_bytes = [0u8; 32];
    rand::rng().fill(&mut verifier_bytes);
    let verifier = base64url(&verifier_bytes);
    let digest = sha2::Sha256::digest(verifier.as_bytes());
    let challenge = base64url(&digest);
    (verifier, challenge)
}

/// Discover the auth server's metadata (RFC 8414:
/// `{base}/.well-known/oauth-authorization-server`, fallback
/// `{base}/.well-known/openid-configuration`).
pub async fn discover(
    auth_server_url: &str,
    client: &reqwest::Client,
) -> Result<ServerMetadata, String> {
    let base = auth_server_url.trim_end_matches('/');
    for path in [
        "/.well-known/oauth-authorization-server",
        "/.well-known/openid-configuration",
    ] {
        let url = format!("{base}{path}");
        let Ok(resp) = client.get(&url).send().await else {
            continue;
        };
        if !resp.status().is_success() {
            continue;
        }
        let Ok(v) = resp.json::<Value>().await else {
            continue;
        };
        let Some(authorization_endpoint) = v.get("authorization_endpoint").and_then(Value::as_str)
        else {
            continue;
        };
        let Some(token_endpoint) = v.get("token_endpoint").and_then(Value::as_str) else {
            continue;
        };
        return Ok(ServerMetadata {
            authorization_endpoint: authorization_endpoint.to_string(),
            token_endpoint: token_endpoint.to_string(),
            registration_endpoint: v
                .get("registration_endpoint")
                .and_then(Value::as_str)
                .map(str::to_string),
        });
    }
    Err("no OAuth metadata found (tried the well-known endpoints)".to_string())
}

/// Extract the `resource_metadata="url"` value from a `WWW-Authenticate`
/// header (RFC 9728 — the 401's pointer to the Protected Resource
/// Metadata).
pub fn from_www_authenticate(header: &str) -> Option<String> {
    let marker = "resource_metadata=\"";
    let start = header.find(marker)? + marker.len();
    let rest = &header[start..];
    let end = rest.find('"')?;
    Some(rest[..end].to_string())
}

/// Fetch a Protected Resource Metadata document (RFC 9728) and return its
/// `authorization_servers` (a list of auth-server URLs).
pub async fn resource_metadata(url: &str, client: &reqwest::Client) -> Result<Vec<String>, String> {
    let resp = client.get(url).send().await.map_err(|e| e.to_string())?;
    if !resp.status().is_success() {
        return Err(format!(
            "the resource metadata request failed: HTTP {}",
            resp.status()
        ));
    }
    let v: Value = resp.json().await.map_err(|e| e.to_string())?;
    let servers = v
        .get("authorization_servers")
        .and_then(Value::as_array)
        .ok_or("no authorization_servers in the resource metadata")?;
    Ok(servers
        .iter()
        .filter_map(Value::as_str)
        .map(str::to_string)
        .collect())
}

/// Register a client (DCR — `POST {registration_endpoint}`). A pre-registered
/// `clientId` (+ `clientSecret`) skips DCR (the `authenticate` orchestrator
/// decides).
pub async fn register_client(
    metadata: &ServerMetadata,
    client_name: Option<&str>,
    redirect_uri: &str,
    client: &reqwest::Client,
) -> Result<Credentials, String> {
    let Some(reg_endpoint) = &metadata.registration_endpoint else {
        return Err("the auth server has no registration endpoint (DCR unsupported)".to_string());
    };
    let body = json!({
        "client_name": client_name,
        "redirect_uris": [redirect_uri],
        "grant_types": ["authorization_code", "refresh_token"],
        "response_types": ["code"],
        "token_endpoint_auth_method": "client_secret_basic",
    });
    let resp = client
        .post(reg_endpoint)
        .json(&body)
        .send()
        .await
        .map_err(|e| e.to_string())?;
    if !resp.status().is_success() {
        return Err(format!("the DCR request failed: HTTP {}", resp.status()));
    }
    let v: Value = resp.json().await.map_err(|e| e.to_string())?;
    let client_id = v
        .get("client_id")
        .and_then(Value::as_str)
        .ok_or("no client_id in the DCR response")?;
    Ok(Credentials {
        client_id: client_id.to_string(),
        client_secret: v
            .get("client_secret")
            .and_then(Value::as_str)
            .map(str::to_string),
    })
}

/// A token request's result (the `access_token` / `refresh_token` /
/// `expires_in` → `expires_at`).
fn parse_token_response(
    v: &Value,
    creds: &Credentials,
    auth_server_url: &str,
    scope: Option<&str>,
) -> Result<StoredAuth, String> {
    let token = v
        .get("access_token")
        .and_then(Value::as_str)
        .ok_or("no access_token in the token response")?
        .to_string();
    let now_secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    Ok(StoredAuth {
        client_id: creds.client_id.clone(),
        client_secret: creds.client_secret.clone(),
        token,
        refresh_token: v
            .get("refresh_token")
            .and_then(Value::as_str)
            .map(str::to_string),
        expires_at: v
            .get("expires_in")
            .and_then(Value::as_i64)
            .map(|e| now_secs.saturating_add(e as u64) as i64),
        auth_server_url: auth_server_url.to_string(),
        scope: scope.map(str::to_string),
    })
}

/// Exchange an authorization `code` for a token (the `authorization_code`
/// grant; `code_verifier` = the PKCE verifier; client auth `basic` when a
/// secret exists, `post` otherwise).
pub async fn exchange_code(
    metadata: &ServerMetadata,
    creds: &Credentials,
    code: &str,
    verifier: &str,
    redirect_uri: &str,
    scope: Option<&str>,
    auth_server_url: &str,
) -> Result<StoredAuth, String> {
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()
        .expect("the reqwest client builds");
    let mut form: Vec<(&str, String)> = vec![
        ("grant_type", "authorization_code".to_string()),
        ("code", code.to_string()),
        ("code_verifier", verifier.to_string()),
        ("redirect_uri", redirect_uri.to_string()),
    ];
    if let Some(s) = scope {
        form.push(("scope", s.to_string()));
    }
    let has_secret = creds.client_secret.is_some();
    if !has_secret {
        // `client_secret_post`: a `client_id` form field.
        form.push(("client_id", creds.client_id.clone()));
    }
    let mut req = client.post(&metadata.token_endpoint).form(&form);
    if let Some(secret) = &creds.client_secret {
        // `client_secret_basic`: a Basic auth header.
        req = req.basic_auth(&creds.client_id, Some(secret.as_str()));
    }
    let resp = req.send().await.map_err(|e| e.to_string())?;
    let status = resp.status();
    if !status.is_success() {
        let text = resp.text().await.unwrap_or_default();
        return Err(format!("the token request failed: HTTP {status} {text}"));
    }
    let v: Value = resp.json().await.map_err(|e| e.to_string())?;
    parse_token_response(&v, creds, auth_server_url, scope)
}

/// The `client_credentials` grant (a straight `token_endpoint` request —
/// non-interactive).
pub async fn client_credentials_grant(
    metadata: &ServerMetadata,
    creds: &Credentials,
    scope: Option<&str>,
    auth_server_url: &str,
    client: &reqwest::Client,
) -> Result<StoredAuth, String> {
    let mut form: Vec<(&str, String)> = vec![("grant_type", "client_credentials".to_string())];
    if let Some(s) = scope {
        form.push(("scope", s.to_string()));
    }
    let has_secret = creds.client_secret.is_some();
    if !has_secret {
        form.push(("client_id", creds.client_id.clone()));
    }
    let mut req = client.post(&metadata.token_endpoint).form(&form);
    if let Some(secret) = &creds.client_secret {
        req = req.basic_auth(&creds.client_id, Some(secret.as_str()));
    }
    let resp = req.send().await.map_err(|e| e.to_string())?;
    let status = resp.status();
    if !status.is_success() {
        let text = resp.text().await.unwrap_or_default();
        return Err(format!(
            "the client_credentials request failed: HTTP {status} {text}"
        ));
    }
    let v: Value = resp.json().await.map_err(|e| e.to_string())?;
    parse_token_response(&v, creds, auth_server_url, scope)
}

/// Refresh a token (the `refresh_token` grant). The ADR 0015 guard: a
/// pre-registered PUBLIC client (a `client_id`, NO `client_secret`) is
/// NEVER auto-refreshed (the auth server rejects the refresh grant with
/// `invalid_client`) → `None` (the caller degrades to a fresh interactive
/// flow).
pub async fn refresh(stored: &StoredAuth, client: &reqwest::Client) -> Option<StoredAuth> {
    let Some(refresh_token) = &stored.refresh_token else {
        return None;
    };
    // The ADR 0015 guard: a public client (no secret) is never auto-refreshed.
    stored.client_secret.as_ref()?;
    // Re-discover the token endpoint (the stored `auth_server_url` is the base).
    let metadata = discover(&stored.auth_server_url, client).await.ok()?;
    let mut form: Vec<(&str, String)> = vec![
        ("grant_type", "refresh_token".to_string()),
        ("refresh_token", refresh_token.clone()),
    ];
    if let Some(s) = &stored.scope {
        form.push(("scope", s.clone()));
    }
    let secret = stored.client_secret.as_deref().unwrap_or("");
    let req = client
        .post(&metadata.token_endpoint)
        .form(&form)
        .basic_auth(&stored.client_id, Some(secret));
    let Ok(resp) = req.send().await else {
        return None;
    };
    if !resp.status().is_success() {
        return None;
    }
    let Ok(v) = resp.json::<Value>().await else {
        return None;
    };
    let creds = Credentials {
        client_id: stored.client_id.clone(),
        client_secret: stored.client_secret.clone(),
    };
    parse_token_response(&v, &creds, &stored.auth_server_url, stored.scope.as_deref()).ok()
}

/// The `authenticate` orchestrator:
/// - `client_credentials` grant → a straight `token_endpoint` request.
/// - `authorization_code` (the default) → DCR (unless a pre-registered
///   `clientId`) → PKCE → open the browser (the `open_browser` seam) →
///   wait for the local callback (5 min, raced against `cancel`) →
///   `exchange_code` → store.
///
/// `auth_server_url`: the auth server's base URL (the config's
/// `authorizationServerUrl`, OR a 401's `WWW-Authenticate`
/// `resource_metadata` → RFC 9728 → `authorization_servers[0]` — the caller
/// resolves it and passes it; `None` → an error).
/// `open_browser`: a seam (production = the platform opener, fire-and-forget;
/// a test = a reqwest request to the authorization URL — the mock server
/// 302-redirects it to the callback). `Send + Sync` (the `authenticate`
/// future is spawned — it must be `Send`).
pub async fn authenticate(
    def: &HttpDef,
    auth_server_url: Option<&str>,
    cancel: &CancellationToken,
    open_browser: &(dyn Fn(&str) + Send + Sync),
) -> Result<StoredAuth, String> {
    let Some(auth_server_url) = auth_server_url else {
        return Err(
            "no authorization server URL (set `authorizationServerUrl` in the config, \
             or the server's 401 must carry a `WWW-Authenticate` `resource_metadata`)"
                .to_string(),
        );
    };
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()
        .expect("the reqwest client builds");
    let metadata = discover(auth_server_url, &client).await?;
    let AuthFields {
        grant_type,
        client_id,
        client_secret,
        scope,
        client_name,
    } = extract_auth_fields(def);

    match grant_type.as_deref() {
        Some("client_credentials") => {
            let Some(client_id) = client_id else {
                return Err(
                    "a `client_credentials` server needs a `clientId` in the config".to_string(),
                );
            };
            let creds = Credentials {
                client_id,
                client_secret,
            };
            let stored = client_credentials_grant(
                &metadata,
                &creds,
                scope.as_deref(),
                auth_server_url,
                &client,
            )
            .await?;
            Ok(stored)
        }
        // `authorization_code` (the default) / an unknown grant.
        _ => {
            // Bind the local callback (a bind failure → an error carrying the
            // manual-URL guidance is the manager's problem; here, an `Err`).
            let callback = CallbackServer::bind()
                .await
                .map_err(|e| format!("callback bind failed: {e}"))?;
            let redirect_uri = callback.redirect_uri().to_string();
            // The client: a pre-registered `clientId` (+ `clientSecret`) skips
            // DCR; otherwise DCR.
            let creds = match client_id {
                Some(client_id) => Credentials {
                    client_id,
                    client_secret,
                },
                None => {
                    register_client(&metadata, client_name.as_deref(), &redirect_uri, &client)
                        .await?
                }
            };
            let (verifier, challenge) = pkce();
            let state = {
                let mut state_bytes = [0u8; 16];
                rand::rng().fill(&mut state_bytes);
                base64url(&state_bytes)
            };
            // The authorization URL.
            let mut auth_url = format!(
                "{}?response_type=code&client_id={}&redirect_uri={}&code_challenge={}&code_challenge_method=S256&state={}",
                metadata.authorization_endpoint,
                url_encode(&creds.client_id),
                url_encode(&redirect_uri),
                url_encode(&challenge),
                url_encode(&state),
            );
            if let Some(s) = &scope {
                auth_url.push_str(&format!("&scope={}", url_encode(s)));
            }
            // Open the browser (the seam — fire-and-forget; a failure is
            // non-fatal, the URL is in the error / the model can relay it).
            open_browser(&auth_url);
            // Wait for the callback (5 min, raced against `cancel`).
            let result = callback
                .wait(&state, std::time::Duration::from_secs(300), cancel)
                .await
                .map_err(|e| {
                    format!("the OAuth callback failed: {e} (authorize manually at: {auth_url})")
                })?;
            let stored = exchange_code(
                &metadata,
                &creds,
                &result.code,
                &verifier,
                &redirect_uri,
                scope.as_deref(),
                auth_server_url,
            )
            .await?;
            Ok(stored)
        }
    }
}

/// The `authenticate`-relevant `AuthSpec::OAuth` fields (a small extracted
/// struct — the `def.auth` is an `AuthSpec`, and we only need the OAuth
/// variant's fields).
struct AuthFields {
    grant_type: Option<String>,
    client_id: Option<String>,
    client_secret: Option<String>,
    scope: Option<String>,
    client_name: Option<String>,
}

fn extract_auth_fields(def: &HttpDef) -> AuthFields {
    // Only an `OAuth` def has these fields; a non-OAuth def → all `None`
    // (the caller should only call `authenticate` for an OAuth def).
    match &def.auth {
        super::types::AuthSpec::OAuth {
            grant_type,
            client_id,
            client_secret,
            scope,
            client_name,
            ..
        } => AuthFields {
            grant_type: grant_type.clone(),
            client_id: client_id.clone(),
            client_secret: client_secret.clone(),
            scope: scope.clone(),
            client_name: client_name.clone(),
        },
        _ => AuthFields {
            grant_type: None,
            client_id: None,
            client_secret: None,
            scope: None,
            client_name: None,
        },
    }
}

/// A minimal percent-encode (the authorization URL's query values).
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

/// Load the stored credentials (the `mcp-auth.json` — a `server → StoredAuth`
/// map). A missing / unparseable file → an empty map (best-effort, never an
/// error).
pub fn load_credentials(path: &Path) -> BTreeMap<String, StoredAuth> {
    let Ok(contents) = std::fs::read_to_string(path) else {
        return BTreeMap::new();
    };
    let Ok(map) = serde_json::from_str::<BTreeMap<String, StoredAuth>>(&contents) else {
        return BTreeMap::new();
    };
    map
}

/// Save the stored credentials (the `mcp-auth.json` — `0600` on unix; a
/// missing parent dir is created; a write failure is `Err`).
pub fn save_credentials(path: &Path, map: &BTreeMap<String, StoredAuth>) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|e| format!("creating the credentials dir failed: {e}"))?;
    }
    let contents =
        serde_json::to_string_pretty(map).map_err(|e| format!("serializing failed: {e}"))?;
    std::fs::write(path, contents).map_err(|e| format!("writing failed: {e}"))?;
    // `0600` on unix (the file holds a token — not world-readable).
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
            .map_err(|e| format!("chmod 0600 failed: {e}"))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    // ── the pure helpers ─────────────────────────────────────────────

    #[test]
    fn pkce_challenge_is_the_s256_of_the_verifier() {
        let (verifier, challenge) = pkce();
        // The verifier is 43 chars (32 bytes → base64url, no padding).
        assert_eq!(verifier.len(), 43);
        // The challenge is base64url(SHA-256(verifier_ascii)).
        let expected = {
            let digest = sha2::Sha256::digest(verifier.as_bytes());
            base64url(&digest)
        };
        assert_eq!(challenge, expected);
    }

    #[test]
    fn base64url_is_url_safe_and_unpadded() {
        // `foo` → `Zm9v` (no padding).
        assert_eq!(base64url(b"foo"), "Zm9v");
        // A byte that would be `+` / `/` in standard base64 is `-` / `_`.
        let encoded = base64url(b"\xff\xff");
        assert_eq!(encoded, "__8");
        // No `+`, `/`, or `=` (URL-safe + unpadded).
        assert!(!encoded.contains('+'));
        assert!(!encoded.contains('/'));
        assert!(!encoded.contains('='));
    }

    #[test]
    fn from_www_authenticate_extracts_the_resource_metadata() {
        let header = r#"Bearer error="invalid_request_credentials", resource_metadata="https://auth.example/.well-known/oauth-protected-resource""#;
        assert_eq!(
            from_www_authenticate(header),
            Some("https://auth.example/.well-known/oauth-protected-resource".to_string())
        );
    }

    #[test]
    fn from_www_authenticate_none_when_absent() {
        assert_eq!(
            from_www_authenticate(r#"Bearer error="invalid_token""#),
            None
        );
    }

    #[test]
    fn refresh_a_public_client_is_never_auto_refreshed() {
        // A public client (a `client_id`, NO `client_secret`) → `None`
        // (the ADR 0015 guard).
        let stored = StoredAuth {
            client_id: "cid".into(),
            client_secret: None,
            token: "tok".into(),
            refresh_token: Some("rt".into()),
            expires_at: Some(0),
            auth_server_url: "https://auth.example".into(),
            scope: None,
        };
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("runtime");
        assert!(rt
            .block_on(refresh(&stored, &reqwest::Client::new()))
            .is_none());
    }

    #[test]
    fn refresh_without_a_refresh_token_is_none() {
        let stored = StoredAuth {
            client_id: "cid".into(),
            client_secret: Some("sec".into()),
            token: "tok".into(),
            refresh_token: None,
            expires_at: Some(0),
            auth_server_url: "https://auth.example".into(),
            scope: None,
        };
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("runtime");
        assert!(rt
            .block_on(refresh(&stored, &reqwest::Client::new()))
            .is_none());
    }

    // ── the storage ──────────────────────────────────────────────────

    #[test]
    fn storage_round_trip() {
        let dir = tempfile::tempdir().expect("a temp dir");
        let path = dir.path().join("mcp-auth.json");
        let mut map = BTreeMap::new();
        map.insert(
            "server-a".to_string(),
            StoredAuth {
                client_id: "cid-a".into(),
                client_secret: Some("sec-a".into()),
                token: "tok-a".into(),
                refresh_token: Some("rt-a".into()),
                expires_at: Some(123),
                auth_server_url: "https://auth-a.example".into(),
                scope: Some("s1".into()),
            },
        );
        save_credentials(&path, &map).expect("saves");
        let loaded = load_credentials(&path);
        assert_eq!(loaded, map);
    }

    #[cfg(unix)]
    #[test]
    fn storage_is_0600_on_unix() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().expect("a temp dir");
        let path = dir.path().join("mcp-auth.json");
        let mut map = BTreeMap::new();
        map.insert(
            "s".to_string(),
            StoredAuth {
                client_id: "cid".into(),
                client_secret: None,
                token: "tok".into(),
                refresh_token: None,
                expires_at: None,
                auth_server_url: "https://x".into(),
                scope: None,
            },
        );
        save_credentials(&path, &map).expect("saves");
        let mode = std::fs::metadata(&path)
            .expect("metadata")
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o600, "the file is 0600");
    }

    #[test]
    fn load_a_missing_file_is_an_empty_map() {
        let dir = tempfile::tempdir().expect("a temp dir");
        let path = dir.path().join("does-not-exist.json");
        assert!(load_credentials(&path).is_empty());
    }

    // ── the network functions (a mock OAuth server) ──────────────────

    /// A mock OAuth server (a raw tokio HTTP handler serving the metadata
    /// JSON, the DCR endpoint, the token endpoint (recording the
    /// `code` / `verifier` / client-auth it received), + the authorization
    /// endpoint (a 302 redirect to the callback URL with a `code`)).
    ///
    /// Returns the bound port + a `recorded` map (the token request's
    /// fields, for assertions). ASYNC (the tokio `bind` completes before
    /// returning — no bind race).
    async fn mock_oauth_server() -> (
        u16,
        Arc<Mutex<BTreeMap<String, String>>>,
        tokio::task::JoinHandle<()>,
    ) {
        // A `std` bind to pick a free port (a `Result` — a failure is a
        // test error), then a tokio `bind` on that port (AWAITED — completes
        // before returning, so there's no bind race).
        let std_listener = std::net::TcpListener::bind("127.0.0.1:0").expect("binds");
        let port = std_listener
            .local_addr()
            .expect("the listener has an address")
            .port();
        drop(std_listener);
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", port))
            .await
            .expect("the tokio listener binds");
        let recorded = Arc::new(Mutex::new(BTreeMap::new()));
        let recorded_inner = recorded.clone();
        let handle = tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    return;
                };
                let recorded_inner = recorded_inner.clone();
                tokio::spawn(async move {
                    use tokio::io::{AsyncReadExt, AsyncWriteExt};
                    let mut buf = [0u8; 8192];
                    let mut data = Vec::new();
                    while !data.windows(4).any(|w| w == b"\r\n\r\n") {
                        let Ok(n) = stream.read(&mut buf).await else {
                            return;
                        };
                        if n == 0 {
                            return;
                        }
                        data.extend_from_slice(&buf[..n]);
                    }
                    let header_end = data.windows(4).position(|w| w == b"\r\n\r\n").unwrap();
                    let headers = String::from_utf8_lossy(&data[..header_end + 4]).to_string();
                    let request_line = headers.lines().next().unwrap_or_default();
                    let content_length = headers
                        .lines()
                        .find_map(|l| {
                            l.to_lowercase()
                                .strip_prefix("content-length:")
                                .map(|v| v.trim().parse::<usize>().unwrap_or(0))
                        })
                        .unwrap_or(0);
                    let mut body = data[header_end + 4..].to_vec();
                    while body.len() < content_length {
                        let Ok(n) = stream.read(&mut buf).await else {
                            return;
                        };
                        if n == 0 {
                            break;
                        }
                        body.extend_from_slice(&buf[..n]);
                    }
                    let body_str = String::from_utf8_lossy(&body).to_string();
                    // Route by the request path.
                    let path = request_line
                        .split_whitespace()
                        .nth(1)
                        .unwrap_or_default()
                        .to_string();
                    let base = format!("http://127.0.0.1:{port}");
                    let (status, content_type, response_body) = if path.starts_with("/.well-known")
                    {
                        let body = json!({
                            "authorization_endpoint": format!("{base}/authorize"),
                            "token_endpoint": format!("{base}/token"),
                            "registration_endpoint": format!("{base}/register"),
                        })
                        .to_string();
                        ("200 OK", "application/json", body)
                    } else if path.starts_with("/register") {
                        // DCR: record the `redirect_uris` (assert the callback
                        // URL was registered).
                        let v: Value = serde_json::from_str(&body_str).unwrap_or(Value::Null);
                        if let Some(rus) = v["redirect_uris"].as_array() {
                            if let Some(ru) = rus.first().and_then(Value::as_str) {
                                recorded_inner
                                    .lock()
                                    .unwrap()
                                    .insert("registered_redirect".into(), ru.to_string());
                            }
                        }
                        let body = json!({ "client_id": "dcr-cid", "client_secret": "dcr-sec" })
                            .to_string();
                        ("200 OK", "application/json", body)
                    } else if path.starts_with("/authorize") {
                        // The authorization endpoint: a 302 redirect to the
                        // callback URL with a `code` + the `state` (echoed).
                        let query = request_line.split('?').nth(1).unwrap_or_default();
                        let mut redirect_uri = String::new();
                        let mut state = String::new();
                        for pair in query.split('&') {
                            let mut kv = pair.splitn(2, '=');
                            let k = kv.next().unwrap_or_default();
                            let v = kv.next().unwrap_or_default();
                            if k == "redirect_uri" {
                                redirect_uri = url_decode(v);
                            } else if k == "state" {
                                state = url_decode(v);
                            }
                        }
                        let location = format!("{redirect_uri}?code=mock-code&state={state}");
                        (
                            "302 Found",
                            "text/plain",
                            format!("Location: {location}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"),
                        )
                    } else if path.starts_with("/token") {
                        // The token endpoint: record the `code` /
                        // `code_verifier` / `client_id` / the Basic auth.
                        for pair in body_str.split('&') {
                            let mut kv = pair.splitn(2, '=');
                            let k = kv.next().unwrap_or_default();
                            let v = kv.next().unwrap_or_default();
                            recorded_inner
                                .lock()
                                .unwrap()
                                .insert(k.to_string(), url_decode(v));
                        }
                        if let Some(auth) = headers
                            .lines()
                            .find(|l| l.to_lowercase().starts_with("authorization:"))
                        {
                            recorded_inner
                                .lock()
                                .unwrap()
                                .insert("authorization".into(), auth.trim().to_string());
                        }
                        let body = json!({
                            "access_token": "mock-access-token",
                            "refresh_token": "mock-refresh-token",
                            "expires_in": 3600,
                        })
                        .to_string();
                        ("200 OK", "application/json", body)
                    } else {
                        ("404 Not Found", "text/plain", "not found".to_string())
                    };
                    let full = if path.starts_with("/authorize") {
                        // The 302 response already carries the `Location` +
                        // `Content-Length` + `Connection: close` in
                        // `response_body`.
                        format!("HTTP/1.1 {response_body}")
                    } else {
                        format!(
                            "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{response_body}",
                            response_body.len()
                        )
                    };
                    if stream.write_all(full.as_bytes()).await.is_err() {
                        return;
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                });
            }
        });
        (port, recorded, handle)
    }

    fn url_decode(s: &str) -> String {
        let bytes = s.as_bytes();
        let mut out: Vec<u8> = Vec::with_capacity(bytes.len());
        let mut i = 0;
        while i < bytes.len() {
            if bytes[i] == b'%' && i + 2 < bytes.len() {
                let hex = &s[i + 1..i + 3];
                if let Ok(v) = u8::from_str_radix(hex, 16) {
                    out.push(v);
                    i += 3;
                    continue;
                }
            }
            out.push(bytes[i]);
            i += 1;
        }
        String::from_utf8_lossy(&out).to_string()
    }

    /// A `HttpDef` with an `OAuth` auth (for the `authenticate` tests).
    fn oauth_def(grant_type: Option<&str>, client_id: Option<&str>) -> HttpDef {
        use super::super::types::AuthSpec;
        HttpDef {
            url: "http://127.0.0.1:1/mcp".into(),
            headers: Default::default(),
            auth: AuthSpec::OAuth {
                grant_type: grant_type.map(str::to_string),
                client_id: client_id.map(str::to_string),
                client_secret: None,
                scope: None,
                redirect_uri: None,
                client_name: Some("test-client".into()),
                authorization_server_url: None,
            },
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn discover_reads_the_metadata() {
        let (port, _recorded, handle) = mock_oauth_server().await;
        let client = reqwest::Client::new();
        let metadata = discover(&format!("http://127.0.0.1:{port}"), &client)
            .await
            .expect("the metadata is found");
        assert_eq!(
            metadata.authorization_endpoint,
            format!("http://127.0.0.1:{port}/authorize")
        );
        assert_eq!(
            metadata.token_endpoint,
            format!("http://127.0.0.1:{port}/token")
        );
        assert_eq!(
            metadata.registration_endpoint,
            Some(format!("http://127.0.0.1:{port}/register"))
        );
        handle.abort();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn the_full_authorization_code_flow() {
        let (port, recorded, handle) = mock_oauth_server().await;
        let def = oauth_def(Some("authorization_code"), None); // DCR (no pre-registered client).
        let auth_server_url = format!("http://127.0.0.1:{port}");
        // The `open_browser` seam: SIMULATE the browser by directly triggering
        // the callback (parse `redirect_uri` + `state` from the auth URL, send
        // a GET to the callback URL with a `code` — no `/authorize` endpoint,
        // no redirect — a reqwest redirect following is flaky with the mock's
        // `Connection: close`).
        let open_browser: Box<dyn Fn(&str) + Send + Sync> = Box::new(|url: &str| {
            let query = url.split('?').nth(1).unwrap_or_default();
            let mut redirect_uri = String::new();
            let mut state = String::new();
            for pair in query.split('&') {
                let mut kv = pair.splitn(2, '=');
                let k = kv.next().unwrap_or_default();
                let v = kv.next().unwrap_or_default();
                if k == "redirect_uri" {
                    redirect_uri = url_decode(v);
                } else if k == "state" {
                    state = url_decode(v);
                }
            }
            let callback_url = format!("{redirect_uri}?code=mock-code&state={state}");
            tokio::spawn(async move {
                let client = reqwest::Client::new();
                let _ = client.get(callback_url).send().await;
            });
        });
        let stored = authenticate(
            &def,
            Some(&auth_server_url),
            &CancellationToken::new(),
            &open_browser,
        )
        .await
        .expect("the flow completes");
        assert_eq!(stored.token, "mock-access-token");
        assert_eq!(stored.refresh_token.as_deref(), Some("mock-refresh-token"));
        // The token request carried the `code` + the `code_verifier`.
        let recorded = recorded.lock().unwrap();
        assert_eq!(recorded.get("code"), Some(&"mock-code".to_string()));
        assert!(recorded.contains_key("code_verifier"));
        // The DCR registered the callback's redirect URI.
        assert!(recorded.get("registered_redirect").is_some());
        handle.abort();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn the_client_credentials_flow() {
        let (port, recorded, handle) = mock_oauth_server().await;
        let def = oauth_def(Some("client_credentials"), Some("pre-cid"));
        let auth_server_url = format!("http://127.0.0.1:{port}");
        let open_browser: Box<dyn Fn(&str) + Send + Sync> = Box::new(|_url: &str| {});
        let stored = authenticate(
            &def,
            Some(&auth_server_url),
            &CancellationToken::new(),
            &open_browser,
        )
        .await
        .expect("the flow completes");
        assert_eq!(stored.token, "mock-access-token");
        // The token request carried `grant_type=client_credentials`.
        let recorded = recorded.lock().unwrap();
        assert_eq!(
            recorded.get("grant_type"),
            Some(&"client_credentials".to_string())
        );
        handle.abort();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_missing_auth_server_url_is_an_error() {
        let def = oauth_def(Some("authorization_code"), None);
        let r = authenticate(
            &def,
            None, // no auth server URL.
            &CancellationToken::new(),
            &|_url: &str| {},
        )
        .await;
        assert!(r.is_err(), "a missing auth server URL is an error");
    }
}

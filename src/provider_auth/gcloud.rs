//! Google Application Default Credentials (ADC) — the `gcloud_adc` flow (issue #128).
//!
//! Reads the ADC file (NEVER writes one), validates the `authorized_user`
//! shape, exchanges the refresh token natively over HTTP, and caches the minted
//! access token IN MEMORY ONLY for 50 minutes.
//!
//! Two invariants this module exists to hold, both pinned by
//! `src-tauri/tests/provider_gcloud_adc.rs`:
//!
//! 1. **Zero `gcloud` CLI subprocess.** The exchange is a `reqwest` POST; no
//!    child process is ever spawned here (the zero-subprocess census scans this
//!    file's production region for `Command::new(` / `process::Command` /
//!    `tokio::process` text and FAILS — not prints — on a hit).
//! 2. **The minted access token is never written to disk.** This module performs
//!    NO persistence at all: it reads one file, posts one request, and stores
//!    the result in the process-memory cache below. It takes no `store` import
//!    and must never gain one — the provider-auth store is the ONE place the
//!    suite persists credentials, and an ADC access token must not join it
//!    (the never-`store` census + the store-stays-absent runtime assertion).
//!
//! Discovery is READ-ONLY: nothing here creates `~/.config/gcloud`, and no test
//! reads the developer's real ADC file — every fixture injects its own path.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{LazyLock, Mutex};
use std::time::{Duration, Instant};

use serde::Deserialize;

use crate::agent::debuglog;

/// The in-memory cache TTL: 50 minutes (spec Subissue 4 — "in-memory cache
/// valid for 50 minutes"; Google's own access tokens live 60 minutes, so a
/// 50-minute ceiling leaves a 10-minute safety margin without tracking
/// `expires_in` at all).
pub const CACHE_TTL_MS: u64 = 50 * 60 * 1000;

/// Google's token endpoint (fixed per spec — no discovery, no metadata).
pub const TOKEN_ENDPOINT: &str = "https://oauth2.googleapis.com/token";

/// The per-request HTTP timeout for the exchange — the repo's loyal pattern
/// (`mcp::oauth::refresh_and_persist` and the `oauth` flow use 30 s). Applied
/// PER REQUEST rather than on the client, because the caller hands us its
/// SHARED client (the resolve spine builds one per resolve) and reqwest has NO
/// default timeout: an unbounded request would hang a resolve forever.
pub const HTTP_TIMEOUT: Duration = Duration::from_secs(30);

/// The cache size cap. A bounded cache needs no background eviction task, and
/// the realistic working set is ONE entry — every provider sharing one ADC file
/// resolves to one `(client_id, refresh_token)` key. FIFO by insert order
/// (evict the oldest), pinned by `cache_evicts_old_entries`.
const MAX_CACHE_ENTRIES: usize = 4;

/// The env var holding an explicit ADC path (Google's own convention, honoured
/// verbatim — the same variable the Google client libraries read).
pub const ADC_ENV_VAR: &str = "GOOGLE_APPLICATION_CREDENTIALS";

/// The ADC file's location relative to the user's home on Linux/macOS.
#[cfg(not(windows))]
const ADC_RELATIVE: &str = ".config/gcloud/application_default_credentials.json";

/// The ADC file's location relative to `%APPDATA%` on Windows (a DIFFERENT base
/// from `dirs::config_dir()`, which is `%APPDATA%\Roaming` — the spec names
/// `%APPDATA%\gcloud`, so it is read from the environment directly).
#[cfg(windows)]
const ADC_RELATIVE: &str = "gcloud/application_default_credentials.json";

/// A validated `authorized_user` ADC file (the discovery result).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdcCredential {
    pub client_id: String,
    pub client_secret: String,
    pub refresh_token: String,
    /// The file the credential was read from — carried into the diagnostics.
    pub source: PathBuf,
}

/// Where an ADC attempt stopped. `token` turns every variant into `None`
/// ("serve the row's static key") — the enum exists so the DIAGNOSTIC can be
/// specific, never so a caller can branch on failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdcError {
    /// No ADC file at any candidate location (or the named one is unreadable).
    FileNotFound,
    /// The file is not valid JSON, or carries no usable `type` field.
    Unparseable,
    /// `type` is present and is NOT `authorized_user` (the common case being a
    /// `service_account` from a workload/VM identity setup).
    UnsupportedType,
    /// `authorized_user`, but a required field is missing or empty.
    Malformed,
    /// The token exchange failed on the transport, on a non-2xx status, or the
    /// response carried no `access_token`.
    ExchangeFailed,
}

/// ADC-file discovery order: [`ADC_ENV_VAR`] (used verbatim), then
/// `$HOME/.config/gcloud/application_default_credentials.json`
/// (`%APPDATA%\gcloud\…` on Windows). The FIRST existing file wins; `None` when
/// none exists.
///
/// Read-only by construction — a candidate that is not a readable file is
/// simply skipped, never created, never repaired.
pub fn adc_file_path() -> Option<PathBuf> {
    // `var_os` (not `var`): a path is bytes, not text — an `OsString` converts
    // into a `PathBuf` losslessly on every platform this app ships on, so a
    // non-UTF-8 ADC path still resolves instead of silently falling through.
    let named = std::env::var_os(ADC_ENV_VAR).map(PathBuf::from);
    let base = adc_base();
    adc_file_path_from(named.as_deref(), base.as_deref())
}

/// The base [`ADC_RELATIVE`] hangs off: `$HOME` on the Unix-family targets,
/// `%APPDATA%` on Windows.
///
/// Windows reads `%APPDATA%` from the environment rather than taking
/// `dirs::config_dir()`, because that appends `Roaming` while the spec names
/// `%APPDATA%` itself. Resolving the base HERE — outside the seam below — is
/// what keeps the seam pure: it takes a path, so a test can hand in its own base
/// instead of relocating the developer's real `%APPDATA%` (which
/// `dirs::config_dir()` reads all over the suite) or their real home.
fn adc_base() -> Option<PathBuf> {
    #[cfg(windows)]
    {
        std::env::var_os("APPDATA").map(PathBuf::from)
    }
    #[cfg(not(windows))]
    {
        dirs::home_dir()
    }
}

/// The path-injection seam behind [`adc_file_path`] (the repo's hermeticity
/// rule: injection, never relocation). A test hands in its own `env` and `base`
/// and never touches the developer's real ADC file, home, or `%APPDATA%`.
///
/// The two arms of [`adc_base`] differ ONLY in which base the one shared
/// `ADC_RELATIVE` hangs off, so this seam is target-independent: the fallback
/// test below runs identically on Unix (`$HOME/.config/gcloud/…`) and on
/// Windows (`%APPDATA%\gcloud\…`) — which is the point, since the Windows
/// spelling of `ADC_RELATIVE` is otherwise compiled nowhere but there.
fn adc_file_path_from(env: Option<&Path>, base: Option<&Path>) -> Option<PathBuf> {
    let mut candidates: Vec<PathBuf> = Vec::new();
    if let Some(path) = env {
        candidates.push(path.to_path_buf());
    }
    if let Some(base) = base {
        candidates.push(base.join(ADC_RELATIVE));
    }
    // `is_file` (not `exists`): a dangling symlink or a directory named like the
    // ADC file must not become a candidate that then fails to read.
    candidates.into_iter().find(|p| p.is_file())
}

/// The raw ADC document (only the four fields this flow can care about).
#[derive(Deserialize)]
struct RawAdc {
    #[serde(rename = "type")]
    ty: Option<String>,
    #[serde(default)]
    client_id: Option<String>,
    #[serde(default)]
    client_secret: Option<String>,
    #[serde(default)]
    refresh_token: Option<String>,
}

/// Parse + validate one ADC file. `Ok` ONLY for `type: "authorized_user"` with
/// non-empty `client_id`, `client_secret` and `refresh_token`.
///
/// A `service_account` (or any other `type`) is REJECTED, never quietly
/// tolerated: it is the single most likely way a user hits this code with a
/// credential that cannot work, and [`describe_error`] names the fix.
pub fn parse_adc(contents: &str, source: &Path) -> Result<AdcCredential, AdcError> {
    // `parse_adc` is a PURE validator — it does not log. `token_for_paths` is the
    // ONE logging site for this module (a single place to look for "why is my
    // provider serving a static key", and no double-logged rejection).
    let raw: RawAdc = serde_json::from_str(contents).map_err(|_| AdcError::Unparseable)?;
    match raw.ty.as_deref() {
        Some("authorized_user") => {}
        // A present-but-other type is a REJECTION with a named fix; a missing
        // `type` is not an ADC file we understand at all.
        Some(_) => return Err(AdcError::UnsupportedType),
        None => return Err(AdcError::Unparseable),
    }
    let field = |v: Option<String>| v.filter(|s| !s.trim().is_empty());
    let (Some(client_id), Some(client_secret), Some(refresh_token)) = (
        field(raw.client_id),
        field(raw.client_secret),
        field(raw.refresh_token),
    ) else {
        return Err(AdcError::Malformed);
    };
    Ok(AdcCredential {
        client_id,
        client_secret,
        refresh_token,
        source: source.to_path_buf(),
    })
}

/// The ONE diagnostic surface (so the message a user reads lives in exactly one
/// place). Call it with the error AND log it — the ADC path never surfaces an
/// error at the `resolve_bearer` boundary, so the debug log is the only place a
/// user can find out why their ADC row is serving a static key.
pub fn describe_error(err: &AdcError, source: Option<&Path>) -> String {
    let checked =
        "checked $GOOGLE_APPLICATION_CREDENTIALS, ~/.config/gcloud/ and %APPDATA%\\gcloud";
    match err {
        AdcError::FileNotFound => format!(
            "gcloud_adc: no Google ADC file found ({checked}) — serving the provider's static \
             key. Run `gcloud auth application-default login` to create one."
        ),
        AdcError::Unparseable => format!(
            "gcloud_adc: the ADC file{} is not a readable ADC document (no valid JSON `type` \
             field) — serving the provider's static key.",
            source_suffix(source)
        ),
        AdcError::UnsupportedType => format!(
            "gcloud_adc: the ADC file{} has a non-user credential type (e.g. `service_account`); \
             only `authorized_user` credentials are supported. Run `gcloud auth \
             application-default login` to mint user ADC credentials, or use the key-helper path \
             (`!command`) / a static key for a service account.",
            source_suffix(source)
        ),
        AdcError::Malformed => format!(
            "gcloud_adc: the ADC file{} is `authorized_user` but is missing `client_id`, \
             `client_secret` or `refresh_token` — serving the provider's static key.",
            source_suffix(source)
        ),
        AdcError::ExchangeFailed => format!(
            "gcloud_adc: the token exchange with {} failed — serving the previously cached token \
             if one is still fresh, otherwise the provider's static key.",
            TOKEN_ENDPOINT
        ),
    }
}

fn source_suffix(source: Option<&Path>) -> String {
    match source {
        Some(p) => format!(" (`{}`)", p.display()),
        None => String::new(),
    }
}

/// Log a failure through [`describe_error`] (the single logging shape for this
/// module — a `note` from the call site is appended when one is available).
fn log_failure(err: &AdcError, source: Option<&Path>, note: Option<&str>) {
    let mut line = describe_error(err, source);
    if let Some(note) = note {
        line.push_str(&format!(" detail: {note}"));
    }
    debuglog::log(&line);
}

/// The cache key: exactly the pair the TTL binds. Two providers sharing one ADC
/// file therefore share ONE minted token (and one exchange).
#[derive(Debug, Clone, PartialEq, Eq)]
struct CacheKey {
    client_id: String,
    refresh_token: String,
}

/// One cached mint: the token, when it was minted, and the insert sequence that
/// gives FIFO eviction its order.
struct CachedToken {
    access_token: String,
    minted_at: Instant,
    seq: u64,
}

/// Insert-order counter (monotonic; only ever compared to itself).
static INSERT_SEQ: AtomicU64 = AtomicU64::new(0);

/// THE cache: a `Vec` of `(key, mint)` pairs, oldest first. A `Vec` rather than
/// a map because the working set is ≤ [`MAX_CACHE_ENTRIES`] and insertion order
/// IS the eviction order — no separate order list to keep in sync, no clock
/// dependency, no background task. Every access takes the same `Mutex`, so the
/// parallel test suite cannot interleave a lookup with an eviction.
static CACHE: LazyLock<Mutex<Vec<(CacheKey, CachedToken)>>> =
    LazyLock::new(|| Mutex::new(Vec::new()));

fn cache_lock() -> std::sync::MutexGuard<'static, Vec<(CacheKey, CachedToken)>> {
    // Poison-tolerant (the repo's shared convention): a sibling test panicking
    // while holding this lock must not turn this call into an opaque panic.
    CACHE.lock().unwrap_or_else(|p| p.into_inner())
}

/// A fresh (non-stale) cached token for `key`, or `None`.
///
/// A STALE entry is a MISS and is left in place: this flow never serves an
/// expired token, and the stale entry is what a FAILED refetch falls back on
/// (see [`cache_insert`]'s keep-old rule).
fn cache_lookup(key: &CacheKey, ttl: Duration, now: Instant) -> Option<String> {
    let cache = cache_lock();
    cache.iter().find_map(|(k, v)| {
        (k == key && now.duration_since(v.minted_at) < ttl).then(|| v.access_token.clone())
    })
}

/// Insert (or replace) the mint for `key`, evicting the OLDEST entries until the
/// cache fits [`MAX_CACHE_ENTRIES`].
///
/// Replacing an existing key moves it to the NEWEST position (a fresh mint is
/// the freshest thing in the cache, and a provider whose token just got re-minted
/// must not be the next one evicted).
fn cache_insert(key: CacheKey, access_token: String) {
    let mut cache = cache_lock();
    cache.retain(|(k, _)| k != &key);
    let seq = INSERT_SEQ.fetch_add(1, Ordering::Relaxed);
    cache.push((
        key,
        CachedToken {
            access_token,
            minted_at: Instant::now(),
            seq,
        },
    ));
    while cache.len() > MAX_CACHE_ENTRIES {
        // `seq` is monotonic and the Vec is insert-ordered, so the front IS the
        // oldest; sort defensively anyway so eviction stays FIFO if a future
        // edit ever touches entries out of order.
        let oldest = cache
            .iter()
            .enumerate()
            .min_by_key(|(_, (_, v))| v.seq)
            .map(|(i, _)| i)
            .unwrap_or(0);
        cache.remove(oldest);
    }
}

/// (TEST SEAM) The cache contents as `((client_id, refresh_token), token)` in
/// insert order. Production has no reason to enumerate its own cache; the tests
/// need it to prove the keep-old-on-failure rule, which no outward behaviour can
/// show (a stale entry is never SERVED — it is a miss).
#[doc(hidden)]
pub fn cache_snapshot() -> Vec<((String, String), String)> {
    cache_lock()
        .iter()
        .map(|(k, v)| {
            (
                (k.client_id.clone(), k.refresh_token.clone()),
                v.access_token.clone(),
            )
        })
        .collect()
}

/// (TEST SEAM) Empty the cache. The cache is process-global (one mint per
/// `(client_id, refresh_token)` for the whole app), so a test that asserts on
/// mint COUNTS must start from empty; it acquires the same `Mutex` every real
/// access uses, so clearing races nothing.
#[doc(hidden)]
pub fn cache_clear() {
    cache_lock().clear();
}

/// The minted access token, or `None` = "serve the row's static `api_key`".
///
/// `config_dir` is in the signature because the caller (`resolve_bearer`) has it
/// and the interface's arm names it; the ADC file does NOT live in the app's
/// config dir (it lives in the user's home / `%APPDATA%`), so this module has no
/// use for it.
///
/// The sequence (spec Subissue 4):
/// 1. discover + parse the ADC file — the parse is REQUIRED before the cache
///    lookup, because the cache KEY is `(client_id, refresh_token)`;
/// 2. a fresh cache hit for that key is returned without touching the network;
/// 3. a miss (absent OR stale) runs the native exchange;
/// 4. success inserts the mint; failure returns `None` and LEAVES the previous
///    entry alone — a transient outage must not invalidate a still-valid token
///    (the same discipline as the store refresher's keep-serving rule).
pub async fn token(config_dir: &Path, client: &reqwest::Client) -> Option<String> {
    let _ = config_dir;
    // The test seam (see `set_test_adc_override`): production sees `None` and
    // resolves the real discovery + the real endpoint. An installed override
    // REPLACES discovery unless it explicitly asks for it (`AdcFileSource::
    // Discover`), so a test never even STAT-S the ambient candidates and the
    // developer's real ADC file stays unreachable from the suite (the plan's
    // hazard box).
    let override_ = test_adc_override();
    let (adc_file, endpoint) = match &override_ {
        Some(o) => {
            let file = match &o.adc_file {
                AdcFileSource::Explicit(path) => path.clone(),
                AdcFileSource::Discover => adc_file_path(),
            };
            (
                file,
                o.token_endpoint
                    .clone()
                    .unwrap_or_else(|| TOKEN_ENDPOINT.to_string()),
            )
        }
        None => (adc_file_path(), TOKEN_ENDPOINT.to_string()),
    };
    token_for_paths(
        adc_file.as_deref(),
        &endpoint,
        Duration::from_millis(CACHE_TTL_MS),
        client,
    )
    .await
}

/// The path- and endpoint-taking core (the tested seam — the
/// `resolve_bearer_at` / `with_auth_file` precedent): a test hands in a scratch
/// ADC file and a `wiremock` URL, so NOTHING here can reach the developer's real
/// ADC file or Google's real endpoint. `ttl` is a parameter for the same reason:
/// the TTL tests use milliseconds, never a 50-minute wait.
pub async fn token_for_paths(
    adc_file: Option<&Path>,
    token_endpoint: &str,
    ttl: Duration,
    client: &reqwest::Client,
) -> Option<String> {
    let Some(path) = adc_file else {
        log_failure(&AdcError::FileNotFound, None, None);
        return None;
    };
    let contents = match std::fs::read_to_string(path) {
        Ok(contents) => contents,
        Err(e) => {
            log_failure(
                &AdcError::FileNotFound,
                Some(path),
                Some(&format!("the ADC file could not be read: {e}")),
            );
            return None;
        }
    };
    let cred = match parse_adc(&contents, path) {
        Ok(cred) => cred,
        Err(e) => {
            // The `service_account` rejection lands here, with the diagnostic
            // naming `gcloud auth application-default login`.
            log_failure(&e, Some(path), None);
            return None;
        }
    };
    let key = CacheKey {
        client_id: cred.client_id.clone(),
        refresh_token: cred.refresh_token.clone(),
    };
    if let Some(hit) = cache_lookup(&key, ttl, Instant::now()) {
        return Some(hit);
    }
    match exchange(&cred, client, token_endpoint).await {
        Ok(access_token) => {
            cache_insert(key, access_token.clone());
            Some(access_token)
        }
        Err(e) => {
            // Keep whatever entry is already in the cache (the lookup above
            // treated it as a miss because it is stale — it is still the better
            // answer than nothing for the STORE refresher's purposes, and
            // dropping it would cost a mint on every later failure).
            log_failure(&e, Some(path), None);
            None
        }
    }
}

/// The native token exchange: `POST {endpoint}` with
/// `grant_type=refresh_token` + the ADC file's three fields. NO `gcloud`
/// subprocess, no metadata server, no CLI parsing.
///
/// `Err(ExchangeFailed)` on a transport error, a non-2xx status, a non-JSON
/// body, or a body with no `access_token` — the response body is deliberately
/// NOT parsed further, because the caller cannot act on any Google error detail
/// beyond "it failed" (the ADC file is the user's, the endpoint is Google's).
async fn exchange(
    cred: &AdcCredential,
    client: &reqwest::Client,
    endpoint: &str,
) -> Result<String, AdcError> {
    let form = [
        ("grant_type", "refresh_token"),
        ("client_id", cred.client_id.as_str()),
        ("client_secret", cred.client_secret.as_str()),
        ("refresh_token", cred.refresh_token.as_str()),
    ];
    let resp = client
        .post(endpoint)
        .timeout(HTTP_TIMEOUT)
        .form(&form)
        .send()
        .await
        .map_err(|e| {
            log_failure(&AdcError::ExchangeFailed, None, Some(&e.to_string()));
            AdcError::ExchangeFailed
        })?;
    let status = resp.status();
    if !status.is_success() {
        log_failure(
            &AdcError::ExchangeFailed,
            None,
            Some(&format!("the token endpoint answered {status}")),
        );
        return Err(AdcError::ExchangeFailed);
    }
    let body: serde_json::Value = resp.json().await.map_err(|e| {
        log_failure(
            &AdcError::ExchangeFailed,
            None,
            Some(&format!("the token response was not JSON: {e}")),
        );
        AdcError::ExchangeFailed
    })?;
    body.get("access_token")
        .and_then(|v| v.as_str())
        .map(str::to_string)
        .ok_or_else(|| {
            log_failure(
                &AdcError::ExchangeFailed,
                None,
                Some("no `access_token` field"),
            );
            AdcError::ExchangeFailed
        })
}

// ── the test seam for the CALLER-side arms ────────────────────────────────

/// A process-wide ADC file / token endpoint override, so the `gcloud_adc` arm in
/// `resolve_bearer` and the resolve spine can be driven against a `ScratchDir`
/// fixture and a `wiremock` URL.
///
/// This is the ONLY way the arm path is reachable in a test without either
/// reading the developer's real ADC file or POSTING a user's refresh token to
/// real Google — and it is deliberately a FUNCTION call from Rust test code,
/// never an environment variable: an env-configurable token endpoint would be a
/// route for a hostile environment to exfiltrate the ADC refresh token, which is
/// exactly the secret this flow is careful about.
#[derive(Debug, Clone)]
pub struct TestAdcOverride {
    /// What the flow sees INSTEAD of the real discovery (which is never run while
    /// an override is installed for it — so a test cannot reach the developer's
    /// real ADC file by accident).
    pub adc_file: AdcFileSource,
    /// The token endpoint (a `wiremock` URL). `None` = the real
    /// [`TOKEN_ENDPOINT`], which no test ever wants.
    pub token_endpoint: Option<String>,
}

/// Where `token()` takes the ADC file from while a test override is installed.
#[derive(Debug, Clone)]
pub enum AdcFileSource {
    /// Exactly this file — and when it is `None`, "no ADC file exists at all".
    /// This is the default a test should want: the ambient candidates are not
    /// even STAT-ed.
    Explicit(Option<PathBuf>),
    /// Run the real discovery (the `ADC_ENV_VAR` leg, then the home leg). Only
    /// for the tests that pin discovery itself, and ONLY with the env and `HOME`
    /// pointed at scratch dirs under the env lock.
    Discover,
}

/// The guard that puts the override back (restoring on panic too — the
/// `RestoreEnv` discipline).
#[doc(hidden)]
pub struct TestAdcOverrideGuard {
    previous: Option<TestAdcOverride>,
}

impl Drop for TestAdcOverrideGuard {
    fn drop(&mut self) {
        let previous = self.previous.take();
        *TEST_ADC_OVERRIDE.lock().unwrap_or_else(|p| p.into_inner()) = previous;
    }
}

static TEST_ADC_OVERRIDE: LazyLock<Mutex<Option<TestAdcOverride>>> =
    LazyLock::new(|| Mutex::new(None));

fn test_adc_override() -> Option<TestAdcOverride> {
    TEST_ADC_OVERRIDE
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .clone()
}

/// (TEST SEAM) Install `override_` until the returned guard drops. Callers must
/// serialize against each other (hold `test_support::ENV_LOCK`) — the override is
/// process-global, exactly like the environment it replaces. Production code has
/// no reason to call this and none does.
#[doc(hidden)]
pub fn set_test_adc_override(override_: TestAdcOverride) -> TestAdcOverrideGuard {
    let mut slot = TEST_ADC_OVERRIDE.lock().unwrap_or_else(|p| p.into_inner());
    let previous = slot.clone();
    *slot = Some(override_);
    TestAdcOverrideGuard { previous }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::matchers::method;
    use wiremock::{Mock, MockServer, ResponseTemplate};

    /// The serialization for the shared globals: the SHARED `test_support::ENV_LOCK`
    /// — NOT a private mutex. The mint cache and the ADC override are process-global
    /// and the `agent::session` resolve tests clear them too while holding
    /// `ENV_LOCK`; a second, private lock would let the two families run at once and
    /// wipe each other's cache entries (measured: `a_failed_exchange_keeps_the_
    /// previous_entry` went red under `cargo test --lib`, green in isolation — the
    /// exact shape of a cross-module global-state race). One lock, one global.
    fn serial() -> std::sync::MutexGuard<'static, ()> {
        crate::test_support::ENV_LOCK
            .lock()
            .unwrap_or_else(|p| p.into_inner())
    }

    fn adc_json(ty: &str, client_id: &str, secret: &str, refresh: &str) -> String {
        serde_json::json!({
            "type": ty,
            "client_id": client_id,
            "client_secret": secret,
            "refresh_token": refresh,
            "quota_project_id": "some-project"
        })
        .to_string()
    }

    // ── parse_adc ──────────────────────────────────────────────────────────

    #[test]
    fn parse_adc_accepts_a_minimal_authorized_user_file() {
        let src = Path::new("/scratch/adc.json");
        let cred = parse_adc(
            &adc_json("authorized_user", "cid", "secret", "refresh"),
            src,
        )
        .expect("a well-formed authorized_user file parses");
        assert_eq!(cred.client_id, "cid");
        assert_eq!(cred.client_secret, "secret");
        assert_eq!(cred.refresh_token, "refresh");
        assert_eq!(cred.source, src);
    }

    #[test]
    fn parse_adc_rejects_service_account_with_an_unsupported_type_error() {
        // The spec's central diagnostic: a service-account ADC file is REJECTED,
        // never silently tolerated (silence here means "my provider mysteriously
        // serves no token").
        let err = parse_adc(
            r#"{"type":"service_account","client_email":"x@y.iam","private_key":"k"}"#,
            Path::new("/scratch/adc.json"),
        )
        .expect_err("a service_account must be rejected");
        assert_eq!(err, AdcError::UnsupportedType);
    }

    #[test]
    fn parse_adc_rejects_any_other_type_as_unsupported() {
        let err = parse_adc(
            &adc_json("external_account", "cid", "secret", "refresh"),
            Path::new("/scratch/adc.json"),
        )
        .expect_err("only authorized_user is supported");
        assert_eq!(err, AdcError::UnsupportedType);
    }

    #[test]
    fn parse_adc_rejects_missing_fields_as_malformed() {
        for body in [
            r#"{"type":"authorized_user","client_secret":"s","refresh_token":"r"}"#,
            r#"{"type":"authorized_user","client_id":"c","refresh_token":"r"}"#,
            r#"{"type":"authorized_user","client_id":"c","client_secret":"s"}"#,
            // An EMPTY string is as missing as an absent key (a blank refresh
            // token cannot be exchanged).
            r#"{"type":"authorized_user","client_id":"c","client_secret":"s","refresh_token":""}"#,
        ] {
            let err = parse_adc(body, Path::new("/scratch/adc.json"))
                .expect_err("a credential this flow cannot exchange is malformed");
            assert_eq!(err, AdcError::Malformed, "for {body}");
        }
    }

    #[test]
    fn parse_adc_rejects_garbage_as_unparseable() {
        for body in [
            "not json at all",
            "",
            // Valid JSON, but no `type` to key the flow off.
            r#"{"client_id":"c","client_secret":"s","refresh_token":"r"}"#,
        ] {
            let err = parse_adc(body, Path::new("/scratch/adc.json"))
                .expect_err("an unidentifiable document is unparseable");
            assert_eq!(err, AdcError::Unparseable, "for {body:?}");
        }
    }

    // ── discovery ──────────────────────────────────────────────────────────

    #[test]
    fn adc_file_path_prefers_the_env_var() {
        let _lock = crate::test_support::env_lock();
        let base = crate::test_support::ScratchDir::in_env_temp("gcloud-adc-", "home");
        // BOTH candidates exist: the env-named file must win (Google's own
        // precedence, and the only way a user with several ADC files can pick).
        let env_file = base.join("env-creds.json");
        std::fs::write(&env_file, "{}").unwrap();
        let base_file = base.join(ADC_RELATIVE);
        std::fs::create_dir_all(base_file.parent().unwrap()).unwrap();
        std::fs::write(&base_file, "{}").unwrap();
        let got = adc_file_path_from(Some(env_file.as_path()), Some(base.path()))
            .expect("the env-named file is a candidate");
        assert_eq!(got, env_file);
    }

    #[test]
    fn adc_file_path_falls_back_from_env_to_the_platform_base() {
        // The env candidate is absent (it names nothing) → the platform base's
        // candidate. The path is built from `ADC_RELATIVE` rather than spelled
        // `.config/gcloud/…`, because that constant is per-target: on Windows it
        // is `gcloud\application_default_credentials.json` under `%APPDATA%`, and
        // the home-relative POSIX spelling is not a candidate there at all. A
        // hardcoded POSIX path made this red on CI's Windows leg — the seam took
        // `home` and discarded it under `#[cfg(windows)]`. Building from the
        // constant means this ONE test pins the `%APPDATA%` layout on Windows and
        // the `$HOME` layout everywhere else.
        let base = crate::test_support::ScratchDir::in_env_temp("gcloud-adc-", "home");
        let base_file = base.join(ADC_RELATIVE);
        std::fs::create_dir_all(base_file.parent().unwrap()).unwrap();
        std::fs::write(&base_file, "{}").unwrap();
        let got = adc_file_path_from(None, Some(base.path())).expect("the base file is found");
        assert_eq!(got, base_file);
    }

    #[test]
    fn adc_file_path_is_none_when_nothing_exists() {
        let base = crate::test_support::ScratchDir::in_env_temp("gcloud-adc-", "empty");
        assert_eq!(adc_file_path_from(None, Some(base.path())), None);
        // A candidate that exists but is NOT a file (a directory) is skipped, not
        // chosen-then-failed.
        let dir = base.join("dir-creds.json");
        std::fs::create_dir_all(&dir).unwrap();
        assert_eq!(
            adc_file_path_from(Some(dir.as_path()), Some(base.path())),
            None
        );
    }

    /// The `ADC_ENV_VAR` name itself is part of the contract (Google's own
    /// variable) — pin it so a rename reddens a test.
    #[test]
    fn the_adc_env_var_name_is_googles_own() {
        assert_eq!(ADC_ENV_VAR, "GOOGLE_APPLICATION_CREDENTIALS");
        assert_eq!(TOKEN_ENDPOINT, "https://oauth2.googleapis.com/token");
        assert_eq!(CACHE_TTL_MS, 50 * 60 * 1000);
    }

    // ── describe_error ─────────────────────────────────────────────────────

    #[test]
    fn describe_error_service_account_names_the_gcloud_fix() {
        let lines = describe_error(
            &AdcError::UnsupportedType,
            Some(Path::new(
                "/home/u/.config/gcloud/application_default_credentials.json",
            )),
        );
        assert!(
            lines.contains("service_account"),
            "the diagnostic must name the offending type: {lines}"
        );
        assert!(
            lines.contains("gcloud auth application-default login"),
            "the diagnostic must name the fix: {lines}"
        );
        assert!(
            lines.contains("authorized_user"),
            "the diagnostic must name what IS supported: {lines}"
        );
    }

    #[test]
    fn describe_error_file_not_found_names_the_checked_locations() {
        let lines = describe_error(&AdcError::FileNotFound, None);
        assert!(lines.contains("GOOGLE_APPLICATION_CREDENTIALS"), "{lines}");
        assert!(lines.contains(".config/gcloud"), "{lines}");
        assert!(
            lines.contains("gcloud auth application-default login"),
            "{lines}"
        );
    }

    // ── the exchange (wiremock — NEVER real Google) ────────────────────────

    /// The exchange posts the refresh token + both client fields, form-encoded,
    /// and hands back the minted `access_token`.
    #[tokio::test]
    async fn exchange_posts_the_refresh_token_to_google() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "access_token": "ya29.minted",
                "expires_in": 3600,
                "token_type": "Bearer"
            })))
            .mount(&server)
            .await;
        let cred = AdcCredential {
            client_id: "cid".to_string(),
            client_secret: "secret".to_string(),
            refresh_token: "refresh".to_string(),
            source: PathBuf::from("/scratch/adc.json"),
        };
        let got = exchange(&cred, &reqwest::Client::new(), &server.uri())
            .await
            .expect("a 200 with an access_token succeeds");
        assert_eq!(got, "ya29.minted");

        let requests = server.received_requests().await.expect("recorded");
        assert_eq!(requests.len(), 1);
        let form: Vec<(String, String)> = url::form_urlencoded::parse(&requests[0].body)
            .map(|(k, v)| (k.into_owned(), v.into_owned()))
            .collect();
        let field = |name: &str| form.iter().find(|(k, _)| k == name).map(|(_, v)| v.clone());
        assert_eq!(field("grant_type").as_deref(), Some("refresh_token"));
        assert_eq!(field("client_id").as_deref(), Some("cid"));
        assert_eq!(field("client_secret").as_deref(), Some("secret"));
        assert_eq!(field("refresh_token").as_deref(), Some("refresh"));
    }

    #[tokio::test]
    async fn exchange_reports_failure_on_a_non_2xx_response() {
        let server = MockServer::start().await;
        // The real shape of a revoked refresh token (`invalid_grant`) — we do not
        // parse it, we only refuse it.
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(400).set_body_json(serde_json::json!({
                "error": "invalid_grant",
                "error_description": "Bad Request"
            })))
            .mount(&server)
            .await;
        let cred = AdcCredential {
            client_id: "cid".to_string(),
            client_secret: "secret".to_string(),
            refresh_token: "expired".to_string(),
            source: PathBuf::from("/scratch/adc.json"),
        };
        let err = exchange(&cred, &reqwest::Client::new(), &server.uri())
            .await
            .expect_err("a 400 is a failed exchange");
        assert_eq!(err, AdcError::ExchangeFailed);
    }

    #[tokio::test]
    async fn expires_in_is_not_required() {
        // Google sometimes answers with only the access token; the TTL is fixed
        // at 50 minutes regardless, so `expires_in` is not read.
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "access_token": "ya29.bare"
            })))
            .mount(&server)
            .await;
        let cred = AdcCredential {
            client_id: "cid".to_string(),
            client_secret: "secret".to_string(),
            refresh_token: "refresh".to_string(),
            source: PathBuf::from("/scratch/adc.json"),
        };
        assert_eq!(
            exchange(&cred, &reqwest::Client::new(), &server.uri())
                .await
                .expect("a bare access_token is enough"),
            "ya29.bare"
        );
    }

    #[tokio::test]
    async fn a_200_without_an_access_token_is_a_failure() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(serde_json::json!({"id_token": "x"})),
            )
            .mount(&server)
            .await;
        let cred = AdcCredential {
            client_id: "cid".to_string(),
            client_secret: "secret".to_string(),
            refresh_token: "refresh".to_string(),
            source: PathBuf::from("/scratch/adc.json"),
        };
        assert_eq!(
            exchange(&cred, &reqwest::Client::new(), &server.uri())
                .await
                .expect_err("nothing was minted"),
            AdcError::ExchangeFailed
        );
    }

    // ── token_for_paths + the cache ────────────────────────────────────────

    /// A scratch ADC file with a UNIQUE refresh token (so a test's cache key can
    /// never collide with a sibling's even if the clear were skipped).
    fn scratch_adc(dir: &Path, tag: &str) -> PathBuf {
        let path = dir.join(format!("adc-{tag}.json"));
        std::fs::write(
            &path,
            adc_json(
                "authorized_user",
                "cid",
                "secret",
                &format!("refresh-{tag}"),
            ),
        )
        .unwrap();
        path
    }

    fn token_mock(token: &str) -> ResponseTemplate {
        ResponseTemplate::new(200).set_body_json(serde_json::json!({"access_token": token}))
    }

    /// The `#[allow(clippy::await_holding_lock)]` on every test below that takes
    /// `serial()` is INTENTIONAL: the guard exists precisely to stay held across
    /// the `await`s. The token cache and the ADC override are process-global, so
    /// releasing it at an `await` would let a sibling mint into the middle of this
    /// test's request count. The repo precedent is the same shape (`commands/
    /// settings.rs`, `agent/session/mod.rs`).
    #[allow(clippy::await_holding_lock)]
    #[tokio::test]
    async fn token_mints_and_caches_from_a_scratch_adc_file() {
        let _serial = serial();
        cache_clear();
        let dir = crate::test_support::ScratchDir::in_env_temp("gcloud-adc-", "mint");
        let adc = scratch_adc(&dir, "mint");
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(token_mock("ya29.first"))
            .mount(&server)
            .await;
        let client = reqwest::Client::new();
        let ttl = Duration::from_secs(60);

        assert_eq!(
            token_for_paths(Some(&adc), &server.uri(), ttl, &client)
                .await
                .as_deref(),
            Some("ya29.first")
        );
        assert_eq!(
            token_for_paths(Some(&adc), &server.uri(), ttl, &client)
                .await
                .as_deref(),
            Some("ya29.first"),
            "the second call is served from the cache"
        );
        assert_eq!(
            server.received_requests().await.expect("recorded").len(),
            1,
            "a fresh cache entry must skip the exchange entirely"
        );
    }

    #[allow(clippy::await_holding_lock)] // `serial()` must outlive the awaits
    #[tokio::test]
    async fn cache_hit_within_ttl_skips_the_exchange_and_expiry_refetches() {
        let _serial = serial();
        cache_clear();
        let dir = crate::test_support::ScratchDir::in_env_temp("gcloud-adc-", "ttl");
        let adc = scratch_adc(&dir, "ttl");
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(token_mock("ya29.rotating"))
            .mount(&server)
            .await;
        let client = reqwest::Client::new();

        // Inside the TTL: one request, however many calls.
        token_for_paths(
            Some(&adc),
            &server.uri(),
            Duration::from_secs(3600),
            &client,
        )
        .await;
        token_for_paths(
            Some(&adc),
            &server.uri(),
            Duration::from_secs(3600),
            &client,
        )
        .await;
        assert_eq!(server.received_requests().await.expect("recorded").len(), 1);

        // Past the TTL: a MISS (a stale entry is never served) → a refetch.
        // 30 ms is the injected TTL — the 50-minute constant is pinned by name
        // above, never waited out in a test.
        tokio::time::sleep(Duration::from_millis(40)).await;
        token_for_paths(
            Some(&adc),
            &server.uri(),
            Duration::from_millis(30),
            &client,
        )
        .await;
        assert_eq!(
            server.received_requests().await.expect("recorded").len(),
            2,
            "a stale entry must refetch"
        );
    }

    #[allow(clippy::await_holding_lock)] // `serial()` must outlive the awaits
    #[tokio::test]
    async fn a_failed_exchange_keeps_the_previous_entry() {
        let _serial = serial();
        cache_clear();
        let dir = crate::test_support::ScratchDir::in_env_temp("gcloud-adc-", "keep");
        let adc = scratch_adc(&dir, "keep");
        let ok = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(token_mock("ya29.keepme"))
            .mount(&ok)
            .await;
        let bad = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(500))
            .mount(&bad)
            .await;
        let client = reqwest::Client::new();

        assert_eq!(
            token_for_paths(Some(&adc), &ok.uri(), Duration::from_millis(20), &client)
                .await
                .as_deref(),
            Some("ya29.keepme")
        );
        // Let it go stale, then fail the refetch: `None` at the boundary (the
        // caller serves the static key) but the mint SURVIVES in the cache.
        tokio::time::sleep(Duration::from_millis(40)).await;
        assert_eq!(
            token_for_paths(Some(&adc), &bad.uri(), Duration::from_millis(20), &client).await,
            None,
            "a failed exchange never invents a token"
        );
        let snapshot = cache_snapshot();
        assert_eq!(
            snapshot.len(),
            1,
            "a transient outage must not evict a still-held token: {snapshot:?}"
        );
        assert_eq!(snapshot[0].1, "ya29.keepme");

        // And the stale entry stays in place until the NEXT SUCCESS overwrites it.
        assert_eq!(
            token_for_paths(Some(&adc), &ok.uri(), Duration::from_millis(20), &client)
                .await
                .as_deref(),
            Some("ya29.keepme")
        );
        assert_eq!(cache_snapshot().len(), 1, "replaced, not appended");
    }

    #[allow(clippy::await_holding_lock)] // `serial()` must outlive the awaits
    #[tokio::test]
    async fn cache_evicts_old_entries() {
        let _serial = serial();
        cache_clear();
        let dir = crate::test_support::ScratchDir::in_env_temp("gcloud-adc-", "evict");
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(token_mock("ya29.any"))
            .mount(&server)
            .await;
        let client = reqwest::Client::new();
        let ttl = Duration::from_secs(3600);

        // One MORE than the cap, each with a distinct key.
        for i in 0..=MAX_CACHE_ENTRIES {
            let adc = scratch_adc(&dir, &format!("evict-{i}"));
            assert_eq!(
                token_for_paths(Some(&adc), &server.uri(), ttl, &client)
                    .await
                    .as_deref(),
                Some("ya29.any"),
                "mint #{i}"
            );
        }
        let snapshot = cache_snapshot();
        assert_eq!(
            snapshot.len(),
            MAX_CACHE_ENTRIES,
            "the cache is capped (a long-lived Supervisor must not grow it): {snapshot:?}"
        );
        // FIFO: the FIRST key minted is the one that is gone.
        assert!(
            !snapshot.iter().any(|(k, _)| k.1 == "refresh-evict-0"),
            "the oldest entry must be evicted: {snapshot:?}"
        );
        assert!(
            snapshot.iter().any(|(k, _)| k.1 == "refresh-evict-1"),
            "the newer entries stay: {snapshot:?}"
        );
    }

    #[allow(clippy::await_holding_lock)] // `serial()` must outlive the awaits
    #[tokio::test]
    async fn token_returns_none_when_no_adc_file_exists() {
        let _serial = serial();
        cache_clear();
        // No mock server at all: nothing may be reached when there is no ADC
        // file — the call must return before the network.
        assert_eq!(
            token_for_paths(
                None,
                "http://127.0.0.1:1/token",
                Duration::from_secs(60),
                &reqwest::Client::new()
            )
            .await,
            None
        );
    }

    #[allow(clippy::await_holding_lock)] // `serial()` must outlive the awaits
    #[tokio::test]
    async fn token_returns_none_for_a_service_account_without_touching_the_network() {
        let _serial = serial();
        cache_clear();
        let dir = crate::test_support::ScratchDir::in_env_temp("gcloud-adc-", "sa");
        let adc = dir.join("adc.json");
        std::fs::write(
            &adc,
            r#"{"type":"service_account","client_email":"x@y.iam","private_key":"k"}"#,
        )
        .unwrap();
        // Port 1: a live request would fail with a transport error and log a
        // DIFFERENT diagnostic; the point is that the parse rejects first.
        assert_eq!(
            token_for_paths(
                Some(&adc),
                "http://127.0.0.1:1/token",
                Duration::from_secs(60),
                &reqwest::Client::new()
            )
            .await,
            None
        );
        assert!(
            cache_snapshot().is_empty(),
            "a rejected ADC file caches nothing"
        );
    }
}

//! The `provider-auth.json` store: one entry per provider id, keyed by the
//! provider's `settings.json` id (interface §3).
//!
//! The file lives in the platform DATA dir beside `mcp-auth.json`
//! ([`auth_file_path`] resolves it exactly like `mcp::oauth::auth_file_path`
//! — interface ruling R3; NEVER the config dir, which is a DIFFERENT
//! directory on Linux: `~/.config/archimedes` vs
//! `~/.local/share/archimedes`). The spec's `~/.archimedes/provider-auth.json`
//! is shorthand for "the same dir as `mcp-auth.json`" (ADR 0018) — a literal
//! `~/.archimedes` would be invisible to the app's own path conventions.
//!
//! Hermeticity is path INJECTION, never relocation: every read/write takes a
//! `&Path` (the `mcp::manager` `with_auth_file` precedent), so a test passes a
//! scratch path and production passes [`auth_file_path`] — the two are the
//! same path by construction, never by convention.
//!
//! Known Windows limitation: this file is created WITHOUT a mode argument on
//! Windows; it inherits the user profile ACL exactly like the sibling
//! `mcp-auth.json` (no `#[cfg(windows)]` ACL code exists in this repo). Unix
//! gets `0600` AT CREATION (never chmod-after-write).

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

/// One entry in `provider-auth.json`, keyed by provider id.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", tag = "type")]
pub enum ProviderCredential {
    #[serde(rename = "oauth")]
    OAuth(OAuthCredential),
    /// The `oidc` slot T1's plan reserves verbatim
    /// (`// T6 adds #[serde(rename = "oidc")] Oidc(OidcCredential).`) — the
    /// direct OIDC PKCE login's record (issue #127). ADDITIVE: nothing about the
    /// `OAuth` variant, the envelope or the load/save machinery changes, and the
    /// two exhaustive `match`es over this enum (`refresh::refresh` and
    /// `resolve_bearer_at`'s arm 2) gained their `Oidc` arms as the compile-driven
    /// checklist T1's plan describes.
    #[serde(rename = "oidc")]
    Oidc(OidcCredential),
    // An unknown `type` in a hand-edited file must not poison the whole
    // store — see `load`'s per-entry tolerance below.
}

/// A stored OAuth-family credential (interface §3's fixed shape).
///
/// NOTE: the `Option`s deliberately carry NO `skip_serializing_if` — a `None`
/// field serializes as `null`; that is the contract's exact wire shape.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OAuthCredential {
    /// Which flow minted this, so `refresh` can dispatch. `litellm_cli_pkce`
    /// (T5) | `litellm_cli_sso` (T5) | `pasted_token` (T5) | `gcloud_adc` (T4)
    /// | `oidc` (T6).
    pub flow: String,
    pub access_token: String,
    #[serde(default)]
    pub refresh_token: Option<String>,
    /// Epoch MILLISECONDS (the unit the spec pins — differs from
    /// `mcp::oauth::StoredAuth`'s SECONDS on purpose; never "unify" — a silent
    /// unit swap across the two stores reads as a green suite).
    #[serde(default)]
    pub expires_at: Option<i64>,
    pub base_url: String,
    #[serde(default)]
    pub client_id: Option<String>,
    #[serde(default)]
    pub token_endpoint: Option<String>,
    #[serde(default)]
    pub resource: Option<String>,
}

/// The `oidc` credential record (interface §3's T6 slot; issue #127) — the same
/// camelCase + millis-expiry discipline as [`OAuthCredential`].
///
/// The bearer this row exists for is `id_token` (the VALIDATED identity token the
/// proxy accepts), NOT an access token — `access_token` is carried for
/// diagnostics only.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OidcCredential {
    /// `flow == "oidc"` — the refresh dispatcher's arm key (stored WITH the
    /// credential, exactly as `OAuthCredential.flow` is: interface §3).
    pub flow: String,
    /// The VALIDATED `id_token` — what `resolve_bearer`'s arm 2 hands the proxy.
    pub id_token: String,
    /// The access token (diagnostics only; never the proxy bearer).
    #[serde(default)]
    pub access_token: Option<String>,
    #[serde(default)]
    pub refresh_token: Option<String>,
    /// Epoch MILLISECONDS (the store's unit — identical to
    /// [`OAuthCredential::expires_at`]; never seconds).
    #[serde(default)]
    pub expires_at: Option<i64>,
    /// The account subject — THE refresh-consistency key. A refresh whose new
    /// `sub` differs is REJECTED (`oidc::refresh`), so a stored credential can
    /// never silently become a different account.
    pub sub: String,
    /// The configured issuer (so a refresh re-validates `iss` without re-reading
    /// the settings document).
    pub issuer: String,
    /// The pre-registered client id, so a refresh can re-run the `aud`/`azp`
    /// checks. (A recorded deviation from the plan's field list, which omitted
    /// it: validating the audience of a refreshed token against an `aud` read
    /// OUT OF THAT TOKEN would prove nothing, so the known value has to be
    /// stored beside it.)
    #[serde(default)]
    pub client_id: Option<String>,
    /// The IdP token endpoint (re-discovery is not needed at refresh time).
    #[serde(default)]
    pub token_endpoint: Option<String>,
    #[serde(default)]
    pub scope: Option<String>,
}

/// The whole `provider-auth.json` document — `version` + the per-provider map.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProviderAuthStore {
    #[serde(default = "default_version")]
    pub version: u32,
    #[serde(default)]
    pub providers: BTreeMap<String, ProviderCredential>,
}

fn default_version() -> u32 {
    1
}

impl Default for ProviderAuthStore {
    fn default() -> Self {
        Self {
            version: default_version(),
            providers: BTreeMap::new(),
        }
    }
}

/// Lead time: refresh when the token expires within REFRESH_LEAD_MILLIS from
/// `now`.
pub const REFRESH_LEAD_MILLIS: i64 = 5 * 60 * 1000; // 5 minutes — the spec's lead (§2.2)

/// The `provider-auth.json` path (interface ruling R3): the platform DATA
/// dir, resolved EXACTLY like `mcp::oauth::auth_file_path()`
/// (`dirs::data_dir()/archimedes/provider-auth.json`) so the store sits
/// BESIDE `mcp-auth.json` (ADR 0018; the spec's `~/.archimedes` shorthand
/// means the DATA dir, never the config dir — on Linux `config_dir` is
/// `~/.config/archimedes`, a DIFFERENT directory). Do NOT "fix" this to
/// `config_dir`.
pub fn auth_file_path() -> PathBuf {
    dirs::data_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join("archimedes")
        .join("provider-auth.json")
}

/// Wall-clock epoch MILLISECONDS (this store's expiry unit — the spec pins
/// millis; `mcp::oauth::now_epoch_secs` is SECONDS and stays that way).
pub fn now_epoch_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

/// Is a stored `expires_at` (epoch millis) inside the refresh lead of `now`?
/// Expiring inside the lead IS expired (the in-flight request must survive —
/// the `mcp::oauth` rationale verbatim). `None` = the auth server never told
/// us an expiry → NEVER expires (never auto-refresh — MCP's guidance).
///
/// `pub`, NOT `pub(crate)`: a spec-pinned unit with a documented millis
/// contract, exercised by a SEPARATE compilation unit (`tests/provider_auth.rs`
/// — an integration test cannot see a `pub(crate)` item), and a later-wave
/// caller too (T6's OIDC refresh path); `REFRESH_LEAD_MILLIS` is `pub` for the
/// same reason (both are re-exported from `mod.rs`).
pub fn is_expiring_soon(expires_at: Option<i64>, now: i64) -> bool {
    expires_at.is_some_and(|at| at.saturating_sub(REFRESH_LEAD_MILLIS) <= now)
}

/// Load the store (tolerant — a missing OR corrupt file is an EMPTY store,
/// never an error; a bad ENTRY does not poison the whole store).
///
/// Returning an empty store is only survivable because [`save`] publishes by
/// rename: a reader can never observe a half-written document, so a parse
/// failure means the file really is absent or really is corrupt, not that this
/// reader lost a race with a writer (the `mcp::oauth::load_credentials`
/// rationale verbatim).
///
/// Per-entry tolerance: a hand-edited file with an unknown `type` (a future
/// app version's `oidc` entry read by this build, say) must drop ONLY that
/// entry — a whole-document parse failure falls back to parsing the map entry
/// by entry, so the good providers survive and the NEXT [`save`] does not
/// silently wipe them (the log names each dropped provider id).
pub fn load(path: &Path) -> ProviderAuthStore {
    let Ok(contents) = std::fs::read_to_string(path) else {
        return ProviderAuthStore::default();
    };
    if let Ok(store) = serde_json::from_str::<ProviderAuthStore>(&contents) {
        return store;
    }
    // The per-entry fallback: keep what parses, drop (and log) what does not.
    let Ok(value) = serde_json::from_str::<serde_json::Value>(&contents) else {
        crate::agent::debuglog::log(
            "provider-auth: store unparseable — serving an empty store (the file stays as it is \
             until the next save rewrites it)",
        );
        return ProviderAuthStore::default();
    };
    let version = value
        .get("version")
        .and_then(|v| v.as_u64())
        .and_then(|v| u32::try_from(v).ok())
        .unwrap_or_else(default_version);
    let providers = value
        .get("providers")
        .and_then(|v| v.as_object())
        .map(|entries| {
            entries
                .iter()
                .filter_map(|(id, entry)| {
                    match serde_json::from_value::<ProviderCredential>(entry.clone()) {
                        Ok(cred) => Some((id.clone(), cred)),
                        Err(e) => {
                            // One line naming the dropped provider (the rest
                            // of the store survives — never a whole-store
                            // wipe over one bad entry).
                            crate::agent::debuglog::log(&format!(
                                "provider-auth: dropping provider `{id}` from the store: {e}"
                            ));
                            None
                        }
                    }
                })
                .collect()
        })
        .unwrap_or_default();
    ProviderAuthStore { version, providers }
}

/// Save the store (atomic: stage + rename; `0600` at creation on unix; a
/// missing parent dir is created; a write failure is `Err`).
///
/// The mechanism is `mcp::oauth::save_credentials` verbatim, for the same
/// reasons: a plain `std::fs::write` TRUNCATES the target and then fills it,
/// so a concurrent reader can catch a half-written document — and because
/// [`load`] answers any parse failure with an EMPTY store, that reader's next
/// save would persist an empty file and lose every token. A rename is atomic
/// w.r.t. readers: they see the old document or the new one, never a prefix of
/// either. The staging name is UNIQUE PER WRITE (pid + a process-global
/// counter) and opened with `create_new`, so a collision is a refusal, never
/// a truncated overwrite of another writer's file (see [`stage_with_retry`]).
///
/// `0600` is applied AT CREATION (`create_new` + `mode(0o600)`, see
/// [`open_private`]) rather than by a chmod after the bytes are on disk: the
/// old `fs::write` + `set_permissions` shape created the staging file at the
/// umask mode and the secret sat world-readable between the two syscalls. So
/// neither the staging file nor the published file ever exists at the process
/// umask's mode, and the dir this creates for them is `0700` (see
/// [`create_credentials_dir`]).
///
/// `sync_all` is deliberately NOT called (the `mcp` store's stance;
/// `config::write_settings_atomic`'s `sync_all` is a different,
/// settings-specific decision).
///
/// A failure is an `Err`, and the staging file is removed on EVERY path that
/// does not rename it (the [`Staging`] `Drop`), so a failed save leaves no
/// secret-bearing litter.
pub fn save(path: &Path, store: &ProviderAuthStore) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        create_credentials_dir(parent)
            .map_err(|e| format!("creating the credentials dir failed: {e}"))?;
    }
    let contents =
        serde_json::to_string_pretty(store).map_err(|e| format!("serializing failed: {e}"))?;
    stage_with_retry(&contents, || unique_tmp_path(path))?.publish(path)
}

/// Read the store, insert ONE provider's credential, and save it — the
/// read-modify-write every persist site does, in the one order that is safe:
/// the file is re-read IMMEDIATELY before the write, so the merge is against
/// what is on disk NOW rather than a snapshot taken before a network round
/// trip (the `mcp::oauth::store_credentials` rationale verbatim: between
/// reading a stored token and persisting its refresh, an interactive auth may
/// have replaced that entry with a fresh browser-granted token, and other
/// providers' entries may have appeared; merging into the live document keeps
/// all of them).
pub fn store_credential(
    path: &Path,
    provider_id: &str,
    cred: ProviderCredential,
) -> Result<(), String> {
    let mut fresh = load(path);
    fresh.providers.insert(provider_id.to_string(), cred);
    save(path, &fresh)
}

/// Remove one provider's entry (load → remove → save; only when the entry
/// exists — removing a name that is not there saves nothing).
pub fn remove_credential(path: &Path, provider_id: &str) -> Result<(), String> {
    let mut fresh = load(path);
    if fresh.providers.remove(provider_id).is_none() {
        return Ok(());
    }
    save(path, &fresh)
}

/// The credentials' file mode (the file holds a token — not group- or
/// other-readable).
#[cfg(unix)]
const CREDENTIALS_MODE: u32 = 0o600;

/// How many staging names one save may try before giving up (see
/// [`stage_with_retry`]).
const STAGING_ATTEMPTS: u32 = 3;

/// Create the staging file, retrying ONLY on the one failure a fresh name can
/// cure: the staging name being taken. `create_new` REFUSES a taken name (that
/// is the point — see [`open_private`]), so a coincidence has to be
/// survivable rather than fatal: a file a killed process left behind, or a pid
/// recycled onto a counter that restarted at zero. Each attempt asks the
/// `next_name` closure for another name, so the retry can never land on the
/// same one twice, and a second writer mid-save is simply stepped over. Every
/// other error — the dir, the write, the chmod — returns immediately, because
/// a new name cannot cure it.
fn stage_with_retry(
    contents: &str,
    mut next_name: impl FnMut() -> PathBuf,
) -> Result<Staging, String> {
    let mut last: Option<(PathBuf, String)> = None;
    let mut attempts = 0;
    for _ in 0..STAGING_ATTEMPTS {
        attempts += 1;
        let name = next_name();
        match Staging::create(&name, contents) {
            Ok(staging) => return Ok(staging),
            Err(e) if is_name_taken(&e) => last = Some((name, e)),
            Err(e) => return Err(e),
        }
    }
    let (last_name, last_error) =
        last.expect("the loop only ends by exhausting the attempts it recorded");
    Err(exhausted_staging_error(attempts, &last_name, &last_error))
}

/// The exhausted-retry message. `last_name` doubles as the example of the
/// pattern, so the operator can glob it literally: the staging names this
/// module produces are `<stem>.<pid>.<counter>.<ext>.tmp` beside the target.
fn exhausted_staging_error(attempts: u32, last_name: &Path, last_error: &str) -> String {
    format!(
        "staging the provider credentials file failed after {attempts} attempts: every candidate \
         name was already taken ({last_error}). That is what a STALE staging file from a killed \
         save looks like — remove the leftovers matching `<stem>.<pid>.<counter>.<ext>.tmp` next \
         to the credentials file (the last name this run tried was {}) and re-run",
        last_name.display()
    )
}

/// Is this `save` error the staging NAME being taken (the one failure worth
/// another attempt — see [`stage_with_retry`])? Both spellings are matched
/// because they differ by platform: `EEXIST` renders as "File exists (os error
/// 17)" on unix and as Windows' "Cannot create a file when that file already
/// exists. (os error 183)" — matching only the unix wording would silently
/// disable the retry on Windows. (The retry is about a stale staging name, not
/// about Windows; no `#[cfg(windows)]` code exists in this module.)
fn is_name_taken(error: &str) -> bool {
    let e = error.to_lowercase();
    e.contains("file exists") || e.contains("already exists") || e.contains("os error 17")
}

/// Create the credentials dir, `0700` on unix when THIS call created it (the
/// dir holds the token file; `0755` from the umask would let anyone list the
/// names inside it). An existing dir keeps the mode its owner gave it (the
/// `mcp::oauth::create_credentials_dir` rationale — a credentials file named
/// relative to an arbitrary cwd is a caller bug and gets a message that says
/// which).
fn create_credentials_dir(path: &Path) -> std::io::Result<()> {
    if path.as_os_str().is_empty() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "the credentials dir resolved to an empty path (a credentials file named without a \
             directory, e.g. `provider-auth.json`) — it would land in the current working \
             directory; use the platform data dir path (`auth_file_path`)",
        ));
    }
    #[cfg(unix)]
    let existed = path.is_dir();
    std::fs::create_dir_all(path)?;
    #[cfg(unix)]
    if !existed {
        use std::os::unix::fs::PermissionsExt;
        return std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700));
    }
    #[cfg(not(unix))]
    let _ = path;
    Ok(())
}

/// Open the staging file for one [`save`], PRIVATE from the first instant it
/// exists: `create_new` (refused if the name is taken) with mode `0600` on
/// unix, so no bytes are ever written to a file another user could read.
fn open_private(path: &Path) -> std::io::Result<std::fs::File> {
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(CREDENTIALS_MODE);
    }
    options.open(path)
}

/// The staging file of one [`save`], and the reason a failed publish leaves
/// nothing behind: it is created private and removed on EVERY exit that did
/// not rename it into place — a failed write, a failed chmod, a failed rename,
/// or a panic on the way.
#[derive(Debug)]
struct Staging {
    path: PathBuf,
}

impl Staging {
    /// Create the staging file (see [`open_private`]) and write `contents`
    /// into it. The guard is built on the line AFTER the file comes to exist,
    /// so there is no error path that can return while the file is alive.
    fn create(path: &Path, contents: &str) -> Result<Self, String> {
        let file =
            open_private(path).map_err(|e| format!("creating the staging file failed: {e}"))?;
        // The guard exists from THIS line onward: every later exit — an `Err`
        // return or a panic — removes the file.
        let staging = Self {
            path: path.to_path_buf(),
        };
        // `file` MOVES into `write_and_close`, so its handle is closed by the
        // time this returns — which matters for the guard: `Drop` runs here,
        // and a `remove_file` against a still-open handle is an error on
        // Windows (a sharing violation), which would put the litter right
        // back. (That ordering comment is about Windows, but the code path is
        // the same everywhere — no new Windows-specific code.)
        write_and_close(file, contents)?;
        // Belt-and-braces on unix: the mode came with the creation (see
        // [`open_private`]), but a filesystem that ignores the create mode
        // (some FUSE / SMB mounts impose their own) would otherwise publish a
        // readable document.
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(CREDENTIALS_MODE))
                .map_err(|e| format!("chmod 0600 failed: {e}"))?;
        }
        Ok(staging)
    }

    /// The atomic publish: rename the staging file onto the target. This
    /// consumes the guard — on success the staging path is already gone (the
    /// rename MOVED it, so the `Drop`'s `remove_file` is a harmless `ENOENT`)
    /// and on failure the `Drop` removes the secret-bearing litter.
    fn publish(self, target: &Path) -> Result<(), String> {
        std::fs::rename(&self.path, target).map_err(|e| format!("renaming into place failed: {e}"))
    }
}

/// Write the document into the freshly-created staging file and CLOSE the
/// handle. It takes the handle BY VALUE so the close happens here — before any
/// [`Staging`] `Drop` can try to remove the path.
fn write_and_close(mut file: std::fs::File, contents: &str) -> Result<(), String> {
    use std::io::Write;
    if let Err(e) = file
        .write_all(contents.as_bytes())
        .and_then(|()| file.flush())
    {
        return Err(format!("writing failed: {e}"));
    }
    drop(file);
    Ok(())
}

impl Drop for Staging {
    fn drop(&mut self) {
        // Best-effort: there is nothing else to do with a file nobody else can
        // read, and the next save picks a fresh unique name.
        let _ = std::fs::remove_file(&self.path);
    }
}

/// A unique temp path next to `path` for one [`save`] write (the same stem +
/// extension, so a `.json` target still gets a `.json` temp file). Unique
/// across processes (pid) AND across concurrent threads of this process (a
/// counter) — see [`save`].
fn unique_tmp_path(path: &Path) -> PathBuf {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
    let pid = std::process::id();
    let stem = path
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("credentials");
    let extension = path.extension().and_then(|s| s.to_str());
    let name = match extension {
        Some(ext) => format!("{stem}.{pid}.{seq}.{ext}.tmp"),
        None => format!("{stem}.{pid}.{seq}.tmp"),
    };
    path.with_file_name(name)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A stored OAuth credential for the storage tests (an `id`-suffixed token).
    /// Gated `#[cfg(unix)]` like its callers (the two Unix storage tests and the
    /// Unix-only arm of `failed_save_leaves_no_staging_litter`).
    #[cfg(unix)]
    fn credential(id: &str) -> ProviderCredential {
        ProviderCredential::OAuth(OAuthCredential {
            flow: "litellm_cli_pkce".to_string(),
            access_token: format!("tok-{id}"),
            refresh_token: Some(format!("rt-{id}")),
            expires_at: Some(1_729_000_000_000),
            base_url: "https://gw.example/v1".to_string(),
            client_id: Some(format!("cid-{id}")),
            token_endpoint: Some("https://auth.example/token".to_string()),
            resource: Some("https://gw.example".to_string()),
        })
    }

    /// The target's inode (`0` for a path that does not exist — no real inode
    /// is ever `0`).
    #[cfg(unix)]
    fn inode_of(path: &Path) -> u64 {
        use std::os::unix::fs::MetadataExt;
        std::fs::metadata(path).map_or(0, |m| m.ino())
    }

    /// Every `*.tmp` path in `dir` (the staging files alive right now).
    /// Gated `#[cfg(unix)]` like `inode_of`: both callers assert the
    /// no-litter property on the Unix arms (the mode-gated dir and the rename
    /// that follows it), so on Windows this helper would be dead code.
    #[cfg(unix)]
    fn staging_files(dir: &Path) -> Vec<PathBuf> {
        std::fs::read_dir(dir)
            .expect("the dir is readable")
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| {
                p.extension()
                    .and_then(|s| s.to_str())
                    .is_some_and(|s| s == "tmp")
            })
            .collect()
    }

    #[cfg(unix)]
    #[test]
    fn storage_is_0600_on_unix() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().expect("a temp dir");
        let path = dir.path().join("provider-auth.json");
        store_credential(&path, "p", credential("p")).expect("saves");
        let mode = std::fs::metadata(&path)
            .expect("metadata")
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o600, "the file is 0600");
    }

    #[cfg(unix)]
    #[test]
    fn atomic_save_publishes_by_rename() {
        // The discriminator that matters: a save replaces the document by
        // RENAMING a staging file onto it, so the published file is a
        // DIFFERENT inode every time — a truncate-in-place `fs::write` would
        // keep the same inode and let a concurrent reader catch a half-written
        // document (which `load` would answer with an empty store, and that
        // reader's next save would persist an empty file: every token lost).
        let dir = tempfile::tempdir().expect("a temp dir");
        let path = dir.path().join("provider-auth.json");
        store_credential(&path, "a", credential("a")).expect("the first save");
        let first = inode_of(&path);
        assert_ne!(first, 0, "the published file exists");
        store_credential(&path, "b", credential("b")).expect("the second save");
        let second = inode_of(&path);
        assert_ne!(
            first, second,
            "the second save published by rename (a NEW inode), not by truncating the old file"
        );
        assert!(
            staging_files(dir.path()).is_empty(),
            "a successful save leaves no staging litter"
        );
        let loaded = load(&path);
        assert_eq!(loaded.providers.len(), 2, "both entries survived");
    }

    #[test]
    fn failed_save_leaves_no_staging_litter() {
        // Every exit that does not rename must clean up (the `Staging` `Drop`)
        // — a failed save of a SECRET-BEARING document must leave nothing.
        let dir = tempfile::tempdir().expect("a temp dir");
        // `path` and `store` serve only the mode-gated-dir arm below, which is
        // `#[cfg(unix)]` (POSIX dir permissions are what take the dir out of
        // the writer's hands) — gating them keeps the cross-platform rename
        // arm running on Windows without unused-variable errors.
        #[cfg(unix)]
        let path = dir.path().join("provider-auth.json");
        #[cfg(unix)]
        let store = ProviderAuthStore {
            version: 1,
            providers: BTreeMap::from([("a".to_string(), credential("a"))]),
        };

        // The real error path: the parent dir goes out of the writer's hands,
        // so the staging open fails and the save reports it.
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o500))
                .expect("the dir goes read-only");
            let r = save(&path, &store);
            // Restore the mode BEFORE any assertion can bail (a failing test
            // must not leave a dir the temp-dir cleanup cannot remove).
            std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700))
                .expect("the dir comes back");
            match r {
                // A root run (or a mount that ignores the dir mode) has nothing
                // to prove here — say so instead of passing vacuously.
                Ok(()) => eprintln!("skipping: this filesystem accepted a read-only-dir write"),
                Err(e) => {
                    assert!(
                        e.contains("creating the staging file failed"),
                        "with `create_new` the failure is the OPEN, not a later write: {e}"
                    );
                    assert!(
                        staging_files(dir.path()).is_empty(),
                        "a save that could not even stage leaves no litter"
                    );
                }
            }
        }

        // The rename failure through the guard: the target is an existing
        // DIRECTORY, so `rename` refuses it (EISDIR) while a real
        // secret-bearing staging file exists beside it.
        let blocked = dir.path().join("blocked");
        std::fs::create_dir(&blocked).expect("the blocker dir exists");
        let staged = unique_tmp_path(&blocked);
        let staging = Staging::create(&staged, "{}").expect("staging opens beside the dir");
        assert!(
            staged.exists(),
            "the staging file really exists before the publish is attempted"
        );
        let rename = staging.publish(&blocked);
        assert!(
            rename.is_err(),
            "renaming a FILE onto an existing directory is refused (EISDIR) — that is the forced \
             rename failure this path needs"
        );
        assert!(
            !staged.exists(),
            "a refused rename removes the staging file that holds the secret"
        );
    }

    #[test]
    fn load_a_missing_file_is_an_empty_store() {
        // The tolerant-load half of the §1 contract (the `oauth.rs`
        // `load_a_missing_file_is_an_empty_map` mirror): `resolve_bearer`'s
        // `None` path and the integration tests both lean on "a missing file
        // loads as an EMPTY store, never an Err".
        let dir = tempfile::tempdir().expect("a temp dir");
        let path = dir.path().join("does-not-exist.json");
        let store = load(&path);
        assert_eq!(store, ProviderAuthStore::default());
        assert!(store.providers.is_empty());
        assert_eq!(store.version, 1, "the default version is 1");
    }
}

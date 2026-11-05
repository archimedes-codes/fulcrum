//! Supervisor-side provider credentials: the `provider-auth.json` store, the
//! bearer-resolution dispatcher, and the background refresher. Deliberately
//! NOT under `agent/harness/provider/` (the wire clients — ADR 0024).
//!
//! The token's destination is `Model.api_key` (interface §1):
//! `EffectiveCatalog::resolve` is the single substitution site, so the Worker
//! never learns about OAuth/ADC/OIDC — it holds a pushed snapshot string.
//! Wiring `resolve_bearer` into `resolve` is T2/T4's seam; this module owns
//! the resolution, the store and the refresh lifecycle.
//!
//! # T7 — this directory is SPLIT across the package boundary
//!
//! Seven of the desktop's nine `provider_auth/` files moved here: S7.1 brought
//! `store`, `key_helper`, `gcloud`, `cli_sso`, `litellm_oauth`, `oidc` together
//! with this file's three free functions (`gcloud_token_auth_enabled`,
//! `resolve_bearer`, `resolve_bearer_at`), and S7.2 brought `budget.rs`. TWO
//! files stayed desktop and this module deliberately declares neither:
//!
//! * `refresh.rs` — the `tauri` half (`SupervisorRefresherDeps<R:
//!   tauri::Runtime>`, `tauri::AppHandle`, `session::SessionManager`). It is the
//!   split shell's only real desktop file;
//! * `headers.rs` — moved in T5/S3 as the crate's `provider_headers`.
//!
//! `budget.rs` could not move as a pure move: its `reload_providers` re-read the
//! provider rows off `settings.json` on every poll tick, and that read is the
//! desktop's disk boundary. It crossed as a seam instead —
//! [`budget::ProviderSource`], implemented by the desktop shell's
//! `SettingsProviderSource` — so the crate holds the SHAPE (`ProviderConfig`) and
//! the polling rules, and the desktop keeps the FILE (ADR 0091).
//!
//! The desktop's `src-tauri/src/agent/provider_auth/mod.rs` is a re-export shell
//! that keeps `pub mod refresh;` desktop, re-exports this module (both as module
//! aliases and by name), and holds the desktop half of that seam
//! (`budget_poller_for`). Every pre-existing desktop spelling
//! (`crate::agent::provider_auth::store::…`, `crate::agent::provider_auth::
//! budget::…`) resolves UNCHANGED through the shell.
//!
//! The dependency graph inside this directory is a DAG, which is what makes the
//! cut legal: `store` / `key_helper` / `gcloud` are leaves; `cli_sso → store`;
//! `litellm_oauth → cli_sso, store`; `oidc → litellm_oauth, store`; `budget →
//! store` + the three functions below; and the three functions call `gcloud`,
//! `oidc`, `store` — and NEVER `refresh::`. `refresh.rs` never calls
//! `resolve_bearer_at` either, so the two halves of the split directory are not
//! a cycle in either direction.

pub mod budget;
pub mod cli_sso;
pub mod gcloud;
pub mod key_helper;
pub mod litellm_oauth;
pub mod oidc;
pub mod store;

// The named surface this directory's `mod.rs` exported before the move, minus
// the two staying files' blocks (`budget` — moved here at S7.2 — and `refresh`)
// and `headers` (whose names the desktop shell still re-exports from
// `provider_headers`). A module re-export is a module path like any other, so
// the desktop shell re-issues these same names through
// `fulcrum_core::provider_auth::{…}`.
pub use budget::{
    classify_status, poll_all, poll_key_info, poll_team_info, poll_user_info, run_budget_tick,
    should_probe, BudgetEndpoint, BudgetFacts, BudgetPoller, DenialMemory, KeyBudget, PollStatus,
    ProviderSource, TeamBudget, UserBudget, BUDGET_POLL_INTERVAL_MILLIS,
};
pub use gcloud::{adc_file_path, describe_error, parse_adc, token as gcloud_token, AdcCredential};
pub use key_helper::run as run_key_helper;
pub use oidc::{authenticate_oidc, credential_from_session, OidcSession};
// T5's login surface (issue #126): the PKCE DCR orchestrator + the legacy CLI
// SSO device flow. `ProviderLoginStep`/`TeamDto` are declared in
// `litellm_oauth` (the flow that PRODUCES them) and re-exported by
// `commands::settings` as its wire surface — ONE type, never a mirror struct.
pub use litellm_oauth::{login as litellm_oauth_login, ProviderLoginStep, TeamDto};
pub use store::{
    auth_file_path, is_expiring_soon, load, now_epoch_millis, remove_credential, save,
    store_credential, OAuthCredential, OidcCredential, ProviderAuthStore, ProviderCredential,
    REFRESH_LEAD_MILLIS,
};

use std::path::Path;

/// Is the `gcloud_adc` flow enabled for one provider row? (issue #128.)
///
/// Precedence, in the ONE place it is computed:
/// - `LITELLM_GCLOUD_TOKEN_AUTH` unset (or an unrecognised value) → the row
///   decides (`use_gcloud_token_auth` OR `authMode: "gcloud_adc"`);
/// - `=0` / `=false` → OFF even for a row that enabled it (an explicit env
///   disable beats the config — the escape hatch for "never touch my ADC file");
/// - `=1` / `=true` → ON for EVERY row (an env force beats a config that did not
///   opt in — the container/CI shape, where the ADC file is the only credential).
///
/// The env is read PER CALL, deliberately: this runs on a Supervisor resolve
/// (not a per-turn Worker path), it keeps the toggle injectable from a test
/// without touching `settings.json`, and it means the flag cannot be cached at
/// startup behind a settings load.
pub fn gcloud_token_auth_enabled(provider: &crate::config_types::ProviderConfig) -> bool {
    let enabled_by_row = provider.use_gcloud_token_auth || provider.auth_mode == "gcloud_adc";
    match std::env::var_os("LITELLM_GCLOUD_TOKEN_AUTH").as_deref() {
        Some(v) if v == "0" || v.eq_ignore_ascii_case("false") => false,
        Some(v) if v == "1" || v.eq_ignore_ascii_case("true") => true,
        // Unset, or any other spelling: the row decides (a typo in the env must
        // not silently disable a flow the user configured).
        _ => enabled_by_row,
    }
}

/// The effective bearer for one provider row, or `None` to mean "use the row's
/// static `api_key` as-is". Never `Err` at this boundary — a failure resolves
/// to `None` + a `debuglog` line, because blanking a working key is worse than
/// a stale one (interface §1).
///
/// THE production entry (the interface fixes this signature — T2/T4 wire it
/// into `EffectiveCatalog::resolve`): it resolves the store path itself via
/// [`store::auth_file_path`] — the platform DATA dir, beside `mcp-auth.json`
/// (ruling R3). NEVER resolved from `config_dir` (a different directory —
/// `~/.config/archimedes` vs `~/.local/share/archimedes` on Linux). It is a
/// thin alias over [`resolve_bearer_at`], so the two are the same path by
/// construction, never by convention.
///
/// `execute_helpers` is the spec's `execute_helpers = false` seeding gate (T2
/// owns threading it); T1 declares the parameter so T2 does not have to change
/// the signature every caller uses.
pub async fn resolve_bearer(
    provider: &crate::config_types::ProviderConfig,
    config_dir: &Path,
    client: &reqwest::Client,
    execute_helpers: bool,
) -> Option<String> {
    resolve_bearer_at(
        provider,
        &store::auth_file_path(),
        config_dir,
        client,
        execute_helpers,
    )
    .await
}

/// THE path-taking core (the tested seam — the `mcp::manager` `with_auth_file`
/// shape): every `oauth`/`oidc` read goes through `auth_file: &Path`, so a test
/// passes `scratch.join("provider-auth.json")` and nothing ever touches the
/// real data dir. Production reaches it through [`resolve_bearer`], which
/// passes [`store::auth_file_path`].
///
/// The `gcloud_adc` / `!command` arms (which may need `config_dir`) share this
/// ONE body so later tasks extend a single function, not two.
pub async fn resolve_bearer_at(
    provider: &crate::config_types::ProviderConfig,
    auth_file: &Path,
    config_dir: &Path,
    client: &reqwest::Client,
    execute_helpers: bool,
) -> Option<String> {
    // `client` and `config_dir` are both live now (the `gcloud_adc` flow uses
    // them; T5/T6 will use `client` for a synchronous refresh before serving an
    // expiring token). No parameter is left unnamed.
    // The dispatch ORDER is FIXED by interface §4 — earlier arms must not be
    // reordered. Slot 1 (the ADC flow) sits above the landed match, which keeps
    // T1's/T2a's arm order exactly as it landed.
    // Dispatch slot 1 (interface §4, T4 / issue #128): the Google ADC flow.
    // `gcloud::token` mints (or reuses a cached) access token from the user's ADC
    // file; ANY failure inside it — no ADC file, a `service_account`, a
    // malformed file, a failed exchange — resolves to `None`, which means
    // "serve the row's static key" (interface §1: a failed credential resolution
    // degrades, it never blanks), and `gcloud` logs the specific diagnostic
    // through `debuglog`.
    //
    // A MISS FALLS THROUGH to the rest of the ladder (interface §4's slot order
    // says ADC is asked FIRST, not that it is the only thing asked: a row with
    // both `use_gcloud_token_auth` and a `!command` key helper keeps its helper when
    // the ADC file is absent). It is written as an early-return rather than a
    // `match` arm so that fall-through is the plain control flow, and the
    // match below keeps T1's/T2a's landed arm order untouched (ruling R8's
    // arm-3-vs-oauth ordering is T5's to change, not this task's).
    //
    // This flow is NOT a subprocess and takes no `execute_helpers` gate: the
    // exchange is one `reqwest` POST, so the "no child process at seeding"
    // invariant is untouched (pinned by the zero-subprocess census). The minted
    // token is NEVER persisted — `gcloud` holds it in a 50-minute in-memory cache
    // and nothing writes it to the provider-auth store.
    if gcloud_token_auth_enabled(provider) {
        if let Some(token) = gcloud::token(config_dir, client).await {
            return Some(token);
        }
        crate::debuglog::log(&format!(
            "provider-auth: gcloud_adc produced no token for provider `{}` — falling back to \
             the row's other credential sources (the static key if there are no others)",
            provider.id
        ));
    }
    match provider.auth_mode.as_str() {
        // Arm 3 (T2a, issue #129; guard narrowed by T5 per ruling R8, issue #126):
        // an `api_key` starting with `!` is a KEY HELPER — executed when
        // `execute_helpers` is true. A helper FAILURE degrades to `None` (the
        // static key as-is) per the interface's rule — never an `Err`, never a
        // blank — with a debuglog line. With `execute_helpers == false` (the
        // seeding path) the helper is NEVER spawned and `None` is returned (the
        // command string stays the static key; the "no subprocess at startup"
        // invariant, pinned by test).
        //
        // RULING R8 (interface §10): the guard used to be `_`, which matches ANY
        // `auth_mode`, so a row carrying BOTH a `!`-prefixed `api_key` AND
        // `authMode: "oauth"` took this arm and silently ignored its stored
        // OAuth token. The guard is now narrowed to the `api_key` auth mode —
        // the fix R8 prefers, because it keeps interface §4's documented
        // precedence (an EXPLICIT auth mode beats an inferred `!` prefix)
        // WITHOUT reordering the match (this stays in T1's landed slot). Pinned
        // by `a_row_with_a_bang_key_and_oauth_mode_serves_the_stored_token` and
        // its helper-side twin in `tests/provider_oauth_flow.rs`.
        //
        // Consequence owned: a row with an UNRECOGNISED auth mode and a `!`-key
        // now falls to the default arm (the static key as-is) instead of running
        // the helper — the permissive boundary's own rule (validate at use). A
        // row with `authMode` absent is unaffected: serde defaults it to
        // `"api_key"`, so every existing helper keeps working.
        _ if provider.auth_mode == "api_key" && provider.api_key.starts_with('!') => {
            if !execute_helpers {
                None
            } else {
                match key_helper::run(&provider.api_key).await {
                    Ok(token) => Some(token),
                    Err(e) => {
                        crate::debuglog::log(&format!(
                            "provider-auth: key helper failed for {}: {e}",
                            provider.id
                        ));
                        None
                    }
                }
            }
        }
        "oauth" | "oidc" => {
            // T1 implements the `oauth` arm; T6 fills the `oidc` half of this
            // shared entry point (interface §4's arm 2 — `"oauth" | "oidc"` → the
            // stored credential).
            //
            // The stored token is SERVED as-is: the background loop
            // (`refresh::SupervisorRefresher`) refreshes ahead of expiry with the
            // 5-minute lead, and a refresh failure must never take a working
            // bearer away (interface §1). T5/T6 may extend this arm to refresh
            // synchronously when the token is inside the lead margin.
            let loaded = store::load(auth_file);
            match loaded.providers.get(&provider.id) {
                Some(ProviderCredential::OAuth(cred)) => {
                    // The lead check is computed here so the extension point is
                    // load-bearing rather than speculative; T1 serves the
                    // stored token either way.
                    let _ = store::is_expiring_soon(cred.expires_at, store::now_epoch_millis());
                    Some(cred.access_token.clone())
                }
                // The `oidc` arm (issue #127). THE BEARER IS THE VALIDATED
                // `id_token` — never the access token (the proxy accepts the
                // identity token). The resolve spine substitutes it into
                // `Model.api_key` (interface §1: NO new `Model` field).
                //
                // Refresh-before-serve: an `id_token` inside the 5-minute lead is
                // refreshed SYNCHRONOUSLY here, because unlike an OAuth access
                // token an `id_token` is short-lived and the Supervisor loop's
                // next pass may not arrive before it dies. A SUCCESS is persisted
                // through the existing atomic 0600 path and served; a FAILURE —
                // including the `sub`-mismatch account-swap rejection — serves the
                // STORED token unchanged (interface §1: degrade, never blank; and
                // in the mismatch case the stored token is exactly the honest one,
                // since the candidate was refused).
                Some(ProviderCredential::Oidc(cred)) => {
                    if store::is_expiring_soon(cred.expires_at, store::now_epoch_millis()) {
                        match oidc::refresh(cred, client).await {
                            Ok(refreshed) => {
                                // Persist best-effort: a write failure must not
                                // cost us the fresh token we just earned (the
                                // store loop retries the write on its next pass).
                                if let Err(e) = store::store_credential(
                                    auth_file,
                                    &provider.id,
                                    ProviderCredential::Oidc(refreshed.clone()),
                                ) {
                                    crate::debuglog::log(&format!(
                                        "provider-auth: could not persist the refreshed oidc \
                                         credential for `{}`: {e}",
                                        provider.id
                                    ));
                                }
                                Some(refreshed.id_token)
                            }
                            Err(e) => {
                                // Includes the account-swap rejection: the stored
                                // token keeps serving and the operator sees the
                                // reason in the debuglog — a user is never silently
                                // logged in as somebody else, and never blanked.
                                crate::debuglog::log(&format!(
                                    "provider-auth: the oidc refresh for `{}` failed ({e}) — \
                                     serving the stored id_token (never blanking a working \
                                     bearer)",
                                    provider.id
                                ));
                                Some(cred.id_token.clone())
                            }
                        }
                    } else {
                        Some(cred.id_token.clone())
                    }
                }
                // No stored credential (or one this build cannot serve) → the
                // static key as-is. Degrade, never blank.
                _ => {
                    crate::debuglog::log(&format!(
                        "provider-auth: no stored credential for provider `{}` (auth mode `{}`) — \
                         serving the configured static key",
                        provider.id, provider.auth_mode
                    ));
                    None
                }
            }
        }
        // The default — the static key path (`"api_key"`, an unknown mode, or an
        // empty one: the permissive boundary validates at use, not at parse).
        _ => None,
    }
}

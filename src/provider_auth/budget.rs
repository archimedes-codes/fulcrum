//! Multi-level budget & quota polling + per-endpoint denial memory (issue
//! #132, Task 7).
//!
//! A LiteLLM virtual key may or may not be entitled to each of the three
//! management endpoints that report spend/budget: an admin or master key reads
//! all of them, an ordinary virtual key typically reads only `/key/info` for
//! itself. The acceptance criterion — "spend + max budget for the endpoints the
//! key may read; a forbidden endpoint is probed ONCE and then stays quiet" — is
//! exactly what [`DenialMemory`] delivers.
//!
//! # Loop-shape law (T7)
//!
//! **This module NEVER spawns its own interval loop.** The budget tick is
//! driven by `refresh.rs`'s `SupervisorRefresher` (T1) — the same
//! `tokio::spawn` sleep-loop, the same `select!` sleep-vs-cancel, the same
//! backoff, the same `JoinHandle` + `Drop` + [`super::SupervisorRefresher::stop`]
//! lifecycle, and the same single start point
//! (`lib.rs::setup_dirs_with_factory_refresher`). A second `tokio::time::interval`
//! in this crate would be an architecture fork: two loop flavours, two stop
//! paths, two census-leak shapes (ADR 0078). [`run_budget_tick`] is therefore a
//! *body*, called from that loop and from the command's on-demand path — never a
//! task of its own.
//!
//! # The three endpoints (the wire)
//!
//! * `GET {base}/key/info` — the key's own spend / `maxBudget` / reset;
//! * `GET {base}/v2/user/info` (falling back to `GET {base}/user/info`) — the
//!   user's spend/budget and the team memberships the team fan-out keys off;
//! * `GET {base}/team/info?team_id={id}` — the SHARED team budget, one hop per
//!   membership the user response named.
//!
//! The URL for every hop is the provider row's base VERBATIM + the path
//! (`trim_end_matches('/')` only) — the SAME root rule
//! `catalog::discover_litellm_models` rung 1 applies to `{base}/model/info`, so
//! one convention governs every LiteLLM management hop, not two. A row whose
//! base ends in `/v1` therefore asks `/v1/key/info` (which LiteLLM serves), and
//! nothing here re-twists or strips `/v1`.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use serde::Deserialize;

use super::resolve_bearer_at;
use super::store;
use crate::agent::debuglog;
use crate::config::load_settings;

/// The default budget poll interval (15 s). Long enough that a forbidden
/// endpoint costs one request per interval (the denial-memory bound), short
/// enough that the Settings page shows a fresh number on mount. Injectable on
/// [`BudgetPoller`] so a cadence test never waits on a real interval.
pub const BUDGET_POLL_INTERVAL_MILLIS: u64 = 15_000;

/// The three poll hops, in the order a tick runs them (the team hop needs the
/// ids the user hop names, so `TeamInfo` is last).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum BudgetEndpoint {
    /// `GET {base}/key/info`
    KeyInfo,
    /// `GET {base}/v2/user/info` (with the `/user/info` fallback)
    UserInfo,
    /// `GET {base}/team/info?team_id={id}`
    TeamInfo,
}

impl BudgetEndpoint {
    /// Every hop (the denial-diff loop in the tick walks this — the compiler
    /// cannot be fooled by a missing entry the way a hand-written list can).
    pub const ALL: [BudgetEndpoint; 3] = [
        BudgetEndpoint::KeyInfo,
        BudgetEndpoint::UserInfo,
        BudgetEndpoint::TeamInfo,
    ];

    /// The DTO's `level` value (`"key"` | `"user"` | `"team"`).
    pub fn level(self) -> &'static str {
        match self {
            BudgetEndpoint::KeyInfo => "key",
            BudgetEndpoint::UserInfo => "user",
            BudgetEndpoint::TeamInfo => "team",
        }
    }

    /// The wire path, for the log line (the string a person greps for).
    fn path(self) -> &'static str {
        match self {
            BudgetEndpoint::KeyInfo => "/key/info",
            BudgetEndpoint::UserInfo => "/v2/user/info",
            BudgetEndpoint::TeamInfo => "/team/info",
        }
    }
}

/// The auth-relevant status classification (the poller's decision input).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PollStatus {
    /// 2xx — read the body.
    Ok,
    /// A 4xx EXCEPT 429, or any 5xx — this key cannot read this endpoint (or
    /// the endpoint is broken here); remember it per endpoint.
    Denied,
    /// 429 — rate limited: back off, do NOT remember.
    Transient,
    /// Anything else (3xx, 1xx) — neither.
    Unknown,
}

/// Classify an HTTP status for denial memory.
///
/// THE rule pin: **a 429 is `Transient`, never `Denied`.** Rate-limiting is
/// transient — recording it as a denial would permanently silence a working
/// endpoint that is merely busy, and the interval re-arm would re-arm the storm
/// every 15 s. 401/403/404 (and every other 4xx) mean "this key cannot read
/// this endpoint here" and ARE remembered; a 5xx is remembered too (the spec's
/// rule: a broken endpoint is not worth hammering once per interval either).
pub fn classify_status(status: u16) -> PollStatus {
    match status {
        200..=299 => PollStatus::Ok,
        // Rate limit — back off, do NOT remember (checked BEFORE the 4xx range
        // so the range cannot swallow it).
        429 => PollStatus::Transient,
        400..=499 => PollStatus::Denied,
        500..=599 => PollStatus::Denied,
        _ => PollStatus::Unknown,
    }
}

/// The per-endpoint denial scoreboard, keyed by `(base_url, endpoint)`.
///
/// A 4xx except 429, or any 5xx, enters memory; a 429 NEVER does
/// ([`classify_status`]). Two providers on different proxies share nothing:
/// `key_info` denied on one base cannot suppress `user_info` on another, and
/// even within one base each hop is remembered ALONE (one absent endpoint must
/// not disable the others).
///
/// Not persisted — in-memory only (the spec says "remember", not "durably
/// remember"; a restart pays one quiet re-probe per endpoint, which the
/// once-per-interval rule bounds).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct DenialMemory {
    /// `base_url -> denied endpoints`.
    denied: BTreeMap<String, BTreeSet<BudgetEndpoint>>,
}

impl DenialMemory {
    /// Remember that `endpoint` on `base_url` is not readable.
    pub fn deny(&mut self, base_url: &str, endpoint: BudgetEndpoint) {
        self.denied
            .entry(base_url.to_string())
            .or_default()
            .insert(endpoint);
    }

    /// Is `endpoint` on `base_url` currently suppressed?
    pub fn is_denied(&self, base_url: &str, endpoint: BudgetEndpoint) -> bool {
        self.denied
            .get(base_url)
            .is_some_and(|set| set.contains(&endpoint))
    }

    /// Forget everything for ONE provider base (the bearer-rotation move: a
    /// re-issued key may hold NEW entitlements, and a wrong-but-silent `None`
    /// is worse than one quiet re-probe per rotation).
    pub fn clear_provider(&mut self, base_url: &str) {
        self.denied.remove(base_url);
    }

    /// Forget everything (the interval re-arm: a denial is only good until the
    /// next poll interval elapses, so a proxy that GRANTED an entitlement
    /// mid-interval is heard again on the next arm).
    pub fn clear_all(&mut self) {
        self.denied.clear();
    }

    /// Is the scoreboard empty? (A test assertion more than a decision input.)
    pub fn is_empty(&self) -> bool {
        self.denied.is_empty()
    }

    /// Union `other` into `self` — the tick's write-back discipline: the poll
    /// runs on a CLONE of the shared memory (never a `MutexGuard` across an
    /// `await`), and the clone's denials are merged back under the lock. Sets
    /// only accumulate here, so a union can never resurrect a cleared entry
    /// that the shared copy had already forgotten… except across a genuine
    /// re-arm, where the worst outcome is one extra quiet interval.
    pub fn merge_from(&mut self, other: &DenialMemory) {
        for (base, set) in &other.denied {
            self.denied
                .entry(base.clone())
                .or_default()
                .extend(set.iter().copied());
        }
    }
}

/// The per-endpoint denial guard: the poll hop checks this BEFORE building a
/// request, so a denied endpoint emits NO request AND NO log line.
pub fn should_probe(denied: &DenialMemory, base_url: &str, endpoint: BudgetEndpoint) -> bool {
    !denied.is_denied(base_url, endpoint)
}

// ── the wire-parse types ────────────────────────────────────────────────────
//
// Lenient by design: `#[serde(default)]` on every field and NO
// `deny_unknown_fields` — the LiteLLM JSON is the authority and it carries many
// fields this task ignores. Two eras of `/key/info` are accepted (the
// documented `info` sub-object and the modern top-level keys), because proxies
// in the wild serve both.

/// `/key/info` (see the module docs for the two accepted eras).
///
/// THE WIRE IS SNAKE_CASE (LiteLLM is a FastAPI app and its models are
/// snake_case), so these INBOUND types carry the field names as the wire spells
/// them and add camelCase `alias`es for a proxy that camelCases. DEVIATION from
/// the plan, which put `#[serde(rename_all = "camelCase")]` on the parse types:
/// that would have rejected every documented LiteLLM response body (`max_budget`
/// would only match a `maxBudget` key) and silently produced `None` facts for a
/// perfectly readable endpoint. camelCase belongs on the OUTBOUND DTO
/// (`commands::settings::ProviderBudgetDto`) and only there.
#[derive(Debug, Clone, PartialEq, Deserialize)]
struct KeyInfoResponse {
    #[serde(default)]
    info: Option<KeyInfoBody>,
    #[serde(default)]
    spend: Option<f64>,
    #[serde(default, alias = "maxBudget")]
    max_budget: Option<f64>,
    #[serde(default, alias = "budgetResetAt")]
    budget_reset_at: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
struct KeyInfoBody {
    #[serde(default)]
    spend: Option<f64>,
}

/// `/v2/user/info` (or the `/user/info` fallback): the user's own
/// spend/max_budget plus the team memberships.
#[derive(Debug, Clone, PartialEq, Deserialize)]
struct UserInfoResponse {
    #[serde(default, alias = "userId")]
    user_id: Option<String>,
    #[serde(default)]
    spend: Option<f64>,
    #[serde(default, alias = "maxBudget")]
    max_budget: Option<f64>,
    #[serde(default, alias = "budgetResetAt")]
    budget_reset_at: Option<String>,
    /// The older era carries the user's spend nested here.
    #[serde(default, alias = "userInfo")]
    user_info: Option<UserInfoBody>,
    /// The team fan-out source; each row carries the member-scoped budget.
    #[serde(default)]
    teams: Vec<UserTeamRow>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
struct UserInfoBody {
    #[serde(default)]
    spend: Option<f64>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
struct UserTeamRow {
    #[serde(default, alias = "teamId")]
    team_id: Option<String>,
    #[serde(default, alias = "teamAlias")]
    team_alias: Option<String>,
    #[serde(default)]
    spend: Option<f64>,
    #[serde(default, alias = "maxBudget")]
    max_budget: Option<f64>,
    #[serde(default, alias = "budgetResetAt")]
    budget_reset_at: Option<String>,
    // `team_memberships[].max_budget_in_team` (the MEMBER-scoped cap inside a
    // team) is deliberately NOT parsed: the row T8 renders for a team is the
    // SHARED team budget from `/team/info`, and a second, differently-scoped
    // number on the same row would be indistinguishable from it in the UI.
    // Parsing a field nothing renders would be dead code with a wire contract.
}

/// `/team/info?team_id={id}`: the SHARED team budget.
#[derive(Debug, Clone, PartialEq, Deserialize)]
struct TeamInfoResponse {
    #[serde(default, alias = "teamId")]
    team_id: Option<String>,
    #[serde(default, alias = "teamAlias")]
    team_alias: Option<String>,
    #[serde(default)]
    spend: Option<f64>,
    #[serde(default, alias = "maxBudget")]
    max_budget: Option<f64>,
    #[serde(default, alias = "budgetResetAt")]
    budget_reset_at: Option<String>,
}

/// `remaining = max_budget - spend`, and ONLY when both are numbers and the
/// budget is positive — an unlimited key (`max_budget` absent or `0`) has no
/// "remaining" to show, and inventing one would render a fake cap.
fn remaining_of(spend: Option<f64>, max_budget: Option<f64>) -> Option<f64> {
    match (spend, max_budget) {
        (Some(spend), Some(max)) if max > 0.0 => Some(max - spend),
        _ => None,
    }
}

/// The key-level budget row (`/key/info`).
#[derive(Debug, Clone, PartialEq)]
pub struct KeyBudget {
    pub spend: Option<f64>,
    pub max_budget: Option<f64>,
    pub remaining: Option<f64>,
    pub budget_reset_at: Option<String>,
}

/// The user-level budget row (`/v2/user/info`), plus the team ids the fan-out
/// runs on (extracted from the SAME response — never a second lookup).
#[derive(Debug, Clone, PartialEq)]
pub struct UserBudget {
    pub user_id: Option<String>,
    pub spend: Option<f64>,
    pub max_budget: Option<f64>,
    pub remaining: Option<f64>,
    pub budget_reset_at: Option<String>,
    pub team_ids: Vec<String>,
}

/// One team-level budget row (`/team/info?team_id=…`).
#[derive(Debug, Clone, PartialEq)]
pub struct TeamBudget {
    pub team_id: Option<String>,
    pub team_alias: Option<String>,
    pub spend: Option<f64>,
    pub max_budget: Option<f64>,
    pub remaining: Option<f64>,
    pub budget_reset_at: Option<String>,
}

/// One provider's budget facts for one tick. A `None`/empty member means "this
/// key cannot read that endpoint (or it did not answer)" — which is exactly the
/// row the DTO omits, never an error.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct BudgetFacts {
    pub key: Option<KeyBudget>,
    pub user: Option<UserBudget>,
    pub teams: Vec<TeamBudget>,
}

// ── the pollers ─────────────────────────────────────────────────────────────

/// The management root for one provider base (the SAME rule
/// `catalog::discover_litellm_models` applies to `{base}/model/info`: the base
/// as configured, only the trailing slash trimmed — no `/v1` retwist, no
/// `/v1` strip).
fn management_root(base_url: &str) -> &str {
    base_url.trim_end_matches('/')
}

/// What one HTTP probe produced (the pollers' shared decision input).
enum Probe {
    /// 2xx with a JSON body.
    Answered(serde_json::Value),
    /// A denial-class status (see [`classify_status`]).
    Denied,
    /// 429 — transient, never remembered.
    Transient,
    /// A transport failure, an unparseable 2xx body, or an unclassified status:
    /// neither a denial nor an answer, so NO memory entry.
    Inconclusive,
}

/// One bounded GET with the bearer. NEVER errors and NEVER logs (the loop owns
/// the logging discipline — a denied hop must stay silent).
async fn probe(
    client: &reqwest::Client,
    url: &str,
    bearer: &str,
    query: &[(&str, String)],
) -> Probe {
    let mut request = client.get(url).bearer_auth(bearer);
    if !query.is_empty() {
        request = request.query(query);
    }
    let Ok(response) = request.send().await else {
        return Probe::Inconclusive;
    };
    match classify_status(response.status().as_u16()) {
        PollStatus::Ok => match response.json::<serde_json::Value>().await {
            Ok(body) => Probe::Answered(body),
            // A 2xx that will not parse is not an answer and not a denial.
            Err(_) => Probe::Inconclusive,
        },
        PollStatus::Denied => Probe::Denied,
        PollStatus::Transient => Probe::Transient,
        PollStatus::Unknown => Probe::Inconclusive,
    }
}

/// `GET {base}/key/info` — the key's own spend/budget.
///
/// Effective spend = the documented nested `info.spend`, falling back to the
/// modern top-level `spend`; max budget = the top-level `max_budget`. A denial
/// records the `key_info` hop straight away (this hop has no fallback).
pub async fn poll_key_info(
    client: &reqwest::Client,
    base_url: &str,
    bearer: &str,
    memory: &mut DenialMemory,
) -> Option<KeyBudget> {
    if !should_probe(memory, base_url, BudgetEndpoint::KeyInfo) {
        return None;
    }
    let url = format!("{}/key/info", management_root(base_url));
    match probe(client, &url, bearer, &[]).await {
        Probe::Answered(body) => {
            let parsed: KeyInfoResponse = serde_json::from_value(body).ok()?;
            let spend = parsed
                .info
                .as_ref()
                .and_then(|info| info.spend)
                .or(parsed.spend);
            Some(KeyBudget {
                spend,
                max_budget: parsed.max_budget,
                remaining: remaining_of(spend, parsed.max_budget),
                budget_reset_at: parsed.budget_reset_at,
            })
        }
        Probe::Denied => {
            memory.deny(base_url, BudgetEndpoint::KeyInfo);
            None
        }
        Probe::Transient | Probe::Inconclusive => None,
    }
}

/// `GET {base}/v2/user/info`, falling back to `GET {base}/user/info`.
///
/// The fallback ordering rule (made explicit because a 500 could go either
/// way): a denial-class status on `/v2` is a fallback trigger FIRST. A 2xx from
/// the fallback serves this tick and records NO memory at all — a hop that just
/// answered is never in the scoreboard. The `user_info` entry is written only
/// when the fallback ALSO fails denied-or-5xx, and it is keyed to the `user_info`
/// hop alone (which suppresses both shapes, since the fallback is only ever
/// reached through the primary). A 429 on `/v2` is transient and takes NO
/// fallback (it is a rate limit on that endpoint, not evidence it is absent).
pub async fn poll_user_info(
    client: &reqwest::Client,
    base_url: &str,
    bearer: &str,
    memory: &mut DenialMemory,
) -> Option<UserBudget> {
    if !should_probe(memory, base_url, BudgetEndpoint::UserInfo) {
        return None;
    }
    let root = management_root(base_url);
    let primary = format!("{root}/v2/user/info");
    match probe(client, &primary, bearer, &[]).await {
        Probe::Answered(body) => user_budget_from(body),
        Probe::Transient | Probe::Inconclusive => None,
        Probe::Denied => {
            let fallback = format!("{root}/user/info");
            match probe(client, &fallback, bearer, &[]).await {
                // The fallback answered: serve it, remember nothing (the `/v2`
                // hop is merely absent here, which the fallback already papers
                // over — and a memory entry would silence the pair for an
                // interval for nothing).
                Probe::Answered(body) => user_budget_from(body),
                // BOTH hops failed denied-class (or the fallback denied and the
                // primary already did): only now is the hop worth quieting.
                Probe::Denied => {
                    memory.deny(base_url, BudgetEndpoint::UserInfo);
                    None
                }
                Probe::Transient | Probe::Inconclusive => None,
            }
        }
    }
}

fn user_budget_from(body: serde_json::Value) -> Option<UserBudget> {
    let parsed: UserInfoResponse = serde_json::from_value(body).ok()?;
    let spend = parsed
        .spend
        .or_else(|| parsed.user_info.as_ref().and_then(|nested| nested.spend));
    let team_ids = parsed
        .teams
        .iter()
        .filter_map(|row| row.team_id.clone())
        .collect();
    Some(UserBudget {
        user_id: parsed.user_id,
        spend,
        max_budget: parsed.max_budget,
        remaining: remaining_of(spend, parsed.max_budget),
        budget_reset_at: parsed.budget_reset_at,
        team_ids,
    })
}

/// `GET {base}/team/info?team_id={id}` — one team's SHARED budget.
pub async fn poll_team_info(
    client: &reqwest::Client,
    base_url: &str,
    bearer: &str,
    team_id: &str,
    memory: &mut DenialMemory,
) -> Option<TeamBudget> {
    if !should_probe(memory, base_url, BudgetEndpoint::TeamInfo) {
        return None;
    }
    let url = format!("{}/team/info", management_root(base_url));
    let query = [("team_id", team_id.to_string())];
    match probe(client, &url, bearer, &query).await {
        Probe::Answered(body) => {
            let parsed: TeamInfoResponse = serde_json::from_value(body).ok()?;
            Some(TeamBudget {
                // The response is authoritative for the id, but a proxy that
                // omits it must not lose the row it just answered for.
                team_id: Some(
                    parsed
                        .team_id
                        .clone()
                        .unwrap_or_else(|| team_id.to_string()),
                ),
                team_alias: parsed.team_alias,
                spend: parsed.spend,
                max_budget: parsed.max_budget,
                remaining: remaining_of(parsed.spend, parsed.max_budget),
                budget_reset_at: parsed.budget_reset_at,
            })
        }
        Probe::Denied => {
            memory.deny(base_url, BudgetEndpoint::TeamInfo);
            None
        }
        Probe::Transient | Probe::Inconclusive => None,
    }
}

/// One provider's budget tick: probe the entitled hops, collect the facts.
///
/// Order matters: the user hop names the team ids the team fan-out runs on, so
/// a denied user hop means NO team hops this tick (nothing was named). Per
/// hop: a denial-class verdict enters `deny_memory`, a 429 does NOT, a transport
/// failure does NOT. Never `Err` — a failed poll is a `None` fact, never a
/// panicking tick, and never a log line from here (the caller logs the
/// newly-recorded denials, so a suppressed hop logs nothing at all).
pub async fn poll_all(
    client: &reqwest::Client,
    base_url: &str,
    bearer: &str,
    deny_memory: &mut DenialMemory,
) -> BudgetFacts {
    let key = poll_key_info(client, base_url, bearer, deny_memory).await;
    let user = poll_user_info(client, base_url, bearer, deny_memory).await;
    let mut teams = Vec::new();
    if let Some(user) = &user {
        for team_id in &user.team_ids {
            // The team hop shares the scoreboard (one `TeamInfo` entry per
            // base): a proxy that refuses `/team/info` refuses it for every id,
            // so the second id is not worth a request either.
            if let Some(team) = poll_team_info(client, base_url, bearer, team_id, deny_memory).await
            {
                teams.push(team);
            }
        }
    }
    BudgetFacts { key, user, teams }
}

// ── the shared ledger (the loop's add-on writes it, the command reads it) ───

/// The per-provider budget ledger: ONE shared view of the latest facts and ONE
/// denial memory, owned as `Arc<Mutex<BudgetPoller>>` managed state so the
/// refresher loop's add-on and the `get_provider_budget` command cannot drift
/// apart into two scoreboards.
///
/// Deliberately NOT a credential: budget/quota is POLLING STATE, so it lives
/// beside the session manager as managed state and never touches the
/// `provider-auth.json` store (no third `ProviderCredential` variant — that
/// enum is the credential surface, not a facts cache).
pub struct BudgetPoller {
    /// The settings home the provider rows are re-read from at each arm.
    config_dir: PathBuf,
    /// The credential-store path T1's resolver reads (ruling R3: the DATA dir)
    /// — carried so the tick resolves through `resolve_bearer_at` and never
    /// invents a path of its own.
    auth_file: PathBuf,
    /// The injectable cadence (production [`BUDGET_POLL_INTERVAL_MILLIS`]).
    interval_millis: u64,
    /// The provider rows to poll, refreshed at every arm (a provider added on
    /// the Settings page appears within one interval).
    providers: Vec<crate::config::ProviderConfig>,
    /// The latest facts by provider id (the DTO command's source).
    facts: BTreeMap<String, BudgetFacts>,
    /// The denial scoreboard (shared with the command's on-demand path).
    denial_memory: DenialMemory,
    /// The last arm instant (epoch millis) — the interval re-arm's anchor.
    last_arm_millis: i64,
    /// The bearer each provider was last polled with, so a rotation can clear
    /// that provider's denials (a rotated key may hold new entitlements).
    bearers: HashMap<String, String>,
}

impl std::fmt::Debug for BudgetPoller {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The bearer map is deliberately NOT printed (it holds tokens).
        f.debug_struct("BudgetPoller")
            .field("config_dir", &self.config_dir)
            .field("interval_millis", &self.interval_millis)
            .field("providers", &self.providers.len())
            .field("facts", &self.facts.len())
            .field("denial_memory", &self.denial_memory)
            .finish()
    }
}

impl Default for BudgetPoller {
    fn default() -> Self {
        Self::new(
            PathBuf::from("."),
            PathBuf::from("provider-auth.json"),
            BUDGET_POLL_INTERVAL_MILLIS,
        )
    }
}

impl BudgetPoller {
    /// The production constructor: the settings home + the credential-store
    /// path (both injected — never resolved from inside, so no test can reach
    /// the real data dir) + the default cadence.
    pub fn new(config_dir: PathBuf, auth_file: PathBuf, interval_millis: u64) -> Self {
        Self {
            config_dir,
            auth_file,
            interval_millis,
            providers: Vec::new(),
            facts: BTreeMap::new(),
            denial_memory: DenialMemory::default(),
            // `0` = "never armed", so the FIRST tick is always due (the app
            // should show a budget as soon as the loop's first wake happens).
            last_arm_millis: 0,
            bearers: HashMap::new(),
        }
    }

    /// The provider rows this poller polls (the arm-time snapshot).
    pub fn providers(&self) -> &[crate::config::ProviderConfig] {
        &self.providers
    }

    /// The provider ids currently polled (in settings order).
    pub fn provider_ids(&self) -> Vec<String> {
        self.providers.iter().map(|p| p.id.clone()).collect()
    }

    /// The latest facts for one provider (`None` = never polled).
    pub fn facts(&self, provider_id: &str) -> Option<&BudgetFacts> {
        self.facts.get(provider_id)
    }

    /// Replace one provider's facts (the tick's write-back).
    pub fn store_facts(&mut self, provider_id: &str, facts: BudgetFacts) {
        self.facts.insert(provider_id.to_string(), facts);
    }

    /// The denial scoreboard (read-only view for tests and the loop's guard).
    pub fn denial_memory(&self) -> &DenialMemory {
        &self.denial_memory
    }

    /// Mutable access to the scoreboard (the command's on-demand path).
    pub fn denial_memory_mut(&mut self) -> &mut DenialMemory {
        &mut self.denial_memory
    }

    /// The credential-store path the tick resolves bearers against.
    pub fn auth_file(&self) -> &PathBuf {
        &self.auth_file
    }

    /// The settings home the rows are re-read from.
    pub fn config_dir(&self) -> &PathBuf {
        &self.config_dir
    }

    /// The configured cadence.
    pub fn interval_millis(&self) -> u64 {
        self.interval_millis
    }

    /// How long until the next arm is due (`None` = nothing to poll at all, so
    /// the loop must NOT shorten its sleep for the budget work).
    ///
    /// The "no providers" case is the load-bearing one for the sleep math: a
    /// poller with nothing to poll must never turn the refresher's 60 s cap
    /// into a 15 s wake.
    pub fn millis_until_next_arm(&self, now: i64) -> Option<u64> {
        if self.providers.is_empty() {
            return None;
        }
        let elapsed = now.saturating_sub(self.last_arm_millis).max(0) as u64;
        Some(self.interval_millis.saturating_sub(elapsed))
    }

    /// The interval re-arm: re-read the provider rows from settings and, when
    /// the interval has elapsed, FORGET every denial (so a proxy that granted
    /// an entitlement mid-interval is heard again) and return `true` = "poll
    /// now". `false` means the interval has not elapsed: the caller skips the
    /// whole budget pass.
    ///
    /// The clock is a PARAMETER (injectable), so a cadence test drives this
    /// with explicit instants instead of waiting on a real interval.
    pub fn arm_tick(&mut self, now: i64) -> bool {
        self.reload_providers();
        let due = now.saturating_sub(self.last_arm_millis) as u64 >= self.interval_millis;
        if due {
            self.last_arm_millis = now;
            self.denial_memory.clear_all();
        }
        due
    }

    /// Re-read the provider rows from `settings.json` (the staleness rule: a
    /// provider added on the Settings page appears within one interval, and an
    /// on-demand refresh sees it immediately).
    pub fn reload_providers(&mut self) {
        let settings = load_settings(&self.config_dir);
        // Only LiteLLM rows are polled: `/key/info`, `/v2/user/info` and
        // `/team/info` are LITELLM management routes. Probing them on
        // `api.openai.com` would be a request to an unrelated endpoint (and the
        // 404 it returns would then sit in the denial scoreboard for nothing).
        self.providers = settings
            .providers
            .into_iter()
            .filter(|p| p.api == "litellm")
            .collect();
    }

    /// The bearer-change invalidation: remember the bearer one provider was
    /// polled with and clear that provider's denials when it CHANGED (a rotated
    /// key may read endpoints the old one could not). `true` = it changed.
    pub fn note_bearer(&mut self, provider_id: &str, base_url: &str, bearer: &str) -> bool {
        let changed = self
            .bearers
            .get(provider_id)
            .is_some_and(|previous| previous != bearer);
        if changed {
            self.denial_memory.clear_provider(base_url);
        }
        self.bearers
            .insert(provider_id.to_string(), bearer.to_string());
        changed
    }
}

/// One budget pass over every provider the poller knows — the refresher loop's
/// add-on body, and the `get_provider_budget` command's on-demand path.
///
/// * `force = None` — the loop's cadence pass: gated by
///   [`BudgetPoller::arm_tick`] (the denial re-arm), so a forbidden endpoint is
///   probed at most once per interval.
/// * `force = Some(provider_id)` — the command's refresh affordance: one poll
///   of that provider regardless of the interval (the user clicked "refresh").
/// * `execute_helpers` — the interface §4 seeding gate: the command's path
///   passes `false` so a budget read never spawns a `!command` key helper; the
///   background loop passes `true` (it is not a seed, and a helper-keyed row
///   otherwise loses its budget entirely).
///
/// NEVER holds the `Mutex` guard across an `await`: the work list and a CLONE of
/// the denial memory are taken under the lock, every HTTP hop runs unlocked, and
/// the results (facts + the clone's denials, merged) are written back under a
/// fresh lock.
pub async fn run_budget_tick(
    poller: &Arc<Mutex<BudgetPoller>>,
    client: &reqwest::Client,
    force: Option<&str>,
    execute_helpers: bool,
) {
    let now = store::now_epoch_millis();
    let (providers, config_dir, auth_file, mut memory) = {
        let Ok(mut guard) = poller.lock() else {
            return;
        };
        match force {
            // The user asked for THIS provider right now: re-read the rows (a
            // row saved seconds ago must be pollable immediately) and forget
            // THAT provider's denials — an explicit refresh is the one moment a
            // suppressed hop is worth re-probing, and it is the user's click,
            // not the loop's spam. The scoreboard's other providers are kept, so
            // refreshing one row cannot re-open another row's forbidden hops.
            Some(provider_id) => {
                guard.reload_providers();
                let base = guard
                    .providers()
                    .iter()
                    .find(|p| p.id == provider_id)
                    .map(|p| p.base_url.clone());
                if let Some(base) = base {
                    guard.denial_memory_mut().clear_provider(&base);
                }
            }
            // The loop's cadence pass: gated by the interval, and the gate's
            // other half is the denial re-arm.
            None if !guard.arm_tick(now) => return,
            None => {}
        }
        (
            guard.providers().to_vec(),
            guard.config_dir().clone(),
            guard.auth_file().clone(),
            guard.denial_memory().clone(),
        )
    };

    for provider in providers {
        if let Some(only) = force {
            if provider.id != only {
                continue;
            }
        }
        // The bearer comes from T1's resolver — this task re-resolves NO
        // credential independently (interface §4). `None` means "the row's
        // static key IS the bearer"; an empty key means there is nothing to
        // authenticate with, so the provider is skipped (no requests, no
        // denials).
        // `resolve_bearer_at` is T1's path-taking core (the documented tested
        // seam) and the SAME body the production `resolve_bearer` alias calls —
        // the poller supplies the store path it was CONSTRUCTED with, so a test
        // poller can never read the real DATA-dir store.
        let bearer =
            match resolve_bearer_at(&provider, &auth_file, &config_dir, client, execute_helpers)
                .await
            {
                Some(token) => token,
                None => provider.api_key.clone(),
            };
        if bearer.is_empty() {
            debuglog::log(&format!(
                "budget: provider `{}` has no bearer — skipping the budget poll",
                provider.id
            ));
            continue;
        }
        let before = memory.clone();
        let facts = poll_all(client, &provider.base_url, &bearer, &mut memory).await;
        // Log ONLY the denials THIS tick recorded (the suppression's other
        // half: a hop already in the scoreboard is skipped before the request,
        // so it produces neither a request nor a line).
        for endpoint in BudgetEndpoint::ALL {
            if !before.is_denied(&provider.base_url, endpoint)
                && memory.is_denied(&provider.base_url, endpoint)
            {
                debuglog::log(&format!(
                    "budget: provider `{}` {} denied — suppressing until the next poll \
                     interval or a token rotation",
                    provider.id,
                    endpoint.path()
                ));
            }
        }
        let mut guard = match poller.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        // A rotated bearer means the scoreboard's assumptions are stale: drop
        // this provider's entries BEFORE merging this tick's verdicts (so the
        // rotation costs one quiet re-probe, not a permanent silence).
        if guard.note_bearer(&provider.id, &provider.base_url, &bearer) {
            memory.clear_provider(&provider.base_url);
        }
        guard.store_facts(&provider.id, facts);
        guard.denial_memory_mut().merge_from(&memory);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── the classification pin ──────────────────────────────────────────────

    #[test]
    fn classify_status_maps_4xx_denied_except_429() {
        // 4xx except 429 = "this key cannot read this endpoint" → remembered.
        for status in [400u16, 401, 403, 404, 405, 418, 451] {
            assert_eq!(
                classify_status(status),
                PollStatus::Denied,
                "{status} is a denial"
            );
        }
        // Any 5xx is remembered too (a broken endpoint is not worth a request
        // per interval either).
        for status in [500u16, 502, 503] {
            assert_eq!(classify_status(status), PollStatus::Denied, "{status}");
        }
        // THE pin: 429 is transient, NEVER a denial.
        assert_eq!(classify_status(429), PollStatus::Transient);
        // 2xx answers, anything else is neither.
        for status in [200u16, 201, 299] {
            assert_eq!(classify_status(status), PollStatus::Ok, "{status}");
        }
        for status in [100u16, 301, 302] {
            assert_eq!(classify_status(status), PollStatus::Unknown, "{status}");
        }
    }

    // ── the scoreboard ──────────────────────────────────────────────────────

    #[test]
    fn denial_memory_denies_and_clears_per_base_url() {
        let mut memory = DenialMemory::default();
        assert!(memory.is_empty(), "a fresh scoreboard is empty");
        memory.deny("https://a.example/v1", BudgetEndpoint::KeyInfo);
        assert!(!memory.is_empty());
        // A second provider shares NOTHING: the same hop on another base is
        // still probed.
        assert!(should_probe(
            &memory,
            "https://b.example/v1",
            BudgetEndpoint::KeyInfo
        ));
        assert!(should_probe(
            &memory,
            "https://a.example/v1",
            BudgetEndpoint::UserInfo
        ));
        assert!(!should_probe(
            &memory,
            "https://a.example/v1",
            BudgetEndpoint::KeyInfo
        ));
        // `clear_provider` forgets ONE base and leaves the other untouched.
        memory.deny("https://b.example/v1", BudgetEndpoint::UserInfo);
        memory.clear_provider("https://a.example/v1");
        assert!(should_probe(
            &memory,
            "https://a.example/v1",
            BudgetEndpoint::KeyInfo
        ));
        assert!(!should_probe(
            &memory,
            "https://b.example/v1",
            BudgetEndpoint::UserInfo
        ));
        // `clear_all` (the interval re-arm) empties the scoreboard.
        memory.clear_all();
        assert!(memory.is_empty(), "the re-arm forgets every denial");
    }

    #[test]
    fn denial_key_is_base_url_plus_endpoint() {
        // The load-bearing per-ENDPOINT rule: one absent endpoint must not
        // disable the others on the SAME base, and the same endpoint on a
        // different base is untouched.
        let mut memory = DenialMemory::default();
        memory.deny("https://gw.example/v1", BudgetEndpoint::UserInfo);
        for endpoint in BudgetEndpoint::ALL {
            let denied = memory.is_denied("https://gw.example/v1", endpoint);
            assert_eq!(denied, endpoint == BudgetEndpoint::UserInfo, "{endpoint:?}");
        }
        assert!(should_probe(
            &memory,
            "https://other.example/v1",
            BudgetEndpoint::UserInfo
        ));
    }

    #[test]
    fn denial_memory_clears_when_the_bearer_changes() {
        // A rotated key may hold NEW entitlements, so the rotation forgets that
        // provider's denials (one quiet re-probe per rotation beats a wrong but
        // silent `None`). Direct unit of the memory + the poller's bearer
        // bookkeeping — no server involved.
        let mut memory = DenialMemory::default();
        memory.deny("https://gw.example/v1", BudgetEndpoint::KeyInfo);
        memory.deny("https://other.example/v1", BudgetEndpoint::KeyInfo);
        memory.clear_provider("https://gw.example/v1");
        assert!(!memory.is_denied("https://gw.example/v1", BudgetEndpoint::KeyInfo));
        assert!(memory.is_denied("https://other.example/v1", BudgetEndpoint::KeyInfo));

        // And the poller's side: the FIRST bearer seen is not a change (nothing
        // to forget), an identical re-read is not a change, a different token
        // IS — and it clears exactly the rotated provider's entries.
        let dir = crate::test_support::ScratchDir::in_env_temp("provider-budget-", "bearer");
        let mut poller = BudgetPoller::new(
            dir.path().to_path_buf(),
            dir.path().join("provider-auth.json"),
            BUDGET_POLL_INTERVAL_MILLIS,
        );
        let mut merged = DenialMemory::default();
        merged.deny("https://gw.example/v1", BudgetEndpoint::KeyInfo);
        merged.deny("https://other.example/v1", BudgetEndpoint::KeyInfo);
        poller.denial_memory_mut().merge_from(&merged);
        assert!(!poller.note_bearer("gw", "https://gw.example/v1", "token-1"));
        assert!(
            poller
                .denial_memory()
                .is_denied("https://gw.example/v1", BudgetEndpoint::KeyInfo),
            "the first bearer is not a rotation"
        );
        assert!(!poller.note_bearer("gw", "https://gw.example/v1", "token-1"));
        assert!(poller.note_bearer("gw", "https://gw.example/v1", "token-2"));
        assert!(
            !poller
                .denial_memory()
                .is_denied("https://gw.example/v1", BudgetEndpoint::KeyInfo),
            "the rotated bearer forgot the old key's denials"
        );
        assert!(
            poller
                .denial_memory()
                .is_denied("https://other.example/v1", BudgetEndpoint::KeyInfo),
            "and no other provider lost anything"
        );
    }

    #[test]
    fn remaining_is_none_when_budget_is_absent_or_zero() {
        // An unlimited key carries NO `remaining` (inventing a cap would render
        // a fake limit next to a real spend).
        assert_eq!(remaining_of(Some(3.0), None), None);
        assert_eq!(remaining_of(Some(3.0), Some(0.0)), None);
        assert_eq!(remaining_of(None, Some(10.0)), None);
        assert_eq!(remaining_of(Some(3.0), Some(10.0)), Some(7.0));
        // A spend over budget is a NEGATIVE remaining, not a hidden row (the
        // UI shows an overrun; `moneyOf` renders the sign).
        assert_eq!(remaining_of(Some(12.0), Some(10.0)), Some(-2.0));
    }

    #[tokio::test]
    async fn user_info_fallback_does_not_triple_fetch() {
        // The fallback fires ONCE per call: `/v2` denied → `/user/info` once,
        // and the result is served with NO memory entry (a hop that just
        // answered is never in the scoreboard).
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v2/user/info"))
            .respond_with(
                ResponseTemplate::new(404)
                    .set_body_json(serde_json::json!({"detail": "Not Found"})),
            )
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/user/info"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({"user_id": "u1", "spend": 1.5})),
            )
            .mount(&server)
            .await;

        // The base is the server root VERBATIM (the module's root rule): the
        // request paths below are therefore the mounted ones.
        let base = server.uri();
        let mut memory = DenialMemory::default();
        let facts = poll_user_info(&reqwest::Client::new(), &base, "k", &mut memory)
            .await
            .expect("the fallback serves the tick");
        assert_eq!(facts.user_id.as_deref(), Some("u1"));
        assert!(
            memory.is_empty(),
            "a hop that served a 2xx records no denial"
        );
        let requests = server.received_requests().await.expect("recorded");
        assert_eq!(requests.len(), 2, "exactly two hops, never three");
    }
}

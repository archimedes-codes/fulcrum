//! Configuration for web tools (`web_search` and `fetch_content`).
//!
//! (Fulcrum T5/S2) This module used to carry a SECOND `settings.json` reader,
//! [`WebConfig::from_web_settings`]'s disk-reading sibling `from_settings`. It is
//! GONE: `fulcrum-core` reads no desktop config file (T3's invariant — the crate
//! contains zero `load_settings`), and every call site of `from_settings` was in
//! this file's own `#[cfg(test)]` block, so no production path lost an entry.
//! [`WebConfig::from_web_settings`] is the one entry, fed by `StartEnv.web` /
//! the `config-update` push on the Worker and by the Supervisor's already-loaded
//! `Settings::web` in `commands/settings.rs`. The tests below now drive THAT
//! entry directly with an in-memory `WebSettings`, so the merge semantics the
//! disk-reading fixtures guarded are still guarded — minus the file.

use serde::{Deserialize, Serialize};

/// One Search backend `web_search` can query. Always referred to as a
/// "Search provider"; NEVER just "provider" — bare Provider is the model
/// endpoint (`CONTEXT.md`).
pub const SEARCH_PROVIDERS: [&str; 5] = ["brave", "tavily", "perplexity", "searxng", "duckduckgo"];

/// The `web_search` / `fetch_content` configuration (the settings' `web`
/// block). Every field is optional; an unset key means "this Search provider
/// is not available" (or, for `defaultProvider`/`proxy`, "auto"/"none").
///
/// (Fulcrum T5/S2, plan §2c) A pure value DTO, so it lives HERE and desktop
/// `config.rs` re-exports it back: `Settings::web` is typed by it, and the
/// `StartEnv.web` / `config-update` envelope carries it. What did NOT move is
/// `load_settings` — that is the T3 disk boundary, and the crate reads no
/// settings file. The direction is desktop → crate for a TYPE, which the package
/// rule allows (the crate never names a desktop module).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase", default)]
pub struct WebSettings {
    pub brave_api_key: String,
    pub tavily_api_key: String,
    pub perplexity_api_key: String,
    /// The self-hosted SearXNG instance base URL.
    pub searxng_url: String,
    /// `"auto"` (default) or one of `brave|tavily|perplexity|searxng|duckduckgo`.
    pub default_provider: String,
    /// An explicit proxy URL. Empty = fall back to `HTTPS_PROXY`/`HTTP_PROXY`.
    pub proxy: String,
}

/// The web tools' resolved configuration for this turn: the settings' `web`
/// block with the environment layered OVER it (the reference's precedence,
/// so CI/headless works with no UI).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct WebConfig {
    pub brave_api_key: Option<String>,
    pub tavily_api_key: Option<String>,
    pub perplexity_api_key: Option<String>,
    pub searxng_url: Option<String>,
    /// `"auto"` or one of `SEARCH_PROVIDERS`.
    pub default_provider: String,
    pub proxy: Option<String>,
}

fn non_empty_str(s: &str) -> Option<String> {
    let trimmed = s.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

fn non_empty_env(key: &str) -> Option<String> {
    std::env::var(key).ok().and_then(|v| non_empty_str(&v))
}

impl WebConfig {
    /// Read + merge from an already-loaded `WebSettings` block — the entry the
    /// Worker's per-turn `PermissionContext::refresh` uses (Fulcrum T3), fed by
    /// `StartEnv.web` and overwritten by a `config-update` push, and the entry
    /// the Supervisor's `test_search_provider` uses with the `web` block the UI
    /// handed it. The ONE entry: this module reads no settings file (T5/S2 —
    /// see the module docs).
    pub fn from_web_settings(web: Option<&WebSettings>) -> Self {
        let default_provider = web
            .map(|w| w.default_provider.trim())
            .filter(|dp| SEARCH_PROVIDERS.contains(dp) || *dp == "auto")
            .unwrap_or("auto")
            .to_string();

        Self {
            brave_api_key: non_empty_env("BRAVE_API_KEY")
                .or_else(|| web.and_then(|w| non_empty_str(&w.brave_api_key))),
            tavily_api_key: non_empty_env("TAVILY_API_KEY")
                .or_else(|| web.and_then(|w| non_empty_str(&w.tavily_api_key))),
            perplexity_api_key: non_empty_env("PERPLEXITY_API_KEY")
                .or_else(|| web.and_then(|w| non_empty_str(&w.perplexity_api_key))),
            searxng_url: non_empty_env("SEARXNG_URL")
                .or_else(|| web.and_then(|w| non_empty_str(&w.searxng_url))),
            default_provider,
            proxy: non_empty_env("HTTPS_PROXY")
                .or_else(|| non_empty_env("HTTP_PROXY"))
                .or_else(|| web.and_then(|w| non_empty_str(&w.proxy))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct RestoreEnv(&'static str, Option<std::ffi::OsString>);
    impl Drop for RestoreEnv {
        fn drop(&mut self) {
            match self.1.take() {
                // SAFETY: ENV_LOCK is held while restoring the env var.
                Some(v) => unsafe { std::env::set_var(self.0, v) },
                None => unsafe { std::env::remove_var(self.0) },
            }
        }
    }

    fn set_env(key: &'static str, val: &str) -> RestoreEnv {
        let prev = std::env::var_os(key);
        // SAFETY: ENV_LOCK is held by caller.
        unsafe { std::env::set_var(key, val) };
        RestoreEnv(key, prev)
    }

    fn remove_env(key: &'static str) -> RestoreEnv {
        let prev = std::env::var_os(key);
        // SAFETY: ENV_LOCK is held by caller.
        unsafe { std::env::remove_var(key) };
        RestoreEnv(key, prev)
    }

    /// Clear every env var `from_web_settings` layers over the settings block,
    /// so a developer's exported `BRAVE_API_KEY` cannot decide an assertion that
    /// is about the SETTINGS half. Held for the whole test (the `RestoreEnv`
    /// guards restore on drop).
    fn clear_web_env() -> Vec<RestoreEnv> {
        vec![
            remove_env("BRAVE_API_KEY"),
            remove_env("TAVILY_API_KEY"),
            remove_env("PERPLEXITY_API_KEY"),
            remove_env("SEARXNG_URL"),
            remove_env("HTTPS_PROXY"),
            remove_env("HTTP_PROXY"),
        ]
    }

    #[test]
    fn settings_fill_the_config() {
        let _lock = crate::test_support::env_lock();
        let _env = clear_web_env();

        let web = WebSettings {
            brave_api_key: "b-key".to_string(),
            tavily_api_key: "t-key".to_string(),
            perplexity_api_key: "p-key".to_string(),
            searxng_url: "https://searx.local".to_string(),
            default_provider: "tavily".to_string(),
            proxy: "http://proxy.local:8080".to_string(),
        };

        let cfg = WebConfig::from_web_settings(Some(&web));
        assert_eq!(cfg.brave_api_key.as_deref(), Some("b-key"));
        assert_eq!(cfg.tavily_api_key.as_deref(), Some("t-key"));
        assert_eq!(cfg.perplexity_api_key.as_deref(), Some("p-key"));
        assert_eq!(cfg.searxng_url.as_deref(), Some("https://searx.local"));
        assert_eq!(cfg.default_provider, "tavily");
        assert_eq!(cfg.proxy.as_deref(), Some("http://proxy.local:8080"));
    }

    #[test]
    fn env_wins_over_settings() {
        let _lock = crate::test_support::env_lock();
        let _r1 = set_env("BRAVE_API_KEY", "env-brave");
        let _r2 = set_env("TAVILY_API_KEY", "env-tavily");
        let _r3 = set_env("PERPLEXITY_API_KEY", "env-pplx");
        let _r4 = set_env("SEARXNG_URL", "https://env-searx.local");
        let _r5 = set_env("HTTPS_PROXY", "http://env-proxy:8080");
        let _r6 = remove_env("HTTP_PROXY");

        let web = WebSettings {
            brave_api_key: "settings-brave".to_string(),
            tavily_api_key: "settings-tavily".to_string(),
            perplexity_api_key: "settings-pplx".to_string(),
            searxng_url: "https://settings-searx.local".to_string(),
            default_provider: "duckduckgo".to_string(),
            proxy: "http://settings-proxy:8080".to_string(),
        };

        let cfg = WebConfig::from_web_settings(Some(&web));
        assert_eq!(cfg.brave_api_key.as_deref(), Some("env-brave"));
        assert_eq!(cfg.tavily_api_key.as_deref(), Some("env-tavily"));
        assert_eq!(cfg.perplexity_api_key.as_deref(), Some("env-pplx"));
        assert_eq!(cfg.searxng_url.as_deref(), Some("https://env-searx.local"));
        assert_eq!(cfg.proxy.as_deref(), Some("http://env-proxy:8080"));
        // default_provider has no env var, so settings value is used
        assert_eq!(cfg.default_provider, "duckduckgo");
    }

    #[test]
    fn blank_settings_count_as_unset() {
        let _lock = crate::test_support::env_lock();
        let _env = clear_web_env();

        // The blank-normalization habit, once reached through `load_settings`
        // (which normalized blanks on read); now asserted on the merge itself,
        // which is where the trim actually lives.
        let web = WebSettings {
            brave_api_key: "   ".to_string(),
            tavily_api_key: "\t\n".to_string(),
            perplexity_api_key: "".to_string(),
            searxng_url: "  ".to_string(),
            default_provider: "   ".to_string(),
            proxy: "\t ".to_string(),
        };

        let cfg = WebConfig::from_web_settings(Some(&web));
        assert_eq!(cfg.brave_api_key, None);
        assert_eq!(cfg.tavily_api_key, None);
        assert_eq!(cfg.perplexity_api_key, None);
        assert_eq!(cfg.searxng_url, None);
        assert_eq!(cfg.default_provider, "auto");
        assert_eq!(cfg.proxy, None);
    }

    #[test]
    fn unknown_default_provider_falls_back_to_auto() {
        let _lock = crate::test_support::env_lock();
        let _env = clear_web_env();

        // Typos / unknown names degrade to "auto"
        let mut web = WebSettings {
            default_provider: "bing".to_string(),
            ..WebSettings::default()
        };
        let cfg = WebConfig::from_web_settings(Some(&web));
        assert_eq!(cfg.default_provider, "auto");

        // Explicit "auto" stays "auto"
        web.default_provider = "auto".to_string();
        let cfg = WebConfig::from_web_settings(Some(&web));
        assert_eq!(cfg.default_provider, "auto");

        // Known search providers are preserved
        for provider in SEARCH_PROVIDERS {
            web.default_provider = provider.to_string();
            let cfg = WebConfig::from_web_settings(Some(&web));
            assert_eq!(cfg.default_provider, provider);
        }
    }

    #[test]
    fn proxy_prefers_https_proxy_over_http_proxy() {
        let _lock = crate::test_support::env_lock();

        // A settings block carrying a proxy, for the arms where no env var speaks.
        let web = WebSettings {
            proxy: "http://settings-proxy:3128".to_string(),
            ..WebSettings::default()
        };

        // 1. Both HTTPS_PROXY and HTTP_PROXY set -> HTTPS_PROXY wins
        {
            let _r1 = set_env("HTTPS_PROXY", "http://https-proxy:8080");
            let _r2 = set_env("HTTP_PROXY", "http://http-proxy:8080");
            let cfg = WebConfig::from_web_settings(Some(&web));
            assert_eq!(cfg.proxy.as_deref(), Some("http://https-proxy:8080"));
        }

        // 2. Only HTTP_PROXY set -> HTTP_PROXY wins
        {
            let _r1 = remove_env("HTTPS_PROXY");
            let _r2 = set_env("HTTP_PROXY", "http://http-proxy:8080");
            let cfg = WebConfig::from_web_settings(Some(&web));
            assert_eq!(cfg.proxy.as_deref(), Some("http://http-proxy:8080"));
        }

        // 3. Neither env set -> settings proxy wins
        {
            let _r1 = remove_env("HTTPS_PROXY");
            let _r2 = remove_env("HTTP_PROXY");
            let cfg = WebConfig::from_web_settings(Some(&web));
            assert_eq!(cfg.proxy.as_deref(), Some("http://settings-proxy:3128"));
        }

        // 4. Blank env var does not shadow settings proxy
        {
            let _r1 = set_env("HTTPS_PROXY", "   ");
            let _r2 = set_env("HTTP_PROXY", "");
            let cfg = WebConfig::from_web_settings(Some(&web));
            assert_eq!(cfg.proxy.as_deref(), Some("http://settings-proxy:3128"));
        }
    }
}

//! The Keenable Search provider.
//!
//! Structurally a clone of [`super::tavily`], with two deliberate deviations
//! from that sibling's conventions — both tested, neither accidental:
//!
//! 1. **`clean_snippet` is NOT used.** Keenable returns Markdown, and the
//!    shared helper collapses all whitespace, which destroys a Markdown table.
//!    The snippet is trimmed, and otherwise passed through verbatim.
//! 2. **`description` is NOT read.** Keenable's OpenAPI marks it required and
//!    its prose says it is always the empty string; the text lives in
//!    `snippet`. A port that copies `brave.rs`'s `description` read returns an
//!    empty snippet for every result.
//!
//! Unknown request parameters HARD-FAIL here (`400 unknown_parameter`), so the
//! body carries exactly `query` / `max_results` / `snippet_max_length` plus
//! `published_after` when the recency filter maps. Notably absent, and absent
//! on purpose: tavily's `include_answer`, and Keenable's `mode`, `session_id`
//! and `objective`.

use async_trait::async_trait;
use reqwest::header::{HeaderMap, HeaderValue, CONTENT_TYPE};
use serde::Deserialize;
use tokio_util::sync::CancellationToken;

use crate::web::config::WebConfig;
use crate::web::net::{safe_fetch, FetchOpts};
use crate::web::providers::common::{
    check_http_status, format_provider_error, resolve_num_results, truncate_results, validate_query,
};
use crate::web::providers::types::{SearchOpts, SearchProvider, SearchResultItem};

#[derive(Deserialize)]
struct KeenableResponse {
    results: Option<Vec<KeenableResultItem>>,
}

#[derive(Deserialize)]
struct KeenableResultItem {
    #[serde(default)]
    title: String,
    #[serde(default)]
    url: String,
    #[serde(default)]
    snippet: String,
    #[serde(default)]
    published_at: Option<String>,
}

pub struct KeenableProvider {
    pub base_url: Option<String>,
}

impl Default for KeenableProvider {
    fn default() -> Self {
        Self::new()
    }
}

impl KeenableProvider {
    pub fn new() -> Self {
        Self { base_url: None }
    }

    pub fn with_base_url(base_url: String) -> Self {
        Self {
            base_url: Some(base_url),
        }
    }
}

/// Maps the tool's `recency_filter` vocabulary onto Keenable's relative-window
/// strings. An unmapped value is dropped silently — the same shape `tavily.rs`
/// uses for `days`, and required here because an unrecognised parameter is a
/// hard 400 rather than something the API forgives.
fn recency_window(recency: &str) -> Option<&'static str> {
    match recency.to_ascii_lowercase().as_str() {
        "day" => Some("1d"),
        "week" => Some("7d"),
        "month" => Some("30d"),
        "year" => Some("1y"),
        _ => None,
    }
}

pub fn parse_keenable(body: &str) -> Result<Vec<SearchResultItem>, String> {
    let parsed: KeenableResponse = serde_json::from_str(body)
        .map_err(|e| format_provider_error("keenable", format!("failed to parse response: {e}")))?;

    let results = parsed
        .results
        .unwrap_or_default()
        .into_iter()
        .map(|item| SearchResultItem {
            title: item.title,
            url: item.url,
            // Deliberately NOT clean_snippet: Keenable's snippet is Markdown.
            snippet: item.snippet.trim().to_string(),
            published_date: item.published_at.filter(|s| !s.trim().is_empty()),
        })
        .collect();

    Ok(results)
}

#[async_trait]
impl SearchProvider for KeenableProvider {
    fn id(&self) -> &'static str {
        "keenable"
    }

    fn name(&self) -> &'static str {
        "Keenable"
    }

    fn is_available(&self, config: &WebConfig) -> bool {
        config.keenable_api_key.is_some()
    }

    async fn search(
        &self,
        query: &str,
        opts: &SearchOpts,
        config: &WebConfig,
        cancel: &CancellationToken,
    ) -> Result<Vec<SearchResultItem>, String> {
        let valid_query =
            validate_query(query).map_err(|e| format_provider_error("keenable", e))?;

        let api_key = config
            .keenable_api_key
            .as_deref()
            .ok_or_else(|| format_provider_error("keenable", "Keenable API Key not configured"))?;

        let count = resolve_num_results(opts.num_results, 5);
        let target_url = self
            .base_url
            .as_deref()
            .unwrap_or("https://api.keenable.ai/v1/search");

        let mut json_body = serde_json::json!({
            "query": valid_query,
            "max_results": count,
            "snippet_max_length": 1000,
        });

        if let Some(ref recency) = opts.recency_filter {
            if let Some(window) = recency_window(recency) {
                json_body["published_after"] = serde_json::json!(window);
            }
        }

        let body_bytes =
            serde_json::to_vec(&json_body).map_err(|e| format_provider_error("keenable", e))?;

        let mut headers = HeaderMap::new();
        // Enforced by the API: without CONTENT_TYPE the request is a
        // `400 invalid_request`, not a content-negotiation hint.
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        headers.insert(
            "X-API-Key",
            HeaderValue::from_str(api_key)
                .map_err(|_| format_provider_error("keenable", "invalid header value"))?,
        );

        let fetch_opts = FetchOpts {
            method: reqwest::Method::POST,
            headers,
            body: Some(body_bytes),
            proxy: opts.proxy.clone().or_else(|| config.proxy.clone()),
        };

        let outcome = safe_fetch(target_url, &fetch_opts, cancel)
            .await
            .map_err(|e| format_provider_error("keenable", e))?;

        check_http_status("keenable", outcome.status)?;

        let body_str = String::from_utf8_lossy(&outcome.body);
        let mut results = parse_keenable(&body_str)?;
        truncate_results(&mut results, opts.num_results);

        Ok(results)
    }
}

// TESTS — the implementation above is what the RED step was written against.

#[cfg(test)]
mod tests {
    use super::*;
    use crate::web::config::WebConfig;
    use crate::web::providers::types::SearchOpts;
    use tokio_util::sync::CancellationToken;

    #[test]
    fn keenable_is_available_flips_on_the_key() {
        let provider = KeenableProvider::new();
        let mut config = WebConfig::default();
        assert!(!provider.is_available(&config));

        config.keenable_api_key = Some("test-key".to_string());
        assert!(provider.is_available(&config));
    }

    #[tokio::test]
    async fn keenable_search_wiremock_success() {
        use wiremock::matchers::{body_json, header, method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let fixture = include_str!("../fixtures/keenable.json");
        let server = MockServer::start().await;

        // Exact body equality is how the ABSENCE of `mode` / `include_answer` /
        // `session_id` / `objective` is asserted: an unknown parameter is a
        // hard 400 on this API, so the request shape is a wire contract.
        Mock::given(method("POST"))
            .and(path("/v1/search"))
            .and(header("Content-Type", "application/json"))
            .and(header("X-API-Key", "test-key"))
            .and(body_json(serde_json::json!({
                "query": "rust",
                "max_results": 5,
                "snippet_max_length": 1000
            })))
            .respond_with(ResponseTemplate::new(200).set_body_string(fixture))
            .mount(&server)
            .await;

        let provider = KeenableProvider::with_base_url(format!("{}/v1/search", server.uri()));
        let cancel = CancellationToken::new();
        let config = WebConfig {
            keenable_api_key: Some("test-key".to_string()),
            ..Default::default()
        };
        let opts = SearchOpts::default();

        let results = provider
            .search("rust", &opts, &config, &cancel)
            .await
            .expect("keenable search should succeed");
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].title, "Rust Programming Language");
    }

    #[tokio::test]
    async fn keenable_search_wiremock_recency() {
        use wiremock::matchers::{body_json, method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let fixture = include_str!("../fixtures/keenable.json");
        let server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/v1/search"))
            .and(body_json(serde_json::json!({
                "query": "rust",
                "max_results": 5,
                "snippet_max_length": 1000,
                "published_after": "7d"
            })))
            .respond_with(ResponseTemplate::new(200).set_body_string(fixture))
            .mount(&server)
            .await;

        let provider = KeenableProvider::with_base_url(format!("{}/v1/search", server.uri()));
        let cancel = CancellationToken::new();
        let config = WebConfig {
            keenable_api_key: Some("test-key".to_string()),
            ..Default::default()
        };
        let opts = SearchOpts {
            recency_filter: Some("week".to_string()),
            ..Default::default()
        };

        let results = provider
            .search("rust", &opts, &config, &cancel)
            .await
            .expect("the mapped recency window should be accepted");
        assert_eq!(results.len(), 2);
    }

    #[tokio::test]
    async fn keenable_search_wiremock_unmapped_recency_omits_the_key() {
        use wiremock::matchers::{body_json, method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let fixture = include_str!("../fixtures/keenable.json");
        let server = MockServer::start().await;

        // Exact body equality again: an unmapped value must NOT put
        // `published_after` on the wire at all (the tavily silent-drop shape).
        Mock::given(method("POST"))
            .and(path("/v1/search"))
            .and(body_json(serde_json::json!({
                "query": "rust",
                "max_results": 5,
                "snippet_max_length": 1000
            })))
            .respond_with(ResponseTemplate::new(200).set_body_string(fixture))
            .mount(&server)
            .await;

        let provider = KeenableProvider::with_base_url(format!("{}/v1/search", server.uri()));
        let cancel = CancellationToken::new();
        let config = WebConfig {
            keenable_api_key: Some("test-key".to_string()),
            ..Default::default()
        };
        let opts = SearchOpts {
            recency_filter: Some("unseen".to_string()),
            ..Default::default()
        };

        let results = provider
            .search("rust", &opts, &config, &cancel)
            .await
            .expect("an unmapped recency value is dropped, not sent");
        assert_eq!(results.len(), 2);
    }

    #[tokio::test]
    async fn keenable_search_wiremock_error() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/v1/search"))
            .respond_with(ResponseTemplate::new(401).set_body_string("Unauthorized"))
            .mount(&server)
            .await;

        let provider = KeenableProvider::with_base_url(format!("{}/v1/search", server.uri()));
        let cancel = CancellationToken::new();
        let config = WebConfig {
            keenable_api_key: Some("test-key".to_string()),
            ..Default::default()
        };
        let opts = SearchOpts::default();

        let err = provider
            .search("rust", &opts, &config, &cancel)
            .await
            .unwrap_err();
        assert!(err.contains("[keenable] error: HTTP 401"), "got: {err}");
    }

    #[test]
    fn parse_keenable_fixture_reads_snippet_not_description() {
        let fixture = include_str!("../fixtures/keenable.json");
        let results = parse_keenable(fixture).expect("parsing keenable fixture should succeed");
        assert_eq!(results.len(), 2);

        assert_eq!(results[0].title, "Rust Programming Language");
        assert_eq!(results[0].url, "https://www.rust-lang.org/");
        assert_eq!(
            results[0].snippet,
            "A language empowering everyone to build reliable and efficient software."
        );
        assert_eq!(
            results[0].published_date.as_deref(),
            Some("2023-05-10T00:00:00Z")
        );

        assert_eq!(results[1].title, "Rust Crates");
        assert_eq!(results[1].url, "https://crates.io/");
        assert_eq!(results[1].snippet, "The Rust package registry");
        assert_eq!(results[1].published_date, None);

        // The description-vs-snippet regression guard: Keenable's `description`
        // is always "" — a port that reads it returns empty snippets.
        assert!(!results[0].snippet.is_empty());
        assert!(!results[1].snippet.is_empty());
    }

    #[test]
    fn parse_keenable_keeps_markdown_newlines() {
        // The clean_snippet deviation guard: collapse_whitespace would flatten
        // the markdown Keenable returns.
        let body = r#"{"results":[{"title":"Table","url":"https://example.com/","description":"","snippet":"| a | b |\n| - | - |\n| 1 | 2 |"}]}"#;
        let results = parse_keenable(body).expect("markdown snippet should parse");
        assert_eq!(results.len(), 1);
        assert!(
            results[0].snippet.contains("\n"),
            "markdown newlines must survive parsing, got: {:?}",
            results[0].snippet
        );
        assert!(results[0]
            .snippet
            .contains("| a | b |\n| - | - |\n| 1 | 2 |"));
    }

    #[test]
    fn parse_keenable_empty_results() {
        let json = r#"{"results": []}"#;
        let results = parse_keenable(json).expect("empty results array should parse to empty vec");
        assert_eq!(results, vec![]);

        let json_empty_obj = r#"{}"#;
        let results =
            parse_keenable(json_empty_obj).expect("empty object should parse to empty vec");
        assert_eq!(results, vec![]);
    }

    #[test]
    fn parse_keenable_malformed_json() {
        let err = parse_keenable("not json").unwrap_err();
        assert!(
            err.contains("[keenable]"),
            "error should mention provider name, got: {err}"
        );
    }
}

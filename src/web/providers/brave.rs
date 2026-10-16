use async_trait::async_trait;
use reqwest::header::{HeaderMap, HeaderValue, ACCEPT};
use serde::Deserialize;
use tokio_util::sync::CancellationToken;

use crate::agent::web::config::WebConfig;
use crate::agent::web::net::{safe_fetch, FetchOpts};
use crate::agent::web::providers::types::{SearchOpts, SearchProvider, SearchResultItem};

#[derive(Deserialize)]
struct BraveResponse {
    web: Option<BraveWeb>,
}

#[derive(Deserialize)]
struct BraveWeb {
    results: Option<Vec<BraveResultItem>>,
}

#[derive(Deserialize)]
struct BraveResultItem {
    #[serde(default)]
    title: String,
    #[serde(default)]
    url: String,
    #[serde(default)]
    description: String,
    #[serde(default)]
    age: Option<String>,
}

pub struct BraveProvider {
    pub base_url: Option<String>,
}

impl Default for BraveProvider {
    fn default() -> Self {
        Self::new()
    }
}

impl BraveProvider {
    pub fn new() -> Self {
        Self { base_url: None }
    }

    pub fn with_base_url(base_url: String) -> Self {
        Self {
            base_url: Some(base_url),
        }
    }
}

pub fn parse_brave(body: &str) -> Result<Vec<SearchResultItem>, String> {
    let parsed: BraveResponse = serde_json::from_str(body)
        .map_err(|e| format!("[brave] error: failed to parse response: {e}"))?;

    let results = parsed
        .web
        .and_then(|w| w.results)
        .unwrap_or_default()
        .into_iter()
        .map(|item| SearchResultItem {
            title: item.title,
            url: item.url,
            snippet: item.description,
            published_date: item.age.filter(|s| !s.trim().is_empty()),
        })
        .collect();

    Ok(results)
}

#[async_trait]
impl SearchProvider for BraveProvider {
    fn id(&self) -> &'static str {
        "brave"
    }

    fn name(&self) -> &'static str {
        "Brave"
    }

    fn is_available(&self, config: &WebConfig) -> bool {
        config.brave_api_key.is_some()
    }

    async fn search(
        &self,
        query: &str,
        opts: &SearchOpts,
        config: &WebConfig,
        cancel: &CancellationToken,
    ) -> Result<Vec<SearchResultItem>, String> {
        let api_key = config
            .brave_api_key
            .as_deref()
            .ok_or_else(|| "[brave] error: Brave API Key not configured".to_string())?;

        let count = if opts.num_results > 0 {
            opts.num_results
        } else {
            5
        };
        let encoded_query =
            url::form_urlencoded::byte_serialize(query.as_bytes()).collect::<String>();
        let base = self
            .base_url
            .as_deref()
            .unwrap_or("https://api.search.brave.com/res/v1/web/search");
        let sep = if base.contains('?') { '&' } else { '?' };
        let mut target_url = format!("{base}{sep}q={encoded_query}&count={count}");
        if let Some(ref freshness) = opts.recency_filter {
            target_url.push_str(&format!("&freshness={freshness}"));
        }

        let mut headers = HeaderMap::new();
        headers.insert(
            "X-Subscription-Token",
            HeaderValue::from_str(api_key)
                .map_err(|e| format!("[brave] error: invalid header value: {e}"))?,
        );
        headers.insert(ACCEPT, HeaderValue::from_static("application/json"));

        let fetch_opts = FetchOpts {
            method: reqwest::Method::GET,
            headers,
            body: None,
            proxy: opts.proxy.clone().or_else(|| config.proxy.clone()),
        };

        let outcome = safe_fetch(&target_url, &fetch_opts, cancel)
            .await
            .map_err(|e| format!("[brave] error: {e}"))?;

        if outcome.status < 200 || outcome.status >= 300 {
            return Err(format!("[brave] error: HTTP {}", outcome.status));
        }

        let body_str = String::from_utf8_lossy(&outcome.body);
        let mut results = parse_brave(&body_str)?;
        if opts.num_results > 0 && results.len() > opts.num_results {
            results.truncate(opts.num_results);
        }

        Ok(results)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_brave_is_available() {
        let provider = BraveProvider::new();
        let mut config = WebConfig::default();
        assert!(!provider.is_available(&config));

        config.brave_api_key = Some("test-key".to_string());
        assert!(provider.is_available(&config));
    }

    #[tokio::test]
    async fn test_brave_search_wiremock_success() {
        use wiremock::matchers::{header, method, path, query_param};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let fixture = include_str!("../fixtures/brave.json");
        let server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/res/v1/web/search"))
            .and(query_param("q", "rust"))
            .and(query_param("count", "5"))
            .and(query_param("freshness", "pw"))
            .and(header("X-Subscription-Token", "test-key"))
            .and(header("Accept", "application/json"))
            .respond_with(ResponseTemplate::new(200).set_body_string(fixture))
            .mount(&server)
            .await;

        let provider = BraveProvider::with_base_url(format!("{}/res/v1/web/search", server.uri()));
        let cancel = CancellationToken::new();
        let config = WebConfig {
            brave_api_key: Some("test-key".to_string()),
            ..Default::default()
        };
        let opts = SearchOpts {
            num_results: 5,
            recency_filter: Some("pw".to_string()),
            ..Default::default()
        };

        let results = provider
            .search("rust", &opts, &config, &cancel)
            .await
            .expect("search should succeed");
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].title, "Rust Programming Language");
    }

    #[tokio::test]
    async fn test_brave_search_wiremock_error() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/res/v1/web/search"))
            .respond_with(ResponseTemplate::new(429).set_body_string("Rate limited"))
            .mount(&server)
            .await;

        let provider = BraveProvider::with_base_url(format!("{}/res/v1/web/search", server.uri()));
        let cancel = CancellationToken::new();
        let config = WebConfig {
            brave_api_key: Some("test-key".to_string()),
            ..Default::default()
        };
        let opts = SearchOpts::default();

        let err = provider
            .search("rust", &opts, &config, &cancel)
            .await
            .unwrap_err();
        assert!(err.contains("[brave] error: HTTP 429"), "got: {err}");
    }

    #[test]
    fn test_parse_brave_fixture() {
        let fixture = include_str!("../fixtures/brave.json");
        let results = parse_brave(fixture).expect("parsing brave fixture should succeed");
        assert_eq!(results.len(), 2);

        assert_eq!(results[0].title, "Rust Programming Language");
        assert_eq!(results[0].url, "https://www.rust-lang.org/");
        assert_eq!(
            results[0].snippet,
            "A language empowering everyone to build reliable and efficient software."
        );
        assert_eq!(results[0].published_date.as_deref(), Some("2023-04-01"));

        assert_eq!(results[1].title, "Rust Crates");
        assert_eq!(results[1].url, "https://crates.io/");
        assert_eq!(results[1].snippet, "The Rust package registry");
        assert_eq!(results[1].published_date, None);
    }

    #[test]
    fn test_parse_brave_empty_results() {
        let json = r#"{"web": {"results": []}}"#;
        let results = parse_brave(json).expect("empty results array should parse to empty vec");
        assert_eq!(results, vec![]);

        let json_no_results = r#"{"web": {}}"#;
        let results =
            parse_brave(json_no_results).expect("missing results should parse to empty vec");
        assert_eq!(results, vec![]);

        let json_empty_obj = r#"{}"#;
        let results = parse_brave(json_empty_obj).expect("empty object should parse to empty vec");
        assert_eq!(results, vec![]);
    }

    #[test]
    fn test_parse_brave_malformed_json() {
        let err = parse_brave("not json").unwrap_err();
        assert!(
            err.contains("[brave]"),
            "error should mention provider name, got: {err}"
        );
    }
}

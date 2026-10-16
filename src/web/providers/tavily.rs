use async_trait::async_trait;
use reqwest::header::{HeaderMap, HeaderValue, CONTENT_TYPE};
use serde::Deserialize;
use tokio_util::sync::CancellationToken;

use crate::agent::web::config::WebConfig;
use crate::agent::web::net::{safe_fetch, FetchOpts};
use crate::agent::web::providers::types::{SearchOpts, SearchProvider, SearchResultItem};

#[derive(Deserialize)]
struct TavilyResponse {
    results: Option<Vec<TavilyResultItem>>,
}

#[derive(Deserialize)]
struct TavilyResultItem {
    #[serde(default)]
    title: String,
    #[serde(default)]
    url: String,
    #[serde(default)]
    content: String,
    #[serde(default)]
    published_date: Option<String>,
}

pub struct TavilyProvider {
    pub base_url: Option<String>,
}

impl Default for TavilyProvider {
    fn default() -> Self {
        Self::new()
    }
}

impl TavilyProvider {
    pub fn new() -> Self {
        Self { base_url: None }
    }

    pub fn with_base_url(base_url: String) -> Self {
        Self {
            base_url: Some(base_url),
        }
    }
}

pub fn parse_tavily(body: &str) -> Result<Vec<SearchResultItem>, String> {
    let parsed: TavilyResponse = serde_json::from_str(body)
        .map_err(|e| format!("[tavily] error: failed to parse response: {e}"))?;

    let results = parsed
        .results
        .unwrap_or_default()
        .into_iter()
        .map(|item| SearchResultItem {
            title: item.title,
            url: item.url,
            snippet: item.content,
            published_date: item.published_date.filter(|s| !s.trim().is_empty()),
        })
        .collect();

    Ok(results)
}

#[async_trait]
impl SearchProvider for TavilyProvider {
    fn id(&self) -> &'static str {
        "tavily"
    }

    fn name(&self) -> &'static str {
        "Tavily"
    }

    fn is_available(&self, config: &WebConfig) -> bool {
        config.tavily_api_key.is_some()
    }

    async fn search(
        &self,
        query: &str,
        opts: &SearchOpts,
        config: &WebConfig,
        cancel: &CancellationToken,
    ) -> Result<Vec<SearchResultItem>, String> {
        let api_key = config
            .tavily_api_key
            .as_deref()
            .ok_or_else(|| "[tavily] error: Tavily API Key not configured".to_string())?;

        let count = if opts.num_results > 0 {
            opts.num_results
        } else {
            5
        };
        let target_url = self
            .base_url
            .as_deref()
            .unwrap_or("https://api.tavily.com/search");

        let mut json_body = serde_json::json!({
            "api_key": api_key,
            "query": query,
            "max_results": count,
            "include_answer": false,
        });

        if let Some(ref recency) = opts.recency_filter {
            let days = match recency.to_ascii_lowercase().as_str() {
                "day" => Some(1),
                "week" => Some(7),
                "month" => Some(30),
                "year" => Some(365),
                _ => None,
            };
            if let Some(d) = days {
                json_body["days"] = serde_json::json!(d);
            }
        }

        let body_bytes =
            serde_json::to_vec(&json_body).map_err(|e| format!("[tavily] error: {e}"))?;

        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));

        let fetch_opts = FetchOpts {
            method: reqwest::Method::POST,
            headers,
            body: Some(body_bytes),
            proxy: opts.proxy.clone().or_else(|| config.proxy.clone()),
        };

        let outcome = safe_fetch(target_url, &fetch_opts, cancel)
            .await
            .map_err(|e| format!("[tavily] error: {e}"))?;

        if outcome.status < 200 || outcome.status >= 300 {
            return Err(format!("[tavily] error: HTTP {}", outcome.status));
        }

        let body_str = String::from_utf8_lossy(&outcome.body);
        let mut results = parse_tavily(&body_str)?;
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
    fn test_tavily_is_available() {
        let provider = TavilyProvider::new();
        let mut config = WebConfig::default();
        assert!(!provider.is_available(&config));

        config.tavily_api_key = Some("test-key".to_string());
        assert!(provider.is_available(&config));
    }

    #[tokio::test]
    async fn test_tavily_search_wiremock_success() {
        use wiremock::matchers::{body_json, header, method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let fixture = include_str!("../fixtures/tavily.json");
        let server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/search"))
            .and(header("Content-Type", "application/json"))
            .and(body_json(serde_json::json!({
                "api_key": "test-key",
                "query": "rust",
                "max_results": 5,
                "include_answer": false,
                "days": 7
            })))
            .respond_with(ResponseTemplate::new(200).set_body_string(fixture))
            .mount(&server)
            .await;

        let provider = TavilyProvider::with_base_url(format!("{}/search", server.uri()));
        let cancel = CancellationToken::new();
        let config = WebConfig {
            tavily_api_key: Some("test-key".to_string()),
            ..Default::default()
        };
        let opts = SearchOpts {
            num_results: 5,
            recency_filter: Some("week".to_string()),
            ..Default::default()
        };

        let results = provider
            .search("rust", &opts, &config, &cancel)
            .await
            .expect("tavily search should succeed");
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].title, "Rust Programming Language");
    }

    #[tokio::test]
    async fn test_tavily_search_wiremock_error() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/search"))
            .respond_with(ResponseTemplate::new(403).set_body_string("Forbidden"))
            .mount(&server)
            .await;

        let provider = TavilyProvider::with_base_url(format!("{}/search", server.uri()));
        let cancel = CancellationToken::new();
        let config = WebConfig {
            tavily_api_key: Some("test-key".to_string()),
            ..Default::default()
        };
        let opts = SearchOpts::default();

        let err = provider
            .search("rust", &opts, &config, &cancel)
            .await
            .unwrap_err();
        assert!(err.contains("[tavily] error: HTTP 403"), "got: {err}");
    }

    #[test]
    fn test_parse_tavily_fixture() {
        let fixture = include_str!("../fixtures/tavily.json");
        let results = parse_tavily(fixture).expect("parsing tavily fixture should succeed");
        assert_eq!(results.len(), 2);

        assert_eq!(results[0].title, "Rust Programming Language");
        assert_eq!(results[0].url, "https://www.rust-lang.org/");
        assert_eq!(
            results[0].snippet,
            "A language empowering everyone to build reliable and efficient software."
        );
        assert_eq!(results[0].published_date.as_deref(), Some("2023-05-10"));

        assert_eq!(results[1].title, "Rust Crates");
        assert_eq!(results[1].url, "https://crates.io/");
        assert_eq!(results[1].snippet, "The Rust package registry");
        assert_eq!(results[1].published_date, None);
    }

    #[test]
    fn test_parse_tavily_empty_results() {
        let json = r#"{"results": []}"#;
        let results = parse_tavily(json).expect("empty results array should parse to empty vec");
        assert_eq!(results, vec![]);

        let json_empty_obj = r#"{}"#;
        let results = parse_tavily(json_empty_obj).expect("empty object should parse to empty vec");
        assert_eq!(results, vec![]);
    }

    #[test]
    fn test_parse_tavily_malformed_json() {
        let err = parse_tavily("not json").unwrap_err();
        assert!(
            err.contains("[tavily]"),
            "error should mention provider name, got: {err}"
        );
    }
}

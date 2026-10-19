use async_trait::async_trait;
use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION, CONTENT_TYPE};
use serde::Deserialize;
use tokio_util::sync::CancellationToken;

use crate::agent::web::config::WebConfig;
use crate::agent::web::net::{safe_fetch, FetchOpts};
use crate::agent::web::providers::common::{
    check_http_status, format_provider_error, truncate_results, validate_query,
};
use crate::agent::web::providers::types::{SearchOpts, SearchProvider, SearchResultItem};

#[derive(Deserialize)]
struct PerplexityResponse {
    #[serde(default)]
    choices: Vec<PerplexityChoice>,
    #[serde(default)]
    citations: Vec<String>,
}

#[derive(Deserialize)]
struct PerplexityChoice {
    message: PerplexityMessage,
}

#[derive(Deserialize)]
struct PerplexityMessage {
    #[serde(default)]
    content: String,
}

pub struct PerplexityProvider {
    pub base_url: Option<String>,
}

impl Default for PerplexityProvider {
    fn default() -> Self {
        Self::new()
    }
}

impl PerplexityProvider {
    pub fn new() -> Self {
        Self { base_url: None }
    }

    pub fn with_base_url(base_url: String) -> Self {
        Self {
            base_url: Some(base_url),
        }
    }
}

/// Parses a Perplexity chat completion response into search results.
///
/// Note: Perplexity returns model prose with citations rather than structured
/// search index results. We extract citations as individual source links and include
/// the assistant response text as the snippet for the overall Perplexity answer.
pub fn parse_perplexity(body: &str) -> Result<Vec<SearchResultItem>, String> {
    let parsed: PerplexityResponse = serde_json::from_str(body).map_err(|e| {
        format_provider_error("perplexity", format!("failed to parse response: {e}"))
    })?;

    let mut results = Vec::new();
    for (i, url) in parsed.citations.into_iter().enumerate() {
        results.push(SearchResultItem {
            title: format!("Source {}", i + 1),
            url,
            snippet: String::new(),
            published_date: None,
        });
    }

    if let Some(first_choice) = parsed.choices.into_iter().next() {
        let content = first_choice.message.content.trim().to_string();
        if !content.is_empty() {
            results.push(SearchResultItem {
                title: "Perplexity Answer".to_string(),
                url: "https://perplexity.ai".to_string(),
                snippet: content,
                published_date: None,
            });
        }
    }

    Ok(results)
}

#[async_trait]
impl SearchProvider for PerplexityProvider {
    fn id(&self) -> &'static str {
        "perplexity"
    }

    fn name(&self) -> &'static str {
        "Perplexity"
    }

    fn is_available(&self, config: &WebConfig) -> bool {
        config.perplexity_api_key.is_some()
    }

    async fn search(
        &self,
        query: &str,
        opts: &SearchOpts,
        config: &WebConfig,
        cancel: &CancellationToken,
    ) -> Result<Vec<SearchResultItem>, String> {
        let valid_query =
            validate_query(query).map_err(|e| format_provider_error("perplexity", e))?;

        let api_key = config.perplexity_api_key.as_deref().ok_or_else(|| {
            format_provider_error("perplexity", "Perplexity API Key not configured")
        })?;

        let target_url = self
            .base_url
            .as_deref()
            .unwrap_or("https://api.perplexity.ai/chat/completions");

        let json_body = serde_json::json!({
            "model": "sonar",
            "messages": [{ "role": "user", "content": valid_query }],
        });
        let body_bytes =
            serde_json::to_vec(&json_body).map_err(|e| format_provider_error("perplexity", e))?;

        let mut headers = HeaderMap::new();
        headers.insert(
            AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {api_key}")).map_err(|e| {
                format_provider_error("perplexity", format!("invalid header value: {e}"))
            })?,
        );
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));

        let fetch_opts = FetchOpts {
            method: reqwest::Method::POST,
            headers,
            body: Some(body_bytes),
            proxy: opts.proxy.clone().or_else(|| config.proxy.clone()),
        };

        let outcome = safe_fetch(target_url, &fetch_opts, cancel)
            .await
            .map_err(|e| format_provider_error("perplexity", e))?;

        check_http_status("perplexity", outcome.status)?;

        let body_str = String::from_utf8_lossy(&outcome.body);
        let mut results = parse_perplexity(&body_str)?;
        truncate_results(&mut results, opts.num_results);

        Ok(results)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_perplexity_is_available() {
        let provider = PerplexityProvider::new();
        let mut config = WebConfig::default();
        assert!(!provider.is_available(&config));

        config.perplexity_api_key = Some("test-key".to_string());
        assert!(provider.is_available(&config));
    }

    #[tokio::test]
    async fn test_perplexity_search_wiremock_success() {
        use wiremock::matchers::{body_json, header, method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let fixture = include_str!("../fixtures/perplexity.json");
        let server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/chat/completions"))
            .and(header("Authorization", "Bearer test-key"))
            .and(header("Content-Type", "application/json"))
            .and(body_json(serde_json::json!({
                "model": "sonar",
                "messages": [{ "role": "user", "content": "rust" }]
            })))
            .respond_with(ResponseTemplate::new(200).set_body_string(fixture))
            .mount(&server)
            .await;

        let provider =
            PerplexityProvider::with_base_url(format!("{}/chat/completions", server.uri()));
        let cancel = CancellationToken::new();
        let config = WebConfig {
            perplexity_api_key: Some("test-key".to_string()),
            ..Default::default()
        };
        let opts = SearchOpts::default();

        let results = provider
            .search("rust", &opts, &config, &cancel)
            .await
            .expect("perplexity search should succeed");
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].title, "Source 1");
        assert_eq!(results[2].title, "Perplexity Answer");
    }

    #[tokio::test]
    async fn test_perplexity_search_wiremock_error() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/chat/completions"))
            .respond_with(ResponseTemplate::new(401).set_body_string("Unauthorized"))
            .mount(&server)
            .await;

        let provider =
            PerplexityProvider::with_base_url(format!("{}/chat/completions", server.uri()));
        let cancel = CancellationToken::new();
        let config = WebConfig {
            perplexity_api_key: Some("test-key".to_string()),
            ..Default::default()
        };
        let opts = SearchOpts::default();

        let err = provider
            .search("rust", &opts, &config, &cancel)
            .await
            .unwrap_err();
        assert!(err.contains("[perplexity] error: HTTP 401"), "got: {err}");
    }

    #[test]
    fn test_parse_perplexity_fixture() {
        let fixture = include_str!("../fixtures/perplexity.json");
        let results = parse_perplexity(fixture).expect("parsing perplexity fixture should succeed");
        assert_eq!(results.len(), 3);

        assert_eq!(results[0].title, "Source 1");
        assert_eq!(results[0].url, "https://www.rust-lang.org/");
        assert_eq!(results[0].snippet, "");
        assert_eq!(results[0].published_date, None);

        assert_eq!(results[1].title, "Source 2");
        assert_eq!(
            results[1].url,
            "https://en.wikipedia.org/wiki/Rust_(programming_language)"
        );
        assert_eq!(results[1].snippet, "");
        assert_eq!(results[1].published_date, None);

        assert_eq!(results[2].title, "Perplexity Answer");
        assert_eq!(results[2].url, "https://perplexity.ai");
        assert_eq!(
            results[2].snippet,
            "Rust is a multi-paradigm, general-purpose programming language designed for performance and safety."
        );
        assert_eq!(results[2].published_date, None);
    }

    #[test]
    fn test_parse_perplexity_empty_results() {
        let json = r#"{"citations": [], "choices": []}"#;
        let results =
            parse_perplexity(json).expect("empty citations and choices should parse to empty vec");
        assert_eq!(results, vec![]);

        let json_empty_obj = r#"{}"#;
        let results =
            parse_perplexity(json_empty_obj).expect("empty object should parse to empty vec");
        assert_eq!(results, vec![]);
    }

    #[test]
    fn test_parse_perplexity_preserves_literal_angle_brackets() {
        let json = r#"{
            "citations": [],
            "choices": [{
                "message": {
                    "role": "assistant",
                    "content": "Rust provides Vec<T> and requires x < 5 for validation."
                }
            }]
        }"#;
        let results = parse_perplexity(json).expect("should parse");
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].title, "Perplexity Answer");
        assert_eq!(
            results[0].snippet,
            "Rust provides Vec<T> and requires x < 5 for validation."
        );
    }

    #[test]
    fn test_parse_perplexity_malformed_json() {
        let err = parse_perplexity("not json").unwrap_err();
        assert!(
            err.contains("[perplexity]"),
            "error should mention provider name, got: {err}"
        );
    }
}

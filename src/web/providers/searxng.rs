use async_trait::async_trait;
use reqwest::header::HeaderMap;
use serde::Deserialize;
use tokio_util::sync::CancellationToken;

use crate::agent::web::config::WebConfig;
use crate::agent::web::net::{safe_fetch, FetchOpts};
use crate::agent::web::providers::types::{SearchOpts, SearchProvider, SearchResultItem};

#[derive(Deserialize)]
struct SearXNGResponse {
    results: Option<Vec<SearXNGResultItem>>,
}

#[derive(Deserialize)]
struct SearXNGResultItem {
    #[serde(default)]
    title: String,
    #[serde(default)]
    url: String,
    #[serde(default)]
    content: Option<String>,
    #[serde(default)]
    engine: Option<String>,
    #[serde(default, rename = "publishedDate")]
    published_date: Option<String>,
}

pub struct SearXNGProvider {
    pub base_url: Option<String>,
}

impl Default for SearXNGProvider {
    fn default() -> Self {
        Self::new()
    }
}

impl SearXNGProvider {
    pub fn new() -> Self {
        Self { base_url: None }
    }

    pub fn with_base_url(base_url: String) -> Self {
        Self {
            base_url: Some(base_url),
        }
    }
}

pub fn parse_searxng(body: &str) -> Result<Vec<SearchResultItem>, String> {
    let parsed: SearXNGResponse = serde_json::from_str(body)
        .map_err(|e| format!("[searxng] error: failed to parse response: {e}"))?;

    let results = parsed
        .results
        .unwrap_or_default()
        .into_iter()
        .map(|item| SearchResultItem {
            title: item.title,
            url: item.url,
            snippet: item.content.or(item.engine).unwrap_or_default(),
            published_date: item.published_date.filter(|s| !s.trim().is_empty()),
        })
        .collect();

    Ok(results)
}

#[async_trait]
impl SearchProvider for SearXNGProvider {
    fn id(&self) -> &'static str {
        "searxng"
    }

    fn name(&self) -> &'static str {
        "SearXNG"
    }

    fn is_available(&self, config: &WebConfig) -> bool {
        config.searxng_url.is_some()
    }

    async fn search(
        &self,
        query: &str,
        opts: &SearchOpts,
        config: &WebConfig,
        cancel: &CancellationToken,
    ) -> Result<Vec<SearchResultItem>, String> {
        let base = self
            .base_url
            .as_deref()
            .or(config.searxng_url.as_deref())
            .ok_or_else(|| "[searxng] error: SearXNG URL not configured".to_string())?;

        let clean_base = base.trim_end_matches('/');
        let encoded_query =
            url::form_urlencoded::byte_serialize(query.as_bytes()).collect::<String>();
        let sep = if clean_base.contains('?') { '&' } else { '?' };
        let target_url = format!("{clean_base}{sep}q={encoded_query}&format=json");

        let fetch_opts = FetchOpts {
            method: reqwest::Method::GET,
            headers: HeaderMap::new(),
            body: None,
            proxy: opts.proxy.clone().or_else(|| config.proxy.clone()),
        };

        let outcome = safe_fetch(&target_url, &fetch_opts, cancel)
            .await
            .map_err(|e| format!("[searxng] error: {e}"))?;

        if outcome.status < 200 || outcome.status >= 300 {
            let text = String::from_utf8_lossy(&outcome.body).trim().to_string();
            if !text.is_empty() {
                return Err(format!("[searxng] error: HTTP {}: {text}", outcome.status));
            } else {
                return Err(format!("[searxng] error: HTTP {}", outcome.status));
            }
        }

        let body_str = String::from_utf8_lossy(&outcome.body);
        match parse_searxng(&body_str) {
            Ok(mut results) => {
                if opts.num_results > 0 && results.len() > opts.num_results {
                    results.truncate(opts.num_results);
                }
                Ok(results)
            }
            Err(e) => {
                let trimmed = body_str.trim();
                if !trimmed.starts_with('{') && !trimmed.starts_with('[') && !trimmed.is_empty() {
                    Err(format!("[searxng] error: {trimmed}"))
                } else {
                    Err(e)
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_searxng_is_available() {
        let provider = SearXNGProvider::new();
        let mut config = WebConfig::default();
        assert!(!provider.is_available(&config));

        config.searxng_url = Some("http://localhost:8888".to_string());
        assert!(provider.is_available(&config));
    }

    #[tokio::test]
    async fn test_searxng_search_wiremock_success() {
        use wiremock::matchers::{method, path, query_param};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let fixture = include_str!("../fixtures/searxng.json");
        let server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/search"))
            .and(query_param("q", "rust"))
            .and(query_param("format", "json"))
            .respond_with(ResponseTemplate::new(200).set_body_string(fixture))
            .mount(&server)
            .await;

        // Test with trailing slash stripped
        let provider = SearXNGProvider::with_base_url(format!("{}/search/", server.uri()));
        let cancel = CancellationToken::new();
        let config = WebConfig {
            searxng_url: Some(format!("{}/search/", server.uri())),
            ..Default::default()
        };
        let opts = SearchOpts::default();

        let results = provider
            .search("rust", &opts, &config, &cancel)
            .await
            .expect("searxng search should succeed");
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].title, "Rust Programming Language");
    }

    #[tokio::test]
    async fn test_searxng_search_wiremock_403_surface_error() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/search"))
            .respond_with(
                ResponseTemplate::new(403)
                    .set_body_string("JSON format is not enabled on this instance."),
            )
            .mount(&server)
            .await;

        let provider = SearXNGProvider::with_base_url(format!("{}/search", server.uri()));
        let cancel = CancellationToken::new();
        let config = WebConfig {
            searxng_url: Some(format!("{}/search", server.uri())),
            ..Default::default()
        };
        let opts = SearchOpts::default();

        let err = provider
            .search("rust", &opts, &config, &cancel)
            .await
            .unwrap_err();
        assert!(
            err.contains("JSON format is not enabled on this instance."),
            "error should surface verbatim 403 text, got: {err}"
        );
        assert!(err.contains("[searxng] error: HTTP 403"), "got: {err}");
    }

    #[test]
    fn test_parse_searxng_fixture() {
        let fixture = include_str!("../fixtures/searxng.json");
        let results = parse_searxng(fixture).expect("parsing searxng fixture should succeed");
        assert_eq!(results.len(), 2);

        assert_eq!(results[0].title, "Rust Programming Language");
        assert_eq!(results[0].url, "https://www.rust-lang.org/");
        assert_eq!(
            results[0].snippet,
            "A language empowering everyone to build reliable and efficient software."
        );
        assert_eq!(results[0].published_date.as_deref(), Some("2023-01-01"));

        assert_eq!(results[1].title, "Rust Crates");
        assert_eq!(results[1].url, "https://crates.io/");
        assert_eq!(results[1].snippet, "The Rust package registry");
        assert_eq!(results[1].published_date, None);
    }

    #[test]
    fn test_parse_searxng_empty_results() {
        let json = r#"{"results": []}"#;
        let results = parse_searxng(json).expect("empty results array should parse to empty vec");
        assert_eq!(results, vec![]);

        let json_empty_obj = r#"{}"#;
        let results =
            parse_searxng(json_empty_obj).expect("empty object should parse to empty vec");
        assert_eq!(results, vec![]);
    }

    #[test]
    fn test_parse_searxng_malformed_json() {
        let err = parse_searxng("not json").unwrap_err();
        assert!(
            err.contains("[searxng]"),
            "error should mention provider name, got: {err}"
        );
    }
}

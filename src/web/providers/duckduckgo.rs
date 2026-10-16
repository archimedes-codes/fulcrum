use async_trait::async_trait;
use reqwest::header::{HeaderMap, HeaderValue, ACCEPT, ACCEPT_LANGUAGE, USER_AGENT};
use tokio_util::sync::CancellationToken;
use url::Url;

use crate::agent::web::config::WebConfig;
use crate::agent::web::net::{safe_fetch, FetchOpts};
use crate::agent::web::providers::types::{SearchOpts, SearchProvider, SearchResultItem};

const DEFAULT_USER_AGENT: &str =
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36";
const DEFAULT_DDG_URL: &str = "https://html.duckduckgo.com/html/?q=";

#[cfg(test)]
pub type FallbackMockFn = std::sync::Arc<
    dyn Fn(
            &str,
            &SearchOpts,
            &WebConfig,
            &CancellationToken,
        ) -> Result<Vec<SearchResultItem>, String>
        + Send
        + Sync,
>;

#[derive(Clone)]
pub struct DuckDuckGoProvider {
    pub base_url: Option<String>,
    #[cfg(test)]
    pub fallback_mock: Option<FallbackMockFn>,
}

impl Default for DuckDuckGoProvider {
    fn default() -> Self {
        Self::new()
    }
}

impl DuckDuckGoProvider {
    pub fn new() -> Self {
        Self {
            base_url: None,
            #[cfg(test)]
            fallback_mock: None,
        }
    }

    pub fn with_base_url(base_url: String) -> Self {
        Self {
            base_url: Some(base_url),
            #[cfg(test)]
            fallback_mock: None,
        }
    }

    #[cfg(test)]
    pub fn with_fallback_mock<F>(mut self, mock: F) -> Self
    where
        F: Fn(
                &str,
                &SearchOpts,
                &WebConfig,
                &CancellationToken,
            ) -> Result<Vec<SearchResultItem>, String>
            + Send
            + Sync
            + 'static,
    {
        self.fallback_mock = Some(std::sync::Arc::new(mock));
        self
    }

    async fn run_fallback(
        &self,
        query: &str,
        opts: &SearchOpts,
        cfg: &WebConfig,
        cancel: &CancellationToken,
    ) -> Result<Vec<SearchResultItem>, String> {
        #[cfg(test)]
        if let Some(ref mock) = self.fallback_mock {
            return mock(query, opts, cfg, cancel);
        }

        run_ddgs_fallback(query, opts, cfg, cancel).await
    }
}

async fn run_ddgs_fallback(
    query: &str,
    opts: &SearchOpts,
    cfg: &WebConfig,
    cancel: &CancellationToken,
) -> Result<Vec<SearchResultItem>, String> {
    if cancel.is_cancelled() {
        return Err("[duckduckgo] operation cancelled".to_string());
    }

    let proxy_str = opts.proxy.as_deref().or(cfg.proxy.as_deref());

    let mut headers = HeaderMap::new();
    headers.insert(USER_AGENT, HeaderValue::from_static(DEFAULT_USER_AGENT));
    headers.insert(ACCEPT, HeaderValue::from_static("*/*"));
    headers.insert(ACCEPT_LANGUAGE, HeaderValue::from_static("en-US,en;q=0.9"));

    let mut builder = reqwest::Client::builder()
        .default_headers(headers)
        .timeout(std::time::Duration::from_secs(20))
        .redirect(reqwest::redirect::Policy::limited(10));

    if let Some(proxy_url) = proxy_str {
        let p = reqwest::Proxy::all(proxy_url)
            .map_err(|e| format!("[duckduckgo] invalid proxy `{proxy_url}`: {e}"))?;
        builder = builder.proxy(p);
    } else {
        builder = builder.no_proxy();
    }

    let http_client = builder
        .build()
        .map_err(|e| format!("[duckduckgo] failed to build http client: {e}"))?;

    let ddgs_client = ddgs::Ddgs::with_client(http_client);
    let mut text_opts = ddgs::TextOptions {
        backend: ddgs::TextBackend::Auto,
        ..Default::default()
    };
    if opts.num_results > 0 {
        text_opts.max_results = Some(opts.num_results);
    }

    let fut = ddgs_client.text_with_options(query, text_opts);

    tokio::select! {
        _ = cancel.cancelled() => Err("[duckduckgo] operation cancelled".to_string()),
        res = fut => {
            let items = res.map_err(|e| format!("[duckduckgo] ddgs fallback error: {e}"))?;
            let mut results: Vec<SearchResultItem> = items
                .into_iter()
                .map(|item| SearchResultItem {
                    title: item.title,
                    url: item.href,
                    snippet: item.body,
                    published_date: None,
                })
                .collect();
            if opts.num_results > 0 && results.len() > opts.num_results {
                results.truncate(opts.num_results);
            }
            Ok(results)
        }
    }
}

fn extract_target_url(raw_href: &str) -> Option<String> {
    let mut url_str = raw_href.to_string();
    if let Some(query_part) = url_str.split('?').nth(1) {
        for (key, val) in url::form_urlencoded::parse(query_part.as_bytes()) {
            if key == "uddg" {
                url_str = val.into_owned();
                break;
            }
        }
    }

    if url_str.starts_with("//") {
        url_str = format!("https:{url_str}");
    }

    if Url::parse(&url_str).is_ok() {
        Some(url_str)
    } else {
        None
    }
}

pub fn is_ddg_challenge(doc: &dom_query::Document) -> bool {
    doc.select(".anomaly-modal, [class*='anomaly-modal'], #challenge-form, form[action*='anomaly']")
        .length()
        > 0
}

pub fn parse_ddg_html(html: &str) -> Vec<SearchResultItem> {
    let doc = dom_query::Document::fragment(html);
    parse_ddg_document(&doc)
}

pub fn parse_ddg_document(doc: &dom_query::Document) -> Vec<SearchResultItem> {
    let mut results = Vec::new();

    let elements = {
        let res = doc.select(".result");
        if res.length() > 0 {
            res
        } else {
            let web_res = doc.select(".web-result");
            if web_res.length() > 0 {
                web_res
            } else {
                doc.select(".result__body")
            }
        }
    };

    for el in elements.iter() {
        let a = {
            let a_el = el.select("a.result__a");
            if a_el.length() > 0 {
                a_el
            } else {
                let title_a = el.select(".result__title a");
                if title_a.length() > 0 {
                    title_a
                } else {
                    let h2_a = el.select("h2 a");
                    if h2_a.length() > 0 {
                        h2_a
                    } else {
                        el.select("a.result-link")
                    }
                }
            }
        };

        if a.length() == 0 {
            continue;
        }

        let snippet_el = {
            let s = el.select(".result__snippet");
            if s.length() > 0 {
                s
            } else {
                let b = el.select(".result__body");
                if b.length() > 0 {
                    b
                } else {
                    el.select(".result-snippet")
                }
            }
        };

        if snippet_el.length() == 0 {
            continue;
        }

        let raw_href = a.attr("href").map(|s| s.to_string()).unwrap_or_default();
        let Some(url_str) = extract_target_url(&raw_href) else {
            continue;
        };

        let title = a.text().trim().to_string();
        let snippet = snippet_el.text().trim().to_string();

        if !title.is_empty() {
            results.push(SearchResultItem {
                title,
                url: url_str,
                snippet,
                published_date: None,
            });
        }
    }

    // Fallback: If no results found, check for Lite table format (a.result-link and .result-snippet)
    if results.is_empty() {
        let links = doc.select("a.result-link");
        let snippets = doc.select(".result-snippet");
        for (i, link) in links.iter().enumerate() {
            let raw_href = link.attr("href").map(|s| s.to_string()).unwrap_or_default();
            let Some(url_str) = extract_target_url(&raw_href) else {
                continue;
            };

            let title = link.text().trim().to_string();
            let snippet = if i < snippets.length() {
                snippets.get(i).unwrap().text().trim().to_string()
            } else {
                String::new()
            };

            if !title.is_empty() {
                results.push(SearchResultItem {
                    title,
                    url: url_str,
                    snippet,
                    published_date: None,
                });
            }
        }
    }

    results
}

#[async_trait]
impl SearchProvider for DuckDuckGoProvider {
    fn id(&self) -> &'static str {
        "duckduckgo"
    }

    fn name(&self) -> &'static str {
        "DuckDuckGo"
    }

    fn is_available(&self, _config: &WebConfig) -> bool {
        true
    }

    async fn search(
        &self,
        query: &str,
        opts: &SearchOpts,
        cfg: &WebConfig,
        cancel: &CancellationToken,
    ) -> Result<Vec<SearchResultItem>, String> {
        if cancel.is_cancelled() {
            return Err("[duckduckgo] operation cancelled".to_string());
        }

        let encoded_query =
            url::form_urlencoded::byte_serialize(query.as_bytes()).collect::<String>();
        let target_url = match &self.base_url {
            Some(base) if base.ends_with('?') || base.ends_with('=') => {
                format!("{base}{encoded_query}")
            }
            Some(base) if base.contains('?') => format!("{base}&q={encoded_query}"),
            Some(base) => format!("{base}?q={encoded_query}"),
            None => format!("{DEFAULT_DDG_URL}{encoded_query}"),
        };

        let mut headers = HeaderMap::new();
        headers.insert(USER_AGENT, HeaderValue::from_static(DEFAULT_USER_AGENT));
        headers.insert(
            ACCEPT,
            HeaderValue::from_static(
                "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            ),
        );
        headers.insert(ACCEPT_LANGUAGE, HeaderValue::from_static("en-US,en;q=0.9"));

        let fetch_opts = FetchOpts {
            method: reqwest::Method::GET,
            headers,
            body: None,
            proxy: opts
                .proxy
                .as_deref()
                .or(cfg.proxy.as_deref())
                .map(|s| s.to_string()),
        };

        let primary_err = match safe_fetch(&target_url, &fetch_opts, cancel).await {
            Ok(outcome) => {
                if outcome.status == 200 {
                    let html = String::from_utf8_lossy(&outcome.body);
                    let doc = dom_query::Document::fragment(html.as_ref());
                    if is_ddg_challenge(&doc) {
                        "[duckduckgo] error: bot detection or rate limit challenge (anomaly challenge)".to_string()
                    } else {
                        let mut results = parse_ddg_document(&doc);
                        if opts.num_results > 0 && results.len() > opts.num_results {
                            results.truncate(opts.num_results);
                        }
                        return Ok(results);
                    }
                } else {
                    format!("[duckduckgo] error: HTTP {}", outcome.status)
                }
            }
            Err(e) => {
                let err_str = format!("[duckduckgo] error: {e}");
                if err_str.contains("blocked IP")
                    || err_str.contains("denied scheme")
                    || cancel.is_cancelled()
                {
                    return Err(err_str);
                }
                err_str
            }
        };

        match self.run_fallback(query, opts, cfg, cancel).await {
            Ok(results) => Ok(results),
            Err(fallback_err) => Err(format!("{primary_err} (fallback error: {fallback_err})")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::matchers::{method, path, query_param};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[test]
    fn test_parse_ddg_html_fixture() {
        let fixture = include_str!("../fixtures/duckduckgo_results.html");
        let results = parse_ddg_html(fixture);
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].title, "Rust Programming Language");
        assert_eq!(results[0].url, "https://www.rust-lang.org/");
        assert!(results[0]
            .snippet
            .contains("A language empowering everyone"));

        assert_eq!(results[1].title, "crates.io: Rust Package Registry");
        assert_eq!(results[1].url, "https://crates.io/");

        assert_eq!(results[2].title, "Docs.rs");
        assert_eq!(results[2].url, "https://docs.rs/");
    }

    #[tokio::test]
    async fn test_ddg_provider_search_wiremock() {
        let fixture = include_str!("../fixtures/duckduckgo_results.html");
        let server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/html/"))
            .and(query_param("q", "rust programming"))
            .respond_with(ResponseTemplate::new(200).set_body_string(fixture))
            .mount(&server)
            .await;

        let provider = DuckDuckGoProvider::with_base_url(format!("{}/html/", server.uri()));
        let cancel = CancellationToken::new();
        let config = WebConfig::default();
        let opts = SearchOpts {
            num_results: 2,
            ..Default::default()
        };

        let results = provider
            .search("rust programming", &opts, &config, &cancel)
            .await
            .unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].title, "Rust Programming Language");
        assert_eq!(results[1].title, "crates.io: Rust Package Registry");
    }

    #[tokio::test]
    async fn test_ddg_provider_search_fallback_proxy() {
        let server = MockServer::start().await;
        let provider = DuckDuckGoProvider::with_base_url(format!("{}/html/", server.uri()));
        let cancel = CancellationToken::new();
        let config = WebConfig {
            proxy: Some("http://10.0.0.1:8080".into()),
            ..Default::default()
        };
        let opts = SearchOpts::default();

        let err = provider
            .search("rust programming", &opts, &config, &cancel)
            .await
            .unwrap_err();
        assert!(err.contains("blocked IP"));
    }

    #[tokio::test]
    async fn test_ddg_provider_search_anomaly_202_returns_error() {
        let server = MockServer::start().await;

        let anomaly_html = r#"<!DOCTYPE html>
<html>
<head><title>DuckDuckGo</title></head>
<body>
<div class="anomaly-modal__title">Unfortunately, bots use DuckDuckGo too.</div>
<form id="challenge-form" action="//duckduckgo.com/anomaly.js"></form>
</body>
</html>"#;

        Mock::given(method("GET"))
            .and(path("/html/"))
            .and(query_param("q", "rust programming"))
            .respond_with(ResponseTemplate::new(202).set_body_string(anomaly_html))
            .mount(&server)
            .await;

        let provider = DuckDuckGoProvider::with_base_url(format!("{}/html/", server.uri()))
            .with_fallback_mock(|_, _, _, _| Err("all fallback engines failed".into()));
        let cancel = CancellationToken::new();
        let config = WebConfig::default();
        let opts = SearchOpts::default();

        let err = provider
            .search("rust programming", &opts, &config, &cancel)
            .await
            .unwrap_err();
        assert!(
            err.contains("202") || err.contains("bot detection") || err.contains("anomaly"),
            "Expected error about bot detection or 202, got: {err}"
        );
    }

    #[tokio::test]
    async fn test_ddg_provider_search_anomaly_202_falls_back_seamlessly() {
        let server = MockServer::start().await;

        let anomaly_html = r#"<!DOCTYPE html>
<html>
<head><title>DuckDuckGo</title></head>
<body>
<div class="anomaly-modal__title">Unfortunately, bots use DuckDuckGo too.</div>
<form id="challenge-form" action="//duckduckgo.com/anomaly.js"></form>
</body>
</html>"#;

        Mock::given(method("GET"))
            .and(path("/html/"))
            .and(query_param("q", "rust programming"))
            .respond_with(ResponseTemplate::new(202).set_body_string(anomaly_html))
            .mount(&server)
            .await;

        let provider = DuckDuckGoProvider::with_base_url(format!("{}/html/", server.uri()))
            .with_fallback_mock(|_q, _opts, _cfg, _cancel| {
                Ok(vec![SearchResultItem {
                    title: "Fallback Rust Programming".into(),
                    url: "https://example.com/fallback-rust".into(),
                    snippet: "A language empowering everyone from fallback".into(),
                    published_date: None,
                }])
            });

        let cancel = CancellationToken::new();
        let config = WebConfig::default();
        let opts = SearchOpts::default();

        let results = provider
            .search("rust programming", &opts, &config, &cancel)
            .await
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].title, "Fallback Rust Programming");
        assert_eq!(results[0].url, "https://example.com/fallback-rust");
    }

    #[tokio::test]
    async fn test_ddg_provider_search_403_falls_back_seamlessly() {
        let server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/html/"))
            .and(query_param("q", "rust programming"))
            .respond_with(ResponseTemplate::new(403))
            .mount(&server)
            .await;

        let provider = DuckDuckGoProvider::with_base_url(format!("{}/html/", server.uri()))
            .with_fallback_mock(|_q, _opts, _cfg, _cancel| {
                Ok(vec![SearchResultItem {
                    title: "Fallback On 403".into(),
                    url: "https://example.com/fallback-403".into(),
                    snippet: "Recovered from 403".into(),
                    published_date: None,
                }])
            });

        let cancel = CancellationToken::new();
        let config = WebConfig::default();
        let opts = SearchOpts::default();

        let results = provider
            .search("rust programming", &opts, &config, &cancel)
            .await
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].title, "Fallback On 403");
    }

    #[tokio::test]
    async fn test_ddg_provider_search_sends_browser_headers() {
        let fixture = include_str!("../fixtures/duckduckgo_results.html");
        let server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/html/"))
            .and(query_param("q", "rust programming"))
            .respond_with(ResponseTemplate::new(200).set_body_string(fixture))
            .mount(&server)
            .await;

        let provider = DuckDuckGoProvider::with_base_url(format!("{}/html/", server.uri()));
        let cancel = CancellationToken::new();
        let config = WebConfig::default();
        let opts = SearchOpts {
            num_results: 1,
            ..Default::default()
        };

        let results = provider
            .search("rust programming", &opts, &config, &cancel)
            .await
            .unwrap();
        assert_eq!(results.len(), 1);

        let reqs = server.received_requests().await.unwrap();
        assert_eq!(reqs.len(), 1);
        let accept = reqs[0].headers.get("accept").unwrap();
        assert!(accept.to_str().unwrap().contains("text/html"));
        let accept_lang = reqs[0].headers.get("accept-language").unwrap();
        assert_eq!(accept_lang.to_str().unwrap(), "en-US,en;q=0.9");
        let ua = reqs[0].headers.get("user-agent").unwrap();
        assert_eq!(ua.to_str().unwrap(), DEFAULT_USER_AGENT);
    }

    #[test]
    fn test_is_ddg_challenge_detects_elements_not_substrings() {
        let challenge_html_modal =
            r#"<html><body><div class="anomaly-modal"><p>Please verify</p></div></body></html>"#;
        assert!(is_ddg_challenge(&dom_query::Document::fragment(
            challenge_html_modal
        )));

        let challenge_html_class_variant =
            r#"<html><body><div class="anomaly-modal__title">Bots detected</div></body></html>"#;
        assert!(is_ddg_challenge(&dom_query::Document::fragment(
            challenge_html_class_variant
        )));

        let challenge_html_form =
            r#"<html><body><form id="challenge-form" action="/verify"></form></body></html>"#;
        assert!(is_ddg_challenge(&dom_query::Document::fragment(
            challenge_html_form
        )));

        let challenge_html_action =
            r#"<html><body><form action="//duckduckgo.com/anomaly.js"></form></body></html>"#;
        assert!(is_ddg_challenge(&dom_query::Document::fragment(
            challenge_html_action
        )));

        // Normal HTML with words "anomaly-modal" and "bots use DuckDuckGo too" in content
        let normal_html = r#"<html><body>
            <div class="result">
                <a class="result__a" href="https://example.com">bots use DuckDuckGo too explanation</a>
                <div class="result__snippet">Discussion about anomaly-modal and bots use DuckDuckGo too</div>
            </div>
        </body></html>"#;
        assert!(!is_ddg_challenge(&dom_query::Document::fragment(
            normal_html
        )));
    }

    #[test]
    fn test_parse_ddg_html_fallback_shapes() {
        // Fallback 1: result__title without result__a, and result__body without result__snippet
        let html_fallback = r#"
        <div class="result">
            <h2 class="result__title">
                <a href="https://example.com/fallback">Fallback Title</a>
            </h2>
            <div class="result__body">
                Fallback snippet text
            </div>
        </div>
        "#;
        let results = parse_ddg_html(html_fallback);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].title, "Fallback Title");
        assert_eq!(results[0].url, "https://example.com/fallback");
        assert_eq!(results[0].snippet, "Fallback snippet text");

        // Fallback 2: Lite format
        let html_lite = r#"
        <table class="results">
            <tr>
                <td><a class="result-link" href="//duckduckgo.com/l/?uddg=https%3A%2F%2Flite.example.com%2F">Lite Title</a></td>
            </tr>
            <tr>
                <td class="result-snippet">Lite snippet text</td>
            </tr>
        </table>
        "#;
        let lite_results = parse_ddg_html(html_lite);
        assert_eq!(lite_results.len(), 1);
        assert_eq!(lite_results[0].title, "Lite Title");
        assert_eq!(lite_results[0].url, "https://lite.example.com/");
        assert_eq!(lite_results[0].snippet, "Lite snippet text");
    }

    #[tokio::test]
    async fn test_ddg_provider_search_content_mentioning_bots_or_anomaly_modal_succeeds() {
        let server = MockServer::start().await;

        let html = r#"<!DOCTYPE html>
<html>
<head><title>bots use DuckDuckGo too at DuckDuckGo</title></head>
<body>
<div class="result results_links results_links_deep web-result">
    <div class="links_main result__body">
        <h2 class="result__title">
            <a class="result__a" href="//duckduckgo.com/l/?uddg=https%3A%2F%2Fexample.com%2Fbot-article">bots use DuckDuckGo too explained</a>
        </h2>
        <div class="result__snippet">
            This article discusses the anomaly-modal challenge and bots use DuckDuckGo too message.
        </div>
    </div>
</div>
</body>
</html>"#;

        Mock::given(method("GET"))
            .and(path("/html/"))
            .and(query_param("q", "bots use DuckDuckGo too"))
            .respond_with(ResponseTemplate::new(200).set_body_string(html))
            .mount(&server)
            .await;

        let provider = DuckDuckGoProvider::with_base_url(format!("{}/html/", server.uri()))
            .with_fallback_mock(|_, _, _, _| Err("fallback should not be called".into()));
        let cancel = CancellationToken::new();
        let config = WebConfig::default();
        let opts = SearchOpts::default();

        let results = provider
            .search("bots use DuckDuckGo too", &opts, &config, &cancel)
            .await
            .expect("Search should succeed and not falsely trigger challenge detection");
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].title, "bots use DuckDuckGo too explained");
        assert_eq!(results[0].url, "https://example.com/bot-article");
        assert!(results[0].snippet.contains("anomaly-modal"));
    }

    #[tokio::test]
    async fn test_ddg_provider_search_anomaly_200_detected_as_challenge() {
        let server = MockServer::start().await;

        let anomaly_html = r#"<!DOCTYPE html>
<html>
<head><title>DuckDuckGo</title></head>
<body>
<div class="anomaly-modal">
    <div class="anomaly-modal__title">Unfortunately, bots use DuckDuckGo too.</div>
    <form id="challenge-form" action="//duckduckgo.com/anomaly.js"></form>
</div>
</body>
</html>"#;

        Mock::given(method("GET"))
            .and(path("/html/"))
            .and(query_param("q", "rust programming"))
            .respond_with(ResponseTemplate::new(200).set_body_string(anomaly_html))
            .mount(&server)
            .await;

        let provider = DuckDuckGoProvider::with_base_url(format!("{}/html/", server.uri()))
            .with_fallback_mock(|_, _, _, _| Err("fallback failed".into()));
        let cancel = CancellationToken::new();
        let config = WebConfig::default();
        let opts = SearchOpts::default();

        let err = provider
            .search("rust programming", &opts, &config, &cancel)
            .await
            .unwrap_err();
        assert!(
            err.contains("bot detection") || err.contains("anomaly"),
            "Expected bot detection challenge error, got: {err}"
        );
    }

    #[tokio::test]
    #[ignore = "live network query"]
    async fn test_live_ddg_challenging_queries() {
        let provider = DuckDuckGoProvider::new();
        let cancel = CancellationToken::new();
        let config = WebConfig::default();
        let opts = SearchOpts {
            num_results: 5,
            ..Default::default()
        };

        let res1 = provider
            .search(
                "dog breeders oslo norway oppdretter hund",
                &opts,
                &config,
                &cancel,
            )
            .await;
        assert!(res1.is_ok(), "query 1 failed: {:?}", res1.err());
        let hits1 = res1.unwrap();
        assert!(!hits1.is_empty(), "query 1 returned empty hits");
        println!("=== Hits for 'dog breeders oslo norway oppdretter hund': ===");
        for (i, hit) in hits1.iter().enumerate() {
            println!("{}. {} ({})", i + 1, hit.title, hit.url);
        }

        let res2 = provider
            .search("oppdretter hund Oslo NKK", &opts, &config, &cancel)
            .await;
        assert!(res2.is_ok(), "query 2 failed: {:?}", res2.err());
        let hits2 = res2.unwrap();
        assert!(!hits2.is_empty(), "query 2 returned empty hits");
        println!("=== Hits for 'oppdretter hund Oslo NKK': ===");
        for (i, hit) in hits2.iter().enumerate() {
            println!("{}. {} ({})", i + 1, hit.title, hit.url);
        }
    }
}

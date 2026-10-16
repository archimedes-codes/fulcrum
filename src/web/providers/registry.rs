use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;
use url::Url;

use crate::agent::web::config::WebConfig;
use crate::agent::web::providers::brave::BraveProvider;
use crate::agent::web::providers::duckduckgo::DuckDuckGoProvider;
use crate::agent::web::providers::perplexity::PerplexityProvider;
use crate::agent::web::providers::searxng::SearXNGProvider;
use crate::agent::web::providers::tavily::TavilyProvider;
use crate::agent::web::providers::types::{SearchOpts, SearchProvider, SearchResultItem};

pub const AVAILABILITY_ORDER: &[&str] = &["brave", "tavily", "perplexity", "searxng", "duckduckgo"];

pub struct ResolvedProvider {
    pub provider: Arc<dyn SearchProvider>,
    pub was_auto_resolved: bool,
}

#[derive(Debug, Clone)]
pub struct SearchOutcome {
    pub provider: String,
    pub results: Vec<SearchResultItem>,
}

pub fn all_providers() -> Vec<Arc<dyn SearchProvider>> {
    vec![
        Arc::new(BraveProvider::new()),
        Arc::new(TavilyProvider::new()),
        Arc::new(PerplexityProvider::new()),
        Arc::new(SearXNGProvider::new()),
        Arc::new(DuckDuckGoProvider::new()),
    ]
}

pub fn resolve_provider(
    requested: Option<&str>,
    config: &WebConfig,
) -> Result<ResolvedProvider, String> {
    resolve_provider_from(&all_providers(), requested, config)
}

pub fn resolve_provider_from(
    providers: &[Arc<dyn SearchProvider>],
    requested: Option<&str>,
    config: &WebConfig,
) -> Result<ResolvedProvider, String> {
    // 1. If explicit provider requested (excluding empty and "auto")
    if let Some(req) = requested.filter(|r| !r.is_empty() && *r != "auto") {
        if let Some(p) = providers.iter().find(|p| p.id() == req) {
            return Ok(ResolvedProvider {
                provider: p.clone(),
                was_auto_resolved: false,
            });
        }
        return Err(format!("Unknown provider: {req}"));
    }

    // 2. If config specifies a default provider that isn't "auto"
    if !config.default_provider.is_empty() && config.default_provider != "auto" {
        if let Some(p) = providers.iter().find(|p| p.id() == config.default_provider) {
            return Ok(ResolvedProvider {
                provider: p.clone(),
                was_auto_resolved: false,
            });
        }
        return Err(format!("Unknown provider: {}", config.default_provider));
    }

    // 3. Fall back through AVAILABILITY_ORDER
    for id in AVAILABILITY_ORDER {
        if let Some(p) = providers.iter().find(|p| p.id() == *id) {
            if p.is_available(config) {
                return Ok(ResolvedProvider {
                    provider: p.clone(),
                    was_auto_resolved: true,
                });
            }
        }
    }

    Err("No search provider available".to_string())
}

pub fn canonicalize_url(url_str: &str) -> String {
    if let Ok(mut url) = Url::parse(url_str) {
        url.set_fragment(None);
        let mut s = url.to_string();
        if s.ends_with('/') {
            s.pop();
        }
        s
    } else {
        url_str.to_string()
    }
}

fn matches_domain_filter(url_str: &str, domains: &[String]) -> bool {
    if domains.is_empty() {
        return true;
    }
    if let Ok(url) = Url::parse(url_str) {
        if let Some(host) = url.host_str() {
            return domains
                .iter()
                .any(|d| host == d || host.ends_with(&format!(".{d}")));
        }
    }
    false
}

pub async fn execute_search(
    queries: &[String],
    opts: &SearchOpts,
    config: &WebConfig,
    cancel: &CancellationToken,
) -> Result<SearchOutcome, String> {
    execute_search_with_providers(&all_providers(), queries, opts, config, cancel).await
}

pub async fn execute_search_with_providers(
    providers: &[Arc<dyn SearchProvider>],
    queries: &[String],
    opts: &SearchOpts,
    config: &WebConfig,
    cancel: &CancellationToken,
) -> Result<SearchOutcome, String> {
    if queries.is_empty() {
        return Err("Queries array is empty".to_string());
    }

    let resolved = resolve_provider_from(providers, opts.provider.as_deref(), config)?;
    let mut current_provider = resolved.provider;
    let was_auto_resolved = resolved.was_auto_resolved;

    loop {
        match execute_queries_for_provider(current_provider.as_ref(), queries, opts, config, cancel)
            .await
        {
            Ok(results) => {
                return Ok(SearchOutcome {
                    provider: current_provider.id().to_string(),
                    results,
                });
            }
            Err(err) => {
                if !was_auto_resolved {
                    return Err(err);
                }

                // If auto-resolved, try the next available provider in AVAILABILITY_ORDER
                let current_id = current_provider.id();
                let current_idx = AVAILABILITY_ORDER
                    .iter()
                    .position(|id| *id == current_id)
                    .unwrap_or(0);

                let next_provider = AVAILABILITY_ORDER[current_idx + 1..].iter().find_map(|id| {
                    providers
                        .iter()
                        .find(|p| p.id() == *id && p.is_available(config))
                        .cloned()
                });

                if let Some(next) = next_provider {
                    current_provider = next;
                } else {
                    return Err(err);
                }
            }
        }
    }
}

async fn execute_queries_for_provider(
    provider: &dyn SearchProvider,
    queries: &[String],
    opts: &SearchOpts,
    config: &WebConfig,
    cancel: &CancellationToken,
) -> Result<Vec<SearchResultItem>, String> {
    let semaphore = Arc::new(Semaphore::new(3));
    let mut handles = Vec::new();

    for query in queries {
        let sem = semaphore.clone();
        let q = query.clone();
        let p = provider;
        let c = cancel.clone();
        let opts_clone = opts.clone();
        let cfg_clone = config.clone();

        // Concurrency with permit
        let fut = async move {
            let _permit = sem.acquire().await.map_err(|e| e.to_string())?;
            p.search(&q, &opts_clone, &cfg_clone, &c).await
        };
        handles.push(fut);
    }

    let query_results = futures_util::future::try_join_all(handles).await?;
    let mut merged = Vec::new();
    for results in query_results {
        merged.extend(results);
    }

    // Domain filter
    if !opts.domain_filter.is_empty() {
        merged.retain(|item| matches_domain_filter(&item.url, &opts.domain_filter));
    }

    // Canonical deduplication
    let mut seen = HashSet::new();
    let mut unique = Vec::new();

    for mut item in merged {
        let canonical = canonicalize_url(&item.url);
        if seen.insert(canonical.clone()) {
            item.url = canonical;
            unique.push(item);
        }
    }

    Ok(unique)
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;

    struct MockProvider {
        id: &'static str,
        available: bool,
        fail: bool,
        results: Vec<SearchResultItem>,
    }

    #[async_trait]
    impl SearchProvider for MockProvider {
        fn id(&self) -> &'static str {
            self.id
        }
        fn name(&self) -> &'static str {
            self.id
        }
        fn is_available(&self, _config: &WebConfig) -> bool {
            self.available
        }
        async fn search(
            &self,
            _query: &str,
            _opts: &SearchOpts,
            _config: &WebConfig,
            _cancel: &CancellationToken,
        ) -> Result<Vec<SearchResultItem>, String> {
            if self.fail {
                Err(format!("{} search failed", self.id))
            } else {
                Ok(self.results.clone())
            }
        }
    }

    #[test]
    fn test_all_providers_registered() {
        let providers = all_providers();
        assert_eq!(providers.len(), 5);
        let ids: Vec<&str> = providers.iter().map(|p| p.id()).collect();
        assert_eq!(
            ids,
            vec!["brave", "tavily", "perplexity", "searxng", "duckduckgo"]
        );
    }

    #[test]
    fn test_providers_availability() {
        let empty_config = WebConfig::default();
        let providers = all_providers();

        // With empty config, only DuckDuckGo is available
        assert!(!providers
            .iter()
            .find(|p| p.id() == "brave")
            .unwrap()
            .is_available(&empty_config));
        assert!(!providers
            .iter()
            .find(|p| p.id() == "tavily")
            .unwrap()
            .is_available(&empty_config));
        assert!(!providers
            .iter()
            .find(|p| p.id() == "perplexity")
            .unwrap()
            .is_available(&empty_config));
        assert!(!providers
            .iter()
            .find(|p| p.id() == "searxng")
            .unwrap()
            .is_available(&empty_config));
        assert!(providers
            .iter()
            .find(|p| p.id() == "duckduckgo")
            .unwrap()
            .is_available(&empty_config));

        // When configured, each is available
        let brave_cfg = WebConfig {
            brave_api_key: Some("k".into()),
            ..Default::default()
        };
        assert!(providers
            .iter()
            .find(|p| p.id() == "brave")
            .unwrap()
            .is_available(&brave_cfg));

        let tavily_cfg = WebConfig {
            tavily_api_key: Some("k".into()),
            ..Default::default()
        };
        assert!(providers
            .iter()
            .find(|p| p.id() == "tavily")
            .unwrap()
            .is_available(&tavily_cfg));

        let pplx_cfg = WebConfig {
            perplexity_api_key: Some("k".into()),
            ..Default::default()
        };
        assert!(providers
            .iter()
            .find(|p| p.id() == "perplexity")
            .unwrap()
            .is_available(&pplx_cfg));

        let searxng_cfg = WebConfig {
            searxng_url: Some("http://localhost:8888".into()),
            ..Default::default()
        };
        assert!(providers
            .iter()
            .find(|p| p.id() == "searxng")
            .unwrap()
            .is_available(&searxng_cfg));
    }

    #[test]
    fn test_resolve_provider_order() {
        let config = WebConfig::default();
        let resolved = resolve_provider(None, &config).unwrap();
        assert_eq!(resolved.provider.id(), "duckduckgo");
        assert!(resolved.was_auto_resolved);

        let explicit = resolve_provider(Some("duckduckgo"), &config).unwrap();
        assert_eq!(explicit.provider.id(), "duckduckgo");
        assert!(!explicit.was_auto_resolved);

        let auto_explicit = resolve_provider(Some("auto"), &config).unwrap();
        assert_eq!(auto_explicit.provider.id(), "duckduckgo");
        assert!(auto_explicit.was_auto_resolved);

        assert!(resolve_provider(Some("nonexistent"), &config).is_err());
    }

    #[test]
    fn test_canonicalize_url() {
        assert_eq!(
            canonicalize_url("https://example.com/path/#section"),
            "https://example.com/path"
        );
        assert_eq!(
            canonicalize_url("https://example.com/path/"),
            "https://example.com/path"
        );
        assert_eq!(
            canonicalize_url("https://example.com"),
            "https://example.com"
        );
    }

    #[tokio::test]
    async fn test_execute_search_dedup_and_domain_filter() {
        let mock = Arc::new(MockProvider {
            id: "duckduckgo",
            available: true,
            fail: false,
            results: vec![
                SearchResultItem {
                    title: "Item 1".into(),
                    url: "https://example.com/foo/#frag".into(),
                    snippet: "snippet 1".into(),
                    published_date: None,
                },
                SearchResultItem {
                    title: "Item 1 dup".into(),
                    url: "https://example.com/foo/".into(),
                    snippet: "snippet 1 dup".into(),
                    published_date: None,
                },
                SearchResultItem {
                    title: "Item 2".into(),
                    url: "https://sub.other.org/bar".into(),
                    snippet: "snippet 2".into(),
                    published_date: None,
                },
                SearchResultItem {
                    title: "Item 3".into(),
                    url: "https://disallowed.com/baz".into(),
                    snippet: "snippet 3".into(),
                    published_date: None,
                },
            ],
        });

        let providers: Vec<Arc<dyn SearchProvider>> = vec![mock];
        let cancel = CancellationToken::new();
        let config = WebConfig::default();
        let opts = SearchOpts {
            domain_filter: vec!["example.com".into(), "other.org".into()],
            ..Default::default()
        };

        let outcome =
            execute_search_with_providers(&providers, &["query".into()], &opts, &config, &cancel)
                .await
                .unwrap();

        // Check deduplication and domain filter:
        // Item 1 and Item 1 dup deduplicate to https://example.com/foo
        // Item 2 matches *.other.org
        // Item 3 (disallowed.com) is excluded
        assert_eq!(outcome.results.len(), 2);
        assert_eq!(outcome.results[0].url, "https://example.com/foo");
        assert_eq!(outcome.results[1].url, "https://sub.other.org/bar");
    }

    #[tokio::test]
    async fn test_failover_when_auto_resolved() {
        let failing = Arc::new(MockProvider {
            id: "brave",
            available: true,
            fail: true,
            results: vec![],
        });
        let succeeding = Arc::new(MockProvider {
            id: "duckduckgo",
            available: true,
            fail: false,
            results: vec![SearchResultItem {
                title: "DDG result".into(),
                url: "https://duckduckgo.com".into(),
                snippet: "found it".into(),
                published_date: None,
            }],
        });

        let providers: Vec<Arc<dyn SearchProvider>> = vec![failing, succeeding];
        let cancel = CancellationToken::new();
        let config = WebConfig::default();
        let opts = SearchOpts::default();

        let outcome =
            execute_search_with_providers(&providers, &["query".into()], &opts, &config, &cancel)
                .await
                .unwrap();

        assert_eq!(outcome.provider, "duckduckgo");
        assert_eq!(outcome.results.len(), 1);
        assert_eq!(outcome.results[0].title, "DDG result");
    }

    #[tokio::test]
    async fn test_concurrent_queries_execution() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        struct ConcurrentMockProvider {
            active: AtomicUsize,
            max_active: AtomicUsize,
        }

        #[async_trait]
        impl SearchProvider for ConcurrentMockProvider {
            fn id(&self) -> &'static str {
                "concurrent_mock"
            }
            fn name(&self) -> &'static str {
                "Concurrent Mock"
            }
            fn is_available(&self, _config: &WebConfig) -> bool {
                true
            }
            async fn search(
                &self,
                query: &str,
                _opts: &SearchOpts,
                _config: &WebConfig,
                _cancel: &CancellationToken,
            ) -> Result<Vec<SearchResultItem>, String> {
                let current = self.active.fetch_add(1, Ordering::SeqCst) + 1;
                self.max_active.fetch_max(current, Ordering::SeqCst);
                tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
                self.active.fetch_sub(1, Ordering::SeqCst);
                Ok(vec![SearchResultItem {
                    title: format!("Result for {query}"),
                    url: format!("https://example.com/{query}"),
                    snippet: "snippet".into(),
                    published_date: None,
                }])
            }
        }

        let mock = Arc::new(ConcurrentMockProvider {
            active: AtomicUsize::new(0),
            max_active: AtomicUsize::new(0),
        });
        let provider: Arc<dyn SearchProvider> = mock.clone();

        let queries = vec!["q1".to_string(), "q2".to_string(), "q3".to_string()];
        let opts = SearchOpts {
            num_results: 5,
            recency_filter: None,
            domain_filter: vec![],
            proxy: None,
            provider: Some("concurrent_mock".into()),
        };
        let config = WebConfig::default();
        let cancel = CancellationToken::new();

        let outcome = execute_search_with_providers(
            std::slice::from_ref(&provider),
            &queries,
            &opts,
            &config,
            &cancel,
        )
        .await
        .unwrap();

        assert_eq!(outcome.results.len(), 3);
        // Because queries are run concurrently, max_active should be > 1
        assert!(
            mock.max_active.load(Ordering::SeqCst) > 1,
            "queries should run concurrently"
        );
    }
}

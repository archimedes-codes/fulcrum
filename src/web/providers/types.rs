use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

use crate::agent::web::config::WebConfig;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SearchResultItem {
    pub title: String,
    pub url: String,
    pub snippet: String,
    #[serde(skip_serializing_if = "Option::is_none", rename = "publishedDate")]
    pub published_date: Option<String>,
}

#[derive(Debug, Clone)]
pub struct SearchOpts {
    pub num_results: usize,
    pub recency_filter: Option<String>,
    pub domain_filter: Vec<String>,
    pub proxy: Option<String>,
    pub provider: Option<String>,
}

impl Default for SearchOpts {
    fn default() -> Self {
        Self {
            num_results: 5,
            recency_filter: None,
            domain_filter: Vec::new(),
            proxy: None,
            provider: None,
        }
    }
}

#[async_trait]
pub trait SearchProvider: Send + Sync {
    fn id(&self) -> &'static str;
    fn name(&self) -> &'static str;
    fn is_available(&self, config: &WebConfig) -> bool;
    async fn search(
        &self,
        query: &str,
        opts: &SearchOpts,
        config: &WebConfig,
        cancel: &CancellationToken,
    ) -> Result<Vec<SearchResultItem>, String>;
}

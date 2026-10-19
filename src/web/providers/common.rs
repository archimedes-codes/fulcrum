//! Common utilities for web search providers.

use std::sync::LazyLock;

use regex::Regex;
use reqwest::header::{HeaderMap, HeaderValue, ACCEPT, ACCEPT_LANGUAGE, USER_AGENT};

use crate::agent::web::providers::types::SearchResultItem;

pub const DEFAULT_USER_AGENT: &str =
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36";

static HTML_TAG_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)<!--[\s\S]*?-->|</?(?:a|abbr|article|aside|b|bdi|bdo|blockquote|body|br|button|caption|cite|code|dd|del|details|dfn|dialog|div|dl|dt|em|figcaption|figure|footer|form|h[1-6]|head|header|hr|html|i|iframe|img|input|ins|kbd|label|legend|li|link|main|mark|meta|nav|ol|optgroup|option|p|pre|q|s|samp|script|section|select|small|source|span|strong|style|sub|summary|sup|svg|table|tbody|td|template|textarea|tfoot|th|thead|time|title|tr|u|ul|var|video|wbr)(?:\s+[^>]*?)?/?>").unwrap()
});

/// Collapses consecutive whitespace (spaces, tabs, newlines) into a single space and trims.
pub fn collapse_whitespace(raw: &str) -> String {
    raw.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// Normalizes snippets by stripping basic HTML tags, unescaping common entities,
/// and collapsing whitespace. Non-HTML angle brackets (e.g. `Vec<T>`, `x < 5`) are preserved.
pub fn clean_snippet(raw: &str) -> String {
    if raw.trim().is_empty() {
        return String::new();
    }

    // Strip HTML tags using regex matching known HTML tags to avoid eating literal angle brackets
    let stripped = HTML_TAG_RE.replace_all(raw, "");

    // Decode common HTML entities
    let decoded = stripped
        .replace("&quot;", "\"")
        .replace("&#34;", "\"")
        .replace("&apos;", "'")
        .replace("&#39;", "'")
        .replace("&lt;", "<")
        .replace("&#60;", "<")
        .replace("&gt;", ">")
        .replace("&#62;", ">")
        .replace("&nbsp;", " ")
        .replace("&#160;", " ")
        .replace("&amp;", "&");

    collapse_whitespace(&decoded)
}

/// Validates that a search query is non-empty after trimming.
pub fn validate_query(query: &str) -> Result<&str, String> {
    let trimmed = query.trim();
    if trimmed.is_empty() {
        Err("search query cannot be empty".to_string())
    } else {
        Ok(trimmed)
    }
}

/// Truncates search results in-place if limit is greater than 0 and results exceed limit.
pub fn truncate_results(results: &mut Vec<SearchResultItem>, limit: usize) {
    if limit > 0 && results.len() > limit {
        results.truncate(limit);
    }
}

/// Standardized error message formatter for search providers: `[{provider}] error: {msg}`.
pub fn format_provider_error(provider: &str, msg: impl std::fmt::Display) -> String {
    format!("[{provider}] error: {msg}")
}

/// Formats an HTTP status error: `[{provider}] error: HTTP {status}`.
pub fn format_http_error(provider: &str, status: u16) -> String {
    format!("[{provider}] error: HTTP {status}")
}

/// Formats an HTTP status error including response body text if available.
pub fn format_http_error_with_body(provider: &str, status: u16, body: &[u8]) -> String {
    let text = String::from_utf8_lossy(body).trim().to_string();
    if !text.is_empty() {
        format!("[{provider}] error: HTTP {status}: {text}")
    } else {
        format_http_error(provider, status)
    }
}

/// Validates that an HTTP status code is in the 2xx success range.
pub fn check_http_status(provider: &str, status: u16) -> Result<(), String> {
    if !(200..300).contains(&status) {
        Err(format_http_error(provider, status))
    } else {
        Ok(())
    }
}

/// Constructs standard browser headers used for web scraping and search fallbacks.
pub fn build_browser_headers() -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(USER_AGENT, HeaderValue::from_static(DEFAULT_USER_AGENT));
    headers.insert(
        ACCEPT,
        HeaderValue::from_static("text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"),
    );
    headers.insert(ACCEPT_LANGUAGE, HeaderValue::from_static("en-US,en;q=0.9"));
    headers
}

/// Resolves the requested number of results, falling back to a default if 0.
pub fn resolve_num_results(num_results: usize, default_count: usize) -> usize {
    if num_results > 0 {
        num_results
    } else {
        default_count
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_collapse_whitespace() {
        assert_eq!(collapse_whitespace("  hello   world  "), "hello world");
        assert_eq!(
            collapse_whitespace("line1\n\nline2\tline3"),
            "line1 line2 line3"
        );
        assert_eq!(collapse_whitespace(""), "");
        assert_eq!(collapse_whitespace("   "), "");
    }

    #[test]
    fn test_clean_snippet() {
        let raw = "  <b>Rust</b> is a <em>systems</em> programming &amp; language.  ";
        assert_eq!(
            clean_snippet(raw),
            "Rust is a systems programming & language."
        );

        let entities = "Quotes: &quot;hello&quot; and &#39;world&#39; &lt;tags&gt; &nbsp;";
        assert_eq!(
            clean_snippet(entities),
            "Quotes: \"hello\" and 'world' <tags>"
        );

        assert_eq!(clean_snippet(""), "");
        assert_eq!(clean_snippet("   \n\t  "), "");

        // Literal angle brackets and unclosed comparisons
        assert_eq!(
            clean_snippet("Rust has Vec<T> and Option<T> types"),
            "Rust has Vec<T> and Option<T> types"
        );
        assert_eq!(clean_snippet("x < 5 is required"), "x < 5 is required");
        assert_eq!(
            clean_snippet("if x < 5 and y > 2 { do_something(); }"),
            "if x < 5 and y > 2 { do_something(); }"
        );
        assert_eq!(
            clean_snippet("<b>Rust</b> has <code>Vec<T></code> when x < 5 &amp; y &gt; 2"),
            "Rust has Vec<T> when x < 5 & y > 2"
        );
    }

    #[test]
    fn test_validate_query() {
        assert_eq!(validate_query("rust lang").unwrap(), "rust lang");
        assert_eq!(validate_query("  rust lang  ").unwrap(), "rust lang");
        assert!(validate_query("").is_err());
        assert!(validate_query("   ").is_err());
    }

    #[test]
    fn test_truncate_results() {
        let mut items = vec![
            SearchResultItem {
                title: "1".into(),
                url: "u1".into(),
                snippet: "s1".into(),
                published_date: None,
            },
            SearchResultItem {
                title: "2".into(),
                url: "u2".into(),
                snippet: "s2".into(),
                published_date: None,
            },
            SearchResultItem {
                title: "3".into(),
                url: "u3".into(),
                snippet: "s3".into(),
                published_date: None,
            },
        ];

        // limit = 0 -> no truncation
        truncate_results(&mut items, 0);
        assert_eq!(items.len(), 3);

        // limit = 2 -> truncated to 2
        truncate_results(&mut items, 2);
        assert_eq!(items.len(), 2);
        assert_eq!(items[0].title, "1");
        assert_eq!(items[1].title, "2");

        // limit = 5 -> no truncation because len < limit
        truncate_results(&mut items, 5);
        assert_eq!(items.len(), 2);
    }

    #[test]
    fn test_format_provider_error() {
        assert_eq!(
            format_provider_error("brave", "something broke"),
            "[brave] error: something broke"
        );
    }

    #[test]
    fn test_format_http_error() {
        assert_eq!(
            format_http_error("duckduckgo", 404),
            "[duckduckgo] error: HTTP 404"
        );
    }

    #[test]
    fn test_format_http_error_with_body() {
        assert_eq!(
            format_http_error_with_body("searxng", 403, b"Forbidden text"),
            "[searxng] error: HTTP 403: Forbidden text"
        );
        assert_eq!(
            format_http_error_with_body("searxng", 500, b"   "),
            "[searxng] error: HTTP 500"
        );
    }

    #[test]
    fn test_check_http_status() {
        assert!(check_http_status("brave", 200).is_ok());
        assert!(check_http_status("brave", 204).is_ok());
        assert_eq!(
            check_http_status("brave", 404).unwrap_err(),
            "[brave] error: HTTP 404"
        );
        assert_eq!(
            check_http_status("brave", 500).unwrap_err(),
            "[brave] error: HTTP 500"
        );
    }

    #[test]
    fn test_build_browser_headers() {
        let headers = build_browser_headers();
        assert_eq!(headers.get(USER_AGENT).unwrap(), DEFAULT_USER_AGENT);
        assert!(headers
            .get(ACCEPT)
            .unwrap()
            .to_str()
            .unwrap()
            .contains("text/html"));
        assert_eq!(headers.get(ACCEPT_LANGUAGE).unwrap(), "en-US,en;q=0.9");
    }

    #[test]
    fn test_resolve_num_results() {
        assert_eq!(resolve_num_results(10, 5), 10);
        assert_eq!(resolve_num_results(0, 5), 5);
    }
}

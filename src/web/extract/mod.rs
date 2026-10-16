use crate::agent::tools::exec::ToolCtx;
use crate::agent::web::net::{safe_fetch, FetchOpts};
use url::Url;

pub mod article;
pub mod github;
pub mod pdf;
pub mod youtube;

#[cfg(test)]
mod tests;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    Readable,
    Raw,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExtractedDoc {
    pub title: String,
    pub url: String,
    pub markdown: String,
    pub word_count: usize,
    pub status: u16,
    pub extractor: &'static str,
}

pub async fn extract(url: &str, mode: Mode, ctx: &ToolCtx) -> Result<ExtractedDoc, String> {
    let parsed_url = Url::parse(url).map_err(|e| format!("invalid url `{url}`: {e}"))?;

    // Raw mode check before GitHub, YouTube, or PDF routing
    if mode == Mode::Raw {
        let opts = FetchOpts {
            proxy: ctx.web.proxy.clone(),
            ..Default::default()
        };
        let outcome = safe_fetch(url, &opts, &ctx.cancel)
            .await
            .map_err(|e| e.to_string())?;

        let body_str = String::from_utf8_lossy(&outcome.body).into_owned();
        let title = extract_raw_title(&body_str).unwrap_or_else(|| "Raw Content".to_string());
        let word_count = body_str.split_whitespace().count();
        return Ok(ExtractedDoc {
            title,
            url: outcome.final_url.to_string(),
            markdown: body_str,
            word_count,
            status: outcome.status,
            extractor: "raw",
        });
    }

    let host = parsed_url.host_str().unwrap_or_default();

    // 1. GitHub routing
    if host == "github.com" || host == "www.github.com" {
        return github::extract(url, ctx).await;
    }

    // 2. YouTube routing
    if host == "youtube.com"
        || host == "www.youtube.com"
        || host == "m.youtube.com"
        || host == "youtu.be"
    {
        return youtube::extract(url, ctx).await;
    }

    // 3. Safe fetch the URL
    let opts = FetchOpts {
        proxy: ctx.web.proxy.clone(),
        ..Default::default()
    };
    let outcome = safe_fetch(url, &opts, &ctx.cancel)
        .await
        .map_err(|e| e.to_string())?;

    // 4. PDF check: content-type contains application/pdf or url path ends in .pdf
    let is_pdf = outcome
        .content_type
        .to_lowercase()
        .contains("application/pdf")
        || parsed_url.path().to_lowercase().ends_with(".pdf");

    if is_pdf {
        return pdf::extract(outcome.body, url, outcome.status, &ctx.cancel).await;
    }

    // 5. Article extraction fallback
    let body_str = String::from_utf8_lossy(&outcome.body);
    Ok(article::extract(
        &body_str,
        outcome.final_url.as_str(),
        outcome.status,
    ))
}

fn extract_raw_title(html: &str) -> Option<String> {
    let re = regex::Regex::new(r"(?i)<title[^>]*>(.*?)</title>").ok()?;
    let caps = re.captures(html)?;
    let title = caps
        .get(1)?
        .as_str()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");
    if title.is_empty() {
        None
    } else {
        Some(title)
    }
}

/// Check if an extracted document looks blocked or empty (needs headless render escalation).
pub fn looks_blocked(doc: &ExtractedDoc) -> bool {
    matches!(doc.status, 403 | 429 | 503) || doc.markdown.split_whitespace().count() < 80
}

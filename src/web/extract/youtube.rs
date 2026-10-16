//! YouTube extraction.
//!
//! Note: extracting captions via regex from `ytInitialPlayerResponse` is rot-prone
//! because YouTube frequently changes its internal web player response structures.
//! When player JSON parsing or caption retrieval fails, extraction falls back
//! to the official oEmbed JSON endpoint, and ultimately to article HTML extraction.

use super::ExtractedDoc;
use crate::agent::tools::exec::ToolCtx;
use crate::agent::web::extract::article;
use crate::agent::web::net::{safe_fetch, FetchOpts};
use serde_json::Value;
use url::Url;

pub fn extract_video_id(url_str: &str) -> Option<String> {
    let parsed = Url::parse(url_str).ok()?;
    let host = parsed.host_str()?;

    if host == "youtu.be" {
        return parsed
            .path_segments()?
            .next()
            .map(|s| s.split('/').next().unwrap_or(s).to_string())
            .filter(|s| !s.is_empty());
    }

    if host == "youtube.com" || host == "www.youtube.com" || host == "m.youtube.com" {
        for (k, v) in parsed.query_pairs() {
            if k == "v" && !v.is_empty() {
                return Some(v.into_owned());
            }
        }
    }

    None
}

pub async fn extract(url: &str, ctx: &ToolCtx) -> Result<ExtractedDoc, String> {
    let video_id = extract_video_id(url);
    let opts = FetchOpts {
        proxy: ctx.web.proxy.clone(),
        ..Default::default()
    };

    let watch_outcome = safe_fetch(url, &opts, &ctx.cancel).await;
    let watch_body = watch_outcome
        .as_ref()
        .map(|o| String::from_utf8_lossy(&o.body).into_owned())
        .unwrap_or_default();
    let watch_status = watch_outcome.as_ref().map(|o| o.status).unwrap_or(200);

    if video_id.is_some() && !watch_body.is_empty() {
        if let Some(doc) = try_extract_captions(&watch_body, url, &opts, ctx).await {
            return Ok(doc);
        }
    }

    // Try oEmbed fallback
    if let Some(vid) = video_id {
        let oembed_url = format!(
            "https://www.youtube.com/oembed?url=https://www.youtube.com/watch?v={}&format=json",
            vid
        );
        if let Ok(outcome) = safe_fetch(&oembed_url, &opts, &ctx.cancel).await {
            if outcome.status >= 200 && outcome.status < 300 {
                if let Ok(data) = serde_json::from_slice::<Value>(&outcome.body) {
                    let title = data
                        .get("title")
                        .and_then(Value::as_str)
                        .unwrap_or("YouTube Video")
                        .to_string();
                    let author = data
                        .get("author_name")
                        .and_then(Value::as_str)
                        .unwrap_or_default();
                    let markdown = format!("Video: {}\n\n[Watch on YouTube]({})", title, url);
                    let text_for_count = format!("{} {}", title, author);
                    let word_count = text_for_count.split_whitespace().count();
                    return Ok(ExtractedDoc {
                        title,
                        url: url.to_string(),
                        markdown,
                        word_count,
                        status: outcome.status,
                        extractor: "youtube",
                    });
                }
            }
        }
    }

    // Fall back to article extractor on watch page HTML if available
    if !watch_body.is_empty() {
        let mut doc = article::extract(&watch_body, url, watch_status);
        doc.extractor = "youtube";
        return Ok(doc);
    }

    Err(format!("failed to fetch YouTube page: {}", url))
}

async fn try_extract_captions(
    watch_page: &str,
    url: &str,
    opts: &FetchOpts,
    ctx: &ToolCtx,
) -> Option<ExtractedDoc> {
    let re = regex::Regex::new(r"ytInitialPlayerResponse\s*=\s*(\{.+?\});").ok()?;
    let caps = re.captures(watch_page)?;
    let json_str = caps.get(1)?.as_str();
    let val: Value = serde_json::from_str(json_str).ok()?;

    let title = val
        .pointer("/videoDetails/title")
        .and_then(Value::as_str)
        .unwrap_or("YouTube Video")
        .to_string();

    let caption_url = val
        .pointer("/captions/playerCaptionsTracklistRenderer/captionTracks/0/baseUrl")
        .and_then(Value::as_str)?;

    let caption_outcome = safe_fetch(caption_url, opts, &ctx.cancel).await.ok()?;
    if caption_outcome.status < 200 || caption_outcome.status >= 300 {
        return None;
    }

    let xml = String::from_utf8_lossy(&caption_outcome.body);
    let transcript = parse_captions_xml(&xml);
    if transcript.trim().is_empty() {
        return None;
    }

    let markdown = format!("## {}\n\n{}", title, transcript.trim());
    let word_count = markdown.split_whitespace().count();

    Some(ExtractedDoc {
        title,
        url: url.to_string(),
        markdown,
        word_count,
        status: 200,
        extractor: "youtube",
    })
}

fn parse_captions_xml(xml: &str) -> String {
    // Strip <text ...> into lines, unescape entities
    let text_tag_re = regex::Regex::new(r#"<text[^>]*>(.*?)</text>"#).ok();
    let mut lines = Vec::new();

    if let Some(re) = text_tag_re {
        for cap in re.captures_iter(xml) {
            if let Some(m) = cap.get(1) {
                let unescaped = unescape_xml(m.as_str());
                let trimmed = unescaped.trim();
                if !trimmed.is_empty() {
                    lines.push(trimmed.to_string());
                }
            }
        }
    }

    if lines.is_empty() {
        // Fallback: strip all tags
        let tag_strip = regex::Regex::new(r"<[^>]+>").unwrap();
        let stripped = tag_strip.replace_all(xml, " ");
        unescape_xml(&stripped)
    } else {
        lines.join("\n")
    }
}

fn unescape_xml(s: &str) -> String {
    s.replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&apos;", "'")
        .replace("&#39;", "'")
}

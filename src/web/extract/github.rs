use super::ExtractedDoc;
use crate::agent::tools::exec::ToolCtx;
use crate::agent::web::extract::article;
use crate::agent::web::net::{safe_fetch, FetchOpts};
use serde::Deserialize;
use url::Url;

#[derive(Debug, PartialEq, Eq)]
pub enum GitHubTarget {
    Blob {
        owner: String,
        repo: String,
        sub_id: String,
    },
    IssueOrPr {
        owner: String,
        repo: String,
        sub_id: String,
    },
    Repo {
        owner: String,
        repo: String,
    },
}

#[derive(Deserialize)]
struct GhIssue {
    title: String,
    state: String,
    body: Option<String>,
}

#[derive(Deserialize)]
struct GhRepo {
    full_name: String,
    description: Option<String>,
    stargazers_count: u64,
}

pub fn parse_github_url(url_str: &str) -> Option<GitHubTarget> {
    let parsed = Url::parse(url_str).ok()?;
    let host = parsed.host_str()?;
    if host != "github.com" && host != "www.github.com" {
        return None;
    }

    let segments: Vec<&str> = parsed.path_segments()?.filter(|s| !s.is_empty()).collect();

    if segments.len() < 2 {
        return None;
    }

    let owner = segments[0].to_string();
    let repo = segments[1].to_string();

    if segments.len() >= 4 && segments[2] == "blob" {
        // e.g. /owner/repo/blob/ref/path...
        let sub_id = segments[3..].join("/");
        return Some(GitHubTarget::Blob {
            owner,
            repo,
            sub_id,
        });
    }

    if segments.len() == 4 && (segments[2] == "issues" || segments[2] == "pull") {
        let sub_id = segments[3].to_string();
        return Some(GitHubTarget::IssueOrPr {
            owner,
            repo,
            sub_id,
        });
    }

    if segments.len() == 2 {
        return Some(GitHubTarget::Repo { owner, repo });
    }

    None
}

pub async fn extract(url: &str, ctx: &ToolCtx) -> Result<ExtractedDoc, String> {
    let target = parse_github_url(url);

    match target {
        Some(GitHubTarget::Blob {
            owner,
            repo,
            sub_id,
        }) => {
            let raw_url = format!(
                "https://raw.githubusercontent.com/{}/{}/{}",
                owner, repo, sub_id
            );
            let opts = FetchOpts {
                proxy: ctx.web.proxy.clone(),
                ..Default::default()
            };
            if let Ok(outcome) = safe_fetch(&raw_url, &opts, &ctx.cancel).await {
                if outcome.status >= 200 && outcome.status < 300 {
                    let text = String::from_utf8_lossy(&outcome.body).into_owned();
                    let title = format!("{}/{} - {}", owner, repo, sub_id);
                    let markdown = format!("```\n{}\n```", text);
                    let word_count = text.split_whitespace().count();
                    return Ok(ExtractedDoc {
                        title,
                        url: url.to_string(),
                        markdown,
                        word_count,
                        status: 200,
                        extractor: "github",
                    });
                }
            }
        }
        Some(GitHubTarget::IssueOrPr {
            owner,
            repo,
            sub_id,
        }) => {
            let api_url = format!(
                "https://api.github.com/repos/{}/{}/issues/{}",
                owner, repo, sub_id
            );
            let mut opts = FetchOpts {
                proxy: ctx.web.proxy.clone(),
                ..Default::default()
            };
            opts.headers
                .insert("user-agent", "archimedes-agent".parse().unwrap());
            if let Ok(outcome) = safe_fetch(&api_url, &opts, &ctx.cancel).await {
                if outcome.status >= 200 && outcome.status < 300 {
                    if let Ok(issue) = serde_json::from_slice::<GhIssue>(&outcome.body) {
                        let body_text = issue.body.unwrap_or_default();
                        let markdown = format!(
                            "## {}\n\n**State:** {}\n\n{}",
                            issue.title, issue.state, body_text
                        );
                        let word_count = markdown.split_whitespace().count();
                        return Ok(ExtractedDoc {
                            title: issue.title,
                            url: url.to_string(),
                            markdown,
                            word_count,
                            status: 200,
                            extractor: "github",
                        });
                    }
                }
            }
        }
        Some(GitHubTarget::Repo { owner, repo }) => {
            let api_url = format!("https://api.github.com/repos/{}/{}", owner, repo);
            let mut opts = FetchOpts {
                proxy: ctx.web.proxy.clone(),
                ..Default::default()
            };
            opts.headers
                .insert("user-agent", "archimedes-agent".parse().unwrap());
            if let Ok(outcome) = safe_fetch(&api_url, &opts, &ctx.cancel).await {
                if outcome.status >= 200 && outcome.status < 300 {
                    if let Ok(r) = serde_json::from_slice::<GhRepo>(&outcome.body) {
                        let desc = r.description.unwrap_or_default();
                        let markdown = format!(
                            "## {}\n\n{}\n\nStars: {}",
                            r.full_name, desc, r.stargazers_count
                        );
                        let text_for_count = format!("{} {}", r.full_name, desc);
                        let word_count = text_for_count.split_whitespace().count();
                        return Ok(ExtractedDoc {
                            title: r.full_name,
                            url: url.to_string(),
                            markdown,
                            word_count,
                            status: 200,
                            extractor: "github",
                        });
                    }
                }
            }
        }
        None => {}
    }

    // Fall back to safe_fetch of the original URL and article extractor
    let opts = FetchOpts {
        proxy: ctx.web.proxy.clone(),
        ..Default::default()
    };
    let outcome = safe_fetch(url, &opts, &ctx.cancel)
        .await
        .map_err(|e| e.to_string())?;

    let html = String::from_utf8_lossy(&outcome.body);
    let mut doc = article::extract(&html, url, outcome.status);
    doc.extractor = "github";
    Ok(doc)
}

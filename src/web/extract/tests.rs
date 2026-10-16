use super::article;
use super::github;
use super::pdf;
use super::youtube;
use super::{extract, Mode};
use crate::agent::tools::exec::ToolCtx;
use std::path::PathBuf;
use tokio_util::sync::CancellationToken;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn test_ctx() -> ToolCtx {
    ToolCtx {
        cwd: PathBuf::from("/tmp"),
        cancel: CancellationToken::new(),
        skill_roots: None,
        boundary: vec![PathBuf::from("/tmp")],
        file_policy: crate::agent::policy::FilePolicy {
            reads: crate::agent::policy::AccessPolicy::Sandboxed,
            writes: crate::agent::policy::AccessPolicy::Sandboxed,
            shell: crate::agent::policy::AccessPolicy::Allow,
        },
        protected: vec![],
        web: crate::agent::web::config::WebConfig::default(),
        render_requester: None,
    }
}

#[test]
fn test_article_extract_article_fixture() {
    let html = include_str!("../fixtures/article.html");
    let doc = article::extract(html, "https://example.com/article", 200);

    assert_eq!(doc.title, "Understanding Modern Web Search Systems");
    assert_eq!(doc.url, "https://example.com/article");
    assert_eq!(doc.status, 200);
    assert_eq!(doc.extractor, "article");
    assert!(doc
        .markdown
        .contains("# Understanding Modern Web Search Systems"));
    assert!(doc.markdown.contains("Crawlers fetch documents at scale"));
    assert!(doc.markdown.contains("## Architecture Components"));
    assert!(doc
        .markdown
        .contains("- Crawler subsystem for URL frontier management"));
    // Header nav and footer should be pruned
    assert!(!doc.markdown.contains("Copyright 2025"));
    assert!(!doc.markdown.contains("[Home](/)"));
    assert!(doc.word_count > 30);
}

#[test]
fn test_article_extract_docs_page_fixture() {
    let html = include_str!("../fixtures/docs_page.html");
    let doc = article::extract(html, "https://example.com/docs", 200);

    assert_eq!(doc.title, "API Documentation - Safe Fetch Client");
    assert!(doc.markdown.contains("# Safe Fetch Client API"));
    assert!(doc.markdown.contains("```"));
    assert!(doc.markdown.contains("safe_fetch(\"https://example.com\""));
    // Sidebar nav should be pruned
    assert!(!doc.markdown.contains("[Introduction](#intro)"));
}

#[test]
fn test_article_extract_table_fixture() {
    let html = include_str!("../fixtures/table_page.html");
    let doc = article::extract(html, "https://example.com/table", 200);

    assert_eq!(doc.title, "Quarterly Performance Metrics");
    assert!(doc.markdown.contains("| Metric | Q1 Target | Q1 Actual |"));
    assert!(doc
        .markdown
        .contains("| Requests per Second | 10,000 | 12,450 |"));
}

#[tokio::test]
async fn test_pdf_extract_sample() {
    let cancel = CancellationToken::new();
    let bytes = include_bytes!("../fixtures/pdf_sample.pdf").to_vec();
    let doc = pdf::extract(bytes, "https://example.com/doc.pdf", 200, &cancel)
        .await
        .expect("pdf extraction should succeed");

    assert_eq!(doc.title, "PDF Document");
    assert_eq!(doc.extractor, "pdf");
    assert_eq!(doc.url, "https://example.com/doc.pdf");
    assert!(doc.markdown.contains("--- Page 1 ---"));
    assert!(doc.markdown.contains("Hello PDF World"));
}

#[tokio::test]
async fn test_pdf_extract_invalid_bytes_panic_containment() {
    let cancel = CancellationToken::new();
    let garbage = vec![0x25, 0x50, 0x44, 0x46, 0xff, 0xff, 0x00, 0x01];
    let res = pdf::extract(garbage, "https://example.com/bad.pdf", 200, &cancel).await;
    // Should return Err cleanly, never panic out of spawn_blocking
    assert!(res.is_err());
}

#[tokio::test]
async fn test_pdf_extract_cancellation() {
    let cancel = CancellationToken::new();
    cancel.cancel();
    let bytes = include_bytes!("../fixtures/pdf_sample.pdf").to_vec();
    let res = pdf::extract(bytes, "https://example.com/doc.pdf", 200, &cancel).await;
    assert_eq!(res.unwrap_err(), "pdf extraction cancelled");
}

#[tokio::test]
async fn test_github_extract_blob_success() {
    // Check parse_github_url logic
    let target =
        github::parse_github_url("https://github.com/rust-lang/rust/blob/master/Cargo.toml");
    match target {
        Some(github::GitHubTarget::Blob {
            owner,
            repo,
            sub_id,
        }) => {
            assert_eq!(owner, "rust-lang");
            assert_eq!(repo, "rust");
            assert_eq!(sub_id, "master/Cargo.toml");
        }
        _ => panic!("expected Blob target"),
    }
}

#[tokio::test]
async fn test_github_extract_issue_target() {
    let target = github::parse_github_url("https://github.com/rust-lang/rust/issues/12345");
    match target {
        Some(github::GitHubTarget::IssueOrPr {
            owner,
            repo,
            sub_id,
        }) => {
            assert_eq!(owner, "rust-lang");
            assert_eq!(repo, "rust");
            assert_eq!(sub_id, "12345");
        }
        _ => panic!("expected Issue target"),
    }
}

#[tokio::test]
async fn test_github_extract_repo_target() {
    let target = github::parse_github_url("https://github.com/rust-lang/rust");
    match target {
        Some(github::GitHubTarget::Repo { owner, repo }) => {
            assert_eq!(owner, "rust-lang");
            assert_eq!(repo, "rust");
        }
        _ => panic!("expected Repo target"),
    }
}

#[tokio::test]
async fn test_youtube_extract_video_id() {
    assert_eq!(
        youtube::extract_video_id("https://youtu.be/dQw4w9WgXcQ"),
        Some("dQw4w9WgXcQ".to_string())
    );
    assert_eq!(
        youtube::extract_video_id("https://www.youtube.com/watch?v=dQw4w9WgXcQ"),
        Some("dQw4w9WgXcQ".to_string())
    );
}

#[tokio::test]
async fn test_extract_raw_mode_wiremock() {
    let server = MockServer::start().await;
    let html = "<html><head><title>Raw Page</title></head><body>Hello Raw Content</body></html>";
    Mock::given(method("GET"))
        .and(path("/raw"))
        .respond_with(ResponseTemplate::new(200).set_body_string(html))
        .mount(&server)
        .await;

    let ctx = test_ctx();
    let url = format!("{}/raw", server.uri());
    let doc = extract(&url, Mode::Raw, &ctx)
        .await
        .expect("raw extraction");

    assert_eq!(doc.title, "Raw Page");
    assert_eq!(doc.extractor, "raw");
    assert_eq!(doc.markdown, html);
    assert_eq!(doc.status, 200);
}

#[tokio::test]
async fn test_extract_pdf_detection_wiremock() {
    let server = MockServer::start().await;
    let pdf_bytes = include_bytes!("../fixtures/pdf_sample.pdf");
    Mock::given(method("GET"))
        .and(path("/document.pdf"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_bytes(pdf_bytes.to_vec())
                .insert_header("content-type", "application/pdf"),
        )
        .mount(&server)
        .await;

    let ctx = test_ctx();
    let url = format!("{}/document.pdf", server.uri());
    let doc = extract(&url, Mode::Readable, &ctx)
        .await
        .expect("pdf extraction");

    assert_eq!(doc.extractor, "pdf");
    assert_eq!(doc.title, "PDF Document");
    assert!(doc.markdown.contains("Hello PDF World"));
}

#[test]
fn test_looks_blocked_status_codes() {
    use super::ExtractedDoc;

    let doc_403 = ExtractedDoc {
        title: "Forbidden".into(),
        url: "https://example.com".into(),
        markdown: "word ".repeat(100),
        word_count: 100,
        status: 403,
        extractor: "article",
    };
    assert!(super::looks_blocked(&doc_403));

    let doc_429 = ExtractedDoc {
        title: "Too Many Requests".into(),
        url: "https://example.com".into(),
        markdown: "word ".repeat(100),
        word_count: 100,
        status: 429,
        extractor: "article",
    };
    assert!(super::looks_blocked(&doc_429));

    let doc_503 = ExtractedDoc {
        title: "Service Unavailable".into(),
        url: "https://example.com".into(),
        markdown: "word ".repeat(100),
        word_count: 100,
        status: 503,
        extractor: "article",
    };
    assert!(super::looks_blocked(&doc_503));
}

#[test]
fn test_looks_blocked_short_content() {
    use super::ExtractedDoc;

    let doc_short = ExtractedDoc {
        title: "Empty Shell".into(),
        url: "https://example.com".into(),
        markdown: "Please enable JavaScript to view this site.".into(),
        word_count: 7,
        status: 200,
        extractor: "article",
    };
    assert!(super::looks_blocked(&doc_short));

    let doc_long = ExtractedDoc {
        title: "Full Article".into(),
        url: "https://example.com".into(),
        markdown: "word ".repeat(85),
        word_count: 85,
        status: 200,
        extractor: "article",
    };
    assert!(!super::looks_blocked(&doc_long));
}

#[tokio::test]
async fn test_extract_raw_mode_bypasses_specialized_extractors() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/file.pdf"))
        .respond_with(ResponseTemplate::new(200).set_body_string("%PDF-1.4 raw body bytes"))
        .mount(&server)
        .await;

    let url = format!("{}/file.pdf", server.uri());
    let ctx = test_ctx();

    let doc = extract(&url, Mode::Raw, &ctx).await.unwrap();
    assert_eq!(doc.extractor, "raw");
    assert_eq!(doc.markdown, "%PDF-1.4 raw body bytes");
}

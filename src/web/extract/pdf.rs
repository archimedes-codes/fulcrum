use tokio_util::sync::CancellationToken;

use super::ExtractedDoc;

pub async fn extract(
    bytes: Vec<u8>,
    url: &str,
    status: u16,
    cancel: &CancellationToken,
) -> Result<ExtractedDoc, String> {
    let url_owned = url.to_string();

    tokio::select! {
        result = tokio::task::spawn_blocking(move || pdf_extract::extract_text_from_mem_by_pages(&bytes)) => {
            match result {
                Ok(Ok(pages)) => {
                    let mut markdown = String::new();
                    for (idx, page_text) in pages.iter().enumerate() {
                        let trimmed = page_text.trim();
                        markdown.push_str(&format!("--- Page {} ---\n\n{}\n\n", idx + 1, trimmed));
                    }
                    let word_count = markdown.split_whitespace().count();
                    Ok(ExtractedDoc {
                        title: "PDF Document".to_string(),
                        url: url_owned,
                        markdown,
                        word_count,
                        status,
                        extractor: "pdf",
                    })
                }
                Ok(Err(e)) => Err(format!("pdf extraction failed: {e}")),
                Err(join_err) => {
                    if join_err.is_panic() {
                        Err("pdf: the extractor panicked on this file".to_string())
                    } else {
                        Err(format!("pdf task error: {join_err}"))
                    }
                }
            }
        }
        _ = cancel.cancelled() => {
            Err("pdf extraction cancelled".into())
        }
    }
}

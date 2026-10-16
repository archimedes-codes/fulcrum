use dom_query::{Document, NodeRef, Selection};
use url::Url;

use super::ExtractedDoc;

/// Extract readable content and convert to markdown.
pub fn extract(html: &str, url: &str, status: u16) -> ExtractedDoc {
    let doc = Document::from(html);

    // Title: <title> -> meta[property="og:title"] -> URL host
    let title = extract_title(&doc, url);

    // Prune unneeded elements
    doc.select("script, style, noscript, nav, footer, header, aside, iframe, form, svg")
        .remove();

    // Score candidate elements (article, main, section, div)
    let candidates = doc.select("article, main, section, div");
    let mut best_node: Option<NodeRef> = None;
    let mut best_score: f64 = 0.0;

    for sel in candidates.iter() {
        let total_text = sel.text();
        let total_len = total_text.trim().len();
        if total_len < 200 {
            continue;
        }

        let anchor_text_len: usize = sel.select("a").iter().map(|a| a.text().trim().len()).sum();

        let link_density = if total_len > 0 {
            (anchor_text_len as f64) / (total_len as f64)
        } else {
            0.0
        };

        // Paragraph text len
        let p_text_len: usize = sel.select("p").iter().map(|p| p.text().trim().len()).sum();

        let score = (p_text_len as f64) * (1.0 - link_density);
        if score > best_score {
            best_score = score;
            if let Some(first) = sel.nodes().first() {
                best_node = Some(*first);
            }
        }
    }

    let base_url = Url::parse(url).ok();
    let markdown = if let Some(winner) = best_node {
        render_node_to_markdown(&winner, &base_url)
    } else {
        let body = doc.select("body");
        if body.length() > 0 {
            render_node_to_markdown(&body.nodes()[0], &base_url)
        } else {
            render_node_to_markdown(&doc.root(), &base_url)
        }
    };

    let trimmed_markdown = markdown.trim().to_string();
    let word_count = trimmed_markdown.split_whitespace().count();

    ExtractedDoc {
        title,
        url: url.to_string(),
        markdown: trimmed_markdown,
        word_count,
        status,
        extractor: "article",
    }
}

fn extract_title(doc: &Document, url: &str) -> String {
    let title_sel = doc.select("head > title");
    if title_sel.length() > 0 {
        let t = collapse_whitespace(&title_sel.text());
        if !t.is_empty() {
            return t;
        }
    }

    let og_title = doc.select("meta[property=\"og:title\"]");
    if og_title.length() > 0 {
        if let Some(content) = og_title.attr("content") {
            let t = collapse_whitespace(&content);
            if !t.is_empty() {
                return t;
            }
        }
    }

    if let Ok(parsed) = Url::parse(url) {
        if let Some(host) = parsed.host_str() {
            return host.to_string();
        }
    }

    "Untitled".to_string()
}

fn collapse_whitespace(s: &str) -> String {
    s.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn resolve_url(base: &Option<Url>, link: &str) -> String {
    if let Some(base) = base {
        if let Ok(joined) = base.join(link) {
            return joined.to_string();
        }
    }
    link.to_string()
}

fn render_node_to_markdown(node: &NodeRef, base_url: &Option<Url>) -> String {
    let mut out = String::new();
    walk_node(node, base_url, &mut out);
    out
}

fn walk_node(node: &NodeRef, base_url: &Option<Url>, out: &mut String) {
    if node.is_text() {
        let txt = node.text();
        out.push_str(&txt);
        return;
    }

    if !node.is_element() {
        for child in node.children() {
            walk_node(&child, base_url, out);
        }
        return;
    }

    let tag_name = node
        .node_name()
        .map(|n| n.to_string().to_lowercase())
        .unwrap_or_default();

    match tag_name.as_str() {
        "h1" | "h2" | "h3" | "h4" | "h5" | "h6" => {
            let level = tag_name[1..].parse::<usize>().unwrap_or(1);
            let hashes = "#".repeat(level);
            let text = collapse_whitespace(&node.text());
            out.push_str(&format!("\n\n{} {}\n\n", hashes, text));
        }
        "p" => {
            let mut p_content = String::new();
            for child in node.children() {
                walk_node(&child, base_url, &mut p_content);
            }
            let trimmed = p_content.trim();
            if !trimmed.is_empty() {
                out.push_str(&format!("\n\n{}\n\n", trimmed));
            }
        }
        "pre" => {
            // Fenced code block
            let code_text = node.text();
            out.push_str(&format!(
                "\n\n```\n{}\n```\n\n",
                code_text.trim_matches('\n')
            ));
        }
        "code" => {
            // If code is not inside pre, inline code
            let is_in_pre = node.ancestors(Some(3)).iter().any(|a| a.has_name("pre"));
            if is_in_pre {
                for child in node.children() {
                    walk_node(&child, base_url, out);
                }
            } else {
                let code_text = node.text();
                out.push_str(&format!("`{}`", code_text));
            }
        }
        "ul" => {
            out.push_str("\n\n");
            for li in node.children().into_iter().filter(|c| c.has_name("li")) {
                let mut li_text = String::new();
                for child in li.children() {
                    walk_node(&child, base_url, &mut li_text);
                }
                out.push_str(&format!("- {}\n", collapse_whitespace(&li_text)));
            }
            out.push('\n');
        }
        "ol" => {
            out.push_str("\n\n");
            for (idx, li) in (1..).zip(node.children().into_iter().filter(|c| c.has_name("li"))) {
                let mut li_text = String::new();
                for child in li.children() {
                    walk_node(&child, base_url, &mut li_text);
                }
                out.push_str(&format!("{}. {}\n", idx, collapse_whitespace(&li_text)));
            }
            out.push('\n');
        }
        "table" => {
            render_table(node, base_url, out);
        }
        "a" => {
            let href = node.attr("href").map(|s| s.to_string()).unwrap_or_default();
            let mut text = String::new();
            for child in node.children() {
                walk_node(&child, base_url, &mut text);
            }
            let text = collapse_whitespace(&text);
            if !href.is_empty() && !text.is_empty() {
                let resolved = resolve_url(base_url, &href);
                out.push_str(&format!("[{}]({})", text, resolved));
            } else if !text.is_empty() {
                out.push_str(&text);
            }
        }
        "img" => {
            let src = node.attr("src").map(|s| s.to_string()).unwrap_or_default();
            let alt = node.attr("alt").map(|s| s.to_string()).unwrap_or_default();
            if !src.is_empty() {
                let resolved = resolve_url(base_url, &src);
                out.push_str(&format!("![{}]({})", alt, resolved));
            }
        }
        "br" => {
            out.push_str("  \n");
        }
        _ => {
            for child in node.children() {
                walk_node(&child, base_url, out);
            }
        }
    }
}

fn render_table(table: &NodeRef, base_url: &Option<Url>, out: &mut String) {
    let sel = Selection::from(*table);
    let trs = sel.select("tr");
    if trs.length() == 0 {
        return;
    }

    out.push_str("\n\n");

    let mut headers: Vec<String> = Vec::new();
    let mut rows: Vec<Vec<String>> = Vec::new();

    for tr in trs.iter() {
        let ths = tr.select("th");
        if ths.length() > 0 && headers.is_empty() {
            for th in ths.iter() {
                if let Some(n) = th.nodes().first() {
                    let mut cell_out = String::new();
                    walk_node(n, base_url, &mut cell_out);
                    headers.push(collapse_whitespace(&cell_out));
                }
            }
            continue;
        }

        let tds = tr.select("td");
        if tds.length() > 0 {
            let mut row = Vec::new();
            for td in tds.iter() {
                if let Some(n) = td.nodes().first() {
                    let mut cell_out = String::new();
                    walk_node(n, base_url, &mut cell_out);
                    row.push(collapse_whitespace(&cell_out));
                }
            }
            rows.push(row);
        }
    }

    let col_count = if !headers.is_empty() {
        headers.len()
    } else if let Some(first_row) = rows.first() {
        first_row.len()
    } else {
        0
    };

    if col_count == 0 {
        return;
    }

    if !headers.is_empty() {
        out.push_str("| ");
        out.push_str(&headers.join(" | "));
        out.push_str(" |\n");

        out.push_str("| ");
        let sep: Vec<&str> = vec!["---"; col_count];
        out.push_str(&sep.join(" | "));
        out.push_str(" |\n");
    }

    for row in rows {
        let mut row_padded = row;
        while row_padded.len() < col_count {
            row_padded.push(String::new());
        }
        out.push_str("| ");
        out.push_str(&row_padded[..col_count].join(" | "));
        out.push_str(" |\n");
    }

    out.push('\n');
}

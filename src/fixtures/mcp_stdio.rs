//! A deterministic MCP stdio test double (no LLM, no sleeps) for the
//! `StdioClient` tests.
//!
//! Wire behavior mirrors the MCP stdio transport: one JSON-RPC 2.0
//! object per line. `initialize` → the server capabilities response;
//! `notifications/initialized` → no response; `tools/list` → three canned
//! tools; `tools/call` → `echo` (its `text` argument back as a
//! `content[]` text item) / `hang` (NO response — the timeout test) / `err`
//! (a JSON-RPC error). Closing stdin exits 0.
//!
//! `--tools=<csv>` replaces the advertised `tools/list` set with EXACTLY the
//! named tools (see the `tools_list_result` helper below) — the seam a test
//! uses when it needs two HEALTHY servers whose tool sets DIFFER, which is the
//! only way to tell "the server I scoped to has no such tool" from "some OTHER
//! server has it".
//!
//! `--name=<label>` labels this server, for the tests that run TWO instances
//! of this double through ONE manager and must prove which instance answered:
//! every advertised name (`serverInfo.name`, each `tools/list` entry) is
//! PREFIXED with `<label>-`, a `tools/call` `params.name` is matched AFTER
//! STRIPPING that prefix (so `A_echo` dispatches as `echo` — the prefix only
//! tells the server which label it is, and since each server is its own
//! process `A_echo` can only ever reach server `A`), and every `tools/call`
//! `content[].text` is PREFIXED with `<label>:` (an `echo` of `hi` answers
//! `A:hi`). With the flag ABSENT every response is byte-identical to the
//! unlabelled fixture — that is the no-regression rule the existing
//! `--tools=` seam tests depend on.
//!
//! Two extra `tools/call` tools exist ALWAYS (dispatchable regardless of
//! `--tools=`, and deliberately NOT advertised by `tools/list` — the canned
//! three-tool advertisement is what the existing tests count):
//! - `hold` — records the request `id` and answers NOTHING; the answer
//!   `released` arrives only when the matching `release` shows up. A second
//!   `hold` while one is pending answers `busy` (defensive — no test does
//!   this).
//! - `release` — completes the pair, ORDER-INDEPENDENTLY: whichever of
//!   `hold`/`release` arrives first waits for the other, and then BOTH ids are
//!   answered `released`. A `release` with no `hold` pending is remembered
//!   exactly like a pending `hold`, so every interleaving ends with both
//!   responses `released` — never a hang, never a `noop`.
//!
//! That pairing is what lets a concurrency test prove two `call_tool`s are in
//! flight at once WITHOUT any timing: the held call can only complete when the
//! test's second `call_tool` arrives, so the sequence of lines the test writes
//! alone decides the outcome. No sleeps, no timers, no response counting.

use std::io::{BufRead, Write};

use serde_json::json;

const INIT_RESULT: &str = r#"{"protocolVersion":"2025-06-18","capabilities":{},"serverInfo":{"name":"fake-mcp","version":"0.1.0"}}"#;

const TOOLS_LIST_RESULT: &str = r#"{"tools":[{"name":"echo","description":"echo its text argument","inputSchema":{"type":"object","properties":{"text":{"type":"string"}}}},{"name":"hang","description":"never answers","inputSchema":{"type":"object"}},{"name":"err","description":"always errors","inputSchema":{"type":"object"}}]}"#;

/// The `--name=<label>` this instance was started with (`None` = the
/// unlabelled fixture — every response keeps its exact historical bytes).
fn label(args: &[String]) -> Option<String> {
    args.iter()
        .find_map(|a| a.strip_prefix("--name="))
        .map(str::to_string)
}

/// The `initialize` result: the canned bytes unlabelled (byte-identical), or
/// the same document with `serverInfo.name` prefixed `<label>-`.
fn init_result(label: Option<&str>) -> String {
    match label {
        None => INIT_RESULT.to_string(),
        Some(x) => json!({
            "protocolVersion": "2025-06-18",
            "capabilities": {},
            "serverInfo": { "name": format!("{x}-fake-mcp"), "version": "0.1.0" },
        })
        .to_string(),
    }
}

/// The `tools/list` result: the canned three-tool set, or — when started with
/// `--tools=<csv>` — EXACTLY the named tools, each with a canned description +
/// schema. An empty value (`--tools=`) advertises NO tools; the flag being
/// absent is the only way to get the canned three. With `--name=<label>` every
/// advertised name is prefixed `<label>-` (the `hold` / `release` tools are
/// dispatch-only and never advertised, so they never enter this list).
fn tools_list_result(args: &[String]) -> String {
    let label = label(args);
    let named = |name: &str| match &label {
        Some(x) => format!("{x}-{name}"),
        None => name.to_string(),
    };
    let Some(arg) = args.iter().find_map(|a| a.strip_prefix("--tools=")) else {
        return match &label {
            None => TOOLS_LIST_RESULT.to_string(),
            Some(_) => json!({
                "tools": [
                    {
                        "name": named("echo"),
                        "description": "echo its text argument",
                        "inputSchema": {
                            "type": "object",
                            "properties": { "text": { "type": "string" } },
                        },
                    },
                    {
                        "name": named("hang"),
                        "description": "never answers",
                        "inputSchema": { "type": "object" },
                    },
                    {
                        "name": named("err"),
                        "description": "always errors",
                        "inputSchema": { "type": "object" },
                    },
                ]
            })
            .to_string(),
        };
    };
    let tools: Vec<serde_json::Value> = arg
        .split(',')
        .filter(|name| !name.is_empty())
        .map(|name| {
            json!({
                "name": named(name),
                "description": format!("{name} on the narrowed fake server"),
                "inputSchema": { "type": "object" },
            })
        })
        .collect();
    json!({ "tools": tools }).to_string()
}

/// One `tools/call` response (the `id` + the `content[]` text item, prefixed
/// `<label>:` when this instance is labelled).
///
/// The UNLABELLED path writes the historical format string verbatim — the key
/// ORDER included (a `json!` map sorts its keys, which would emit
/// `{"text":…,"type":…}` instead of the original `{"type":…,"text":…}`), so a
/// fixture started without `--name=` is byte-identical to the pre-`--name`
/// double.
fn write_text(stdout: &mut impl Write, id: &serde_json::Value, label: Option<&str>, text: &str) {
    match label {
        None => {
            let _ = writeln!(
                stdout,
                "{{\"jsonrpc\":\"2.0\",\"id\":{},\"result\":{{\"content\":[{{\"type\":\"text\",\"text\":\"{}\"}}]}}}}",
                id, text
            );
        }
        Some(x) => {
            let body = json!({ "content": [ { "type": "text", "text": format!("{x}:{text}") } ] });
            let _ = writeln!(
                stdout,
                "{{\"jsonrpc\":\"2.0\",\"id\":{},\"result\":{}}}",
                id, body
            );
        }
    }
    let _ = stdout.flush();
}

/// One JSON-RPC error response.
fn write_error(stdout: &mut impl Write, id: &serde_json::Value, code: i64, message: &str) {
    let _ = writeln!(
        stdout,
        "{{\"jsonrpc\":\"2.0\",\"id\":{},\"error\":{{\"code\":{code},\"message\":\"{message}\"}}}}",
        id
    );
    let _ = stdout.flush();
}

/// The fixture entry point (was this file's `fn main()` — moved VERBATIM from
/// `src-tauri/src/bin/fake_mcp_stdio.rs` in S8b.1 so ONE body backs both the
/// desktop's `fake_mcp_stdio` bin and the crate's `fake_mcp_stdio_core` bin;
/// the two bins are 3-line shims over this function).
pub fn mcp_stdio_main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let label = label(&args);
    let tools_list_result = tools_list_result(&args);
    let init_result = init_result(label.as_deref());
    let stdin = std::io::stdin();
    let mut stdout = std::io::stdout();
    // The hold/release pairing (ONE outstanding of each — the tests issue a
    // single pair per server). Whichever arrives first waits here for the
    // other, and then BOTH ids are answered `released`.
    let mut pending_hold: Option<serde_json::Value> = None;
    let mut pending_release: Option<serde_json::Value> = None;
    for line in stdin.lock().lines() {
        let Ok(line) = line else {
            break;
        };
        if line.trim().is_empty() {
            continue;
        }
        let Ok(v) = serde_json::from_str::<serde_json::Value>(&line) else {
            continue;
        };
        let id = v.get("id").cloned();
        let Some(method) = v.get("method").and_then(|m| m.as_str()) else {
            continue;
        };
        match method {
            "initialize" => {
                if let Some(id) = id {
                    let _ = writeln!(
                        stdout,
                        "{{\"jsonrpc\":\"2.0\",\"id\":{},\"result\":{}}}",
                        id, init_result
                    );
                    let _ = stdout.flush();
                }
            }
            "notifications/initialized" => {} // a notification: no response.
            "tools/list" => {
                if let Some(id) = id {
                    let _ = writeln!(
                        stdout,
                        "{{\"jsonrpc\":\"2.0\",\"id\":{},\"result\":{}}}",
                        id, tools_list_result
                    );
                    let _ = stdout.flush();
                }
            }
            "tools/call" => {
                let Some(id) = id else {
                    continue;
                };
                let name = v["params"]["name"].as_str().unwrap_or_default();
                // A labelled server strips its own label prefix from the
                // dispatched name (`A_hold` → `hold`). The separator is `-` or
                // `_` and the label match is case-insensitive: the callable
                // spelling the `mcp` tool documents is `{server}_{tool}`, while
                // an advertised name is prefixed `<label>-`, so the dispatcher
                // accepts both spellings rather than forcing one on the caller.
                let stripped = match &label {
                    Some(x) => {
                        let bytes = name.as_bytes();
                        let cut = x.len();
                        if bytes.len() > cut
                            && bytes[..cut].eq_ignore_ascii_case(x.as_bytes())
                            && matches!(bytes[cut], b'-' | b'_')
                        {
                            name[cut + 1..].to_string()
                        } else {
                            name.to_string()
                        }
                    }
                    None => name.to_string(),
                };
                match stripped.as_str() {
                    "echo" => {
                        let text = v["params"]["arguments"]["text"]
                            .as_str()
                            .unwrap_or_default();
                        write_text(&mut stdout, &id, label.as_deref(), text);
                    }
                    "hang" => {} // NO response (the timeout test).
                    "err" => write_error(&mut stdout, &id, -32000, "boom"),
                    "hold" => {
                        if pending_hold.is_some() {
                            // Defensive: a hold is already waiting (no test
                            // triggers this — the concurrency test issues one
                            // hold per server).
                            write_text(&mut stdout, &id, label.as_deref(), "busy");
                        } else if let Some(queued) = pending_release.take() {
                            // The `release` arrived FIRST: complete the pair.
                            write_text(&mut stdout, &queued, label.as_deref(), "released");
                            write_text(&mut stdout, &id, label.as_deref(), "released");
                        } else {
                            pending_hold = Some(id);
                        }
                    }
                    "release" => {
                        if let Some(held) = pending_hold.take() {
                            // The `hold` arrived FIRST: complete the pair.
                            write_text(&mut stdout, &held, label.as_deref(), "released");
                            write_text(&mut stdout, &id, label.as_deref(), "released");
                        } else {
                            pending_release = Some(id);
                        }
                    }
                    _ => write_error(&mut stdout, &id, -32602, "unknown tool"),
                }
            }
            _ => {} // unknown methods: no response.
        }
    }
}

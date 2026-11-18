//! A `display-upsert` frame reaches the `ARCHIMEDES_DEBUG` log as a SUMMARY,
//! not as its full body.
//!
//! The measured problem this pins: **75.30 %** of a rotated `supervisor.log`
//! is `display-upsert` frames — one accumulating display row re-serialised IN
//! FULL on every streamed token (one `message_key` re-emitted 1 388× =
//! 2 691 601 B = 51.3 % of a whole 5 MB file), which rotated the log in ~71 s
//! of streaming. The fix is at the Supervisor's inbound frame-log site
//! (`worker/client.rs`): a short identity+size line for that one frame type,
//! every other type verbatim.
//!
//! The crate's unit tests on `summarise_display_upsert` prove the line's
//! CONTENT. This file proves the other half — that the line actually REACHES
//! THE FILE through the real read loop of a real child process, and that the
//! quadratic body is gone from it.
//!
//! # This binary runs ONE test
//!
//! The invariant is copied from the desktop's
//! `src-tauri/tests/worker_stderr.rs:11-12`: the debug log's enabled flag and
//! dir are PROCESS-GLOBAL (`debuglog::init`), so this binary owns that state
//! outright rather than racing a sibling test's `init(false, None)`. That is
//! also why this is a NEW file: `tests/fixture_bins.rs` deliberately holds no
//! `debuglog::init` global, and a log-owning test added there would race the
//! real one.

use std::path::PathBuf;
use std::time::Duration;

use fulcrum_harness::catalog::{Model, ModelCatalog};
use fulcrum_harness::debuglog;
use fulcrum_harness::worker::client::WorkerHandle;
use fulcrum_harness::worker::protocol::{StartEnv, StartMode};

/// The platform's executable suffix ("" on unix, `.exe` on Windows).
///
/// Load-bearing, and copied verbatim from `tests/fixture_bins.rs`:
/// `Path::is_file()` does NOT append it while `CreateProcessW` DOES append it
/// when spawning, so a suffix-less name spawns fine on Windows and then fails
/// any existence check — a check that passes or fails for reasons unrelated to
/// what it claims to check.
const EXE_SUFFIX: &str = std::env::consts::EXE_SUFFIX;

/// Where cargo put the binaries for the test currently running.
///
/// Replicated from `tests/fixture_bins.rs:63-73` — the crate's own resolver.
/// The desktop's `env!("CARGO_BIN_EXE_fake_worker")` idiom CANNOT work here:
/// `CARGO_BIN_EXE_*` is package-local and this package's bins are
/// `fake_worker_core` / `fake_mcp_stdio_core`, and `test_support::fixture_bin`
/// is `#[cfg(test)] pub(crate)`, so an integration target cannot see it.
///
/// Derived from `current_exe()` (this binary lives in `target/debug/deps/`),
/// NOT by counting levels above `CARGO_MANIFEST_DIR`: the crate sits at
/// `<root>/crates/fulcrum-core` inside a workspace and at `<root>` standalone,
/// so a level count names a directory only one of the two layouts writes.
fn target_debug_dir() -> PathBuf {
    let exe = std::env::current_exe().expect("a test binary has a path");
    exe.parent() // target/debug/deps
        .and_then(|deps| deps.parent()) // target/debug
        .expect("the test binary lives in target/debug/deps")
        .to_path_buf()
}

fn worker_fixture() -> PathBuf {
    target_debug_dir().join(format!("fake_worker_core{EXE_SUFFIX}"))
}

fn model() -> Model {
    Model {
        id: "m1".into(),
        provider: "fake".into(),
        base_url: "http://127.0.0.1:1/v1".into(),
        api_key: "k".into(),
        context_window: 100_000,
        cost_per_mtok_in: 0.0,
        cost_per_mtok_out: 0.0,
        supports_tools: true,
        supports_thinking: false,
        thinking_levels: Vec::new(),
        api: Some("openai-completions".to_string()),
    }
}

/// A `start` envelope (the fixture handles a `prompt` only once its session
/// has started). The shape follows the desktop's `worker_stderr.rs:39-61`;
/// `from_parts`' trailing `None` is the `permission` block.
fn start_env(session_id: &str) -> StartEnv {
    let cwd = std::env::temp_dir().display().to_string();
    StartEnv::from_parts(
        session_id.to_string(),
        cwd.clone(),
        StartMode::Fresh,
        None,
        model(),
        ModelCatalog {
            models: vec![model()],
            ..Default::default()
        },
        None,
        true,
        Some(vec!["bash".to_string()]),
        cwd,
        false,
        None,
        None,
    )
}

/// The `display-upsert` line in the log is the summary, and it carries NO row
/// payload.
///
/// The fixture's canned turn (`src/fixtures/worker.rs:549-560`) emits exactly
/// one `DisplayUpsert`: `kind: "agent-text"`, `message_key: "{incarnation}:m1"`,
/// `payload_json: {"text":"canned answer"}`. So the summary line must name that
/// row and must not contain its text.
///
/// NOTE on what is asserted WHERE: `"canned answer"` appears in the fixture's
/// `message_end` event (`worker.rs:522`) and its `TranscriptInsert` row
/// (`worker.rs:546`) as well, and BOTH of those frames are still logged
/// verbatim — that is the intended behaviour (only `display-upsert` is
/// summarised). A whole-log `!contains("canned answer")` would therefore be
/// asserting something the change never claimed. The property is per-line: the
/// `display-upsert` line holds no payload, and no line in the file is a
/// `display-upsert` frame with a `payload_json` in it.
#[tokio::test]
async fn a_display_upsert_reaches_the_log_as_a_summary_with_no_payload() {
    let dir = tempfile::TempDir::new().expect("a temp dir for the debug log");
    debuglog::init(true, Some(dir.path().to_path_buf()));

    let handle = WorkerHandle::spawn(&worker_fixture()).expect("the fixture spawns");
    handle
        .wait_ready(Duration::from_secs(15))
        .await
        .expect("the ready frame arrives");
    handle
        .send_start(&start_env("s-summary"))
        .expect("the start frame is queued");
    // The DEFAULT prompt — the canned turn that emits the `DisplayUpsert`.
    handle
        .send_prompt("hello", &[])
        .expect("the prompt encodes");

    // The read task writes the log beside this task, so poll (the shape from
    // the desktop's `worker_stderr.rs:86-99`): the marker arrives a moment
    // after the prompt, and a missing frame is a timeout failure, not a hang.
    let log = dir.path().join("supervisor.log");
    let marker = "← {\"type\":\"display-upsert\"";
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    let text = loop {
        let now = std::fs::read_to_string(&log).unwrap_or_default();
        if now.contains(marker) || tokio::time::Instant::now() > deadline {
            break now;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    };

    let summary = text
        .lines()
        .find(|l| l.contains(marker))
        .unwrap_or_else(|| panic!("the summary line must reach the log, got:\n{text}"));
    // The whole `<-` body IS the JSON — `type` first, so the instruments'
    // `"type":"` regex finds it.
    let body = summary
        .split_once("← ")
        .expect("the inbound arrow prefix")
        .1;
    assert!(
        body.starts_with(r#"{"type":"display-upsert""#),
        "`type` must be the first key of the logged body:\n{body}"
    );
    assert!(
        body.contains(r#""sessionId":"s-summary""#),
        "the session id:\n{body}"
    );
    assert!(body.contains(r#""rows":1"#), "the row count:\n{body}");
    // The row's identity survives (the instruments' `"message_key":"…"` regex).
    assert!(
        body.contains(r#""kind":"agent-text""#) && body.contains(r#""message_key":""#),
        "the row's kind + key must survive:\n{body}"
    );
    // And `bytes` is the PRE-truncation wire length — bigger than the summary
    // itself, which is what records the size of the body that is no longer here.
    let bytes: usize = serde_json::from_str::<serde_json::Value>(body)
        .expect("the logged body is valid JSON")
        .get("bytes")
        .and_then(|v| v.as_u64())
        .expect("`bytes` is a number") as usize;
    assert!(
        bytes > body.len(),
        "`bytes` is the frame's true wire length and must exceed the summary's \
         own {} bytes (the fixture's canned frame is ~400 B): {bytes}\n{body}",
        body.len()
    );

    // THE QUADRATIC BODY IS GONE — from every `display-upsert` line, not just
    // this one (a second, verbatim frame of that type would mean the `match`
    // arm does not cover the path).
    let upsert_lines: Vec<&str> = text
        .lines()
        .filter(|l| l.contains("\"type\":\"display-upsert\""))
        .collect();
    assert!(
        !upsert_lines.is_empty(),
        "the canned turn emits one `display-upsert`"
    );
    for line in upsert_lines {
        assert!(
            !line.contains("payload_json") && !line.contains("canned answer"),
            "a `display-upsert` line still carries the row payload — the 75 % \
             of the file this change removes:\n{line}"
        );
    }

    let _ = handle.send_close();
    let _ = handle.kill().await;
}

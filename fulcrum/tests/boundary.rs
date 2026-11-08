//! T9 Task 6 — the boundary pins of the `fulcrum` daemon member.
//!
//! ## Why this file exists at all
//!
//! The engine's own pins (`src/boundary_pins.rs`) walk `{CARGO_MANIFEST_DIR}/src`
//! (`crate_code`, `:166-209` there) and read only `{CARGO_MANIFEST_DIR}/Cargo.toml`
//! (`the_manifest_declares_no_forbidden_dependency`, `:302-328`). `CARGO_MANIFEST_DIR`
//! for that file is the REPO ROOT, so this member's `src/` is outside its walk and
//! `fulcrum/Cargo.toml` is never opened. Measured at HEAD before this file existed:
//! the daemon could name the desktop UI framework or import the desktop package and
//! every existing pin stayed green, because none of them reads a byte of this
//! member.
//!
//! This repo's standing rule is that an invariant held by discipline is not held,
//! so the daemon gets its own.
//!
//! ## What is pinned, and what is deliberately NOT
//!
//! The point is narrow: **the daemon must not become a second desktop.** The SQLite
//! driver is therefore LEGAL here — ADR 0091 makes this member the thing that owns
//! the disk, and `fulcrum/src/db/` is the proof — and the pins forbid only what
//! would make this member a Tauri app or a desktop client: the UI framework and its
//! plugin crates, the desktop package, and paths into the desktop's settings and
//! persistence modules.
//!
//! The reverse direction is pinned too: the driver must still be ABSENT from the
//! engine's tree, and that message names the DAEMON as the offender, because a
//! daemon-side persistence change is where such code would be written. The engine's
//! own pin catches it as well, but a pin that blames the wrong repo is a pin whose
//! reader fixes the wrong file.
//!
//! ## Why these scan files instead of calling functions
//!
//! Same reason the engine's pins do: every property here is an ABSENCE, and an
//! absence has no call site. A grep living in a shell script nobody runs is a weak
//! guard; inside a test target it runs on every `cargo test`, on all three CI OSes,
//! because the workspace root's `default-members` lists this member and CI runs the
//! bare form.
//!
//! ## Why comments are stripped, and why that is not cheating
//!
//! The raw grep over `fulcrum/src` is NOT zero and cannot be: `db/mod.rs` and
//! `db/persistence.rs` explain in prose that the driver is legal here and forbidden
//! in the engine, `db/schema.rs` names the desktop package's directory and its
//! database file, `worker_factory.rs` and `main.rs` discuss the desktop's sidecar
//! and opener. Prose ABOUT a forbidden thing is not the forbidden thing. So the
//! scans match CODE, through the same two strippers the engine uses (line and block
//! comments, then string literals) — and [`prose_is_stripped_but_code_is_not`] keeps
//! the strippers honest, because a stripper that over-eats blanks the very code
//! these pins hunt and reports green.
//!
//! ## Why every needle is assembled at runtime
//!
//! A guard whose own diagnostic contains the needle it reports fires on its own file
//! the moment that file is inside the walk. The engine solved this by building each
//! needle from one escaped character plus the rest ([`needle`], copied from
//! `src/boundary_pins.rs:128-130`), so the SOURCE TEXT differs from the RUNTIME
//! STRING while the runtime string is the exact, unmodified identifier.
//!
//! The escape is load-bearing and easy to get wrong: the engine's history records a
//! draft that joined needle parts with a separator, every needle then matched
//! nothing, and all three scans passed a planted violation. So
//! [`the_needles_are_the_identifiers_they_stand_for`] asserts each assembled spelling
//! equals the identifier rebuilt from raw characters AND that the scan FIRES on
//! planted code, and [`the_scanner_source_contains_no_needle`] reads this file back
//! and proves no needle appears in it anywhere — comments and messages included,
//! which is stricter than the scans themselves require.
//!
//! This file lives in `fulcrum/tests/`, which neither walk reads (the daemon walk
//! starts at `fulcrum/src`, the engine walk at the repo root's `src`), so the
//! obfuscation is belt-and-braces here rather than structurally forced. Kept anyway:
//! the cost is zero, and a later task that moves a scanner into `src/` inherits the
//! protection instead of rediscovering the trap.
//!
//! ## The vacuity floors, and why the walk root is INJECTABLE
//!
//! Every test here asserts an absence, so every one of them passes on an empty file
//! set — the failure mode this roadmap has been bitten by repeatedly (the
//! `CARGO_MANIFEST_DIR.parent()` fixture paths after the move, the census guard that
//! quietly stopped covering moved code). So the floor checks are separate functions
//! that take their root and their file list as PARAMETERS, and
//! [`the_vacuity_floors_fail_on_a_walk_that_reads_nothing`] drives them on synthetic
//! inputs: an empty directory for the count floor, and a flat file list for the
//! descent floor. A floor nobody can demonstrate is a floor that gets deleted the
//! first time it is inconvenient.
//!
//! ## Proof level
//!
//! Teeth were proven by MUTATION, not by reading. Five were run, each reverted. The
//! needles cannot be spelled out here — this file is the scanner, and the self-check
//! [`the_scanner_source_contains_no_needle`] audits its own source — so each row
//! names the shape, and the needles are the ones [`forbidden_names`] lists.
//!
//! | planted in | shape | reddened |
//! |---|---|---|
//! | `fulcrum/src/serve.rs` | a local `mod` declaring the UI framework's name, plus `#[allow(unused)] use` | `the_daemon_names_neither_the_ui_framework_nor_its_plugins`, naming the file and the line |
//! | `fulcrum/src/serve.rs` | the same stand-in for the desktop package | `the_daemon_does_not_import_the_desktop_package`, naming the file |
//! | `fulcrum/src/serve.rs` | the two named desktop path prefixes inside a `macro_rules!` body | `no_path_in_the_daemon_reaches_a_foreign_module`, naming the file and line |
//! | `fulcrum/src/serve.rs` | a path into a module root NOBODY pre-named (`…::agent::manager::spawn()`) | the general rule in the same pin, reporting the segment `agent` |
//! | `src/persist.rs` (the engine) | a local `mod` declaring the SQLite driver's name, plus `#[allow(unused)] use` | `the_engine_tree_still_carries_no_sqlite_driver`, naming the file — and the engine's own `boundary_pins::no_code_names_the_sqlite_driver_or_the_desktop_package` reddened in the same run, which is what that message claims |
//!
//! The stand-ins are a `mod` declaration plus an `#[allow(unused)] use`, not a real
//! `use` of the crate, because a real one does not compile — neither crate is a
//! dependency — and a test target that fails to BUILD is not a red test, it is no
//! test at all: the run reports nothing, which reads exactly like "the guard did not
//! fire". Compiling code that carries the token is the stronger of the two pieces of
//! evidence. The path-form probes go inside a `macro_rules!` body for the same
//! reason: a macro body is real code the compiler does not resolve until it is
//! invoked, so a path into a module that does not exist can be present without
//! failing to build.

use std::path::{Path, PathBuf};

// ── the machinery (this member cannot reach fulcrum-core's) ──────────────────
//
// `needle`, `crate_code` and `strip_string_literals` are all `pub(crate)` inside
// `fulcrum-core`, hence invisible to this crate; making them `pub` would mean editing
// the engine, which this task is frozen out of. So: a local copy of the shape.

/// Remove `//`, `///`, `//!` line comments and `/* */` block comments, leaving
/// code. Line-based, deliberately not a Rust lexer — its job is to stop matching
/// text a human wrote ABOUT a forbidden name while still matching the name in a
/// statement, and [`prose_is_stripped_but_code_is_not`] holds it to that.
fn strip_comments(source: &str) -> String {
    let mut out = String::with_capacity(source.len());
    let mut in_block = false;
    for line in source.lines() {
        let trimmed = line.trim_start();
        if in_block {
            if let Some(rest) = trimmed.find("*/") {
                in_block = false;
                out.push_str(&trimmed[rest + 2..]);
                out.push('\n');
            }
            continue;
        }
        if trimmed.starts_with("/*") {
            if let Some(rest) = trimmed.find("*/") {
                out.push_str(&trimmed[rest + 2..]);
                out.push('\n');
            } else {
                in_block = true;
            }
            continue;
        }
        if trimmed.starts_with("//") {
            continue;
        }
        out.push_str(line);
        out.push('\n');
    }
    out
}

/// Drop the contents of string literals, keeping the quotes. The engine makes this
/// choice because its source-root census names the desktop package's DIRECTORY; this
/// member has the same shape in `db/schema.rs`, whose docs cite the desktop's source
/// path. And a forbidden name inside a string literal cannot become a dependency — a
/// path in a string does not compile into a reference — so the blind spot costs lint
/// precision, not the invariant.
fn strip_string_literals(source: &str) -> String {
    let mut out = String::with_capacity(source.len());
    let mut in_string = false;
    let mut escaped = false;
    for ch in source.chars() {
        if in_string {
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == '"' {
                in_string = false;
                out.push('"');
            }
            continue;
        }
        if ch == '"' {
            in_string = true;
            out.push('"');
        } else {
            out.push(ch);
        }
    }
    out
}

/// A forbidden identifier, spelled with its FIRST character as an escape. The
/// assembled string is byte-identical to the plain literal — no separator, no
/// placeholder — while this file's source never contains it. See the module docs.
fn needle(escaped_first: char, rest: &str) -> String {
    format!("{escaped_first}{rest}")
}

/// The desktop UI framework. Forbidden in BOTH members: the engine by
/// `boundary_pins`, the daemon by this file — a headless supervisor that links a
/// webview is a second desktop, which is the one thing T9 exists to avoid.
fn ui_framework_name() -> String {
    needle('\u{74}', "auri")
}

/// The UI framework's plugin namespace — the `_plugin`-suffixed crates that reach a
/// webview and a window.
fn ui_plugin_prefix() -> String {
    format!("{}_plugin", ui_framework_name())
}

/// The desktop package — the thing the engine was cut away from in T5. The daemon
/// talks to the engine, never to it.
fn desktop_package_name() -> String {
    needle('\u{61}', "rchimedes_lib")
}

/// The desktop settings module as a path prefix: `config` is the settings loader's
/// home and the settings DTOs' former home (the DTOs moved to the engine as
/// `config_types`, the file did not).
fn desktop_config_path() -> String {
    format!("{}::{}::", crate_path_word(), needle('\u{63}', "onfig"))
}

/// The desktop persistence module as a path prefix: `Db` and the error type that
/// wraps the SQLite driver's error — the half Step B-bis could not move.
fn desktop_storage_path() -> String {
    format!("{}::{}::", crate_path_word(), needle('\u{73}', "torage"))
}

/// The SQLite driver. Legal in this member, forbidden in the engine.
fn sqlite_driver_name() -> String {
    needle('\u{72}', "usqlite")
}

/// The word `crate`, assembled. This is the scanner, and exempting the scanner
/// would be the loophole the engine's module docs refuse to take — but note this
/// word alone is NOT one of the scanned needles (the scan looks for the two-segment
/// paths above), because the bare word is ordinary English and appears in this file's
/// prose legitimately. Pinning the word would make the self-check below a lie.
fn crate_path_word() -> String {
    needle('\u{63}', "rate")
}

/// Everything a source scan forbids, each with the ruling its absence protects.
/// A list, so the meta-test can assert every entry is the real identifier and that
/// the scan fires on it.
fn forbidden_names() -> Vec<(String, &'static str)> {
    vec![
        (
            ui_framework_name(),
            "the daemon is a headless supervisor: linking the desktop's UI layer \
             makes it a second desktop, which is the one thing T9 exists to avoid \
             (the daemon owns the disk under ADR 0091, not a webview)",
        ),
        (
            ui_plugin_prefix(),
            "a UI-framework plugin crate reaches a webview and a window; the daemon \
             has neither, and `main.rs` passes the engine's ungated no-op opener \
             precisely so this member needs no opener plugin",
        ),
        (
            desktop_package_name(),
            "the desktop package is the thing the engine was cut away from in T5; a \
             daemon importing it re-creates the package cycle from the other side \
             and turns the daemon into a desktop client instead of an engine host",
        ),
        (
            desktop_config_path(),
            "the desktop's settings module — the settings-loader disk boundary (T3), \
             and `crate::` inside THIS member can only mean a module \
             `fulcrum/src/lib.rs` declares, so this prefix is a path into another \
             package",
        ),
        (
            desktop_storage_path(),
            "the desktop's persistence module (`Db`, and the error type wrapping the \
             SQLite driver's error): the daemon owns `fulcrum.db` through its OWN \
             `db` module, and reaching into the desktop's would put two writers on \
             one file",
        ),
    ]
}

/// The ruling attached to `name` in [`forbidden_names`]. A lookup rather than an
/// index, so a reordered list cannot silently swap two messages.
fn reason(name: &str) -> &'static str {
    forbidden_names()
        .into_iter()
        .find(|(n, _)| n == name)
        .map(|(_, why)| why)
        .unwrap_or_else(|| panic!("`{name}` has no recorded reason — add it to the list"))
}

/// The daemon member's own `src/` — the walk root the real tests use.
fn daemon_src_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("src")
}

/// The engine's `src/`, as the manifest dir's parent: this member's manifest is
/// `fulcrum/Cargo.toml`, and the engine is the workspace root package (Layout A —
/// the root `[package]` with `src/` beside it, which the census pins forbid moving).
fn engine_src_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("the fulcrum member lives inside the workspace root")
        .join("src")
}

/// How many `.rs` files the daemon walk must find before its green means anything.
///
/// WHAT THIS FLOOR ACTUALLY PROTECTS — measured, not guessed. `fulcrum/src` holds
/// **11** `.rs` files: **4** directly in `fulcrum/src` (`lib.rs`, `main.rs`,
/// `serve.rs`, `worker_factory.rs`) and **7** inside its two subdirectories (`db/`:
/// 3, `transport/`: 4). So:
///
/// * a wrong root, or a root that stopped existing, finds **0**;
/// * a walk whose recursion is ever simplified into a flat `read_dir` finds **4**;
/// * the real recursive walk finds **11**.
///
/// 6 sits between the shallow count and the real one, which makes it a discriminator
/// rather than a headroom check — the same reasoning the engine records above its own
/// `> 40` (measured today: 150 files recursive, 38 for a flat read of its `src/`). Do
/// NOT raise this one toward 11 to "keep it honest": that makes it fire on an ordinary
/// module deletion, and a floor that cries wolf gets deleted, which is how a guard
/// actually dies here. Do NOT drop it to 1 either: the shallow walk then passes it.
///
/// [`the_vacuity_floors_fail_on_a_walk_that_reads_nothing`] re-measures the
/// discriminator property on both trees on every run, so these numbers cannot rot
/// into a fiction: if a future `fulcrum/src` ever holds 6+ files at its top level,
/// that test reddens and says to MOVE the floor rather than delete it.
const DAEMON_FILE_FLOOR: usize = 6;

/// The engine half of the cross-check. The engine's own `crate_code` floors its walk
/// at `> 40` against 150 files, and this pin reuses that number rather than inventing
/// a second one: if the engine tree ever shrinks past 40 both pins fire together,
/// which is the correct signal (a tree that small means a walk is wrong, not that the
/// code left).
const ENGINE_FILE_FLOOR: usize = 40;

/// Floor half (a): the walk found enough files for its green to mean anything.
///
/// Separate from [`scan`] and parameterised on the count so the meta-test can drive
/// it with 0, which is the case it exists for.
fn assert_file_floor(root: &Path, found: usize, floor: usize) {
    assert!(
        found > floor,
        "the boundary scan found only {found} .rs files under {root:?} (floor \
         {floor}) — a wrong root, a moved tree, or a walk that stopped descending \
         makes every pin in this file pass vacuously",
    );
}

/// Does `dir` hold a `.rs` file one level down? Independent of [`scan`]'s own
/// recursion, which is what makes floor half (b) checkable rather than circular.
fn has_nested_rs(dir: &Path) -> bool {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return false;
    };
    entries.filter_map(|e| e.ok()).any(|e| {
        let path = e.path();
        path.is_dir()
            && std::fs::read_dir(&path)
                .map(|sub| {
                    sub.filter_map(|e| e.ok()).any(|e| {
                        e.path().is_file()
                            && e.path().extension().and_then(|x| x.to_str()) == Some("rs")
                    })
                })
                .unwrap_or(false)
    })
}

/// Floor half (b): if the tree HAS files below its top level, the walk must have
/// read some.
///
/// The count floor alone cannot catch this: a walk that stops descending reports a
/// number, and on a big tree that number clears the floor while every module inside a
/// subdirectory goes unpoliced. For the engine that is the whole interesting surface
/// (`harness/`, `worker/`, `mcp/`, `tools/`, `web/`, `codemode/`), and for the daemon
/// it is `db/` — the one module most likely to gain a desktop import, because it is
/// the one that touches the disk.
///
/// Takes the file list as a parameter so the meta-test can hand it the output of a
/// deliberately broken (flat) walk and watch this fire.
fn assert_walk_descended(root: &Path, files: &[PathBuf]) {
    let top_level = files.iter().filter(|p| p.parent() == Some(root)).count();
    assert!(
        !(has_nested_rs(root) && top_level == files.len() && !files.is_empty()),
        "the boundary scan under {root:?} read {} files and ALL {top_level} of them \
         sit directly in the root, while that tree has .rs files in its \
         subdirectories — the walk is not descending, so every module inside one is \
         unpoliced while these pins report green",
        files.len()
    );
}

/// Every `.rs` file under `root`, as (path, code-with-comments-and-literals-gone).
///
/// The root is a PARAMETER on purpose — see the module docs. The real tests pass the
/// real trees; the floor meta-test drives [`assert_file_floor`] and
/// [`assert_walk_descended`] directly, because no real tree is empty.
fn scan(root: &Path, floor: usize) -> Vec<(PathBuf, String)> {
    assert!(
        root.is_dir(),
        "boundary walk root {root:?} is not a directory — a scan that reads nothing \
         makes every pin in this file vacuous"
    );
    let mut found: Vec<(PathBuf, String)> = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        for entry in std::fs::read_dir(&dir).unwrap_or_else(|e| panic!("read_dir {dir:?}: {e}")) {
            let path = entry.expect("a dir entry").path();
            if path.is_dir() {
                stack.push(path);
            } else if path.extension().and_then(|e| e.to_str()) == Some("rs") {
                let text = std::fs::read_to_string(&path)
                    .unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
                let code = strip_string_literals(&strip_comments(&text));
                found.push((path, code));
            }
        }
    }
    let paths: Vec<PathBuf> = found.iter().map(|(p, _)| p.clone()).collect();
    assert_file_floor(root, found.len(), floor);
    assert_walk_descended(root, &paths);
    found
}

/// The `.rs` files directly in `dir`, without descending — the count a broken walk
/// would report, and the number the meta-test compares the floors against.
fn top_level_rs(dir: &Path) -> usize {
    std::fs::read_dir(dir)
        .unwrap_or_else(|e| panic!("read_dir {dir:?}: {e}"))
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.path().is_file() && e.path().extension().and_then(|x| x.to_str()) == Some("rs")
        })
        .count()
}

/// `(file, line)` for every surviving line containing `forbidden`. The file is
/// reported in full: a message that does not name the offending path fails the one
/// acceptance criterion this file exists to satisfy.
fn offenders(code: &[(PathBuf, String)], forbidden: &str) -> Vec<(PathBuf, String)> {
    code.iter()
        .filter(|(_, body)| body.contains(forbidden))
        .map(|(path, body)| {
            let line = body
                .lines()
                .find(|l| l.contains(forbidden))
                .unwrap_or("(the match spans lines)")
                .trim()
                .to_string();
            (path.clone(), line)
        })
        .collect()
}

/// The UI-framework matcher: the name as a TOKEN (a path segment, a type, an
/// attribute) or as the first component of an underscore-joined crate name — not as
/// a substring of an unrelated identifier. Same rule as the engine's
/// `no_code_names_the_desktop_ui_framework`, case-insensitivity included: the daemon
/// has no legitimate identifier to protect today (measured: zero hits in
/// `fulcrum/src` once comments are stripped), and if one ever appears the fix is to
/// name it here, not to loosen the matcher.
fn names_ui_framework(body: &str) -> bool {
    let name = ui_framework_name();
    body.split(|c: char| !c.is_alphanumeric() && c != '_')
        .any(|tok| tok.eq_ignore_ascii_case(&name) || tok.starts_with(&format!("{name}_")))
}

/// The same `(file, line)` report, for the token matcher.
fn token_offenders(code: &[(PathBuf, String)]) -> Vec<(PathBuf, String)> {
    code.iter()
        .filter(|(_, body)| names_ui_framework(body))
        .map(|(path, body)| {
            let shown = body
                .lines()
                .find(|l| names_ui_framework(l))
                .unwrap_or("(the token spans lines)")
                .trim()
                .to_string();
            (path.clone(), shown)
        })
        .collect()
}

/// The `mod` declarations of the file at `lib_path` — the complete set `crate::X` may
/// legally name, read from the file rather than a hand-maintained array so the pin
/// cannot go stale by forgetting a module: one arriving is automatically allowed, and
/// a path into another package is automatically not.
fn declared_modules(lib_path: &Path) -> Vec<String> {
    let text = std::fs::read_to_string(lib_path)
        .unwrap_or_else(|e| panic!("read {}: {e}", lib_path.display()));
    let mut mods = Vec::new();
    for line in text.lines() {
        // A trailing comment is legal on a `mod` line — `fulcrum/src/lib.rs` uses one
        // per module to record which T9 task fills it — so cut the line at `//` before
        // reading the name. The engine's own parser does not need this (its `lib.rs`
        // spells the declarations bare); this member's does, and the first run of this
        // file proved it: without the cut, every name came back `db; // Task 4` and the
        // general rule reported all nine legitimate `crate::` paths as intruders.
        let t = line.split("//").next().unwrap_or("").trim();
        for prefix in ["pub mod ", "pub(crate) mod ", "mod "] {
            if let Some(rest) = t.strip_prefix(prefix) {
                let name = rest.trim_end_matches(';').trim().to_string();
                if !name.is_empty() {
                    mods.push(name);
                }
            }
        }
    }
    // The parser's own liveness check, same shape as the engine's. `fulcrum/src/lib.rs`
    // declares 4 modules, so `> 2` is not headroom: it is the guard against this
    // function's three accepted spellings drifting out of sync with the file. A fourth
    // spelling would drop the parse toward zero and make EVERY legal crate path look
    // like an intruder — loud, yes, but loud and WRONG, and a reader who "fixes" the
    // list by adding the desktop root disarms the pin permanently. It cannot catch a
    // single missed name; that half is loud by itself.
    assert!(
        mods.len() > 2,
        "parsed only {} `mod` declarations out of {} — the parser or the file is \
         wrong, and the allow-list below is not trustworthy",
        mods.len(),
        lib_path.display()
    );
    // …and the STRIP half of the parser works, which the count cannot check: a name
    // carrying a `;`, a space or a `/` means the line was cut in the wrong place, and
    // the allow-list then matches nothing while still looking populated. That is the
    // silent version of the failure the count floor only catches loudly, and this file
    // hit it on its first run.
    let malformed: Vec<&String> = mods
        .iter()
        .filter(|m| !m.chars().all(|c| c.is_alphanumeric() || c == '_'))
        .collect();
    assert!(
        malformed.is_empty(),
        "`{}` parsed into malformed module names {malformed:?} — the name extractor \
         is wrong, so the allow-list cannot match a real path and the general rule \
         reports every crate path as an intruder",
        lib_path.display()
    );
    mods
}

/// Every `crate::<segment>` in the given code, with its file and line.
///
/// Textual rather than an AST walk, for the engine's stated reason: a parser
/// dependency just to service a test is its own decision, and the textual rule is
/// exact for the shape that matters — `use crate::x::…` and `crate::x::y()` spell the
/// prefix identically.
fn crate_path_segments(code: &[(PathBuf, String)]) -> Vec<(PathBuf, String, String)> {
    let prefix = format!("{}::", crate_path_word());
    let mut found = Vec::new();
    for (path, body) in code {
        let mut cursor = 0;
        while let Some(rel) = body[cursor..].find(&prefix) {
            let start = cursor + rel + prefix.len();
            let seg: String = body[start..]
                .chars()
                .take_while(|c| c.is_alphanumeric() || *c == '_')
                .collect();
            if !seg.is_empty() {
                let line = body[..start]
                    .lines()
                    .last()
                    .unwrap_or("(unprintable)")
                    .trim()
                    .to_string();
                found.push((path.clone(), seg, line));
            }
            cursor = start;
        }
    }
    found
}

/// A dependency key the given manifest declares under that name (or a `_`-suffixed
/// variant of it), ignoring comment lines — both manifests explain at length what they
/// do NOT depend on, and that prose is not a dependency.
fn manifest_declares(manifest: &Path, crate_name: &str) -> Option<String> {
    let text = std::fs::read_to_string(manifest)
        .unwrap_or_else(|e| panic!("read {}: {e}", manifest.display()));
    text.lines()
        .map(str::trim)
        .filter(|l| !l.starts_with('#'))
        .find(|l| {
            l.split_once('=')
                .map(|(name, _)| {
                    let name = name.trim();
                    name == crate_name || name.starts_with(&format!("{crate_name}_"))
                })
                .unwrap_or(false)
        })
        .map(str::to_string)
}

/// Run `f` and return the panic message, or `None` if it did not panic. The floors
/// are asserts, so proving they bite means catching them fire.
fn panic_message<R>(f: impl FnOnce() -> R + std::panic::UnwindSafe) -> Option<String> {
    let outcome = std::panic::catch_unwind(f);
    match outcome {
        Ok(_) => None,
        Err(e) => Some(
            e.downcast_ref::<String>()
                .cloned()
                .unwrap_or_else(|| "(the panic carried no message)".to_string()),
        ),
    }
}

// ── the floors, demonstrated ─────────────────────────────────────────────────

/// The most important lines in this file, proven by running them.
///
/// Every other test here asserts an ABSENCE, so all of them pass on an empty file
/// set. Without a floor that demonstrably bites, a renamed directory — or a
/// `CARGO_MANIFEST_DIR` that stops pointing where this file thinks it does — turns
/// the whole file into a green light over nothing, which is the exact failure this
/// roadmap keeps recording. The floors are separate functions taking a root and a
/// file list precisely so they can be pointed at inputs the real trees cannot supply:
/// an empty directory, and the output of a walk that stopped descending.
#[test]
fn the_vacuity_floors_fail_on_a_walk_that_reads_nothing() {
    let empty = tempfile::tempdir().expect("a temp dir");

    // (1) The count floor, on an empty tree. This is the case that would otherwise
    // be unreachable: no real tree is empty, so an undemonstrable floor is one that
    // has never fired and cannot be trusted.
    let message = panic_message(|| assert_file_floor(empty.path(), 0, DAEMON_FILE_FLOOR)).expect(
        "the vacuity floor PASSED on an empty directory — it is decorative, \
                 and every absence-pin in this file is then unverifiable",
    );
    assert!(
        message.contains("floor") && message.contains("vacuous"),
        "the empty-walk failure did not come from the floor: {message}"
    );
    // …and the whole scan, end to end, on that same empty directory: the walk must
    // not return an empty vec and call that green.
    let message = panic_message(|| scan(empty.path(), DAEMON_FILE_FLOOR)).expect(
        "scan() returned successfully on an empty directory — every pin in this \
         file would then pass on nothing",
    );
    assert!(
        message.contains("vacuous") || message.contains("not a directory"),
        "scan()'s empty-tree failure did not come from a floor: {message}"
    );

    // (2) The descent floor, on the output of a deliberately flat walk. Build a tree
    // that HAS nested files, hand the floor the file list a non-recursive walk would
    // produce (the top-level ones only), and require it to fire.
    let tree = tempfile::tempdir().expect("a temp dir");
    std::fs::write(tree.path().join("top.rs"), "fn top() {}\n").expect("write top.rs");
    std::fs::create_dir(tree.path().join("deep")).expect("mkdir deep");
    for i in 0..20 {
        std::fs::write(
            tree.path().join("deep").join(format!("f{i}.rs")),
            "fn f() {}\n",
        )
        .expect("write a fixture file");
    }
    assert!(
        has_nested_rs(tree.path()),
        "the fixture tree must have files below its top level or this proves nothing"
    );
    assert_eq!(top_level_rs(tree.path()), 1);
    let flat_walk = vec![tree.path().join("top.rs")];
    let message = panic_message(|| assert_walk_descended(tree.path(), &flat_walk)).expect(
        "the descent floor PASSED on a walk that read only the top level, while the \
         tree has 20 files in a subdirectory — the floor is decorative",
    );
    assert!(
        message.contains("descending"),
        "the flat-walk failure did not come from the descent floor: {message}"
    );
    // …and it does NOT fire on the correct walk over the same tree, or the floor is
    // a machine that reports everything and will be silenced.
    let good = scan(tree.path(), 0);
    assert_eq!(good.len(), 21, "the real walk mis-counted the fixture tree");

    // (3) Both floors are LIVE discriminators on the real trees, not formalities: a
    // flat walk over either root would fall under its count floor, and both trees do
    // have files below the top level, so the descent floor has something to catch.
    for (root, floor) in [
        (daemon_src_root(), DAEMON_FILE_FLOOR),
        (engine_src_root(), ENGINE_FILE_FLOOR),
    ] {
        assert!(
            top_level_rs(&root) < floor,
            "the floor {floor} no longer discriminates a flat walk: {} files sit \
             directly in {root:?} — move the floor, do not delete it",
            top_level_rs(&root)
        );
        assert!(
            has_nested_rs(&root),
            "{root:?} has no .rs files in its subdirectories, so the descent floor \
             has nothing to catch and this file's second guard is vacuous"
        );
    }
}

/// The walks must find the files they claim to police — a floor says HOW MANY, this
/// says WHICH.
#[test]
fn the_walks_read_the_trees_they_claim() {
    let daemon = scan(&daemon_src_root(), DAEMON_FILE_FLOOR);
    let engine = scan(&engine_src_root(), ENGINE_FILE_FLOOR);

    let names = |files: &[(PathBuf, String)]| -> Vec<String> {
        files
            .iter()
            .map(|(p, _)| p.file_name().unwrap().to_string_lossy().into_owned())
            .collect()
    };
    for expected in ["lib.rs", "main.rs", "serve.rs", "mod.rs", "persistence.rs"] {
        assert!(
            names(&daemon).iter().any(|n| n == expected),
            "the daemon walk did not read `{expected}` — it is not policing the \
             member it names. Read: {:?}",
            names(&daemon)
        );
    }
    // `db/mod.rs` is the daemon's legitimate SQLite home and `transport/unix.rs` the
    // platform-gated half the unix CI legs compile; both must be in the walk or the
    // pins have a hole shaped exactly like the daemon.
    for tail in ["db/mod.rs", "transport/unix.rs", "worker_factory.rs"] {
        assert!(
            daemon.iter().any(|(p, _)| p.ends_with(tail)),
            "the daemon walk skipped {tail} — the pins would then miss the file most \
             likely to gain a desktop import"
        );
    }
    assert!(
        engine.iter().any(|(p, _)| p.ends_with("boundary_pins.rs"))
            && engine.iter().any(|(p, _)| p.ends_with("persist.rs")),
        "the engine walk read {} files but not the two the cross-check is about: {:?}",
        engine.len(),
        names(&engine)
    );
}

// ── the fail-open proofs ─────────────────────────────────────────────────────

/// The needles are the identifiers they stand for, and the scan FIRES on them.
///
/// Paired with the engine's recorded lesson: a needle assembled wrong (a separator, a
/// placeholder, a typo) is indistinguishable from a clean tree. So each assembled
/// spelling is compared against the identifier rebuilt from raw characters — built
/// here from char arrays, so the literal never appears in this file either — and each
/// is planted in code the scan must report.
#[test]
fn the_needles_are_the_identifiers_they_stand_for() {
    let ui: String = ['t', 'a', 'u', 'r', 'i'].iter().collect();
    let plugin = format!("{ui}_plugin");
    let desktop: String = [
        'a', 'r', 'c', 'h', 'i', 'm', 'e', 'd', 'e', 's', '_', 'l', 'i', 'b',
    ]
    .iter()
    .collect();
    let rate: String = ['c', 'r', 'a', 't', 'e'].iter().collect();
    let config = format!("{rate}::config::");
    let storage = format!("{rate}::storage::");
    let driver: String = ['r', 'u', 's', 'q', 'l', 'i', 't', 'e'].iter().collect();

    let expected = [ui.clone(), plugin, desktop, config, storage];
    let assembled: Vec<String> = forbidden_names()
        .into_iter()
        .map(|(name, _)| name)
        .collect();
    assert_eq!(
        assembled, expected,
        "an assembled needle is not the real identifier — the scan is matching nothing"
    );
    assert_eq!(
        sqlite_driver_name(),
        driver,
        "the driver needle is not the real crate name, so the engine cross-check is \
         blind (it would pass any tree)"
    );
    assert_eq!(
        crate_path_word(),
        rate,
        "the crate-path word is not the real keyword, so the general rule is blind"
    );

    // And the scan FIRES, in both spellings that matter: the `use` form and the
    // qualified-path form.
    for name in &expected {
        for planted in [
            format!("use {name}Thing;"),
            format!("fn f() {{ let _ = {name}Thing::new(); }}"),
        ] {
            let sample = vec![(PathBuf::from("planted.rs"), planted.clone())];
            assert_eq!(
                offenders(&sample, name).len(),
                1,
                "the scan did not fire on planted `{name}`: {planted}"
            );
        }
    }
    // The driver, in the shapes a migration would actually take.
    for planted in [
        format!("use {driver}::{{params, Connection}};"),
        format!("fn f(c: &{driver}::Connection) {{}}"),
    ] {
        let sample = vec![(PathBuf::from("planted.rs"), planted.clone())];
        assert_eq!(
            offenders(&sample, &driver).len(),
            1,
            "the driver scan did not fire on planted code: {planted}"
        );
    }

    // The UI-framework TOKEN matcher fires too, plugin spelling included — the pin
    // that reports those uses the matcher, so the matcher is what gets proven.
    for planted in [
        format!("{ui}::run();"),
        format!("mod {ui} {{ pub struct Url; }}"),
        format!("#[{ui}_plugin_dialog::message]"),
    ] {
        let sample = vec![(PathBuf::from("planted.rs"), planted.clone())];
        assert_eq!(
            token_offenders(&sample).len(),
            1,
            "the token matcher did not fire on planted code: {planted}"
        );
    }
    // …and it does not report everything, or it gets silenced. The matcher's rule is
    // "a token EQUAL to the name, or starting with `name_`" — the engine's rule,
    // deliberately: it fires on a path segment, a type, an attribute, and on a plugin
    // crate, and it does NOT fire on an identifier that merely contains the name.
    // Both sides of that line get asserted, so a later edit that widens the matcher
    // into a substring grep (which would report a same-prefixed unrelated crate) reddens here instead of surprising someone.
    for miss in [format!("open_{ui}_window"), format!("{ui}x")] {
        let sample = vec![(PathBuf::from("clean.rs"), format!("fn {miss}() {{}}"))];
        assert!(
            token_offenders(&sample).is_empty(),
            "the matcher reports an identifier that only CONTAINS the name: {sample:?}"
        );
    }

    // The segment extractor fires on an intruding root and reports its NAME…
    let planted = vec![(
        PathBuf::from("planted.rs"),
        format!("use {rate}::agent::Manager;"),
    )];
    let segs = crate_path_segments(&planted);
    assert_eq!(
        segs.len(),
        1,
        "the segment extractor found nothing in a planted foreign path"
    );
    assert_eq!(
        segs[0].1, "agent",
        "wrong segment extracted, so the message would blame the wrong module: {segs:?}"
    );

    // …and does not fire on a legal path.
    let clean = vec![(
        PathBuf::from("clean.rs"),
        format!("use {rate}::transport::frame::Connection;"),
    )];
    let segs = crate_path_segments(&clean);
    assert_eq!(
        segs.len(),
        1,
        "the extractor mis-read a legal path: {clean:?}"
    );
    assert_eq!(segs[0].1, "transport");
    let allowed = declared_modules(&daemon_src_root().join("lib.rs"));
    assert!(
        allowed.iter().any(|m| m == "transport"),
        "`transport` must be declared in the daemon's lib.rs or the allow-list is \
         wrong: {allowed:?}"
    );
    assert!(
        !allowed
            .iter()
            .any(|m| m == "agent" || m == "config" || m == "storage"),
        "the allow-list contains a foreign root, so the general rule is disarmed: \
         {allowed:?}"
    );
}

/// The stripper half of the fail-open proof: prose about a forbidden thing must
/// vanish, code carrying it must not.
#[test]
fn prose_is_stripped_but_code_is_not() {
    let names: Vec<String> = forbidden_names()
        .into_iter()
        .map(|(name, _)| name)
        .collect();
    let driver = sqlite_driver_name();
    let mut sample = String::new();
    sample.push_str(&format!(
        "//! This module explains why {driver} is legal here and why {} stayed \
         desktop.\n",
        names[0]
    ));
    sample.push_str(&format!(
        "// A line comment naming {} and {} too.\n",
        names[2], names[3]
    ));
    sample.push_str(&format!("/* a block comment naming {} */\n", names[4]));
    for (i, name) in names.iter().enumerate() {
        sample.push_str(&format!("pub fn f{i}() {{ let _ = {name}Thing; }}\n"));
    }
    sample.push_str(&format!("pub fn g() {{ let _ = {driver}::open(); }}\n"));

    let stripped = strip_comments(&sample);
    assert!(
        !stripped.contains("stayed desktop"),
        "doc prose should be stripped, got:\n{stripped}"
    );
    assert!(
        !stripped.contains("block comment"),
        "a block comment survived stripping, got:\n{stripped}"
    );
    for name in names.iter().chain(std::iter::once(&driver)) {
        assert!(
            stripped.contains(name.as_str()),
            "the comment-stripper swallowed `{name}` out of a real statement — the \
             pins would then pass vacuously. Stripped:\n{stripped}"
        );
    }

    // The literal-stripper: it must blank a desktop DIRECTORY literal without blanking
    // an adjacent real path segment, or the UI-framework pin is blind.
    let dir = format!("src-{}", names[0]);
    let with_path = format!(
        "let root = dir.join(\"{dir}\").join(\"src\"); {}::run();",
        names[0]
    );
    let literals_gone = strip_string_literals(&with_path);
    assert!(
        !literals_gone.contains(&dir),
        "the directory literal should be blanked, got {literals_gone}"
    );
    assert!(
        literals_gone.contains(&format!("{}::run", names[0])),
        "a real path segment must SURVIVE literal-stripping or the pin is blind: \
         {literals_gone}"
    );
}

/// The acceptance criterion "the guard does not fire on its own diagnostic text",
/// checked by reading this file back.
///
/// Stricter than the scans need (this file sits in `fulcrum/tests/`, and both walks
/// start at a `src/`), because the property worth having is that NO needle appears
/// anywhere in this source — comments and messages included. If a later edit widens a
/// walk to cover `tests/`, or moves a scanner into `src/`, there is nothing here for
/// it to catch.
///
/// The bare word `crate` is deliberately NOT in this list: the scan hunts the
/// two-segment paths, and the bare word is ordinary English in this file's prose.
/// Listing it would force the self-check to pass by rewording honest sentences.
#[test]
fn the_scanner_source_contains_no_needle() {
    let here = {
        let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
        let relative = Path::new(file!());
        // `file!()` is relative to the WORKSPACE root for an integration target, but
        // do not bet the check on that: try both and take whichever exists.
        let candidates = [
            manifest_dir.join(relative),
            manifest_dir.join("tests").join("boundary.rs"),
        ];
        candidates
            .into_iter()
            .find(|p| p.is_file())
            .unwrap_or_else(|| panic!("cannot find this test's own source to audit it"))
    };
    let text =
        std::fs::read_to_string(&here).unwrap_or_else(|e| panic!("read {}: {e}", here.display()));
    // And it is THIS file, not merely some file that exists. `file!()` is relative to
    // the WORKSPACE root for an integration target while `CARGO_MANIFEST_DIR` is the
    // member's directory, so the first candidate above does not exist and the lookup
    // falls through — which is fine, but a candidate list that happened to hit a
    // DIFFERENT file would make this whole pin pass on nothing. That is the exact
    // failure shape the rest of this file exists to catch, so check the content is
    // this file's before trusting the absence it reports.
    assert!(
        text.contains("fn needle(escaped_first: char, rest: &str) -> String")
            && text.contains("fn the_scanner_source_contains_no_needle()"),
        "{} is not this test's own source, so the self-audit read the wrong file and \
         proves nothing about this one",
        here.display()
    );
    let mut names: Vec<String> = forbidden_names()
        .into_iter()
        .map(|(name, _)| name)
        .collect();
    names.push(sqlite_driver_name());
    for name in names {
        assert!(
            !text.contains(&name),
            "this file's own source contains `{name}`, so a scanner that read it \
             would report its own diagnostic — assemble the needle with `needle()` \
             instead of writing it out: {}",
            here.display()
        );
    }
}

// ── the pins ────────────────────────────────────────────────────────────────

/// The daemon names neither the desktop UI framework nor one of its plugin crates, in
/// CODE (`use`, a type, an attribute, a path segment).
///
/// This is the pin that reddened for the mutation: a compiling `mod` + `use` stand-in
/// in `fulcrum/src/serve.rs` was reported by path and line. Comments are stripped
/// first, which is what lets `main.rs` and `worker_factory.rs` discuss the desktop's
/// opener and sidecar in prose.
#[test]
fn the_daemon_names_neither_the_ui_framework_nor_its_plugins() {
    let code = scan(&daemon_src_root(), DAEMON_FILE_FLOOR);
    let hits = token_offenders(&code);
    assert!(
        hits.is_empty(),
        "the desktop UI framework is named in daemon CODE at {hits:?} — {}. And \
         likewise a plugin crate under its namespace: {}.\n\nThe daemon is a headless \
         supervisor: the moment it links the desktop's UI layer it is a second \
         desktop, which is the one thing T9 exists to avoid. `main.rs` passes the \
         engine's ungated no-op opener for exactly this reason, so a legitimate \
         feature does not need a plugin crate here. If this member genuinely needs \
         what the desktop's UI layer provides, the change belongs in the engine \
         behind a seam — not in a `use` here.",
        reason(&ui_framework_name()),
        reason(&ui_plugin_prefix()),
    );
}

/// The daemon does not import the desktop package.
#[test]
fn the_daemon_does_not_import_the_desktop_package() {
    let code = scan(&daemon_src_root(), DAEMON_FILE_FLOOR);
    let forbidden = desktop_package_name();
    let hits = offenders(&code, &forbidden);
    assert!(
        hits.is_empty(),
        "`{forbidden}` appears in daemon CODE at {hits:?} — {}",
        reason(&forbidden)
    );
}

/// No path in the daemon reaches a module of another package.
///
/// Two halves, in the engine's shape: the two roots the plan names, each with its own
/// message, and then the GENERAL rule — a `crate::X` whose `X` the daemon's `lib.rs`
/// does not declare is a path into another package, whatever it is called. The named
/// roots are a list someone had to think of; the general rule catches the family,
/// which is the lesson the engine recorded as S5b (its six-identifier scan stayed
/// green while a path into the desktop's settings module sat in production code).
#[test]
fn no_path_in_the_daemon_reaches_a_foreign_module() {
    let code = scan(&daemon_src_root(), DAEMON_FILE_FLOOR);

    for forbidden in [desktop_config_path(), desktop_storage_path()] {
        let hits = offenders(&code, &forbidden);
        assert!(
            hits.is_empty(),
            "`{forbidden}` appears in daemon CODE at {hits:?} — {}",
            reason(&forbidden)
        );
    }

    let allowed = declared_modules(&daemon_src_root().join("lib.rs"));
    let strays: Vec<(PathBuf, String, String)> = crate_path_segments(&code)
        .into_iter()
        .filter(|(_, seg, _)| !allowed.iter().any(|m| m == seg))
        .collect();
    assert!(
        strays.is_empty(),
        "daemon paths name modules `{}` does not declare, at {strays:?} — `crate::` \
         can only mean THIS member, so any other root is another package's module and \
         a dependency edge these pins exist to forbid. Declared modules: {allowed:?}",
        daemon_src_root().join("lib.rs").display()
    );
}

/// The cross-check in the other direction: the SQLite driver is still absent from the
/// ENGINE's tree, and the message names the DAEMON as the offender.
///
/// The engine's own `boundary_pins` forbids this too, but it blames the engine, and
/// the change that would put a driver in the engine is a daemon-side persistence
/// change — someone moving `fulcrum::db`'s SQL behind the seam, or hoisting a
/// `DbError` variant into `StoreError`. ADR 0091's content is that a host owns the
/// disk and the engine does not, so the disk's driver must not walk back.
#[test]
fn the_engine_tree_still_carries_no_sqlite_driver() {
    let code = scan(&engine_src_root(), ENGINE_FILE_FLOOR);
    let forbidden = sqlite_driver_name();
    let hits = offenders(&code, &forbidden);
    assert!(
        hits.is_empty(),
        "`{forbidden}` appears in the ENGINE's code at {hits:?} — the daemon's \
         persistence code has migrated into `fulcrum-core`, which puts the disk back \
         under the engine and breaks ADR 0091: the engine is not a host and holds no \
         connection. The driver is legal in the DAEMON (`fulcrum/src/db` is its home \
         and `fulcrum/Cargo.toml` declares it); move this code back behind \
         `fulcrum_core::FulcrumPersistence` instead of moving the driver forward. \
         The engine's own pin \
         (`no_code_names_the_sqlite_driver_or_the_desktop_package`) reddens too — \
         fix it in the same commit, never by relaxing either pin."
    );
}

/// The manifest half, which no source scan can see: a dependency arrives with ZERO
/// source changes, and no string literal can become one.
///
/// Both directions, and the positive half is not decoration. Someone who reddens the
/// engine cross-check may be tempted to "fix" the boundary by deleting the driver from
/// THIS manifest — which would take the daemon's store down and silence the pin that
/// was right. So the driver is pinned as ALLOWED here, and the UI framework and the
/// desktop package as absent, in the very file the engine's pins never open.
#[test]
fn the_daemon_manifest_allows_the_driver_and_forbids_the_desktop() {
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml");
    for forbidden in [ui_framework_name(), desktop_package_name()] {
        let offender = manifest_declares(&manifest, &forbidden);
        assert!(
            offender.is_none(),
            "`{forbidden}` is declared as a dependency of the daemon member \
             ({offender:?}) in {} — the source pins above cannot see a manifest line, \
             and the manifest is the harder fact: it makes the forbidden thing \
             reachable before a single `use` is written.",
            manifest.display()
        );
    }
    let driver = manifest_declares(&manifest, &sqlite_driver_name());
    assert!(
        driver.is_some(),
        "the daemon member no longer declares the SQLite driver ({}) — that is NOT a \
         fix for an engine-side violation: ADR 0091 makes this member the thing that \
         owns `fulcrum.db`, and `fulcrum/src/db` does not compile without it. Move the \
         offending code back into this member instead of deleting this line.",
        sqlite_driver_name()
    );

    // And the engine's manifest still declares none — the fact the engine's own
    // manifest pin holds, read from this member so a workspace-root edit cannot
    // silently un-police the sibling relationship from the other side.
    let engine_manifest = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("the fulcrum member lives inside the workspace root")
        .join("Cargo.toml");
    let offender = manifest_declares(&engine_manifest, &sqlite_driver_name());
    assert!(
        offender.is_none(),
        "the ENGINE's manifest declares the SQLite driver ({offender:?}) — the engine \
         holds no disk, and `StoreError` exists precisely so `DbError` did not have to \
         move (Step B-bis)",
    );
}

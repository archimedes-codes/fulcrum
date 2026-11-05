//! The crate's boundary pins (plan Step C, §2c-bis's pin, and the S4 gate's
//! "the crate contains no `tauri` reference" test).
//!
//! ## Why these scan files instead of calling functions
//!
//! The properties they hold are ABSENCES — the crate does not read the desktop's
//! settings file, does not name the desktop UI framework, does not link a SQLite
//! driver or depend on the desktop package. An absence has no call site to test,
//! which is why the plan specifies a grep ("a crate-wide test that `grep -c
//! "load_settings\|settings.json" crates/fulcrum-core/src` is 0, so a future move
//! cannot reintroduce the read unnoticed") and why the S4 gate asks for the UI
//! framework one as "a test, not a hope". A grep living in a shell script nobody
//! runs is a weak guard; inside the test binary it runs on every `cargo test`.
//!
//! ## Why comments are STRIPPED, and why that is not cheating
//!
//! The raw grep is not 0 and cannot be: this crate's docs explain *why* `open.rs`
//! and `web/render.rs` stayed desktop ("the desktop's `crate::open` names tauri"),
//! and `config_types` explains why the settings loader stayed behind. Prose about a
//! forbidden thing is not the forbidden thing — the S3 gate recorded its two prose
//! hits as expected. So the pins match CODE.
//!
//! That is only honest if the stripper is real, so it removes line comments and
//! block comments and the pins count what survives. Two consequences, both
//! intended: a settings read in a function body is caught; the word in a `///` line
//! is not. The first is the point, and it is verified by
//! [`prose_is_stripped_but_code_is_not`], which feeds the stripper code it must
//! catch and requires it to catch it.
//!
//! ## The one literal exception, and why it is narrow
//!
//! `childproc.rs`'s source-root census asserts over real paths, and one of them is
//! the desktop package's DIRECTORY — `src-tauri` — which contains the UI
//! framework's name as a substring and always will. So the UI-framework scan
//! ignores string literals and nothing else. The blind spot cannot hide the thing
//! that matters: a dependency arrives through `Cargo.toml`, not through a string
//! literal, which is why
//! [`the_manifest_declares_no_forbidden_dependency`] is a separate pin.
//!
//! ## Why this file contains no forbidden identifier as a literal
//!
//! The scanner reads this file. The obvious fix — exempt this file — is a loophole:
//! an exempted file is one the invariant does not hold in. So instead every needle
//! is assembled at runtime via [`needle`] with ONE character written as a `\u{…}`
//! escape, which makes the SOURCE TEXT differ from the RUNTIME STRING while the
//! runtime string is the exact, unmodified identifier. [`the_needles_are_the_identifiers_they_stand_for`]
//! is the pin that keeps that trick honest.

use std::path::{Path, PathBuf};

/// Remove `//`, `///`, `//!` line comments and `/* */` block comments, leaving
/// code. Line-based and deliberately simple — it is not a Rust lexer and does not
/// need to be. Its job is to stop matching text a human wrote ABOUT a forbidden
/// name while still matching the name in a statement, and
/// [`prose_is_stripped_but_code_is_not`] is the pin that holds it to that.
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

/// Drop the contents of string literals, keeping the quotes. Its ONLY caller is
/// the UI-framework scan, for the one reason in the module docs: the source-root
/// census names the desktop package's directory.
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

/// A forbidden identifier, spelled with its FIRST character as an escape.
///
/// The assembled string is byte-identical to the plain literal — there is no
/// separator and no placeholder to strip — while this file's SOURCE never contains
/// it, so the scanner does not report the scanner. See the module docs for why this
/// beats exempting the file.
///
/// The escape is load-bearing and easy to get wrong: an earlier draft built needles
/// by JOINING parts with a separator character, which made every needle
/// unmatchable, and all three source scans then passed a deliberately planted
/// settings read and reported green. That is what
/// [`the_needles_are_the_identifiers_they_stand_for`] exists to prevent.
fn needle(escaped_first: char, rest: &str) -> String {
    format!("{escaped_first}{rest}")
}

/// The settings-file reader's name, the one the plan's grep is written against.
fn settings_loader_name() -> String {
    needle('\u{6c}', "oad_settings")
}

/// The settings file itself.
fn settings_file_name() -> String {
    needle('\u{73}', "ettings.json")
}

/// The settings writer's name (the other half of the disk boundary).
fn settings_writer_name() -> String {
    needle('\u{77}', "rite_settings")
}

/// The SQLite driver the crate must not link (Step B-bis: `DbError` cannot move
/// because it wraps the driver's error type, which is why `StoreError` exists).
fn sqlite_driver_name() -> String {
    needle('\u{72}', "usqlite")
}

/// The desktop package the crate must not import — the package cycle Step A
/// exists to sever.
fn desktop_package_name() -> String {
    needle('\u{61}', "rchimedes_lib")
}

/// The desktop UI framework the crate must not name (the master roadmap's own
/// decoupling criterion).
fn ui_framework_name() -> String {
    needle('\u{74}', "auri")
}

/// Every `.rs` file under this crate's `src/`, as (path, code-with-comments-gone).
fn crate_code() -> Vec<(PathBuf, String)> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    assert!(root.is_dir(), "the crate's own src/ must exist: {root:?}");
    let mut found = Vec::new();
    let mut stack = vec![root];
    while let Some(dir) = stack.pop() {
        for entry in std::fs::read_dir(&dir).unwrap_or_else(|e| panic!("read_dir {dir:?}: {e}")) {
            let path = entry.expect("a dir entry").path();
            if path.is_dir() {
                stack.push(path);
            } else if path.extension().and_then(|e| e.to_str()) == Some("rs") {
                let text = std::fs::read_to_string(&path)
                    .unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
                found.push((path, strip_comments(&text)));
            }
        }
    }
    // The scan's OWN vacuity guard: a wrong root walks nothing and every pin in
    // this module then passes on an empty set — the failure mode this plan has
    // already been bitten by twice (the `CARGO_MANIFEST_DIR.parent()` fixture paths
    // after the move, and the census guard that quietly stopped covering moved
    // code).
    //
    // WHAT THIS FLOOR ACTUALLY PROTECTS (measured at T8/S8.5, so the number is
    // not a formality — 135 `.rs` files live under `src/`, and a floor of 40
    // against 135 looks like a rubber stamp until you ask what it fires on).
    // It does NOT fire on "the crate got smaller" and it cannot catch a single
    // missing file. It fires on the ONE realistic way this walk goes wrong
    // without going empty: it stops DESCENDING. A flat `read_dir` of `src/` —
    // the shape this loop collapses to if the `stack.push(dir)` branch is ever
    // simplified away — reads 36 files, because every moved subtree
    // (`harness/`, `worker/`, `mcp/`, `tools/`, `web/`, `codemode/`) is a
    // directory. 36 < 40, so the floor sits deliberately ABOVE the shallow-walk
    // count and BELOW the recursive one: it is a discriminator, not a
    // headroom check. Raising it to hug 135 would be worse, not better — it
    // would make the guard fire on an ordinary deletion and get silenced.
    assert!(
        found.len() > 40,
        "the scan found only {} .rs files — a wrong root (or a walk that stopped \
         descending: a flat read of src/ sees 36) makes every boundary pin vacuous",
        found.len()
    );
    found
}

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

/// Step C / §2c: the crate reads NO desktop config file. The settings loader is the
/// T3 disk boundary and it stayed in the desktop; a second reader appearing here
/// would silently undo T3 — the exact failure the REVIEW ROUND's CRITICAL finding
/// was about, and the reason the plan mandates this pin.
#[test]
fn no_code_reads_the_desktop_settings_file() {
    let code = crate_code();
    for forbidden in [
        settings_loader_name(),
        settings_writer_name(),
        settings_file_name(),
    ] {
        let hits = offenders(&code, &forbidden);
        assert!(
            hits.is_empty(),
            "`{forbidden}` appears in crate CODE at {hits:?} — the crate reads no \
             desktop config file; the five fields arrive through the packaged \
             `StartEnv` and the `config-update` push (T3)"
        );
    }
}

/// The master roadmap's decoupling criterion, as the S4 gate demands: "the crate
/// contains no tauri reference — make this a test, not a hope".
///
/// Matches the name as a TOKEN (a path segment, a type, an attribute) rather than a
/// substring, and ignores string literals for the single directory-name reason in
/// the module docs.
#[test]
fn no_code_names_the_desktop_ui_framework() {
    let code = crate_code();
    let forbidden = ui_framework_name();
    let is_a_hit = |body: &str| {
        strip_string_literals(body)
            .split(|c: char| !c.is_alphanumeric() && c != '_')
            .any(|tok| {
                tok.eq_ignore_ascii_case(&forbidden) || tok.starts_with(&format!("{forbidden}_"))
            })
    };
    let hits: Vec<(PathBuf, String)> = code
        .iter()
        .filter(|(_, body)| is_a_hit(body))
        .map(|(path, body)| {
            let shown = strip_string_literals(body)
                .lines()
                .find(|l| is_a_hit(l))
                .unwrap_or("(the token spans lines)")
                .trim()
                .to_string();
            (path.clone(), shown)
        })
        .collect();
    assert!(
        hits.is_empty(),
        "the desktop UI framework is named in crate CODE at {hits:?}"
    );
}

/// The same criterion for the other two things the crate must be free of: the
/// SQLite driver and the desktop package.
#[test]
fn no_code_names_the_sqlite_driver_or_the_desktop_package() {
    let code = crate_code();
    for forbidden in [sqlite_driver_name(), desktop_package_name()] {
        let hits = offenders(&code, &forbidden);
        assert!(
            hits.is_empty(),
            "`{forbidden}` appears in crate CODE at {hits:?}"
        );
    }
}

/// The manifest half, which no source grep can see: a dependency line is a harder
/// fact than an identifier, and any of the three could arrive with ZERO source
/// changes. This is also the pin that closes the string-literal blind spot above.
#[test]
fn the_manifest_declares_no_forbidden_dependency() {
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml");
    let text = std::fs::read_to_string(&manifest).expect("the crate has a manifest");
    for forbidden in [
        sqlite_driver_name(),
        desktop_package_name(),
        ui_framework_name(),
    ] {
        let offender = text
            .lines()
            .map(str::trim)
            .filter(|l| !l.starts_with('#'))
            .find(|l| {
                l.split_once('=')
                    .map(|(name, _)| {
                        let name = name.trim();
                        name == forbidden || name.starts_with(&format!("{forbidden}_plugin"))
                    })
                    .unwrap_or(false)
            });
        assert!(
            offender.is_none(),
            "`{forbidden}` is declared as a dependency ({offender:?}) — the crate \
             compiling without it IS the decoupling criterion"
        );
    }
}

/// The meta-pin: the needles must be the REAL identifiers, and the scan must FIRE.
///
/// This exists because the first draft assembled needles by joining parts with a
/// separator character, which made every needle unmatchable — all three source
/// scans then passed a deliberately planted settings read in a function body and
/// reported green. A guard whose needle cannot prick is worse than no guard,
/// because it is trusted. So: assert each assembled spelling equals the identifier
/// built from its raw characters, and assert [`offenders`] actually reports a
/// planted hit for each.
#[test]
fn the_needles_are_the_identifiers_they_stand_for() {
    let loader: String = [
        'l', 'o', 'a', 'd', '_', 's', 'e', 't', 't', 'i', 'n', 'g', 's',
    ]
    .iter()
    .collect();
    assert_eq!(settings_loader_name(), loader);
    let file: String = [
        's', 'e', 't', 't', 'i', 'n', 'g', 's', '.', 'j', 's', 'o', 'n',
    ]
    .iter()
    .collect();
    assert_eq!(settings_file_name(), file);
    let driver: String = ['r', 'u', 's', 'q', 'l', 'i', 't', 'e'].iter().collect();
    assert_eq!(sqlite_driver_name(), driver);
    let desktop: String = [
        'a', 'r', 'c', 'h', 'i', 'm', 'e', 'd', 'e', 's', '_', 'l', 'i', 'b',
    ]
    .iter()
    .collect();
    assert_eq!(desktop_package_name(), desktop);
    let ui: String = ['t', 'a', 'u', 'r', 'i'].iter().collect();
    assert_eq!(ui_framework_name(), ui);

    // And the scan FIRES on planted code. Without this, a needle that matches
    // nothing is indistinguishable from a clean crate.
    for forbidden in [
        loader.as_str(),
        file.as_str(),
        driver.as_str(),
        desktop.as_str(),
    ] {
        let planted = vec![(
            PathBuf::from("planted.rs"),
            format!("fn f() {{ let _ = {forbidden}; }}"),
        )];
        assert_eq!(
            offenders(&planted, forbidden).len(),
            1,
            "the scan did not fire on a planted `{forbidden}` — it is matching nothing"
        );
    }

    // The UI-framework scan uses the token matcher, so it gets its own firing test.
    let planted_ui = strip_string_literals(&format!("fn f() {{ {ui}::run(); }}"));
    assert!(
        planted_ui
            .split(|c: char| !c.is_alphanumeric() && c != '_')
            .any(|tok| tok.eq_ignore_ascii_case(&ui)),
        "the token matcher did not fire on a real path segment: {planted_ui}"
    );
}

/// The vacuity guards for the two strippers.
///
/// Every pin here asserts an ABSENCE, so all of them pass on empty input, and both
/// strippers could over-eat and silently blank the very code the pins hunt. So:
/// feed each one code it must preserve and prose it must remove, and require both.
#[test]
fn prose_is_stripped_but_code_is_not() {
    let (loader, ui, driver) = (
        settings_loader_name(),
        ui_framework_name(),
        sqlite_driver_name(),
    );
    let sample = format!(
        "//! This module explains why {loader} and {ui} stayed desktop.\n\
         // A line comment naming {driver} and the other forbidden crate.\n\
         /* a block comment naming the settings file too */\n\
         pub fn reads() -> u32 {{\n    \
         let _ = {loader}();\n    \
         let _ = {ui}::noop();\n    \
         let _ = {driver}::open();\n    \
         0\n}}\n"
    );
    let stripped = strip_comments(&sample);
    assert!(
        !stripped.contains("stayed desktop"),
        "doc prose should be stripped, got:\n{stripped}"
    );
    for forbidden in [&loader, &ui, &driver] {
        assert!(
            stripped.contains(forbidden.as_str()),
            "the comment-stripper swallowed `{forbidden}` out of a real statement — \
             the pins would then pass vacuously. Stripped:\n{stripped}"
        );
    }

    // The literal-stripper: it must blank a directory literal WITHOUT blanking an
    // adjacent real path segment, or the UI-framework pin is blind.
    let dir_name = format!("src-{ui}");
    let with_path = format!("let root = dir.join(\"{dir_name}\").join(\"src\"); {ui}::run();");
    let literals_gone = strip_string_literals(&with_path);
    assert!(
        !literals_gone.contains(&dir_name),
        "the directory literal should be blanked, got {literals_gone}"
    );
    assert!(
        literals_gone.contains(&format!("{ui}::run")),
        "a real path segment must SURVIVE literal-stripping or the pin is blind, got {literals_gone}"
    );
}

// ── S5b: the REAL boundary test ──────────────────────────────────────────────
//
// # Why the six identifiers above were never a boundary
//
// Everything in the first half of this module hunts NAMES: the settings loader,
// the settings file, the writer, the SQLite driver, the desktop package, the UI
// framework. That is a list of six identifiers, and S5b proved it is not the
// boundary: after `harness` moved, the crate was full of `crate::config::Settings`
// — a path into a desktop module that matches NONE of the six needles. The scans
// stayed green while the boundary was wide open, because a name-based scan can
// only find the names someone thought to list.
//
// # What this scan asserts instead
//
// A PATH. `crate::X` inside this crate can only mean one of this crate's own
// modules; a `crate::X::` whose `X` is not a module `lib.rs` declares is a path
// into the desktop, i.e. the package edge itself. That catches the whole family at
// once — `config`, `storage`, `open`, and whatever a later move invents — without
// needing anyone to pre-name it. The brief's three named roots (`crate::config::`,
// `crate::storage::`, `crate::open::`) are ALSO scanned individually, so the
// specific regressions T3/T5 care about each carry their own message.
//
// # Why `crate::config::` in a doc comment is not a violation, and what is
//
// The crate's docs explain why `load_settings` and `open.rs` stayed desktop, and
// that prose is the documentation of the boundary. So the scan reads CODE, through
// the same `strip_comments` the other pins use — and the same
// [`prose_is_stripped_but_code_is_not`] discipline keeps that honest. String
// literals are stripped too (the same choice the UI-framework pin makes), which
// leaves one blind spot: a forbidden path hidden inside a string literal. That
// cannot smuggle a dependency — a path in a string does not compile into a
// reference — so the blind spot costs lint precision, not the invariant.
//
// # Fail-open-proof
//
// A scan whose needle cannot prick is worse than no scan, because it is trusted —
// that is the S4 lesson (`needle()` once joined its parts with a separator, every
// needle matched nothing, and all three original scans passed a planted settings
// read). So [`no_path_reaches_a_desktop_module`] is paired with
// [`the_path_needles_are_the_paths_they_stand_for`], which asserts each assembled
// spelling equals the path built from raw characters AND that the scan FIRES on
// planted code, in both an import and a call position.

/// The three desktop module roots S5b specifically closes: `config` (the settings
/// loader and the DTOs' former home), `storage` (the SQLite `Db`), `open` (the
/// Tauri opener). Assembled at runtime — see the module docs for why.
fn desktop_path_roots() -> Vec<(String, &'static str)> {
    vec![
        (
            format!(
                "{}::{}::",
                needle('\u{63}', "rate"),
                needle('\u{63}', "onfig")
            ),
            "the desktop settings module (the settings-loader disk boundary, T3, \
             and the settings DTOs' former home — Ruling 1 moved the types, not \
             the file)",
        ),
        (
            format!(
                "{}::{}::",
                needle('\u{63}', "rate"),
                needle('\u{73}', "torage")
            ),
            "the desktop persistence module (`Db` and its error type — the \
             SQLite-driver-wrapping half Step B-bis could not move)",
        ),
        (
            format!(
                "{}::{}::",
                needle('\u{63}', "rate"),
                needle('\u{6f}', "pen")
            ),
            "the desktop opener (`open.rs` names `tauri::Url` and \
             `tauri_plugin_opener`, so S3 injected it instead of moving it)",
        ),
    ]
}

/// Every module name `lib.rs` declares — the complete set `crate::X` may legally
/// name. Read from the manifest's own module list rather than a hand-maintained
/// array, so the pin cannot go stale by forgetting a move: a module that arrives
/// in a later sub-step is automatically allowed, and a path into the desktop is
/// automatically not.
fn declared_crate_modules() -> Vec<String> {
    let lib = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/lib.rs");
    let text = std::fs::read_to_string(&lib).expect("the crate has a lib.rs");
    let mut mods = Vec::new();
    for line in text.lines() {
        let t = line.trim();
        for prefix in ["pub mod ", "pub(crate) mod ", "mod "] {
            if let Some(rest) = t.strip_prefix(prefix) {
                let name = rest.trim_end_matches(';').trim().to_string();
                if !name.is_empty() {
                    mods.push(name);
                }
            }
        }
    }
    // The vacuity guard, same reason as `crate_code`'s: an empty allow-list would
    // make EVERY crate-internal path an offender (loud, so safe), but a `lib.rs`
    // this cannot parse would also make it empty, and then the pin is shouting
    // about the wrong thing.
    //
    // WHAT THE FLOOR ACTUALLY PROTECTS (measured at T8/S8.5, refreshed rather
    // than left as a formality). `lib.rs` declares 41 modules today, so `> 30`
    // looks like slack. It is not a headroom check either — it is the parser's
    // own liveness check. This function accepts exactly three spellings
    // (`pub mod `, `pub(crate) mod `, `mod `); the realistic failure is a fourth
    // arriving (a `#[cfg(test)] mod` line is already a fourth shape living in
    // this file at `mod boundary_pins;`, which the `mod ` prefix happens to
    // catch) or the prefixes drifting out of sync with `lib.rs`'s style. Either
    // mistake drops the parse to near-zero, and the floor turns "the allow-list
    // silently stopped including `harness`" into a panic naming the count. It
    // CANNOT catch a single missed module — one missing name makes its paths
    // report as offenders, which is the loud half — so do not raise this floor
    // toward 41 to "keep it honest": that would only make it fire on the next
    // legitimate `pub mod` and invite someone to delete it.
    assert!(
        mods.len() > 30,
        "parsed only {} of the crate's `mod` declarations out of lib.rs (41 as of \
         T8/S8.5) — the parser or the file is wrong, and the allow-list is not \
         trustworthy",
        mods.len()
    );
    mods
}

/// The path prefix `crate::`, assembled (this file must not contain it literally —
/// it is the thing it scans for, and exempting this file would be the loophole the
/// module docs refuse to take).
fn crate_path_prefix() -> String {
    needle('\u{63}', "rate::")
}

/// Every `crate::<segment>` in the given code, with the file and line it came from.
///
/// Deliberately textual rather than an AST walk: the crate cannot depend on a
/// parser (no `syn` in its graph, and adding one to service a test would be its own
/// decision). The textual rule is exact for the shape that matters — `crate::word`
/// — and both the `use` form and the call-site form spell it identically.
fn crate_path_segments(code: &[(PathBuf, String)]) -> Vec<(PathBuf, String, String)> {
    let prefix = crate_path_prefix();
    let mut found = Vec::new();
    for (path, body) in code {
        let body = strip_string_literals(body);
        let mut cursor = 0;
        while let Some(rel) = body[cursor..].find(&prefix) {
            let start = cursor + rel + prefix.len();
            // The segment: the identifier run that follows `crate::`.
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

/// THE BOUNDARY TEST: no path in the crate reaches a desktop module.
///
/// Two halves, one strict and one specific:
///
///   1. the three roots S5b named (`crate::config::`, `crate::storage::`,
///      `crate::open::`) must appear nowhere in crate code — each with its own
///      message naming the ruling it protects;
///   2. no `crate::X::` at all may name an `X` that `lib.rs` does not declare, so
///      a NEW desktop edge — `crate::agent::`, `crate::session::`, anything a
///      later move invents — reddens here too, without anyone adding a needle.
#[test]
fn no_path_reaches_a_desktop_module() {
    let code = crate_code();

    // (1) The three named roots.
    for (forbidden, why) in desktop_path_roots() {
        let hits = offenders(&code, &forbidden);
        assert!(
            hits.is_empty(),
            "`{forbidden}` appears in crate CODE at {hits:?} — {why}. After S5b the \
             crate has ZERO production edges to the desktop; a path back into it is \
             the package cycle T5 exists to sever, and `cargo check` will NOT catch \
             it on its own (the desktop lib still compiles, because the crate is \
             one of its dependencies — the edge only shows up when the crate is \
             built as its own package, i.e. exactly what this pin does)."
        );
    }

    // (2) The general rule, which is the actual boundary.
    let allowed = declared_crate_modules();
    let strays: Vec<(PathBuf, String, String)> = crate_path_segments(&code)
        .into_iter()
        .filter(|(_, seg, _)| !allowed.iter().any(|m| m == seg))
        .collect();
    assert!(
        strays.is_empty(),
        "crate paths name modules `lib.rs` does not declare, at {strays:?} — \
         `crate::` can only mean THIS crate, so any other root is a desktop module \
         and a package edge. Declared modules: {allowed:?}"
    );
}

/// The fail-open-proof for the boundary test, in the shape S4 established.
///
/// Without this, a needle that matches nothing is indistinguishable from a clean
/// crate — which is precisely how the six-identifier list managed to look green
/// while `crate::config::Settings` sat in production code.
#[test]
fn the_path_needles_are_the_paths_they_stand_for() {
    // (a) Each assembled needle is the REAL path, character for character.
    let rate: String = ['c', 'r', 'a', 't', 'e'].iter().collect();
    let expected: Vec<String> = ["config", "storage", "open"]
        .iter()
        .map(|root| format!("{rate}::{root}::"))
        .collect();
    let assembled: Vec<String> = desktop_path_roots()
        .into_iter()
        .map(|(needle, _)| needle)
        .collect();
    assert_eq!(
        assembled, expected,
        "an assembled path needle is not the real path — the scan is matching nothing"
    );
    assert_eq!(
        crate_path_prefix(),
        format!("{rate}::"),
        "`crate_path_prefix()` is not the real prefix"
    );

    // (b) The scan FIRES on planted code, in both spellings that matter: the
    //     `use` form and the call-site form.
    // The planted code must not name a forbidden IDENTIFIER literally either —
    // this file is inside the scan's own walk, so a literal here would trip the
    // settings-loader pin it sits next to. Assemble it, as everything else here.
    let loader = settings_loader_name();
    let db = needle('\u{44}', "b");
    for needle in &expected {
        for planted in [
            format!("use {needle}Settings;"),
            format!("fn f() {{ {needle}{loader}(); }}"),
            format!("fn f() {{ let _ = <{needle}{db} as Clone>::clone(&x); }}"),
        ] {
            let sample = vec![(PathBuf::from("planted.rs"), planted.clone())];
            assert_eq!(
                offenders(&sample, needle).len(),
                1,
                "the path scan did not fire on planted `{needle}`: {planted}"
            );
        }
    }

    // (c) The segment extractor FIRES, and reports the intruding root by name —
    //     otherwise half (2) of the boundary test could pass on an empty set.
    let planted = vec![(
        PathBuf::from("planted.rs"),
        format!("use {rate}::agent::worker::WorkerManager;"),
    )];
    let segs = crate_path_segments(&planted);
    assert_eq!(
        segs.len(),
        1,
        "the segment extractor found nothing in a planted desktop path"
    );
    assert_eq!(segs[0].1, "agent", "wrong segment extracted: {segs:?}");

    // (d) And it does NOT fire on a legitimate crate path — the pin must not be
    //     a machine that reports everything, or it will be silenced.
    let clean = vec![(
        PathBuf::from("clean.rs"),
        format!("use {rate}::harness::store::Store;"),
    )];
    let segs = crate_path_segments(&clean);
    assert_eq!(segs.len(), 1);
    assert_eq!(segs[0].1, "harness");
    let allowed = declared_crate_modules();
    assert!(
        allowed.iter().any(|m| m == "harness"),
        "`harness` must be declared in lib.rs or the allow-list is wrong: {allowed:?}"
    );
    assert!(
        !allowed
            .iter()
            .any(|m| m == "agent" || m == "config" || m == "storage" || m == "open"),
        "the allow-list contains a desktop root, so half (2) of the boundary test \
         is disarmed: {allowed:?}"
    );
}

/// The stripper half of the fail-open-proof for the new scan: prose about a
/// forbidden path must be stripped, code carrying one must not be.
#[test]
fn path_prose_is_stripped_but_code_is_not() {
    let roots = desktop_path_roots();
    let config = &roots[0].0;
    let storage = &roots[1].0;
    let loader = settings_loader_name();
    let db = needle('\u{44}', "b");
    let sample = format!(
        "//! This module explains why {config}{loader} stayed desktop.\n\
         // A line comment about {storage}{db} and why it cannot move.\n\
         /* a block comment naming {config}Settings too */\n\
         pub fn reads() -> u32 {{\n    \
         let _ = {config}{loader}();\n    \
         let _ = {storage}{db}::open();\n    \
         0\n}}\n"
    );
    let stripped = strip_comments(&sample);
    assert!(
        !stripped.contains("stayed desktop"),
        "doc prose should be stripped, got:\n{stripped}"
    );
    for needle in [&config, &storage] {
        assert!(
            stripped.contains(needle.as_str()),
            "the comment-stripper swallowed `{needle}` out of a real statement — \
             the boundary pin would then pass vacuously. Stripped:\n{stripped}"
        );
    }
}

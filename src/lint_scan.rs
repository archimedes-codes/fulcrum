//! The crate's ONE definition of "read the PRODUCTION region of a source file".
//!
//! # Why this module exists, and why it is a MOVE and not a new scanner
//!
//! `childproc`'s census grew the first version of this scanner, and it lives in
//! `#[cfg(test)] mod tests` — so no other module could reach it. When S8b.4 moved
//! five source-text lints into this crate they needed exactly that scanner, and
//! the honest options were (a) a shared `#[cfg(test)]` helper or (b) a local copy
//! per lint file. A third definition of "read the production region of a file"
//! inside one crate is precisely the drift the existing helpers exist to prevent,
//! so this is (a): the three items below are MOVED VERBATIM out of
//! `childproc::tests` (only their visibility and their doc cross-references
//! changed) and `childproc` now imports them from here.
//!
//! # `strip_comments` is NOT interchangeable with `production_region`
//!
//! [`crate::boundary_pins::strip_comments`] removes comments and keeps the whole
//! file. [`production_region`] removes comments **and truncates at the first
//! item-position `#[cfg(test)]`**. They are not interchangeable, and picking the
//! wrong one silently changes what is policed:
//!
//!   * a claim about PRODUCTION behaviour ("the ADC flow never spawns", "the
//!     crate never constructs the opener") must use [`production_region`], or a
//!     test-module line that legitimately names the forbidden thing — the stub
//!     `NoOpenRequester` in `worker/core.rs`'s test module, for instance — either
//!     false-fails the lint (and gets it silenced) or forces the author to widen
//!     the needle;
//!   * a claim about the WHOLE FILE, including its tests ("this module contains
//!     no header injection point anywhere, not even in a test") must use
//!     [`crate::boundary_pins::strip_comments`] or a raw read, or the truncation
//!     NARROWS the guarantee to the part that ships.
//!
//! Each lint in [`crate::source_lints`] states which one it uses and why.
//!
//! [`production_region`] also carries the census's SCOPE rule — it returns an
//! empty region for `**/tests.rs` and anything under `/tests/` — so a moved lint
//! inherits "test targets are not production code" rather than restating it.

/// One surviving line of a production region: its 1-based number in the file (so
/// a violation reads as `file:line`) and its text.
pub(crate) struct SourceLine {
    pub(crate) line: usize,
    pub(crate) text: String,
}

/// Whether a trimmed line is COMMENT text, which the census ignores — for
/// the `#[cfg(test)]` boundary and for every match. Not cosmetic: three
/// prose occurrences would otherwise be false positives or, worse, cut the
/// scanned region short (`src/commands/update.rs` has a doc comment
/// describing Tauri upstream's `Command::new(path)…`, this module's `//!`
/// header contains `Command::new(`, and `worker/manager.rs`'s doc comment
/// contains `` `#[cfg(test)]` ``, which under a "line contains" rule would
/// silently leave ~1,100 lines unscanned forever). `///`, `//!`, `/*` and
/// `*/` are already covered by `//` and `*`; they are listed anyway so the
/// rule reads as intended.
///
/// The `*` prefix is the one honest COST of a line-based lint: it also drops
/// ~29 lines of deref ASSIGNMENTS (`*self.usage.lock()… = …`), so a
/// `Command::new(` written as the target of a deref-assignment on its own
/// line would be missed. No such line exists, and a spawn is a call on a
/// bound `let mut cmd` in this codebase, so the blind spot costs nothing
/// against the prose it buys — but it is a blind spot, not a subtlety.
pub(crate) fn is_comment(trimmed: &str) -> bool {
    ["//", "///", "//!", "/*", "*", "*/"]
        .iter()
        .any(|prefix| trimmed.starts_with(prefix))
}

/// The line index the PRODUCTION region ends at: the first line whose trimmed
/// text STARTS WITH `#[cfg(test)]` — an attribute in item position, since a
/// boundary that merely *contains* the token is prose and must also survive
/// [`is_comment`]. `None` when the file has no test module.
///
/// This is the ONE definition of that boundary. [`production_region`] truncates
/// on it, and a lint that needs to read BOTH sides of it (the crate-side half of
/// `the_crate_threads_the_opener_into_its_production_loop` asserts something
/// about the production region *and* about the test region below it) reads it
/// from here rather than re-deriving `position(|l| l.starts_with("#[cfg(test)]"))`
/// a second time.
pub(crate) fn production_boundary(lines: &[&str]) -> Option<usize> {
    lines.iter().position(|l| {
        let trimmed = l.trim();
        trimmed.starts_with("#[cfg(test)]") && !is_comment(trimmed)
    })
}

/// The PRODUCTION region of one file: every code line before
/// [`production_boundary`], with comment lines already dropped. Returns an empty
/// `Vec` for paths matching `**/tests.rs` or containing `/tests/`.
///
/// Panics rather than skipping an unreadable/unparseable file: a census
/// that quietly stops at the first odd file is a census that passes for the
/// wrong reason.
pub(crate) fn production_region(path: &std::path::Path) -> Vec<SourceLine> {
    let norm = path.to_string_lossy().replace('\\', "/");
    if norm.ends_with("/tests.rs") || norm == "tests.rs" || norm.contains("/tests/") {
        return Vec::new();
    }
    let text = std::fs::read_to_string(path)
        .unwrap_or_else(|e| panic!("census cannot read {}: {e}", path.display()));
    let lines: Vec<&str> = text.lines().collect();
    let end = production_boundary(&lines).unwrap_or(lines.len());
    let mut region = Vec::new();
    for (idx, raw) in lines[..end].iter().enumerate() {
        if is_comment(raw.trim()) {
            continue;
        }
        region.push(SourceLine {
            line: idx + 1,
            text: raw.to_string(),
        });
    }
    region
}

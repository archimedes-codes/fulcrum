//! Source-text lints that moved INTO this crate in **T8b/S8b.4**.
//!
//! # Why they had to move, and why no path tweak could have kept them
//!
//! Six of this repo's structural pins lived on the DESKTOP and read the ENGINE's
//! source by path (`../crates/fulcrum-core/src/…`). After T8b the desktop consumes
//! this crate as a **git** dependency, and a cargo git checkout has **no stable
//! path** — it lands under `~/.cargo/git/checkouts/<name>-<hash>/<rev>/`. So those
//! reads cannot survive the split at all: not with a path edit, not with a CI step,
//! and not by "duplicating the lint on both sides", because the crate-side half of
//! a duplicate has nowhere to live. A lint belongs with the source it lints, so
//! that half comes here, where `env!("CARGO_MANIFEST_DIR")/src/…` names the same
//! file in BOTH layouts (workspace member and standalone checkout — the DEPTH of
//! `CARGO_MANIFEST_DIR` is what changes between them, which is why nothing below
//! ever counts `.parent()`s).
//!
//! This is `AGENTS.md`'s recorded hazard class — "integration tests that read
//! SOURCE TEXT by PATH … a move reddens them with no compile error anywhere" —
//! caught before the move instead of after it.
//!
//! # The lints here, and where their other half lives
//!
//! | lint | reads | desktop half |
//! |---|---|---|
//! | [`gcloud_rs_never_spawns_a_subprocess`] | `provider_auth/gcloud.rs` | none (moved whole) |
//! | [`gcloud_rs_never_calls_store`] | `provider_auth/gcloud.rs` | none (moved whole) |
//! | [`oidc_module_has_no_header_injection_point`] | `provider_auth/oidc.rs` | none (moved whole) |
//! | [`callback_state_check_is_constant_time_lint`] | `mcp/callback.rs` | none (moved whole) |
//! | [`the_constant_time_compare_is_imported_not_copy_pasted`] | crate `provider_auth/` | `tests/provider_oidc.rs` |
//! | [`every_crate_mcp_manager_carries_the_open_requester`] | `harness/loop.rs` | none (moved whole) |
//! | [`the_crate_threads_the_opener_into_its_production_loop`] | `worker/core.rs` | `open.rs` (the desktop needle) |
//! | [`the_harness_takes_the_opener_instead_of_constructing_it`] | `harness/loop.rs` | none (moved whole) |
//! | [`every_denial_marking_site_in_the_batch_is_accounted`] | `harness/loop/turn.rs`, `harness/loop/tools.rs`, `interactive/mod.rs` | none (this crate's own refusal guard) |
//!
//! The ones with a desktop half were **SPLIT, not moved**: moving a two-sided lint
//! whole silently drops the half that polices the other tree, which is the
//! fail-open this run has been bitten by repeatedly. Each side carries **its own
//! vacuity floor**, so a side that reads nothing cannot pass.
//!
//! # Scanner reuse — one definition, and a deliberate semantic choice
//!
//! [`crate::lint_scan::production_region`] (comments gone **and** truncated at the
//! first item-position `#[cfg(test)]`) is used where the claim is about
//! PRODUCTION code; [`crate::boundary_pins::strip_comments`] (comments gone, whole
//! file kept) is used where the claim is about the WHOLE FILE. They are **not**
//! interchangeable and each lint below says which it uses: truncating a whole-file
//! claim narrows the guarantee to the part that ships, and not truncating a
//! production claim lets a test-module stub false-fail the lint — and a lint that
//! false-fails gets silenced, which is the same outcome as never having it.
//!
//! ⚠ One measured reason this is not pedantry: `harness/loop.rs` carries an
//! item-position `#[cfg(test)]` at line 54 on a `use` statement, so its production
//! region is 53 lines and the two `McpManager::new(` sites are NOT in it. "Just use
//! the census scanner everywhere" would have made the manager-wiring lint read a
//! region containing neither site — and its `assert_eq!(count, 2)` would then have
//! gone red for the wrong reason, or been "fixed" to `assert_eq!(count, 0)`, which
//! is a lint that polices nothing. Those two lints therefore keep their ORIGINAL
//! region rule (drop comment lines, keep the whole file), reimplemented once here
//! as [`code_lines`] rather than per lint.
//!
//! # Why two needles are assembled at runtime
//!
//! This file is inside the scans `childproc` and `boundary_pins` run over the
//! crate, and it NAMES the constructs it forbids. So the two needles that are
//! themselves forbidden in crate code — a raw process constructor (`childproc`
//! invariant A) and a path into the desktop's opener module
//! (`no_path_reaches_a_desktop_module`) — are built through
//! [`crate::boundary_pins::needle`] with one character escaped, exactly the trick
//! that module documents: exempting a file is a loophole, making the SOURCE TEXT
//! differ from the runtime string is not.
//! [`the_assembled_needles_are_the_constructs_they_stand_for`] keeps that honest.

use crate::boundary_pins::{needle, strip_comments};
use crate::lint_scan::{production_boundary, production_region, SourceLine};

/// This package's manifest directory, read exactly ONCE and never re-derived by
/// counting `.parent()`s — the mistake S8b.2 had to undo. It is
/// `<repo>/crates/fulcrum-core` while the crate is a workspace member and
/// `<copy>` when the crate stands alone, and every path below is
/// `manifest_dir()/src/…`, which is correct in BOTH.
fn manifest_dir() -> &'static std::path::Path {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
}

/// The crate's own `src/` — the ONLY root any lint here reads. No lint in this
/// file can reach the desktop's tree, which is what makes them survive T8b.
fn crate_src(relative: &str) -> std::path::PathBuf {
    let path = manifest_dir().join("src").join(relative);
    assert!(
        path.is_file(),
        "{} is not a file in THIS package — a crate-side lint that cannot find its \
         subject must fail loudly here rather than scan a tree it did not open",
        path.display()
    );
    path
}

/// A file's lines with COMMENT lines dropped, keeping 1-based line numbers, over
/// the WHOLE file (no `#[cfg(test)]` truncation).
///
/// This is the region the `harness/loop.rs` lints read, because the desktop
/// originals dropped comment lines and kept the whole file. Comment-dropping is
/// the fail-OPEN direction this repo refuses: a doc comment naming
/// `.with_open_requester(` must not credit a construction site that never calls
/// it — and this very file's prose names every forbidden construct below it.
fn code_lines(path: &std::path::Path) -> Vec<(usize, String)> {
    let text = std::fs::read_to_string(path)
        .unwrap_or_else(|e| panic!("census cannot read {}: {e}", path.display()));
    text.replace("\r\n", "\n")
        .lines()
        .enumerate()
        .map(|(i, l)| (i + 1, l.to_string()))
        .filter(|(_, l)| !l.trim_start().starts_with("//"))
        .collect()
}

/// The corpus both gcloud ADC censuses share.
///
/// # The windows decision — the gate is DROPPED, deliberately
///
/// These two arrived `#[cfg(not(windows))]` from
/// `src-tauri/tests/provider_gcloud_adc.rs`, a file with FIVE such gates covering
/// behavioural tests that drive a live ADC file through platform-specific
/// home-directory discovery. These two lints are not that: they read one source
/// file as TEXT and match strings. No process, no platform path convention beyond
/// `CARGO_MANIFEST_DIR`, no env var — and `gcloud.rs`'s own `#[cfg(windows)]` /
/// `#[cfg(not(windows))]` arms are both *inside* the file being scanned, so the
/// scan sees the same text on every OS.
///
/// Carrying the gate in would make these the only two child-process censuses in a
/// crate whose sibling census (`childproc`) has **zero** `cfg(not(windows))` gates
/// and runs on all three OSes — i.e. the only censuses that go silent on the one
/// platform where a console-spawning bug manifests. Dropping it makes them run on
/// Windows for the first time, so the risk of THAT was measured, not assumed: the
/// three needles occur nowhere in `gcloud.rs` below its `#[cfg(test)]` line (the
/// region the gate would have skipped), so the widened scope has nothing new to
/// trip on. Same argument, same choice, for [`gcloud_rs_never_calls_store`].
///
/// ⚠ That is a reasoning claim, not a Windows test result. This machine has no
/// wine and no `lib.exe`; the only local Windows evidence is
/// `cargo check --target x86_64-pc-windows-gnu --all-targets`, which proves these
/// lints COMPILE for Windows and says NOTHING about whether they pass there.
fn gcloud_region() -> Vec<SourceLine> {
    // `production_region` returns an empty Vec for `**/tests.rs` and anything
    // under `/tests/`; a leaf named `gcloud.rs` is neither, so this is a region.
    production_region(&crate_src("provider_auth/gcloud.rs"))
}

/// The flow's headline claim: **zero `gcloud` CLI subprocess**. The token comes
/// from a `reqwest` POST to Google's endpoint — no CLI, no PATH dependency, no
/// user machine state beyond the ADC file.
///
/// A LINT, not a parser: it matches TEXT, so a string literal containing one of
/// these needles would also trip it (the honest trade every grep-level lint makes
/// — a false alarm costs one rename, a false pass costs a subprocess per resolve).
///
/// Region: the PRODUCTION region — the claim is about shipped behaviour.
#[test]
fn gcloud_rs_never_spawns_a_subprocess() {
    // The raw constructor is itself forbidden in crate code by `childproc`
    // invariant A, and this file is inside that census's walk — so the needle is
    // assembled, exactly as `boundary_pins` does. The escape is load-bearing: an
    // earlier draft of THAT module joined needles with a separator, which made
    // every needle unmatchable and let all three scans pass a planted violation.
    let spawn = needle('\u{43}', "ommand::new(");
    let needles: [&str; 3] = [spawn.as_str(), "process::Command", "tokio::process"];
    let region = gcloud_region();
    assert!(
        region.len() > 100,
        "the census is vacuous — it scanned {} lines of gcloud.rs",
        region.len()
    );
    let hits: Vec<String> = region
        .iter()
        .filter(|line| needles.iter().any(|n| line.text.contains(n)))
        .map(|line| format!("gcloud.rs:{}: {}", line.line, line.text.trim()))
        .collect();
    assert!(
        hits.is_empty(),
        "the ADC flow must never spawn a subprocess (issue #128 — the exchange is a \
         reqwest POST):\n{}",
        hits.join("\n")
    );
}

/// The security census: the ADC module must never reach the provider-auth store.
/// A stray store-write would persist the minted access token into
/// `provider-auth.json` beside the ADC refresh token and client secret — exactly
/// the secret-at-rest this flow exists to keep out of a plaintext file (the
/// in-memory 50-minute cache is its ONLY home).
///
/// Region and windows reasoning: [`gcloud_rs_never_spawns_a_subprocess`].
#[test]
fn gcloud_rs_never_calls_store() {
    const NEEDLES: [&str; 3] = ["store_credential", "store::save", "store::"];
    let region = gcloud_region();
    assert!(
        region.len() > 100,
        "the census is vacuous — it scanned {} lines of gcloud.rs",
        region.len()
    );
    let hits: Vec<String> = region
        .iter()
        .filter(|line| needles_hit(line, &NEEDLES))
        .map(|line| format!("gcloud.rs:{}: {}", line.line, line.text.trim()))
        .collect();
    assert!(
        hits.is_empty(),
        "the ADC flow must never write the credential store (issue #128 — the minted \
         token lives only in the in-memory cache):\n{}",
        hits.join("\n")
    );
}

/// Whether any of `needles` appears in one scanned line. One spelling for both
/// gcloud censuses so the filter cannot drift between them.
fn needles_hit(line: &SourceLine, needles: &[&str]) -> bool {
    needles.iter().any(|n| line.text.contains(n))
}

/// The structural half of the header-hygiene criterion: `oidc.rs` contains NO
/// header injection point at all, so a gateway header CANNOT be attached to an
/// IdP-bound request without adding one (which this test reddens).
///
/// Region: the WHOLE FILE, comments stripped — deliberately NOT the production
/// region. The claim is "there is no way to attach a header here", and truncating
/// at the test module would narrow an absolute guarantee to the code that ships
/// while leaving the sentence above false. `oidc.rs`'s test module contains none
/// of the four needles, so widening costs nothing today; it is the widening that
/// keeps the claim true next month.
#[test]
fn oidc_module_has_no_header_injection_point() {
    let path = crate_src("provider_auth/oidc.rs");
    let text = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("{} is unreadable: {e}", path.display()));
    // Vacuity floor for the WHOLE-FILE form of scan. The gcloud lints' `> 100`
    // floor cannot be reused: that counts production-region LINES of a different
    // file, and a floor copied across two different corpora stops being a
    // discriminator. This one measures code lines surviving the stripper — the
    // file has ~1,050 with comments gone, and 400 separates "read the file" from
    // "read a fragment" without hugging the total (so an ordinary deletion does
    // not get it silenced).
    let code = strip_comments(&text);
    let surviving = code.matches("\n").count();
    assert!(
        surviving > 400,
        "this scan read only {surviving} code lines out of oidc.rs — vacuous, and a \
         whole-file claim about header injection cannot be made about a fragment"
    );
    for forbidden in [
        ".default_headers(",
        ".header(",
        ".headers(",
        ".bearer_auth(",
    ] {
        assert!(
            !code.contains(forbidden),
            "`{forbidden}` appeared in oidc.rs — an IdP-bound request can now carry a \
             header the provider row configured (the census guards the acceptance criterion)"
        );
    }
    // And the client really is built here (fresh + headerless), not accepted from
    // a caller who could stamp it. THIS is the load-bearing half: four absent
    // needles pass on an empty file, this one cannot.
    assert!(
        code.contains("reqwest::Client::builder()"),
        "oidc.rs must build its own client (the header-hygiene guarantee is structural)"
    );
}

/// The ONE constant-time compare, reached the only legal way: `provider_auth/oidc.rs`
/// IMPORTS the definition in `mcp/callback.rs` (ruling R4). A copy-pasted second
/// definition is the divergence this pins out.
///
/// Region: the WHOLE FILE for both files, comments stripped. A definition hidden
/// in a test module is still a second implementation of a security primitive, so
/// this scan must NOT stop at the test boundary.
///
/// # The split
///
/// The desktop original walked BOTH trees — `src-tauri/src/agent/provider_auth`
/// and `crates/fulcrum-core/src/provider_auth` — because T7/S7.1 moved seven of
/// nine files and left two. One test cannot do that after T8b (the desktop cannot
/// name the crate's tree, and the crate must never name the desktop's), so it is
/// TWO tests with the invariant stated in both places:
///   * this one, over `env!("CARGO_MANIFEST_DIR")/src/provider_auth`;
///   * `provider_oidc.rs::no_desktop_provider_auth_file_defines_a_constant_time_compare`,
///     over the desktop's own tree.
///
/// Each has its OWN vacuity floor. That is not decoration: the S8b.2 census shipped
/// exactly the bug this shape prevents — an AGGREGATE floor stayed green while one
/// root was completely blind, because the other root's count carried it.
#[test]
fn the_constant_time_compare_is_imported_not_copy_pasted() {
    let oidc = std::fs::read_to_string(crate_src("provider_auth/oidc.rs"))
        .expect("oidc.rs is readable (this lint is vacuous otherwise)");
    assert!(
        strip_comments(&oidc).contains("use crate::mcp::callback::constant_time_eq"),
        "oidc.rs must IMPORT the constant-time compare (ruling R4) — the import line \
         is missing, which means it is either defining its own or not comparing in \
         constant time at all"
    );
    // The callback file still OWNS the single definition (a move would silently
    // break the import, and this lint's premise with it). `pub` and not
    // `pub(crate)` because a package boundary erases `pub(crate)` and the
    // DESKTOP's importer has needed the wider visibility since T5/S3.
    let callback = std::fs::read_to_string(crate_src("mcp/callback.rs"))
        .expect("mcp/callback.rs is readable (the ONE definition lives there)");
    assert!(
        strip_comments(&callback).contains("pub fn constant_time_eq"),
        "the ONE definition must stay in mcp/callback.rs (`pub`, because a package \
         boundary erases `pub(crate)` and the desktop's provider_auth imports it)"
    );
    // And no file in THIS package's provider_auth tree may DEFINE one. A
    // DEFINITION is the copy-paste; an import is the fix. Comments are stripped
    // first because this suite's doc comments NAME the primitive they guard.
    let tree = manifest_dir().join("src").join("provider_auth");
    let mut offenders = Vec::new();
    let mut scanned = 0usize;
    for entry in
        std::fs::read_dir(&tree).unwrap_or_else(|e| panic!("the dir {tree:?} is readable: {e}"))
    {
        let path = entry.expect("an entry").path();
        if path.extension().and_then(|e| e.to_str()) != Some("rs") {
            continue;
        }
        scanned += 1;
        let text = std::fs::read_to_string(&path).expect("a readable source file");
        let body = strip_comments(&text);
        for definition in [
            "fn constant_time_eq",
            "fn ct_eq",
            "fn constant_time",
            "fn timing_safe_eq",
        ] {
            if body.contains(definition) {
                offenders.push(format!("{} defines `{definition}`", path.display()));
            }
        }
    }
    // CRATE-SIDE vacuity floor, PER SIDE. 8 `.rs` files live in `src/provider_auth`
    // at this commit (budget, cli_sso, gcloud, key_helper, litellm_oauth, mod,
    // oidc, store). The floor is a DISCRIMINATOR, not headroom: `> 3` sits above
    // what a root that drifts into a subtree yields, and far below 8 so an ordinary
    // move does not get it silenced. The desktop half asserts its OWN floor over
    // its OWN tree — neither side can carry the other, which is the whole point of
    // splitting instead of moving.
    assert!(
        scanned > 3,
        "the crate-side duplicate-compare scan read only {scanned} files out of \
         {tree:?} — VACUOUS (8 at this commit), so a wrong root cannot pass here",
    );
    assert!(
        offenders.is_empty(),
        "a second constant_time_eq definition appeared in the crate's provider_auth: \
         {offenders:?} — ruling R4 says ONE definition, in mcp/callback.rs, imported"
    );
}

/// The load-bearing STRUCTURAL pin of the OAuth `state` guard: the callback's
/// production region routes the guard through `constant_time_eq` and contains no
/// `String !=` comparison a revert would reintroduce. `cargo test` cannot observe
/// timing, so what is pinned is the SHAPE; behaviour is pinned by
/// `mcp/callback.rs`'s own tests (equal / first-byte / last-byte /
/// length-mismatch). NO test here claims to measure timing.
///
/// Region: as the desktop original had it — comment lines dropped, whole file.
/// The two slices it wants (the `wait` handler, the helper) are located by text,
/// and both sit in production; `callback.rs`'s test module deliberately NAMES the
/// forbidden shapes, which is why every slice here is comment-stripped.
#[test]
fn callback_state_check_is_constant_time_lint() {
    let path = crate_src("mcp/callback.rs");
    let source = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("reading {}: {e}", path.display()))
        .replace("\r\n", "\n");
    // Vacuity guard: the region we lint exists at all.
    let wait_start = source
        .find("pub async fn wait(")
        .expect("the wait handler exists");
    let wait_end = source[wait_start..]
        .find("\n}\n")
        .map(|i| wait_start + i)
        .expect("the wait handler closes");
    let wait_region = &source[wait_start..wait_end];
    let wait_code: String = wait_region
        .lines()
        .filter(|line| !line.trim_start().starts_with("//"))
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        wait_code.contains("constant_time_eq(state.as_bytes(), expected_state.as_bytes())"),
        "the `state` guard goes through the constant-time helper:\n{wait_code}"
    );
    // The revert detectors: any of these texts in the handler means the guard is
    // back to a short-circuiting comparison.
    for forbidden in [
        "state != expected_state",
        "!= expected_state",
        "state.as_str() !=",
        "expected_state !=",
        "state == expected_state",
    ] {
        assert!(
            !wait_code.contains(forbidden),
            "the callback must not compare `state` with `{forbidden}`: a `String !=` \
             comparison short-circuits on the first differing byte",
        );
    }
    // And the helper itself has no early exit inside its byte loop. The lint runs
    // over the CODE ONLY — comment lines are stripped first, or the helper's own
    // doc comment (which NAMES the forbidden constructs to explain why they are
    // absent) would trip every detector.
    let helper_start = source
        .find("pub fn constant_time_eq(")
        .expect("the ONE constant-time helper exists (ruling R4)");
    let helper_end = source[helper_start..]
        .find("\n}\n")
        .map(|i| helper_start + i)
        .expect("the helper closes");
    let helper: String = source[helper_start..helper_end]
        .lines()
        .filter(|line| !line.trim_start().starts_with("//"))
        .collect::<Vec<_>>()
        .join("\n");
    for forbidden in [
        "?;",
        "return true",
        "break",
        "continue",
        ".position(",
        ".iter().any(",
    ] {
        assert!(
            !helper.contains(forbidden),
            "the helper must not contain `{forbidden}` — an early exit on a byte \
             mismatch is exactly the timing leak this exists to avoid:\n{helper}"
        );
    }
    // Reachable by its importers, per R4. The spelling is `pub` and not
    // `pub(crate)` since T5/S3 moved the file into this crate: a package boundary
    // erases `pub(crate)`, so the narrower form would not reach the DESKTOP's
    // importer — the one importer this side can no longer see, and why the import
    // itself is pinned by [`the_constant_time_compare_is_imported_not_copy_pasted`]
    // rather than assumed.
    assert!(
        helper.contains("pub fn constant_time_eq"),
        "R4 needs the helper reachable by its importers (`pub`, because a package \
         boundary erases `pub(crate)`)"
    );
}

/// Every `McpManager::new(` in `harness/loop.rs` is followed by
/// `.with_open_requester(` before its closing `;`. Both sites are required, and
/// that is the point: `set_desktop_config` REPLACES the manager built by `new`, so
/// wiring only the first one leaves the production loop — the one that runs with a
/// packaged `mcpServers` map — on the no-op default.
///
/// Moved whole from the desktop's `open.rs` in S8b.4: it asserts nothing about the
/// desktop, only about this crate's construction sites, and after T8b the desktop
/// cannot read this file at all.
///
/// Region: the WHOLE FILE with comment lines dropped, NOT the production region —
/// see the module docs for the measured reason.
#[test]
fn every_crate_mcp_manager_carries_the_open_requester() {
    let code = code_lines(&crate_src("harness/loop.rs"));
    let ctor_lines: Vec<usize> = code
        .iter()
        .filter(|(_, l)| l.contains("McpManager::new("))
        .map(|(n, _)| *n)
        .collect();
    assert_eq!(
        ctor_lines.len(),
        2,
        "expected exactly 2 `McpManager::new(` construction sites in harness/loop.rs \
         (`AgentLoop::new` + the `set_desktop_config` rebuild), found {ctor_lines:?} — a \
         different count means this lint's premise moved, so either a site lost its \
         wiring or a new one appeared that this lint does not read",
    );
    for (n, ctor_line) in ctor_lines.iter().enumerate() {
        // The region runs from the constructor to the next one (or EOF), so a
        // missing first wiring cannot be credited by the second site's call.
        let stop = ctor_lines.get(n + 1).copied().unwrap_or(usize::MAX);
        let wired: Vec<usize> = code
            .iter()
            .filter(|(line, text)| {
                *line > *ctor_line && *line < stop && text.contains(".with_open_requester(")
            })
            .map(|(line, _)| *line)
            .collect();
        assert!(
            !wired.is_empty(),
            "construction site #{} (`McpManager::new(` at harness/loop.rs:{ctor_line}) \
             never reaches `.with_open_requester(` — the manager would run on the no-op \
             opener, so the MCP `auth` action would silently never open a browser and \
             its OAuth flow would die at the callback timeout",
            n + 1
        );
    }
}

/// One function's body: the lines from the signature that starts with `signature`
/// to the line its opening brace closes, comment lines already dropped.
///
/// Panics if the signature is absent or unmatched — a census over a region it
/// could not find is a census that passes for the wrong reason.
fn function_body(path: &std::path::Path, signature: &str) -> Vec<(usize, String)> {
    let code = code_lines(path);
    let start = code
        .iter()
        .find(|(_, l)| l.trim_start().starts_with(signature))
        .unwrap_or_else(|| {
            panic!(
                "no line starting `{signature}` in {} — the census's subject moved, so \
                 it would silently read the wrong region",
                path.display()
            )
        })
        .0;
    let mut depth = 0usize;
    let mut opened = false;
    let mut end = None;
    for (n, l) in code.iter().filter(|(n, _)| *n >= start) {
        depth += l.matches('{').count();
        depth = depth.saturating_sub(l.matches('}').count());
        if l.contains('{') {
            opened = true;
        }
        if opened && depth == 0 {
            end = Some(*n);
            break;
        }
    }
    let end = end.unwrap_or_else(|| panic!("`{signature}` in {} never closes", path.display()));
    code.into_iter()
        .filter(|(n, _)| *n >= start && *n <= end)
        .collect()
}

/// The refusal guard's denial-marking sites, COUNTED. The census that survives a
/// corridor nobody has thought of yet.
///
/// # Why this exists when several behavioural pins already do
///
/// The identical-refusal guard ends a user's prompt when it is wrong, so every
/// shape that is NOT "the engine told the model to re-send this" has to be
/// excluded from it. Three CORRIDORS are excluded today — the `Ask`-answered-no
/// denial, `Decision::Deny`, and a `sudo_exec` sub-prompt refusal — each by a
/// `bool` threaded from the arm that KNOWS, and each by its own behavioural pin.
///
/// A FOURTH exclusion arrived by a different route, and the census had to grow to
/// see it: `sudo_exec`'s command can be CONFIRMED, RUN and FAIL, which is nobody's
/// argument refusal either. That exclusion is not a third `true` in
/// `run_tool_batch` — it is a THIRD ELEMENT of the tuple, a `ran` flag forwarded
/// from `sudo_run_flow`'s post-run arms. So the counts below are now over TWO
/// flags, and they are counted PER OCCURRENCE rather than per line: two flags
/// riding one line would otherwise let a dropped flag leave the count untouched,
/// which is precisely the drift this lint exists to catch.
///
/// None of the pins can see a corridor nobody thought of. A refusal site that
/// forgets to mark itself is invisible to a test that scripts only the shapes its
/// author already knew about, and a non-exhaustive enum would not help either: an
/// unhandled variant still compiles at every `matches!`. The `sudo_exec` confirm
/// refusal is the proof — it was a live false positive for a whole commit, at
/// **4 provider calls and one `turn_capped`** on a user's own three "no" clicks,
/// with a green pin on either side of it. The granted-confirm-then-failed-command
/// shape is the SECOND proof: same tool, same omitted `reason`, the opposite
/// verdict, and it too measured **4 provider calls and one `turn_capped`** — with
/// the declined-confirm pin green the whole time, because that pin scripts a
/// DECLINED confirm and this shape is not one.
///
/// What does survive is a COUNT. This lint asserts the number of denial-marking
/// sites in `run_tool_batch`'s body, so a fourth corridor that has not been
/// reasoned about reddens HERE and forces its author to name it.
///
/// # The second half is the half that actually caught the sudo case
///
/// Counting `true` in `run_tool_batch` alone would have MISSED the sudo corridor:
/// that refusal arrives through `dispatch_tool`, and the bug was that the three
/// `dispatch_tool` call sites hardcoded `false` instead of forwarding the flag.
/// So the census also pins (a) how many `dispatch_tool` forwards there are, (b)
/// that NONE of them pins the flag at the call site, and (c) that `dispatch_tool`
/// forwards rather than decides. A corridor whose flag is dropped on the floor is
/// the failure mode, and (b) is what forbids it.
///
/// Region: each named function's BODY, brace-matched from its signature, comment
/// lines dropped — built on [`code_lines`], so it keeps the whole-file region rule
/// the other `harness/loop*` lints use and for the reason they state. The body is
/// brace-matched rather than the file scanned because a needle that fired file-wide
/// would credit a test module's `true` to production; `turn.rs` has no test module
/// today and `interactive/mod.rs` has a large one.
#[test]
fn every_denial_marking_site_in_the_batch_is_accounted() {
    let body = function_body(
        &crate_src("harness/loop/turn.rs"),
        "pub(super) async fn run_tool_batch(",
    );

    // ── the denial-marking sites: a `true` that means "a person refused this" ──
    // Two needles because the two arms differ in shape: the `Ask`-answered-no arm
    // builds a literal `ToolResult` whose flag is a line of its own, and the
    // `Decision::Deny` arm is a single expression. A THIRD needle form would be an
    // unmarked corridor — which is what the exact-count assert is for, not an
    // inconvenience.
    let constructed: Vec<usize> = body
        .iter()
        .filter(|(_, l)| l.trim() == "true,")
        .map(|(n, _)| *n)
        .collect();
    let inline: Vec<usize> = body
        .iter()
        .filter(|(_, l)| l.contains("deny_tool_result(tc), true"))
        .map(|(n, _)| *n)
        .collect();
    let total = constructed.len() + inline.len();
    assert_eq!(
        total, 2,
        "expected exactly 2 denial-marking sites inside `run_tool_batch` (the \
         `Ask`-answered-no arm and `Decision::Deny`), found {total} (constructed \
         {constructed:?}, inline {inline:?}). A different count means a corridor was \
         added or removed: name it in `refused_before_the_model_could_act`'s docblock \
         and in `MAX_IDENTICAL_REFUSAL_RUNS`'s, and add its behavioural pin — THEN \
         update this number. Updating the number without the sentence is how the \
         unmarked over-count comes back.",
    );
    // Each half non-zero: an aggregate that is right while one root is empty is
    // the vacuity class `childproc`'s per-root floors exist for.
    assert_eq!(
        constructed.len(),
        1,
        "expected exactly 1 constructed denial (`true,` on its own line — the \
         `Ask`-answered-no arm), found {constructed:?}",
    );
    assert_eq!(
        inline.len(),
        1,
        "expected exactly 1 inline denial (`Decision::Deny`), found {inline:?}",
    );

    // ── the forwarding sites: corridors that arrive THROUGH `dispatch_tool` ──
    let forwards: Vec<usize> = body
        .iter()
        .filter(|(_, l)| l.contains("self.dispatch_tool(tc, turn).await"))
        .map(|(n, _)| *n)
        .collect();
    assert_eq!(
        forwards.len(),
        3,
        "expected exactly 3 `dispatch_tool` call sites inside `run_tool_batch`, found \
         {forwards:?} — this census reads the flag every one of them forwards, so a \
         4th dispatch is a corridor this lint has not been told about",
    );
    // THE HALF THAT MATTERS: no forward may pin the flag at the call site. A
    // `(…, false)` here silently drops every refusal `dispatch_tool` knows about —
    // exactly how the `sudo_exec` confirm refusal stayed a false positive while both
    // behavioural pins stayed green.
    let pinned_false: Vec<usize> = body
        .iter()
        .filter(|(_, l)| l.contains("self.dispatch_tool(tc, turn).await") && l.contains("false"))
        .map(|(n, _)| *n)
        .collect();
    assert!(
        pinned_false.is_empty(),
        "the `dispatch_tool` forwards at {pinned_false:?} pin the refusal flag at the \
         call site instead of forwarding it — that drops the `sudo_exec` sub-prompt \
         refusal (and any refusal `dispatch_tool` learns about later) on the floor, \
         and the resulting over-count ends a user's prompt over their own \"no\"",
    );
    // The per-line needle above is DEFEATED by hoisting (`let (r, _) =
    // dispatch_tool(…); (r, false)` puts the call and the `false` on different
    // lines and compiles) — the exact mutation class `AGENTS.md`'s "prove it with a
    // mutation that still compiles — hoist a value into a local, don't delete it"
    // rule names, and it defeated the first version of this assert. So the
    // aggregate closes the hole: the body answers "not a person's refusal" and
    // "the executor did not run it" in EXACTLY twelve places, each one an arm that
    // genuinely cannot be either (a child's result, two cancellation arms, two
    // unknown-param rejections — TWO flags each — plus the two denial arms, which
    // mark `denied: true` and so write only the `ran` half). A hoisted `false` is a
    // THIRTEENTH and reddens here.
    //
    // THE COUNT WENT 5 → 12 because the tuple widened from `(result, denied)` to
    // `(result, denied, ran)`, NOT because a corridor was added or removed: the
    // five ARMS are unchanged, each now answers two flags. It is therefore counted
    // PER OCCURRENCE rather than per line — with two flags riding the same line a
    // per-line count would credit one arm with one answer and would not move when a
    // flag was dropped from it, which is the very thing this assert watches for.
    // The per-line count is asserted alongside it (8 lines) so a change in EITHER
    // direction — a flag dropped, an arm added — moves a number someone has to
    // explain, the same pair-of-floors shape the `total` / `constructed` / `inline`
    // trio above uses.
    let not_refused: Vec<usize> = body
        .iter()
        .flat_map(|(n, l)| std::iter::repeat_n(*n, l.matches("false").count()))
        .collect();
    assert_eq!(
        not_refused.len(),
        12,
        "expected exactly 12 `false` answers in `run_tool_batch` (the five arms that \
         neither a person refused nor ran — a child's result, the two cancellation \
         arms, the two unknown-param rejections — at TWO flags each, plus the two \
         denial arms marking only `ran`), found {} at {not_refused:?} — a 13th is a \
         place that stops forwarding one of the flags, most likely by hoisting the \
         dispatch result into a local and pinning it, which the per-line assert \
         above cannot see",
        not_refused.len(),
    );
    let not_refused_lines: Vec<usize> = body
        .iter()
        .filter(|(_, l)| l.contains("false"))
        .map(|(n, _)| *n)
        .collect();
    assert_eq!(
        not_refused_lines.len(),
        8,
        "expected exactly 8 LINES answering a `false` flag in `run_tool_batch` (the \
         five never-refused-never-run arms, the constructed denial's `ran`, and the \
         `Decision::Deny` arm's `ran`), found {} at {not_refused_lines:?} — 12 flag \
         answers spread over fewer lines means two flags were collapsed onto an arm \
         that should not carry them",
        not_refused_lines.len(),
    );

    // `dispatch_tool` FORWARDS, it does not decide: its own body marks no denial.
    // The one refusal it can pass on comes from `sudo_run_flow`.
    let dispatch = function_body(
        &crate_src("harness/loop/tools.rs"),
        "pub(super) async fn dispatch_tool(",
    );
    let dispatch_decides: Vec<usize> = dispatch
        .iter()
        .filter(|(_, l)| l.trim() == "true," || l.contains("), true"))
        .map(|(n, _)| *n)
        .collect();
    assert!(
        dispatch_decides.is_empty(),
        "`dispatch_tool` must not DECIDE a refusal, it forwards one; found `true` \
         markings at {dispatch_decides:?} — a refusal built there belongs in the \
         `run_tool_batch` census above, not in the dispatcher",
    );
    // And it really does read the flow's flag. Without this the empty `true` count
    // above would be satisfiable by deleting the capability — the absence pin the
    // `childproc` daemon census documents.
    let sudo_sites: Vec<usize> = dispatch
        .iter()
        .filter(|(_, l)| l.contains("sudo_run_flow("))
        .map(|(n, _)| *n)
        .collect();
    assert_eq!(
        sudo_sites.len(),
        1,
        "expected exactly 1 `sudo_run_flow(` call inside `dispatch_tool`, found \
         {sudo_sites:?} — the sudo sub-prompt refusal is the corridor this half of the \
         census reads; without it the empty `true` count above proves nothing",
    );

    // `sudo_run_flow` marks exactly the two arms a PERSON decided, and marks the
    // engine's own refusals `false`.
    let flow = function_body(
        &crate_src("interactive/mod.rs"),
        "pub async fn sudo_run_flow(",
    );
    let flow_refused: Vec<usize> = flow
        .iter()
        .filter(|(_, l)| l.trim() == "true,")
        .map(|(n, _)| *n)
        .collect();
    assert_eq!(
        flow_refused.len(),
        2,
        "expected exactly 2 refusals marked in `sudo_run_flow` (the declined confirm + \
         the cancelled password entry), found {flow_refused:?} — a third `true` there is \
         a new sub-prompt that needs naming here and in the predicate's docblock, and a \
         missing one re-arms the over-count",
    );
    let flow_not_refused: Vec<usize> = flow
        .iter()
        .filter(|(_, l)| l.trim() == "false," || l.contains("(result, false)"))
        .map(|(n, _)| *n)
        .collect();
    assert!(
        !flow_not_refused.is_empty(),
        "`sudo_run_flow` must also mark what is NOT a person's refusal (the \
         empty-`command` arm and the two sub-prompt arms' `ran` half) — without those, \
         the two `true` markings above are an unexplained pair and nothing says the \
         executor's own failures are still counted"
    );

    // ── the SECOND flag: `ran`, the executor attested it accepted and ran ──
    // The census above reads the `denied` half, which excludes a PERSON's "no".
    // This reads the half that excludes a COMMAND that ran and failed, and it is
    // the mirror false positive: `sudo_exec {"command":"x"}` (the advertised
    // `required` `reason` DEFAULTED by this very flow) + a GRANTED confirm + a
    // wrong password was measured at 4 provider calls and one `turn_capped`, on a
    // sentence that invites the retry.
    //
    // ONE marking site, because the flow's four post-run arms build one `result`
    // and share one tail return. That single line is the whole of the exclusion, so
    // it gets both halves of the pair the `denied` census uses: the count, and a
    // positive assertion that the arms which must NOT claim a run do not.
    let flow_ran: Vec<usize> = flow
        .iter()
        .filter(|(_, l)| l.contains("(result, false, true)"))
        .map(|(n, _)| *n)
        .collect();
    assert_eq!(
        flow_ran.len(),
        1,
        "expected exactly 1 `ran` marking in `sudo_run_flow` — the shared tail return \
         of the four POST-RUN arms (process failure, auth failure, the classifier's \
         error verdict, success), found {flow_ran:?}. Deleting it re-arms the false \
         positive that ends a user's prompt mid-authentication: a command that RAN and \
         FAILED would be counted as an argument refusal again, because the flow \
         DEFAULTS the `reason` this tool advertises as `required`",
    );
    // And the three arms that return BEFORE any run must not claim one. FOUR
    // `false,` lines in total: the empty-`command` rejection writes both flags
    // (`denied` then `ran`), and each of the two sub-prompt refusals writes only
    // its `ran` half — their `denied` half is the `true,` counted above.
    let flow_not_ran: Vec<usize> = flow
        .iter()
        .filter(|(_, l)| l.trim() == "false,")
        .map(|(n, _)| *n)
        .collect();
    assert_eq!(
        flow_not_ran.len(),
        4,
        "expected exactly 4 `false,` flag answers in `sudo_run_flow` (the empty-`command` \
         arm writing both flags, plus each sub-prompt refusal writing its `ran` half), \
         found {flow_not_ran:?} — one fewer is an arm that never ran now claiming it did, \
         which silences the guard on a shape that should still count (the empty-`command` \
         refusal is the card's headline shape for this tool), and an extra one is a flag \
         no longer forwarded",
    );
}

/// The crate's half of the opener seam: this crate's production loop is built
/// from a THREADED opener, never one the crate made.
///
/// # The split, and which half is which
///
/// The desktop's `open.rs::the_production_loop_caller_passes_the_desktop_opener`
/// read BOTH this crate's `worker/core.rs` (structure) and the desktop's `lib.rs`
/// (the needle: the desktop's real opener supplied at the one hand-off). After
/// T8b each side reads only its own tree:
///   * **here** — the STRUCTURE: exactly one production `#[cfg(test)]` split,
///     exactly one production `AgentLoop::new(`, `default_build_loop` still TAKES
///     the opener, that call passes the threaded value, production constructs
///     neither the desktop's opener nor a no-op, and the no-op stays a test-only
///     choice;
///   * **desktop** — the NEEDLE, over `src-tauri/src/lib.rs::run_worker`, which
///     keeps the original name in `open.rs`.
///
/// Neither half alone proves the chain. Together they chain to the same
/// conclusion the single pre-split lint did: the loop carries a value the crate
/// did not invent, and the desktop hands in the real one.
///
/// Region: the PRODUCTION region — and this is the one lint here that needs the
/// boundary itself, not just the region, because the last assert reads the TEST
/// region below it. Both come from [`production_boundary`], so "where production
/// ends" keeps one definition in this crate.
#[test]
fn the_crate_threads_the_opener_into_its_production_loop() {
    let path = crate_src("worker/core.rs");
    let source = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("reading {}: {e}", path.display()))
        .replace("\r\n", "\n");
    let lines: Vec<&str> = source.lines().collect();

    // The production region ends at the `#[cfg(test)]` that opens the test module.
    // Exactly one, or this split is a guess.
    let test_starts: Vec<usize> = lines
        .iter()
        .enumerate()
        .filter(|(_, l)| l.trim_start() == "#[cfg(test)]")
        .map(|(i, _)| i)
        .collect();
    assert_eq!(
        test_starts.len(),
        1,
        "expected exactly 1 `#[cfg(test)]` in worker/core.rs to split production from \
         the stub fixtures, found {test_starts:?} — this lint cannot tell the \
         production `AgentLoop::new(` from the stub's without that boundary",
    );
    // The shared scanner's OWN boundary, cross-checked against this lint's
    // independent count. If the two ever disagree, the region this lint asserts
    // over is not the region the census scanner defines, and that is a bug in one
    // of them — the cheapest way to keep "one definition of the boundary" honest.
    assert_eq!(
        production_boundary(&lines),
        Some(test_starts[0]),
        "lint_scan::production_boundary and this lint's own scan disagree about where \
         production ends in worker/core.rs — two definitions of that boundary is how a \
         structural lint starts policing the wrong lines"
    );
    let region = production_region(&path);
    let code: Vec<(usize, &str)> = region.iter().map(|l| (l.line, l.text.as_str())).collect();
    // Vacuity floor for THIS scan: the production region is ~840 lines. A region
    // of a few dozen would mean the boundary resolved early — exactly what happens
    // in `harness/loop.rs`, where a `use` carries `#[cfg(test)]` at line 54 — and
    // every assert below would then be reading a stub.
    assert!(
        code.len() > 300,
        "this scan read only {} production code lines of worker/core.rs — vacuous",
        code.len()
    );

    let ctor_lines: Vec<usize> = code
        .iter()
        .filter(|(_, l)| l.contains("AgentLoop::new("))
        .map(|(n, _)| *n)
        .collect();
    assert_eq!(
        ctor_lines.len(),
        1,
        "expected exactly 1 production `AgentLoop::new(` in worker/core.rs (the \
         `default_build_loop` call site), found {ctor_lines:?}: a second one would be \
         an unwired loop this lint does not read",
    );
    // The seam is still a SEAM, and this reads `default_build_loop`'S OWN
    // signature — NOT the whole file. `WorkerCore::new` carries an
    // identically-typed parameter further down the same file, so a file-wide
    // `contains` was MEASURED VACUOUS on the desktop side: deleting
    // `default_build_loop`'s parameter and hard-coding a no-op at the
    // `AgentLoop::new(` site left that scan green. Hence the scoped region.
    let fn_start = code
        .iter()
        .find(|(_, l)| l.contains("pub fn default_build_loop("))
        .map(|(n, _)| *n)
        .unwrap_or_else(|| panic!("no `pub fn default_build_loop(` in {path:?}"));
    let fn_end = code
        .iter()
        .find(|(n, l)| *n > fn_start && l.contains("-> Box<dyn Fn(&StartEnv) -> LoopHandles"))
        .map(|(n, _)| *n)
        .unwrap_or_else(|| panic!("no `-> Box<dyn Fn(&StartEnv) -> LoopHandles` after {fn_start}"));
    let signature: Vec<&str> = code
        .iter()
        .filter(|(n, _)| *n > fn_start && *n <= fn_end)
        .map(|(_, l)| l.trim())
        .collect();
    assert!(
        signature
            .iter()
            .any(|l| l.starts_with("open_requester: Arc<dyn crate::mcp::OpenRequester>")),
        "`default_build_loop` ({fn_start}..{fn_end}) no longer takes the OpenRequester \
         as a parameter — the opener can only ever ARRIVE here, so a version that \
         builds one inside the crate is the boundary crossing \
         `no_path_reaches_a_desktop_module` exists to catch",
    );
    // And the production `AgentLoop::new(` must pass THAT parameter, not a no-op
    // it built itself.
    let ctor = ctor_lines[0];
    let ctor_end = code
        .iter()
        .find(|(n, l)| *n > ctor && l.trim() == ");")
        .map(|(n, _)| *n)
        .unwrap_or_else(|| panic!("no closing `);` found for AgentLoop::new at {ctor}"));
    let passed: Vec<usize> = code
        .iter()
        .filter(|(n, l)| *n > ctor && *n < ctor_end && l.trim() == "open_requester.clone(),")
        .map(|(n, _)| *n)
        .collect();
    assert_eq!(
        passed.len(),
        1,
        "the production `AgentLoop::new(` at {path:?}:{ctor} does not pass the \
         threaded `open_requester` exactly once (found {passed:?}) — a loop built on a \
         no-op opener means the MCP `auth` action silently never opens a browser and \
         its OAuth flow dies at the callback timeout",
    );
    // The crate may neither reach the desktop's opener nor hard-code the no-op in
    // PRODUCTION (the test region below does, on purpose — that is the stub's
    // contract, and the split above is what keeps it out of the count). The opener
    // needle names a forbidden path root, so it is assembled: this file is inside
    // the boundary scan's walk and that root is one of its three.
    let opener_root = needle('\u{63}', "rate::open::");
    let offender = code
        .iter()
        .find(|(_, l)| l.contains(opener_root.as_str()) || l.contains("mcp::NoOpenRequester"))
        .map(|(n, l)| (*n, l.trim().to_string()));
    assert!(
        offender.is_none(),
        "worker/core.rs constructs the opener in PRODUCTION at {offender:?} — the \
         desktop's opener cannot live here, and a production no-op is exactly the \
         silent failure this pin exists to catch; the opener must arrive through \
         `default_build_loop`'s parameter",
    );
    // And the no-op opener must be a TEST-ONLY choice in this file, so the split
    // above is not quietly crediting the stub. Reads BELOW the boundary — the
    // other half of the same claim.
    let test_region = &lines[test_starts[0]..];
    assert!(
        test_region.iter().any(|l| l.contains("NoOpenRequester")),
        "expected the test region of worker/core.rs to inject the crate's no-op opener \
         (the stub's contract) — if that changed, re-check which region this lint's \
         production half is reading",
    );
}

/// The boundary half: `harness/loop.rs` must not CONSTRUCT the desktop's opener,
/// because this crate cannot name a UI-framework module. Checked as an ABSENCE in
/// code plus a PRESENCE in the signature, so a half-done revert (take the
/// parameter, keep constructing) is red rather than green.
///
/// Moved whole from the desktop's `open.rs` in S8b.4. Region: the WHOLE FILE with
/// comment lines dropped — see
/// [`every_crate_mcp_manager_carries_the_open_requester`] for the measured reason.
#[test]
fn the_harness_takes_the_opener_instead_of_constructing_it() {
    let code = code_lines(&crate_src("harness/loop.rs"));
    let joined = code
        .iter()
        .map(|(_, l)| l.as_str())
        .collect::<Vec<_>>()
        .join("\n");
    // (a) The ABSENCE: no code in this module reaches the desktop's opener.
    let opener_root = needle('\u{63}', "rate::open::");
    let offender: Option<String> = code
        .iter()
        .find(|(_, l)| l.contains(opener_root.as_str()))
        .map(|(n, l)| format!("{n}: {}", l.trim()));
    assert!(
        offender.is_none(),
        "harness/loop.rs constructs the opener itself at {:?} — the desktop's opener \
         cannot move into this crate, so it must arrive through `AgentLoop::new`'s \
         parameter and the `open_requester` field.",
        offender.unwrap_or_default()
    );
    // (b) The PRESENCE: the constructor TAKES it, and the rebuild reads it back
    // off the field (the swap that S3's pin exists to catch).
    assert!(
        code.iter()
            .any(|(_, l)| l.contains("open_requester: Arc<dyn OpenRequester>")),
        "`AgentLoop::new` no longer takes the OpenRequester as a parameter — the seam \
         was wired from the desktop side and correcting it put the parameter here; \
         reverting to construction inside the loop is the boundary crossing this pin \
         exists to catch",
    );
    assert!(
        joined.contains(".with_open_requester(self.open_requester.clone())"),
        "the `set_desktop_config` rebuild no longer re-wires the SAME injected opener \
         — replacing the manager with a fresh default silently drops the opener from \
         the loop that actually runs with a packaged `mcpServers` map",
    );
}

/// The fail-open proof for the two assembled needles, in the shape
/// `boundary_pins` established: a needle that matches nothing is
/// indistinguishable from clean code, and that is exactly how a source scan ends
/// up reporting green over a violation. So: assert each assembled spelling IS the
/// real construct, and assert the scan's own filter FIRES on planted code that
/// carries it.
#[test]
fn the_assembled_needles_are_the_constructs_they_stand_for() {
    let spawn = needle('\u{43}', "ommand::new(");
    let opener = needle('\u{63}', "rate::open::");
    assert_eq!(
        spawn,
        ['C', 'o', 'm', 'm', 'a', 'n', 'd', ':', ':', 'n', 'e', 'w', '(']
            .iter()
            .collect::<String>(),
        "the assembled spawn needle is not the real constructor — the gcloud scan is \
         matching nothing"
    );
    assert_eq!(
        opener,
        ['c', 'r', 'a', 't', 'e', ':', ':', 'o', 'p', 'e', 'n', ':', ':']
            .iter()
            .collect::<String>(),
        "the assembled opener needle is not the real path — the opener scans are \
         matching nothing"
    );
    // And the filters FIRE. Both gcloud censuses share [`needles_hit`], so one
    // planted line proves the shape for both; a needle that cannot prick is caught
    // here instead of being trusted.
    let spawn_needles: [&str; 3] = [spawn.as_str(), "process::Command", "tokio::process"];
    let planted = SourceLine {
        line: 7,
        text: format!("    let mut cmd = {spawn}\"gcloud\");"),
    };
    assert!(
        needles_hit(&planted, &spawn_needles),
        "the spawn filter did not fire on planted code — both gcloud censuses are vacuous"
    );
    let planted_clean = SourceLine {
        line: 8,
        text: "    let client = reqwest::Client::builder().build();".to_string(),
    };
    assert!(
        !needles_hit(&planted_clean, &spawn_needles),
        "the spawn filter fires on a reqwest builder — it would false-fail the real \
         gcloud file and get silenced"
    );
    let planted_opener = format!("    let _ = {opener}desktop_open_requester();");
    assert!(
        planted_opener.contains(opener.as_str()),
        "the opener needle does not match the call shape it stands for"
    );
}

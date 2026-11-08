//! The daemon's schema + migrations (T9 Task 4).
//!
//! # Where this DDL comes from, and how the copy is kept honest
//!
//! These four tables are the DESKTOP's. They were transcribed from
//! `archimedes-desktop/src-tauri/src/storage/db.rs:1565-1604` (the
//! `archimedes-desktop` path is the pre-rename name of what is now checked out as
//! `archimedes-codes/desktop`, whose file is `src-tauri/src/storage/db.rs`), column
//! for column, constraint for constraint. The daemon opens a DIFFERENT file
//! (`fulcrum.db`, not `archimedes.db`), so it is free of the desktop's migration
//! chain — but it is NOT free of its shape: the day this store's `sessions` row
//! stops being readable as the desktop's `SessionRow`, the "the daemon owns the same
//! disk the desktop owns" premise of ADR 0091 quietly stops being true. So the DDL
//! is copied, not re-derived.
//!
//! A one-time inspection of that transcription is worth nothing, and this module
//! used to claim it was worth something ("verified against that tree at
//! `4f9b99dc`"). It is not: a copy verified once at a commit nobody is building
//! drifts the day either repo edits a column, and nothing downstream notices. What
//! keeps the copy honest now is a **shape-pin contract**, and it is mechanical:
//!
//! - [`schema_shape_fingerprint`] serialises [`TABLE_SHAPES`] (plus the index) into
//!   the canonical text checked in at the repo root as `schema-sha.txt`. That file
//!   IS the fingerprint — no hash, so a mismatch names the drifted table instead of
//!   reporting two digests that differ.
//! - `the_pinned_schema_fingerprint_matches_the_shipped_file` in
//!   `fulcrum/tests/db.rs` diffs that file against the shapes this build pins, so a
//!   DDL change HERE reddens `cargo test -p fulcrum` in the same commit that made it.
//! - `scripts/check-schema-drift.sh` is the reusable comparator, run on every
//!   `main`/`feature/*` push and every PR by
//!   `.github/workflows/schema-drift.yml`, preceded by its own `--self-test` so a
//!   guard that can no longer fail reddens the run instead of printing a green nobody
//!   earned.
//! - The desktop runs the SAME comparator in its own CI against the fingerprint of
//!   the fulcrum rev it pins, so a DDL change THERE reddens the desktop.
//!
//! Both DDLs are byte-identical today, so this is a tripwire being installed, not a
//! fire being fought. See `README.md`'s *Schema-shape contract with the desktop* for
//! the update path.
//!
//! Two things here are deliberately load-bearing and must not be "simplified":
//!
//! - **The `UNIQUE` constraints.** `fulcrum_harness::persist::TranscriptPersister`
//!   is idempotent because `INSERT … ON CONFLICT` has something to conflict
//!   with. `UNIQUE(session_id, kind, message_key)` is the display upsert key and
//!   `UNIQUE(session_id, seq)` is the transcript upsert key. Drop either and a
//!   replayed frame silently becomes a duplicate row — and a store that stores
//!   the transcript twice is a corrupted context, not merely a bigger file.
//! - **`ON DELETE CASCADE`** on both transcript tables. The daemon's
//!   `delete_session` issues ONE `DELETE FROM sessions`; the cascade is what
//!   takes the rows with it. (`foreign_keys` is OFF by default in SQLite, so
//!   `Db::open` turns it on and asserts it took — see `super::Db::open`.)
//!
//! # What is NOT here
//!
//! The desktop's migration chain (the `agent_id` rip-out, the `archived` /
//! `context_usage_json` / `todos_json` / `cost_usd` / `trusted` column adds, the
//! `native_messages` FK rebuild). Every one of those exists because a user's
//! `archimedes.db` predates the column. `fulcrum.db` is born with them, so
//! porting the chain would be porting the bug history of a file this daemon does
//! not own.
//!
//! # The two repos' migration mechanisms are unrelated, and what that costs
//!
//! The desktop migrates with `ADD COLUMN` gates over `archimedes.db` and never
//! stamps `user_version`; the daemon migrates with the [`migrate`] slot over
//! `fulcrum.db` and refuses a file whose `user_version` is ahead. Byte-identical
//! DDL today, therefore, and nothing keeping it that way tomorrow. The mappers
//! in `db/mod.rs` (`session_row_from`, `message_row_from`) read columns BY
//! INDEX off the `Db::SESSION_COLUMNS` / `Db::MESSAGE_COLUMNS` SELECT lists, so
//! the two directions of drift are both bad and neither is loud:
//!
//! * the desktop **adds** a column → the daemon does not read it and says
//!   nothing;
//! * the desktop **removes or renames** one → every later index shifts, and the
//!   symptom is an `InvalidColumnType` that `persistence.rs` turns into
//!   `Ok(None)` — a session that quietly stopped existing.
//!
//! Documented-and-undetected is the shape that rots, so [`TABLE_SHAPES`] pins
//! each table's FULL column list (name + declared type, in order) and
//! `Db::verify_shapes` checks `PRAGMA table_info` against it at open, refusing
//! the file on any mismatch — added column included, because a fork is a fork
//! and the daemon must not silently ignore data it does not read.
//!
//! **The honest limit, and what replaced it:** that check runs against the
//! daemon's OWN file. It detects a `fulcrum.db` that does not match the build
//! reading it — a file written by a daemon built against a forked DDL, a restore
//! from the wrong file, a hand-edited table. It does NOT and cannot detect the
//! desktop forking `archimedes.db`, which this process never opens. Checking a file
//! against its own expectations was never going to be the answer, so the two owners
//! now agree on something else instead: not a version NUMBER, but an explicit,
//! reviewed **schema-shape contract**.
//!
//! - What it pins: per table, every column's name and declared type in declaration
//!   order, plus the `idx_messages_session` index statement. Order is part of it
//!   because the positional mappers above read by index.
//! - What it does NOT pin: the `NOT NULL` / `DEFAULT` / `PRIMARY KEY` flags (see
//!   [`ColumnShape`] for why), the two `UNIQUE` upsert keys and both `ON DELETE
//!   CASCADE`s (pinned by probes that ask the database, not the text:
//!   `Db::table_has_unique` and the cascade tests in `fulcrum/tests/db.rs`), the
//!   migration history, and the SELECT-list subsets — `Db::SESSION_COLUMNS` is 10 of
//!   `sessions`' 12 columns and the desktop's `SPACE_COLUMNS` is 4 of `spaces`' 5, so
//!   comparing those would FALSE-POSITIVE on `spaces.sort_order`.
//! - Where the daemon side is enforced: `cargo test -p fulcrum` (the pin-vs-shapes
//!   test) and `.github/workflows/schema-drift.yml` on every push and PR.
//! - Where the desktop side is enforced: the same `scripts/check-schema-drift.sh`
//!   comparator in the desktop's own CI, against the fingerprint of the fulcrum rev
//!   that repo pins.
//!
//! And the degraded state is stated here because it is the state that ships today:
//! the desktop pins fulcrum at `78e595436ac5612d8ef87f692bee61f48f6e58e7`, a rev
//! whose tree carries no `fulcrum` member and therefore no `schema-sha.txt` at all.
//! So the desktop half of the comparison cannot pass yet, and it must not pretend
//! to — the comparator REFUSES with a named reason (`exit 2`: the pinned rev
//! publishes no fingerprint) rather than passing because it found nothing to
//! compare. A check that reports green on an empty comparison is worse than no
//! check, because it launders the absence of a contract into the appearance of one.
//! Until the desktop both publishes its own fingerprint and re-pins fulcrum to a rev
//! that has one, the cross-repo leg is red by design — and that red is the honest
//! answer.

use rusqlite::Connection;

use super::DbError;

/// The schema version this build writes, stamped into `PRAGMA user_version`.
///
/// `1` = "the four tables, as [`SCHEMA`] spells them". A fresh database is
/// migrated to this; a database already at this version is left alone; a
/// database at a HIGHER version is refused (see [`migrate`]).
pub const SCHEMA_VERSION: i64 = 1;

/// The four tables, verbatim from the desktop (see the module docs).
///
/// `IF NOT EXISTS` throughout, so [`SCHEMA`] is safe to hand to
/// `execute_batch` on a file that already has them — the property the migration
/// slot relies on.
pub const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS sessions (
    id TEXT PRIMARY KEY,
    cwd TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    title TEXT,
    capabilities_json TEXT NOT NULL,
    archived INTEGER NOT NULL DEFAULT 0,
    context_usage_json TEXT,
    is_subagent INTEGER NOT NULL DEFAULT 0,
    todos_json TEXT,
    cost_usd REAL,
    sort_order INTEGER NOT NULL DEFAULT 0,
    title_stage INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS messages (
    id INTEGER PRIMARY KEY,
    session_id TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
    kind TEXT NOT NULL,
    message_key TEXT,
    payload_json TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    UNIQUE(session_id, kind, message_key)
);
CREATE INDEX IF NOT EXISTS idx_messages_session ON messages(session_id, id);
CREATE TABLE IF NOT EXISTS native_messages (
    session_id TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
    seq INTEGER NOT NULL,
    role TEXT NOT NULL,
    content_json TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    UNIQUE(session_id, seq)
);
CREATE TABLE IF NOT EXISTS spaces (
    path TEXT PRIMARY KEY,
    created_at INTEGER NOT NULL,
    last_opened_at INTEGER NOT NULL,
    trusted INTEGER NOT NULL DEFAULT 0,
    sort_order INTEGER NOT NULL DEFAULT 0
);
"#;

/// One column of a pinned table shape: its name and its DECLARED type.
///
/// Only these two things are pinned, and the choice is deliberate:
///
/// * `notnull`, the `DEFAULT` expression, and the `PRIMARY KEY` flag are NOT
///   pinned, because the daemon's failure mode is not "a constraint weakened" —
///   it is "column 7 is not the column my mapper reads out of slot 7". The
///   constraints that actually matter here (both `UNIQUE` upsert keys, both
///   `ON DELETE CASCADE`s) are pinned by `Db::table_has_unique` and the cascade
///   tests in `fulcrum/tests/db.rs`, which ask the database rather than the DDL text.
/// * The declared TYPE is pinned even though the mappers do not name it, because
///   SQLite's dynamic typing means a `TEXT` `cost_usd` does not fail loudly — it
///   coerces, and the daemon stores a value class it did not ask for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ColumnShape {
    /// The column's name, exactly as the desktop declares it.
    pub name: &'static str,
    /// The declared type, as `PRAGMA table_info` reports it (`TEXT`, `INTEGER`,
    /// `REAL`). Compared case-insensitively.
    pub decl_type: &'static str,
}

/// A pinned table: its name and its FULL column list, in declaration order.
#[derive(Debug, Clone, Copy)]
pub struct TableShape {
    /// The table's name.
    pub name: &'static str,
    /// EVERY column the table has, in the order the DDL declares them — the
    /// order `PRAGMA table_info` reports and the order the positional mappers
    /// assume.
    pub columns: &'static [ColumnShape],
}

/// The shape constant, ONE per table, and why it is separate from the SELECT list.
///
/// `Db::SESSION_COLUMNS` is a deliberate SUBSET of this table — 10 of `sessions`'
/// 12 columns, because `SessionRow` has no field for `todos_json` or
/// `sort_order`. That is why the check cannot assert "the SELECT list equals the
/// table": it would reject a perfectly good database, which is worse than the bug
/// it was written to catch. So the daemon pins TWO things, and they answer two
/// different questions:
///
/// * `Db::SESSION_COLUMNS` (private) — "what do we read, and
///   in what order?" Its arity tie lives in `db/mod.rs`.
/// * [`SESSION_TABLE`] here — "what does this table LOOK like?" The full shape,
///   including the columns nobody reads.
///
/// The full shape is the one that gets verified, and it refuses a fork in both
/// directions: an added column is refused as surely as a removed one. "The
/// desktop added a column we do not read" is a fork, not a shrug — the daemon
/// would go on writing rows into a table whose owner has moved on, and from the
/// desktop's side "the daemon ignored the column" and "the daemon lost the data"
/// are the same file.
pub const SESSION_TABLE: [ColumnShape; 12] = [
    ColumnShape {
        name: "id",
        decl_type: "TEXT",
    },
    ColumnShape {
        name: "cwd",
        decl_type: "TEXT",
    },
    ColumnShape {
        name: "created_at",
        decl_type: "INTEGER",
    },
    ColumnShape {
        name: "title",
        decl_type: "TEXT",
    },
    ColumnShape {
        name: "capabilities_json",
        decl_type: "TEXT",
    },
    ColumnShape {
        name: "archived",
        decl_type: "INTEGER",
    },
    ColumnShape {
        name: "context_usage_json",
        decl_type: "TEXT",
    },
    ColumnShape {
        name: "is_subagent",
        decl_type: "INTEGER",
    },
    ColumnShape {
        name: "todos_json",
        decl_type: "TEXT",
    },
    ColumnShape {
        name: "cost_usd",
        decl_type: "REAL",
    },
    ColumnShape {
        name: "sort_order",
        decl_type: "INTEGER",
    },
    ColumnShape {
        name: "title_stage",
        decl_type: "INTEGER",
    },
];

/// `messages` — the display rows. All six columns are read;
/// `Db::MESSAGE_COLUMNS` (private) selects them.
pub const MESSAGES_TABLE: [ColumnShape; 6] = [
    ColumnShape {
        name: "id",
        decl_type: "INTEGER",
    },
    ColumnShape {
        name: "session_id",
        decl_type: "TEXT",
    },
    ColumnShape {
        name: "kind",
        decl_type: "TEXT",
    },
    ColumnShape {
        name: "message_key",
        decl_type: "TEXT",
    },
    ColumnShape {
        name: "payload_json",
        decl_type: "TEXT",
    },
    ColumnShape {
        name: "created_at",
        decl_type: "INTEGER",
    },
];

/// `native_messages` — the provider transcript, read by name (`content_json`),
/// not positionally. Still verified: "loud at the first query" is a worse failure
/// than "refused at open".
pub const NATIVE_MESSAGES_TABLE: [ColumnShape; 5] = [
    ColumnShape {
        name: "session_id",
        decl_type: "TEXT",
    },
    ColumnShape {
        name: "seq",
        decl_type: "INTEGER",
    },
    ColumnShape {
        name: "role",
        decl_type: "TEXT",
    },
    ColumnShape {
        name: "content_json",
        decl_type: "TEXT",
    },
    ColumnShape {
        name: "created_at",
        decl_type: "INTEGER",
    },
];

/// `spaces` — the workspace rows (ADR 0010).
pub const SPACES_TABLE: [ColumnShape; 5] = [
    ColumnShape {
        name: "path",
        decl_type: "TEXT",
    },
    ColumnShape {
        name: "created_at",
        decl_type: "INTEGER",
    },
    ColumnShape {
        name: "last_opened_at",
        decl_type: "INTEGER",
    },
    ColumnShape {
        name: "trusted",
        decl_type: "INTEGER",
    },
    ColumnShape {
        name: "sort_order",
        decl_type: "INTEGER",
    },
];

/// Every table whose shape `Db::verify_shapes` (private, in `super`) holds this
/// build to.
///
/// ONE list, so a fifth table in the DDL cannot quietly escape the check: adding
/// a table to [`SCHEMA`] without adding its shape here is caught by
/// `the_shape_list_covers_every_table_in_the_schema` rather than by the next
/// person's production outage.
pub const TABLE_SHAPES: [TableShape; 4] = [
    TableShape {
        name: "sessions",
        columns: &SESSION_TABLE,
    },
    TableShape {
        name: "messages",
        columns: &MESSAGES_TABLE,
    },
    TableShape {
        name: "native_messages",
        columns: &NATIVE_MESSAGES_TABLE,
    },
    TableShape {
        name: "spaces",
        columns: &SPACES_TABLE,
    },
];

/// Serialise [`TABLE_SHAPES`] — plus the index — into the schema-shape pin sections.
///
/// One section per table: the table's name on its own line, then a `name:type` line
/// per column in declaration order; then a final `idx_messages_session` section
/// carrying the trimmed `CREATE INDEX` statement. The text is produced directly
/// from [`TABLE_SHAPES`] (the single source of truth; [`ColumnShape`] pins exactly
/// name + declared type), parsed from [`SCHEMA`] only for the index line, which the
/// shape constants do not carry. This is the serialisation of the same fact
/// `the_pinned_shapes_match_the_ddl_they_claim_to_describe` proves against `SCHEMA`,
/// not a second representation.
pub(crate) fn schema_shape_sections() -> Vec<(String, Vec<String>)> {
    let mut out: Vec<(String, Vec<String>)> = Vec::new();
    for shape in TABLE_SHAPES {
        out.push((
            shape.name.to_string(),
            shape
                .columns
                .iter()
                .map(|c| format!("{}:{}", c.name, c.decl_type))
                .collect(),
        ));
    }
    let index = SCHEMA
        .lines()
        .map(str::trim)
        .find(|line| line.starts_with("CREATE INDEX "))
        .expect("SCHEMA declares the idx_messages_session index");
    out.push(("idx_messages_session".to_string(), vec![index.to_string()]));
    out
}

/// The full pin text: `schema_shape_sections()`, one line per entry, each line
/// `\n`-terminated. Byte-identical to `schema-sha.txt` with comment lines and blank
/// lines stripped (the integration test
/// `the_pinned_schema_fingerprint_matches_the_shipped_file` in `fulcrum/tests/db.rs`
/// asserts exactly that).
///
/// The link above is deliberately NOT an intra-doc link: `schema_shape_sections` is
/// `pub(crate)` — nothing outside this module calls it — and `rustdoc` warns on a
/// public item linking to a private one.
pub fn schema_shape_fingerprint() -> String {
    let mut out = String::new();
    for (name, lines) in schema_shape_sections() {
        out.push_str(&name);
        out.push('\n');
        for line in lines {
            out.push_str(&line);
            out.push('\n');
        }
    }
    out
}

/// Bring a database from `from` (its stored `user_version`) to
/// [`SCHEMA_VERSION`], and report the version it reached.
///
/// ONE slot, and the shape is deliberately small: slot 1 builds the four tables
/// from [`SCHEMA`] and stamps the version. Each future slot is one more `if`
/// branch here — a step that runs only when the file is at the version below it,
/// in a transaction of its own, and never renumbering anything the user placed.
///
/// Two properties worth naming, because both are the reason a slot exists at all:
///
/// - **Every step is idempotent by construction.** The version is read ONCE
///   before the loop and written once per step, so a crash between a step's
///   commit and its stamp re-runs a step that is safe to re-run
///   (`IF NOT EXISTS` / a guarded `ALTER`), rather than skipping one that was
///   half-applied.
/// - **A NEWER file is refused, not opened.** `from > SCHEMA_VERSION` is an
///   error, not a shrug. A daemon built against an older schema that opens a
///   database a newer daemon already migrated would write rows the newer one
///   cannot read; "I refuse to open this" is the only honest answer, and it is
///   cheap. Without the version slot, that failure is silent corruption.
///
/// SQLite DDL is transactional, so each step's DDL + stamp commit together.
pub fn migrate(conn: &mut Connection, from: i64) -> Result<i64, DbError> {
    if from > SCHEMA_VERSION {
        return Err(DbError::SchemaTooNew {
            found: from,
            supported: SCHEMA_VERSION,
        });
    }

    if from < 1 {
        // Slot 1: the four tables, and nothing else. One transaction — a crash
        // cannot leave a table half-created and stamped as migrated.
        let tx = conn.transaction()?;
        tx.execute_batch(SCHEMA)?;
        tx.pragma_update(None, "user_version", SCHEMA_VERSION)?;
        tx.commit()?;
    }

    Ok(SCHEMA_VERSION)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The DDL text is the desktop's, so its shape is asserted here rather than
    /// eyeballed: four tables, the index, both upsert keys, both cascades.
    #[test]
    fn schema_text_holds_the_desktops_shape() {
        assert_eq!(SCHEMA.matches("CREATE TABLE").count(), 4);
        assert_eq!(SCHEMA.matches("ON DELETE CASCADE").count(), 2);
        assert!(SCHEMA.contains("UNIQUE(session_id, kind, message_key)"));
        assert!(SCHEMA.contains("UNIQUE(session_id, seq)"));
        assert!(SCHEMA.contains("CREATE INDEX IF NOT EXISTS idx_messages_session"));
        // `cost_usd` has NO default on purpose: the desktop's docs are explicit
        // that a column default would collapse "no figure" into `Some(0.0)`,
        // which is the D5 defect ADR 0082 exists to avoid.
        assert!(SCHEMA.contains("cost_usd REAL,"));
        assert!(!SCHEMA.contains("cost_usd REAL NOT NULL"));
    }

    /// [`TABLE_SHAPES`] covers every table [`SCHEMA`] creates, and nothing else.
    ///
    /// The check in `Db::verify_shapes` loops over [`TABLE_SHAPES`], so a fifth
    /// table added to [`SCHEMA`] without a shape added here would simply be
    /// unchecked — the silent gap this pins shut. Parsing [`SCHEMA`] for the
    /// names is crude on purpose: it reads the same text `migrate` executes, so
    /// it cannot drift from what actually lands on disk.
    #[test]
    fn the_shape_list_covers_every_table_in_the_schema() {
        let mut declared: Vec<String> = SCHEMA
            .lines()
            .filter_map(|line| {
                let rest = line.trim().strip_prefix("CREATE TABLE IF NOT EXISTS ")?;
                Some(rest.split([' ', '(']).next()?.to_string())
            })
            .collect();
        let mut pinned: Vec<String> = TABLE_SHAPES
            .iter()
            .map(|shape| shape.name.to_string())
            .collect();
        declared.sort();
        pinned.sort();
        assert_eq!(
            declared, pinned,
            "`SCHEMA` and `TABLE_SHAPES` must name the same tables — a table with \
             no pinned shape is a table `Db::verify_shapes` never looks at"
        );
    }

    /// The pin's sections name every table and carry its FULL column list.
    ///
    /// `schema-sha.txt` is the contract the desktop compares against, so the
    /// shape of its sections is itself pinned: five sections, the exact arity of
    /// each, and every line equal to the `name:type` pair the `*_TABLE` consts
    /// declare. A section that silently lost a column would compare equal to a
    /// desktop that had lost the same one — the vacuity this refuses.
    #[test]
    fn the_shape_sections_name_each_table_and_its_full_column_list() {
        let sections = schema_shape_sections();
        let names: Vec<&str> = sections.iter().map(|(name, _)| name.as_str()).collect();
        assert_eq!(
            names,
            vec![
                "sessions",
                "messages",
                "native_messages",
                "spaces",
                "idx_messages_session",
            ],
            "the pin's sections must be the four tables in `TABLE_SHAPES` order \
             plus the index, and nothing else"
        );
        assert_eq!(
            sections
                .iter()
                .map(|(_, lines)| lines.len())
                .collect::<Vec<_>>(),
            vec![12, 6, 5, 5, 1],
            "each section's line count is the column count its shape constant pins \
             (the index section carries the one `CREATE INDEX` line)"
        );

        for (shape, (name, lines)) in TABLE_SHAPES.iter().zip(sections.iter()) {
            let want: Vec<String> = shape
                .columns
                .iter()
                .map(|c| format!("{}:{}", c.name, c.decl_type))
                .collect();
            assert_eq!(
                *lines, want,
                "`{name}`: the pin's sections must be `TABLE_SHAPES` serialised — \
                 name + declared type, in declaration order"
            );
            assert_eq!(name, shape.name);
        }
    }

    /// The index section is the shipped index statement, trimmed.
    ///
    /// The index is in the contract because `idx_messages_session` is the only
    /// thing in the DDL the shape constants do not carry, and a lost index is a
    /// full-table scan on the transcript read — drift the desktop would not see
    /// in its own column lists.
    #[test]
    fn the_index_section_is_the_shipped_index_line() {
        let sections = schema_shape_sections();
        let index = sections
            .iter()
            .find(|(name, _)| name == "idx_messages_session")
            .expect("the pin carries an idx_messages_session section");
        assert_eq!(
            index.1,
            vec![
                "CREATE INDEX IF NOT EXISTS idx_messages_session ON messages(session_id, id);"
                    .to_string()
            ],
            "the index section must be the exact trimmed `CREATE INDEX` line from \
             `SCHEMA` — the desktop compares this text verbatim"
        );
    }

    /// The shape constants agree with the DDL, column for column, in order.
    ///
    /// This is the check that keeps `TABLE_SHAPES` from being a hopeful list. It
    /// parses the column lines out of [`SCHEMA`] — the text `migrate` actually
    /// runs — and compares name and declared type, position by position. Without
    /// it, a wrong shape constant would refuse good databases (or accept bad
    /// ones) and the only evidence would be a production daemon that will not
    /// start.
    #[test]
    fn the_pinned_shapes_match_the_ddl_they_claim_to_describe() {
        for shape in TABLE_SHAPES {
            let ddl = SCHEMA
                .split("CREATE TABLE IF NOT EXISTS ")
                .find(|chunk| chunk.starts_with(shape.name))
                .unwrap_or_else(|| panic!("`SCHEMA` does not create `{}`", shape.name));
            let body = &ddl[..ddl.find(");").expect("a closing paren")];
            let declared: Vec<(String, String)> = body
                // Drop the `name (` prefix of the CREATE line itself.
                .split_once('(')
                .expect("an opening paren")
                .1
                .lines()
                .map(|line| line.trim().trim_end_matches(',').to_string())
                .filter(|line| !line.is_empty())
                // Table-level constraints are not columns.
                .filter(|line| !line.starts_with("UNIQUE") && !line.starts_with("PRIMARY KEY"))
                .map(|line| {
                    let mut parts = line.splitn(2, [' ']);
                    let name = parts.next().unwrap_or("").to_string();
                    // `id TEXT PRIMARY KEY` → the type is the second token; a
                    // column declared with no type has none at all.
                    let ty = parts
                        .next()
                        .unwrap_or("")
                        .split(' ')
                        .next()
                        .unwrap_or("")
                        .to_string();
                    (name, ty)
                })
                .collect();

            assert_eq!(
                declared.len(),
                shape.columns.len(),
                "`{}`: `SCHEMA` declares {} columns, `TABLE_SHAPES` pins {}",
                shape.name,
                declared.len(),
                shape.columns.len(),
            );
            for (at, (name, ty)) in declared.into_iter().enumerate() {
                let want = &shape.columns[at];
                assert_eq!(
                    name, want.name,
                    "`{}` position {at}: `SCHEMA` declares `{name}`, the pinned \
                     shape names `{}` — the mappers read by index, so this \
                     ordering is not cosmetic",
                    shape.name, want.name,
                );
                assert_eq!(
                    ty, want.decl_type,
                    "`{}`.{}: `SCHEMA` declares `{ty}`, the pinned shape says `{}`",
                    shape.name, want.name, want.decl_type,
                );
            }
        }
    }
}

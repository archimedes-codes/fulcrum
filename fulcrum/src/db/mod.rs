//! The daemon's own store (T9 Task 4): the connection, the pragmas, the
//! migrations, the SQL.
//!
//! `rusqlite` is legal in this member and forbidden in the engine — ADR 0091's
//! title is "the desktop owning the disk", and the daemon is the thing that takes
//! the disk. `boundary_pins` walks `{CARGO_MANIFEST_DIR}/src` and reads only
//! `{CARGO_MANIFEST_DIR}/Cargo.toml`, so it does not police this sibling at all;
//! that is intended for `rusqlite` and is Task 6's job to pin positively.
//!
//! ## Why a `std::sync::Mutex<Connection>` and not an async pool
//!
//! `rusqlite::Connection` is `!Sync`, so it cannot be shared across tasks as-is.
//! The desktop solved this the same way (`storage/db.rs:305-311`) and for the
//! same reason: the calls that reach the store — `fulcrum_harness::persist`'s
//! `apply` / `apply_interactive_event`, and every method on
//! [`fulcrum_harness::FulcrumPersistence`] — are SYNCHRONOUS. Wrapping the pool in
//! `tokio-rusqlite` would buy a `.await` on every one of them and force the
//! persister's callers to be async for no behaviour change, so this holds the
//! connection in a plain mutex and blocks, exactly as the desktop does.
//!
//! The consequence to keep in mind (Task 5 owns it, not this file): a task that
//! holds this lock across an `.await` would stall the daemon's writer. Every
//! method below takes the lock, does ONE statement (or one transaction), and
//! releases it — never hold the guard past the SQL.
//!
//! ## The three pragmas, and why the order is a rule
//!
//! `journal_mode=WAL` → `synchronous=NORMAL` → `foreign_keys=ON`, and `journal_mode`
//! FIRST because it is the only one of the three that can silently fail to apply
//! (see [`Db::open`]). `foreign_keys` is per-CONNECTION in SQLite and defaults to
//! OFF, so it must be set on every open, not once at creation.
//!
//! ## A fourth step at open: the shape check
//!
//! After the pragmas and AFTER [`schema::migrate`], `Db::verify_shapes` (private) reads
//! `PRAGMA table_info` for all four tables and refuses a file whose columns do not
//! match [`schema::TABLE_SHAPES`] — a missing, renamed, reordered, retyped, or
//! unexpected-extra column all stop the open rather than surfacing later as a
//! positional read that returns the wrong value. Why the expectation is the full
//! table shape and not the SELECT lists, and what this check therefore does NOT
//! cover (the desktop's own file), is spelled out on
//! [`schema::TABLE_SHAPES`] and in `schema`'s module docs.

use std::path::Path;
use std::sync::Mutex as StdMutex;

use rusqlite::{params, Connection};

pub mod persistence;
pub mod schema;

pub use schema::SCHEMA_VERSION;

/// The daemon's store error.
///
/// Three variants that mirror the desktop's `DbError` (`storage/db.rs:49-56`)
/// plus one the desktop has no need for: it opens a file it did not create and
/// can migrate forward, whereas `fulcrum.db` can be opened by a daemon built
/// BEFORE the file's schema version (downgrade, or two versions on one dir).
#[derive(Debug, thiserror::Error)]
pub enum DbError {
    /// SQLite itself.
    #[error("sqlite error: {0}")]
    Sqlite(#[from] rusqlite::Error),
    /// The filesystem under the data dir.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    /// A JSON envelope that will not parse. A corrupt row is an ERROR, never a
    /// silently-empty load.
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
    /// `journal_mode` was set to WAL and did not come back as `wal`.
    ///
    /// This is the failure the task exists to catch. Some network filesystems
    /// refuse WAL and leave the database in `delete` mode WITHOUT ERRORING — so
    /// `pragma_update` succeeds, everything looks fine, and the daemon's
    /// durability story ("concurrent readers, crash-safe append-only log") is
    /// simply false. Refusing to start is the only honest response: a supervisor
    /// that will not boot is recoverable, a supervisor that lies about its
    /// journal is not.
    #[error(
        "could not enter WAL mode: `PRAGMA journal_mode` reports `{0}` (a filesystem that \
             cannot do WAL leaves `delete` without erroring)"
    )]
    NotWal(String),
    /// The file's `user_version` is ahead of what this build knows
    /// (see [`schema::migrate`]).
    #[error("database schema version {found} is newer than this build supports ({supported})")]
    SchemaTooNew {
        /// What the file says.
        found: i64,
        /// What this build can migrate to.
        supported: i64,
    },
    /// A table in the file does not have the shape this build reads (finding 10;
    /// see [`schema::TABLE_SHAPES`]).
    ///
    /// This is the "the two repos' schemas forked" error, and it exists so the
    /// operator can tell the two causes apart from the message alone:
    ///
    /// * a column this build expects is **missing** → the file PREDATES this
    ///   build (or is corrupt);
    /// * a column the file has is **unknown** → the file comes from a NEWER
    ///   schema than this build.
    ///
    /// Both refuse the open. `user_version` cannot carry that distinction,
    /// because the desktop does not stamp it — its migrations are `ADD COLUMN`
    /// gates over `archimedes.db` and never touch this counter — so the column
    /// list is the only evidence available, and it is read at the one moment
    /// where refusing costs nothing.
    #[error("schema mismatch in table `{table}`: {detail}")]
    SchemaFork {
        /// The table whose `PRAGMA table_info` did not match.
        table: String,
        /// What was expected, what was found, and where — a full sentence, built
        /// by `Self::verify_shapes` rather than a bare kind tag,
        /// because the three failure shapes need three different diagnoses and
        /// the operator reading a log line has to pick one.
        detail: String,
    },
}

/// The daemon's SQLite database — the sole writer of `fulcrum.db`.
///
/// Shared as `Arc<Db>`; it coerces to `Arc<dyn FulcrumPersistence>` for the
/// engine's persister (see [`persistence`]).
pub struct Db {
    conn: StdMutex<Connection>,
}

/// Make `path` itself 0600, creating it if it is not there yet.
///
/// Why this exists at all: `fulcrum.db` holds `native_messages.content_json` —
/// the FULL conversation transcript of every session the daemon has run — plus
/// `sessions.cwd` and `sessions.cost_usd`. `rusqlite`'s `Connection::open` hands
/// the create to the C library, which applies the process umask, so the default
/// result is 0644: world-readable. Deferring that to T13's authentication is a
/// category error — a file mode is not a credential check, it is the layer that
/// decides whether another user's process can `open()` the file at all.
///
/// Two details that are not incidental:
///
/// * It CREATES the file before handing the path to rusqlite. `Connection::open`
///   would otherwise be the creator and would apply the umask; creating first is
///   the only way to own the mode without racing the creation.
/// * It NARROWS an existing file. A fix that only acted on creation would leave
///   every installation that already has real transcripts world-readable, which
///   is the opposite of the population worth protecting.
///
/// `--db` lets an operator point this at a path that already exists with a mode
/// they chose, and this call overrides it. That is deliberate: the failure mode
/// of NOT overriding is a silently world-readable transcript, and the failure
/// mode of overriding is an operator noticing their mode changed and reading this
/// comment.
#[cfg(unix)]
fn make_private(path: &Path) -> Result<(), DbError> {
    use std::os::unix::fs::{OpenOptionsExt as _, PermissionsExt as _};
    if !path.exists() {
        // `create(true)` + `mode(0o600)`: the mode is applied AT creation, so
        // there is no window where the file exists and is readable. Note that
        // `OpenOptions::mode` is still masked by umask on some platforms, which
        // is why the explicit `set_permissions` below is not redundant — 0600 is
        // already the tightest umask can make it, but the belt costs nothing and
        // the guarantee must not depend on the environment.
        std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            // NOT `truncate`: this is a database. `create(true)` alone leaves an
            // existing file's bytes alone, which is the only behaviour that can
            // be correct here — truncating would destroy every transcript in the
            // store before rusqlite ever opened it. Stated because `create`
            // without an explicit `truncate` is a clippy lint precisely because
            // the omission is usually an accident.
            .truncate(false)
            .mode(0o600)
            .open(path)?;
    }
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
    Ok(())
}

/// Create `path` and every component this call had to create, each at 0700.
///
/// THE ONE COPY in this crate, and `pub(crate)` for exactly that reason. This
/// function used to exist twice, byte-for-byte — here and in
/// `transport::unix::ensure_private_dir`, with a comment apologising that there
/// was "no natural common home". There is: both modules are in this crate, `db`
/// is declared by `lib.rs`, and `transport::unix` can name `crate::db` without
/// any change to `lib.rs`. Two copies of a security-relevant function is how one
/// gets fixed and the other gets forgotten, and the property they both carry —
/// "only the components I created get narrowed" — is the one protecting
/// `$HOME` from a daemon started with `--db /home/me/fulcrum.db`.
///
/// # Error type
///
/// `std::io::Error`, because that is what this function actually does: filesystem
/// syscalls, nothing SQLite. Each caller keeps ITS OWN error type by letting `?`
/// convert — `db::DbError::Io` has a `#[from] std::io::Error`, and
/// `transport::TransportError::Io` likewise — so sharing the body did not force
/// either module to adopt the other's error vocabulary. That is the reason the
/// shared function is not `fn … -> Result<(), DbError>`: it would have made the
/// transport depend on a store error type for a `chmod`.
///
/// The DIRECTORY is the layer that actually protects the WAL siblings. SQLite
/// creates `fulcrum.db-wal` and `fulcrum.db-shm` itself, with umask-derived modes
/// (measured: 0644), and rotates them — so tightening only the `.db` file would
/// leave the freshest, most complete transcript world-readable in the WAL. The
/// daemon cannot fix that from the outside: it does not create those files and
/// cannot know when SQLite will recreate them. A 0700 directory does bound them,
/// because opening a file requires the search bit on every directory above it.
///
/// Only directories THIS call creates are narrowed. `chmod`ing one that already
/// existed would let `--db /home/me/fulcrum.db` quietly set `$HOME` to 0700 —
/// arguably the right mode for `$HOME`, but not this daemon's decision to make,
/// and it would break any other program relying on traversal. So the components
/// that are missing are recorded BEFORE the create and only those are narrowed
/// afterwards; asking afterwards is not possible, since `create_dir_all` reports
/// success for components it never touched and leaves no trace of which they were.
#[cfg(unix)]
pub(crate) fn ensure_private_dir(path: &Path) -> std::io::Result<()> {
    use std::os::unix::fs::PermissionsExt as _;
    // The deepest component that already exists: everything strictly below it is
    // ours to create and therefore ours to set. `ancestors()` runs leaf-first, so
    // the first hit is the deepest.
    let existing = path
        .ancestors()
        .find(|candidate| candidate.exists())
        .map(|found| found.to_path_buf());
    std::fs::create_dir_all(path)?;
    // Narrow every component from `path` down to (but not including) the one that
    // was already there. If the whole chain was missing — impossible for a real
    // filesystem root, but `path` could be relative — the walk simply stops at
    // the root and `$HOME` is never reached, because a component above the
    // deepest existing one does not exist to be chmodded.
    let mut cursor = Some(path);
    while let Some(current) = cursor {
        if Some(current) == existing.as_deref() {
            break;
        }
        std::fs::set_permissions(current, std::fs::Permissions::from_mode(0o700))?;
        cursor = current.parent();
    }
    Ok(())
}

/// The non-unix leg: nothing to set, and the reason is stated rather than hidden.
///
/// Windows carries this guarantee in the file's ACL, and the daemon does not
/// touch ACLs — the named-pipe side has the same limitation and says so in
/// `transport::windows`. Both are called out in the module docs and in `README`
/// so the claim is never stronger than the code.
#[cfg(not(unix))]
fn make_private(_path: &Path) -> Result<(), DbError> {
    Ok(())
}

/// The non-unix leg of [`ensure_private_dir`]: create, and nothing to set.
///
/// Windows carries this guarantee in the directory's ACL, and the daemon does not
/// touch ACLs — the same limitation the named-pipe side documents in
/// `transport::windows`, and the reason the shared function returns `io::Result`
/// rather than a unix-flavoured permission error: there is no mode to fail at
/// setting here.
#[cfg(not(unix))]
pub(crate) fn ensure_private_dir(path: &Path) -> std::io::Result<()> {
    if !path.as_os_str().is_empty() && !path.exists() {
        std::fs::create_dir_all(path)?;
    }
    Ok(())
}

impl Db {
    /// Open (creating if needed) the database at `path`, then migrate it.
    /// Parent directories are created as needed.
    ///
    /// The pragma order is load-bearing: `journal_mode` before `foreign_keys`,
    /// and `journal_mode` is READ BACK. See [`DbError::NotWal`] for why the read
    /// back is not paranoia.
    ///
    /// The FILE is owner-only (0600) and every directory this call creates is
    /// 0700 — see [`make_private`] for why that is a correctness requirement and
    /// not hygiene, and for the reason a pre-existing directory is left alone.
    pub fn open(path: &Path) -> Result<Self, DbError> {
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                ensure_private_dir(parent)?;
            }
        }
        make_private(path)?;
        let mut conn = Connection::open(path)?;

        // 1. WAL, FIRST, and then verified. `journal_mode` is persistent (it
        //    lives in the database header), so this is a no-op on a file that is
        //    already WAL — but it is the ONLY pragma here that can be silently
        //    refused, so its refusal has to be fatal rather than assumed away.
        conn.pragma_update(None, "journal_mode", "WAL")?;
        let mode = conn.query_row("PRAGMA journal_mode", [], |row| row.get::<_, String>(0))?;
        if !mode.eq_ignore_ascii_case("wal") {
            return Err(DbError::NotWal(mode));
        }

        // 2. `NORMAL` rather than `FULL`: WAL already makes the commit atomic,
        //    so `NORMAL` only risks losing the last few transactions' worth of
        //    commits on an OS crash — not a torn database. That is the trade the
        //    daemon wants (a supervisor commits on every frame and a torn file
        //    is what `NORMAL` still prevents).
        conn.pragma_update(None, "synchronous", "NORMAL")?;

        // 3. AFTER journal_mode, and per-connection: SQLite defaults it to OFF,
        //    and without it the `ON DELETE CASCADE` in the schema is inert —
        //    `delete_session` would strand every transcript row on disk forever.
        conn.pragma_update(None, "foreign_keys", true)?;

        // The migration slot (see `schema::migrate`).
        let version = conn.query_row("PRAGMA user_version", [], |row| row.get::<_, i64>(0))?;
        schema::migrate(&mut conn, version)?;

        // The schema cross-check, AFTER `migrate` — order is load-bearing, not
        // style. Before the migration the four tables do not exist yet, so a
        // check placed here-but-earlier would refuse every fresh database.
        let db = Self {
            conn: StdMutex::new(conn),
        };
        db.verify_shapes()?;
        Ok(db)
    }

    /// Verify every table this build reads against its pinned shape (finding 10).
    ///
    /// # What this asserts, and what it deliberately does not
    ///
    /// It asserts the FULL declared shape of each of the four tables: the name
    /// AND the declared type of EVERY column, in declaration order. Not "the
    /// columns we read are present" — see [`schema::TABLE_SHAPES`] for why the
    /// SELECT lists cannot serve as the expectation (they are deliberate
    /// subsets), and why an ADDED column is refused rather than ignored.
    ///
    /// It does NOT pin `NOT NULL`, the `DEFAULT` expressions, or the constraint
    /// list. Those are pinned elsewhere, by probes that ask the DATABASE whether
    /// the property holds rather than whether the text claims it:
    /// [`Db::table_has_unique`] for both upsert keys, and
    /// `deleting_a_session_cascades_to_its_messages` for `ON DELETE CASCADE` (a
    /// cascade is only observable by deleting a row and watching what follows).
    ///
    /// # The honest scope of this check
    ///
    /// It detects a fork **inside the daemon's own file** — a `fulcrum.db` whose
    /// tables do not have the shape this build was compiled against. It CANNOT
    /// detect the desktop forking `archimedes.db`, because this daemon never
    /// opens that file. What it buys against that scenario is indirect: a
    /// `fulcrum.db` produced by a daemon built against a forked DDL, or one
    /// shaped by hand/`sqlx`/a restore from the wrong file, is refused here
    /// instead of mis-read. True cross-repo schema versioning — a shared,
    /// negotiated version between the desktop and the daemon — is a follow-up
    /// ticket, not this, and no amount of checking a file against its own
    /// reader's expectations substitutes for it.
    ///
    /// # Why it refuses rather than warns
    ///
    /// `session_row_from` and `message_row_from` read BY INDEX. A shifted column
    /// does not produce an error, it produces the wrong value: `archived` read
    /// out of `context_usage_json`'s slot is an `InvalidColumnType` that
    /// `persistence.rs` turns into `Ok(None)`, i.e. a session that silently
    /// vanished. Refusing to start is recoverable; a supervisor that serves
    /// mis-mapped rows is not.
    fn verify_shapes(&self) -> Result<(), DbError> {
        let guard = self.conn();
        for shape in schema::TABLE_SHAPES {
            // `PRAGMA table_info` reports `cid | name | type | notnull | dflt_value | pk`.
            // The `type` column is NULL for a column declared without one
            // (legal in SQLite), which `Option` here makes explicit rather than
            // turning into a `InvalidColumnType` three frames up the stack.
            let mut stmt = guard
                .prepare(&format!("PRAGMA table_info('{}')", shape.name))
                .map_err(|e| DbError::SchemaFork {
                    table: shape.name.to_string(),
                    detail: format!("cannot read `PRAGMA table_info`: {e}"),
                })?;
            let found = stmt
                .query_map([], |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, Option<String>>(2)?,
                    ))
                })
                .map_err(|e| DbError::SchemaFork {
                    table: shape.name.to_string(),
                    detail: format!("cannot read `PRAGMA table_info`: {e}"),
                })?
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| DbError::SchemaFork {
                    table: shape.name.to_string(),
                    detail: format!("cannot read `PRAGMA table_info`: {e}"),
                })?;
            drop(stmt);

            // A MISSING TABLE reports zero rows, and zero rows would otherwise
            // compare equal to "nothing to disagree about" — the naive read that
            // lets a daemon claim health and then fail its first write.
            if found.is_empty() {
                return Err(DbError::SchemaFork {
                    table: shape.name.to_string(),
                    detail: format!(
                        "the table is absent (`PRAGMA table_info` reports no columns \
                         at all); this build expects {} column(s) starting with \
                         `{}`",
                        shape.columns.len(),
                        shape.columns.first().map(|c| c.name).unwrap_or("?"),
                    ),
                });
            }

            // Walk the two lists together. `found` is in `cid` order, which is
            // declaration order, which is the order the mappers assume.
            let mut at = 0usize;
            while at < shape.columns.len() {
                let want = &shape.columns[at];
                let Some((_, got_name, got_type)) = found.get(at) else {
                    // Ran out of actual columns with expected ones left: the
                    // file is behind this build.
                    return Err(DbError::SchemaFork {
                        table: shape.name.to_string(),
                        detail: format!(
                            "column `{}` (position {at}, declared `{}`) is MISSING — \
                             the file has {} column(s), this build expects {}; this \
                             file PREDATES this build or is corrupt",
                            want.name,
                            want.decl_type,
                            found.len(),
                            shape.columns.len(),
                        ),
                    });
                };
                if got_name != want.name {
                    // The name at this position is someone else's. Two readings,
                    // and the message gives the operator both because the fix
                    // differs: a rename (the column exists elsewhere in the
                    // table) or a reorder (it exists at another position).
                    let elsewhere = found
                        .iter()
                        .position(|(_, n, _)| n == want.name)
                        .map(|at_other| {
                            format!(
                                "`{}` IS present, at position {at_other} — the columns \
                                 are REORDERED",
                                want.name
                            )
                        })
                        .unwrap_or_else(|| {
                            format!(
                                "`{}` is MISSING from the table entirely (no column of \
                                 that name at any position)",
                                want.name
                            )
                        });
                    return Err(DbError::SchemaFork {
                        table: shape.name.to_string(),
                        detail: format!(
                            "position {at}: this build expects `{}` (declared `{}`), \
                             the file has `{}`. {elsewhere}",
                            want.name, want.decl_type, got_name
                        ),
                    });
                }
                // Same name, same position: the declared type is the last thing
                // that can differ, and it differs quietly (SQLite coerces), so
                // it is compared too. Case-insensitively: SQLite stores the type
                // as written, and the DDL here is upper case while a hand-edited
                // file need not be.
                let got_type = got_type.as_deref().unwrap_or("");
                if !got_type.eq_ignore_ascii_case(want.decl_type) {
                    return Err(DbError::SchemaFork {
                        table: shape.name.to_string(),
                        detail: format!(
                            "column `{}` (position {at}) is declared `{}`, this build \
                             expects `{}` — a RETYPED column, which SQLite coerces \
                             silently instead of erroring",
                            want.name, got_type, want.decl_type
                        ),
                    });
                }
                at += 1;
            }

            // Every expected column matched position for position; anything LEFT
            // over in the file is a column this build has never heard of.
            if found.len() > shape.columns.len() {
                let extras: Vec<&str> = found[shape.columns.len()..]
                    .iter()
                    .map(|(_, name, _)| name.as_str())
                    .collect();
                return Err(DbError::SchemaFork {
                    table: shape.name.to_string(),
                    detail: format!(
                        "the file has {} column(s), this build knows {} — UNEXPECTED \
                         column(s) {}. This file comes from a NEWER schema than this \
                         build; refusing rather than silently ignoring columns it \
                         does not read",
                        found.len(),
                        shape.columns.len(),
                        extras.join(", "),
                    ),
                });
            }
        }
        Ok(())
    }

    /// The lock, or the guard a panicked holder left behind.
    ///
    /// `unwrap_or_else(PoisonError::into_inner)` rather than `expect`: a panic
    /// while holding this lock leaves a connection that is still perfectly
    /// usable (rusqlite's own statements are individually atomic, and every
    /// multi-statement write here runs in a transaction that was never committed,
    /// so SQLite rolls it back). A daemon that took all of its clients down
    /// because one write panicked is worse than one that kept serving from an
    /// unharmed connection — and ADR 0025's `panic = "unwind"` posture exists to
    /// keep exactly that supervisor alive.
    fn conn(&self) -> std::sync::MutexGuard<'_, Connection> {
        self.conn.lock().unwrap_or_else(|p| p.into_inner())
    }

    // ── Introspection (the tests' view of what actually took) ─────────────────

    /// What `PRAGMA journal_mode` reports — `wal`, or the mode the filesystem
    /// actually got. The assertion the durability claim rests on.
    pub fn journal_mode(&self) -> Result<String, DbError> {
        Ok(self
            .conn()
            .query_row("PRAGMA journal_mode", [], |row| row.get(0))?)
    }

    /// What `PRAGMA synchronous` reports, lowercased (`0`/`1`/`2`/`3` are
    /// normalised to `off`/`normal`/`full`/`extra` here so the assertion reads as
    /// the pragma was written).
    pub fn synchronous(&self) -> Result<String, DbError> {
        let raw = self
            .conn()
            .query_row("PRAGMA synchronous", [], |row| row.get::<_, i64>(0))?;
        Ok(match raw {
            0 => "off".to_string(),
            1 => "normal".to_string(),
            2 => "full".to_string(),
            3 => "extra".to_string(),
            other => other.to_string(),
        })
    }

    /// Whether `PRAGMA foreign_keys` is on for THIS connection (it is a
    /// per-connection setting, so this is the only honest way to ask).
    pub fn foreign_keys_enabled(&self) -> Result<bool, DbError> {
        Ok(self
            .conn()
            .query_row("PRAGMA foreign_keys", [], |row| row.get::<_, i64>(0))?
            != 0)
    }

    /// The stored `user_version`.
    pub fn user_version(&self) -> Result<i64, DbError> {
        Ok(self
            .conn()
            .query_row("PRAGMA user_version", [], |row| row.get(0))?)
    }

    /// Every user table in the file. A fifth entry here is a fork of the
    /// desktop's schema, and this is how that fork gets noticed.
    pub fn table_names(&self) -> Result<Vec<String>, DbError> {
        let guard = self.conn();
        let mut stmt = guard.prepare(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'",
        )?;
        let names = stmt
            .query_map([], |row| row.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(names)
    }

    /// Whether `table` carries a UNIQUE constraint over exactly `columns`
    /// (order-insensitive). The persister's idempotency IS these constraints, so
    /// the test asks the database rather than reading the DDL and hoping.
    ///
    /// `PRAGMA index_list` names the indexes and says which are UNIQUE and where
    /// they came from (`origin = 'u'` is a table UNIQUE constraint, `'pk'` a
    /// primary key, `'c'` a standalone `CREATE UNIQUE INDEX`); `PRAGMA index_info`
    /// then gives one index's columns. Filtering on `'u'` is what keeps a primary
    /// key from being mistaken for the upsert key.
    pub fn table_has_unique(&self, table: &str, columns: &[&str]) -> bool {
        let mut want: Vec<String> = columns.iter().map(|c| c.to_lowercase()).collect();
        want.sort();

        let guard = self.conn();
        let Ok(mut list) = guard.prepare(&format!("PRAGMA index_list('{table}')")) else {
            return false;
        };
        // (name, is_unique, origin)
        let indexes = match list.query_map([], |row| {
            Ok((
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)? != 0,
                row.get::<_, String>(3)?,
            ))
        }) {
            Ok(rows) => rows.filter_map(Result::ok).collect::<Vec<_>>(),
            Err(_) => return false,
        };
        drop(list);

        for (name, unique, origin) in indexes {
            if !unique || origin != "u" {
                continue;
            }
            let Ok(mut info) = guard.prepare(&format!("PRAGMA index_info('{name}')")) else {
                continue;
            };
            let Ok(cols) = info
                .query_map([], |row| row.get::<_, Option<String>>(2))
                .and_then(|rows| rows.collect::<Result<Vec<_>, _>>())
            else {
                continue;
            };
            // An expression index has a NULL column name; those never match a
            // plain column list, so they are skipped rather than coerced.
            let mut got: Vec<String> = cols
                .into_iter()
                .flatten()
                .map(|c| c.to_lowercase())
                .collect();
            if got.len() != want.len() {
                continue;
            }
            got.sort();
            if got == want {
                return true;
            }
        }
        false
    }

    /// Rows in one of the daemon's own tables. `table` is not parameterised (SQL
    /// identifiers cannot bind), so the name is checked against the table list
    /// first — a typo fails here rather than becoming a `SELECT` string anyone
    /// can shape.
    pub fn count_rows(&self, table: &str) -> Result<i64, DbError> {
        if !self.table_names()?.iter().any(|t| t == table) {
            return Err(DbError::Sqlite(rusqlite::Error::SqliteFailure(
                rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_ERROR),
                Some(format!("no such table: {table}")),
            )));
        }
        Ok(self
            .conn()
            .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                row.get(0)
            })?)
    }

    // ── The SQL ───────────────────────────────────────────────────────────────
    //
    // Every statement below is the desktop's, column for column, including the
    // deliberate silent no-ops (`record_session_cost` / `record_session_todos`
    // on a missing row) that `persist.rs`'s Trap 1 depends on. The two places
    // this file DEVIATES from the desktop are commented at the statement.

    /// The ONE `sessions` SELECT list every `SessionRow` read uses.
    ///
    /// `todos_json` and `sort_order` are deliberately ABSENT: `SessionRow` has no
    /// field for either (`sort_order` is an ordering key, `todos_json` has its own
    /// reader, `session_todos`). The arity tie below is what catches a half-finished
    /// edit; [`session_row_from`] is the mapper it must agree with.
    const SESSION_COLUMNS: [&str; 10] = [
        "id",
        "cwd",
        "created_at",
        "title",
        "capabilities_json",
        "archived",
        "context_usage_json",
        "is_subagent",
        "cost_usd",
        "title_stage",
    ];

    const MESSAGE_COLUMNS: [&str; 6] = [
        "id",
        "session_id",
        "kind",
        "message_key",
        "payload_json",
        "created_at",
    ];

    /// Record (or refresh) a session — the desktop's upsert, verbatim.
    ///
    /// `created_at` and `title` are preserved on a re-record (a resume re-runs
    /// this); cwd / capabilities / the subagent flag are refreshed. A NEW row
    /// joins the list at the BOTTOM (`MAX + 1`) and the CONFLICT branch never
    /// renumbers — a row the user positioned must not move when its session is
    /// resumed (ADR 0075).
    pub fn record_session(
        &self,
        info: &fulcrum_harness::store_rows::SessionInfo,
    ) -> Result<(), DbError> {
        let capabilities = serde_json::to_string(&info.capabilities)?;
        self.conn().execute(
            "INSERT INTO sessions (id, cwd, created_at, title, capabilities_json, is_subagent, sort_order)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, COALESCE((SELECT MAX(sort_order) FROM sessions), -1) + 1)
             ON CONFLICT(id) DO UPDATE SET
               cwd = excluded.cwd,
               capabilities_json = excluded.capabilities_json,
               is_subagent = excluded.is_subagent",
            params![
                info.session_id,
                info.cwd.display().to_string(),
                fulcrum_harness::clock::now_ms(),
                Option::<String>::None,
                capabilities,
                info.is_subagent,
            ],
        )?;
        Ok(())
    }

    /// Persist the session's last known context usage as the JSON envelope the
    /// engine reads back. A separate statement: the `record_session` upsert's
    /// `DO UPDATE` never touches `context_usage_json`, so a resume cannot clobber
    /// it. A missing row is a no-op.
    pub fn record_session_context_usage(
        &self,
        id: &str,
        used: u64,
        window: u64,
    ) -> Result<(), DbError> {
        let payload = serde_json::to_string(&serde_json::json!({
            "used": used,
            "window": window,
        }))?;
        self.conn().execute(
            "UPDATE sessions SET context_usage_json = ?2 WHERE id = ?1",
            params![id, payload],
        )?;
        Ok(())
    }

    /// The `sessions.todos_json` write (ADR 0073). A missing row is a no-op.
    pub fn record_session_todos(&self, id: &str, todos_json: Option<&str>) -> Result<(), DbError> {
        self.conn().execute(
            "UPDATE sessions SET todos_json = ?2 WHERE id = ?1",
            params![id, todos_json],
        )?;
        Ok(())
    }

    /// The stored todos JSON (`None` when missing or NULL).
    pub fn session_todos(&self, id: &str) -> Result<Option<String>, DbError> {
        let guard = self.conn();
        let mut stmt = guard.prepare("SELECT todos_json FROM sessions WHERE id = ?1")?;
        Ok(
            match stmt.query_row(params![id], |row| row.get::<_, Option<String>>(0)) {
                Ok(val) => val,
                Err(rusqlite::Error::QueryReturnedNoRows) => None,
                Err(e) => return Err(e.into()),
            },
        )
    }

    /// The `sessions.cost_usd` SNAPSHOT write (Fulcrum T2's amendment of ADR
    /// 0082's limit 7): `total` is the CUMULATIVE figure and this overwrites with
    /// it, so a replayed frame writes the same number — idempotent with no dedupe
    /// and no read-modify-write.
    ///
    /// NO `SELECT` before the `UPDATE`, and the `execute` count is DISCARDED on
    /// purpose: a missing row matching zero rows is the silent `Ok(())` that
    /// `persist.rs`'s Trap 1 requires the CALLER to guard (it calls
    /// `ensure_session_row` first). Do not "fix" this into an error.
    pub fn record_session_cost(&self, id: &str, total: f64) -> Result<(), DbError> {
        self.conn().execute(
            "UPDATE sessions SET cost_usd = ?2 WHERE id = ?1",
            params![id, total],
        )?;
        Ok(())
    }

    /// The stored cost. `None` covers both "no such row" and "a row that never
    /// had a cost frame"; `Some(0.0)` is a genuine zero (D5 / ADR 0082) and is
    /// never collapsed into `None`.
    pub fn session_cost(&self, id: &str) -> Result<Option<f64>, DbError> {
        let guard = self.conn();
        let mut stmt = guard.prepare("SELECT cost_usd FROM sessions WHERE id = ?1")?;
        Ok(
            match stmt.query_row(params![id], |row| row.get::<_, Option<f64>>(0)) {
                Ok(val) => val,
                Err(rusqlite::Error::QueryReturnedNoRows) => None,
                Err(e) => return Err(e.into()),
            },
        )
    }

    /// One session row by id (`None` when absent).
    ///
    /// Absence is matched EXPLICITLY rather than via `query_map(..).next()
    /// .transpose()`: once the mapper can error, that chain makes "row present but
    /// failed to map" indistinguishable from "no such row".
    pub fn session(
        &self,
        id: &str,
    ) -> Result<Option<fulcrum_harness::store_rows::SessionRow>, DbError> {
        let guard = self.conn();
        let mut stmt = guard.prepare(&format!(
            "SELECT {} FROM sessions WHERE id = ?1",
            Self::SESSION_COLUMNS.join(", ")
        ))?;
        Ok(match stmt.query_row(params![id], session_row_from) {
            Ok(row) => Some(row),
            Err(rusqlite::Error::QueryReturnedNoRows) => None,
            Err(e) => return Err(e.into()),
        })
    }

    /// All stored sessions in the user's manual order, `id` descending on ties.
    ///
    /// BOTH arms filter `is_subagent = 0` (a subagent's row is hidden by
    /// definition — ADR 0025 §3) and share ONE `ORDER BY`, so the Archived
    /// section cannot drift from the live one. `include_archived` only decides
    /// whether `archived = 0` joins the filter.
    pub fn list_sessions(
        &self,
        include_archived: bool,
    ) -> Result<Vec<fulcrum_harness::store_rows::SessionRow>, DbError> {
        let guard = self.conn();
        let sql = if include_archived {
            format!(
                "SELECT {} FROM sessions
                 WHERE is_subagent = 0
                 ORDER BY sort_order ASC, id DESC",
                Self::SESSION_COLUMNS.join(", ")
            )
        } else {
            format!(
                "SELECT {} FROM sessions
                 WHERE archived = 0 AND is_subagent = 0
                 ORDER BY sort_order ASC, id DESC",
                Self::SESSION_COLUMNS.join(", ")
            )
        };
        let mut stmt = guard.prepare(&sql)?;
        let rows = stmt
            .query_map([], session_row_from)?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    /// Insert or refresh one DISPLAY row. The `(session_id, kind, message_key)`
    /// triple always collapses to one row, so a replayed frame never duplicates.
    pub fn record_message(
        &self,
        session_id: &str,
        kind: &str,
        message_key: Option<&str>,
        payload_json: &str,
    ) -> Result<(), DbError> {
        self.conn().execute(
            "INSERT INTO messages (session_id, kind, message_key, payload_json, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT(session_id, kind, message_key) DO UPDATE SET
               payload_json = excluded.payload_json,
               created_at = excluded.created_at",
            params![
                session_id,
                kind,
                message_key,
                payload_json,
                fulcrum_harness::clock::now_ms()
            ],
        )?;
        Ok(())
    }

    /// A session's display rows in insertion order.
    pub fn messages_for(
        &self,
        session_id: &str,
    ) -> Result<Vec<fulcrum_harness::store_rows::MessageRow>, DbError> {
        let guard = self.conn();
        let mut stmt = guard.prepare(&format!(
            "SELECT {} FROM messages
             WHERE session_id = ?1
             ORDER BY id",
            Self::MESSAGE_COLUMNS.join(", ")
        ))?;
        let rows = stmt
            .query_map(params![session_id], message_row_from)?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    /// Insert or refresh one native transcript row — the idempotent
    /// `(session_id, seq)` collapse. `role` is a denormalized index; the content
    /// is the serialized `ChatMessage`.
    pub fn insert_native_message(
        &self,
        session_id: &str,
        seq: i64,
        role: &str,
        content_json: &str,
    ) -> Result<(), DbError> {
        self.conn().execute(
            "INSERT INTO native_messages (session_id, seq, role, content_json, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT(session_id, seq) DO UPDATE SET
               content_json = excluded.content_json,
               created_at = excluded.created_at",
            params![
                session_id,
                seq,
                role,
                content_json,
                fulcrum_harness::clock::now_ms()
            ],
        )?;
        Ok(())
    }

    /// The provider transcript in `seq` order, as raw `content_json` STRINGS —
    /// parsing them is the caller's job, so this layer never names `ChatMessage`.
    pub fn load_native_messages(&self, session_id: &str) -> Result<Vec<String>, DbError> {
        let guard = self.conn();
        let mut stmt = guard.prepare(
            "SELECT content_json FROM native_messages WHERE session_id = ?1 ORDER BY seq",
        )?;
        let rows = stmt
            .query_map(params![session_id], |row| row.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    /// The compaction rewrite, ATOMIC: delete every row, then insert the new
    /// ones, in ONE transaction — a crash mid-rewrite can never leave an empty or
    /// partial transcript on disk.
    pub fn replace_native_messages(
        &self,
        session_id: &str,
        rows: &[(i64, String, String)],
    ) -> Result<(), DbError> {
        let mut guard = self.conn();
        let tx = guard.transaction()?;
        tx.execute(
            "DELETE FROM native_messages WHERE session_id = ?1",
            params![session_id],
        )?;
        for (seq, role, content_json) in rows {
            tx.execute(
                "INSERT INTO native_messages (session_id, seq, role, content_json, created_at)
                 VALUES (?1, ?2, ?3, ?4, ?5)
                 ON CONFLICT(session_id, seq) DO UPDATE SET
                   content_json = excluded.content_json,
                   created_at = excluded.created_at",
                params![
                    session_id,
                    *seq,
                    *role,
                    *content_json,
                    fulcrum_harness::clock::now_ms()
                ],
            )?;
        }
        tx.commit()?;
        Ok(())
    }

    /// Delete a session.
    ///
    /// DEVIATES from the desktop on purpose. The desktop's version pre-deletes the
    /// `native_messages` rows as "belt-and-suspenders" for a pre-migration file
    /// that never gained the FK; `fulcrum.db` is born with the FK, so this issues
    /// ONE `DELETE FROM sessions` and lets `ON DELETE CASCADE` take both child
    /// tables. That is not cosmetic: the pre-delete made it impossible to tell a
    /// working cascade from a dead one, because the rows were gone either way.
    /// `deleting_a_session_cascades_to_its_messages` can only fail here — which is
    /// the point.
    pub fn delete_session(&self, session_id: &str) -> Result<(), DbError> {
        self.conn()
            .execute("DELETE FROM sessions WHERE id = ?1", params![session_id])?;
        Ok(())
    }

    /// Insert or touch a Space row, keyed by the canonical path (ADR 0010).
    ///
    /// `created_at` is preserved and `trusted` is NEVER touched on conflict (no
    /// retroactive trust); `last_opened_at` always refreshes. `default_trusted`
    /// lands only on INSERT.
    ///
    /// The path is canonicalized with the desktop's raw-string fallback (a
    /// best-effort write; reads stay fail-closed) — but note this is the ONE
    /// place the daemon could diverge from the desktop's key form, and the
    /// frontend joins Spaces to sessions by raw string equality on `sessions.cwd`
    /// while `record_session` stores `info.cwd.display()` UN-canonicalized. Both
    /// sides keep the desktop's behaviour, so the two strings stay the pair the
    /// desktop's own test pins them as; changing either one alone is the defect
    /// that test exists to catch.
    pub fn upsert_space(&self, path: &str, default_trusted: bool) -> Result<(), DbError> {
        let key = space_key(path).unwrap_or_else(|| path.to_string());
        let now = fulcrum_harness::clock::now_ms();
        self.conn().execute(
            "INSERT INTO spaces (path, created_at, last_opened_at, trusted, sort_order) \
             VALUES (?1, ?2, ?2, ?3, COALESCE((SELECT MAX(sort_order) FROM spaces), -1) + 1)
                 ON CONFLICT(path) DO UPDATE SET last_opened_at = excluded.last_opened_at",
            params![key, now, default_trusted],
        )?;
        Ok(())
    }

    /// Set a Space's trust flag. `true` = a row matched; a missing row is a no-op
    /// that says so, not an error.
    pub fn set_space_trusted(&self, path: &str, trusted: bool) -> Result<bool, DbError> {
        let key = space_key(path).unwrap_or_else(|| path.to_string());
        let n = self.conn().execute(
            "UPDATE spaces SET trusted = ?2 WHERE path = ?1",
            params![key, trusted],
        )?;
        Ok(n > 0)
    }
}

/// The single key form of a `spaces` row (ADR 0010): the canonical path. `None`
/// when the path cannot be canonicalized (the folder is gone, the FS moved).
fn space_key(path: impl AsRef<std::path::Path>) -> Option<String> {
    std::fs::canonicalize(path)
        .ok()
        .map(|c| c.display().to_string())
}

/// Map one row of [`Db::SESSION_COLUMNS`] onto a `SessionRow`, index for index.
///
/// The two `get::<_, i64>` calls on the INTEGER-as-bool columns are a RUNTIME
/// conversion, not a compile-time check: a DDL type change fails loudly with
/// `InvalidColumnType` instead of silently mis-mapping.
fn session_row_from(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<fulcrum_harness::store_rows::SessionRow> {
    Ok(fulcrum_harness::store_rows::SessionRow {
        id: row.get(0)?,
        cwd: row.get(1)?,
        created_at: row.get(2)?,
        title: row.get(3)?,
        capabilities_json: row.get(4)?,
        archived: row.get::<_, i64>(5)? != 0,
        context_usage_json: row.get(6)?,
        is_subagent: row.get::<_, i64>(7)? != 0,
        cost_usd: row.get(8)?,
        title_stage: row.get(9)?,
    })
}

/// Map one row of [`Db::MESSAGE_COLUMNS`] onto a `MessageRow`, index for index.
fn message_row_from(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<fulcrum_harness::store_rows::MessageRow> {
    Ok(fulcrum_harness::store_rows::MessageRow {
        id: row.get(0)?,
        session_id: row.get(1)?,
        kind: row.get(2)?,
        message_key: row.get(3)?,
        payload_json: row.get(4)?,
        created_at: row.get(5)?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The arity tie: the SELECT list and the mapper must move together. This
    /// catches a half-finished edit (a column added to one and not the other);
    /// the row-mapping test in `tests/db.rs` is the guard for the rest.
    const _: [(); 10] = [(); Db::SESSION_COLUMNS.len()];
    const _: [(); 6] = [(); Db::MESSAGE_COLUMNS.len()];

    #[test]
    fn a_fresh_store_has_every_pragma_it_claims() {
        let dir = tempfile::tempdir().expect("temp dir");
        let db = Db::open(&dir.path().join("fulcrum.db")).expect("open");
        assert_eq!(db.journal_mode().expect("mode").to_lowercase(), "wal");
        assert_eq!(db.synchronous().expect("sync"), "normal");
        assert!(
            db.foreign_keys_enabled().expect("fk"),
            "foreign_keys is per-connection and defaults OFF"
        );
        assert_eq!(db.user_version().expect("version"), SCHEMA_VERSION);
    }

    #[test]
    fn count_rows_refuses_a_table_that_is_not_there() {
        let dir = tempfile::tempdir().expect("temp dir");
        let db = Db::open(&dir.path().join("fulcrum.db")).expect("open");
        // The name is checked against the real table list, so a typo cannot
        // become a shaped `SELECT`.
        let err = db.count_rows("sessions; DROP TABLE spaces").unwrap_err();
        assert!(err.to_string().contains("no such table"));
        assert!(db.table_names().unwrap().contains(&"spaces".to_string()));
    }

    /// `table_has_unique` must be able to say NO. A probe that always returned
    /// `true` would make the schema test in `tests/db.rs` vacuous, so the
    /// negative cases are pinned beside the probe: a subset of the key, a
    /// different key, a PRIMARY KEY (which is `origin = 'pk'`, not a UNIQUE
    /// constraint), and a table with no UNIQUE at all (`spaces`).
    #[test]
    fn the_unique_probe_answers_no_when_the_constraint_is_not_there() {
        let dir = tempfile::tempdir().expect("temp dir");
        let db = Db::open(&dir.path().join("fulcrum.db")).expect("open");

        // The real key answers yes (the positive case lives in `tests/db.rs`).
        assert!(db.table_has_unique("messages", &["session_id", "kind", "message_key"]));
        // A strict SUBSET of the key is a different constraint and does not exist.
        assert!(!db.table_has_unique("messages", &["session_id", "kind"]));
        // A SUPERSET neither.
        assert!(!db.table_has_unique("messages", &["session_id", "kind", "message_key", "id"]));
        // Order-insensitive, though — the constraint is a set of columns.
        assert!(db.table_has_unique("messages", &["message_key", "session_id", "kind"]));
        // `sessions.id` is a PRIMARY KEY, which the probe must not report as a
        // UNIQUE constraint (`origin = 'pk'`).
        assert!(!db.table_has_unique("sessions", &["id"]));
        // `spaces` has a primary key and no UNIQUE constraint at all.
        assert!(!db.table_has_unique("spaces", &["path"]));
        // A table that does not exist answers no rather than panicking.
        assert!(!db.table_has_unique("nope", &["whatever"]));
    }

    /// A file whose `user_version` is AHEAD of this build must be refused, not
    /// opened. Without this, a daemon built against an older schema would write
    /// rows a newer daemon cannot read, and the corruption would be silent.
    #[test]
    fn a_newer_schema_version_is_refused_rather_than_opened() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("fulcrum.db");
        {
            let db = Db::open(&path).expect("open");
            // Pretend a NEWER daemon migrated this file.
            db.conn()
                .pragma_update(None, "user_version", SCHEMA_VERSION + 1)
                .expect("bump user_version");
        }

        // Matched rather than `expect_err`, which would need `Debug` on `Db` —
        // and a `Mutex<Connection>` has nothing worth printing.
        let err = match Db::open(&path) {
            Ok(_) => panic!("a database newer than this build must NOT open"),
            Err(e) => e,
        };
        match err {
            DbError::SchemaTooNew { found, supported } => {
                assert_eq!(found, SCHEMA_VERSION + 1);
                assert_eq!(supported, SCHEMA_VERSION);
            }
            other => panic!("expected SchemaTooNew, got {other:?}"),
        }
        // And the refusal left the file alone — the newer daemon's rows are intact.
        assert!(path.exists());
    }
}
